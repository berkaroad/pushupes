# PushupES

面向 CQRS 框架的**领域事件流存储服务**：只追加（append-only）、高可用、高性能。
Go 实现，单二进制、无外部依赖（不需要 ZooKeeper/etcd），内置 React 管理控制台。

分层原则：**Raft 只复制元数据**（槽位分配表、成员、节点地址），**事件数据走
leader→follower 专用拉取协议**（ISR + 高水位语义），两条链路互不影响。

```
客户端 ──gRPC(EventService)──▶ 槽 leader ──append-only WAL──▶ 本地落盘
                                  │
                     follower 长轮询拉取（PeerService.MFetch，与 Raft 同端口）
                                  │
Raft（自研简化 Raft + 自研分段 WAL，仅元数据）──▶ 槽位分配表 / epoch / 集群成员
```

## 集群拓扑

以 **3 个控制节点 + 2 个数据节点** 为例（medium 副本策略：每槽 2 份，
两个数据节点互为对方的副本）：

```
客户端（CQRS 框架）
  │ gRPC EventService（client 面 85xx）：读写都直连槽 leader
  │ 收 MOVED(1003)/ASK(1004) → 刷新本地 slot→node 路由缓存后重试
  ▼
控制面 · 3 个控制节点（role=voter）—— Raft 法定数 2/3
  ctrl-1 ◀──Raft 日志复制──▶ ctrl-2 ◀──Raft 日志复制──▶ ctrl-3
  只复制元数据：槽位分配表 slot→{leader, replicas[], epoch}、集群成员、副本策略
  voter 不承载事件数据；controller 在控制面上：每 1s 探活，
  某节点连续 3 次失败 → 从其 ISR 副本重选该节点名下所有槽的 leader（epoch+1）
  │ WatchLayout：把槽位分配表分发到各数据节点（peer 面 83xx）
  │ 探活 / 新节点报名（peer 面 83xx）
  ▼
数据面 · 2 个数据节点（role=learner）—— 承载槽副本：append-only WAL + 派生索引
  data-1：leader 槽 0–511 ┃ 副本 槽 512–1023
  data-2：leader 槽 512–1023 ┃ 副本 槽 0–511
  data-1 ◀──MFetch 长轮询拉取（peer 面 83xx，与 Raft 同端口族）──▶ data-2
  follower 按 leader 分组批量拉增量，应答是裸 WAL 字节、原样落盘，
  批量回报 LEO，leader 推进 HW = ISR 内最小 LEO；
  写入等到高水位覆盖该条记录才回 success（单节点宕机不丢已确认写入）
```

三条链路各走各的面：共识与数据复制都走 peer 面，事件读写只走 client 面，
运维与观察（含管理控制台）走 admin 面（80xx）。

## 功能特性

### 高可用与复制

- 控制面用自研 Raft 只复制**元数据**（槽位分配表与集群成员）；事件数据由副本
  向 leader 拉取复制，两相分离。
- 写入经全部同步副本落盘后才确认：回 `success` 的写入不会因单节点宕机而丢失；
  副本全部掉线的极端情况下可用性优先（承诺只对同步副本成立）。
- 节点失联自动故障切换（该节点名下所有槽重选 leader），恢复后自动回切；读写
  都靠 MOVED/ASK 重定向到达槽 owner，服务端不中继流量，客户端库自动跟随。
- 读只返回已同步高水位以内的记录，不会读到被回滚的脏数据。

### 槽位热迁移（不停写）

- 副本因子增减、节点加入/重加入、leader 回切、副本修复，都走同一条
  「入场 → 追平 → 提交」链路，全程不停写、不丢已确认写入。
- 迁移到副本集以外的目标一次调用自动完成；迁走后原节点的本地副本按宽限期
  （`-drop-after`）自动清理。
- 客户端无感：路由缓存随重定向自动刷新，幂等规则保证转发期间的写入不重复。

### 再平衡（Rebalance）

- controller 周期（`-rebalance-interval`，默认 2s，可关闭）把各槽 leader 交接
  回环上的归属位置，槽主归属在数据节点间均摊；故障切换留下的失衡自动回切。
- 交接逐个进行，写扰动恒定限于单个槽，节点重启后无切换风暴；再平衡全程不停写。

### 节点级流控（令牌桶）

- 每个节点独立的写入令牌桶：admin HTTP（`POST /admin/flow-control`）就地配置，
  只限该节点作为 leader 的写入，稳态上限 = `tokens / period`；桶空写入报
  `fail/1006`，副本复制流量不占预算。
- 运维应急面：不落盘、不进集群共识，重启即恢复不限流，`DELETE` 立即解除。
- 客户端库把 1006 吸收为回压自动退避重发，调用方无感；控制台节点卡片显示
  流控配置，触发限流时闪烁提醒，并可直接设置。

### 管理控制台（frontend/）

React 管理台：集群页（存储数量/大小、客户端写入速度、副本策略与同步状态、
按面分组的节点卡片与写入速率折线图、流控入口）、槽位页（分配表、槽详情与
迁移发起）、事件流页；light/dark 主题，2s 自动刷新可开关。

成员管理（加入/移除节点）与副本策略档位等运维命令的完整形态见 DESIGN.md；
以上各机制的设计推导同样以 DESIGN.md 为准。

## 部署与启动

### 构建

要求 Go 1.26+：

```bash
CGO_ENABLED=0 go build -o bin/pushupes ./cmd/pushupes   # 纯 Go 静态链接，不依赖 glibc
go build ./... && go vet ./... && go test ./... -count=1   # 全量门禁
```

前端（可选，构建产物 `frontend/dist` 供静态托管）：

```bash
cd frontend && npm install && npm run build
```

### 一键起 3 节点集群（推荐）

```bash
REPLICAS=5 VOTERS=3 scripts/cluster.sh start   # 一条命令建整簇：3 控制节点 + 2 数据节点
scripts/cluster.sh restart   # 裸跑即可：节点数、每节点角色、端口都读自 $RUN_DIR/cluster.env
scripts/cluster.sh status    # 各节点 Raft 角色、leader 槽数、迁移中槽数
scripts/cluster.sh smoke     # 端到端冒烟：MOVED → v1/v2 写入 → 幂等 exists
                             #           → 版本冲突 fail/1001 → 回读
scripts/cluster.sh slotcheck # 副本一致性体检：逐槽比较 leader 与各副本的聚合摘要，
                             #   报出「LEO 相同但目录偏短」的发散副本（有则退出码 1）
                             #   可加 -quiet / -json / -list=N 透传给 bin/slotcheck
scripts/cluster.sh logs 1    # 跟踪 node-1 日志
scripts/cluster.sh restart   # 重启（保留数据，验证 WAL/Raft 崩溃恢复）
scripts/cluster.sh stop      # 停止（只杀 pid 文件记录的进程）
scripts/cluster.sh clean     # stop 并删除运行目录（含数据，慎用）
```

每个节点占用三个端口，按节点序号依次递增（node-i = BASE + i − 1）；数据、日志、
pid 分别放在 `$RUN_DIR/node-i/`（默认在仓库根的 `.cluster/`）。可用环境变量覆盖：
`REPLICAS`（节点数）、`VOTERS`（控制面节点数：node-1..VOTERS=voter，其余=learner；
也可用 `ROLE_<n>=voter|learner` 逐个点名）、`HOST`、`ADMIN_BASE`、`PEER_BASE`、
`CLIENT_BASE`、`SEGMENT_BYTES`、`RUN_DIR`、`READY_TIMEOUT`、`BUILD`。`start`/`join`
会把生效形态（节点数、每节点角色、端口）写进 `$RUN_DIR/cluster.env`，之后裸跑
`restart`/`stop`/`status`/`smoke` 都按它执行，命令行显式传的变量仍优先；运行目录里
只有节点目录而没有 `cluster.env`（老目录）时脚本按最大 `node-N` 推断节点数并提示补传
角色。每槽副本数由副本策略分档（`PUSHUPES_REPLICA_POLICY`=low/medium/high，默认
medium：1/2/3 份，或正整数直接指定副本因子；副本因子 = MIN(数据节点数, 策略要求份数)）：该值只为新集群种初始值，策略存放在 Raft 复制的
槽表里，之后改档位走 `POST /admin/cluster/replica-policy`（控制台集群页的「副本策略」
卡片即可操作）。扩节点两种做法等价：`REPLICAS=<新总数> cluster.sh start`（集群已存在
时，缺的节点自动报名加入）或逐个 `cluster.sh join N`。

集群配置只由**一个**节点写下（`-peers` 里 id 最小的那个，或用 `-bootstrap` 指定的
那个）；其余配置了 `-peers` 的节点一律以「种子」身份启动、向这些成员报名加入，自己
不写配置。所以「把新节点用更大的 `-peers` 列表拉起来」就是扩容，它不会和现有集群
各写一份配置而分裂成多个各自能提交的 raft 组。

### 手工启动单个节点

```bash
# 最小部署 = 1 个控制节点（voter）+ 1 个数据节点（learner）。-role 必填，
# 只能是这两个值：voter 投票但不承载槽副本，learner 承载副本但不投票。
# -peers 只列控制面（voter）节点：它既是 voter 的初始配置种子，也是 learner
# 连控制面的投递点（注册与布局分发都走 peer 端口）。
# learner 要先进目录（控制面 POST /admin/cluster/nodes {"role":"learner"}）
# 再启动进程；准入与拉起顺序见 scripts/cluster.sh。
./bin/pushupes -node node-1 -role voter \
  -peer 127.0.0.1:8391 -admin 127.0.0.1:8091 -client 127.0.0.1:8591 \
  -data ./node-1 \
  -peers 'node-1=127.0.0.1:8391'

./bin/pushupes -node node-2 -role learner \
  -peer 127.0.0.1:8392 -admin 127.0.0.1:8092 -client 127.0.0.1:8592 \
  -data ./node-2 \
  -peers 'node-1=127.0.0.1:8391'

# 再加一个控制成员：它不在配置里，按 -peers 里的成员报名进 raft（无需 -join
# 也能轮转报名；-join 只用于点名某一个成员）。
./bin/pushupes -node node-3 -role voter \
  -peer 127.0.0.1:8393 -admin 127.0.0.1:8093 -client 127.0.0.1:8593 \
  -data ./node-3 \
  -peers 'node-1=127.0.0.1:8391' \
  -join 127.0.0.1:8391
```

地址规范：地址统一带 scheme 存储，未写协议时默认补 `http://`；`-peers`
也接受裸 `host:peerport`（此时地址兼作节点 id）。

### 启动参数

| flag | env | 默认 | 说明 |
|---|---|---|---|
| `-node` | `PUSHUPES_NODE` | `node-1` | 节点 id |
| `-role` | `PUSHUPES_ROLE` | 无（必填） | 节点角色，只能是两者之一：`voter`=控制面（参与选举、**不承载槽副本**）、`learner`=数据面（不投票，从控制面接收布局分发）。最小部署 = 1 个 `voter` + 1 个 `learner`；缺失或非法值启动即报错。角色由节点自报进复制目录，布局池只认 `learner` 成员。`learner` 的槽表由控制面经 `WatchLayout` 流分发（DESIGN §12.4），它不建共识组、不提交、也不报名成为投票者 |
| `-peer` | `PUSHUPES_PEER` | `http://127.0.0.1:8391` | peer 面：Raft + PeerService gRPC（全部节点间流量，运维只需配这个端口） |
| `-admin` | `PUSHUPES_ADMIN` | `http://127.0.0.1:8091` | admin 面：HTTP 管理 API + pprof |
| `-client` | `PUSHUPES_CLIENT` | `http://127.0.0.1:8591` | client 面：gRPC 事件读写唯一入口 |
| `-data` | `PUSHUPES_DATA` | `./data` | 数据目录 |
| `-peers` | `PUSHUPES_PEERS` | 空 | 集群种子 `id=host:peerport,...` |
| `-join` | `PUSHUPES_JOIN` | 空 | 启动时向指定成员（`host:peerport`）报名。可选：不写时，非 `-peers` 首位的节点也会按配置成员轮转报名，`-join` 用于指定其中一个 |
| `-adopt-interval` | `PUSHUPES_ADOPT_INTERVAL` | 3s | 尚未成为集群成员的节点每隔多久重试一次报名 |
| `-bootstrap` | — | false | 由本节点写下集群的初始配置。默认由 `-peers` 里 id 最小的节点写；其余配置了 `-peers` 的节点以种子身份启动、向这些成员报名加入，自己不写配置 |
| `-replica-policy` | `PUSHUPES_REPLICA_POLICY` | medium | 副本策略档位：`low`=每槽 1 份、`medium`=2 份（默认）、`high`=每槽 3 份（容忍失去 2 个数据节点），或**正整数直接指定副本因子**。**只用于新集群的初始值**：策略存放在 Raft 复制的槽表里，之后只能经 `POST /admin/cluster/replica-policy` 修改，重启不会重刷；副本因子 = MIN(数据节点数（布局池 `HostingIDs()`）, 策略要求份数)。`GET /admin/cluster/status` 的 `replica_policy` / `replica_factor` 读回当前值；非法值启动即报错 |
| `-flush-messages` | `PUSHUPES_FLUSH_MESSAGES` | 1000 | 每 N 条 fsync（0 关闭） |
| `-flush-interval` | `PUSHUPES_FLUSH_INTERVAL` | 5s | 每周期 fsync（0 关闭） |
| `-segment-bytes` | `PUSHUPES_SEGMENT_BYTES` | 64MiB | WAL 段滚动大小：64MiB 整数倍，≤2GiB；可写字节数或带单位（`64MiB`/`1GiB`），env 同样接受带单位形式 |
| `-fetch-settle` | `PUSHUPES_FETCH_SETTLE` | 200µs | fetch 轮被数据唤醒后的合并窗口：一轮覆盖整批写入涉及的多个槽；每次 ack 都要付它一次，换的是轮次与上报次数；0=首个槽有数据就答（轮次更多） |
| `-drop-after` | `PUSHUPES_DROP_AFTER` | 30s | 迁移后前源副本保留期；0=默认，负数启动即报错，不可关闭 |
| `-rebalance-interval` | `PUSHUPES_REBALANCE_INTERVAL` | 2s | controller 每隔多久检查一次 leader 布局并执行回切（节点宕机恢复后把槽 leader 迁回环上预期节点）；**每轮只提交一次**（提交严格串行，任一时刻只有一个槽在交接栅栏上），所以收敛时间由这个间隔决定；0=关闭，负数启动即报错 |
| `-grpc-max-msg-size` | `PUSHUPES_GRPC_MAX_MSG_SIZE` | 4MiB | client 面 gRPC 消息上限（recv/send 同值）；`BatchAppend` 单批要装进它，可写 `16MiB` 等带单位形式（env 同样接受）；≤0 启动即报错 |
| `-batch-slot-parallelism` | `PUSHUPES_BATCH_SLOT_PARALLELISM` | 100 | `BatchAppend` 一批最多同时执行几个槽（异槽并行、同槽串行；铺满 1024 槽的宽批也不会瞬间压上等量并发 WAL 写者）；≤0 启动即报错 |
| `-raft-heartbeat-timeout` | `PUSHUPES_RAFT_HEARTBEAT_TIMEOUT` | 100ms | 共识心跳间隔。leader 的心跳没按时到达就会被投票换掉，所以宿主机可能拖住共识事件循环时要与 `-raft-election-timeout` 一起放大 |
| `-raft-election-timeout` | `PUSHUPES_RAFT_ELECTION_TIMEOUT` | 500ms | follower 多久收不到心跳就发起选举。必须 ≥ 2× 心跳，否则一次迟到的心跳就会掀掉 leader（启动即报错）；与心跳是一对比例，一起放大而不是只收窄其中一个 |
| `-raft-flush-interval` | `PUSHUPES_RAFT_FLUSH_INTERVAL` | 200µs | 共识 WAL 组提交窗口：窗口内到达的追加共用一次 fsync |
| `-raft-segment-bytes` | `PUSHUPES_RAFT_SEGMENT_BYTES` | 64MiB | 共识 WAL 段滚动大小；可写字节数或带单位（`64MiB`），env 同样接受 |

### 客户端接入

事件读写只走 **gRPC client 面**（proto 契约 `proto/pushupes/v1/events.proto`）：

- `EventService/BatchAppend`：事件写入的唯一入口，单次与批量写入都走它——
  一条 `AppendRequest` 就是单条写，多条就是批量。服务端逐条校验幂等与版本
  （见 DESIGN.md），按请求同序返回逐条结果（回显
  `aggregate_id`），槽不在本节点时以响应字段返回重定向：`err_id=1003/1004`
  （MOVED/ASK）+ `node`（槽 leader 的 client 地址），客户端据此重连。批内
  `aggregate_id` 必须各不相同，重复的那组整组 `fail/1002` 且不执行。服务端
  同槽串行、异槽并行（并发上限 `-batch-slot-parallelism`，默认 100）。单批
  必须装进 client 面 gRPC 消息上限（`-grpc-max-msg-size`，默认 4MiB）。
- `EventService/ReadStream`：按聚合读取事件流（≤HW 语义）。
- `EventService/ReadTails`：一次查询多个聚合的最新版本（resume 扫描用）。
  服务端逐条应答：本地可服务的槽给版本，其余给该条自己的 1003 + `node`；
  `pkg/client` 按本地路由表分组扇出、逐条修补重发。
- `EventService/ReadVersionByTime`：按聚合 + 时间取「时间点及之前」的最大
  版本锚点，再用 ReadStream 回放。
- `EventService/ReadByCommand`：按 `command_id` 查询已写入的记录。
  以上读接口与写一样走 MOVED 重定向（应答带 `err_id`/`slot`/`node`），
  非本地槽不再由服务端代理转发；`pkg/client` 对调用方透明地跟随。

错误 ID：`1001` 版本冲突（附 current_version）、`1002` 参数非法、
`1003` MOVED、`1004` ASK、`1005` 本节点非 controller（admin 写命令专用，
响应附 `controller` 与 `controller_admin_addr`）、`1006` 本节点流控拒绝
（见「节点级流控」；经 `pkg/client` 写入时该码被库吸收为回压，调用方看不到它）、
`1007` 本地副本来源未证实（owner 围栏拦下该槽：leader 拒写、本地读答空，直到
运维经 `/admin/slots/pending/adopt` 采纳、或删除槽目录，见 DESIGN §3.1。来源
证明以复制表为权威：表把你的节点排进某槽，该槽目录即由表盖章；只有整个数据
目录的准入戳指向别的世代（指错 `-data`、共识状态重建）且副本无记录时才会围栏，
adopt 一条命令即可同时采纳副本与准入目录）。世代号
是复制状态里的集群级值（`GET /admin/cluster/status` 的 `topology`，每个节点报同一个），
只有真正重建共识状态才会换世代；普通重启、raft 换主都不影响副本来源的证明。

admin 面 HTTP（仅管理）：`GET /admin/cluster/status`、`GET /admin/status`、
`GET /admin/slots/{slot}/describe`、`GET /admin/slots/{slot}/streams`、
`POST /admin/slots/{slot}/migrate`、`POST /admin/slots/{slot}/remove-replica`、
`POST /admin/cluster/plan`、`POST /admin/cluster/replica-policy`、
`DELETE /admin/cluster/nodes/{id}`（摘成员：服务端先排干再正式移除，在线与否不问，客户端无中断）、
`POST/GET/DELETE /admin/flow-control`（节点本地：收到请求的节点就是被设置/查询/
恢复的节点）、`GET /admin/slots/pending`、`POST /admin/slots/pending/adopt`
（owner 围栏：查看被拦副本并采纳「属于当前世代」，同样节点本地；丢弃走运维
手工删除槽目录，见 DESIGN §3.1）、`GET /healthz`、`/debug/pprof/`。
**控制器专属写命令（migrate/remove-replica/plan/replica-policy）必须直接发到 Raft leader 的
admin 地址**，follower 一律拒绝（425 + 1005），不做转发。

仓库自带工具：

```bash
# 冒烟探针（MOVED 跟随 + 幂等 + 版本冲突 + 回读断言）
go run ./cmd/grpccheck -addrs http://127.0.0.1:8591,http://127.0.0.1:8592,http://127.0.0.1:8593

# 写压测（-nodes 传 client 面 gRPC 地址，路由表经 PrefetchRoutes 解析并按
# -route-refresh 周期刷新，MOVED/ASK 即时修补本地行；槽数取自应答长度；
# -batch N 为每批条数，N=0 即一条 BatchAppend 只带一条记录；-duration 是硬
# 窗口：到点时在飞调用不再排干限流积压，未答记录单记 window-dropped，
# 不计入吞吐与 fail）
go run ./cmd/bench -nodes http://127.0.0.1:8591 -conns 8 -size 1024 -duration 30s
go run ./cmd/bench -nodes http://127.0.0.1:8591,http://127.0.0.1:8592,http://127.0.0.1:8593 -conns 8 -size 1024 -batch 64 -duration 30s

# BatchAppend 冒烟（经 pkg/client 客户端库：success/exists/1001/批内重复拒绝/回读断言）
go run ./cmd/batchsmoke -nodes http://127.0.0.1:8591,http://127.0.0.1:8592,http://127.0.0.1:8593

# 流控回压冒烟（经 pkg/client：给 -admin 节点设 N tokens/period，并发写断言
# 调用方零 1006 零失败——节点拒绝全部被库的回压吸收）
go run ./cmd/flowcheck -seed http://127.0.0.1:8591 -admin 127.0.0.1:8091 -tokens 2 -period 1s

# 单槽灌数据（容量/恢复调试）
go run ./cmd/seed -slot 7 -mib 512

# 副本一致性体检：逐槽比较 leader 与各副本的聚合摘要（读 /admin/slots/N/describe）。
# 报出「LEO 相同但目录偏短」的发散副本 —— 这类副本在环上不可见（leader 回切器只看
# 偏离环的槽），但读不到自己尾部，且会让该槽永远回切不回去。有发散时退出码 1。
go run ./cmd/slotcheck -admins http://127.0.0.1:8091,http://127.0.0.1:8092,http://127.0.0.1:8093
```

修改 proto 后重新生成：`./scripts/genproto.sh`（buf 工具链在 `proto/` 下随仓库
分发；events.proto 生成到 `pkg/grpcapi/`，peer.proto 生成到 `internal/grpcapi/`，勿手改）。

## 性能数据

### 硬件环境

- CPU：4 核 2.4GHz
- 内存：8 GiB
- 存储：NVMe SSD，顺序读写约 3 GB/s

以下均为**实测**（非估算）。测试环境：3 节点、RF=2、Docker 沙箱 **4 核 CPU 配额**，bench 客户端与服务端共享配额——绝对吞吐受容器配额封顶，不代表宿主机的上限；比较改动收益时以「每条消息 CPU 成本」与同负载 A/B 为准。以下写入数字都是「高水位确认」（success 时每个 ISR 副本均已落盘，见「高可用与复制」）口径下的测量。

#### 写入吞吐

组合矩阵（节点数 × 连接数 × 批大小）实测。参数：1 KiB 事件体、`aggs=1000`、每组 30 秒；每组冷启动独立集群（单节点 RF=1、3 节点 RF=2），测完销毁数据重测。批量行的延迟按**批往返**计（整批一个样本），批大小=1 即一条 `BatchAppend` 携带一条记录（一次 RPC 一条记录）。12 组均 fail=0、exists=0、redirects=0。

| 节点 | 连接数 | 批大小 | 吞吐量 (msg/s) | p50 | p99 |
|---|---|---|---|---|---|
| 1 | 1 | 1 | 4711 | 0.18 ms | 1.00 ms |
| 1 | 1 | 10 | 30795 | 0.27 ms | 1.02 ms |
| 1 | 1 | 100 | 70285 | 1.19 ms | 2.98 ms |
| 1 | 4 | 1 | 12288 | 0.28 ms | 1.14 ms |
| 1 | 4 | 10 | 60625 | 0.52 ms | 1.99 ms |
| 1 | 4 | 100 | 101635 | 2.80 ms | 9.20 ms |
| 3 | 1 | 1 | 636 | 1.62 ms | 4.41 ms |
| 3 | 1 | 10 | 2130 | 4.42 ms | 20.34 ms |
| 3 | 1 | 100 | 13730 | 6.78 ms | 12.56 ms |
| 3 | 4 | 1 | 3172 | 1.09 ms | 4.53 ms |
| 3 | 4 | 10 | 9197 | 4.08 ms | 9.05 ms |
| 3 | 4 | 100 | 27645 | 11.47 ms | 22.63 ms |

