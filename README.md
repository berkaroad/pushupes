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
  写入等高水位覆盖该条记录且 leader 本地 fsync 落定才回 success（单节点宕机或整簇掉电都不丢已确认写入）
```

三条链路各走各的面：共识与数据复制都走 peer 面，事件读写只走 client 面，
运维与观察（含管理控制台）走 admin 面（80xx）。

## 功能特性

### 高可用与复制

- 控制面用自研 Raft 只复制**元数据**（槽位分配表与集群成员）；事件数据由副本
  向 leader 拉取复制，两相分离。
- 写入经全部同步副本写入日志、且 leader 本地 fsync 落定后才确认：回
  `success` 的写入不会因单节点宕机或整簇掉电而丢失；
  副本全部掉线的极端情况下可用性优先（复制承诺只对同步副本成立，落盘承诺由 leader 自己的盘兜底）。
- 节点失联自动故障切换（该节点名下所有槽重选 leader），恢复后自动回切；读写
  都靠 MOVED/ASK 重定向到达槽 owner，服务端不中继流量，客户端库自动跟随。
- 恢复判定不等探活的连接退避：重启的节点自报注册即清掉离线标记（要求对端 peer 端口
  可达），实测 1-2 秒内回到在线集，随后槽主按再平衡节奏回切。
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
- 整簇重启（控制节点先起、数据节点随后起来）时槽主分布保持重启前的位置：控制节点初次
  启动的宽限期（`-startup-grace`，默认 90s）内，目录里已注册但还没启动的数据节点不被
  判为宕机，首次再平衡也等在这段窗口之外；这些节点全部应答后立即回到常规判定。

### 节点级流控（令牌桶）

- 每个节点独立的写入令牌桶：admin HTTP（`POST /admin/flow-control`）就地配置，
  只限该节点作为 leader 的写入，稳态上限 = `tokens / period`；桶空写入报
  `fail/1005`，副本复制流量不占预算。
- 运维应急面：不落盘、不进集群共识，重启即恢复不限流，`DELETE` 立即解除。
- 客户端库把 1005 吸收为回压自动退避重发，调用方无感；控制台节点卡片显示
  流控配置，触发限流时闪烁提醒，并可直接设置。

### 管理控制台（frontend/）

React 管理台：集群页（存储数量/大小、客户端写入速度、副本策略与同步状态、
按面分组的节点卡片与写入速率折线图、流控入口、处于缩容标记的节点卡片带
「取消缩容」）、槽位页（分配表、槽详情与迁移发起）、事件流页；light/dark
主题，2s 自动刷新可开关。

**加节点没有控制台或管理面入口**：新进程带 `-join` 启动即自报入群；控制台只提供
「取消缩容」（撤销误发的 DELETE），移除走 `DELETE /admin/cluster/nodes/{id}` /
`cluster.sh remove N`。完整形态见 DESIGN §12.3；以上各机制的设计推导同样以 DESIGN.md 为准。

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

### 一键起集群（用于本地开发）

```bash
REPLICAS=5 VOTERS=3 scripts/cluster.sh start   # 一条命令建整簇：3 控制节点 + 2 数据节点
scripts/cluster.sh restart   # 重启（保留数据，验证 WAL/Raft 崩溃恢复）；裸跑即可，
                             #   节点数、每节点角色、端口都读自 $RUN_DIR/cluster.env
scripts/cluster.sh status    # 各节点 Raft 角色、leader 槽数、迁移中槽数
scripts/cluster.sh smoke     # 端到端冒烟：MOVED → v1/v2 写入 → 幂等 exists
                             #           → 版本冲突 fail/1001 → 回读
scripts/cluster.sh slotcheck # 副本一致性体检：逐槽比较 leader 与各副本的聚合摘要，
                             #   报出「LEO 相同但目录偏短」的发散副本（有则退出码 1）
                             #   可加 -quiet / -json / -list=N 透传给 bin/slotcheck
scripts/cluster.sh logs 1    # 跟踪 node-1 日志
scripts/cluster.sh members   # 列出当前 Raft 成员（GET /admin/cluster/nodes）
scripts/cluster.sh join 6    # 运行时扩 node-6（-join 自报；角色按 VOTERS/ROLE_<n> 推导）
scripts/cluster.sh remove 6  # 运行时缩 node-6（只允许离线节点：进程在跑会先停，
                             #   等探活标离线后提交配置删除，槽主迁到存活副本）
scripts/cluster.sh stop      # 停止（只杀 pid 文件记录的进程）
scripts/cluster.sh clean     # stop 并删除运行目录（含数据，慎用）
```

每个节点占用三个端口，按节点序号依次递增（node-i = BASE + i − 1）；数据、日志、
pid 分别放在 `$RUN_DIR/node-i/`（默认在仓库根的 `.cluster/`）。可用环境变量覆盖：
`REPLICAS`（节点数）、`VOTERS`（控制面节点数：node-1..VOTERS=voter，其余=learner；
也可用 `ROLE_<n>=voter|learner` 逐个点名）、`HOST`、`ADMIN_BASE`、`PEER_BASE`、
`CLIENT_BASE`、`SEGMENT_BYTES`、`RAFT_HEARTBEAT_TIMEOUT`、`RAFT_ELECTION_TIMEOUT`、
`RUN_DIR`、`READY_TIMEOUT`、`BUILD`（两个 raft 时间旋钮是成对的：选举超时须 ≥ 2×
心跳，宿主机/容器 CPU 紧张时一起放大，而不是只收窄其中一个）。`start`/`join`
会把生效形态（节点数、每节点角色、端口）写进 `$RUN_DIR/cluster.env`，之后裸跑
`restart`/`stop`/`status`/`smoke` 都按它执行，命令行显式传的变量仍优先；运行目录里
只有节点目录而没有 `cluster.env`（老目录）时脚本按最大 `node-N` 推断节点数并提示补传
角色。每槽副本数由副本策略分档（`PUSHUPES_REPLICA_POLICY`=low/medium/high，默认
medium：1/2/3 份，或正整数直接指定副本因子；副本因子 = MIN(数据节点数, 策略要求份数)）：该值只为新集群种初始值，策略存放在 Raft 复制的
槽表里，之后改档位走 `POST /admin/cluster/replica-policy`（控制台集群页的「副本策略」
卡片即可操作）。**扩容分两种，别混**：`REPLICAS` 只决定节点总数，角色由 `VOTERS`
区间推导（`node-1..VOTERS` = voter，其余 = learner）——`REPLICAS=7 cluster.sh start`
在 `VOTERS=3` 下新增的 node-4..7 **全是数据节点（learner）**；扩**控制节点**必须同时
抬 `VOTERS`（如 `VOTERS=5 REPLICAS=7 start`）或用 `ROLE_<n>=voter` 点名。两个方向
都等价于逐个 `cluster.sh join N`（角色同样按 `VOTERS`/`ROLE_<n>` 推导）。方向不可逆：
成员身份以 raft 配置为准，调低 `VOTERS` 只是让脚本把既有 voter 按 learner 拉起——
它的 `-join` 自报会因「已是 raft voter」被拒、进程启动即退（成员身份不变，集群不受
影响）。缩控制面只能走 `cluster.sh remove N`。

集群配置只由**一个**节点写下：带 `-peers` 且 id 最小的那个（或用 `-bootstrap` 点名的
那个）——`-peers` 只是**撰写者的初始配置文档**，必须点名撰写者自己。其余所有节点
一视同仁（新增 voter 与 learner 同一条路）：带 `-join`（控制面地址列表，按
`-adopt-interval` 轮转自报）加入，voter 进 raft 配置、learner 进复制目录，全部由
节点在启动时自己声明。`-join` 与 `-peers` 互斥（同时设置启动即报错），被明确拒绝
（id 已属于另一种角色、地址已属于别的成员）也启动即退出。这样新节点各写一份配置、
把集群劈成多个各自能提交的 raft 组的情况不会发生。

### 手动启动节点

```bash
# 入群声明二选一（互斥）：初始配置撰写者用 -peers（文档必须点名自己），
# 其余所有节点——新增 voter 与 learner 一样——用 -join 指向控制面成员自报。
# 多个 -join 地址按 -adopt-interval 轮转（个别成员重启不耽误加入）。
# -role 必填：voter 投票但不承载槽副本，learner 承载副本但不投票。
# 最小部署 = 1 voter + 1 learner；下面是本地开发的完整形态——3 控制 + 2 数据。

# --- 撰写者 node-1：写下 3 个控制节点的初始配置 ---
./bin/pushupes -node node-1 -role voter \
  -peer 127.0.0.1:8391 -admin 127.0.0.1:8091 -client 127.0.0.1:8591 \
  -data ./node-1 \
  -peers 'node-1=127.0.0.1:8391,node-2=127.0.0.1:8392,node-3=127.0.0.1:8393'

# --- 其余控制节点：-join 自报，配置由 leader 的日志带过来 ---
./bin/pushupes -node node-2 -role voter \
  -peer 127.0.0.1:8392 -admin 127.0.0.1:8092 -client 127.0.0.1:8592 \
  -data ./node-2 \
  -join '127.0.0.1:8391,127.0.0.1:8393'

./bin/pushupes -node node-3 -role voter \
  -peer 127.0.0.1:8393 -admin 127.0.0.1:8093 -client 127.0.0.1:8593 \
  -data ./node-3 \
  -join '127.0.0.1:8391,127.0.0.1:8392'

# --- 追加数据节点：同样只有 -join，没有 admin 准入这一步 ---
./bin/pushupes -node node-4 -role learner \
  -peer 127.0.0.1:8394 -admin 127.0.0.1:8094 -client 127.0.0.1:8594 \
  -data ./node-4 \
  -join '127.0.0.1:8391,127.0.0.1:8392,127.0.0.1:8393'

./bin/pushupes -node node-5 -role learner \
  -peer 127.0.0.1:8395 -admin 127.0.0.1:8095 -client 127.0.0.1:8595 \
  -data ./node-5 \
  -join '127.0.0.1:8391,127.0.0.1:8392,127.0.0.1:8393'
```

地址规范：地址统一带 scheme 存储，未写协议时默认补 `http://`；`-peers` 与 `-join`
都接受裸 `host:peerport`（此时地址兼作节点 id）。

### 启动参数

| flag | env | 默认 | 说明 |
|---|---|---|---|
| `-node` | `PUSHUPES_NODE` | `node-1` | 节点 id |
| `-role` | `PUSHUPES_ROLE` | 无（必填） | 节点角色，只能是两者之一：`voter`=控制面（参与选举、**不承载槽副本**）、`learner`=数据面（不投票，从控制面接收布局分发）。最小部署 = 1 个 `voter` + 1 个 `learner`；缺失或非法值启动即报错。角色由节点自报进复制目录，布局池只认 `learner` 成员。`learner` 的槽表由控制面经 `WatchLayout` 流分发（DESIGN §12.4），它不建共识组、不提交、也不报名成为投票者 |
| `-peer` | `PUSHUPES_PEER` | `http://127.0.0.1:8391` | peer 面：Raft + PeerService gRPC（全部节点间流量，运维只需配这个端口） |
| `-admin` | `PUSHUPES_ADMIN` | `http://127.0.0.1:8091` | admin 面：HTTP 管理 API + pprof |
| `-client` | `PUSHUPES_CLIENT` | `http://127.0.0.1:8591` | client 面：gRPC 事件读写唯一入口 |
| `-data` | `PUSHUPES_DATA` | `./data` | 数据目录 |
| `-peers` | `PUSHUPES_PEERS` | 空 | **初始配置撰写者的文档**：`id=host:peerport,...`（裸 `host:peerport` 亦可），必须包含本节点；只有 id 最小的成员（或 `-bootstrap` 点名者）能带它。与 `-join` 互斥，同时设置启动即报错 |
| `-join` | `PUSHUPES_JOIN` | 空 | **加入现有集群的唯一入口**：控制面成员的 peer 地址列表（`host:peerport` 或 `id=host:peerport`，逗号分隔），按 `-adopt-interval` 轮转自报直到被 leader 接纳——voter 进 raft 配置、learner 进复制目录。被明确拒绝（`ErrBadMember`）启动即退出；与 `-peers` 互斥 |
| `-adopt-interval` | `PUSHUPES_ADOPT_INTERVAL` | 3s | 尚未成为集群成员的节点每隔多久向 `-join` 目标重试一次自报（多地址轮转，个别成员不可达不影响加入） |
| `-bootstrap` | — | false | 由本节点撰写集群的初始配置：配合 `-peers` 用于点名非最小 id 的作者；单独使用（无 `-peers`）时撰写只有自己的单 voter 配置，后续 voter 用 `-join` 加入。learner 不能撰写，与 `-join` 同用启动即报错 |
| `-replica-policy` | `PUSHUPES_REPLICA_POLICY` | medium | 副本策略档位：`low`=每槽 1 份、`medium`=2 份（默认）、`high`=每槽 3 份（容忍失去 2 个数据节点），或**正整数直接指定副本因子**。**只用于新集群的初始值**：策略存放在 Raft 复制的槽表里，之后只能经 `POST /admin/cluster/replica-policy` 修改，重启不会重刷；副本因子 = MIN(数据节点数（布局池 `HostingIDs()`）, 策略要求份数)。`GET /admin/cluster/status` 的 `replica_policy` / `replica_factor` 读回当前值；非法值启动即报错 |
| `-flush-messages` | `PUSHUPES_FLUSH_MESSAGES` | 1000 | 每 N 条 fsync（0 关闭）——这是无人等待时的后台节奏；写入确认不等窗口：停靠的确认即时触发该槽的组提交轮，合批 = 一轮 fsync 期间积累的全部追加 |
| `-flush-interval` | `PUSHUPES_FLUSH_INTERVAL` | 100ms | 每周期 fsync（0 关闭）——同上，只定后台节奏；确认路径的本地落盘腿付的是 fsync 时长与队列深度，复制高水位腿与它并行，ack ≈ 两腿取最大 |
| `-flush-parallelism` | `PUSHUPES_FLUSH_PARALLELISM` | 1 | 全存储同时在飞的段 fsync 上限（1=一次刷一个，即历史单刷盘链路）：一个许可信号量给所有 fsync 封顶，刷盘队列由持久 worker 池执行；显式 `Flush()`/`Close` 的 fsync 也在调用方 goroutine 上取同一许可、同受上限约束。刷盘轮按预算切片（每轮至多 8 个单元或 200ms）。<1 启动即报错；生效值可从 `/admin/stats` 的 `parallelism` 字段读回（机制见 DESIGN §3） |
| `-segment-bytes` | `PUSHUPES_SEGMENT_BYTES` | 64MiB | WAL 段滚动大小：2 的次幂（64MiB/128MiB/256MiB…，192MiB 不接受），64MiB～2GiB；可写字节数或带单位（`64MiB`/`1GiB`），env 同样接受带单位形式 |
| `-fetch-settle` | `PUSHUPES_FETCH_SETTLE` | 200µs | fetch 轮被数据唤醒后的合并窗口：一轮覆盖整批写入涉及的多个槽；每次 ack 都要付它一次，换的是轮次与上报次数；0=首个槽有数据就答（轮次更多） |
| `-drop-after` | `PUSHUPES_DROP_AFTER` | 30s | 迁移后前源副本保留期；0=默认，负数启动即报错，不可关闭 |
| `-rebalance-interval` | `PUSHUPES_REBALANCE_INTERVAL` | 2s | controller 每隔多久检查一次 leader 布局并执行回切（节点宕机恢复后把槽 leader 迁回环上预期节点）；**每轮只提交一次**（提交严格串行，任一时刻只有一个槽在交接栅栏上），所以收敛时间由这个间隔决定；0=关闭，负数启动即报错 |
| `-startup-grace` | `PUSHUPES_STARTUP_GRACE` | 90s | 控制节点初次启动的宽限期：目录里已注册、本进程启动以来还没应答过探活的数据节点在窗口内不被判为宕机（也就不会触发故障切换把它们的槽挪走），首次再平衡也等在这段窗口之外；这些节点全部应答即提前结束，到点仍无应答的按常规判定（故障切换最多推迟这一段）；0=不等，负数启动即报错 |
| `-grpc-max-msg-size` | `PUSHUPES_GRPC_MAX_MSG_SIZE` | 4MiB | client 面 gRPC 消息上限（recv/send 同值）；`BatchAppend` 单批要装进它，可写 `16MiB` 等带单位形式（env 同样接受）；≤0 启动即报错 |
| `-raft-heartbeat-timeout` | `PUSHUPES_RAFT_HEARTBEAT_TIMEOUT` | 100ms | 共识心跳间隔。leader 的心跳没按时到达就会被投票换掉，所以宿主机可能拖住共识事件循环时要与 `-raft-election-timeout` 一起放大 |
| `-raft-election-timeout` | `PUSHUPES_RAFT_ELECTION_TIMEOUT` | 500ms | follower 多久收不到心跳就发起选举。必须 ≥ 2× 心跳，否则一次迟到的心跳就会掀掉 leader（启动即报错）；与心跳是一对比例，一起放大而不是只收窄其中一个 |
| `-raft-flush-interval` | `PUSHUPES_RAFT_FLUSH_INTERVAL` | 100ms | 共识 WAL 组提交窗口：窗口内到达的追加共用一次 fsync；等待确认的应答会把所属轮提前关窗（确认延迟 = fsync 轮，不等窗口） |
| `-raft-segment-bytes` | `PUSHUPES_RAFT_SEGMENT_BYTES` | 64MiB | 共识 WAL 段滚动大小；可写字节数或带单位（`64MiB`），env 同样接受 |

### 客户端接入

事件读写只走 **gRPC client 面**（proto 契约 `proto/pushupes/v1/events.proto`）；admin 面只做集群运维，不承载数据读写。Go 调用方直接用 `pkg/client`：

```go
cl, err := client.New(
	[]string{"http://127.0.0.1:8591", "http://127.0.0.1:8592", "http://127.0.0.1:8593"},
	&client.Config{RouteRefresh: 30 * time.Second}, // 槽→leader 路由表刷新周期；0 = 只在启动取一次
)
defer cl.Close()

resps, err := cl.BatchAppend(ctx, reqs)                     // 写：一条 AppendRequest 即单条写
tails, err := cl.ReadTails(ctx, []string{"agg-1", "agg-2"}) // 读：一次取多个聚合的最新版本
resp, err := cl.ReadStream(ctx, "agg-1", fromVersion, 100)  // 读：按版本回放事件流
```

库替调用方做了三件事：启动时调 `EventService/PrefetchRoutes` 取槽→leader 路由表并按 `RouteRefresh` 周期整体刷新、MOVED/ASK 就地修补本地行、每个节点一条复用长连接（写与读都按路由分组扇出，重定向对调用方透明）。另外三个旋钮：`Attempts`（一次调用的最大发送轮数，默认 3）、`CallTimeout`（单次 RPC 超时，默认 30s）、`FlowBackoff`（收到 1005 后的回压停顿，默认 100ms）。

**写：`EventService/BatchAppend`** —— 事件写入的唯一入口，单条写与批量写都是它：一条 `AppendRequest` 即单条，多条即批量。服务端逐条校验幂等与版本（见 DESIGN.md），按请求同序返回逐条结果（回显 `aggregate_id`）；槽不在本节点时不代理转发，而是用响应字段回答重定向（`err_id=1003/1004` + `node` 给出槽 leader 的 client 地址），由库重连重发。批内 `aggregate_id` 必须各不相同，重复的那组整组按 1002 拒绝且不执行。同槽串行、异槽并行，并发上限为进程级 worker 池 `16 × runtime.GOMAXPROCS(0)`。单批必须装进 client 面 gRPC 消息上限（`-grpc-max-msg-size`，默认 4MiB）。

**读**——四个接口，和写一样走 MOVED 重定向（应答带 `err_id`/`slot`/`node`），`pkg/client` 对调用方透明跟随：

- `ReadStream`：按聚合回放事件流（≤HW 语义）。
- `ReadTails`：一次查询多个聚合的最新版本（resume 扫描用）；本地不可服务的条目各自带 1003 + `node`，库按路由分组扇出、逐条修补重发。
- `ReadVersionByTime`：取「该时间点及之前」的最大版本锚点，再用 `ReadStream` 回放。
- `ReadByCommand`：按 `command_id` 查已写入的记录（幂等排查用）。

**调用方需要处理的错误 ID**（其余都被库消化掉了）：

- `1001` 版本冲突（附 `current_version`）：按应答给出的版本重发即可。
- `1002` 参数非法：批内 `aggregate_id` 重复、字段越界等，整组不执行。
- `1003` MOVED / `1004` ASK：`pkg/client` 自动跟随；用库时看不到这两个码。
- `1005` 节点级流控拒绝（见「节点级流控」）：用库时被吸收为回压，调用方看不到；直连 gRPC 才需要自己退避。
- `1006` 本地副本来源未证实：owner 围栏拦下该槽——leader 拒写、本地读答空。数据没丢，但 client 面没有可自愈的动作，要运维确认该副本属于当前世代后采纳、或删掉该槽目录才解除（命令在 admin 面，见 DESIGN §3.1）。
- `1007` 确认等待超时：记录**已落 leader WAL**，只是同步副本高水位或 leader 本地 fsync 没等到。带同一 `command_id` 重试会命中 exists——重试即确认，不是重写。
- `1008` 本节点是该槽 leader 但暂不可服务（无本地副本，或副本是空壳而别处有历史）：**什么都没写**，稍后重试即可。
- `1009` 纯控制面节点：集群还没有数据面成员持有该槽（布局未规划）。什么都没写，重试无用，需要先规划布局（admin 面的运维动作）。

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
# 调用方零 1005 零失败——节点拒绝全部被库的回压吸收）
go run ./cmd/flowcheck -seed http://127.0.0.1:8591 -admin 127.0.0.1:8091 -tokens 2 -period 1s

# 单槽灌数据（容量/恢复调试）
go run ./cmd/seed -slot 7 -mib 512

# 副本一致性体检：逐槽比较 leader 与各副本的聚合摘要（经各节点 admin 面读槽位描述）。
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

#### 写入吞吐

组合矩阵（数据节点数 × 连接数 × 批大小）实测。参数：3 个固定控制面节点（voter，只投票不承载槽副本），数据节点（learner）{1,2}，副本因子 2（每槽实落副本 = min(数据节点数, 2)，即数据节点=1 的格实为 1 副本）；1 KiB 事件体、`aggs=10000`、每组 1 分钟；每格冷启动独立集群，测完销毁数据重测。批量行的延迟按**批往返**计（整批一个样本），批大小=1 即一条 `BatchAppend` 携带一条记录（一次 RPC 一条记录）。12 组均 fail=0、exists=0、redirects=0。

| 数据节点 | 连接数 | 批大小 | 吞吐量 (msg/s) | p50 | p90 | p99 |
|---|---|---|---|---|---|---|
| 1 | 1 | 1 | 3208 | 0.20 ms | 0.27 ms | 2.64 ms |
| 1 | 1 | 10 | 29299 | 0.28 ms | 0.36 ms | 1.31 ms |
| 1 | 1 | 100 | 73744 | 1.04 ms | 1.64 ms | 3.58 ms |
| 1 | 4 | 1 | 11395 | 0.28 ms | 0.44 ms | 2.55 ms |
| 1 | 4 | 10 | 71467 | 0.47 ms | 0.74 ms | 1.86 ms |
| 1 | 4 | 100 | 109402 | 2.70 ms | 5.28 ms | 16.13 ms |
| 2 | 1 | 1 | 320 | 3.31 ms | 5.43 ms | 7.05 ms |
| 2 | 1 | 10 | 4155 | 2.01 ms | 2.57 ms | 15.39 ms |
| 2 | 1 | 100 | 16362 | 4.73 ms | 8.74 ms | 25.77 ms |
| 2 | 4 | 1 | 2380 | 1.20 ms | 2.30 ms | 10.29 ms |
| 2 | 4 | 10 | 9620 | 3.26 ms | 4.93 ms | 15.10 ms |
| 2 | 4 | 100 | 23723 | 15.04 ms | 23.41 ms | 40.27 ms |
