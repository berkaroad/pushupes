# PushupES

PushupES是面向 **CQRS** 框架的**事件存储服务** ，具有高可用、高性能、支持水平扩展的特点。部署上采用纯go的单二进制文件（不依赖glibc）、无外部依赖（不需要 ZooKeeper/etcd）。

提供基本的管理控制台，采用 **http api** + **Web UI**；客户端支持批量写入、按版本范围（或按截至时间）获取事件流，采用 **gRPC** ，便于非go语言的客户端接入。

分层原则：**Raft 只复制元数据**（槽位分配表、成员、节点地址），事件数据走 **leader→follower 专用拉取协议**（ISR + 高水位语义），两条链路互不影响。

```
客户端 ──gRPC(EventService)──▶ 槽 leader ──append-only WAL──▶ 本地落盘
                                  │
                     follower 长轮询拉取（DataPeerService.MFetch，与 Raft 同端口）
                                  │
Raft（自研简化 Raft + 自研分段 WAL，仅元数据）──▶ 槽位分配表 / epoch / 集群成员
```

## 集群拓扑

以 **3 个控制节点 + 2 个数据节点** 为例（medium 副本策略：每槽 2 份，两个数据节点互为对方的副本）。

默认槽组 `g3` （对应 **物理槽 8–15** ），（代 g 的段是 `[2^g, 2^(g+1))`，基址 = 大小 = 8），客户端按**逻辑槽 0–7** 寻址（物理 = 基址 + 逻辑）。

```
客户端（CQRS 框架）
  │ gRPC EventService（client 面 85xx）：读写都直连槽 leader
  │ 路由表按逻辑槽 0–7 索引；收 MOVED(1003)/ASK(1004) → 刷新路由缓存后重试
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
  data-1：leader 物理槽 8,10,12,14（逻辑 0,2,4,6）┃ 副本 9,11,13,15（逻辑 1,3,5,7）
  data-2：leader 物理槽 9,11,13,15（逻辑 1,3,5,7）┃ 副本 8,10,12,14（逻辑 0,2,4,6）
  data-1 ◀──MFetch 长轮询拉取（peer 面 83xx，与 Raft 同端口族）──▶ data-2
  follower 按 leader 分组批量拉增量，应答是裸 WAL 字节、原样落盘，
  批量回报 LEO，leader 推进 HW = ISR 内最小 LEO；
  写入等高水位覆盖该条记录且 leader 本地 fsync 落定才回 success（单节点宕机或整簇掉电都不丢已确认写入）
```

三条链路各走各的面：共识与数据复制都走 peer 面，事件读写只走 client 面，
运维与观察（含管理控制台）走 admin 面（80xx）。

## 功能特性

### 高可用与复制

- 控制面用自研 Raft 只复制**元数据**（槽位分配表与集群成员）；事件数据由副本向 leader 拉取复制，两相分离。
- 写入经全部同步副本写入日志、且 leader 本地 fsync 落定后才确认：回 `success` 的写入不会因单节点宕机或整簇掉电而丢失；
  副本全部掉线的极端情况下可用性优先（复制承诺只对同步副本成立，落盘承诺由 leader 自己的盘兜底）。
- 节点失联自动故障切换（该节点名下所有槽重选 leader），恢复后自动回切；读写都靠 MOVED/ASK 重定向到达槽 owner，服务端不中继流量，客户端库自动跟随。
- 恢复判定不等探活的连接退避：重启的节点自报注册即清掉离线标记（要求对端 peer 端口可达），实测 1-2 秒内回到在线集，随后槽主按再平衡节奏回切。
- 读只返回已同步高水位以内的记录，不会读到被回滚的脏数据。

### 槽位迁移（不停写）

- 副本因子增减、节点加入/重加入、leader 回切、副本修复，都走同一条「入场 → 追平 → 提交」链路，全程不停写、不丢已确认写入。
- 迁移到副本集以外的目标一次调用自动完成；迁走后原节点的本地副本按宽限期（`-drop-after`）自动清理。
- 客户端无感：路由缓存随重定向自动刷新，幂等规则保证转发期间的写入不重复。

### 再平衡（Rebalance）

- controller 周期（`-rebalance-interval`，默认 2s，可关闭）把各槽 leader 交接回环上的归属位置，槽主归属在数据节点间均摊；故障切换留下的失衡自动回切。
- 交接逐个进行，写扰动恒定限于单个槽，节点重启后无切换风暴；再平衡全程不停写。
- 整簇重启（控制节点先起、数据节点随后起来）时槽主分布保持重启前的位置：控制节点初次启动的宽限期（`-startup-grace`，默认 90s）内，目录里已注册但还没启动的数据节点不被判为宕机，首次再平衡也等在这段窗口之外；这些节点全部应答后立即回到常规判定。

### 节点级流控（令牌桶）

- 每个节点独立的写入令牌桶：admin HTTP（`POST /admin/flow-control`）就地配置，只限该节点作为 leader 的写入，稳态上限 = `tokens / period`；桶空写入报 `fail/1005`，副本复制流量不占预算。
- 运维应急面：不落盘、不进集群共识，重启即恢复不限流，`DELETE` 立即解除。
- 客户端库把 1005 吸收为回压自动退避重发，调用方无感；控制台节点卡片显示流控配置，触发限流时闪烁提醒，并可直接设置。

### 管理控制台（frontend/）

- 集群页

  存储数量/大小、客户端写入速度、副本策略与同步状态、按面分组的节点卡片与写入速率折线图、流控入口、处于缩容标记的节点卡片带「取消缩容」）。

- 槽位页
    
    分配表、槽详情与迁移发起、事件流。

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
| `-peer` | `PUSHUPES_PEER` | `http://127.0.0.1:8391` | peer 面：Raft + 节点间 gRPC（`ControllerPeerService` 成员/布局/探活、`DataPeerService` 槽数据与复制），全部节点间流量都走这个端口 |
| `-admin` | `PUSHUPES_ADMIN` | `http://127.0.0.1:8091` | admin 面：HTTP 管理 API + pprof |
| `-client` | `PUSHUPES_CLIENT` | `http://127.0.0.1:8591` | client 面：gRPC 事件读写唯一入口 |
| `-data` | `PUSHUPES_DATA` | `./data` | 数据目录 |
| `-peers` | `PUSHUPES_PEERS` | 空 | **初始配置撰写者的文档**：`id=host:peerport,...`（裸 `host:peerport` 亦可），必须包含本节点；只有 id 最小的成员（或 `-bootstrap` 点名者）能带它。与 `-join` 互斥，同时设置启动即报错 |
| `-join` | `PUSHUPES_JOIN` | 空 | **加入现有集群的唯一入口**：控制面成员的 peer 地址列表（`host:peerport` 或 `id=host:peerport`，逗号分隔），按 `-adopt-interval` 轮转自报直到被 leader 接纳——voter 进 raft 配置、learner 进复制目录。被明确拒绝（`ErrBadMember`）启动即退出；与 `-peers` 互斥 |
| `-adopt-interval` | `PUSHUPES_ADOPT_INTERVAL` | 3s | 尚未成为集群成员的节点每隔多久向 `-join` 目标重试一次自报（多地址轮转，个别成员不可达不影响加入） |
| `-bootstrap` | — | false | 由本节点撰写集群的初始配置：配合 `-peers` 用于点名非最小 id 的作者；单独使用（无 `-peers`）时撰写只有自己的单 voter 配置，后续 voter 用 `-join` 加入。learner 不能撰写，与 `-join` 同用启动即报错 |
| `-replica-policy` | `PUSHUPES_REPLICA_POLICY` | medium | 副本策略档位：`low`=每槽 1 份、`medium`=2 份（默认）、`high`=每槽 3 份（容忍失去 2 个数据节点），或**正整数直接指定副本因子**。**只用于新集群的初始值**：策略存放在 Raft 复制的槽表里，之后只能经 `POST /admin/cluster/replica-policy` 修改，重启不会重刷；副本因子 = MIN(数据节点数（布局池 `HostingIDs()`）, 策略要求份数)。`GET /admin/cluster/status` 的 `replica_policy` / `replica_factor` 读回当前值；非法值启动即报错 |
| `-slot-group` | `PUSHUPES_SLOT_GROUP` | g3 | 新集群服务的槽位代：`g0`..`g10`（g0 = 1 个槽、g3 = 8 个、g10 = 1024 个；代 g 的物理槽段是 `[2^g, 2^(g+1))`，各代段互不重叠，g0..g2 仅用于开发与测试）。**只用于新集群的第一次布局**——代存在 Raft 复制的槽表里，节点启动参数不会改写已有集群；客户端按代内的逻辑槽号路由（物理 = 基址 + 逻辑），非法值启动即报错 |
| `-flush-messages` | `PUSHUPES_FLUSH_MESSAGES` | 1000 | 每 N 条 fsync（0 关闭）——这是无人等待时的后台节奏；写入确认不等窗口：停靠的确认即时触发该槽的组提交轮，合批 = 一轮 fsync 期间积累的全部追加 |
| `-flush-interval` | `PUSHUPES_FLUSH_INTERVAL` | 100ms | 每周期 fsync（0 关闭）——同上，只定后台节奏；确认路径的本地落盘腿付的是 fsync 时长与队列深度，复制高水位腿与它并行，ack ≈ 两腿取最大 |
| `-flush-parallelism` | `PUSHUPES_FLUSH_PARALLELISM` | 1 | 全存储同时在飞的段 fsync 上限（1=一次刷一个，即历史单刷盘链路）：一个许可信号量给所有 fsync 封顶，刷盘队列由持久 worker 池执行；显式 `Flush()`/`Close` 的 fsync 也在调用方 goroutine 上取同一许可、同受上限约束。刷盘轮按预算切片（每轮至多 8 个单元或 200ms）。<1 启动即报错；生效值可从 `/admin/stats` 的 `parallelism` 字段读回（机制见 DESIGN §3） |
| `-segment-bytes` | `PUSHUPES_SEGMENT_BYTES` | 256MiB | WAL 段滚动大小：2 的次幂（256MiB/512MiB/1GiB/2GiB，192MiB 不接受），最小 256MiB（4KiB 记录下的容量数学，见 DESIGN §7.2）、最大 2GiB；可写字节数或带单位（`256MiB`/`1GiB`），env 同样接受带单位形式 |
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

**记录压缩编解码（writer-side gzip）** —— 压缩与解压都发生在 `pkg/client`，服务端**从不解压、也从不压缩**：它只把请求声明的 codec（`AppendRequest.compression`，枚举值即帧头 flags 的 codec id：0 不压、1 gzip）盖进记录头的 flags 字节并**原样落盘**——复制、封段、迁移、fetch 追平搬运的都是这份存储字节，因此压缩缩的是追平带宽而不是换来服务端 CPU。`COMPRESSION_UNSPECIFIED`（字段缺席）= 按给定字节存储；已压记录必须显式带 `COMPRESSION_GZIP`，服务端按 gzip magic（`1f 8b`）浅验每段 body，不做全流校验（那是读者的库负责）。库侧旋钮：`Config.CompressMinTotalBody`（记录内 body 合计达到该字节数才压，0=功能关闭，默认关）、`Config.CompressLevel`（gzip 等级，默认 `BestSpeed`=1 级：写路径的高压下用比率换每-append CPU）、`Config.AutoDecompress`（nil=开：读响应与 exists 记录的压缩记录在返回调用方前透明解压，解压超 1MiB 上限拒绝，gzip CRC 顺带验证）。压制比不达 90% 收益（图片/protobuf 等已紧凑 payload）自动放弃、存明文；调用方预置的 codec 一律权威，库不二次压缩。`cmd/bench -compress <floor>` 可实测该档位。

**读**——四个接口，和写一样走 MOVED 重定向（应答带 `err_id`/`slot`/`node`），`pkg/client` 对调用方透明跟随：

- `ReadStream`：按聚合回放事件流（≤HW 语义）。
- `ReadTails`：一次查询多个聚合的最新版本（resume 扫描用）；本地不可服务的条目各自带 1003 + `node`，库按路由分组扇出、逐条修补重发。
- `ReadVersionByTime`：取「该时间点及之前」的最大版本锚点，再用 `ReadStream` 回放。
- `ReadByCommand`：按 `command_id` 查已写入的记录（幂等排查用）。

**错误 ID 表**（定义在 `pkg/client/errs.go`，服务端 `internal/data` 只做同号别名；编号从 1001 起连续、无保留号）：

| 错误 ID | 名称 | 含义 | 调用方动作 |
|---|---|---|---|
| 1001 | ERR_VERSION_CONFLICT | 版本非递增 1（应答附 `current_version`） | 按应答给出的版本重发 |
| 1002 | ERR_BAD_REQUEST | 参数非法：批内 `aggregate_id` 重复、字段越界等 | 整组不执行，修请求后重发 |
| 1003 | ERR_SLOT_NOT_LOCAL | 槽不在本节点（MOVED，应答带槽 leader 的 client 地址） | `pkg/client` 自动跟随重发；直连 gRPC 时按 `node` 重发 |
| 1004 | ERR_MIGRATING | 槽迁移中（ASK，应答带目标节点） | `pkg/client` 自动跟随重发；直连时按 `node` 重发 |
| 1005 | ERR_FLOW_CONTROL | 本节点流控拒绝，未写入 | 用库时被吸收为回压；直连时自行退避重试 |
| 1006 | ERR_SLOT_UNPROVEN | 本地副本来源未证实（owner 围栏）：leader 拒写、本地读答空 | client 面没有自愈动作；运维确认该副本属于当前世代后采纳，或删掉该槽目录 |
| 1007 | ERR_ACK_TIMEOUT | 确认等待超时，记录**已落 leader WAL**（同步副本高水位或 leader 本地 fsync 未在期限内覆盖） | 带同一 `command_id` 重试即命中 exists——重试即确认，不是重写 |
| 1008 | ERR_REPLICA_CATCHING_UP | 本节点是该槽 leader 但暂无可用本地副本（无副本 / 空壳副本），未写入 | 稍后重试 |
| 1009 | ERR_NO_DATA_PLANE | 纯控制面成员（集群尚无数据面成员持有该槽），未写入 | 重试无用；先规划布局（admin 面运维动作） |
| 1010 | ERR_GROUP_MISMATCH | 请求的 `layout_epoch` 与当前布局不符，或集群尚未服务任何槽位代，整批未执行 | 重新拉取 `PrefetchRoutes`（拿到新的 `layout_epoch` 与代）后重试 |

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
- 存储：NVMe SSD（无 DRAM 的 SLC 缓存盘），顺序读写约 3 GB/s
  - fsync（4 KiB 写入 + `fsync`，连续 30 次）：干净窗口 p50 1.5~1.8 ms、p90 1.9~2.4 ms；
    退化时 p50 6.0~6.9 ms、p90 6.6~7.2 ms

#### 写入吞吐

组合矩阵（数据节点数 × 连接数 × 批大小）实测。参数：3 个固定控制面节点（voter，只投票不承载槽副本），数据节点（learner）{1,2}，副本因子 2（每槽实落副本 = min(数据节点数, 2)，即数据节点=1 的格实为 1 副本）；**槽组随数据节点数固定（每个数据节点恰好一个槽）：d 个数据节点 ⇒ g(d-1)，即数据节点=1 是 g0（1 个槽）、数据节点=2 是 g1（2 个槽）**，聚合全部落在这一段内，数据节点=2 时聚合分布在 2 个槽、批次按 leader 分组分别发送；1 KiB 事件体、聚合空间 = 连接数 × max(批大小, 1)（批大小=1 时即连接数，单聚合单流）、每组 1 分钟；确认路径含 ISR 全部写入 + leader 本地落盘，吞吐受 leader fsync 约束；每格冷启动独立集群，测完销毁数据重测。批量行的延迟按**批往返**计（整批一个样本），批大小=1 即一条 `BatchAppend` 携带一条记录（一次 RPC 一条记录）。12 组均 fail=0、exists=0、redirects=0；数据节点=2 的后 5 格在宿主 I/O 干净窗口内逐格复测（每格起跑前 fsync p50 ≤ 2 ms），其余格取自同一轮（起跑前同口径探测通过）。

| 数据节点 | 连接数 | 批大小 | 吞吐量 (msg/s) | p50 | p90 | p99 |
|---|---|---|---|---|---|---|
| 1 | 1 | 1 | 612 | 1.58 ms | 1.79 ms | 2.51 ms |
| 1 | 1 | 10 | 6595 | 1.46 ms | 1.66 ms | 2.19 ms |
| 1 | 1 | 100 | 35456 | 2.58 ms | 2.96 ms | 4.37 ms |
| 1 | 4 | 1 | 1389 | 2.78 ms | 3.11 ms | 5.01 ms |
| 1 | 4 | 10 | 16591 | 2.29 ms | 2.60 ms | 4.21 ms |
| 1 | 4 | 100 | 73891 | 3.94 ms | 5.21 ms | 14.03 ms |
| 2 | 1 | 1 | 579 | 1.66 ms | 1.86 ms | 2.73 ms |
| 2 | 1 | 10 | 4252 | 2.44 ms | 2.83 ms | 4.33 ms |
| 2 | 1 | 100 | 26225 | 3.33 ms | 4.15 ms | 10.27 ms |
| 2 | 4 | 1 | 850 | 2.98 ms | 12.15 ms | 24.64 ms |
| 2 | 4 | 10 | 9283 | 4.12 ms | 4.53 ms | 7.49 ms |
| 2 | 4 | 100 | 42936 | 5.80 ms | 17.86 ms | 36.88 ms |
