# PushupES 设计文档

面向 CQRS 框架的领域事件流存储服务。只追加（append-only）、高可用、
高性能。分层原则：Raft 只复制元数据，事件数据走 leader→follower 专用拉取协议。

## 1. 数据模型

```
EventRecord（事件流中的一条记录）
├── aggregate_id: string     # 聚合 ID（事件流即"同一 aggregate_id 的记录序列"）
├── version:      uint32     # 连续版本号，从 1 开始，同聚合内严格 +1 递增
├── unix_time:    int64      # 发生时间（Unix 秒）
├── command_id:   string     # 关联命令 ID（幂等键）
└── events:       [Event]    # 事件列表
     └── Event{ type: string, body: []byte }

槽位（Slot）——固定哈希分区
├── 固定 SlotCount = 1024（不可配置；2^10，按最多 256 个数据节点 × 每节点 4 槽估算，数据节点数取偶数，见 §7.2）
├── slot = mix64(FNV-1a(aggregate_id)) % SlotCount（见 §7.2.4）
└── 每个 slot 对应一条 WAL（按 64MiB 分段），支持在集群中热迁移
```

两套编号，职责不同：

- `version`：**聚合内**连续版本号，是 CQRS 乐观锁语义（业务规则 2）。
- `seq`：**槽内**全局单调写序列号（每个 slot 一个计数器），是复制/迁移的
  物理偏移坐标：follower 按 seq 拉取、迁移按 seq 追增量。

### 1.1 seq 的强制不变式（复制与迁移的正确性前提）

下面三条是**硬性契约**，不是实现细节。任何改动存储、复制、迁移的提交都必须
保持它们；破坏其中任一条都会让「按 seq 对齐」的机制静默出错。

**规则 1：相同 seq 在槽的所有副本上必须是同一条记录。**

同一个 slot 的所有副本中，`seq` 相同即记录相同——不存在「同一 seq 在不同
节点上是不同记录」的情形。强制点有两处：

- **seq 只由 leader 分配**：客户端不能指定 seq（`AppendRequest` 无 seq 字段，
  `version` 是聚合维度的，与 seq 无关），序号来自 `Slot.Append` 内的
  `seqCounter+1`。
- **副本落盘逐条校验**：`appendAtSeq` 对已存在的 seq 比对命令号与版本，
  相符才认「已复制」；**不相符即报 `ErrSeqDivergence`**，绝不覆盖。
  复制路径按槽隔离该错误，不拖垮整条会话。

**规则 2：槽内 seq 严格单调递增、不断层。**

每条新记录 `seq = 上一 seq + 1`。强制点：leader 侧在同一个槽锁内
「读计数器 → 写 WAL → 更新计数器」，并发写入被串行化；副本侧
`appendAtSeq` 对 `seq != counter+1` 直接拒绝（跳号不接受）。恢复路径按
seq 升序重放同一批记录，把计数器重建到 LEO。

由此可得两个用法结论：

- `seq` 是记录在盘上的**位移坐标**：`ReadSlotBytes(slot, seq, seq+1)`
  总能取到该 seq 的那条记录。
- **`seq` 到位 ≠ 记录对读者可见**。可见性由聚合目录（`version == 已解析
  seq 数`）决定，见规则 3。

**规则 3：已落盘的记录，其所属聚合的目录必须能定位到它。**

一条记录既然占了 seq、写进了 WAL，它所属聚合的目录就必须能解析到它；
否则任何读都看不到它，而 LEO 已经把它计入——**追平/水位类判据会因此误判**
（它们比的是 seq，不是可见性）。

写入侧由 `indexMetaLocked` 保障：目录只采纳「流的下一个版本」，
不采纳无法解析的版本（既不采纳、就不存在「声称了却读不到」的目录状态）。
但**记录层面的缺口仍可能出现**：迁移栅栏的 `pushFrames` 从目标自报的
`tgtLEO` 起推帧，若目标某聚合流短于其槽 LEO，推来的记录会落盘却不被目录
采纳，形成「已落盘但不可见」的缺口，而目标的 LEO 已推进到该位置。

**因此：「追平」判据只验证规则 2（seq 到位），不验证规则 3（记录可见）。**
凡以 `LEO` 比较作为「已同步/可提交」依据的地方（迁移 `awaitCaughtUp`、
写确认的水位等待），都必须清楚这一点。

## 2. 业务规则（写入协议）

一条写入记录（`AppendRequest(aggregate_id, version, unix_time, command_id, events[])`，
经 `BatchAppend` 提交，单条与批量同一路径）：

1. **幂等**：command 索引（slot 内 `map[command_id]→seq`）命中
   → 返回 `status=exists` + 已存储的记录（按 command_id 反查 seq 后从 WAL 读出）。
2. **版本校验**：该 aggregate 当前最大版本 `cur`（slot 内 `map[aggregate_id]→cur`）。
   - `cur == 0 && version == 1`，或 `version == cur + 1` → 通过；
   - 否则 → 返回 `status=fail` + 错误 ID `1001 (ERR_VERSION_CONFLICT)`，
     附带 `current_version`。
3. 通过校验后：追加到 slot WAL 尾部，原子更新三个索引
   （command 索引 / 聚合版本索引 / seq 计数器），返回 `status=success`。

错误 ID 表（定义在 `pkg/client/errs.go`——客户端按 ID 分支处理，服务端引用
同一张表作答，`internal/data` 只做命名别名；钉测防漂移）：

| 错误 ID | 名称 | 含义 |
|---|---|---|
| 1001 | ERR_VERSION_CONFLICT | 版本非递增 1 |
| 1002 | ERR_BAD_REQUEST | 参数非法（空 events、超长字段等） |
| 1003 | ERR_SLOT_NOT_LOCAL | 槽位不在本节点（MOVED，附正确节点） |
| 1004 | ERR_MIGRATING | 槽位迁移中（ASK 语义，附目标节点） |
| 1005 | ERR_NOT_LEADER | 本节点非该槽 leader（附 leader 地址） |
| 1006 | ERR_FLOW_CONTROL | 本节点流控拒绝（message 固定 `FlowControl`；令牌桶取空时写入不落 WAL 直接失败，见 §6.2） |
| 1007 | ERR_SLOT_UNPROVEN | 本地副本来源未证实（owner 围栏：leader 拒写、本地读答空，直到运维采纳或丢弃，见 §3.1） |

并发控制：锁粒度 = slot。同一槽串行写（保证 seq/version 原子推进），
不同槽并行。1024 槽 = 最多 1024 条并行写路径。

**批量写入 `BatchAppend`（gRPC client 面）**：一次 RPC 携带多条
`AppendRequest`，逐条独立走上面同一套业务规则（幂等/版本/HW 确认），按请求
**同序**逐条返回 `success / exists / fail`（响应里回显 `aggregate_id`，结果与
请求一一对应，客户端不依赖位置也能对号）。批内约束与执行模型：

- **批内 aggregate_id 必须各不相同**：任一聚合出现多次，该聚合的所有条目一起
  返回 `fail/1002` 且**一条都不执行**，其余聚合照常执行。同批两次写同一聚合
  没有确定的先后顺序，部分执行还会让重试语义变浑，所以整组拒绝而不是先到先得。
- **空批次返回空 results**，不算错误。
- **不设条数上限**：唯一的界是 gRPC 消息上限（`-grpc-max-msg-size`，默认 4MiB，
  recv/send 同值），批次大小随该配置一起调。
- **执行模型 = 同槽串行、异槽并行**：服务端按路由槽分组，每槽一个 worker 串行走
  `Engine.SubmitBatch`——整组一次写栅栏、一次路由表读取、按请求顺序逐条落盘
  （WAL 顺序 = 请求顺序），然后**整组只等一次槽高水位**（合并等待：对组内最大
  seq 等一次；超时水位覆盖到的前缀仍 success——其持久化承诺已兑现——水位之上
  的条目才 fail/1005，记录仍在 leader WAL，按同 command_id 重试命中 exists。
  副本停滞时整组只付一个截止周期，不是每条一个）。不同槽并发执行，并发上限
  `-batch-slot-parallelism`（默认 100；一批铺满 1024 槽时不至于瞬间压上等量并发 WAL 写者）。
- 重定向（MOVED/ASK/NOT_LEADER）也是**逐条结果**：该条 fail + 权威
  `slot`/`node`，服务端不代理转发写；客户端刷新路由后只重发受影响的那几条。
  幂等规则保证"部分成功 + 按 command_id 重试"安全收敛。

## 3. 存储设计（slot WAL）

目录布局：

```
data/
├── slot-000/
│   ├── 0000000000000001.wal   # 文件名 = 该段 baseSeq
│   ├── 0000000000000514.wal   # 满 64MiB 滚动出下一段
│   └── ...
├── slot-001/...
```

WAL 段文件格式（append-only，顺序读写）：

```
Header(20B): magic "ESWL"(4) fmtVer(1) reserved(3) slotID(4be) baseSeq(8be)
Body: Record*，每条记录：
  recLen(4be)                        # 记录体总长
  aggLen(2be) aggregate_id
  version(4be) unix_time(8be)
  cmdLen(2be) command_id
  eventCount(2be)
  Event*: typeLen(2be) type bodyLen(4be) body
```

- **段名即 baseSeq**：整段可独立滚动、拷贝、删除。
- **稀疏索引**：每 64KiB 数据一个 `seq→(段文件, 偏移)` 的常驻 seek 提示
  （16 B/条），查找先落到最近一条提示再走帧前进——提示间隔既决定一次定位
  最多走多少帧，也决定这份表占多少内存；按 version 读聚合由聚合索引
  （`.aidx`）直达。
- **重启恢复**：加载各 slot 段列表 → 从最后一段 `scanBody` 顺序扫尾，
  重建三个内存索引与 seq 计数器；**不设独立 meta.json**（避免元数据与 WAL
  双写不一致的竞态）。
- 崩溃尾部撕裂：扫描遇不完整记录即在完整边界截断。
- **刷盘策略**：flush.policy 可配（每 N 条 / 每 T 毫秒 fsync；默认组提交
  依赖页缓存）。写入确认的是**复制**（ISR 高水位覆盖该 seq），不是逐条
  本地 fsync——持久化边界与刷盘策略挂钩，见 §4。
- **刷盘执行：全局单线程、段级队项**（`internal/storage/flusher.go`）：
  全存储一个刷盘 goroutine，队列单元是「（槽，段）」并按入队时间老先出；
  每次 fsync 分三相——锁内快照（`prepareFlushLocked`）→ **锁外** `File.Sync()`
  逐次计时 → 锁内按快照做减法结算（`settleFlush`，fsync 期间新写入的记录仍
  算未刷），同时最多一个 fsync 在飞。策略触发（`IntervalMessages`/`Interval`）、
  封段滚动（封段即入队）与显式 `Flush()`/`Close`（入队 + 等完成）三个入口
  都收敛到这条队列。
- **刷盘路径自视**：`GET /admin/stats` 的 `flush` 段给出无锁可读的
  `pending_flush_sum`（未刷记录总数）、`oldest_unflushed_age_s`（最老一条
  等待 fsync 的记录等了多久：槽从「干净」转入「有未刷」时起表，每次
  flush 若仍留下记录就重新起表——持续写入的槽不会让它无限增长）、队列长度
  `queue_units`、在飞数 `inflight`、排队等待 `fsync_wait_ms{p50,p99,avg,max}`
  与 syscall 服务时间 `fsync_svc_ms{p50,p99}`、以及每次 fsync 新覆盖的字节
  `fsync_new_bytes_avg/max` 与它覆盖的段文件大小
  `fsync_file_size_avg/max`（后者才是服务时间量级的解释）。这台设备上
  4K+fdatasync 的参照：盘空闲时 p50 ≈ 2ms、邻租户打满时 ≈ 7ms。

### 3.1 owner 围栏（本地副本的来源证明）

两份数据、两条恢复路径互不依赖（槽 WAL 只认自己目录，共识状态只认自己的
日志/快照），危险只在两者错配时出现：**共识状态丢失或被替换、槽文件还在**
——重建后的表把槽规划给一个盘上已有旧 seq 空间的节点，旧事件与新事件就会
混进同一个槽。围栏给每个槽目录配一份证据文件 `slot-xxx/owner.json`，内容
是「写下它时，表把这个槽指给谁、哪个任期、我扮演什么席位」，外加本节点的
世代号：

- **世代号**：这条共识历史的标识，值存在**复制状态**里（slot 表的 `topology` 字段）。
  控制面首启时由 controller 把自己的共识目录 id（`<data>/cluster/topology`，随目录
  共存亡）作为一条 `set_topology` 命令提交进去，此后每个节点——每个 voter，以及跟随
  布局 feed 的数据面节点——都读同一个值（先写者胜，不会被后来的提交改写）。清空/替换
  共识 ⇒ 新世代 ⇒ 所有旧记录自动不匹配，这是围栏的主判据。值必须是集群级的：voter
  各自的目录 id 互不相同，若把「轮到哪个 leader 的目录 id」当作世代，普通重启换了
  leader 就会让数据面读到另一个 id，把整片副本误判成「另一个世代」全部围栏。
- **出生见证**：进程启动瞬间扫描一遍哪些槽目录带 WAL。「本进程生命周期内
  出现的副本」（首写、迁移推送、fetch 补齐）天然属于当前世代，直接盖记录；
  「出生就在盘上、又没有匹配记录」的副本才是来历不明，等待运维裁决。
- **判定**（leader 席位才会被围栏；冗余副本的读由同步证明门控，写由复制协议
  对账）：记录匹配 ⇒ 盖新记录（表变化后刷新，比较后写，稳态零文件 IO）；
  记录缺失/世代不符/任期倒退 ⇒ 该槽 PENDING——leader 拒写 `fail/1007`
  （不落 WAL，与 1006 同级的前置拒绝）、本地读答空。围栏只拒绝，从不删除：
  证据留在盘上等运维决定。
- **运维裁决**（节点本地接口，不经 Raft、不转发，与 §6.2 流控同形）：
  `GET /admin/slots/pending` 查看本节点被围的槽与原因；
  `POST /admin/slots/pending/adopt` 采纳（「这份数据属于当前世代」的显式
  确认；请求体 `{}` 或空=全部被围槽——升级到这个特性后的首启全量采纳就走
  这条，`{"slots":[3,5]}`=只点名槽，非 pending 的槽一律回 in skipped、绝不
  顺手采纳）。丢弃没有接口：删除一份集群仍在引用的副本会造成来源分裂
  （幸存副本随时可能带着旧 seq 空间复活），所以运维按传统方式删除磁盘目录
  `slot-xxx/`，下一拍围栏见 WAL 已不在自然解除裁决。手动删除 owner.json
  表达的是「这份副本来历不明」（重新进 pending），不是丢弃。
- **席位记录防「借位转正」**：来历不明的出生副本在席位阶段不盖章，晋升为
  leader 的那一刻按 leader 规则重新判定，未证实的副本不能靠持有只读席位
  洗白来源。

## 4. 高可用（复制设计）

分层结构：

- **控制面（自研 Raft）**：集群共识层是**仓库内自研的简化 Raft**
  （`internal/raft`：选主/日志复制/提交推进/快照），日志与 term/vote 存于
  **自研分段 WAL**（`internal/raft/wal.go`，组提交 + CRC + 崩溃截断恢复）。
  集群配置在**首次启动**时由 `-peers` 写进 WAL
  首条，且**只由一个节点写**：`-peers` 里 id 最小的那个（`-bootstrap` 可显式指定
  别的节点）。其余配置了 `-peers` 的节点一律以种子身份启动、向这些成员**报名加入**
  （`Adopt`），配置由 leader 的日志/快照带过来 —— 因此「新节点带更大的 `-peers`
  列表启动」就是扩容，不会与现有集群各写一份 index 1 配置而分裂。有 conf 记录的
  节点一律以记录为准（`-peers` 只当种子）；之后增删成员走运行时变更（admin 加删成员，
  见下文 §运行时成员变更）。不支持 PreVote/ReadIndex。
  它只复制元数据——
  slot 分配表（`slot → {leader, replicas[], epoch, state, seat_grant[]}`）、集群成员、
  节点数据面地址（`OpRegister`，见 §6 peer 面）。FSM 模型：
  `Applier` 接口 + 快照/恢复（快照为二进制表编码 `table_bin.go`，1024 槽
  ~850KB JSON → 几 KB）。成员集合由静态 voter 集唯一决定；路由表里每个成员
  同时记 `PeerAddr/AdminAddr/ClientAddr` 三个地址。
  设计稿见本文 §11。基准实测（256B 命令、真实 TCP、
  3s×3 取中位）：单节点顺序 Apply 1.46ms/op（p50 1.40ms、p99 1.94ms）、
  单节点并发 1.45ms/op（p50 2.84ms、p99 3.72ms）、3 节点顺序 5.52ms/op
  （p99 6.68ms）；每 op 分配 17 allocs、2KB 级。
- **数据面（专用拉取）**：
  - 客户端把写入（`BatchAppend`，单条与批量同一路径）发给槽 leader 的 gRPC
    client 面；leader 追加本地
    WAL 得到 seq。
  - follower 把「我作为副本跟随的槽」按 leader 分组，**每个 leader 一条
    常驻 PeerService.MFetch 长轮询会话**（gRPC，`peer.proto`，与 Raft 共用
    peer 端口；payload 是裸 WAL 字节区间 `FetchItem.payload`，不经 base64、
    事件体不重编码、不进 Raft 日志），一次请求多槽复用、空闲轮零额外往返。
    请求里的位置用 **packed 并行数组**表达（`slots[i]` 从 `from_seqs[i]` 起），
    而不是每槽一个消息对象——一个会话每轮要携带全部跟随槽（可达数千个），
    逐个构造对象的开销在水位等待下甚至主导了 CPU；响应则是**稀疏**的：
    只有携带数据的槽出现在 `items` 里，空槽直接省略。leader 端长轮询不为
    每个槽单独建 select 分支，而是监听一条**全 store 唤醒总线**（任一槽
    append 时触发），命中后再对处于等待中的各槽句柄做一次非阻塞扫描，
    确认究竟哪些槽有了新数据。
  - follower 追加到自己 WAL 后回报 LEO（批量 PeerService.ReplicaProgress）；
    leader 推进 **HW**（高水位 = ISR 内最小 LEO）。**follower 的落盘是帧直写**：
    收到的裸 WAL 字节按 seq 校验后**原样写入**（`Slot.AppendFrameAtSeq` /
    `Segment.AppendFrame`），内存索引只从帧头解析（`data.DecodeRecordMeta`，
    不物化事件体）——复制路径不 decode 成记录再 re-encode，100KiB 事件体下
    每条省掉两份大分配与两次全量拷贝（提交栅栏的冻结尾巴推送 `Replicate` 同路径）。
    leader 端还设有全局 payload 预算与轮转游标，防止积压的槽被饿死。
    应答语义：**请求到达时已有数据的槽立即随响应返回（有数据即答，不在长轮询
    里滞留）**，全部为空才进入等待；等待期间任一槽被 append 唤醒时做
    **burst drain**——在一个很短的合并窗口内扫描全部等待者，一次响应带走
    本轮所有有数据的槽。若不做合并，每个槽都要各付一次往返才能推进 HW，
    写入确认的延迟就会按槽数放大。预算耗尽但仍有数据的槽不重新进入
    等待（它等新句柄也等不到尚未发生的 append，只会空等到超时），交给
    下一轮的轮转游标处理。follower 侧健康的轮次（拿到数据、或被长轮询
    自然吸收的空轮）不等待立即重发，只有传输错误才退避。
  - 记账成本正比于变化量：leader 对每轮捎带的全部位置先比对再决定是否
    写入——LEO 未变的条目不更新状态、不重算 HW。副本的**存活戳**（lastOK，
    掉出 ISR 的判定依据）不靠每轮全量刷新，而是每 ~2s 发一次 **sweep 轮**
    （MFetch 的 sweep 标志位）为所有条目补戳，sweep 间隔远小于 stale 窗口；
    有数据的轮次只对真正推进过的槽即时补报 LEO（ReplicaProgress），
    不再全量重发。
  - **写入成功的条件**：一条记录（`BatchAppend` 携带的每条 `AppendRequest`
    各自适用）只有满足以下全部条件才对客户端回
    `success`——(1) 幂等与版本校验通过并落入 leader 的 WAL；(2) 该槽的高水位
    已覆盖此 seq，即**每个 in-sync 副本都已把同一记录写进自己的 WAL**。
    因此任何一个单节点故障都不会丢失已确认的写入。三个边界：ISR 内所有副本
    失联时，持久化承诺只对 in-sync 副本成立，高水位跟随 leader LEO、写入
    不阻塞（等副本重新追平 ISR 后该记录重新受保护）；**\"不在服务中的副本\"不算
    in-sync、不压高水位**——(1) 正在搭建的席位（迁移的准入目标
    `migrating_out → MigratingTo`；成员数变化后重排新补进来的席位；
    以及在 leader 切换那一刻仍年轻（`seatBuildGrace` 内入席）且未追平的席位——
    `slotRepl.building`/`takeover`）；(2) fetch 长轮询没有应答就结束的副本
    （连接已断，`goneSess`）；(3) 已被 controller 判定离线（`mark_down`）的副本。
    这三类都是\"正在被搬运/拉取的额外副本或已不可达的副本\"，等它追平到 leader LEO
    或重新上报那一刻起才按普通 ISR 成员参与把关（`isr` 视图同样把它们排除在外）；
    **复制面自视**（`GET /admin/stats` 的 `repl` 段，`?worst=N` 列出最久的槽）：把「客户端在等高水位」
    这一状态直接读出来——本节点所领槽里 `hw < LEO` 的**槽数**、最大缺口、
    最老缺口的**持续时长**（`slotRepl.hwAt` 记录 hw 上次推进时刻，于是它就是
    「这些 append 已经等了多久」），`?worst=N` 直接点名等得最久的 N 个槽；
    另有每副本的上报新鲜度与 LEO 落后、以及 leader 侧 fetch 轮的收尾方式
    （`ended_by_wake` = 被新记录唤醒收尾，`ended_by_deadline` = 长轮询等待预算
    到期才收尾）。发病时节点看起来是空闲的（无读在飞、无 handler 在等、CPU 接近 0），
    所以这几项是唯一能把「健康」与「被等待预算牵着走」分开的量。
    **一轮的写形状**：一轮的 items 是不同槽，**按槽并行写**（worker 数 =
    2×`GOMAXPROCS(0)`，`GOMAXPROCS(0)` 是本进程可用核数——容器里反映配额而非宿主核数，
    与 store 开槽 worker 同一口径；每槽内仍逐条按 seq 写，每条记录一次 durable 写），
    items 少于 `GOMAXPROCS(0)` 时走串行；一轮落地后把这一轮的槽位**一次性上报**
    （ReplicaProgress，带轮末标记）。
    依据：设备对并发写者的服务能力远高于单写者（4K pwrite+fdatasync 实测：1 写者 222 写/s、
    2 写者 388、4 写者 818、8 写者 1430），而串行写一轮时整轮的耗时就是各记录写耗时的**和**，
    于是「设备变慢」被整轮放大成「水位推进慢 ⇒ 确认慢」。同一套对照组实测（副本落后 +
    自造磁盘占用，冷启动，conns=1/batch=100、conns=8/batch=20/100 三格）：串行 237/379/318
    msg/s ⇒ 并行 5737/15049/5840，隐含每记录成本 6.33/3.96/4.72ms ⇒ 0.26/0.10/0.26ms。
    对照实验同时排除了「把上报拆成轮内多波」这条路：单独改上报（串行 apply + 轮内多波）
    与基线等价（233/248/231），叠在并行之上也无增益——一批的确认等的是这批里最后一条
    记录所在槽的 apply，而那条上报只能在它 apply 之后发出，提前上报无法缩短它。
    等待高水位
    超过 10s 返回 `fail/1005`，此时记录已在 leader WAL 落定——客户端应带同一
    command_id 重试，命中 `exists` 即确认。
  - ISR 维护：follower 在 `replica.lag.time.max` 内跟上进 ISR；
    掉出后按自己 LEO 重新追。
- **故障切换**：controller（Raft leader）每 1s 经 peer 面 `PeerService.Ping` 探活，
  连续 3 次失败判定失联 → 经 Raft 提交 `mark_down`：为该节点名下所有槽从存活
  副本重选 leader（epoch+1）→ 客户端收到 MOVED 重定向。**择主取「LEO 最高」的存活
  副本**（见下「择主的依据」）。
  **两条提前判定的路径**（它们只是在探活阈值之前拿到同样那个 `mark_down`）：
  ① 探活失败若来自「端口拒绝连接」（`dialRefused`：ECONNREFUSED，即那里没有进程在听）
  则当轮即判失联——慢与死的区分正是 3 次阈值要回答的问题，被拒绝的连接已经回答了
  （超时、半开连接等一律不算）；② **多个 follower 上报「该 leader 传输层不可达」**
  （`PeerService.ReportUnreachable`，观测者只在连续两次传输失败后才报、每 1s 至多一条）：
  controller 收下证据，**在同一 1s 窗口内出现 ≥2 个不同观测者**就立刻 `mark_down`
  （单个观测者的链路坏掉不算证据；探活仍是兜底——没人复制它的槽、或观测者自身被隔离时
  只能靠探活）。被杀进程的可见性是即时的（它的 TCP 一断所有观测者当场就知道），所以
  这条路把「死 → mark_down」从 ~3s 压到 ~1s 以内，也就是死节点冻结 LEO 压住高水位、
  它名下槽还指向死 leader 的那段时间。
  **择主的依据**：已确认的写入只保证在 in-sync 副本上——那些记录此刻还在**刚死掉那个
  leader 自己的日志**里，副本集里落后的那个副本并没有。所以「取副本集最前面的存活副本」
  这个纯表内规则会把槽交到一个缺数据的副本手上，那些已确认的写入就静默消失了（同类的
  症状实测过一次：整表重排后 71 个槽的 leader 是空副本、真数据留在副本上）。表里没有
  任何偏移量，能回答「谁最全」的只有副本自己，于是：提交 `mark_down` 之前，controller 用
  `PeerService.SlotLeos`（一次批量问一个候选节点：你手上这批槽的 LEO 分别是多少）问遍
  该节点名下槽的存活副本（候选按 slot 分组的并行一轮，`failoverPickTimeout=1s`，best
  effort），逐槽取 LEO 最高者，作为 `Command.NewLeaders{slot→node}` **随命令一起提交**。
  带上命令是因为 `mark_down` 要在每个节点上重放出同一个结果：表内应用时校验这个 pick
  （目录成员、该槽的副本、不是失败节点自己、自己不在离线态），不满足就落回表内兜底规则，
  所以有 pick 没 pick 的命令都能收敛。代价是故障切换前多**一轮** peer RTT（毫秒级）。分界
  与排除：已被 controller 判 down、或探活已连续失败的节点不作为候选——把槽交给一个正在
  死的过程等于没换。**运维移除（`leave_node`）不接这套 pick**：按流程 `scripts/cluster.sh
  remove` 先等该成员被判离线，那时它名下的槽早被上面的 `mark_down` 换走了，leave_node 只
  兜异常路径，用表内规则即可（它也不是「刚死」的时刻，没有新鲜度可比）。
  这段成本的账（事件驱动、非心跳）：单次故障 (N-1)×~100-150B ≈ 0.5~1KB，连接走既有
  peer 客户端池（无新握手），controller 侧只是一张带 TTL 的见证者表，落库仍只有那一条
  `mark_down`；空闲时为零，比它要替代的那 1s 周期探活还便宜。
  **目录条目不删除**（`mark_down` 只打 down 标记：admin/client 地址、副本集成员
  身份全部保留）——失联节点在路由表里始终以「离线」形态可见，恢复后 `mark_up`
  清标记、fetch 追平、回切拿回环上槽位。（旧实现用 `leave_node` 删除条目，与
  「按 Raft 配置同步目录」的 join 步骤互相抵消：删掉→重新塞回空壳（无地址）→
  再删……节点在控制台忽隐忽现，且宕机期间每几秒重写整表。`leave_node` 现在只在
  对账「目录条目 ∉ Raft 投票者」时提交——即运维真正移除成员。）无 peer 地址
  （PeerAddr 为空）的 peer 不参与探活，避免启动竞态误杀。
- **在线集合**：`Table.OnlinePeerIDs()` = 已自报 client 地址 **且** 未被
  `mark_down` 的成员（`Peer.Offline()` 取反）。回切环布局（PlanLeaderRebalance）
  与故障切换择主（backupLeaderFor）只锚定这个集合——控制台「离线」与集群放置
  决策同源不漂移。plan_slots/replan_slots 则铺满**全目录**：bootstrap 期只有
  自己注册完成，按在线集规划会把所有槽堆到单个节点上；而真宕机的成员反正已被
  `mark_down` 移走 leadership，回切环也禁止把它放回去。
- **leader 回切（再平衡）**：失联节点恢复并重新加入后，`replan_slots` 只把它
  补回副本集（follower，经 fetch 追数据），**不动 leader**——故障切换留下的
  失衡由 controller 的再平衡循环收敛：每 `-rebalance-interval`（默认 2s）一
  轮，把偏离环布局（leader ≠ `node[slot % N]`，环按**在线成员**目录计算——已
  自报 client 地址（clientAddr）的节点才算在线，只 join 未注册的节点既不在环
  上、也绝不会成为回切目标，因此宕机期间剩余在线节点之间也会互相平衡，节点
  重新注册后自动回到环上）的 stable 槽，每轮**只提交一次**（主关键路径严格串行：
  任一时刻只有一个槽在交接栅栏上；560 槽最坏情况 560 轮收敛）由控制器驱动 §5 的三阶段
  链路迁回环上的预期 leader。安全门禁：任一秒位在迁移中（如人工 migrate 在飞）整轮让路；
  回切目标必须已完成注册且 Ping 存活。每轮按槽推进**一步**：目标不在副本集就入场
  （`OpSlotAddReplica`），在副本集且已持有源端日志就提交（fence + `OpLeaderMove`），
  摘要不一致（同 LEO 不同目录）则丢弃该副本让它重新入场；等待目标追平（LEO 落后）
  只是让路，不计入退避。**失败按槽退避重试**（2s→4s→…→上限 1 分钟；记录按
  (from,to) 键控，故障切换换了端点即视为新交接、立刻可试）——丢弃副本与提交栅栏都会
  按这个退避重试，所以一个修不好的目标不会每轮被丢一次。0=关闭再平衡；负数启动即报错。
  每一轮先做 leader 交接、再做**席位回收**（`reclaimRound`，见下面「副本集重排」）：
  两件事共用同一个 cadence 与让路门禁，且各自每轮推进一次提交。
- **默认拓扑**：`slot_count=1024`（固定）。每槽副本数由**副本策略**（replica
  policy）分档决定，策略值存放在 Raft 复制的槽表里、全集群一份：
  `low` = 1 份、`medium` = 2 份（**默认**）、`high` = 每个数据节点各存一份全量，
  即因子等于布局池的大小（`Table.HostingIDs()`，§12.2）——最强的持久性档位，
  代价是每个数据节点存全部字节。任何一档的因子都按数据节点数钳制（因子不会超过
  池大小：1 个数据节点的集群上的 medium 落为 1 份）。控制器每轮按「表里的策略 ×
  当前数据节点数」重算并写进槽表（`OpConfig`），策略一变或承载池一变就把因子对齐；
  副本集按环 `slot % N` 起、向前取该数量个节点。
  策略的修改入口是 admin 接口 `POST /admin/cluster/replica-policy`（控制器专属
  的 Raft 命令 `OpSetPolicy`，命令只带档位、副本数在应用时按数据节点数推导，保持
  所有节点状态机确定性），控制台集群页的「副本策略」卡片配了编辑态（修改 →
  草稿 → 保存确认 → 提交）；启动参数 `-replica-policy`（env
  `PUSHUPES_REPLICA_POLICY`，默认 medium）只为新集群种下初始值——控制器仅在
  槽表首次规划（表还是空的）时提交一次种子，此后表里的值就是权威，重启节点
  不会重刷策略，改档位只能走接口。
- **副本集重排由因子跳变、或布局漏掉某个成员触发**（operator 拍板）：控制器据此把槽表按环重铺。
  判据有三条：① 任一 stable 槽的副本集大小 ≠ 因子——「因子变了、布局还没跟上」（退出成员留下的
  多余席位，或新成员缺的席位）；② 环计划要给它席位的成员里，有谁在**任何** stable 槽的副本集里
  都没有席位——「布局把某个成员整个漏掉了」，这正是**因子没动**的成员变更的形状（low/medium 档的
  因子不随成员数变化，high 档的偶数成员沿用基数以下的因子），它造成的后果与因子跳变一模一样：
  该成员的环上预期 leader 不是任何槽的副本，而再平衡的交接门禁正是「目标必须是该槽的副本」，
  所以那些槽永远轮不到交接（实测：low 档 1 成员起步长到 3 成员，1680 个槽的 leader 仍然全在
  node-1；medium 档 2 成员长到 3 成员，node-3 一个席位都拿不到）；③ 因子为 1 时，任一 stable 槽的
  leader ≠ 环的预期 leader——单副本集腾不出席位给环上那个预期 leader 当 follower，前两条判据
  都可能看不见「只是放错位置」的单副本槽（一次手工迁移在环外提交、旧席位被回收收走之后，
  集合大小又等于因子、也没有成员被漏掉）。三条判据都走同一条补席位动作：
  - **补席位是重排里唯一搬动副本集的动作**：每个 stable 槽按环 `slot % N` 起算，
    把环上它还没有的席位补进副本集（已持有的席位保持原顺序在前）。补齐是必须的：
    再平衡的交接门禁是「目标必须是该槽的副本」，副本集不回到环上，环上的预期
    leader 就永远不是副本、永远轮不到交接，该槽永久留在环外（实测：7 节点移除
    2 个离线成员、因子 4→3 时，未复位的表留下 294 个卡在环外的槽，leader 分布
    冻结在 377/332/433/294/244，而 5 节点环应为 336×5）。新补的席位按普通
    fetch 追平，leader 交回环上仍由再平衡循环的栅栏完成；新补的席位按上面的
    「正在搭建的席位」规则处理：追平前不进 ISR、不压高水位，所以一次重排不会
    让受影响槽的写入停下来等拷贝搬完；
  - **多的席位由席位回收单独收回**（`reclaimRound`）：重排只补不收，因为「收」
    是在对真实副本做判断——一个槽此刻持有的席位正是它在复制的那几个副本，而
    故障切换择主的表内兜底（`backupLeaderFor`）取的是副本集**最前面**的存活副本；成员数
    一变就立刻删席位，会让刚补进来的空席位排到前面，leader 一挂就可能落到空副本
    上（实测：一次「补 + 收」同轮完成的整表重排之后，71 个槽的 leader 是空副本、
    真数据留在副本上，`scripts/cluster.sh slotcheck` 全部报 DIVERGED）。所以回收是
    独立一轮：一轮内**按预算扫完所有多余席位**（`reclaimBudget`=1s，到点停、下一轮接着扫），
    且只在**留下来的每个席位都与 leader 摘要等价**时才删——在那之前，多出来的那个席位
    正是 leader 挂掉时 acknowledged 记录的第二个家。leader 永不被回收（没有写者的槽
    无法接收写入），leader 自己就是多余席位时，等再平衡先把它交接走，下一轮再收。
    一轮扫全部要有三道护栏，否则它会把「一次探测超时」放大成整轮停摆：
    **席位级探测并发**（`reclaimProbeConcurrency`=16）、**对端熔断**（某个对端一次探测
    失败就在本轮把它 park 掉，它名下的席位全部跳过——一次超时而不是每席位一次）、
    **按席位批量提交**（`OpSlotRemoveReplica` 带槽列表，一轮 1024 席回收是「每个席位一条
    命令」而不是「每个槽一条」）。实测（3 节点 1680 槽，medium→low）：收敛从
    「每轮 1 席、约 56 分钟」变成**一轮内完成**（t=13s 已是 {1:1680}），磁盘在保留期后
    释放（345→175 MiB/节点）；
  - 数据节点数取 2 的幂（2、4、8、…、256）时重排，全员摊开：1024 槽在 2/4/8 节点上分别是
    512/256/128 个槽主，副本席位（factor 2）1024/512/256 每节点；其余偶数成员由环布局
    摊平，槽主数相差不超过 1（补席位即时完成，席位回收一轮扫完、
    按席位批量提交）；
  - **因子没动的成员变更也会重排**（判据 ②），因为新成员零席位就是「漏掉一个成员」：
    medium 档（factor 2）从 2 成员长到 4 成员时因子仍是 2，但新成员该拿它们那份环上席位
    （1024 槽里 node-3、node-4 各补 512 个席位），收敛后是 256×4 个槽主。补席即时完成，
    回收一轮扫完（按席位批量提交），新席位追平期间不压高水位，所以写入不停。
    代价是新成员要按 fetch 拉一遍它那份副本（medium 档成员翻倍时，新成员各拿到
    2048/1024 个槽的席位，量级是「一个节点当前数据量的一半」），这是「成员加入即参与承载」的
    一次性代价；只有因子与成员集合都不变时（稳态）布局不动。
  策略与生效因子
  从 `GET /admin/cluster/status` 的 `replica_policy` / `replica_factor` 读回。重排与回收都不动已有 leader：
  leader 交回环上只由再平衡循环完成。唯一的例外是**没有 leader 的槽**——退出成员持有某槽的
  唯一副本时，故障切换没有别的副本可托（`backupLeaderFor` 返回空），leader 留空；而再平衡按契约
  跳过空 leader 的槽、整表重排只在槽表为空时跑，该槽会永久无主。重排对这样的槽直接补上环的
  预期 leader（epoch 递增，与其它换主路径同一把栅栏），而同一次补席已经把这个 leader 补进了
  它的副本集，所以补完即有写者、无需再交接。

## 5. 槽位热迁移（副本构建三阶段）

槽是布局变更的原子单位。任何让某个槽多一个副本、或换一个 leader 的动作都走同一条链路，
**不区分起因**（副本因子提升、节点加入/重加入、leader 回切、分叉副本修复）：

**一次布局变更 = 入场 → 追平 → 提交**

1. **入场（stage，Raft 命令，便宜）**：把目标节点加入该槽的副本集
   （`OpSlotAddReplica`，幂等）。不动 leader、不搬数据、不冻结写入。目标从此以**普通副本**身份
   跟随该槽——与晚加入的副本完全同构。
2. **追平（catch up，只走 fetch）**：目标用复制协议（`PeerService.MFetch`）按 seq 拉取，
   每槽每轮 ≤4MiB、整轮 ≤`maxPayloadBytes`（32MiB，尾巴优先）。追平中的席位不参与该槽的
   水位 min（`advanceHW` 排除），也不被写入等待（`waitForHW` 只等 ack holder），
   所以写入不受影响：追平速度只影响布局收敛时间。
   追平也**不按先来后到**：环席位（节点新加入 / 重加入、或成员离开后环重新分摊得到的槽）先于
   因子补的席位。判据不能从"谁落后"推——因子提升同样让每个节点在大量槽上落后，形态一样——所以由
   **席位授权时的原因**决定并随表复制：`Placement.SeatGrant[node] ∈ {ring, factor}` ——
   一次 replan 若发生在成员目录相对上次规划有变化时（加入 / 重加入 / 成员离开后环重新分摊），
   它补的席位整批记 `ring`（加载，整环都要收敛，所以一起优先）；成员集合不变、只是因子抬升
   补出来的席位记 `factor`（冗余）。`OpSlotAddReplica` 的席位是交接目标，也记 `ring`。fetch 轮在同一会话内把位置分三类——
   ack 关键尾巴 → 环席位 → 因子席位，类内保持轮转顺序；跨会话则让正在追平环席位的节点拿满轮预算、
   其余会话按半预算服务。尾巴不参与预算让路，所以写入 ack 永不被这条规则牵连。规则自己可见：
   `/admin/stats` 的 `repl.mfetch.ring_catch_up_rounds` / `half_budget_rounds`；注意单副本布局
   （因子 1）里一个节点的环席位就是它自己领的槽，停机再拉起后不存在副本侧的环追平，计数为 0。
3. **提交（commit，有界、串行）**：只有 leader 交还需要提交——
   （a）置 `slot → migrating_out, migrating_to=T`（读客户端提前改道，写请求进入栅栏排队）；
   （b）**commit fence**：源端冻结该槽写入（排队，不失败）、吸干在途写、把**冻结尾巴**按 seq
   推给目标（`PeerService.Replicate`，O(尾巴)），并只在目标**确认持有冻结 LEO 且已落盘**
   （`SlotLeo{flush:true}` → `durable_leo ≥ 冻结 LEO`，有界等待）后才返回——已提交的 leader_move
   是终态，源端副本随后不再是可恢复的那一份，单副本布局下目标若带着未刷盘的尾巴掉电会丢已确认写；
   （c）`OpLeaderMove`（epoch+1）原子切换 leader；
   （d）回收多余席位（`OpSlotRemoveReplica`，best effort）。
   栅栏是唯一的冻结窗口，且每轮 controller 只提交一次（严格串行），所以任一时刻被冻结的槽 ≤1。

**提交前置判据（唯一）**：`slotCopyStatus` 比较两端目录摘要与 LEO——

- 目标 LEO 落后 ⇒ 等待（fetch 还有活干，不累计退避）；
- 摘要一致且不落后 ⇒ 可提交；
- **同一 LEO、摘要不同** ⇒ 目标的结构性短缺（§1.1 规则 3），fetch 永远修不好 ⇒
  丢弃该副本（`PeerService.DropSlot`）让它从空重新拉，而不是原地修。这个「分叉」判定要
  **复核确认**：副本的聚合目录在 apply 批落地前会短暂落后于自己的 LEO，读起来与分叉一样
  （实测抓到过这种瞬时读数，一分钟后再读已干净）。所以门只在**同一分叉读数持续到下一轮**时
  才动手丢（`confirmDiverged`，窗口 2s），读到一致或仅落后即清候选。

完整性由**提交门**负责（摘要一致 + 目标不落后，在栅栏之前完成）；fence 内的守卫只拦
**分叉**方向——目标持有源端没有的版本；提交窗口因此保持 O(尾巴)。

`Replicate` 只服务提交栅栏的**冻结尾巴**（一次一条记录、同 seq 幂等落盘）。
**槽状态**：`stable → migrating_out(源) → stable`（epoch 递增）。`migrating_out` 只在提交窗口
出现：入场与追平期间槽保持 stable，读写照常由源端服务。

**回收让路**：`reclaimRound` 跳过在飞交接的槽——操作员点名的目标可以不是环上预期的席位，
回收不得把正在追平的那个副本当成多余副本删掉（否则提交时会发现目标已不在副本集）。

**清理（前源副本）**：源节点在**自己观察到已失去该槽副本集成员资格**之后才开始倒计时——
计时由源节点自己掌握，不依赖控制器：控制器既不持有那份副本，也不该为一次可能回滚的迁移启动
保留期。倒计时到期即 `DropSlot` 删除本地副本。开始倒计时的条件是**成员资格的丢失**，
而不是"当过迁移源"：

- **目标不在副本集**（入场加过席位）：leader 切到 T 后由回收步骤（`OpSlotRemoveReplica`）
  把 S 移出集合——S 这才真正变成多余副本，开始倒计时；
- **目标本来就在副本集里**：没有任何成员被移除，S 仍是 RF 的一部分，**不启动倒计时、不置灰、
  数据永不删**——如果按"谁当过迁移源"来判断，就会在这个场景里删错数据。

每次丢失成员资格都**重新开始**倒计时；上一轮的调度一律作废，绝不复用旧的到期时刻
（复用会让新一轮的副本在旧窗口到点时被立刻删掉，延迟删除就变成了立即删除）。
倒计时是**一个节点一个扫描器**，不是每个槽挂一个计时器：节点把「槽 → 到期时刻」排进队列，
唯一的那条扫描 goroutine 睡到**最早**的到期时刻（被新调度唤醒则重算），醒来一次把**所有到期**的
副本一起删。一次缩容会让上千个槽同时变成多余副本，按槽各起一个 goroutine + 计时器是白付的
负载；实测 3 节点 1680 槽缩容：3 条回收命令在同一秒提交，1680 次 `DropSlot` 在 +30s 的 3 秒窗口内
全部删完（每节点一次唤醒），磁盘 345→140 MiB/节点。
时长 = `-drop-after`（默认 30s，`0` 取默认值，负数启动即报错）；**没有关闭选项**：前源节点的
那份副本已成死数据，长期留在盘上会把节点写满。倒计时期间该调度对 admin 面可见
（`/admin/writes` 的 `dropping[]`、`/admin/slots/{slot}/describe` 的 `pending_drop_at`），
控制台据此把该副本置灰。若期间该槽又回到本节点名下（回滚、故障切换或又被加回副本集），
取消调度、保留数据；到期时还会**再查一次表**，成员/leader 的副本一律不删。倒计时只存在于
内存：进程重启即丢失，副本保留（等下一轮迁移或人工回收），**重启绝不删数据**。

客户端路由：

- 本地缓存 `slot → node` 路由表（gRPC `node` 字段携带目标节点 client 地址），MOVED 驱动刷新。
- 提交窗口内写源端：写请求在 fence 上排队后照常落盘；读请求返回 ASK 重定向指向 T。
- 提交后命中旧路由：返回 `MOVED(1003)` 重定向。
- 幂等规则保证重试不产生重复：两侧 command_id 相同，命中 exists 即收敛。

## 6. API 设计

每节点三个端口，各司其职，事件读写**只走 gRPC**：

- **client 面（gRPC，客户端事件写入/查询唯一入口，默认 `-client
  http://127.0.0.1:8591`，`PUSHUPES_CLIENT`）**：proto3 契约
  `proto/pushupes/v1/events.proto`（`pushupes.v1.EventService`：`BatchAppend`（写入唯一入口）+
  `ReadStream` + `ReadTails` + `ReadByCommand` +
  `ReadVersionByTime` + `PrefetchRoutes`）。body 为原始
  bytes，无 JSON/base64 层。client 面 gRPC 消息上限由 `-grpc-max-msg-size`
  配置（默认 4MiB，recv/send 同值）：`BatchAppend` 一整批要装进这个上限，
  批次大小随它一起调。
  MOVED/ASK 重定向的 `node` 字段携带槽 leader 的 **client 地址**（路由表
  Peer 同时记 PeerAddr/AdminAddr/ClientAddr），客户端据此重连。读准入
  （`Engine.LocalRead`）决定服务端是否本地作答：**leader 恒本地**（读自己的
  durable LEO，不受水位约束）；**副本必须「同步已证明」才本地**——取到过该
  槽**当前 placement epoch** 的 HW 账本样本、且本副本 durable LEO ≥ 样本水位，
  本地读上界即该水位（恰好是已确认前缀）；样本为 0 表示尚无确认，本地答空
  （绝不因 store 的 0=无界约定泄露未确认尾巴）。未证明同步的副本（新入集的
  follower 还在追、掉队的副本、或 epoch 换代后失效的旧证明）与不持有该槽的
  节点一样，读应答就地 1003（MOVED）+ 槽 + leader 的 client 地址，客户端
  刷新路由行后改投——服务端从不中继读流量。这正是「新 follower 副本不可读、
  同步完成才可读」的落点：ack 等过 ISR 高水位，leader 上确认数据必然完整。

  **`PrefetchRoutes` 是客户端的路由预取接口**：无请求参数（路由表是集群级
  状态，不是逐查询的答案），返回 `node_client_addrs`（集群各节点 client 面
  地址，首次出现序去重）与 `slot_node_index`（按槽号稠密排列的 leader 地址
  下标；**数组长度就是集群槽数**）。客户端初始调用一次、此后周期性刷新，
  写入命中 MOVED 时也即时修补本地路由行——路由表只是**提示**，写入正确性
  由 MOVED/ASK 重定向自愈定义，不依赖表的新旧。任何节点都能回答（表经
  Raft 复制）。客户端可见的只有**算法**：槽计算 `SlotOf` 与错误 ID 表落在
  `pkg/client`（`slot.go`/`errs.go`），服务端代码引用同一实现（算法一致性
  有钉测）；**槽数对客户端代码不可见**——客户端不硬编码它，从
  `PrefetchRoutes` 应答的数组长度读取。
- **admin 面（HTTP，默认 `-admin http://127.0.0.1:8091`，`PUSHUPES_ADMIN`）**：**仅管理**——
  status/writes/plan/migrate/槽 describe/healthz/pprof，**不承载任何节点间流量**
  （原 `/internal/*` 复制/迁移端点已全部迁到 peer 面 PeerService gRPC，admin HTTP
  路由已删除）。

  **append 的回显是不对称的**：`exists` 回显存储记录本体（`*EventRecord`，零拷贝引用；
  重放方唯一无法自行还原的信息就是它），`success` 只回 status/seq/version——
  调用方手里就是刚发的那条记录，再回显 100KiB body 等于每条 append 多付一次
  marshal 与整包往返（实测同负载 627→762 msg/s、p50 5.69→5.09ms、每条消息
  CPU -16%，其中还含客户端省下的一次 unmarshal）。事件面上**没有 JSON 包装层**：
  旧的 `{"_b64":...}` 方案在大 body 上要付出 json.Valid 全量校验 + base64 重建 +
  解码往返的 CPU（实测 100KiB body 下占 leader CPU 一半），已随 HTTP 事件面退役
  一并删除。
- **peer 面（Raft + PeerService gRPC，默认 `-peer http://127.0.0.1:8391`，`PUSHUPES_PEER`）**：复制槽位分配表等元数据（Raft），并承载**全部节点间数据面**——副本拉取、LEO 上报/探活、提交栅栏的冻结尾巴推送、LEO 追平、地址注册协议（proto3 契约 `proto/pushupes/v1/peer.proto`，`pushupes.v1.PeerService`，服务端 `peersvc.go`）。**运维只配这一个端口**：`-peers` 主格式 `node-id=http://ip:peerport`（或裸 `ip:peerport`，地址兼作节点 id）。**地址统一规范：存储/路由表/status JSON 中所有 admin/client/peer 地址都带 scheme——未写协议默认补 `http://`，显式协议以传入为准；TCP 拨号（listen/dial/gRPC/Raft transport）前再剥掉 scheme**（`cluster.NormalizeAddr`/`HostPort`）。admin/client 地址不配置，由各节点自报进路由表：
  - peer 端口上是复用监听器（`peerMux`/`peer_mux.go`，实现共识层的 `raft.Transport`）：按连接**首字节 `P`**（HTTP/2 client preface `"PRI ..."` 以 `P` 开头）把 PeerService gRPC 流量与共识流量分流（自研 Raft 的握手首字节是 magic `0x9E`，**断言 ≠ 'P'**，永不冲突；两路都经 `replayConn` 回填被 peek 消费的字节）。gRPC 服务端由 `Engine.ServePeer` 挂在该分流 listener 上，peer 面与共识层从此共用一个端口、一套 gRPC 语义（HTTP/2 多路复用，每对节点一条缓存连接 `peerClient`，keepalive 10s，服务端放宽 enforcement）。
  - leader 收到注册 RPC 后提交 `OpRegister`（就地修补路由表中该成员的 `AdminAddr/ClientAddr`，不新增成员——成员集合仍由 Raft 配置决定）；follower 收到则转发给 leader 的 peer 地址。
  - announcer 幂等周期重试（启动期 1s，收敛后转 10s 心跳），任意启动顺序都能收敛；节点换端口重启也会被自报值修补。
  - 旧多端口格式 `id:peerport:adminport:clientport` / `id:host:peerport:adminport:clientport` 仍兼容（作为静态种子，注册落地后以自报值为准）。

```
gRPC  EventService/BatchAppend    写入唯一入口（单条=一条记录、批量=多条）：幂等(command_id)/版本(+1)/等 ISR 高水位确认，逐条结果同序返回，MOVED/ASK
gRPC  EventService/ReadStream     范围查询（≤HW 语义；非本地就地 1003+node 重定向）
gRPC  EventService/ReadByCommand  command_id 幂等探针（重定向同上）
gRPC  EventService/ReadTails      批量取多个聚合的最新版本（resume 扫描用；服务端逐条应答——本地槽给版本、其余给该条自己的 1003+node，客户端库按路由表分组扇出、逐条修补重发）
gRPC  EventService/ReadVersionByTime  按聚合 id + unix_time 查询「时间戳早于或等于给定时间」的最大 version（客户端按时间点追溯历史：先定锚点版本，再 ReadStream 回放；恰好写在给定时刻的记录计入锚点；0=该时间点及之前无可见记录；从索引作答、不读 WAL 帧；可见性按读准入判据（leader=自身 LEO、同步副本=已证水位），未证明同步的副本与非持有者答 1003+node 重定向）
gRPC  EventService/PrefetchRoutes  路由表预取（无请求参数；返回全集群节点 client 地址表 + 按槽稠密的 leader 下标表，长度即槽数；客户端初始一次 + 周期刷新 + MOVED 即时修补）

# ---- peer 面（PeerService gRPC，与 Raft 同端口，proto/pushupes/v1/peer.proto）----
gRPC  PeerService/MFetch          副本拉取（长轮询，多槽复用，payload=裸 WAL 字节）
gRPC  PeerService/ReplicaProgress 副本 LEO 上报（批量）
gRPC  PeerService/Replicate       提交栅栏的冻结尾巴推送（同 seq 落盘，幂等）
gRPC  PeerService/SlotLeo         查询节点某槽 LEO（提交前置判据，见 §5）
gRPC  PeerService/DropSlot        丢弃对端某槽本地副本（分叉副本重新入场用）
gRPC  PeerService/Ping            探活（controller 故障切换）
gRPC  PeerService/Register        数据面地址自报（OpRegister 提交/转发）
gRPC  PeerService/Adopt           新节点自报入编（add_member 提交/转发，见 §6.1）

# ---- admin 面（HTTP，仅管理）----
GET  /admin/slots/{slot}/describe              # 本节点视角的槽状态/seq/HW/大小（不代开槽，带 node/role/loaded）
GET  /admin/cluster/status                     # 分配表/epoch/ISR 视图（含 client_addr）；
                                               #   raft 段里 heartbeat_timeout/election_timeout 是
                                               #   本进程实际生效的共识时序（见 §8 的比例约束）；
                                               #   顶层 storage_bytes 是集群存储大小 = 每个槽的
                                               #   **leader 副本**落盘字节之和（写落在 leader 上，
                                               #   其它副本是同一份数据晚一个复制轮；按副本求和会把
                                               #   同一份字节乘以因子）。任何节点都能回答：各节点后台
                                               #   每 2s 采一次样（自己 leader 的槽本地读，其余问 peer），
                                               #   status 读缓存值；storage_bytes_complete=false 表示
                                               #   有 slot leader 没应答，此时该值是下界
GET  /admin/writes                             # 每槽 durable 计数 + 每槽落盘字节 write_bytes（与计数同下标；两条落盘路径共用：batch append 与追平路径 AppendAtSeq/AppendFrameAtSeq——副本拉取、迁移搬运都会计入，所以它们是「本节点落了什么」的总量口径，不是客户端写入口径）+ client_writes/client_bytes（与计数同下标的**仅客户端**口径：只有 batch append 落盘才增长，追平路径不碰；控制台的「客户端写入速度」卡片与折线图的实线对它按槽做「leader 身份两拍相同」的稳定作差——换主的槽该窗不计，节点间流量不进速率，折线图的虚线「总写入速度」才按 writes/write_bytes 全槽求和作差）+ 槽内总字节/事件流数量 + 本节点待清理副本 + 本节点流控（flow_control = 生效配置视图 {tokens, period_ms, configured}；flow_control_hits = 每槽累计流控拒绝数——桶是节点的、计数归槽——与计数同下标）+ 同步计量 synced_slots = [本节点领导的槽数, 其中已按副本因子收敛的槽数]——「收敛」= 副本集与 in-sync 副本（leader 计入，follower 用 ISR 同口径）都达到表里的因子（Engine.SyncedLeaderSlots）；控制台按「slot_count − Σsynced」给出构建中剩余数（前端轮询）
GET  /admin/stats[?worst=N]                    # 三个「等在哪里」自视合一：flush = 待刷记录数/最老未刷记录年龄/刷盘队列长度与在飞数/fsync 排队等待与 syscall 服务时间分位/每次新覆盖字节与覆盖的段文件大小；repl = `hw < LEO` 的槽数与最老等待时长（?worst=N 列出最久的槽）/每副本上报新鲜度与 LEO 落后/fetch 轮次与「按数据唤醒 vs 按等待预算到期」计数；ack = 每条写入的确认链路四段耗时（栅栏+路由 / 本地 WAL 落地 / 等 ISR 高水位 / 整调用）与结果、失败原因分布、正在等待的调用数、水位落后条数（?worst=N 列出等得最久的槽）
GET  /admin/slots/{slot}/streams?after=&limit=  # 事件流列表（聚合id+最新版本+最新记录 unix_time，仅内存索引，不读 WAL）
GET  /admin/slots/pending                      # 本节点被 owner 围栏拦下的槽（slot/原因/表与记录的 leader-epoch-世代；节点本地）
POST /admin/slots/pending/adopt                # 采纳被围副本（节点本地，见 §3.1；{}=全部 pending 槽，{"slots":[...]}=点名且仅限 pending，非 pending 回 skipped）
POST /admin/slots/{slot}/migrate  {to_node}    # 发起热迁移（控制器专属，必须发到 Raft leader 的 admin 地址）
POST /admin/slots/{slot}/remove-replica {node} # 回收副本集里的一个成员（控制器专属，同上）
POST /admin/cluster/plan                       # 触发重新规划（控制器专属，同上）
POST /admin/cluster/replica-policy {policy}    # 切换副本策略档位 low/medium/high（控制器专属，同上；回当前策略与生效因子）
POST /admin/flow-control {tokens,period}      # 设置本节点流控（**节点本地**：收到请求的节点就是被设置的节点；重启后回到不限流。一个节点级令牌桶、该节点全部 leader 槽共享；period 是 Go 时长字符串（"1s"）；tokens<=0 或 period<=0 均表示不限流；回读当前配置 + 命中统计（节点总数 + 每槽列表））
GET  /admin/flow-control                       # 本节点流控配置与累计拒绝数（节点级 total_hits + 每槽列表；同样节点本地）
DELETE /admin/flow-control                     # 恢复本节点不限流（节点本地，同上）
GET  /admin/cluster/nodes                      # 列出当前 raft 成员（本节点视角：id/peer/admin/client/down）
POST /admin/cluster/nodes {id,peer_addr,role}  # 运行时加成员（控制器专属，同上；role 必填 voter|learner；admin/client 地址由新节点自报）
DELETE /admin/cluster/nodes/{id}               # 运行时摘成员（控制器专属，同上；仅离线节点，在线回 400）
GET  /healthz
```

### 6.1 运行时成员变更（扩/缩集群）

Raft 配置（谁投票）与复制的成员目录（槽表里的成员列表）是两件事，这一节把它们
绑在一起。**接口只在 admin 面，且只有 controller（Raft leader）能执行**：follower
拒绝并回 `controller`/`controller_admin_addr`（客户端改投），admin 面从不转发。

- **加成员 `POST /admin/cluster/nodes {"id":"node-4","peer_addr":"10.0.0.4:8394","role":"voter"}`**：
  只给 peer（共识）地址，`role` 必填（见 §12.1）。加 `voter` 时 leader 先把新节点当
  **raft 追赶者**（只收日志、不计入 quorum）复制，追到 leader 当时的 commit index
  后才把 `add_member` 配置项写进日志
  （20s 追不上也放行，避免死节点卡住操作）——否则新成员可能空日志就参与 quorum，
  leader 被它替换后读不出数据。配置项提交后：目录同步自动补 `join_node`，
  它自己通过注册协议补 admin/client 地址，`replan_slots` 按副本因子补齐副本集，
  回切器把环上的槽主交给它。幂等：加已在编成员是 no-op。
- **成员表从日志里推**：初始配置是日志第 1 条 `conf` 条目（不只写在每个节点自己的
  配置记录里），快照与 `InstallSnapshot` 也带上快照点的成员表——新节点无论是靠复制
  日志还是靠装快照追平，推出来的成员表都和集群一致。少了这一条，新节点只知道
  「把自己加进来」那一项，会以「整个集群就我一个」的姿态运行（quorum=1、日志陈旧
  也能自己当选、能确认别人没有的写入）。
- **摘成员 `DELETE /admin/cluster/nodes/{id}`**：提交 `remove_member`。只允许
  摘**离线**成员（与 `Peer.Offline` 同一条规则：未上报 client 地址或被 controller
  标记 down）；在线成员回 400（`cluster.ErrOnlineMember`），要摘必须先把它下线
  ——摘除是死节点的清理动作，不是重新调度在线槽位分布的手段。leader **不会
  立刻断开**被摘节点：它要等该节点确认收到配置项，再发一轮带新 commit 的
  AppendEntries，让被摘方本地也提交该配置后才停复制（否则被摘节点留着未提交的
  尾巴，仍信旧配置、可能对幸存者发起竞选；共识层同时在握手后拒绝非成员的共识流量）。
  之后 controller 把它的槽主迁到存活副本、`replan_slots` 补副本。幂等；拒绝摘掉
  最后一个成员（否则集群会把自己配没了）。
- **开局节点 `-join`**：**不是配置作者的节点**（即 id 不等于 `-peers` 最小值、且没有
  `-bootstrap`）以种子身份启动：`-peers` 只当成员地址列表，它向其中之一（或 `-join`
  指定的那个）发 `Adopt` 报名，leader 走上面的加成员流程。因此「把新节点用更大的
  `-peers` 列表拉起来」自动完成扩容，`-join` 仅用于指定报名对象；`-bootstrap` 只用于
  让「非 id 最小」的节点单独充当配置作者。**有 conf 记录的节点永远以记录为准**
  （`-peers` 只当种子，不再拒绝启动），因此扩过容的集群重启不需要改每个成员的
  `-peers`。替代做法：运维直接 POST 加成员，再拉起新节点。
- **`scripts/cluster.sh start` 与二进制同一套判断**：集群已经存在（有节点在跑，或
  数据目录里已有 raft 日志）时，缺的、且没有本地 raft 日志的节点由脚本显式用 `-join`
  指向现有成员启动（冷启动则按 id 最小的节点写配置、其余报名）。**这条区分必须有**：
  首启时让每个节点各写一份初始配置只在「同一次启动、同一份 `-peers`」下才成立，而
  扩容时新节点拿到的 `-peers` 与老成员记录的配置不同 —— 每个新节点会在 index 1 写下
  内容不同的配置项（同 term、同 index、内容不同），集群随之劈成多个各自成多数派、
  彼此都不完整的 raft 组（实测 `REPLICAS=1 → 3 → 5 → 7` 依次 `start` 得到 4 个互不
  隶属的组）。二进制侧的同款判断保证手工启动也走同一条路。
- 节点自报与手动加成员都走 peer 面的 `Adopt` RPC（`PeerService/Adopt`，非
  leader 转发），与 `Register` 同一套 relay 语义。

读语义只暴露 `seq ≤ HW` 的记录：查询不读未达高水位的数据，防止副本回滚后
出现脏读。副本侧这条约束由准入判据兑现：本地作答的副本把水位作为读的可见上界
（`LocalRead` 的 `uptoSeq`），未证明同步前直接问 leader；leader 本地读不受此界
（已确认的记录必须可见）。水位样本经复制面下发，两条通道互补：ReplicaProgress
应答**零滞后回声**——副本上报落盘位置的那次请求，其应答携带 leader 推进后的新
水位，完成 ack 的同一 RTT 内该副本即获证明（acked 数据对副本读立即可见，无轮间
延迟）；MFetch 每轮应答带 HW 账本（sweep 轮全量、普通轮只带水位变化的槽）负责
兜底——重启/新入集的副本、以及多副本集里由**其他席位**的上报推进的水位，在 ≤2s
的 sweep 内补齐。follower 据此记 `hwSeen`（epoch 键控，换代即失效、读自动回退到
重定向直到新一轮证明）。

槽位事件流列表（`/admin/slots/{slot}/streams`）回答的是**槽内索引**：每条事件流的
聚合 id + 最新版本号 + 该最新记录的 `unix_time`（控制台「最近UTC时间」列；
它随版本一起维护在内存目录里，所以取列表**不读任何 WAL 文件**）。索引在开槽时只解析记录的**帧头**（`data.DecodeRecordMeta`：
聚合 id/版本/长度，event body 既不拷贝也不解码；与 follower 帧落盘共用同一套
遍历逻辑），因此取列表**不读任何 WAL 文件**。本节点未打开过的槽返回
`loaded:false` 而不是现开——开槽意味着遍历该槽的全部分段，正是该接口要避免的
全文件扫描；这类问题交给持有该槽的节点（leader/副本，前端按 placement 选
admin 地址）回答。分页用 `?after=<aggregate_id>` 游标顺序游走，一页只材料化
limit 条（有界堆选取，内存 O(limit)，与槽内聚合数无关）；单次响应上限
`MaxStreamPage=1000`，默认 `DefaultStreamPage=200`。

`/admin/slots/{slot}/describe` 返回的是**被询问节点自己的视角**：`hw`/`isr` 只有槽
leader 才有（从副本进度上报里维护，`isr` 列的是**已同步的副本**，不含 leader 自己），
`last_seq`/`segments`/`total_bytes` 来自该节点本地的槽副本。因此响应带
`node`/`role`(leader|replica|none)/`loaded` 三个身份字段，控制台按 placement
**先问 leader、再问副本**（旧实现只问控制台代理到的那个节点，非持有节点自然回
0/空）。该接口**不代开槽**：`Store.Slot()` 会在不持有该槽的节点上建出目录并
遍历其全部分段，只读视图两者都不该做；未加载的槽回 `loaded:false` + 零值。
**槽位列表的总字节/事件流数量走同一套「不开槽」口径**：`GET /admin/writes`
在 durable 计数之外再回两条按槽下标对齐的数组 `bytes[]`/`streams[]`
（已加载槽给真实值，未加载槽直接读 0、不去开它——开槽就是遍历该槽全部分段），
前端沿用已有的 2s 轮询，取「持有该槽的节点中数值最大的那个」作答，不额外发
请求。槽位表列出 `总字节` 一列；事件流数量不进列，改在「事件流」抽屉里看
（接口的两条数组都保留）。

**控制器专属写命令（migrate / remove-replica / plan）必须直接发到 Raft leader 的
admin 地址**：admin 面**不做**任何到 leader 的转发（节点间流量统一走 peer 面），
follower 收到这类命令一律**拒绝**（HTTP 425 + `err_id=1005`，响应里带
`controller`（节点 id）与 `controller_admin_addr`，提示直接发到该 admin 地址）。
前端从已在轮询的 `/admin/cluster/status` 拿 `raft.leader` → `peers[id].admin_addr`
得到目标地址（零额外请求）；若「读表到发出请求」之间 leader 发生切换（响应
1005），**重读一次 status 换新 controller 重试一次**，仍失败则报可读错误
（`withController`，见 `frontend/src/api.ts`）。

**待清理副本口径**：`GET /admin/writes` 的 `dropping[]` 与 `/admin/slots/{slot}/describe`
的 `pending_drop_at` 都按槽下标对齐，`0` = 无待清理；非 0 是**本节点**执行自动清理
（`DropSlot`）的时刻（unix 秒）。两个字段都是**节点自己的视角**（只有持有那份
待清理副本的节点知道），所以前端按节点聚合：`dropping[slot] > 0` 的那个节点，
其副本在「Replicas」列与详情抽屉里置灰（弱化样式用主题 token，light/dark 均可读），
未上报的节点不标记。**置灰完全由后端这两个字段驱动，前端不做任何"谁当过迁移源"
式的推断**；后端也只对**已离开副本集**的多余副本上报，因此仍在副本集里的成员
永远不置灰、也永远不删。

### 6.2 节点级流控（令牌桶）

流控是**节点级旋钮**：`POST /admin/flow-control {"tokens":N,"period":"1s"}`
打到哪个节点，就只对该节点生效——预算也是节点级的：**该节点作为 leader 的所有槽
共享一个令牌桶**，任何一个 leader 槽写入一个事件流都从这一个桶取令牌。控制的粒度
与设置的粒度一致（都在节点层），副本写入走复制落地路径，不消耗令牌、不受控。配置
不进 Raft、不持久化，进程重启回到默认的不限流；这是运维应急面，与副本策略（集群级、
持久）是两个东西。`tokens<=0` 或 `period<=0`（含解析为空）都表示不限流，
`DELETE` 同样恢复不限流。

令牌桶按操作者的模型运转：取走一个令牌，就记下它的归还时间点（取走时刻 + period）；
每次取令牌前先把所有已到期的时间点一次性归还（最小堆扫堆头），桶空即触发流控——
写入报 `fail/1006`（message 固定 `FlowControl`），记录不落 WAL、不等高水位。没有
后台补充线程：堆空意味着令牌全可取，空闲桶零成本；稳态上限 = tokens / period
（整个节点）。改配置换代：旧桶整弃、下一笔写在新预算下重建桶；每槽累计拒绝计数
在换代中保留（前端作差判「是否刚触发」需要单调计数）。**桶归节点，计数归槽**：
一次拒绝发生在具体某个槽的写入上，计数记到那个槽——预算共享不模糊「哪条写入被
拒」的归属，槽级观测面（`flow_control_hits` 数组）因此原样保留，控制台的呈现与
触发判定则统一收在节点层（见下）。

取走的令牌何时退还：落不进 leader WAL 的记录不占预算——业务规则失败（1001 版本
冲突 / 1002 参数非法）与 WAL 写错误都立即归还令牌；EXISTS（同 command_id 幂等
重试）与高水位超时（fail/1005，记录实际已在 leader WAL 落定）**不**退款：exists
同样是「一条写入请求」，按请求计数计费，退款会让客户端的重试风暴免费穿透流控；
1005 的预算确实换到了持久化。批量写入非原子：
逐条向节点桶取令牌，第 k 条取不到则第 k 条及其后所有条目直接 1006（不再尝试落盘），
之前已落地的条目照常合并等待高水位。栅栏/路由/幂等顺序不变：流控检查发生在
localAppend/batchLocal 的落盘之前，与写栅栏、路由表读取同一条 leader 路径。

控制台的呈现只在**集群页节点卡片**一处：橙色「流控」tag = 该节点配置了流控
（configured 即亮，便于巡检谁被治理）；2s 轮询发现该节点的每槽拒绝计数
有增量时，tag **持续闪烁 3s**（每轮新增量都会续期——持续限流就一直闪，末次触发
3s 后熄灭）。卡片上的「流控」按钮打开该节点的设置 modal（预填 GET 回读的配置，
保存 POST / 恢复不限流 DELETE，直发该节点 admin 面）。槽位页与槽详情抽屉不带
流控面：桶是节点级的，「哪个槽被拒」的观测留在数据面（`flow_control_hits`），
UI 只在治理发生的对象——节点——上呈现。数据都不走新请求：per-node
`GET /admin/writes` 轮询顺带 `flow_control`（配置视图）与 `flow_control_hits`
（与 writes 同下标的每槽累计拒绝数，前端按节点作差判触发）。

**客户端库把 1006 吸收为回压**：`pkg/client` 的 `BatchAppend` 从不把 1006 交给调用方。
一次调用的**各目的节点并发发送**（每节点一个 goroutine）：某节点被限流只拖累它自己的
记录，同批里发往其他节点的记录照常单程往返——串行分发会让一个节点的回压窗口把整批
所有节点的写入一起压到 1/窗口倍数（实测 bench --conns 1 --batch 100 下健康节点跟着
掉速）。被拒的记录（同批中被 accepted 的已应答、被 redirect 的照常走重定向自愈）进入回退环：
持有该节点的**闸门**（每节点一个，首个 1006 创建），每 `Config.FlowBackoff`（默认
100ms）只重发被拒子集，直到一轮应答不再含 1006 才放闸；闸门关闭期间，发往该节点的
后续写请求全部在闸门外排队（读不进闸门：副本读不取令牌、永远不会收到 1006）。排队
等待发生在 Attempts 计圈之外——限流是等待，不是重试。回发安全靠 command_id 幂等兜底：
落在拒判之前的记录下一轮答 EXISTS，不会双份。持闸者 ctx 截止则放闸并把记录降级回
普通重发队列，绝不把节点永久关闸。观测面 `Client.FlowPauses()` 数被拒的回发轮次。
活簇冒烟：`cmd/flowcheck`（节点 2 tokens/1s + 8 并发写 160 流）——2516 次节点拒绝
全部被库吸收，调用方零 1006、零失败。

## 7. 高性能要点

- **command 幂等索引是定长哈希表，不是字符串 map**：每槽的「command_id → seq」索引
  在大节点上是**每条记录一项**（30GiB 量级 = 数千万项），字符串 key 的 map 会把
  id 本体、bucket、GC 元数据一起常驻（实测 31GiB/3250 万条：堆 4.4GiB，绝大部分
  来自这一项）。现在 `cmdTable` 是两条扁平 `uint64` 数组（hash→seq，16B/项、
  装载因子 7/10、按需翻倍重哈希），**不存 id 字符串**：命中后读该 seq 的记录、
  比对真实 command_id 才认（EXISTS 回显本来就要读这条记录，by-command 查询也是），
  所以哈希碰撞只会加长探测链、不会误判；帧头元数据（`RecordMeta`）因此只带
  `CommandHash`（FNV-1a，`data.HashCommandID`/`HashCommandBytes`），复制落盘路径
  需要精确比对时用 `data.CommandIDBytes(frame)` 从帧里取 id（这是罕见的重放路径）。
  同一份 31GiB 数据：堆 inuse 3.7~4.3GiB → **1.42GiB**，`OpenStore` 38.7~80.9s →
  **20.0~26.0s**，节点启动到可服务 29.3s → **18.6s**，稳态 RSS 3.88 → **2.08GiB**。
- **段索引文件（Phase 2，已落地）**：每段三个**追加式**派生文件（与 WAL 同目录同名前缀，
  `internal/storage/segidx.go`）：`<baseSeq>.idx`（header 32B + 块 `crc32c(4) + n×28B`，
  entry = command hash(8) seqOff(4) dictID(4) version(4) unix_time(8)）、`<baseSeq>.agx`（段内聚合字典，
  新增聚合即 sync）、`<baseSeq>.spx`（稀疏 seq→pos，16B/条）。
  **不变式**：索引是派生数据——只加载 CRC 验证通过的最长前缀，其余从 WAL 重放；
  缺失、损坏、滞后一律退化为全量帧头扫描（即索引出现之前的行为），因此正确性
  不依赖索引。恢复时用 `.spx` 的最后位置续走到文件尾（不超过一个稀疏间隔）
  定出段长与条数，entry 数用 WAL 实际条数封顶（截断过的段不会凭空多出记录）。
  **策略**：`OpenStore` 启动时把「缺失/损坏/滞后」的段补齐索引（`OpenSlotRepairing`）；
  运行期（`ReloadSlot`、懒加载 `Slot()`、迁移推来的段）只加载、绝不补建，缺索引的段
  留给下次启动——即「启动补建、运行期维持现状」。**代价**：索引约 28B/条 ≈ 数据的
  **3.1%**（31GiB → 1.0GB；下述实测数值是在 20B 条目时测得，28B 只加宽条目、不改机制）。实测同一 31GiB 数据集（补建前清空索引）：冷启动 27.6s
  （遍历 + 补建，堆 1353MiB/sys 1978MiB）→ 热启动 **9.7s**（堆 1353MiB 不变）；
  两次启动逐槽 `last_seq == Σversion` 均精确。热启动剩余开销主要是把 3250 万条
  索引载入内存结构（Phase 3 的目标）。
- **聚合索引：一份目录 + 定长分块 arena**（取代原来的「`map[string]uint32` 版本表 +
  `map[string][]uint64` seq 表」两张表）：现在每槽一张 `aggs map[string]aggEntry`
  （版本 + 该聚合在 arena 里的 (off,n)）加一块 `seqChunks [][]uint64`（每块 1024 条
  = 8KiB，`i>>10`/`i&1023` 定位，append O(1)），**浪费上限是每槽一块**，而不是
  每个聚合一份 slice 头 + 至多 2× 容量。实测：20 万聚合 × 5 版本（事件溯源常态）
  **30.9 → 16.6 MiB（1.86×）**；31GiB「每槽 8 个大聚合」形态活堆 1368 → 1353 MiB
  （−1%）、`sys` 2050+ → 1930 MiB（−6%）——该形态的内存已被 command 索引主导，
  聚合索引占比小。**踩坑**：第一版用几何分块（256/512/1024…），对「少而大的聚合」
  每个聚合平均空置近一半容量，实测比原实现还高 20~45%，改定长块后才是净收益。

- **顺序追加 + 页缓存**：写路径 = memcpy 进段缓冲 + write()；fsync 交给
  flush 策略与复制确认语义（leader 等 ISR 高水位覆盖该 seq 才回 success），
  组提交合并（dirty 集合定向刷盘，不遍历全槽）。
- **二进制协议面**：节点间流量全部 PeerService gRPC（protobuf 二进制帧，
  `FetchItem.payload`（以及 `ReplicateRequest` 的裸记录）为裸 WAL 字节区间，不经
  base64/重编码）；客户端事件面 gRPC protobuf，body 原始 bytes。
  路由表快照同为二进制编码（字符串字典 + varint + gzip）。
  **sendfile 零拷贝已实测否决**：payload 约 40-64KiB、需用户态解码过滤、
  Go netpoller 使用非阻塞 fd——三个收益条件全都不满足；syscall 优化因此走
  「减少往返」（多槽复用会话、长轮询、批量 LEO 上报、组提交），而不是减少拷贝。
- **稀疏索引常驻**：内存索引 = 1024 槽 ×（聚合版本 map + command map +
  seq 稀疏索引），索引成本每记录几十字节，可支撑千万级聚合（亿级聚合由
  磁盘侧 `.aidx`/`.cidx` 承载，内存只留 O(聚合目录)+热尾，见 §7.2.2 的
  聚合基数上限）。
- **写不放大**：一条记录只在槽 WAL 落一次；副本走日志拉取而非双写。
- **锁竞争**：slot 级锁把热点聚合关进单槽（热点从"锁全库"降为"锁
  1/1024"）；读路径短临界区 RWMutex。
- **启动恢复并行化**：OpenStore 两阶段（先扫目录再 worker 池并行开槽）；每个分段的
  恢复是**一次窗口读（1MiB `frameWalker`）的帧头遍历**——同一次遍历里找尾部完整边界
  （撕裂尾截断）并重建稀疏索引，随后 `Segment.ScanHeaders`+`data.DecodeRecordMeta`
  再按帧头重建聚合/command 索引（body 既不拷贝也不解码）。**不要退回「每条记录一次
  pread」**：31GiB / 3250 万记录下，旧实现的 `LoadSegment`（每记录 1 次 4 字节 pread
  找边界）+ `ScanFrom`（每记录 2 次 pread 解整帧）≈ 9700 万次 syscall，占启动 CPU 的
  45%（实测 OpenStore 96.0s → 36.5s，节点启动到可服务 92.9s → 29.3s）。

## 8. 配置默认值

```
slot_count      = 1024         # 固定不可配置（2^10；按最多 256 个数据节点 × 每节点 4 槽估算，见 §7.2）
segment_bytes   = 67108864     # 默认 64MiB；须为 64MiB 的整数倍，最小 64MiB、最大 2GiB
                              # -segment-bytes / PUSHUPES_SEGMENT_BYTES 可写字节数或带单位
                              # （256MiB / 1GiB / 268435456）
replica_policy  = medium       # 副本策略：low=每槽 1 份 / medium=2 份（默认）/ high=每个数据节点一份全量
                              # -replica-policy / PUSHUPES_REPLICA_POLICY 只为新集群种初始值；
                              # 策略存在复制的槽表里，之后的修改走 POST /admin/cluster/replica-policy
replica_count   = 2                # = ReplicaCountForPolicy(策略, 数据节点数)，控制器每轮重推
election_mode   = leader       # preferred leader 自动回切
flush.policy    = 每 1000 条或 5s（可关闭为纯页缓存）
                              # -flush-messages / PUSHUPES_FLUSH_MESSAGES（每 N 条）、
                              # -flush-interval / PUSHUPES_FLUSH_INTERVAL（每周期），0 关闭
fetch-settle    = 200µs（fetch 轮被数据唤醒后的合并窗口：一轮覆盖整批写入涉及的多个槽；
                  每次 ack 都会付它一次，换的是轮次与上报次数。实测 3 节点 1KiB/conns 4/batch 100
                  的镜像三档（2ms/200µs/0）：hw_wait 均值 5.70/4.60/6.42ms、每轮槽数 37.2/29.4/27.2
                  ⇒ 不合并（0）最差、2ms 又过长，取 200µs）
                              # -fetch-settle / PUSHUPES_FETCH_SETTLE
drop_after      = 30s          # 迁移后前源节点本地副本的保留期，到期自动 DropSlot
                              # -drop-after / PUSHUPES_DROP_AFTER：正数=该时长，0=默认 30s，
                              # 负数启动即报错；没有关闭选项（留着会把节点写满）
raft_heartbeat_timeout = 100ms # -raft-heartbeat-timeout / PUSHUPES_RAFT_HEARTBEAT_TIMEOUT
                              # （启动脚本里 RAFT_HEARTBEAT_TIMEOUT）：leader 发心跳的间隔
raft_election_timeout  = 500ms # -raft-election-timeout / PUSHUPES_RAFT_ELECTION_TIMEOUT
                              # （启动脚本里 RAFT_ELECTION_TIMEOUT）：follower 多久收不到心跳
                              # 就发起选举。必须 ≥ 2× 心跳，否则一次迟到的心跳就掀掉 leader
                              # （节点启动即报错）。两个值是一对比例：共识事件循环被慢 apply
                              # 或紧张的 CPU 拖住时把它们一起放大（例 500ms / 3s），
                              # 而不是只收窄其中一个
admin_addr      = http://127.0.0.1:8091   # -admin / PUSHUPES_ADMIN（含 pprof）
client_addr     = http://127.0.0.1:8591   # -client / PUSHUPES_CLIENT（gRPC）
peer_addr       = http://127.0.0.1:8391   # -peer / PUSHUPES_PEER（Raft + 注册）

# 地址规范：缺协议默认 http://，显式协议以传入为准；TCP 层使用前剥 scheme
```

`-peers` 只要求可达的 peer 地址（`node-id=http://host:peerport`），admin/client
不需要配置——节点启动后经注册协议自报（见 §6）。

## 9. 包结构

```
pushupes/
├── DESIGN.md
├── go.mod                     # module github.com/berkaroad/pushupes
├── cmd/pushupes/              # 入口：装配 storage + cluster + api（admin）+ grpcapi（client）
├── pkg/client/                # 客户端契约：槽算法 SlotOf（slot.go）+ 错误 ID 表（errs.go）。
│                              #   服务端代码引用同一实现（internal/data 只做命名别名）；
│                              #   仓库内的客户端工具（bench/batchsmoke/grpccheck/seed/slotcheck）
│                              #   只允许 import pkg/ 下的包（契约守门 scripts/check-client-imports.sh），
│                              #   槽数不在契约内——从 PrefetchRoutes 应答长度读取
├── internal/data/             # 领域类型、WAL 编解码、槽路由哈希（委托 pkg/client）、错误码（引用 pkg/client）
├── internal/storage/          # WalSegment / Slot(WAL) / Store：
│   │                          #   追加、幂等/版本校验、恢复、字节区间读
│   ├── segment.go  slot.go  store.go
├── internal/cluster/          # 控制面 cluster.go+fsm.go（Raft/分配表/路由表二进制编码
│   │                          #   table_bin.go），数据面 replication.go（PeerService 会话/ISR/HW）
│   │                          #   + peersvc.go（PeerService gRPC 服务端/客户端）+ peer.proto 生成码，
│   │                          #   peer_mux.go（peer 端口首字节分流 Raft/gRPC），
│   │                          #   migration.go（六步热迁移），register.go（地址自报 announcer+RPC）
├── internal/api/              # handler.go（仅管理：status/writes/plan/migrate/describe/pprof），server.go
├── internal/grpcapi/          # gRPC 客户端数据面：server.go（BatchAppend/ReadStream/ReadTails/ReadByCommand/ReadVersionByTime/PrefetchRoutes）
└── proto/pushupes/v1/         # events.proto 客户端契约（生成至 pkg/grpcapi）+ peer.proto 节点间契约（生成至 internal/grpcapi；含 buf 工具链）
```

## 10. 设计要点小结

| 维度 | PushupES 选型 |
|------|------------|
| 基本单元 | 固定 1024 Slot（哈希分区） |
| 路由 | mix64(FNV-1a(aggregate_id)) % 1024 |
| 存储格式 | 自定义 Record 二进制（append-only WAL，段名即 baseSeq） |
| 分段/索引 | 64MiB 段 + 64KiB 稀疏索引（常驻 seek 提示） |
| 共识层 | Raft 只复制元数据：slot 分配表/epoch/节点地址注册 |
| 客户端协议 | gRPC EventService（事件读写唯一入口） |
| 数据复制 | PeerService.MFetch 会话（gRPC/HTTP2 与 Raft 同端口，seq 坐标，多槽复用长轮询，有数据即答） |
| 可靠性语义 | ISR + HW（success 的条件是高水位覆盖该 seq，单点故障不丢已确认写入） |
| 再平衡 | 槽位热迁移（快照+增量+转发窗口，六步不停写）；leader 回切：controller 定期把偏离环布局的槽经热迁移迁回预期 leader（摘要等价即跳过快照） |
| 幂等 | command_id 槽内索引 |

## 7.1 内存索引：记录条数驱动的三档收敛（Phase 3）

启动恢复与索引文件格式见 §3、§7；这里只记内存口径。基准数据集为 31 GiB /
3250 万条 1 KiB 记录，每轮测量都在 `runtime.GC()` 之后读 `MemStats.HeapAlloc`
（不先 GC 读到的是上次 GC 时的堆，同一份数据两轮能差 30%）。

| 阶段 | 做法 | HeapAlloc | 占比 |
| --- | --- | --- | --- |
| 起点（方案 A 后） | command 表 32 B/条 + 共享 arena 8 B/条 + 稀疏索引 4 KiB/条 | 1353.5 MiB | 100% |
| 3.2 | command 表换成布隆（2.5 B/条）+ 封段 `.cidx` 冷查 | 439.0 MiB | 32% |
| 3.4 | 段稀疏索引 4 KiB → 1 MiB（纯 seek 提示：最多多走 1 MiB 帧） | 327.4 MiB | 24% |
| 3.3 | 已封段 seq 移出内存：封段写 `.aidx`，内存只留正在写的段 | 140.3 MiB | 10% |
| 3.4b | 段稀疏索引 1 MiB → 64 KiB（每次 fetch 的窗口读 ≤64 KiB） | ≈148 MiB（推算） | 11% |

3.2：内存只需要「这个 command 是否可能存在」的廉价答案，精确答案交给读盘
（重复的 command 意味着客户端重放，不是热路径）。过滤器只在确定「不存在」时
跳过磁盘；未加载或标记 unsafe 时保守地答「可能存在」——索引缺失只会变慢，
绝不会出错。

3.3 的 `.aidx` 目前是 **v5**：目录项 `id hash(8)/firstVer(4)/count(4)/firstOrd(4)/listOff(4)`，
列表每条记录 **16B =（段内序数, 段内字节偏移, unix_time）**。序数必须逐条存：同一段里某聚合
的记录与其他聚合交错出现，序数并不连续，用 `BaseSeq+firstOrd+i` 推算会指向
其他聚合的记录。有了偏移，「按版本读已封段」变成**一次 `readRecordAt` 直达**，
不再依赖稀疏提示走帧（当时的提示间隔为 1 MiB，约一千帧）。31GiB / 13.1 万个版本的聚合全量读
实测：**1.073 ms/版本 → 0.200 ms/版本**（5.4 倍），逐条核对 `AggregateID/Version`
通过。偏移的来源是封段时本来就要走的那一遍帧（`collectSegCmdEntries`，
同时供 `.cidx` 使用）。布局变了格式版本必须同步升级：CRC 只能发现坏字节，
发现不了「字段写反」这类内容错误。

3.3 的另一处改动：**每个聚合持有自己的链式 seq 列表**（首块 16、64、256，
其后 1024），取代「共享 arena + 每聚合一个区间」。共享 arena 里各聚合是交错的
（`arena = [A1 B1 A2 B2 …]`），「该聚合的 seq 占 arena 的一段连续区间」只在槽内
基本被单个聚合连续写时才成立；两个聚合一交错，按版本取下标的 `arena[off+i]`
就会取到另一个聚合的记录。封段时把该段的 seq 按聚合写入 `<baseSeq>.aidx`
（目录项 `聚合 hash(8) + firstVer(4) + count(4) + listOff(4)`，按 hash 二分，
命中的候选必须读记录核对真实 `AggregateID`），**写成功之后**才从内存丢弃，
内存里只留正在写的段的 seq。启动时：索引缺失或损坏就重建，索引有效就直接
丢弃内存 seq。`.aidx` 的格式版本在布局改正时同步 +1——CRC 校验只能发现
坏字节，发现不了「值本身是错的 seq」，旧文件必须被判无效重写，不能信任。

读路径：版本 `> sealedN` 走内存（热路径，与之前同速）；其余走「从新到旧逐段
`.aidx` 二分定位 + 读那段列表 + 按 seq 读记录」，每段 1~2 次 pread。

列表里的 unix_time 是**时间锚点查询**（`ReadVersionByTime` →
`VersionAtOrBeforeTime`）的数据来源：活版本的时间戳来自内存目录（每条记录
8B，与 seq 同几何），封段版本来自 `.aidx` 列表，因此「某时刻的版本锚点」这一
查询**不读任何 WAL 帧**——只有最终命中的封段版本读一次记录，用于核对真实
`AggregateID/Version`（列表按聚合 hash 组织，碰撞项不能作答）。同一条 unix_time
也供 `/admin/slots/{slot}/streams` 的「最近UTC时间」列使用，列表因此维持零 WAL 读。

3.4b：间隔从 1 MiB 再收到 64 KiB。稀疏间隔是「一次定位最多走多少帧」与「这份
表占多少内存」的同一个旋钮，只改走帧量、不改答案：1 TiB/节点 的内存代价
238 MiB（实测定律 16 B/间隔），换来的是副本拉取读路径每次窗口读从 ≤1 MiB
降到 ≤64 KiB。

## 7.2 容量规划：槽数（SlotCount）与段大小（segment-bytes）的选型判据

以下常数与规则全部来自实测（31 GiB / 3250 万条 1 KiB 记录的加载与内存量测，
以及用仓库真实路由函数 `data.SlotOf` / `cluster.PlanSlots` 跑的离散度测量），
不是估算值。

### 7.2.1 每记录内存常数（决定 1 TiB / 10 亿条量级能否落在 16 GiB）

    布隆过滤器     1.25 B/条   （10 bit/条，k=3）— 只与「本节点记录条数」有关，与槽数、段大小都无关
    未封段 seq 列表 8 B/条      — 只算「未封段」里的条数，∝ 槽数 × 段大小
    稀疏索引       16 B/64 KiB  — = 16 B × 数据量 / 稀疏间隔，与段大小无关
    加载期峰值     ≈ 2.8 × 活堆 （实测比）；加载期分配 ≈ 2.5 GiB / 3250 万条（已含扁平缓冲 + 池化）

活堆 ≈ 1.25·N_节点 + 8·(未封槽数·段大小/2)/记录大小 + 16 B·(数据量/64 KiB) + 0.15 GiB

稀疏索引那一项的常数取自另一次独立量测（6 GiB WAL / 874 段 / 1 KiB 记录，
间隔取 16 MiB、1 MiB、64 KiB 三点）：条目数按 `数据量 / 间隔` 线性、每条目
16 B、装载后 `cap == len` 不浪费；据此 1 TiB/节点 在该项上占 16 MiB
（间隔 1 MiB）与 253 MiB（间隔 64 KiB）。

因此 **内存主体由「记录条数」驱动**：同样 1 TiB，1 KiB 记录（10.7 亿条）比
10 KiB 记录（1.07 亿条）紧张一个数量级。段大小只影响「未封段 seq」一项，
稀疏索引由提示间隔决定，**布隆过滤器那份固定开销两者都削减不了**。

### 7.2.2 单节点失效时未封槽数 = 2K/N（与 K 无关）

`PlanSlots` 的放置规则是：leader = `nodes[s%N]`，副本 = `nodes[(s+i)%N]`（向前取 rf 个）。
撤销一个节点时它的每个槽的 leader 落到 `Replicas[0]`，即**死节点的全部槽只落到它的
下一个邻居**，其余节点负载不变。实测 3/5/7 节点、RF=2：失效后 max/mean 分别为
**1.334 / 1.600 / 1.717**，且对被测的所有 K 完全一致。也就是说：

- 规划内存必须按 **2K/N 个未封槽**计算（不是 K/N，也不是 K/(N-1)）；
- **调整槽数 K 无法缓解失效倾斜**，只有提高 RF 或改变副本环布局才能打散。

### 7.2.3 N | K 的不变性：数据节点数为偶数，K 取 2 的幂

实测：`(hash(id) % K) % N == hash(id) % N` **当且仅当 N 整除 K**（`hash` 指槽
路由哈希，验证时为 crc16；对全部 1e6 条 id、四种 id 形态验证，0 差异；
K=4096/2048/1024 则对 3/5/7 全部
不成立，K=1536/3072 只对 N=3 成立——因为 4096≡1 (mod 3,5,7)）。

含义：控制面与数据面分家后，布局只按数据节点数摊，数据节点数取**偶数**、
目标上限 256。K 取 2 的幂（`1024 = 2^10`）时，2~256 之间的任何 2 的幂次
节点数都整除 K，「聚合落在哪个节点」是 `hash(id) % N` 的纯函数，每节点的
槽主数精确等于 K/N；其余偶数节点数由环布局摊平，槽主数相差不超过 1。
注意：**节点级均衡本身与 K 无关**（所有被测 K 的 max/mean ≤ 1.008），
选 K 的判据是「全规模下每节点的槽数与迁移粒度」。

### 7.2.4 槽位分布均匀性：CRC16 对结构化 id 非均匀（已改为混合哈希）

**算法归属**：`SlotOf` 的实现放在 `pkg/client/slot.go`——槽算法是客户端
契约的一部分（客户端必须自行算出聚合落哪个槽，才能按 `PrefetchRoutes`
的路由行直接写 leader），服务端代码引用同一实现（`internal/data.SlotOf`
只做委托，委托一致性有钉测）。**总槽数不在契约内**：客户端代码不硬编码
它，从 `PrefetchRoutes` 应答的数组长度读取；`data.DefaultSlotCount` 只是
服务端建库默认值。算法改动 = 客户端兼容性事故，所以 `pkg/client` 带金标
向量测试（另一语言独立实现算出），任何漂移都会先撞上它。

实测 CRC16/XMODEM 的 16 位原始直方图：随机 UUID `chi2/df = 1.001`，而
零填充顺序号（`agg-%07d`、`tenant-42-%07d`）为 **1.606** —— 哈希本身对结构化 id
就存在偏斜，与槽数无关。落到槽上，偏斜程度随 K 增大而加剧（顺序 id）：

    K=840   chi2/df 1.03~1.16（槽 max/mean ≤1.10）— 最好
    K=4096  chi2/df 1.26        K=2048 1.54
    K=6720  chi2/df 2.04~2.06   K=3360 2.37~2.50   K=1680 2.46~2.54
    K=1536  chi2/df 2.56 — 最差
    （幅度温和：最重的槽比均值大 44%，空槽数恒为 0）

**已实施**：`SlotOf` 改为 `mix64(FNV-1a(id)) % K`（`mix64` 是 splitmix64 终混，
双射、雪崩性充分；对 16 位的 CRC 结果再做混合是无效的——双射不改变桶计数，
所以混合必须发生在收窄之前）。改后实测（同一 harness、1e6 条 × 4 形态）：
**16 位原始直方图 χ²/df 1.606 → 0.998**（结构化 id），各 K 下的槽级 χ²/df 全部
回到 0.92~1.05（改前 K=1680 为 2.46~2.54、K=1536 为 2.56），槽 max/mean ≤1.23、
空槽恒为 0——「小 K 才均匀、大 K 才细粒度」的两难就此消失。`SlotOf` 的取模
改在 `uint64` 上做，原先 65535 的槽数上限一并解除。**改路由会让既有数据目录里
的记录落到别的槽，开发阶段直接换新数据目录即可（本仓明确不考虑迁移）。**

### 7.2.5 段大小还与迁移粒度耦合

槽是热迁移的单位（§5），所以「每槽数据量 = 每节点数据 / 每节点槽数」就是一次
迁移的搬运量。每节点槽数 = K/N：K=1024 在 2 个数据节点时每节点 512 槽（迁移
单位细），到 256 个数据节点时每节点 4 槽、单个槽承载一个节点数据量的 1/4
——这正是按「至少 4 槽/节点」给 K 定标的含义：规模越大，K 必须越大才撑得住
细粒度迁移；反过来，小拓扑要控制失效态内存（§7.2.2 的 2K/N）。
1KiB 记录、1TiB 单节点的形态下每槽承载 ≈1GiB，一次热迁移就是 1GiB 级的搬运
量（128MiB 段 ×8）；该形态可接受，历史段合并功能后续再压低段数成本。

### 7.2.6 推荐取值（按数据面上限定标）

**K = 1024 = 2^10 = 256 个数据节点 × 每节点 4 槽**（数据节点数取偶数）。
2~256 之间的一切 2 的幂次节点数整除 1024，每节点槽主数精确等于 K/N（§7.2.3）；
每节点至少 4 个槽，保证迁移粒度在全规模下仍然可用。段大小默认
`storage.DefaultSegmentBytes = 64MiB`（经 `-segment-bytes` 可配，64MiB 的整数
倍至 2GiB）——定标场景是「单数据节点 4 核/16GiB/1TiB、记录 1~4KiB」：1KiB 记录
时未封段 seq 项 = 8B × 热槽 × S/2 ÷ 记录 ≈ 0.25GiB（全 1024 槽皆热），加布隆
1.34GiB + 稀疏索引 0.27GiB + 杂项，活堆 ≈2.0GiB、峰值 ≈5.7GiB ✓（聚合 ~1 千万
再 +2GiB，仍在 16GiB 预算内）。小段的代价是段数（16384）、句柄与派生索引文件，
留给后续的历史段合并回收；记录 ≥4KiB 时同一内存下段可放宽到 256MiB。

下面的表是**历史实测**（控制/数据合置的 3 节点最坏情形，RF=2，1 KiB 记录，
要求扛住单节点失效；当时槽数 1680）：

    K     段      活堆    峰值     段数/节点  槽/节点(N=7)  每槽迁移量(1KiB/10KiB)
    840   256M    1.49G   4.16G    2543      120          2.3G / 23G
    840   512M    2.03G   5.68G    1272      120          同上
    1680  256M    2.04G   5.70G    2543      240          1.2G / 11.6G
    1680  512M    3.12G   8.75G    1272      240          同上
    3360  256M    3.14G   8.78G    2543      480          0.58G / 5.8G
    4096  128M    2.29G   6.42G    5086      585          0.48G / 4.8G

从表口径可直接推出 K=1024 的两条推论：**未封槽内存随 2K/N 涨**（§7.2.2），
正式上线前要在实际拓扑复测该两项；**每节点至少 4 槽是布局与迁移的下限**——
K=1024 在 256 个数据节点时恰好用满这一下限，扩节点到超过 K/4 之前必须再动 K。
表内数字为历史实测/外推（当时槽数 1680/4096），新口径 (1024, 64M) 的单节点
1KiB 行：未封 ≈0.25G、布隆 ≈1.3G、段数 ≈16384、每槽迁移量 ≈1G。

- **槽数固定为 `data.DefaultSlotCount = 1024`（无 `-slots`/配置入口）**。
- 5/7 节点同参数下峰值只有 3 节点的约 60% / 45%（历史实测），**节点数越少，
  失效态越重**，按小规模拓扑规划内存即可覆盖所有规模。

---

## 7.3 全局单线程刷盘：实现与实测

机制见 §3：fsync 移出槽写锁，全存储一条段级队列串行执行，写路径只在队尾追加
一个段项；显式 `Flush()`/`Close` 拿刷盘互斥锁在调用方线程内完成，因此任何路径
都不必等刷盘 goroutine（这条性质由 `TestFlusherCloseIsIdempotent` 固定：第二次
`Close()` 必须立刻返回）。

测量口径：`cmd/bench --duration 1m --size 1024 --conns 4 --aggs 10000 --batch 100`。
**每轮重建数据目录**（1m 级一轮写入 GB 量级，设备回写只有几 MB/s，连跑会测到上
一轮的脏页），**每轮记录设备探针**（4K pwrite+fdatasync 写/s），每轮开测前断言
集群成员数与标签一致。前后各 4 轮交错（B A A B B A A B）。

单节点（`bench1`，7191，REPLICAS=1）：

| 轮 | 吞吐 msg/s | p50 | p99 | 设备探针 |
|---|---|---|---|---|
| before1 | 58316 | 5.60ms | 20.02ms | 1987 |
| before2 | 77108 | 4.16ms | 15.62ms | 318 |
| before3 | 73103 | 4.25ms | 17.68ms | 302 |
| before4 | 78155 | 4.15ms | 15.48ms | 305 |
| after1 | 95483 | 3.38ms | 11.57ms | 1458 |
| after2 | 97962 | 3.25ms | 15.85ms | 1533 |
| after3 | 99435 | 3.12ms | 14.78ms | 1348 |
| after4 | 93233 | 3.00ms | 17.68ms | 318 |

均值 71670 → **96528 msg/s（+34.7%）**，p50 4.54 → **3.19ms（−29.8%）**，
p99 17.20 → **14.97ms（−13.0%）**；4 个 after 轮全部高于 4 个 before 轮，
且设备最差的 after 轮（318 写/s）仍高于设备最好的 before 轮（1987 写/s），
说明差值不是设备侧波动给的。p99 ≤ 30ms 目标两版均达标（各 4/4）。

3 节点（`bench3`，7091，REPLICAS=3）：

| 轮 | 吞吐 msg/s | p50 | p99 | 设备探针 |
|---|---|---|---|---|
| before1 | 14476 | 24.46ms | 91.20ms | 1478 |
| before2 | 16368 | 21.82ms | 77.55ms | 1434 |
| before3 | 16523 | 21.87ms | 69.82ms | 1400 |
| before4 | 17249 | 21.49ms | 55.76ms | 1664 |
| after1 | 17096 | 21.58ms | 56.82ms | 1643 |
| after2 | 20375 | 17.24ms | 52.48ms | 1386 |
| after3 | 20238 | 17.31ms | 69.88ms | 1386 |
| after4 | 20722 | 17.18ms | 48.07ms | 1290 |

均值 16154 → **19608 msg/s（+23.5%）**，p50 22.41 → **18.33ms（−18.2%）**，
p99 73.58 → **56.81ms（−22.8%）**；after 侧设备探针均值（1426 写/s）不高于
before 侧（1494 写/s）。3 节点的 ack 延迟由复制那一跳（副本取数 → apply →
上报 → 水位推进）主导，刷盘只占其中一小段，所以收益小于单节点；
**p99 ≤ 30ms 在两版都不达标**（48~70ms），达标要另做复制跳的延迟专项。

**没有变化的东西**：协议、存储格式（新增仅 `Segment.syncedSize` 内存字段）、
`-flush-interval`/`IntervalMessages` 的触发语义、`cmd/bench`、写入确认语义
（leader 等 ISR 高水位覆盖该 seq）都未改动；Raft WAL 的 fsync 未并入本次改造；
段索引文件（`segidx`/`segcidx`/`segaidx`）在封段与关闭时各一次 fsync 的频率
≈ 段滚动，仍未并入。

## 7.4 ack 链路自视（排查「写入慢在哪一步」）

写入被确认的条件是「槽的高水位覆盖该条」（§4），所以一次调用的时延就是四段之和：
取迁移写栅栏并判路由 → 本地 WAL 落地 → 等 ISR 高水位 → 其余（组装响应、调用方调度）。
`GET /admin/stats` 的 `ack` 段把每段单独计时（每调用 4 次 `time.Now()` + 若干原子计数，
固定桶直方图，快照无锁无分配），`?worst=N` 按「等高水位」的 p99 列出最差的槽。

3 节点、`--duration 20s --size 1024 --conns 4 --aggs 10000 --batch 100` 的实测样本：

```
appends=102882 batches=99855 success=102161 fail=0 (timeout=0 version=0 other=0) redirects=331
fence_route p50 0.05ms  p99 0.05ms  max   5.99ms
wal_land    p50 0.05ms  p99    5ms  max  31.88ms
hw_wait     p50    5ms  p99   50ms  max 120.14ms
total       p50    5ms  p99   50ms  max 122.67ms
waiters_now=0  hw_stalls=0  水位落后(条) p50=0 p99=1 max=3
```

读法：`total` ≈ `hw_wait`、而 `fence_route`/`wal_land` 在亚毫秒 ⇒ 这条链路的时延几乎
全在「等副本位置推进高水位」上，本地落盘不是瓶颈；`水位落后` 只有个位数说明高水位
贴着日志走，副本没有积压；`waiters_now` 与 `hw_stalls` 为 0 说明此刻没有调用被拖住、
也没有撞过 10s 截止。反过来，如果 p99 高而 `hw_wait` 低，嫌疑就在客户端/重定向；
如果 `hw_stalls` 上升，看 `repl` 段的每副本上报新鲜度与 LEO 落后。

`hw_wait` 还可以再拆：`report_rtt`（副本上报自己新位置的往返，副本侧测）、
`progress_handle`（leader 处理该上报到水位推进的耗时）、`wake_lag`（水位推进到被唤醒的调用真正
返回之间的时间）——三者都很小 ⇒ 嫌疑就在剩下的两段，现已各自有表：`fetch_serve`（leader 侧：
唤醒到应答组装完——合并窗口、parked 槽复扫与 burst 各槽的 payload 读；实测 avg 0.8ms，其中
0.2ms 是 fetch-settle 本身）、`replica_apply`（follower 侧：一轮 items 全部落进本地 WAL 的
墙钟时间；实测 avg 0.26ms）。健康环境（4 核、无宿主干扰）下五段相加 ≈ hw_wait 均值
（2.4-2.6ms ≈ 0.85 + 0.60 + 0.27 + 0.07 + 0.08 + 两段本机 wire/调度 ~0.5）——差距的每一微秒
都有名字。两条已实测关闭的杠杆：并行化 leader 的 burst 读无收益（fetch_serve 是 CPU-bound
memcpy，不是 syscall 等待：ABBA 并行版 fetch_serve 0.81ms vs 串行 0.82ms、吞吐 -1.9%）；
`report_rtt` 0.6ms 是 localhost gRPC 往返的地板价，无进一步可压缩空间。

`fail` 进一步按原因拆分（`fail_hw_timeout` / `fail_version_conflict` / `fail_other`），
因为「有写入失败」只有分清「复制没跟上」还是「客户端请求本身不合规」才可行动：实测
那 186 例失败全部是 `version_conflict`（业务规则 1001），`fail_hw_timeout` 为 0。

## 11. 控制面共识层（自研 Raft 设计）

> 状态：**已实现并验收**（见 §11.10）。
> 范围：控制面**共识层**（选主 / 日志复制 / 提交推进 / 快照）自研实现。
> 业务状态机（槽位分配表 `Table`）、数据面复制（mfetch / ISR / HW）、六步热迁移
> 全部不动，接口边界也不动（`Applier`、`Node` 的导出方法、FSM 语义）。

### 11.1 定位与目标

#### 11.1.1 共识层承担的能力

| 能力 | 提供模块 |
| --- | --- |
| 节点状态机（Follower/Candidate/Leader）、随机选主、心跳、term | `internal/raft`（state.go） |
| 日志复制（AppendEntries 一致性检查、冲突截断、prevLogTerm/commitIndex） | `internal/raft`（replicate.go / log.go） |
| 提交推进（多数派 matchIndex → commitIndex） | `internal/raft`（state.go） |
| 成员变更（初始配置 / 运行时加删 voter） | `internal/raft`（raft.go / log.go） |
| 日志与稳定状态存储（term/vote） | 自研分段 WAL（`internal/raft/wal.go`） |
| 快照存储 | 自研单文件快照（`internal/raft/snapshot.go`） |
| 传输 | 自研 TCP（`internal/raft/transport.go`），跑在 `peerMux` 之上，与 peer gRPC 共用一个端口 |

Raft 的 API 面很窄，只有 4 个文件与它打交道（`cluster.go`、`fsm.go`、
`peer_mux.go`、`register.go` 一行 + `migration.go` 一处 Stats）+ 3 个测试文件，
所以业务逻辑和共识实现能干净地隔开。

#### 11.1.2 目标

1. 共识层做成**独立自包含的包**：`internal/raft` 不认识业务，只通过 `FSM`
   接口与装配层交互。
2. Raft 自己的日志与稳定状态（term/vote）用**自研 WAL 文件**存（分段、CRC、
   崩溃可恢复）。
3. 对外接口面小而稳：`Node.Apply / IsLeader / LeaderID / Peers /
   PeerAddrs / Stats / Close`；peer 端口一个端口同时承载共识与 peer gRPC；
   `-peers` 是唯一的成员配置入口。

#### 11.1.3 明确不做（简化版边界）

- **成员变更只做「一次一条」**：不加 joint consensus / 多步变更队列，leader 把一条
  `conf` 条目写进日志、正常复制、**提交时**各节点各自落表；加节点先走 learner
  阶段（追平才转 voter）。运维侧见 §6.1 的 admin 接口。
- **不做 PreVote、不做 ReadIndex/租约读**：本项目的读全部走数据面
  （客户端按槽 leader 读本地 ≤HW 副本），共识层不承担读一致性职责。
- 不做 TransferLeadership、不做快照分块流式（表快照只有几 KB）。

#### 11.1.4 安全前提

1. **提交前 fsync**：leader 把条目写进 WAL 并落盘后才计入多数派；follower 先落盘
   再 ack。否则多数派 ack 的日志可能在崩溃后消失。
2. **peer 端口分流的谜面冲突**：`peerMux` 靠「首字节 = 'P' → gRPC，其余 → 共识」
   分类，自研握手魔数**首字节必须 ≠ 'P'**，取 `0x9E`。

### 11.2 架构

```
internal/raft/             ← 自研共识层（不认识 pushupes 业务）
  ├── types.go        Config / Entry / Voter / State / FSM / Logger
  ├── node.go         Node 门面：Apply/Barrier/Stats/WaitForLeader/Close
  ├── state.go        runLoop 状态机（唯一写者）+ 全部协议迁移
  ├── handlers.go     RPC 入口（投事件 + 等回复）
  ├── replicate.go    每 follower 一个 replicator（攒批 + 流控 + 退避）
  ├── log.go          内存日志 + WAL 绑定 + 截断标记
  ├── wal.go          分段 WAL：组提交 + CRC + 崩溃截断恢复 + flock
  ├── snapshot.go     单文件快照（无 meta 指针）
  ├── transport.go    TCP：per-peer 长连接 + 请求/响应复用 + accept 服务
  └── wire.go         握手魔数 + 长度前缀帧 + 消息编解码

internal/cluster/          ← 业务装配层，只认 FSM 接口，接口边界稳定
  ├── cluster.go     Node 包装 raft.Node，装配静态 voter 集
  ├── fsm.go         Apply(raft.Entry) any / Snapshot() []byte / Restore([]byte)
  └── peer_mux.go    实现自研 raft.Transport（首字节分流共识与 peer gRPC）
```

分层原则：**共识包不认识业务语义**，只认 `[]byte` 命令 + `FSM` 接口。

### 11.3 持久化：自研 WAL

#### 11.3.1 目录布局

```
<DataDir>/
├── wal/
│   ├── 000000000000000001.wal   # 段名 = 零填充段序号（不解析 entry）
│   └── 000000000000000002.wal
├── snapshot/
│   └── 000000000000000123.snap  # 单文件快照，文件名 = index
└── wal/LOCK                     # flock 目标
```

#### 11.3.2 记录帧

```
len u32 | type u8 | crc32c u32 | payload[len]      # crc32c 覆盖 type+len+payload
```

记录类型：`1=entry(index/term/kind/data)`、`2=hardstate(term/vote)`、
`3=conf(voters)`、`4=truncate(index)`。

- **term/vote 与日志同一条流**：不引入第二个文件，恢复时取最后一条 hardstate，
  从根上杜绝「重启 term 归零」。
- **truncate 标记**：WAL 是 append-only，follower 冲突截断不能在中间删记录，
  改为写一条 `truncate(index)` 标记；重放时先丢弃 ≥ index 的内存条目再继续。

#### 11.3.3 崩溃恢复

- 启动按段序号顺序扫描，遇到读失败 / CRC 不符 / 长度越界 → **截断该段到最后一
  条有效记录**，删除更后面的段，记 WARN。只丢尾部，已落盘的前缀记录不整库报废。
- `flock(LOCK_EX|LOCK_NB)` 锁 WAL 目录：拿不到锁**立即报错**（不是无限等），
  `Close()` 保证释放——这是进程内重启必须能重开的前提。

#### 11.3.4 组提交（性能关键）

```
Append(typ, payload) → 编码入 buffer，返回 LSN（不落盘）
flusher goroutine    → 等 dirty 信号或 200µs tick → write + fdatasync → 唤醒
                       LSN ≤ durable 的等待者，并回调 OnDurable(durableLSN)
Wait(lsn)            → 阻塞到自己的 LSN 落盘
```

- 顺序写路径：1 命令 1 fsync。
- 并发/批量路径：N 条摊成 1 次 fsync（实测 200 条 / 1 次）。

### 11.4 快照

- 触发：`applied - snapIndex >= SnapshotThreshold`（默认 1024）或 30s 间隔到点且
  有新增应用。
- 形式：`snapshot/<index>.snap`，头部 `magic ver index term confLen conf dataLen crc`；
  写完 `fsync 文件 → rename → fsync 目录`；保留最近 2 份。**无 meta.json 指针**——
  最新快照 = 目录里编号最大且 CRC 通过的那个，少一个会互相矛盾的原子写。
- **conf 段是快照点的成员表**：配置本身在日志里（见 §11.4.1），而快照把 `≤ index`
  的日志换掉了，所以快照必须自带那一刻的 voter 集，否则从快照追平的节点（运行时
  新加的成员正好落在「leader 已压缩掉它缺的那段日志」时）只会在日志里看到「把自己
  加进来」这一条 conf 条目，然后以「整个集群就我一个」的姿态（quorum=1、日志任意
  陈旧也能自己当选）跑起来。`InstallSnapshot` 消息里也带同一份（直接取自被发送的
  快照文件，两者不会分叉），接收方 `setVoters` + 落 `conf` 记录。
- **不变式**：快照基线只能裁掉日志的**前缀**（≤ 基线的内存条目），基线之上的
  条目一律保留；`commitIndex` 不得超过 `LastIndex`。

#### 11.4.1 成员表就是日志的一部分

- **初始配置写进日志第 1 条**（`conf` 条目，op=`confSet`，term 1，voter 按 id 排序
  保证各节点写出的字节一致），同时写一条 `conf` WAL 记录。只写记录（每个初始节点
  各写一份自己那份）对**后来加入**的节点毫无用处：它的成员表要从 leader 复制的
  日志或快照里推出来。
- **运行时的 add/remove 也是日志条目**（op=`confAdd`/`confRemove`），提交时才落表；
  重启重放同一份日志必得同一份成员表。`confSet` 只由 bootstrap 与快照写入，不接受
  运维调用。
- **加节点先 learner**：leader 起复制、等 `matchIndex ≥ 接变时的 commitIndex`
  （墙钟 20s 上限）才 append 那条 add；否则空日志的新成员可能参与计票并当选。
- **删节点不提前摘复制器**：先提交、继续复制，等它 ack 到该条目、再发一轮带新
  commitIndex 的 AppendEntries 让它自己也提交，然后才摘复制器——否则被删节点停在
  「有日志、未提交」，仍信旧配置、仍能竞选。
- **成员表并发**：loop 写、读侧 `votersMu` 取副本；**写侧也必须持同一把锁**
  （`publishVoters`），否则 `Members()` 复制到一半的切片会与 loop 的重建并发
  （`-race` 会抓）。同理 `clients` 映射由 `clientsMu` 保护：loop 改成员、replicator
  goroutine 每轮读它。

### 11.5 共识：线程与不变式

- **单写者 runLoop**：一个 goroutine 独占全部可变状态（state/term/vote/leaderID/
  commit/applied/nextIndex/matchIndex），RPC handler 与 replicator 只投事件；
  外部读走原子发布的 `view` 快照。协议状态**不用 mutex 保护**，从结构上消灭
  「两把锁互咬」。
- **replicator 不碰 loop 状态**：loop 把一轮所需的全部信息（term / prev* /
  entries / leaderCommit / 需要的话整份快照）打包进 `replicateRound` 交给它，
  结果以事件回来。
- **提交规则**：`N` 可提交 ⇔ `N > commitIndex` ∧ 多数派 `matchIndex ≥ N` ∧
  `log[N].term == currentTerm`（只算当前 term 的条目）；leader 上任立即追加一条
  当前 term 的 **no-op** 把前任尾巴顶进提交。
- **冲突回退**：`prevIndex/prevTerm` 不匹配 → `success=false` + `conflictIndex`
  （冲突 term 的**首** index），leader 一次跳到该点重试，不退化成逐条退。
- **Apply 语义**：注册 waiter 必须在可能推进 commit 之前；已 applied 的直接应答；
  **每条推进 applied 的分支都必须应答 waiter**（含条目已被快照裁掉的情况）。
  Apply 请求走独立 channel，loop 一次 drain 整批、**一次 append + 一次 fsync**。
- **follower 的 ack 异步化**：follower 不在 loop 里等 fsync，而是异步追加 + 落盘
  通知到达后再应答（`WAL.OnDurable` → 原子量 + cap-1 信号唤醒 loop）。
- **leader 的 Apply 顺序**：先追加并立即 kick 复制，**再**等本地 fsync —— 本地
  落盘与 follower 往返重叠。
- RPC 集：`RequestVote/Resp`、`AppendEntries/Resp`、`InstallSnapshot/Resp`。
  选举超时 500ms~1s 随机、心跳 100ms（`cluster.Config` 未传时用默认值）。

### 11.6 线上协议（peer 端口）

- **分流不变**：`'P'` → HTTP/2 前导 → gRPC；其余 → 共识层。自研握手首字节
  `0x9E`，**断言 ≠ 'P'**（有单元测试守着）。
- **握手**：`magic(1) | version(1) | nodeID(u8 长度 + bytes)`；acceptor 回同样格式
  的 ACK。没有 role 字节——一条连接是**双向**请求/响应复用，双方都可能在同一条
  连接上发请求。
- **帧**：`len u32 | reqID u64 | kind(0=req 1=resp) u8 | type u8 | payload`。
- **连接模型**：per-peer **长连接**（懒拨号、写互斥、按 reqID 分发响应、断开即
  失败在途请求）；acceptor 侧只回不该连接上的请求，实现对称且简单。

### 11.7 模块边界与装配

| 位置 | 职责 |
| --- | --- |
| `internal/raft/`（10 文件） | 共识层全部实现，不认识业务 |
| `internal/cluster/cluster.go` | 包装 `*raft.Node`；装配静态 voter 集；种子节点 `Adopt` 加入 |
| `internal/cluster/fsm.go` | `Apply(raft.Entry) any` / `Snapshot() ([]byte,error)` / `Restore([]byte) error` |
| `internal/cluster/peer_mux.go` | 实现自研 `raft.Transport`；`Addr() string`；首字节分流 |
| `internal/cluster/register.go` | 用 `LeaderID()` 读当前 leader |
| `internal/cluster/migration.go` | 读 `Stats` key（连字符/下划线两种写法兼容） |
| `cmd/pushupes/main.go` | `-raft-flush-interval`(200µs) / `-raft-segment-bytes`(64MiB) |
| `scripts/cluster.sh` | 启动传 `-peers`（`-bootstrap` 为 no-op 兼容位） |

**共识层不碰**：`internal/storage/*`、`internal/data/*`、`internal/grpcapi/*`、
`internal/api/*`、六步热迁移、ISR/HW、再平衡。

### 11.8 测试与验证方案

- 单元：WAL（崩溃截断只丢尾部、跨段重放、flock 互斥、CRC 篡改检出、组提交合并）、
  内存日志、快照与日志自洽（含快照携带的成员表）、term/vote 跨重启、种子节点启动。
- 成员变更（`raft_membership_test.go`）：运行时加节点后**每个节点**（尤其新节点自己）
  的 voter 集都收敛到 N+1、加/删幂等、删到只剩一个被拒、follower 上调用被拒并指出
  leader、leader 日志已压缩时新节点从**快照**学到的成员表、被加节点单独存活时
  **不能自己当选**（1/4 不是多数派）。
- 进程内真实 TCP 集群：拓扑 1/3/5/7 各跑「选主 → 写批 → 杀 leader → 幸存者选主
  → 再写 → 全体 applied 与 last 一致」，**每拓扑多轮**；`-race` 全绿。
- 端到端：`scripts/cluster.sh` 三段（见 §11.10.3），扩缩由 `join N` / `remove N` 覆盖。

### 11.9 用户拍板记录

（2026-10-05）

1. 成员集合**静态**固定 —— 接受。
2. WAL **自己写**，不剥 `internal/storage` 的 Segment —— 采纳。
3. 传输用 **per-peer 长连接**复用 —— 采纳。
4. 快照**单文件 + 扫目录取最新**，不做分块流式 —— 可以。
5. `kill -9` 故障切换 + 重启追平作为**硬性验收**，基准数字写进文档 —— 照此执行。

### 11.10 实现结果

#### 11.10.1 交付物

见 §11.2 的目录清单；共识层从状态机、日志、WAL、快照到传输、编解码全部在
`internal/raft` 内实现。

#### 11.10.2 与设计稿的偏差（实现中改进）

1. 握手去掉 role 字节（见 §11.6）。
2. WAL 段名改为零填充段序号，而非 baseIndex；压缩按「段 lastLSN < 目标 LSN」整段删。
3. follower 的 ack 异步化；leader 的 Apply 先复制再等本地 fsync（见 §11.5）。
4. 两条防御性不变式（都是本轮踩到的真 bug）：
   - 快照基线只能裁日志**前缀**；裁成后缀会把 applied 游标推过日志末尾，
     每个后续 Apply 都白等满超时。
   - `commitIndex ≤ LastIndex` 每次 apply 前 clamp。
5. `-bootstrap` 保留为 no-op。
6. 运行时成员变更（§11.4.1）落地时踩到并修掉的两个真 bug：
   - **新节点的成员表只有它自己**：初始配置原先只写进每个节点自己的 `conf` WAL
     记录，后来加入的节点从 leader 的日志/快照里推不出这份配置，于是它以 quorum=1
     跑起来（可自己当选、可在陈旧日志上确认写入）。修法：初始配置写进日志第 1 条
     （`confSet`），快照与 `InstallSnapshot` 都带上快照点的成员表。
   - **成员表/连接表读写竞态**：loop 无锁写 `n.voters`、`n.clients`，读侧
     （`Members()`）与 replicator goroutine 并发读 → `-race` 抓到。修法：
     `publishVoters`（写侧持 `votersMu`）+ `clientsMu` 保护连接表；成员表重建一律
     换成新切片，不再原地改元素。

#### 11.10.3 验证（实跑）

- `go build ./... && go vet ./...` 通过；`go test ./... -count=1` 全绿
  （含 1/3/5/7 拓扑多轮与快照回归用例）。
- `go test -race ./internal/raft` 全绿。race detector 曾抓出 `peerClient.write()`
  无锁读 conn 字段与 `close()` 并发写 → 改为把连接句柄按值传入。
- 端到端（3 进程真实集群）：`clean → start → smoke → slotcheck` 全绿；
  `kill -9` leader → 2s 内新 leader 选出 → 重启被杀节点 → 追平，
  `slotcheck` diverged=0、`smoke` 通过；`restart`（保留数据）后槽表不乱。

#### 11.10.4 基准实测

环境：容器 2 核（GOMAXPROCS=2，并发档实际最多 2 路并列），命令体 256B，真实 TCP，
`-benchtime 3s -count 3` 取中位数。被测：自研 `internal/raft` + 自研 WAL。

| 场景 | ns/op | p50 | p99 | B/op | allocs/op |
| --- | --- | --- | --- | --- | --- |
| 单节点顺序 | 1 464 000 | 1.40ms | 1.94ms | 2 135 | 17 |
| 单节点并发16 | 1 454 000 | 2.84ms | 3.72ms | 2 135 | 17 |
| 3 节点顺序 | 5 521 000 | 5.55ms | 6.68ms | 16 022 | — |

- 每命令 fsync：顺序档 1 次/命令，批量/并发档由组提交摊薄（实测 200 条并发写入
  在 5ms 窗口只产生 1 次 fsync）。
- 判据核对：单节点 Apply p99 < 2ms、3 节点 p99 < 7ms、每 op 分配 2KB 级 —— **达标**。

#### 11.10.5 已知边界（不在本次范围）

- `InstallSnapshot` 仍在 runLoop 内同步写盘（仅 follower 落后到基线时才走，表快照
  几 KB）。
- WAL 写失败的极端情况下，已挂起的 follower ack 靠 RPC 超时回收（写失败即节点
  不可用，不做额外降级）。

## 12. 控制面与数据面的角色分离（voter / learner）

> 状态：**角色轴、准入、布局口径已实现并验收**；数据面节点（learner）自身的
> 布局分发流见 §12.4（已接线）。

### 12.1 两个角色

节点用 `-role`（env `PUSHUPES_ROLE`）声明自己的位置。角色**必填**，取值只有
两个：一个节点要么是控制面成员，要么是数据面成员。最小部署 = 1 个控制节点
（voter）+ 1 个数据节点（learner）——开发环境与生产环境因此始终跑同一套
节点间通讯拓扑，这正是只留两种形态的目的。

| 角色 | 投票（Raft） | 承载槽副本 | 用途 |
|---|---|---|---|
| `voter` | 是 | **否** | 控制面：选举域与数据面负载彻底分开，压测/高写入不再干扰心跳 |
| `learner` | 否 | 是 | 数据面：只从控制面接收布局，不参与 quorum（见 §12.4） |

角色是**节点自报**的：与 admin/client 地址同一条注册公告（`RegisterRequest.role`）
写进复制目录的 `peers[].role`，而不是由控制面代为假定——只有节点自己知道它的
`-role`。因此目录里的角色也随快照持久化（表编码 v6；早于 v6 的文件里的成员以
空角色读入，节点重新公告后由注册命令把角色补齐）。

### 12.2 布局口径

- `Table.HostingIDs()` 是唯一的布局池：目录里 `role = learner` 且已自报 client
  地址的成员。`plan_slots`/`replan_slots`、副本因子推导（`ReplicaCountForPolicy`
  取的就是
  这个池的大小）与 `OnlinePeerIDs()` 的候选集全部只认这个池。
- 纯控制节点因此**不持有任何席位**，也不出现在故障转移/再平衡的目标集里。
- **计划等池稳定一拍**：池是按注册逐个长出来的，所以「第一个成员一注册就规划」会
  规划一个还没到齐的集群——旧单面形态的冷启动曾经如此：首个 plan 落在最先注册的那一
  个成员上（factor 1、1680 槽全在它身上），第二个成员注册后再把 1680 个副本集全部
  重排（factor 2），之后领导权还要按再均衡节奏慢慢挪。现在计划只在「池与上一轮观察
  相同」或「已推迟满两轮」时才提交（`Engine.poolSettled`），代价是冷启动晚一个对账
  周期，换来的是第一个布局就是目标形状：实测旧单面形态 3 节点冷启动在 t=3.0s 一次性以
  factor 2 / 560-560-560 落地，不再出现 factor 1 窗口，也没有随后的席位重排波。
- 池为空（集群只有 voter）时：不提交 plan（避免失败条目每秒重提交把 commit
  推高），槽表保持零条目；客户端在该节点写入得到显式拒绝
  `fail/1005` + `"cluster has no data-plane members; this node is control-plane only"`
  （单条 `SubmitAppend` 与 client 面批量 `SubmitBatch` 两条入口都拦，见
  `controlOnlyRefusal`）。第一个数据面成员自报后，控制器自动补 plan。

### 12.3 准入与撤销

- 加数据面节点：`POST /admin/cluster/nodes {"id":..,"peer_addr":..,"role":"learner"}`
  ——提交一条**目录命令**（`join_node` + role），不改 raft 配置：learner 不进投票集，
  其席位由 replan 铺开；节点进程随后自报地址与角色。
- 加投票成员：同接口 `{"role":"voter"}`（必填，与 learner 一样点名角色）——走原有
  两阶段（learner 追平后晋升），语义不变。
- 撤销：`DELETE /admin/cluster/nodes/{id}` 对 learner 提交目录 `leave_node`，对 voter
  仍是 raft 配置变更；两者都只允许撤销**已下线**的成员（在线成员持有的席位正在被
  集群使用）。
- 控制器对账的目录同步因此是双源：投票集来自 raft 配置（`PeerAddrs`），learner 来自
  目录条目本身——反向清理只删「既不是 voter、角色也不是 learner」的条目。

### 12.4 数据面节点的状态分发

> 状态：**已实现并验收**（3 voter + 2 learner 端到端，见 §12.5）。

数据面节点不投票、不持有日志，它的槽表来自控制面的 `WatchLayout` 服务端流：

- **服务端（leader）**：每个 learner 一条订阅，由共识心跳的同一 tick 驱动（不是
  第二个调度器）。帧分五种：`HELLO`（leader id/地址、term、投票集、本集群世代、
  日志下限）、`ENTRY`（一条**已提交**命令，带 index/term/entry kind）、`COMMIT`
  （推进水位）、`SNAPSHOT`（整份状态 + 落点）、`REDIRECT`（非 leader 只给方向，
  不给静默流）。每订阅一个有界缓冲（256 帧）：塞满即**掐流**，learner 重连后
  报告自己应用到哪、由 `LayoutSlice` 决定续传还是给镜像——「卡住的数据节点不占
  控制面内存」是设计目标，不是容错细节。
- **共识层原语**：`raft.Node.LayoutSlice(from, fromTerm, max)` 是唯一的读取面，
  判据与 AppendEntries 同源——term 不符、请求点已被压缩、调用方超前于本组 commit，
  三者任一即回答镜像（镜像取自快照文件，与给落后 voter 装快照是同一份字节）。
- **客户端（learner）**：严格按 index 应用（重复即跳过——命令都是确定性的集合型
  变更；空洞即断流重连），状态与位点一起落盘（`layout-feed.json` + 状态字节；
  位点没有配得上的状态就不是恢复点，落盘时状态在前、位点在后）。应用失败或缓存
  不可用即作废恢复点、以 index 0 请求整份重建。世代不符时清空状态机重来，
  learner 按新世代重新接收布局（世代是复制状态里的值，不是当前 leader 的目录 id，
  所以换 leader 不会让 learner 重来）。**集群不提供「保留槽数据、重建共识」的降级
  路径**：旧副本除非被运维显式采纳（owner 围栏，§3.1），一律停在 pending，
  写入得 `fail/1007`。改造集群形态走成员逐个 `remove` + `join`，或从空目录重建。

learner 的槽表与投票节点的是同一份复制状态，因此路由、取副本、迁移、owner 围栏
全部复用同一套 Engine 代码；它唯一与投票节点不同的地方是「表从 socket 来而不是从
日志来」，以及 `Apply` 恒回 `ErrNotLeader`（它不提交）。它也不跑 joiner：数据面
节点绝不能报名成为投票者。

### 12.5 端到端验收口径

实测（3 个 `voter` + 2 个 `learner`，独立运行目录与端口）：

- 纯控制面就绪后槽表 **0 条**、三个投票节点 **0 席位**；挂上数据面后全部槽都在
  数据面（factor 2，两个数据节点各领一半槽主），`smoke` 通过、`slotcheck` 全槽 0 发散
  （该轮验收当时槽数 4096）。
- 数据面节点的共识目录只有 `layout-feed.json` 与状态缓存（无 wal/无 snapshot）：
  它确实不跑共识组。数据面重启走 `resumed from cached state at index=N` 增量续传。
- **杀掉 2/3 voter 无配额时，数据面写入仍然成功**——写路径不依赖控制面，这是解耦
  的意义所在；控制面自身的写入则按预期失败。
- 关再均衡（`PUSHUPES_REBALANCE_INTERVAL=0`）下的同一套验收也通过：旧单面形态
  3 节点冷启动即 560-560-560（factor 2），拆分形态 840-840，冒烟与 slotcheck 首跑
  即绿——需要「无在途领导权移交」的稳定窗口时用这个姿势。
