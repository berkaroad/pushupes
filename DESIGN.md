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
├── 固定 SlotCount = 1680（不可配置；105×16 = lcm(3,5,7) 的倍数，见 §7.2）
├── slot = mix64(FNV-1a(aggregate_id)) % SlotCount（见 §7.2.4）
└── 每个 slot 对应一条 WAL（按 256MiB 分段），支持在集群中热迁移
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

`Append(aggregate_id, version, unix_time, command_id, events[])`：

1. **幂等**：command 索引（slot 内 `map[command_id]→seq`）命中
   → 返回 `status=exists` + 已存储的记录（按 command_id 反查 seq 后从 WAL 读出）。
2. **版本校验**：该 aggregate 当前最大版本 `cur`（slot 内 `map[aggregate_id]→cur`）。
   - `cur == 0 && version == 1`，或 `version == cur + 1` → 通过；
   - 否则 → 返回 `status=fail` + 错误 ID `1001 (ERR_VERSION_CONFLICT)`，
     附带 `current_version`。
3. 通过校验后：追加到 slot WAL 尾部，原子更新三个索引
   （command 索引 / 聚合版本索引 / seq 计数器），返回 `status=success`。

错误 ID 表：

| 错误 ID | 名称 | 含义 |
|---|---|---|
| 1001 | ERR_VERSION_CONFLICT | 版本非递增 1 |
| 1002 | ERR_BAD_REQUEST | 参数非法（空 events、超长字段等） |
| 1003 | ERR_SLOT_NOT_LOCAL | 槽位不在本节点（MOVED，附正确节点） |
| 1004 | ERR_MIGRATING | 槽位迁移中（ASK 语义，附目标节点） |
| 1005 | ERR_NOT_LEADER | 本节点非该槽 leader（附 leader 地址） |

并发控制：锁粒度 = slot。同一槽串行写（保证 seq/version 原子推进），
不同槽并行。1680 槽 = 最多 1680 条并行写路径。

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
  8×CPU 核数（一批铺满 1680 槽时不至于瞬间压上等量并发 WAL 写者）。
- 重定向（MOVED/ASK/NOT_LEADER）也是**逐条结果**：该条 fail + 权威
  `slot`/`node`，服务端不代理转发写；客户端刷新路由后只重发受影响的那几条。
  幂等规则保证"部分成功 + 按 command_id 重试"安全收敛。

## 3. 存储设计（slot WAL）

目录布局：

```
data/
├── slot-000/
│   ├── 0000000000000001.wal   # 文件名 = 该段 baseSeq
│   ├── 0000000000000514.wal   # 满 256MiB 滚动出下一段
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
- **稀疏索引**：每 4KiB 记录一个 `seq→(段文件, 偏移)` 的内存索引，常驻内存
  开销与数据量解耦；按 version 读聚合时，先经聚合索引定位 seq 区间，再二分
  稀疏索引。
- **重启恢复**：加载各 slot 段列表 → 从最后一段 `scanBody` 顺序扫尾，
  重建三个内存索引与 seq 计数器；**不设独立 meta.json**（避免元数据与 WAL
  双写不一致的竞态）。
- 崩溃尾部撕裂：扫描遇不完整记录即在完整边界截断。
- **刷盘策略**：flush.policy 可配（每 N 条 / 每 T 毫秒 fsync；默认组提交
  依赖页缓存）。写入确认的是**复制**（ISR 高水位覆盖该 seq），不是逐条
  本地 fsync——持久化边界与刷盘策略挂钩，见 §4。

## 4. 高可用（复制设计）

分层结构：

- **控制面（自研 Raft）**：集群共识层是**仓库内自研的简化 Raft**
  （`internal/raft`：选主/日志复制/提交推进/快照），日志与 term/vote 存于
  **自研分段 WAL**（`internal/raft/wal.go`，组提交 + CRC + 崩溃截断恢复），
  不再依赖 `hashicorp/raft` 与 BoltDB。成员集合**静态**（启动时由 `-peers`
  写入 WAL 首条，增减节点=全集群重启），不支持运行时 AddVoter/PreVote/ReadIndex。
  它只复制元数据——
  slot 分配表（`slot → {leader, replicas[], epoch, state}`）、集群成员、
  节点数据面地址（`OpRegister`，见 §6 peer 面）。FSM 模型：
  `Applier` 接口 + 快照/恢复（快照为二进制表编码 `table_bin.go`，1680 槽
  ~350KB JSON → 几 KB）。成员集合由静态 voter 集唯一决定；路由表里每个成员
  同时记 `PeerAddr/AdminAddr/ClientAddr` 三个地址。
  设计稿与实测见 `docs/raft-自研设计.md`。与替换前的 A/B 实测（256B 命令、
  真实 TCP、3s×3 取中位）：单节点顺序 Apply 2.10ms→1.46ms（+43%，p99 2.87ms→1.94ms）、
  单节点并发 2.08ms→1.45ms、3 节点顺序 5.01ms→5.52ms（−9%，p99 7.64ms→6.68ms
  仍优于旧实现），每 op 分配 108→17 allocs；依赖树去掉 hashicorp/raft、
  raft-boltdb、go-hclog 及其带进的 bbolt/msgpack/metrics 等。
- **数据面（专用拉取）**：
  - 客户端把 `Append` 发给槽 leader 的 gRPC client 面；leader 追加本地
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
    每条省掉两份大分配与两次全量拷贝（迁移写转发 `Replicate` 同路径）。
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
  - **写入成功的条件**：一次 `Append` 只有满足以下全部条件才对客户端回
    `success`——(1) 幂等与版本校验通过并落入 leader 的 WAL；(2) 该槽的高水位
    已覆盖此 seq，即**每个 in-sync 副本都已把同一记录写进自己的 WAL**。
    因此任何一个单节点故障都不会丢失已确认的写入。两个边界：ISR 内所有副本
    失联时，持久化承诺只对 in-sync 副本成立，高水位跟随 leader LEO、写入
    不阻塞（等副本重新追平 ISR 后该记录重新受保护）；等待高水位超过 10s
    返回 `fail/1005`，此时记录已在 leader WAL 落定——客户端应带同一
    command_id 重试，命中 `exists` 即确认。
  - ISR 维护：follower 在 `replica.lag.time.max` 内跟上进 ISR；
    掉出后按自己 LEO 重新追。
- **故障切换**：controller（Raft leader）每 1s 经 peer 面 `PeerService.Ping` 探活，
  连续 3 次失败判定失联 → 经 Raft 提交 `mark_down`：为该节点名下所有槽从存活
  副本重选 leader（优先选「在线」的副本；epoch+1）→ 客户端收到 MOVED 重定向。
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
  重新注册后自动回到环上）的 stable 槽，按每轮至多 `-rebalance-batch`
  个（默认 28，串行；回切目标与源端摘要等价，单个交接=元数据+毫秒级栅栏，
  560 槽最坏情况 20 轮收敛）经**既有六步热迁移**的 leader_move 通道迁回环上的预期
  leader。安全门禁：任一秒位在迁移中（如人工 migrate 在飞）整轮让路；回切目标
  必须已完成注册且 Ping 存活；目标副本必须与源端**摘要等价**（LEO 不低于源且
  aggregates/versions/resolvable 三元组相等）才动手——因此回切全程走迁移的快
  路径（摘要等价即跳过快照，不重传封段；副本追平 leader 走的是普通 fetch，
  与再平衡无关）。0=关闭再平衡；负数启动即报错。
- **默认拓扑**：`replica_count=2`（每槽 1 leader + 1 follower，散布不同
  节点），`slot_count=1680`（固定）。3 节点以上时副本环按
  `slot % N` 起、向前取 `replica_count` 个节点。
  新成员加入只做增量补位（`replan_slots` 填未分配槽/补足副本），不扰动
  有数据的槽。

## 5. 槽位热迁移（文件级搬运）

槽是迁移的原子单位；WAL 段文件自包含，迁移的大部分工作是**整文件拷贝**，
不需要解析事件——这正是按槽迁移相较按聚合迁移的核心优势。

槽状态：`stable → migrating(源) / importing(目标) → stable`（epoch 递增）。

六步热迁移（源 S → 目标 T，由 controller 发起，全程不停写）：

1. **准备**：controller 经 Raft 把 `slot → T` 写入 T 分配表（state=importing），
   S 置 state=migrating。
2. **快照拷贝**：S 上该槽所有**已封段**（非活动段）经 `PeerService.PushSegments`
   **client-streaming 分块**（≤4MiB/块）流式拷到 T，两端内存占用为 O(块)
   而非 O(槽)；payload 为原始 WAL 字节，无 base64/JSON 层；T 从流的前 20
   字节装配并校验每段 header 的 magic/slotID/baseSeq 后才落盘（写 .tmp 再改名）。
3. **增量追平**：T 从 `lastCopiedSeq+1` 起用与复制相同的 PeerService.MFetch
   协议向 S 拉增量记录；迁移协调者轮询双端 LEO（`PeerService.SlotLeo`）直到
   `T.leo >= S.leo`（超时 30s 回滚）。期间写请求仍由 S 正常处理。
4. **写转发窗口**：S 对该槽新写入经 `PeerService.Replicate` **同步转发** T 落盘
   （seq 以 S 为准，T 按相同 seq 追加），直到 S 侧无积压。窗口毫秒级，对客户端只是该槽写延迟
   微增。
5. **切换提交**：controller 经 Raft 原子更新分配表：`slot leader=T,
   epoch+1, state=stable`；S 置 `backing-up`。
6. **清理**：S 在**自己观察到已离开该槽的副本集**（既不是 leader、也不在
   `replicas` 里）之后，才在**本地**开始倒计时——计时由源节点自己掌握，
   不依赖控制器：控制器既不持有那份副本，也不该为一次可能回滚的迁移启动
   保留期。倒计时到期即 `DropSlot` 删除本地副本。开始倒计时的条件是**副本集成员
   资格的丢失**，而不是"当过迁移源"：
   - **集外迁移**（目标不在副本集）：T 先被加进副本集（集合变成 factor+1），leader
     切到 T 后由回收步骤（`OpSlotRemoveReplica`）把 S 移出集合 —— S 这才真正变成
     多余副本，开始倒计时；
   - **集内迁移**（目标本来就在副本集里）：没有任何成员被移除，S 仍是 RF 的一部分，
     **不启动倒计时、不置灰、数据永不删** —— 如果按"谁当过迁移源"来判断，就会在这个场景里删错数据。
   每次丢失成员资格都**重新开始**倒计时；上一轮的调度一律作废，绝不复用旧的到期时刻
   （复用会让新一轮的副本在旧窗口到点时被立刻删掉，延迟删除就变成了立即删除）。
   时长 = `-drop-after`（默认 30s，`0` 取默认值，负数启动即报错）；**没有关闭选项**：
   前源节点的那份副本已成死数据，长期留在盘上会把节点写满。倒计时期间该调度对
   admin 面可见（`/admin/writes` 的 `dropping[]`、`/admin/slots/{slot}/describe` 的
   `pending_drop_at`），控制台据此把该副本置灰（它已不在 `replicas` 里，因此渲染在
   副本集旁边）。若期间该槽又回到本节点名下（回滚、故障切换或又被加回副本集），
   取消调度、保留数据；到期时还会**再查一次表**，成员/leader 的副本一律不删。
   倒计时只存在于内存：进程重启即丢失，副本保留（等下一轮迁移或人工回收），
   **重启绝不删数据**。

客户端路由：

- 本地缓存 `slot → node` 路由表（gRPC `node` 字段携带目标节点 client 地址），
  MOVED 驱动刷新。
- 迁移窗口内写 S：S 内部转发（对读请求返回 ASK 重定向指向 T）。
- 提交后命中旧路由：返回 `MOVED(1003)` 重定向。
- 幂等规则保证转发写不产生重复：两侧 command_id 相同，命中 exists 即收敛。

## 6. API 设计

每节点三个端口，各司其职，事件读写**只走 gRPC**：

- **client 面（gRPC，客户端事件写入/查询唯一入口，默认 `-client
  http://127.0.0.1:8591`，`PUSHUPES_CLIENT`）**：proto3 契约
  `proto/pushupes/v1/events.proto`（`pushupes.v1.EventService`：`Append` +
  `BatchAppend` + `ReadStream` + `ReadTails` + `ReadByCommand`）。body 为原始
  bytes，无 JSON/base64 层。client 面 gRPC 消息上限由 `-grpc-max-msg-size`
  配置（默认 4MiB，recv/send 同值）：`BatchAppend` 一整批要装进这个上限，
  批次大小随它一起调。
  MOVED/ASK 重定向的 `node` 字段携带槽 leader 的 **client 地址**（路由表
  Peer 同时记 PeerAddr/AdminAddr/ClientAddr），客户端据此重连。本节点既无槽又无副本
  时，服务端向 leader 的 client 面代理转发（`ReadProxyAddr` 返回 leader
  client 地址）。
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
- **peer 面（Raft + PeerService gRPC，默认 `-peer http://127.0.0.1:8391`，`PUSHUPES_PEER`）**：复制槽位分配表等元数据（Raft），并承载**全部节点间数据面**——副本拉取、LEO 上报/探活、迁移快照/段拷贝/写转发/LEO 追平、地址注册协议（proto3 契约 `proto/pushupes/v1/peer.proto`，`pushupes.v1.PeerService`，服务端 `peersvc.go`）。**运维只配这一个端口**：`-peers` 主格式 `node-id=http://ip:peerport`（或裸 `ip:peerport`，地址兼作节点 id）。**地址统一规范：存储/路由表/status JSON 中所有 admin/client/peer 地址都带 scheme——未写协议默认补 `http://`，显式协议以传入为准；TCP 拨号（listen/dial/gRPC/Raft transport）前再剥掉 scheme**（`cluster.NormalizeAddr`/`HostPort`）。admin/client 地址不配置，由各节点自报进路由表：
  - peer 端口上是复用监听器（`peerMux`/`peer_mux.go`，实现共识层的 `raft.Transport`）：按连接**首字节 `P`**（HTTP/2 client preface `"PRI ..."` 以 `P` 开头）把 PeerService gRPC 流量与共识流量分流（自研 Raft 的握手首字节是 magic `0x9E`，**断言 ≠ 'P'**，永不冲突；两路都经 `replayConn` 回填被 peek 消费的字节）。gRPC 服务端由 `Engine.ServePeer` 挂在该分流 listener 上，peer 面与共识层从此共用一个端口、一套 gRPC 语义（HTTP/2 多路复用，每对节点一条缓存连接 `peerClient`，keepalive 10s，服务端放宽 enforcement）。
  - leader 收到注册 RPC 后提交 `OpRegister`（就地修补路由表中该成员的 `AdminAddr/ClientAddr`，不新增成员——成员集合仍由 Raft 配置决定）；follower 收到则转发给 leader 的 peer 地址。
  - announcer 幂等周期重试（启动期 1s，收敛后转 10s 心跳），任意启动顺序都能收敛；节点换端口重启也会被自报值修补。
  - 旧多端口格式 `id:peerport:adminport:clientport` / `id:host:peerport:adminport:clientport` 仍兼容（作为静态种子，注册落地后以自报值为准）。

```
gRPC  EventService/Append         写入：幂等(command_id)/版本(+1)/等 ISR 高水位确认，MOVED/ASK
gRPC  EventService/ReadStream     范围查询（≤HW 语义）
gRPC  EventService/ReadByCommand  command_id 幂等探针
gRPC  EventService/ReadTails      批量取多个聚合的最新版本（resume 扫描用；按槽分组一次 RPC，非持有者按目标节点成组转发）

# ---- peer 面（PeerService gRPC，与 Raft 同端口，proto/pushupes/v1/peer.proto）----
gRPC  PeerService/MFetch          副本拉取（长轮询，多槽复用，payload=裸 WAL 字节）
gRPC  PeerService/ReplicaProgress 副本 LEO 上报（批量）
gRPC  PeerService/Replicate       迁移写转发（同 seq 落盘）
gRPC  PeerService/SlotLeo         查询节点某槽 LEO（迁移追平判定，见 §5 步骤3+4）
gRPC  PeerService/PushSegments    迁移段拷贝（client-streaming，≤4MiB 分块，原始 bytes）
gRPC  PeerService/TriggerSnapshot 迁移快照（源端向目标推封段）
gRPC  PeerService/Ping            探活（controller 故障切换）
gRPC  PeerService/Register        数据面地址自报（OpRegister 提交/转发）

# ---- admin 面（HTTP，仅管理）----
GET  /admin/slots/{slot}/describe              # 本节点视角的槽状态/seq/HW/大小（不代开槽，带 node/role/loaded）
GET  /admin/cluster/status                     # 分配表/epoch/ISR 视图（含 client_addr）
GET  /admin/writes                             # 每槽 durable 计数 + 槽内总字节/事件流数量 + 本节点待清理副本（前端轮询）
GET  /admin/slots/{slot}/streams?after=&limit=  # 事件流列表（聚合id+最新版本，仅内存索引，不读 WAL）
POST /admin/slots/{slot}/migrate  {to_node}    # 发起热迁移（控制器专属，必须发到 Raft leader 的 admin 地址）
POST /admin/slots/{slot}/remove-replica {node} # 回收副本集里的一个成员（控制器专属，同上）
POST /admin/cluster/plan                       # 触发重新规划（控制器专属，同上）
GET  /healthz
```

读语义只暴露 `seq ≤ HW` 的记录：查询不读未达高水位的数据，防止副本回滚后
出现脏读。

槽位事件流列表（`/admin/slots/{slot}/streams`）回答的是**槽内索引**：每条事件流的
聚合 id + 最新版本号。索引在开槽时只解析记录的**帧头**（`data.DecodeRecordMeta`：
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
  `internal/storage/segidx.go`）：`<baseSeq>.idx`（header 32B + 块 `crc32c(4) + n×20B`，
  entry = command hash(8) seqOff(4) dictID(4) version(4)）、`<baseSeq>.agx`（段内聚合字典，
  新增聚合即 sync）、`<baseSeq>.spx`（稀疏 seq→pos，16B/条）。
  **不变式**：索引是派生数据——只加载 CRC 验证通过的最长前缀，其余从 WAL 重放；
  缺失、损坏、滞后一律退化为全量帧头扫描（即索引出现之前的行为），因此正确性
  不依赖索引。恢复时用 `.spx` 的最后位置续走到文件尾（不超过一个稀疏间隔）
  定出段长与条数，entry 数用 WAL 实际条数封顶（截断过的段不会凭空多出记录）。
  **策略**：`OpenStore` 启动时把「缺失/损坏/滞后」的段补齐索引（`OpenSlotRepairing`）；
  运行期（`ReloadSlot`、懒加载 `Slot()`、迁移推来的段）只加载、绝不补建，缺索引的段
  留给下次启动——即「启动补建、运行期维持现状」。**代价**：索引约 20B/条 ≈ 数据的
  **2.2%**（31GiB → 724MB）。实测同一 31GiB 数据集（补建前清空索引）：冷启动 27.6s
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
  `FetchItem.payload`/`PushSegmentsRequest.data` 为裸 WAL 字节区间，不经
  base64/重编码）；客户端事件面 gRPC protobuf，body 原始 bytes。
  路由表快照同为二进制编码（字符串字典 + varint + gzip）。
  **sendfile 零拷贝已实测否决**：payload 约 40-64KiB、需用户态解码过滤、
  Go netpoller 使用非阻塞 fd——三个收益条件全都不满足；syscall 优化因此走
  「减少往返」（多槽复用会话、长轮询、批量 LEO 上报、组提交），而不是减少拷贝。
- **稀疏索引常驻**：内存索引 = 1680 槽 ×（聚合版本 map + command map +
  seq 稀疏索引），索引成本每记录几十字节，可支撑亿级聚合。
- **写不放大**：一条记录只在槽 WAL 落一次；副本走日志拉取而非双写。
- **锁竞争**：slot 级锁把热点聚合关进单槽（热点从"锁全库"降为"锁
  1/1680"）；读路径短临界区 RWMutex。
- **启动恢复并行化**：OpenStore 两阶段（先扫目录再 worker 池并行开槽）；每个分段的
  恢复是**一次窗口读（1MiB `frameWalker`）的帧头遍历**——同一次遍历里找尾部完整边界
  （撕裂尾截断）并重建稀疏索引，随后 `Segment.ScanHeaders`+`data.DecodeRecordMeta`
  再按帧头重建聚合/command 索引（body 既不拷贝也不解码）。**不要退回「每条记录一次
  pread」**：31GiB / 3250 万记录下，旧实现的 `LoadSegment`（每记录 1 次 4 字节 pread
  找边界）+ `ScanFrom`（每记录 2 次 pread 解整帧）≈ 9700 万次 syscall，占启动 CPU 的
  45%（实测 OpenStore 96.0s → 36.5s，节点启动到可服务 92.9s → 29.3s）。

## 8. 配置默认值

```
slot_count      = 1680         # 固定不可配置（105 的倍数，见 §7.2）
segment_bytes   = 268435456    # 默认 256MiB；须为 64MiB 的整数倍，最小 64MiB、最大 2GiB
                              # -segment-bytes 可写字节数或带单位（256MiB / 1GiB / 268435456）
replica_count   = 2
election_mode   = leader       # preferred leader 自动回切
flush.policy    = 每 1000 条或 5s（可关闭为纯页缓存）
drop_after      = 30s          # 迁移后前源节点本地副本的保留期，到期自动 DropSlot
                              # -drop-after / PUSHUPES_DROP_AFTER：正数=该时长，0=默认 30s，
                              # 负数启动即报错；没有关闭选项（留着会把节点写满）
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
├── go.mod                     # module pushupes
├── cmd/pushupes/              # 入口：装配 storage + cluster + api（admin）+ grpcapi（client）
├── internal/data/             # 领域类型、WAL 编解码、槽路由哈希、错误码
├── internal/storage/          # WalSegment / Slot(WAL) / Store：
│   │                          #   追加、幂等/版本校验、恢复、字节区间读
│   ├── segment.go  slot.go  store.go
├── internal/cluster/          # 控制面 cluster.go+fsm.go（Raft/分配表/路由表二进制编码
│   │                          #   table_bin.go），数据面 replication.go（PeerService 会话/ISR/HW）
│   │                          #   + peersvc.go（PeerService gRPC 服务端/客户端）+ peer.proto 生成码，
│   │                          #   peer_mux.go（peer 端口首字节分流 Raft/gRPC），
│   │                          #   migration.go（六步热迁移），register.go（地址自报 announcer+RPC）
├── internal/api/              # handler.go（仅管理：status/writes/plan/migrate/describe/pprof），server.go
├── internal/grpcapi/          # gRPC 客户端数据面：server.go（Append/ReadStream/ReadTails/ReadByCommand）
└── proto/pushupes/v1/         # events.proto 客户端契约 + peer.proto 节点间契约（buf 生成至 internal/grpcapi）
```

## 10. 设计要点小结

| 维度 | PushupES 选型 |
|------|------------|
| 基本单元 | 固定 1680 Slot（哈希分区） |
| 路由 | mix64(FNV-1a(aggregate_id)) % 1680 |
| 存储格式 | 自定义 Record 二进制（append-only WAL，段名即 baseSeq） |
| 分段/索引 | 256MiB 段 + 4KiB 稀疏索引（常驻） |
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

3.2：内存只需要「这个 command 是否可能存在」的廉价答案，精确答案交给读盘
（重复的 command 意味着客户端重放，不是热路径）。过滤器只在确定「不存在」时
跳过磁盘；未加载或标记 unsafe 时保守地答「可能存在」——索引缺失只会变慢，
绝不会出错。

3.3 的 `.aidx` 目前是 **v4**：目录项 `id hash(8)/firstVer(4)/count(4)/firstOrd(4)/listOff(4)`，
列表每条记录 **8B =（段内序数, 段内字节偏移）**。序数必须逐条存：同一段里某聚合
的记录与其他聚合交错出现，序数并不连续，用 `BaseSeq+firstOrd+i` 推算会指向
其他聚合的记录。有了偏移，「按版本读已封段」变成**一次 `readRecordAt` 直达**，
不再从 1MiB 粒度的稀疏提示往前走约 1000 帧。31GiB / 13.1 万个版本的聚合全量读
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

## 7.2 容量规划：槽数（SlotCount）与段大小（segment-bytes）的选型判据

以下常数与规则全部来自实测（31 GiB / 3250 万条 1 KiB 记录的加载与内存量测，
以及用仓库真实路由函数 `data.SlotOf` / `cluster.PlanSlots` 跑的离散度测量），
不是估算值。

### 7.2.1 每记录内存常数（决定 1 TiB / 10 亿条量级能否落在 16 GiB）

    布隆过滤器     1.25 B/条   （10 bit/条，k=3）— 只与「本节点记录条数」有关，与槽数、段大小都无关
    未封段 seq 列表 8 B/条      — 只算「未封段」里的条数，∝ 槽数 × 段大小
    稀疏索引       4 KiB/段     — ∝ 数据量 / 段大小
    加载期峰值     ≈ 2.8 × 活堆 （实测比）；加载期分配 ≈ 2.5 GiB / 3250 万条（已含扁平缓冲 + 池化）

活堆 ≈ 1.25·N_节点 + 8·(未封槽数·段大小/2)/记录大小 + 4 KiB·(数据量/段大小) + 0.15 GiB

因此 **内存由「记录条数」驱动，不由「数据量」驱动**：同样 1 TiB，1 KiB 记录
（10.7 亿条）比 10 KiB 记录（1.07 亿条）紧张一个数量级。段大小只影响
「未封段 seq」与「稀疏索引」两项，**布隆过滤器那份固定开销无法通过它削减**。

### 7.2.2 单节点失效时未封槽数 = 2K/N（与 K 无关）

`PlanSlots` 的放置规则是：leader = `nodes[s%N]`，副本 = `nodes[(s+i)%N]`（向前取 rf 个）。
撤销一个节点时它的每个槽的 leader 落到 `Replicas[0]`，即**死节点的全部槽只落到它的
下一个邻居**，其余节点负载不变。实测 3/5/7 节点、RF=2：失效后 max/mean 分别为
**1.334 / 1.600 / 1.717**，且对被测的所有 K 完全一致。也就是说：

- 规划内存必须按 **2K/N 个未封槽**计算（不是 K/N，也不是 K/(N-1)）；
- **调整槽数 K 无法缓解失效倾斜**，只有提高 RF 或改变副本环布局才能打散。

### 7.2.3 N | K 的不变性：多拓扑部署取 K 为 105 的倍数

实测：`(hash(id) % K) % N == hash(id) % N` **当且仅当 N 整除 K**（`hash` 指槽
路由哈希，验证时为 crc16；对全部 1e6 条 id、四种 id 形态验证，0 差异；
K=4096/2048/1024 则对 3/5/7 全部
不成立，K=1536/3072 只对 N=3 成立——因为 4096≡1 (mod 3,5,7)）。

含义：K 取 `lcm(3,5,7)=105` 的倍数（如 840 / 1680 / 3360）时，「聚合落在哪个
节点」是 `hash(id) % N` 的纯函数，**调整槽数不会把聚合换到别的节点**（只在本节点
内重排），3/5/7 三种拓扑同时成立。注意：**节点级均衡本身与 K 无关**（所有被测 K 的
max/mean ≤ 1.008），所以均衡不是选 K 的理由，这个不变性才是。

### 7.2.4 槽位分布均匀性：CRC16 对结构化 id 非均匀（已改为混合哈希）

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
迁移的搬运量。K 越小槽越粗：3 节点下每槽数据 K=840 时 2.3 GiB（1 KiB 记录）
/ 23 GiB（10 KiB），K=3360 时 0.58 GiB / 5.8 GiB。

### 7.2.6 推荐取值（3 节点最坏情形，RF=2，1 KiB 记录，要求扛住单节点失效）

    K     段      活堆    峰值     段数/节点  槽/节点(N=7)  每槽迁移量(1KiB/10KiB)
    840   256M    1.49G   4.16G    2543      120          2.3G / 23G
    840   512M    2.03G   5.68G    1272      120          同上
    1680  256M    2.04G   5.70G    2543      240          1.2G / 11.6G   ← 推荐
    1680  512M    3.12G   8.75G    1272      240          同上
    3360  256M    3.14G   8.78G    2543      480          0.58G / 5.8G
    4096  128M    2.29G   6.42G    5086      585          0.48G / 4.8G   ← 破坏 N|K 不变性

- **槽数固定为 `data.DefaultSlotCount = 1680`（无 `-slots`/配置入口）**；段大小默认
  `storage.DefaultSegmentBytes = 256MiB`（经 `-segment-bytes` 可配）
  （3/5/7 同时满足 N|K 不变性；失效态峰值 5.70 GiB，余量充足）。
- 段大小可按记录大小放宽：记录普遍 ≥4 KiB 可用 512MiB，≥10 KiB 可用 1GiB。
- 5/7 节点同参数下峰值只有 3 节点的约 60% / 45%，**按 3 节点规划即可覆盖所有规模**。
- 不要用 4096：它破坏 3/5/7 的 N|K 不变性，且要压到同样内存得把段缩到 128MiB
  （段数翻倍）。K 的上限是 65535（`SlotOf` 里 `uint16(slotCount)`）。
