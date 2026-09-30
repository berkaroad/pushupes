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
├── 固定 SlotCount = 4096（可配置，容量预留式：槽数远大于节点数）
├── slot = CRC16-XMODEM(aggregate_id) % SlotCount
└── 每个 slot 对应一条 WAL（按 256MiB 分段），支持在集群中热迁移
```

两套编号，职责不同：

- `version`：**聚合内**连续版本号，是 CQRS 乐观锁语义（业务规则 2）。
- `seq`：**槽内**全局单调写序列号（每个 slot 一个计数器），是复制/迁移的
  物理偏移坐标：follower 按 seq 拉取、迁移按 seq 追增量。

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
不同槽并行。4096 槽 = 最多 4096 条并行写路径。

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
- **稀疏索引**：每 4KiB 记一个 `seq→(段文件, 偏移)` 内存索引，常驻 footprint
  与数据量解耦；按 version 读聚合时经聚合索引定位 seq 区间，再二分稀疏索引。
- **重启恢复**：加载各 slot 段列表 → 从最后一段 `scanBody` 顺序扫尾，
  重建三个内存索引与 seq 计数器；**不设独立 meta.json**（避免元数据与 WAL
  双写不一致的竞态）。
- 崩溃尾部撕裂：扫描遇不完整记录即在完整边界截断。
- **刷盘策略**：flush.policy 可配（每 N 条 / 每 T 毫秒 fsync；默认组提交
  依赖页缓存）；`acks=all` 时等 ISR 高水位而非本地 fsync。

## 4. 高可用（复制设计）

分层结构：

- **控制面（Raft）**：hashicorp/raft 集群只复制元数据——
  slot 分配表（`slot → {leader, replicas[], epoch, state}`）、集群成员、
  节点数据面地址（`OpRegister`，见 §6 peer 面）。FSM 模型：
  `Applier` 接口 + 快照/恢复（快照为二进制表编码 `table_bin.go`，4096 槽
  ~350KB JSON → 几 KB）。成员集合由 Raft 配置唯一决定；路由表里每个成员
  同时记 `PeerAddr/AdminAddr/ClientAddr` 三个地址。
- **数据面（专用拉取）**：
  - 客户端把 `Append` 发给槽 leader 的 gRPC client 面；leader 追加本地
    WAL 得到 seq。
  - follower 把「我作为副本跟随的槽」按 leader 分组，**每个 leader 一条
    常驻 PeerService.MFetch 长轮询会话**（gRPC，`peer.proto`，与 Raft 共用
    peer 端口；payload 是裸 WAL 字节区间 `FetchItem.payload`，不经 base64、
    事件体不重编码、不进 Raft 日志），一次请求多槽复用、空闲轮零额外往返。
    请求位置用 **packed 并行数组**（`slots[i]` 从 `from_seqs[i]` 起）而非
    每槽一个消息对象——一个会话每轮携带全部跟随槽（可达数千），逐条对象
    的构造/序列化在 `acks=all` 下主导了 CPU；响应则是**稀疏**的：只有带货
    的槽出现在 `items` 里，空槽靠缺席表达。leader 端长轮询不 per-slot 建
    select 分支，而是 watch 一条**全 store 唤醒总线**（任一槽 append 触发），
    命中后对 parked 槽的各自句柄做非阻塞扫描判定"哪些槽真的动了"。
  - follower 追加到自己 WAL 后回报 LEO（批量 PeerService.ReplicaProgress）；
    leader 推进 **HW**（高水位 = ISR 内最小 LEO）。**follower 的落盘是帧直写**：
    收到的裸 WAL 字节按 seq 校验后**原样写入**（`Slot.AppendFrameAtSeq` /
    `Segment.AppendFrame`），内存索引只从帧头解析（`data.DecodeRecordMeta`，
    不物化事件体）——复制路径不 decode 成记录再 re-encode，100KiB 事件体下
    每条省掉两份大分配与两次全量拷贝（迁移写转发 `Replicate` 同路径）。
    leader 端有全局
    payload 预算 + 轮转游标，防积压槽饿死。应答语义：
    **请求到达即有货的槽立即随响应返回（有货即答，不长轮询滞留）**，
    全空才 park 进长轮询；park 后任一槽被 append 唤醒时做 **burst
    drain**——短暂合并窗口内扫全部 waiter，一次响应带走本轮所有有货
    的槽（否则每个槽各付一个 RTT，HW 才能推进，`acks=all` 的写延迟
    直接乘以槽数）。预算耗尽但有货的槽不 park（新句柄等不到"未来"的
    append，会空睡到 deadline），交给下一轮轮转游标。follower 侧健康
    轮（有数据或被长轮询吸收的空轮）零退避立即重发，仅传输错误退避。
  - 记账成本 O(变化)：leader 对每轮捎带的全部位置做"比对后跳过"——LEO
    未变的条目不写状态、不重算 HW。副本的**存活戳**（lastOK，掉出 ISR
    的判定依据）不靠每轮全量写，而是每 ~2s 一次 **sweep 轮**（MFetch 的
    sweep 位）为所有条目补戳，sweep 间隔远小于 stale 窗口；有数据的轮
    只对真正推进过的槽即时补报 LEO（ReplicaProgress，不再全量重发）。
  - `acks=all`：leader 等 `seq ≤ HW` 再回 success；`acks=leader`：本地追加
    成功即返回；`acks=none`：不等响应。
  - ISR 维护：follower 在 `replica.lag.time.max` 内跟上进 ISR；
    掉出后按自己 LEO 重新追。
- **故障切换**：controller（Raft leader）每 1s 经 peer 面 `PeerService.Ping` 探活，
  连续 3 次失败判定失联 → 从剩余 ISR 副本为该节点名下所有槽重选 leader
  （epoch+1，经 Raft 提交）→ 客户端收到 MOVED 重定向。无 peer 地址
  （PeerAddr 为空）的 peer 不参与探活，避免启动竞态误杀。`election_mode=leader`
  （默认）：原 preferred leader 恢复后自动回切，减少抖动。
- **默认拓扑**：`replica_count=2`（每槽 1 leader + 1 follower，散布不同
  节点），`slot_count=4096`（可配置）。3 节点以上时副本环按
  `slot % N` 起、向前取 `replica_count` 个节点。
  新成员加入只做增量补位（`replan_slots` 填未分配槽/补足副本），不扰动
  有数据的槽。

## 5. 槽位热迁移（文件级搬运）

槽是迁移的原子单位；WAL 段文件自包含 → 迁移大部分是**整文件拷贝**，
不解析事件。这是槽设计相较"按聚合迁移"的核心优势。

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
6. **清理**：S 确认新 epoch 生效后隔离本地槽数据（保留到
   `migration.retention` 到期防回滚），置 stable。

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
  `ReadStream` + `ReadByCommand`）。body 为原始 bytes，无 JSON/base64 层。
  MOVED/ASK 重定向的 `node` 字段携带槽 leader 的 **client 地址**（路由表
  Peer 同时记 PeerAddr/AdminAddr/ClientAddr），客户端据此重连。本节点既无槽又无副本
  时，服务端向 leader 的 client 面代理转发（`ReadProxyAddr` 返回 leader
  client 地址）。
- **admin 面（HTTP，默认 `-admin http://127.0.0.1:8091`，`PUSHUPES_ADMIN`）**：**仅管理**——
  status/writes/plan/migrate/槽 describe/healthz/pprof，**不再承载任何节点间流量**
  （原 `/internal/*` 复制/迁移端点已全部迁到 peer 面 PeerService gRPC，admin HTTP
  路由已删除）。事件 body 是任意字节，在 gRPC 面端到端裸传（proto `bytes`），
   **append 回显是不对称的**：`exists` 回显存储记录本体（`*EventRecord`，零
  指针搬运；重放方唯一无法自行还原的信息），`success` 只回 status/seq/version
  ——调用方手里就是刚发的那条记录，回显 100KiB body 等于每条 append 多付一次
  marshal + 整包往返（实测同负载 627→762 msg/s、p50 5.69→5.09ms、每条消息
  CPU -16%，其中还含客户端省下的一次 unmarshal）。不存在
  JSON 包装平面——旧 `{"_b64":...}` 方案会在大 body 上付出 json.Valid 全量
  校验 + base64 重建 + 解码往返的 CPU（实测 100KiB body 下占 leader CPU 一半），
  已随 HTTP 事件面退役删除。
- **peer 面（Raft + PeerService gRPC，默认 `-peer http://127.0.0.1:8391`，`PUSHUPES_PEER`）**：复制槽位分配表等元数据（Raft），并承载**全部节点间数据面**——副本拉取、LEO 上报/探活、迁移快照/段拷贝/写转发/LEO 追平、地址注册协议（proto3 契约 `proto/pushupes/v1/peer.proto`，`pushupes.v1.PeerService`，服务端 `peersvc.go`）。**运维只配这一个端口**：`-peers` 主格式 `node-id=http://ip:peerport`（或裸 `ip:peerport`，地址兼作节点 id）。**地址统一规范：存储/路由表/status JSON 中所有 admin/client/peer 地址都带 scheme——未写协议默认补 `http://`，显式协议以传入为准；TCP 拨号（listen/dial/gRPC/Raft transport）前再剥掉 scheme**（`cluster.NormalizeAddr`/`HostPort`）。admin/client 地址不配置，由各节点自报进路由表：
  - peer 端口上是复用监听器（`peerMux`/`peer_mux.go`，实现 raft.StreamLayer）：按连接**首字节 `P`**（HTTP/2 client preface `"PRI ..."` 以 `P` 开头）把 PeerService gRPC 流量与 Raft 流量分流（Raft 线上协议首字节是版本号 0，永不冲突；两路都经 `replayConn` 回填被 peek 消费的字节）。gRPC 服务端由 `Engine.ServePeer` 挂在该分流 listener 上，peer 面与 Raft 从此共用一个端口、一套 gRPC 语义（HTTP/2 多路复用，每对节点一条缓存连接 `peerClient`，keepalive 10s，服务端放宽 enforcement）。
  - leader 收到注册 RPC 后提交 `OpRegister`（就地修补路由表中该成员的 `AdminAddr/ClientAddr`，不新增成员——成员集合仍由 Raft 配置决定）；follower 收到则转发给 leader 的 peer 地址。
  - announcer 幂等周期重试（启动期 1s，收敛后转 10s 心跳），任意启动顺序都能收敛；节点换端口重启也会被自报值修补。
  - 旧多端口格式 `id:peerport:adminport:clientport` / `id:host:peerport:adminport:clientport` 仍兼容（作为静态种子，注册落地后以自报值为准）。

```
gRPC  EventService/Append         写入：幂等(command_id)/版本(+1)/acks，MOVED/ASK
gRPC  EventService/ReadStream     范围查询（≤HW 语义）
gRPC  EventService/ReadByCommand  command_id 幂等探针

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
GET  /admin/slots/{slot}/describe              # 槽状态/seq/HW/大小（管理）
GET  /admin/cluster/status                     # 分配表/epoch/ISR 视图（含 client_addr）
GET  /admin/writes                             # 每槽 durable 计数 + 槽内总字节/事件流数量（前端轮询）
GET  /admin/slots/{slot}/streams?after=&limit=  # 事件流列表（聚合id+最新版本，仅内存索引，不读 WAL）
POST /admin/slots/{slot}/migrate  {to_node}    # 发起热迁移
POST /admin/cluster/plan                       # 触发重新规划
GET  /healthz
```

读语义只暴露 `seq ≤ HW` 的记录（投影不读未达高水位的数据，防脏读回滚）。

槽位事件流列表（`/admin/slots/{slot}/streams`）回答的是**槽内索引**：每条事件流的
聚合 id + 最新版本号。索引在开槽时只解析记录的**帧头**（`data.DecodeRecordMeta`：
聚合 id/版本/长度，event body 既不拷贝也不解码；与 follower 帧落盘同一套 walk），
因此取列表**不读任何 WAL 文件**。本节点未打开过的槽返回 `loaded:false` 而不是现开
——开槽意味着遍历该槽的全部分段（正是该接口要避免的全文件扫描），交给持有该槽的
节点（leader/副本，前端按 placement 选 admin 地址）回答；分页用
`?after=<aggregate_id>` 游标顺序游走，一页只材料化 limit 条（有界堆选，内存
O(limit)，与槽内聚合数无关），单次响应上限 `MaxStreamPage=1000`，默认
`DefaultStreamPage=200`。**槽位列表的总字节/事件流数量走同一套「不开槽」口径**：
`GET /admin/writes` 在 durable 计数之外再回两条按槽下标对齐的数组 `bytes[]`/`streams[]`
（已加载槽的真实值，未加载槽读 0 而不去开它——开槽就是遍历该槽全部分段），前端
沿用已有的 2s 轮询取「占有该槽的节点中口径最大的那个」作答，不额外发请求。

## 7. 高性能要点

- **顺序追加 + 页缓存**：写路径 = memcpy 进段缓冲 + write()；fsync 交给
  flush 策略/acks 语义，组提交合并（dirty 集合定向刷盘，不遍历全槽）。
- **二进制协议面**：节点间流量全部 PeerService gRPC（protobuf 二进制帧，
  `FetchItem.payload`/`PushSegmentsRequest.data` 为裸 WAL 字节区间，不经
  base64/重编码）；客户端事件面 gRPC protobuf，body 原始 bytes。
  路由表快照同为二进制编码（字符串字典 + varint + gzip）。
  **sendfile 零拷贝已实测否决**：payload ~40-64KiB、需用户态解码过滤、
  Go netpoller 非阻塞 fd——三个收益条件全不满足；syscall 优化走减少
  往返（多槽复用会话、长轮询、批量 LEO 上报、组提交），不是减少拷贝。
- **稀疏索引常驻**：内存索引 = 4096 槽 ×（聚合版本 map + command map +
  seq 稀疏索引），索引成本每记录几十字节，可支撑亿级聚合。
- **写不放大**：一条记录只在槽 WAL 落一次；副本走日志拉取而非双写。
- **锁竞争**：slot 级锁把热点聚合关进单槽（热点从"锁全库"降为"锁
  1/4096"）；读路径短临界区 RWMutex。
- **启动恢复并行化**：OpenStore 两阶段（先扫目录再 worker 池并行开槽）。

## 8. 配置默认值

```
slot_count      = 4096         # 固定 4096，可配置（容量预留式）
segment_bytes   = 268435456    # 256MiB 分段
replica_count   = 2
election_mode   = leader       # preferred leader 自动回切
flush.policy    = 每 1000 条或 5s（可关闭为纯页缓存）
acks_default    = leader
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
├── internal/data/             # 领域类型、WAL 编解码、CRC16 槽路由、错误码
├── internal/storage/          # WalSegment / Slot(WAL) / Store：
│   │                          #   追加、幂等/版本校验、恢复、字节区间读
│   ├── segment.go  slot.go  store.go
├── internal/cluster/          # 控制面 cluster.go+fsm.go（Raft/分配表/路由表二进制编码
│   │                          #   table_bin.go），数据面 replication.go（PeerService 会话/ISR/HW）
│   │                          #   + peersvc.go（PeerService gRPC 服务端/客户端）+ peer.proto 生成码，
│   │                          #   peer_mux.go（peer 端口首字节分流 Raft/gRPC），
│   │                          #   migration.go（六步热迁移），register.go（地址自报 announcer+RPC）
├── internal/api/              # handler.go（仅管理：status/writes/plan/migrate/describe/pprof），server.go
├── internal/grpcapi/          # gRPC 客户端数据面：server.go（Append/ReadStream/ReadByCommand）
└── proto/pushupes/v1/         # events.proto 客户端契约 + peer.proto 节点间契约（buf 生成至 internal/grpcapi）
```

## 10. 设计要点小结

| 维度 | PushupES 选型 |
|------|------------|
| 基本单元 | 固定 4096 Slot（哈希分区） |
| 路由 | CRC16(aggregate_id) % 4096 |
| 存储格式 | 自定义 Record 二进制（append-only WAL，段名即 baseSeq） |
| 分段/索引 | 256MiB 段 + 4KiB 稀疏索引（常驻） |
| 共识层 | Raft 只复制元数据：slot 分配表/epoch/节点地址注册 |
| 客户端协议 | gRPC EventService（事件读写唯一入口） |
| 数据复制 | PeerService.MFetch 会话（gRPC/HTTP2 与 Raft 同端口，seq 坐标，多槽复用长轮询，有货即答） |
| 可靠性语义 | ISR + HW（`acks=all` 等 seq≤HW） |
| 再平衡 | 槽位热迁移（快照+增量+转发窗口，六步不停写） |
| 幂等 | command_id 槽内索引 |
