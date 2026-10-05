# PushupES 自研 Raft 设计（替换 hashicorp/raft + raft-boltdb）

> 状态：**已实现并验收**（见 §10）。
> 范围：仅替换**共识层**（选主 / 日志复制 / 提交推进 / 快照）。
> 业务状态机（槽位分配表 `Table`）、数据面复制（mfetch / ISR / HW）、六步热迁移
> 全部不动，接口边界也不动（`Applier`、`Node` 的导出方法、FSM 语义）。

---

## 1. 现状与目标

### 1.1 替换前依赖第三方做什么

| 能力 | 原由谁提供 |
| --- | --- |
| 节点状态机（Follower/Candidate/Leader）、随机选主、心跳、term | `hashicorp/raft` |
| 日志复制（AppendEntries 一致性检查、冲突截断、prevLogTerm/commitIndex） | `hashicorp/raft` |
| 提交推进（多数派 matchIndex → commitIndex） | `hashicorp/raft` |
| 成员变更（Bootstrap / AddVoter / GetConfiguration） | `hashicorp/raft` |
| 日志与稳定状态存储（term/vote） | `raft-boltdb`（BoltDB 单文件） |
| 快照存储 | `raft.NewFileSnapshotStore`（文件） |
| 传输 | `raft.NetworkTransport`，跑在 `peerMux`（`raft.StreamLayer`）之上 |

代码里只有 4 个文件碰 Raft API（`cluster.go`、`fsm.go`、`peer_mux.go`、
`register.go` 一行 + `migration.go` 一处 Stats）+ 3 个测试文件。这是这次替换能低
成本完成的根本原因。

### 1.2 目标

1. 删掉 `github.com/hashicorp/raft`、`raft-boltdb`（连带 `bbolt`、`go-msgpack`、
   `go-metrics`、`go-hclog`、`go-immutable-radix`、`golang-lru`、`boltdb` 等
   indirect 依赖）。
2. Raft 自己的日志与稳定状态（term/vote）用**自研 WAL 文件**存（分段、CRC、
   崩溃可恢复），不再用 BoltDB。
3. 保持外部行为与接口不变：`Node.Apply / IsLeader / LeaderID / Peers /
   PeerAddrs / Stats / Close`、peer 端口仍然一个端口同时承载共识与 peer gRPC、
   `-peers` 语义不变。

### 1.3 明确不做（简化版边界）

- **不做动态成员变更**：成员集合 = `-peers` 在启动时确定并写入日志第 1 条
  （conf 记录）。运维增减节点 = 全集群重启并带上新 `-peers`。
- **不做 PreVote、不做 ReadIndex/租约读**：本项目的读全部走数据面
  （客户端按槽 leader 读本地 ≤HW 副本），共识层不承担读一致性职责。
- 不做 TransferLeadership、不做快照分块流式（表快照只有几 KB）。

### 1.4 安全前提

1. **提交前 fsync**：leader 把条目写进 WAL 并落盘后才计入多数派；follower 先落盘
   再 ack。否则多数派 ack 的日志可能在崩溃后消失。
2. **peer 端口分流的谜面冲突**：`peerMux` 靠「首字节 = 'P' → gRPC，其余 → 共识」
   分类，自研握手魔数**首字节必须 ≠ 'P'**，取 `0x9E`。

---

## 2. 架构

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

internal/cluster/          ← 只换实现，接口不动
  ├── cluster.go     Node 包装 raft.Node，装配改写
  ├── fsm.go         Apply(raft.Entry) any / Snapshot() []byte / Restore([]byte)
  └── peer_mux.go    实现自研 raft.Transport（形状不变，raftCh 分流逻辑不动）
```

分层原则照旧：**共识包不认识业务语义**，只认 `[]byte` 命令 + `FSM` 接口。

---

## 3. 持久化：自研 WAL

### 3.1 目录布局

```
<DataDir>/
├── wal/
│   ├── 000000000000000001.wal   # 段名 = 零填充段序号（不解析 entry）
│   └── 000000000000000002.wal
├── snapshot/
│   └── 000000000000000123.snap  # 单文件快照，文件名 = index
└── wal/LOCK                     # flock 目标
```

### 3.2 记录帧

```
len u32 | type u8 | crc32c u32 | payload[len]      # crc32c 覆盖 type+len+payload
```

记录类型：`1=entry(index/term/kind/data)`、`2=hardstate(term/vote)`、
`3=conf(voters)`、`4=truncate(index)`。

- **term/vote 与日志同一条流**：不引入第二个文件，恢复时取最后一条 hardstate，
  从根上杜绝「重启 term 归零」。
- **truncate 标记**：WAL 是 append-only，follower 冲突截断不能在中间删记录，
  改为写一条 `truncate(index)` 标记；重放时先丢弃 ≥ index 的内存条目再继续。

### 3.3 崩溃恢复

- 启动按段序号顺序扫描，遇到读失败 / CRC 不符 / 长度越界 → **截断该段到最后一
  条有效记录**，删除更后面的段，记 WARN。只丢尾部，不整库报废（BoltDB 的对比点）。
- `flock(LOCK_EX|LOCK_NB)` 锁 WAL 目录：拿不到锁**立即报错**（不是无限等），
  `Close()` 保证释放——这是进程内重启必须能重开的前提。

### 3.4 组提交（性能关键）

```
Append(typ, payload) → 编码入 buffer，返回 LSN（不落盘）
flusher goroutine    → 等 dirty 信号或 200µs tick → write + fdatasync → 唤醒
                       LSN ≤ durable 的等待者，并回调 OnDurable(durableLSN)
Wait(lsn)            → 阻塞到自己的 LSN 落盘
```

- 顺序写路径：1 命令 1 fsync。
- 并发/批量路径：N 条摊成 1 次 fsync（实测 200 条 / 1 次）。

---

## 4. 快照

- 触发：`applied - snapIndex >= SnapshotThreshold`（默认 1024）或 30s 间隔到点且
  有新增应用。
- 形式：`snapshot/<index>.snap`，头部 `magic ver index term len crc`；写完
  `fsync 文件 → rename → fsync 目录`；保留最近 2 份。**无 meta.json 指针**——
  最新快照 = 目录里编号最大且 CRC 通过的那个，少一个会互相矛盾的原子写。
- **不变式**：快照基线只能裁掉日志的**前缀**（≤ 基线的内存条目），基线之上的
  条目一律保留；`commitIndex` 不得超过 `LastIndex`。

---

## 5. 共识：线程与不变式

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

---

## 6. 线上协议（peer 端口）

- **分流不变**：`'P'` → HTTP/2 前导 → gRPC；其余 → 共识层。自研握手首字节
  `0x9E`，**断言 ≠ 'P'**（有单元测试守着）。
- **握手**：`magic(1) | version(1) | nodeID(u8 长度 + bytes)`；acceptor 回同样格式
  的 ACK。没有 role 字节——一条连接是**双向**请求/响应复用，双方都可能在同一条
  连接上发请求。
- **帧**：`len u32 | reqID u64 | kind(0=req 1=resp) u8 | type u8 | payload`。
- **连接模型**：per-peer **长连接**（懒拨号、写互斥、按 reqID 分发响应、断开即
  失败在途请求）；acceptor 侧只回不该连接上的请求，实现对称且简单。

---

## 7. 对现有代码的改动清单

| 文件 | 改动 |
| --- | --- |
| `internal/raft/`（新增 10 文件） | 全部自研实现，无第三方 |
| `internal/cluster/cluster.go` | `raft.Raft` → `*raft.Node`；装配改写；静态 voter 集；删 `Join`/`startAutoJoin`/Bootstrap |
| `internal/cluster/fsm.go` | `Apply(raft.Entry) any` / `Snapshot() ([]byte,error)` / `Restore([]byte) error` |
| `internal/cluster/peer_mux.go` | `raft.StreamLayer` → 自研 `raft.Transport`；`Addr() string` |
| `internal/cluster/register.go` | `LeaderWithID()` → `LeaderID()` |
| `internal/cluster/migration.go` | Stats key 兼容两种写法（连字符/下划线） |
| `cmd/pushupes/main.go` | +`-raft-flush-interval`(200µs) / `-raft-segment-bytes`(64MiB) |
| `scripts/cluster.sh` | 不再传 `-bootstrap`（静态成员下无意义） |
| `go.mod` | 删 3 个直接依赖 + 一串 indirect |

**不改**：`internal/storage/*`、`internal/data/*`、`internal/grpcapi/*`、
`internal/api/*`、六步热迁移、ISR/HW、再平衡。

---

## 8. 测试与验证方案

- 单元：WAL（崩溃截断只丢尾部、跨段重放、flock 互斥、CRC 篡改检出、组提交合并）、
  内存日志、快照与日志自洽、term/vote 跨重启、成员集合不匹配拒绝启动。
- 进程内真实 TCP 集群：拓扑 1/3/5/7 各跑「选主 → 写批 → 杀 leader → 幸存者选主
  → 再写 → 全体 applied 与 last 一致」，**每拓扑多轮**；`-race` 全绿。
- 端到端：`scripts/cluster.sh` 三段（见 §10.3）。

---

## 9. 用户拍板记录

（2026-10-05）

1. 成员集合**静态**固定 —— 接受。
2. WAL **自己写**，不剥 `internal/storage` 的 Segment —— 采纳。
3. 传输用 **per-peer 长连接**复用 —— 采纳。
4. 快照**单文件 + 扫目录取最新**，不做分块流式 —— 可以。
5. `kill -9` 故障切换 + 重启追平作为**硬性验收**，A/B 数字写进文档 —— 照此执行。

---

## 10. 实现结果

### 10.1 交付物

见 §2 的目录清单；`go.mod` 直接依赖从 7 个降到 4 个，`hashicorp/raft`、
`raft-boltdb`、`go-hclog` 及其带进来的 `bbolt`/`go-msgpack`/`go-metrics`/
`go-immutable-radix`/`golang-lru`/`boltdb` 全部消失；
`grep -rn "hashicorp/raft" --include=*.go .` 为空。

### 10.2 与设计稿的偏差（实现中改进）

1. 握手去掉 role 字节（见 §6）。
2. WAL 段名改为零填充段序号，而非 baseIndex；压缩按「段 lastLSN < 目标 LSN」整段删。
3. follower 的 ack 异步化；leader 的 Apply 先复制再等本地 fsync（见 §5）。
4. 两条防御性不变式（都是本轮踩到的真 bug）：
   - 快照基线只能裁日志**前缀**；裁成后缀会把 applied 游标推过日志末尾，
     每个后续 Apply 都白等满超时。
   - `commitIndex ≤ LastIndex` 每次 apply 前 clamp。
5. `-bootstrap` 保留为 no-op。

### 10.3 验证（实跑）

- `go build ./... && go vet ./...` 通过；`go test ./... -count=1` 全绿
  （含 1/3/5/7 拓扑多轮与快照回归用例）。
- `go test -race ./internal/raft` 全绿。race detector 曾抓出 `peerClient.write()`
  无锁读 conn 字段与 `close()` 并发写 → 改为把连接句柄按值传入。
- 端到端（3 进程真实集群）：`clean → start → smoke → slotcheck` 全绿；
  `kill -9` leader → 2s 内新 leader 选出 → 重启被杀节点 → 追平，
  `slotcheck` diverged=0、`smoke` 通过；`restart`（保留数据）后槽表不乱。

### 10.4 A/B 实测

环境：容器 2 核（GOMAXPROCS=2，并发档实际最多 2 路并列），命令体 256B，真实 TCP，
`-benchtime 3s -count 3` 取中位数。A = `hashicorp/raft` v1.7.3 + `raft-boltdb`
v2.3.1（BoltStore + FileSnapshotStore）；B = 自研 `internal/raft` + 自研 WAL。
A 侧基准代码临时置于 `internal/raft/ref/`，取数后已连同依赖删除。

| 场景 | A ns/op | B ns/op | 吞吐差 | A p50 / p99 | B p50 / p99 | A B/op | B B/op |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 单节点顺序 | 2 098 000 | 1 464 000 | **+43%** | 2.03ms / 2.87ms | 1.40ms / 1.94ms | 22 078 | 2 135 |
| 单节点并发16 | 2 076 000 | 1 454 000 | **+43%** | 4.13ms / 7.05ms | 2.84ms / 3.72ms | 21 700 | 2 135 |
| 3 节点顺序 | 5 008 000 | 5 521 000 | **−9%** | 4.85ms / 7.64ms | 5.55ms / 6.68ms | 77 600 | 16 022 |

- 分配：单节点 108 → 17 allocs/op（−84%）。
- 每命令 fsync：A 每 Apply 一次 Bolt 事务；B 顺序档 1 次/命令，批量/并发档由组提交
  摊薄（实测 200 条并发写入在 5ms 窗口只产生 1 次 fsync）。
- 判据核对：吞吐单节点大幅领先、3 节点 −9%（在 ±10% 界内）；**p99 三档全部优于
  A（未劣化）**；内存/分配大幅下降。**达标**。

### 10.5 已知边界（不在本次范围）

- `InstallSnapshot` 仍在 runLoop 内同步写盘（仅 follower 落后到基线时才走，表快照
  几 KB）。
- WAL 写失败的极端情况下，已挂起的 follower ack 靠 RPC 超时回收（写失败即节点
  不可用，不做额外降级）。
