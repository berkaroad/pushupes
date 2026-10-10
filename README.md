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

## 功能特性

### 数据模型与写入协议

- 事件记录 `EventRecord`：`aggregate_id` + `version`（聚合内连续版本号，从 1
  严格 +1 递增）+ `unix_time` + `command_id`（幂等键）+ `events[]`（type + 原始
  bytes body，端到端裸传，无 JSON/base64 包装层）。
- **幂等**：`command_id` 命中槽内索引 → 返回 `exists` + 已存储记录本体。
- **乐观锁**：版本必须 +1，冲突返回 `fail` + 错误 ID `1001` + `current_version`，
  客户端可据此校准后重试。
- 两套编号各司其职：`version` 是业务版本坐标，`seq`（槽内单调序列号）是
  复制/迁移的物理偏移坐标。

### 存储

- 固定 **1680 个哈希槽**（`mix64(FNV-1a(aggregate_id)) % 1680`）。槽数不可配置，
  取 105 的倍数是为了让「聚合落在哪个节点」不随槽数调整而改变（推导见
  DESIGN.md §7.2）。
- 每槽一条 **append-only WAL**，默认按 256MiB 分段（可配 64MiB~2GiB，须为 64MiB
  的整数倍）。**段文件名即 baseSeq**，整段可独立滚动、拷贝、删除，因此槽级
  热迁移的大部分工作就是整文件搬运。
- 每段附带追加式的**派生索引文件**（`.idx`/`.agx`/`.spx`/`.cidx`/`.aidx`），
  支撑命令幂等查重（布隆过滤器 + 磁盘二分）与聚合按版本直读（靠段内序数与
  字节偏移，一次 pread 直达）。索引全部是派生数据：缺失、损坏或滞后时一律
  退化为全量帧头扫描，**正确性不依赖索引**。
- 刷盘策略可配（默认每 1000 条或每 5s 组提交 fsync）；写入确认等的是 ISR
  高水位，不是本地 fsync。
- 崩溃恢复：启动时并行打开各槽，一次窗口遍历同时完成「找到尾部完整记录的
  边界（丢弃撕裂的尾巴）+ 重建索引」，只解析帧头、不物化事件体。

### 高可用与复制

- 控制面：**自研简化 Raft**（`internal/raft`，日志与 term/vote 存于自研分段
  WAL `internal/raft/wal.go`）复制槽位分配表
  （`slot → {leader, replicas[], epoch, state}`）与成员；成员集合静态（由
  `-peers` 启动时写入）；路由表快照为二进制编码（几 KB 级）。
- 数据面：follower 把要跟随的槽按 leader 分组，**每个 leader 维持一条常驻的
  MFetch 长轮询会话**（gRPC/HTTP2 多路复用，一次请求合并多个槽，payload 是
  裸 WAL 字节；有数据立即应答，唤醒后合并窗口内的全部增量一次带走）。
  follower 收到帧后原样落盘（不重新编解码），批量回报 LEO，leader 据此推进
  **高水位 HW = ISR 内最小 LEO**。
- 写入成功的条件：leader 必须等到 ISR 高水位覆盖该条记录（所有同步副本都
  已把它写进自己的 WAL）才回 success——回 `success` 的写入不会因单节点宕机
  而丢失。副本全部掉出 ISR 的极端情况下，高水位跟随 leader 推进以保证可用性
  （承诺只对同步副本成立）；等待超过 10s 则返回失败，记录此时已在 leader 的
  WAL 中，客户端用同一 `command_id` 重试命中 `exists` 即可确认。
- 故障切换：controller 每 1s 探活，某节点连续 3 次失败即判定失联 → 从其 ISR
  副本中为该节点名下所有槽重选 leader（epoch+1，经 Raft 提交），客户端收
  MOVED 重定向；原 leader 恢复后自动回切（`election_mode=leader`）。
- 读语义只暴露 `seq ≤ HW` 的记录（防脏读回滚）；非本地槽的读由服务端向
  leader 代理转发。

### 槽位热迁移（不停写）

六个步骤：准备（Raft 置 migrating/importing）→ 快照拷贝（已封段经
client-streaming 分块流式搬运，≤4MiB/块，两端内存只占单块大小）→ 增量追平
（与复制走同一协议）→ 写转发窗口（毫秒级）→ 切换提交 → 清理（前源节点本地
副本保留 `-drop-after` 时长后自动删除，默认 30s）。

切换提交内置**写栅栏**：源节点先排空在途写入、追平目标才允许移主，并保持
栅栏直到目标也应用了移主——杜绝新旧 leader 同 seq 写入分叉，也避免客户端在
两个节点间来回弹跳。

- 迁移到**副本集以外**的目标：一次 API 调用内自动完成「加入副本集 → 追平 →
  移主 → 回收前源」三步。
- 客户端路由：本地缓存 `slot → node`，收到 MOVED(1003)/ASK(1004) 即刷新；
  幂等规则保证转发期间的写入不产生重复。

### 内存与容量设计

内存开销主体由**记录条数**驱动。实测常数：命令布隆过滤器 1.25 B/条、未封段
seq 8 B/条、段稀疏索引 16 B/每 64 KiB 数据。配合「封段后把 seq 移出内存」与
派生索引文件，31 GiB / 3250 万条数据的节点活堆约 110 MiB（其中稀疏索引约
8 MiB，按实测常数推算），据此可规划 1 TiB/节点、16 GiB 内存的部署（选型判据
见 DESIGN.md §7.1/§7.2）。

### 管理控制台（frontend/）

React 18 + antd 5 + Vite，提供集群页（含运行中添加 Raft 节点）与槽位管理：槽位表（状态/副本/总字节、
迁移弹窗、待清理副本置灰）、槽详情抽屉（HW/LastSeq/ISR + 写入速率折线图，
2s 轮询）、事件流列表（只读内存索引，不触发 WAL 扫描）。支持 light/dark
主题。

## 部署与启动

### 构建

要求 Go 1.26+：

```bash
go build -o bin/pushupes ./cmd/pushupes
go build ./... && go vet ./... && go test ./... -count=1   # 全量门禁
```

前端（可选，构建产物 `frontend/dist` 供静态托管）：

```bash
cd frontend && npm install && npm run build
```

### 一键起 3 节点集群（推荐）

```bash
scripts/cluster.sh start     # 编译 + 拉起 3 节点，等待选主与槽规划收敛
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
`REPLICAS`（节点数）、`HOST`、`ADMIN_BASE`、`PEER_BASE`、`CLIENT_BASE`、
`SEGMENT_BYTES`、`RUN_DIR`、`READY_TIMEOUT`、`BUILD`。每槽副本数由副本策略分档
（`PUSHUPES_REPLICA_POLICY`=low/medium/high，默认 medium：1/2/容错节点数+1 份）：
该值只为新集群种初始值，策略存放在 Raft 复制的槽表里，之后改档位走
`POST /admin/cluster/replica-policy`（控制台集群页的「副本策略」卡片即可操作）。
扩节点两种做法等价：`REPLICAS=<新总数> cluster.sh start`（集群已存在时，缺的节点
自动报名加入）或逐个 `cluster.sh join N`。

集群配置只由**一个**节点写下（`-peers` 里 id 最小的那个，或用 `-bootstrap` 指定的
那个）；其余配置了 `-peers` 的节点一律以「种子」身份启动、向这些成员报名加入，自己
不写配置。所以「把新节点用更大的 `-peers` 列表拉起来」就是扩容，它不会和现有集群
各写一份配置而分裂成多个各自能提交的 raft 组。

### 手工启动单个节点

```bash
# 全新集群：节点同一次拉起、给同一份 -peers。写初始配置的是 id 最小的节点（node-1），
# 其余节点以种子身份启动并向它报名 —— 不需要额外参数。
# peers 主格式 node-id=host:peerport（等号分隔，只配 peer 端口，admin/client 地址由
# 各节点经注册协议自报进路由表；每槽副本数由成员数推导）
./bin/pushupes -node node-1 \
  -peer 127.0.0.1:8391 -admin 127.0.0.1:8091 -client 127.0.0.1:8591 \
  -data ./node-1 \
  -peers 'node-1=127.0.0.1:8391,node-2=127.0.0.1:8392'

./bin/pushupes -node node-2 \
  -peer 127.0.0.1:8392 -admin 127.0.0.1:8092 -client 127.0.0.1:8592 \
  -data ./node-2 \
  -peers 'node-1=127.0.0.1:8391,node-2=127.0.0.1:8392'

# 已有集群上加一个节点：-peers 列出成（旧+新）成员即可，node-3 会向它们报名；-
# join 只想指定某一个成员时才需要。要单独启动「第一个」节点时才用 -bootstrap。
./bin/pushupes -node node-3 \
  -peer 127.0.0.1:8393 -admin 127.0.0.1:8093 -client 127.0.0.1:8593 \
  -data ./node-3 \
  -peers 'node-1=127.0.0.1:8391,node-2=127.0.0.1:8392' \
  -join 127.0.0.1:8391
```

地址规范：地址统一带 scheme 存储，未写协议时默认补 `http://`；`-peers`
也接受裸 `host:peerport`（此时地址兼作节点 id）。

### 启动参数

| flag | env | 默认 | 说明 |
|---|---|---|---|
| `-node` | `PUSHUPES_NODE` | `node-1` | 节点 id |
| `-peer` | `PUSHUPES_PEER` | `http://127.0.0.1:8391` | peer 面：Raft + PeerService gRPC（全部节点间流量，运维只需配这个端口） |
| `-admin` | `PUSHUPES_ADMIN` | `http://127.0.0.1:8091` | admin 面：HTTP 管理 API + pprof |
| `-client` | `PUSHUPES_CLIENT` | `http://127.0.0.1:8591` | client 面：gRPC 事件读写唯一入口 |
| `-data` | `PUSHUPES_DATA` | `./data` | 数据目录 |
| `-peers` | `PUSHUPES_PEERS` | 空 | 集群种子 `id=host:peerport,...` |
| `-join` | `PUSHUPES_JOIN` | 空 | 启动时向指定成员（`host:peerport`）报名。可选：不写时，非 `-peers` 首位的节点也会按配置成员轮转报名，`-join` 用于指定其中一个 |
| `-adopt-interval` | `PUSHUPES_ADOPT_INTERVAL` | 3s | 尚未成为集群成员的节点每隔多久重试一次报名 |
| `-bootstrap` | — | false | 由本节点写下集群的初始配置。默认由 `-peers` 里 id 最小的节点写；其余配置了 `-peers` 的节点以种子身份启动、向这些成员报名加入，自己不写配置 |
| `-replica-policy` | `PUSHUPES_REPLICA_POLICY` | medium | 副本策略档位：`low`=每槽 1 份、`medium`=2 份、`high`=容错节点数+1（`floor((N-1)/2)+1`）。**只用于新集群的初始值**：策略存放在 Raft 复制的槽表里，之后只能经 `POST /admin/cluster/replica-policy` 修改，重启不会重刷；任何一档的因子按成员数钳制（1 成员时都是 1 份）。`GET /admin/cluster/status` 的 `replica_policy` / `replica_factor` 读回当前值；非法值启动即报错 |
| `-flush-messages` | `PUSHUPES_FLUSH_MESSAGES` | 1000 | 每 N 条 fsync（0 关闭） |
| `-flush-interval` | `PUSHUPES_FLUSH_INTERVAL` | 5s | 每周期 fsync（0 关闭） |
| `-segment-bytes` | `PUSHUPES_SEGMENT_BYTES` | 256MiB | WAL 段滚动大小：64MiB 整数倍，≤2GiB；可写字节数或带单位（`256MiB`/`1GiB`），env 同样接受带单位形式 |
| `-fetch-settle` | `PUSHUPES_FETCH_SETTLE` | 200µs | fetch 轮被数据唤醒后的合并窗口：一轮覆盖整批写入涉及的多个槽；每次 ack 都要付它一次，换的是轮次与上报次数；0=首个槽有数据就答（轮次更多） |
| `-drop-after` | `PUSHUPES_DROP_AFTER` | 30s | 迁移后前源副本保留期；0=默认，负数启动即报错，不可关闭 |
| `-rebalance-interval` | `PUSHUPES_REBALANCE_INTERVAL` | 2s | controller 每隔多久检查一次 leader 布局并执行回切（节点宕机恢复后把槽 leader 迁回环上预期节点）；0=关闭，负数启动即报错 |
| `-rebalance-batch` | `PUSHUPES_REBALANCE_BATCH` | 28 | 每轮回切最多串行执行几个 leader 交接；轮内串行保证任一时刻只有一个槽在交接栅栏上，一轮跑不完下个间隔自动顺延；0=关闭，负数启动即报错 |
| `-grpc-max-msg-size` | `PUSHUPES_GRPC_MAX_MSG_SIZE` | 4MiB | client 面 gRPC 消息上限（recv/send 同值）；`BatchAppend` 单批要装进它，可写 `16MiB` 等带单位形式（env 同样接受）；≤0 启动即报错 |
| `-batch-slot-parallelism` | `PUSHUPES_BATCH_SLOT_PARALLELISM` | 100 | `BatchAppend` 一批最多同时执行几个槽（异槽并行、同槽串行；铺满 1680 槽的宽批也不会瞬间压上等量并发 WAL 写者）；≤0 启动即报错 |
| `-raft-heartbeat-timeout` | `PUSHUPES_RAFT_HEARTBEAT_TIMEOUT` | 100ms | 共识心跳间隔。leader 的心跳没按时到达就会被投票换掉，所以宿主机可能拖住共识事件循环时要与 `-raft-election-timeout` 一起放大 |
| `-raft-election-timeout` | `PUSHUPES_RAFT_ELECTION_TIMEOUT` | 500ms | follower 多久收不到心跳就发起选举。必须 ≥ 2× 心跳，否则一次迟到的心跳就会掀掉 leader（启动即报错）；与心跳是一对比例，一起放大而不是只收窄其中一个 |
| `-raft-flush-interval` | `PUSHUPES_RAFT_FLUSH_INTERVAL` | 200µs | 共识 WAL 组提交窗口：窗口内到达的追加共用一次 fsync |
| `-raft-segment-bytes` | `PUSHUPES_RAFT_SEGMENT_BYTES` | 64MiB | 共识 WAL 段滚动大小；可写字节数或带单位（`64MiB`），env 同样接受 |

### 客户端接入

事件读写只走 **gRPC client 面**（proto 契约 `proto/pushupes/v1/events.proto`）：

- `EventService/Append`：事件写入。服务端校验幂等与版本（见「数据模型与
  写入协议」），槽不在本节点时以响应字段返回重定向：`err_id=1003/1004`
  （MOVED/ASK）+ `node`（槽 leader 的 client 地址），客户端据此重连。
- `EventService/BatchAppend`：批量写入。一批携带多条 `AppendRequest`（批内
  `aggregate_id` 必须各不相同，重复的那组整组 `fail/1002` 且不执行），逐条
  独立走同一套写入规则，按请求同序返回逐条结果（回显 `aggregate_id`）。
  服务端同槽串行、异槽并行（并发上限 `-batch-slot-parallelism`，默认 100）。单批必须装进 client 面
  gRPC 消息上限（`-grpc-max-msg-size`，默认 4MiB）。
- `EventService/ReadStream`：按聚合读取事件流（≤HW 语义）。
- `EventService/ReadByCommand`：按 `command_id` 查询已写入的记录。

错误 ID：`1001` 版本冲突（附 current_version）、`1002` 参数非法、
`1003` MOVED、`1004` ASK、`1005` 本节点非 controller（admin 写命令专用，
响应附 `controller` 与 `controller_admin_addr`）。

admin 面 HTTP（仅管理）：`GET /admin/cluster/status`、`GET /admin/writes`、
`GET /admin/slots/{slot}/describe`、`GET /admin/slots/{slot}/streams`、
`POST /admin/slots/{slot}/migrate`、`POST /admin/slots/{slot}/remove-replica`、
`POST /admin/cluster/plan`、`POST /admin/cluster/replica-policy`、`GET /healthz`、`/debug/pprof/`。
**控制器专属写命令（migrate/remove-replica/plan/replica-policy）必须直接发到 Raft leader 的
admin 地址**，follower 一律拒绝（425 + 1005），不做转发。

仓库自带工具：

```bash
# 冒烟探针（MOVED 跟随 + 幂等 + 版本冲突 + 回读断言）
go run ./cmd/grpccheck -addrs http://127.0.0.1:8591,http://127.0.0.1:8592,http://127.0.0.1:8593

# 写压测（-nodes 传 admin 地址，自动从 status 解析 client 地址；
# -batch N>0 切换为 BatchAppend 批量写，N=每批条数）
go run ./cmd/bench -nodes http://127.0.0.1:8091 -conns 8 -size 1024 -duration 30s
go run ./cmd/bench -nodes http://127.0.0.1:8091 -conns 8 -size 1024 -batch 64 -duration 30s

# BatchAppend 冒烟（success/exists/1001/批内重复拒绝/重定向跟随/空批，直打 8591）
go run ./cmd/batchsmoke

# 单槽灌数据（容量/恢复调试）
go run ./cmd/seed -slot 7 -mib 512

# 副本一致性体检：逐槽比较 leader 与各副本的聚合摘要（读 /admin/slots/N/describe）。
# 报出「LEO 相同但目录偏短」的发散副本 —— 这类副本在环上不可见（leader 回切器只看
# 偏离环的槽），但读不到自己尾部，且会让该槽永远回切不回去。有发散时退出码 1。
go run ./cmd/slotcheck -admins http://127.0.0.1:8091,http://127.0.0.1:8092,http://127.0.0.1:8093
```

修改 proto 后重新生成：`buf generate --template buf.gen.yaml proto`。

## 性能数据

### 硬件环境

- CPU：4 核 2.4GHz
- 内存：8 GiB
- 存储：NVMe SSD，顺序读写约 3 GB/s

以下均为**实测**（非估算）。测试环境：3 节点、RF=2、Docker 沙箱 **4 核 CPU 配额**，bench 客户端与服务端共享配额——绝对吞吐受容器配额封顶，不代表宿主机的上限；比较改动收益时以「每条消息 CPU 成本」与同负载 A/B 为准。以下写入数字都是「高水位确认」（success 时每个 ISR 副本均已落盘，见「高可用与复制」）口径下的测量。

#### 写入吞吐

组合矩阵（节点数 × 连接数 × 批大小）实测。参数：1 KiB 事件体、`aggs=1000`、每组 30 秒；每组冷启动独立集群（单节点 RF=1、3 节点 RF=2），测完销毁数据重测。批量行的延迟按**批往返**计（整批一个样本），批大小=1 即单条 `Append`（一次 RPC 一条记录）。12 组均 fail=0、exists=0、redirects=0。

| 节点 | 连接数 | 批大小 | 吞吐量 (msg/s) | p50 | p99 |
|---|---|---|---|---|---|
| 1 | 1 | 1 | 4588 | 0.18 ms | 1.16 ms |
| 1 | 1 | 10 | 25186 | 0.33 ms | 1.25 ms |
| 1 | 4 | 1 | 11462 | 0.30 ms | 1.27 ms |
| 1 | 4 | 10 | 47581 | 0.70 ms | 2.46 ms |
| 1 | 8 | 1 | 17489 | 0.39 ms | 1.48 ms |
| 1 | 8 | 10 | 73916 | 0.89 ms | 2.98 ms |
| 3 | 1 | 1 | 641 | 1.63 ms | 4.44 ms |
| 3 | 1 | 10 | 2098 | 4.55 ms | 21.03 ms |
| 3 | 4 | 1 | 3161 | 1.08 ms | 4.47 ms |
| 3 | 4 | 10 | 8759 | 4.25 ms | 9.58 ms |
| 3 | 8 | 1 | 5098 | 1.38 ms | 4.63 ms |
| 3 | 8 | 10 | 12720 | 5.77 ms | 10.89 ms |

#### 启动与内存使用

使用 31 GiB 的真实数据集测试：

- **启动时间**：约 4.2 秒
- **内存占用**：活堆 ~100 MiB（31 GiB 数据）
