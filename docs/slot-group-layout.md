# 槽组布局与成员变更：实施清单（2026-10-27 定稿，未动代码）

范围：2048 槽 / 11 代组 / 1024 数据节点 / 单槽 1e9 条 / 最小移动布局 / 偶数闸 / 删除就地补位 / 跳代分批扩容。
机制陈述见 `DESIGN.md` §1、§4.5、§5、§7.2.7；本文只列决策、改动项、验收。

## 1. 决策记录

| # | 决策 | 备注 |
|---|---|---|
| D1 | 物理槽 2048 = g0..g10 + slot-000 | 1+2+…+1024 = 2047，+1 预留 |
| D2 | g 代段 = [2^g, 2^(g+1))，base = size = 2^g | 段不重叠、永久占用；老段退役后槽 ID 不重用 |
| D3 | 路由 `LogicalSlotOf(agg, size) = hash&(size-1)`（组内逻辑槽），物理 = 基址 + 逻辑 | 客户端不学基址 |
| D13 | 术语：表里的槽位号是「物理槽位」，客户端面只说「逻辑槽位」（0..size-1），`物理 = 逻辑 + 基址`；协议按逻辑号携带，服务端换算 | 客户端不学基址，换代只换路由表 |
| D14 | 管理面同用逻辑槽：每槽数组长度 = `group_size`，路径 `{slot}` 与 `slots` 键均为逻辑槽，`describe` 带 `physical_slot` 对照；`slot_count` = 组大小，新增 `physical_slot_count` = 2048 | 控制台的「剩余槽位」按服务槽数算 |
| D4 | slot-000 预留，业务路由不可达，没有任何读写入口 | 表里固定常量 0 |
| D5 | g0~g2 仅开发/测试，g3 起生产 | 生产最小代的规模门槛见 DESIGN §7.2.7 |
| D6 | 满配 1024 节点 × 1 活跃槽 × 1e9 条 = 1.06 PiB 逻辑 | 硬上限：节点数与活跃槽同时到顶 |
| D7 | 跳代扩容允许（g_src → g_dst，dst > src+1） | 数据一次迁到位，实例可分批上线 |
| D8 | 分批迁移：每批 128 槽，迁完退役再下一批 | 并存期磁盘峰值 ≈ 稳态的 1/8 |
| D9 | 无活跃槽的节点做副本节点 | leader 并行度上限 = 活跃槽数 |
| D10 | 布局：显式目标 + 最小移动（不采用 hash 环/虚拟节点） | K/N≈1 时 hash 环必然失衡 |
| D11 | 偶数闸：入席与均衡受闸；bootstrap、failover、因子变更豁免；删除本身立即执行，受闸的是「删除后那一次全局重排」（不触发） | 判据收成一条：gateOpen() + DesiredSeats 非空 |
| D12 | 删除成员 = drain + 摘除 + 就地补位（同一命令内完成，只触及失去席位的槽；补位按「删除后 replan 的均衡配额」一次落位） | 不留幽灵席位、RF 不降；删除结束时布局已等于下一次 replan 的目标，之后不重排 |
| D13 | 单机 8 核 / 32 GiB / NVMe 8 TB / 10~25 GbE；接受 94 分钟单槽重建 | 见 DESIGN §7.2.7 |
| D14 | 术语统一为「槽位迁移」（7 个动作，不再用带步数的叫法） | DESIGN/README/技能已改 |

## 2. 改动项清单

### A 常量与定标
- `internal/storage/config.go:58` `DefaultSlotCount` → 2048，注释改写（2048 = g0..g10 + 预留；1024 节点 × 1 活跃槽）
- `internal/storage/config_test.go:25`、`internal/config/config_test.go:33` 钉测/测试值 → 2048
- 现役槽数注释逐处换算：`internal/config/config.go:94/252/260/267/421`、`internal/cluster/{balance.go:467, balance_test.go:875, startup_grace_test.go:70, replication.go:226, commit_floor_codec_test.go:10, fence.go:142}`、`internal/storage/storage_disk_bytes_test.go:78`
  - 「最坏每节点 512 槽主」数字不变（活跃仍 1024 槽 / 2 节点）；只改「1024-slot table」这类物理槽表述
  - 历史实测句保留原数字并标注当时口径
- `internal/api/health_test.go:74/101-102`、`frontend/scripts/ssr-check.mjs:221-236` 的「1024 槽」断言 → 2048/活跃槽口径

### B 路由契约
- `pkg/client/slot.go` 提供 `LogicalSlotOf(agg, size)`（唯一路由原语）
- `pkg/client/slot_test.go:41-46` 金标向量用独立实现重算（覆盖各代 size 的分桶 chi2 与 mix64 低位）
- `internal/data/record.go:442-446` 删除 `data.SlotOf` 委托
- `internal/storage/store.go:312` 删除 `Store.SlotOf`（布局是集群状态，存储层只接受显式 slot）
- 引用点全量替换：`internal/grpcapi/server.go:278/401/488/519/568`、`pkg/client/client.go:848`、`cmd/*`

### C 协议面
- `proto/pushupes/v1/events.proto`：`PrefetchRoutesResponse` 加 `group_gen/group_base/group_size/layout_epoch`；`BatchAppendRequest` 加 `layout_epoch`；跑 `scripts/genproto.sh`
- `internal/grpcapi/server.go`：分组与四个读 RPC 改组路由；epoch 不符回 1010；无代（GroupSize=0）直接拒绝写入
- `internal/data/errs.go` + DESIGN 错误表 + proto 注释：新增 1010 GROUP_MISMATCH
- `pkg/client/client.go`：routeTable 持 (size,epoch)；1010 → 刷新 PrefetchRoutes 并计入 Attempts；路由只产出逻辑槽号

### D 布局表与规划
- `internal/cluster/table.go:103` Table 加 GroupGen/GroupBase/GroupSize/LayoutEpoch/ReservedSlot；`Clone` 显式携带；新 `OpSetGroup`（apply 校验 base=size=2^gen、段落 1..2047、互不重叠、epoch 单调）
- `internal/cluster/table_bin.go:169` 编码 bump 版本 + 组字段段；往返与跨版本拒绝测试
- [已落] `internal/cluster/table.go:325` `PlanSlots` 收窄为 bootstrap 专用；`DesiredSeats`（`seatplan.go`：补缺 → 修剪 → 均衡 → 换主，输出 Target/Add/Drop/Lead）
- [已落] `layoutStale` 改为「`gateOpen()` + `DesiredSeats` 非空」一条判据（偶数闸在唯一自动入口处生效）
- [已落] `balance.go` reclaim 目标改 `DesiredSeats().Target`；`PlanLeaderRebalance` 锚点 = 计划的 `Lead`，无计划时回落到环（未做「按 floor/ceil 让位」，均衡仍由环兜底）
- `internal/cluster/migration.go:229` `MigrateSlot` 拒绝跨代迁移；新增 `groupExpand`（stage→replay→switch→retire，支持跳代与 128 槽分批）
- [已落] `OpLeaveNode` apply 末尾 `refillAfterLeave()`：删除成员时就地补位（D12），只触及失去席位的槽
- `internal/cluster/replication.go:559/593/594/615` 数组随 SlotCount 自动；复制器/mfetch 只对活跃段建会话
- `internal/storage/store.go` `OpenStore`：只加载活跃段（组外不建 WAL）
- `internal/api/handler.go:252/306` status：slot_count=2048 + 组视图（gen/base/size/epoch/闸门状态）
- 故障重建：fetch 第三类优先级 + 并发限速（`SeatGrant` 增 `rebuild`）

### E 迁移实现（实验特性：链路已完整，并发写收尾未闭合，禁止用于生产扩容）
- `PeerService.ReplicateAgg`（源槽→目标槽，按聚合重放与增量转发）
- replay 复用 `Slot.AggregateVersion(agg, from, limit, upto)`（`storage/slot.go:1205`）分页；游标 (agg, version) 断点续传；预算低于 ring/factor
- 切换：逐槽 `enterWriteFence`（段内 size 个槽）+ 一次 `OpSetGroup(epoch+1)`；窗口 = 尾巴
- 退役：源段逐槽 `DropSlot`；plan 置 inactive
- 回滚：replay 失败丢弃目标段；切换后不可回滚（与 leader_move 同口径）

### F 工具与脚本
- `cmd/bench`：组路由 + 打印组参数
- `cmd/slotcheck`：只扫活跃段
- `cmd/seed / grpccheck / batchsmoke / flowcheck / writeping`：槽计算与断言
- `scripts/cluster.sh`（status 解析）、`scripts/admin-storage-check.py`（数组长度）、`policy-flip-writes.sh`、`check-leader-ring.py`

### G 前端
- 集群页：槽数 2048、当前代/活跃槽数/闸门状态/迁移进度、slot-000 预留标记
- `frontend/scripts/ssr-check.mjs` 槽数断言

### H 文档
- `DESIGN.md` §4.5（布局）、§7.2.7（定标）已写；§1 槽位段与 §7.2.3 的 N|K 表述标注「仅 bootstrap」
- `README.md`：槽数 2048、段默认 256 MiB、加/删成对、奇数窗口、删除即补位、硬上限
- 技能 `pushupes-eventstore`：术语已改；布局章节落地后补指针

## 3. 测试矩阵

| # | 用例 | 断言 |
|---|---|---|
| T1 | 组段划分 | 1..2047 全覆盖、无重叠、base=size=2^g、slot-000 只属预留 |
| T2 | 路由不可达 slot-000 | 每代 1e4 随机 hash：slot = size+(h&(size-1)) ≥ size |
| T3 | LogicalSlotOf(size=1) | 恒返回逻辑槽 0（物理 slot-001） |
| T4 | 金标向量 | 独立实现；各代 size 分桶 chi2/df ∈ [0.9,1.1] |
| T5 | I1/I1a | N∈{1,2,3,4,6,8,16}×RF∈{1,2,3}：副本集互异、副本数=min(RF,N) |
| T6 | I2 | 每节点席位/leader 落在 floor/ceil 区间 |
| T7 | 偶数闸 4→5 | 零席位移动、新节点 0 槽、其余槽 leader 不变 |
| T8 | 偶数闸 5→6 | 两个新节点同轮入席；移位 = 2·K·RF/6 |
| T9 | bootstrap 1/3 节点 | 可起；奇数豁免；RF=min(RF,N) |
| T10 | 计数口径 | 4 节点 1 台 offline 仍按 4 放行；1 台 draining 计数 4、候选 3、不向其加席位 |
| T11 | 删除就地补位 4→3 | 席位全部补到存活节点；RF 不降；其余槽零移动；无幽灵席位 |
| T12 | 删除到 2/1 节点 | RF=2 / RF=1（I1a）；无幽灵 |
| T13 | 删除不触发重排 | 不含被删节点的槽逐槽零变化 |
| T14 | 替换一台（ID 变） | 总移动 ≤ 2·K·RF/N，非全表 |
| T15 | 最小移动上界 | 4→6 加 2 台：移位 = 2·K·RF/6 ± 1 |
| T16 | 因子变更豁免闸 | 3 节点（奇数）改 replica-policy：立即生效 |
| T17 | failover 豁免闸 | 奇数节点 kill leader：换主完成、无 replan |
| T18 | 段级扩容与闸门并行 | 跳代期间做成员变更：都成功、段级迁移只有一个在飞 |
| T19 | 快照/Clone 携带组字段 | 往返一致；跨版本拒绝 |
| T20 | 单槽 1e9 布隆 | 实测误判率 ≈0.049%（bloom-sizing probe 口径） |
| T21 | 分批迁移 | 128 槽/批、8 批完成；每批一次 epoch 切换；磁盘峰值 ≤ 2.4 TiB/节点 |
| T22 | 故障重建 | kill 10 台：10 路并行、约 94 分钟补齐；限速生效 |
| T23 | 硬上限 | 1024 节点可 bootstrap；加第 1025 节点被明确拒绝 |
| T24 | 非 2 的幂节点数（1000） | 24 台 2 槽、976 台 1 槽；那 24 台按 4.2 TiB 校验 |

## 4. 验收口径（活簇）

1. `RUN_DIR` 在仓库外、端口错开；`cluster.sh start` 5 节点 → `status` 每数据节点 leader 槽符合 size/N → `smoke`（prefetch 断言 2048 + 组字段）→ `slotcheck`（只扫活跃段）→ stop/clean。
2. 跳代演练 g3→g10 分 8 批（128 槽/批），全程 bench 不停写：fail/unresolved=0、redirect 有界、每批后核对 epoch 与磁盘峰值。
3. 扩容后对全部聚合 `ReadStream` 校验 version 1..N 连续无洞；老 command_id 重试命中 exists。
4. 性能基线沿用实测曲线（1/2/4/8/16 槽 = 29.9k/33.9k/17.7k/4.5k/2.7k rec/s），跳代后按落槽数复测。

## 5. 不改的边界

段格式与 WAL 布局、槽内 seq 不变式（规则 1/2）、Raft 只复制元数据、ISR/HW/mfetch 语义、owner 围栏、六步→槽位迁移的 fence/floor 语义、副本因子语义、错误码 1001 起连续的编号规则、`-segment-bytes` 等旋钮形态。

## 6. 遗留待实测（不是拍板项）

- raft 1024 learner 的 progress map / 快照分发体积与控制面出站带宽
- 单槽 1e9 条的布隆实测误判率（现参数理论 ≈0.049%）
- 单槽 1e7 聚合时 `.aidx` 与热尾的内存占用
- 分批迁移的实际窗口、每批 epoch 切换的写抖动
- 1000 节点（非 2 的幂）布局的 max/mean 与磁盘热点
