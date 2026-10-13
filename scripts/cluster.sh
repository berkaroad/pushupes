#!/usr/bin/env bash
# PushupES 集群启停脚本
#
# 默认拉起 3 节点（冷启动：node-1 为唯一 voter（控制面），按自己的 -peers
# 写初始配置；node-2..N 默认 learner（数据面），由脚本准入目录后启动，
# 布局池就是这批 learner）。每个节点独立的
# admin / peer(Raft) / client(gRPC) 端口与数据目录，槽位自动均衡。
#
#   scripts/cluster.sh start     启动集群（编译、逐个拉起、等待选主与槽规划，并打印管理台多地址启动命令）
#   scripts/cluster.sh status    查看各节点 Raft 角色、Leader 槽数、迁移中槽数
#   scripts/cluster.sh join N    运行时扩一个节点：先拉起 node-N（-join 指向现有成员），再 POST 到 controller 加成员
#   scripts/cluster.sh remove N  运行时摘掉 node-N（只允许离线节点：进程在跑会先停，等探活标离线后再 DELETE 到 controller；槽主自动迁到存活副本，副本集按新成员数重排为因子个席位）
#   scripts/cluster.sh admit N [learner]  把 node-N 准入目录（数据面节点：不投票、只承载槽副本；默认 role=learner）
#   scripts/cluster.sh members   列出当前 raft 成员（GET /admin/cluster/nodes）
#   scripts/cluster.sh smoke     端到端冒烟：MOVED 重定向 → v1/v2 写入 → 幂等 exists → 版本冲突 fail/1001 → 回读
#   scripts/cluster.sh slotcheck 副本一致性体检：逐槽比较 leader 与各副本摘要（有发散副本时退出码 1）
#   scripts/cluster.sh logs [N]  跟踪某个节点日志（默认 node-1）
#   scripts/cluster.sh restart   重启（保留数据，验证 WAL/Raft 崩溃恢复）
#   scripts/cluster.sh stop      停止集群
#   scripts/cluster.sh clean     stop 并删除运行目录（含数据，慎用）
#
# 脚本可用环境变量覆盖：
#   REPLICAS=3             节点数：冷启动时是要拉起的全部节点，start 在集群
#                          已经存在时只拉起缺的，并把它们 -join 进现有集群
#   ROLE_<n>=voter|learner
#                          节点 n 的角色（默认：node-1=voter，其余=learner）。
#                          一个节点只能是这两者之一：voter 只投票、不承载槽
#                          副本；learner 只承载副本、不投票。最小部署 = 1 个
#                          控制节点 + 1 个数据节点。数据面节点的槽表由控制面
#                          经 WatchLayout 分发，不参与选举
#   HOST=127.0.0.1         绑定与广播地址
#   ADMIN_BASE=8091         节点 i 的 admin 管理端口（HTTP admin + pprof）= ADMIN_BASE + i - 1
#   PEER_BASE=8391         节点 i 的 peer 端口（Raft + peer gRPC，全部节点间通讯）= PEER_BASE + i - 1
#   CLIENT_BASE=8591         节点 i 的客户端(gRPC)端口 = CLIENT_BASE + i - 1
#   SEGMENT_BYTES=256MiB   段大小（默认 256MiB；须为 64MiB 的整数倍，最大 2GiB）
#   RAFT_ELECTION_TIMEOUT   raft 选举超时（默认 500ms，即节点默认值）：follower 等多久
#                          没收到心跳就发起选举。必须是 RAFT_HEARTBEAT_TIMEOUT 的
#                          至少 2 倍，否则一次迟到的心跳就会掀掉 leader（节点会拒绝启动）
#   RAFT_HEARTBEAT_TIMEOUT  raft 心跳间隔（默认 100ms）。宿主/容器 CPU 紧张导致心跳
#                          迟到时，把两个值一起放大（例：RAFT_HEARTBEAT_TIMEOUT=500ms
#                          RAFT_ELECTION_TIMEOUT=3s）
#   RUN_DIR=$ROOT/.cluster 运行目录（数据、日志、pid）
#   READY_TIMEOUT=90       等待就绪秒数
#   BUILD=1                start 前强制重新编译
#
# 客户端入口：任意节点都能收写请求（非 leader 返回 err_id=1003 带目标节点
# 地址，smoke 里演示了跟随重定向）；读走各节点本地 ≤HW 副本。
#
# 运行时成员变更（不重启、不丢数据）：
#   扩：REPLICAS=7 scripts/cluster.sh start
#       集群已经存在（有节点在跑，或数据目录里已有 raft 日志）时，缺的节点
#       会用 -join 指向现有成员启动并自己报名。（二进制侧同样判断：写初始
#       配置的只有 -peers 里 id 最小的节点，其余节点即使手工拉起也只报名、
#       不写配置 —— 所以「新节点带更大的 -peers 列表启动」本身就是扩容。）
#       所以扩容用这条或下面的 join 都行，改用 join N 时它只动一个节点。
#   扩：scripts/cluster.sh join 4
#       node-4 用 -peers（初始 3 成员的种子）+ -join（指向 node-1）启动，
#       自己向 leader 报名；leader 把它加进 raft 配置，槽表 replan 后
#       副本集自动补齐。也可以在已有节点上手动 POST /admin/cluster/nodes。
#   缩：scripts/cluster.sh remove 2
#       只允许移除离线节点（与后端同规则）：node-2 进程还在跑时脚本先停掉它，
#       等 controller 探活把它标离线后提交 raft 配置删除；该节点收到配置项后才
#       停复制，槽主先迁到存活副本，副本集按剩余成员数重排（补环上的席位、
#       等留下的副本追平后回收多余席位）。数据目录仍在，可手动清理。

set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

REPLICAS="${REPLICAS:-3}"
HOST="${HOST:-127.0.0.1}"
ADMIN_BASE="${ADMIN_BASE:-8091}"
PEER_BASE="${PEER_BASE:-8391}"
CLIENT_BASE="${CLIENT_BASE:-8591}"
# 段大小：默认 256MiB（须为 64MiB 的整数倍，最大 2GiB；可写字节数或 256MiB/1GiB 带单位）
SEGMENT_BYTES="${SEGMENT_BYTES:-256MiB}"
seg_args=(-segment-bytes "$SEGMENT_BYTES")
# 副本策略不在此转发：PUSHUPES_REPLICA_POLICY 若已在环境里，子进程自然继承；
# 否则用节点自己的默认档（medium=每槽 2 份）。high=每个数据节点各存一份全量。
# raft 的两个时间旋钮：只在显式设置时才转发，否则用节点自己的默认值（避免把默认值
# 复制到脚本里，节点改默认值时脚本不需要跟着改）。
RAFT_ELECTION_TIMEOUT="${RAFT_ELECTION_TIMEOUT:-}"
RAFT_HEARTBEAT_TIMEOUT="${RAFT_HEARTBEAT_TIMEOUT:-}"
raft_timing_args=()
[[ -n "$RAFT_ELECTION_TIMEOUT" ]] && raft_timing_args+=(-raft-election-timeout "$RAFT_ELECTION_TIMEOUT")
[[ -n "$RAFT_HEARTBEAT_TIMEOUT" ]] && raft_timing_args+=(-raft-heartbeat-timeout "$RAFT_HEARTBEAT_TIMEOUT")
RUN_DIR="${RUN_DIR:-$ROOT/.cluster}"
READY_TIMEOUT="${READY_TIMEOUT:-90}"
BIN="$ROOT/bin/pushupes"

admin_port() { echo $((ADMIN_BASE + $1 - 1)); }
peer_port() { echo $((PEER_BASE + $1 - 1)); }
client_port() { echo $((CLIENT_BASE + $1 - 1)); }

node_dir()  { echo "$RUN_DIR/node-$1"; }
pid_file()  { echo "$(node_dir "$1")/node.pid"; }
log_file()  { echo "$(node_dir "$1")/node.log"; }
base_url()  { echo "http://$HOST:$(admin_port "$1")"; }

info() { printf '%s\n' "$*"; }
warn() { printf '%s\n' "$*" >&2; }
die()  { printf '错误: %s\n' "$*" >&2; exit 1; }

# 节点存活判定只看 pid 文件（.cluster/node-N/node.pid）：start 预检与 stop/
# clean 都以它为准，绝不按命令行特征扫描——否则会连坐用户手动起的同名集群。
node_pid_alive() {
  local f pid
  f=$(pid_file "$1")
  [[ -f "$f" ]] || return 1
  pid=$(tr -d '[:space:]' < "$f")
  [[ "$pid" =~ ^[0-9]+$ ]] || return 1
  kill -0 "$pid" 2>/dev/null
}

# stop_node N：停掉单个节点进程（判定与 cmd_stop 同源：只认 pid 文件，且
# 确认命令行是 pushupes 才杀）。没有运行中的进程时静默成功。
stop_node() {
  local f pid
  f=$(pid_file "$1")
  [[ -f "$f" ]] || return 0
  pid=$(tr -d '[:space:]' < "$f")
  if [[ "$pid" =~ ^[0-9]+$ ]] && kill -0 "$pid" 2>/dev/null \
     && tr '\0' ' ' < "/proc/$pid/cmdline" 2>/dev/null | grep -q 'pushupes'; then
    kill "$pid" 2>/dev/null
    local t=0
    while (( t < 50 )); do kill -0 "$pid" 2>/dev/null || break; sleep 0.1; t=$((t + 1)); done
    kill -9 "$pid" 2>/dev/null
  fi
  rm -f "$f"
}

# 节点是否已经持有本地 raft 日志（＝它已经属于某个集群：启动时按自己记录的
# 配置起来，不会再写一份初始配置）。用于区分「全新冷启动」与「往现有集群里
# 加节点」：只有前者能让节点按 -peers 写初始 voter 集，后者必须用 -join 报名，
# 否则每个新节点写一份内容不同的 index 1 配置，集群会劈成多个互不隶属的 raft 组。
node_has_raft_data() {
  local f
  for f in "$(node_dir "$1")/data/cluster/wal"/*.wal; do
    [[ -s "$f" ]] && return 0
  done
  return 1
}

# 从 JSON 里取标量字段值（不依赖 python/jq；status JSON 中 raft 排在 slots
# 之前，取首个匹配即为 raft 状态）
json_field() {
  printf '%s' "$1" | grep -o "\"$2\":\"\?[^\",}]*" | head -1 | sed 's/^[^:]*://; s/^"//'
}
json_count() {
  printf '%s' "$1" | grep -o "\"$2\"" | wc -l | tr -d ' '
}

build_binary() {
  if [[ ! -x "$BIN" || ! -x "$ROOT/bin/slotcheck" || "${BUILD:-0}" == "1" ]]; then
    info "编译 pushupes..."
    if ! command -v go >/dev/null 2>&1; then
      [[ -x /root/.local/go/bin/go ]] && export PATH=/root/.local/go/bin:$PATH || die "找不到 go，请安装或加入 PATH"
    fi
    export GOPATH="${GOPATH:-/root/gopath}"
    [[ -d "${GOTMPDIR:-}" ]] || export GOTMPDIR=/root/tmp
    [[ -w "${GOCACHE:-/nonexistent}" ]] || export GOCACHE=/root/.cache/go-build
    mkdir -p "$GOTMPDIR" "$GOPATH" "$GOCACHE" 2>/dev/null || true
    export GOPROXY="${GOPROXY:-https://goproxy.cn,direct}"
    (cd "$ROOT" && go build -o bin/pushupes ./cmd/pushupes && go build -o bin/grpccheck ./cmd/grpccheck && go build -o bin/slotcheck ./cmd/slotcheck) || die "编译失败"
  fi
}

# 全集群共享的 peers 表：node-id=host:peerport（单端口形式）。
# admin/client 地址不配置——各节点通过 peer 端口上的注册协议自报。
# SEED_MAX 限定种子范围：运行时加入的节点（join N，N > REPLICAS）只把
# 已存在的成员（1..REPLICAS）当种子，绝不含自己——否则它一启动就以为自己是
# 成员，不会去报名。
peers_csv() {
  local list="" i max="${SEED_MAX:-$REPLICAS}"
  # -peers 是 raft 种子集：只列控制面（voter）节点。learner 不进共识组，
  # 同一份列表对它是「连控制面的投递点」（注册与布局分发都走 peer 端口）。
  for i in $(seq 1 "$max"); do
    [[ "$(node_n_role "$i")" == "voter" ]] || continue
    list+="${list:+,}node-$i=http://$HOST:$(peer_port "$i")"
  done
  printf '%s' "$list"
}

start_node() {
  local n="$1" dir peers pid
  dir="$(node_dir "$n")"
  mkdir -p "$dir"
  peers="$(peers_csv)"
  : > "$dir/node.log"
  local extra=()
  # 每个节点的角色由 ROLE_<n> 指定（voter/learner，默认 node-1=voter、其余=
  # learner，见 node_n_role）。learner 不报名 raft、由控制面准入目录后分发布局。
  local node_role
  node_role="$(node_n_role "$n")"
  extra+=(-role "$node_role")
  # 运行时加入的节点（join N）不是初始成员：它用 -peers 当种子、用 -join
  # 指向现有成员报名。start_node 只负责拉起进程，成员变更由 cmd_join 提交。
  if [[ -n "${JOIN_TARGET:-}" ]]; then
    extra+=(-join "$JOIN_TARGET")
  fi
  # -bootstrap 已废弃：成员集合静态，每个节点启动时按 -peers 写入同一 voter 集，
  # 不再有「先 bootstrap 再 join」的差异。

  cd "$dir" || die "无法进入 $dir"
  # pid 文件必须由节点进程自己写。setsid 在调用方已是进程组长时会 fork，于是
  # $! 是那个 fork 出来、随即退出的包装进程 —— 记录它会让 pid 文件指向死 pid 而
  # 节点仍在跑，stop/remove/status 从此认不出活着的节点（实测：stop 报「记录的
  # 进程已不存在」而集群照旧在跑）。所以让内层 shell 先写自己的 $$ 再 exec 节点：
  # exec 不换 pid，写下的 pid 与最终跑 pushupes 的 pid 恒等。
  local launch=(node.pid "$BIN" -node "node-$n" \
    -admin "$HOST:$(admin_port "$n")" -peer "$HOST:$(peer_port "$n")" \
    -client "$HOST:$(client_port "$n")" \
    -data ./data -peers "$peers" \
    "${seg_args[@]}" "${raft_timing_args[@]}" "${extra[@]}")
  # fd 全部重定向，否则管道调用永不返回
  if command -v setsid >/dev/null 2>&1; then
    # setsid 让节点脱离本脚本会话
    setsid "${BASH:-bash}" -c 'printf "%s\n" "$$" > "$1"; shift; exec "$@"' _ \
      "${launch[@]}" >> node.log 2>&1 < /dev/null &
  else
    nohup "${BASH:-bash}" -c 'printf "%s\n" "$$" > "$1"; shift; exec "$@"' _ \
      "${launch[@]}" >> node.log 2>&1 < /dev/null &
  fi
  cd "$ROOT"
  # 等节点自己写下 pid（毫秒级）：这样紧接着的启动/停止就不会读到空文件
  for _ in 1 2 3 4 5 6 7 8 9 10; do
    pid="$(tr -d '[:space:]' < "$(pid_file "$n")" 2>/dev/null)"
    [[ -n "$pid" ]] && break
    sleep 0.1
  done
  info "node-$n 启动中: admin=$(admin_port "$n") peer=$(peer_port "$n") client=$(client_port "$n") pid=${pid:-未知}"
}

wait_ready() {
  local deadline=$((SECONDS + READY_TIMEOUT)) up body i
  while (( SECONDS < deadline )); do
    up=0
    for i in $(seq 1 "$REPLICAS"); do
      body="$(curl -s --max-time 2 "$(base_url "$i")/admin/cluster/status" 2>/dev/null)" || body=""
      [[ -n "$body" ]] && up=$((up + 1))
    done
    # 全部可达 + 选出 raft leader + 槽表已规划（slot_count 个 placement）
    if (( up == REPLICAS )); then
      for i in $(seq 1 "$REPLICAS"); do
        body="$(curl -s --max-time 2 "$(base_url "$i")/admin/cluster/status")"
        local state; state="$(json_field "$body" state)"
        local planned; planned="$(json_count "$body" epoch)"
        if [[ "${state,,}" == "leader" && "$planned" -gt 0 ]]; then
          local rf; rf="$(json_field "$body" replica_factor)"
          info "集群就绪：slot 规划 $planned 槽（每槽 ${rf:-?} 副本，按副本策略 × 成员数推导）"
          return 0
        fi
        # 纯控制面（每个已注册成员都是 voter）就绪时槽表就是 0 条：承载池为空，控制器
        # 按设计不规划，写入由控制面明确拒绝。没有这一支，角色分离的拓扑每次 start 都
        # 会白等一个 READY_TIMEOUT 再报「未就绪」。
        if [[ "${state,,}" == "leader" && "$planned" == "0" ]]; then
          # 数据面成员 = 已自报 client 地址、且角色不是 voter 的目录条目（与布局池
          # 同一口径）。`grep -c` 计 0 时退出码是 1，别让它决定分支。
          local members hosting=0
          members="$(curl -s --max-time 2 "$(base_url 1)/admin/cluster/nodes" 2>/dev/null \
            | grep -o '{"id":"node-[0-9]*"[^}]*}' | grep '"client_addr":"http')" || members=""
          if [[ -n "$members" ]]; then
            hosting="$(printf '%s\n' "$members" | grep -vc '"role":"voter"')" || hosting=0
          fi
          if [[ "$hosting" == "0" ]]; then
            info "集群就绪：纯控制面（目录里没有数据面成员），槽表 0 条；等数据面节点注册后自动规划"
            return 0
          fi
        fi
      done
    fi
    sleep 1
  done
  warn "等待超时（${READY_TIMEOUT}s），各节点日志尾部："
  for i in $(seq 1 "$REPLICAS"); do
    warn "--- node-$i ---"
    tail -n 5 "$(log_file "$i")" 2>/dev/null || warn "  （无日志）"
  done
  return 1
}

cmd_start() {
  build_binary
  mkdir -p "$RUN_DIR"

  local i p to_start=()
  # 逐节点检测 pid 文件：存在且进程存活 => 跳过该节点；进程已死或无文件 => 启动。
  for i in $(seq 1 "$REPLICAS"); do
    if node_pid_alive "$i"; then
      info "node-$i 已在运行（pid $(tr -d '[:space:]' < "$(pid_file "$i")")），跳过"
    else
      rm -f "$(pid_file "$i")"   # 清掉陈旧 pid 文件再启动
      to_start+=("$i")
    fi
  done
  if (( ${#to_start[@]} == 0 )); then
    info "全部节点已在运行，无需启动"
    cmd_status
    return 0
  fi
  # 端口冲突预检（有 ss 就查，没有则跳过，靠启动失败兜底）——只查要启动的节点
  if command -v ss >/dev/null 2>&1; then
    local busy=""
    for i in "${to_start[@]}"; do
      for p in "$(admin_port "$i")" "$(peer_port "$i")" "$(client_port "$i")"; do
        ss -H -tln 2>/dev/null | grep -qF ":$p " && busy+=" $p"
      done
    done
    [[ -n "$busy" ]] && die "端口被占用:$busy。换 ADMIN_BASE/PEER_BASE 或先停掉占用进程"
  fi

  # 冷启动还是往现有集群里加节点？判据：有没有节点在跑、有没有节点已经持有
  # raft 日志。两者皆无 = 全新冷启动，各节点按 -peers 写下同一份初始配置；
  # 否则本次要拉起、且没有本地 raft 日志的节点必须用 -join 报名加入 ——
  # 让它自己 bootstrap 会用它那份 -peers 写出内容不同的 index 1 配置，和现有
  # 集群分裂成各自成多数派的 raft 组（实测 REPLICAS 1->3->5->7 依次 start 会
  # 得到四个互不隶属的组，配置在同一个 term/index 上分歧，随之快照互相不可读）。
  local cold=1
  for i in $(seq 1 "$REPLICAS"); do
    node_pid_alive "$i" && cold=0
    node_has_raft_data "$i" && cold=0
  done
  local join_to=""
  if (( cold == 0 )); then
    # 报名入口：优先一个已在运行的成员；都在停机但数据还在时，取序号最小、
    # 持有 raft 日志的那个（它在本次启动里会起来，先报名的节点会按 adopt
    # 周期重试到它可连为止）。
    for i in $(seq 1 "$REPLICAS"); do
      node_pid_alive "$i" && { join_to="$HOST:$(peer_port "$i")"; break; }
    done
    if [[ -z "$join_to" ]]; then
      for i in $(seq 1 "$REPLICAS"); do
        node_has_raft_data "$i" && { join_to="$HOST:$(peer_port "$i")"; break; }
      done
    fi
  fi

  # 两段启动：先控制面（voter），等它选出 controller，再把数据面（learner）
  # 逐个准入目录并拉起 —— OpRegister 不新增成员，learner 必须先在目录里有
  # 条目，它的注册心跳才落得进去。
  local voters=() learners=()
  for i in "${to_start[@]}"; do
    if [[ "$(node_n_role "$i")" == "voter" ]]; then voters+=("$i"); else learners+=("$i"); fi
  done
  if (( cold == 1 )) && (( ${#voters[@]} == 0 )); then
    die "冷启动至少需要一个 voter（控制面）：最小部署是 1 个 voter + 1 个 learner"
  fi
  for i in "${voters[@]}"; do
    if (( cold == 1 )) || node_has_raft_data "$i"; then
      # 冷启动：按 -peers 写同一份初始配置；已有日志：按它记录的配置起来。
      start_node "$i"
      continue
    fi
    info "node-$i 没有本地 raft 日志：以 -join $join_to 报名加入现有集群（不写初始配置）"
    JOIN_TARGET="$join_to" start_node "$i"
  done
  if (( ${#learners[@]} > 0 )); then
    # 等 controller 就绪（选出 leader、controller_admin_addr 落表）。
    local deadline=$((SECONDS + READY_TIMEOUT)) ctrl=""
    while (( SECONDS < deadline )); do
      ctrl="$(controller_admin 2>/dev/null)" || ctrl=""
      [[ -n "$ctrl" ]] && break
      sleep 1
    done
    [[ -n "$ctrl" ]] || die "${READY_TIMEOUT}s 内没有选出 controller，无法准入数据面节点"
    for i in "${learners[@]}"; do
      cmd_admit "$i" learner
      start_node "$i"
    done
  fi
  wait_ready || exit 1
  info ""
  local admin_list="" i
  for i in $(seq 1 "$REPLICAS"); do admin_list+="${admin_list:+,}$(base_url "$i")"; done
  info "客户端入口（任意节点均可，写自动重定向到 leader）："
  for i in $(seq 1 "$REPLICAS"); do info "  $(base_url "$i")"; done
  info "管理台（自动跟随 leader）: cd frontend && PUSHUPES_ADMIN_ENDPOINTS=\"$admin_list\" npm run dev"
  cmd_status
}

# 副本一致性体检：逐槽比较 leader 与各副本的聚合摘要（aggregates/versions/
# resolvable），报出「LEO 相同但目录偏短」的发散副本。这类副本在环上不可见
# （回切器只看偏离环的槽），所以例行跑一次才知道有没有。
cmd_slotcheck() {
  build_binary
  local list="" i
  for i in $(cluster_node_indexes); do list+="${list:+,}$HOST:$(admin_port "$i")"; done
  "$ROOT/bin/slotcheck" -admins "$list" "$@"
}

# controller_admin 找到当前 raft leader 的 admin 端口（成员变更必须发到它）。
# 从任一节点的 status 里取 controller_admin_addr；读不到就回退到 admin 端口轮询。
controller_admin() {
  local i body ctrl
  for i in $(seq 1 "$REPLICAS"); do
    body="$(curl -s --max-time 2 "$(base_url "$i")/admin/cluster/status" 2>/dev/null)" || continue
    ctrl="$(json_field "$body" controller_admin_addr)"
    if [[ -n "$ctrl" ]]; then
      printf '%s' "${ctrl#http://}"
      return 0
    fi
  done
  die "找不到 controller（raft leader），集群可能还没选主"
}

# cmd_join N：运行时扩一个节点。先拉起进程（-join 指向现有成员），等它
# 自己报名进 raft 配置，再等它出现在成员表里。
# node_n_role N：节点 N 的角色（ROLE_<n>；默认 node-1=voter、其余=learner）。
# 角色只有两种：voter 投票、不承载副本；learner 承载副本、不投票。
# 最小部署 = 1 个 voter + 1 个 learner。
node_n_role() {
  local role_var="ROLE_$1"
  local r="${!role_var:-}"
  if [[ -z "$r" ]]; then
    if [[ "$1" == "1" ]]; then r=voter; else r=learner; fi
  fi
  [[ "$r" == "voter" || "$r" == "learner" ]] || die "ROLE_$1=$r：角色只能是 voter 或 learner"
  printf '%s' "$r"
}

# cmd_admit N [role]：把 node-N 准入复制目录。数据面节点（role=learner）的准入
# 是一条目录命令，不碰 raft 配置 —— 它不投票。控制面只接受「目录里已有」的节点
# 自报地址（OpRegister 不新增成员），所以准入必须先于进程启动。
cmd_admit() {
  local n="${1:-}" role="${2:-learner}" resp
  [[ -n "$n" && "$n" =~ ^[0-9]+$ ]] || die "用法: $0 admit <节点序号> [voter|learner]"
  [[ "$role" == "voter" || "$role" == "learner" ]] || die "role 只能是 voter 或 learner"
  resp="$(curl -s --max-time 5 -X POST "http://$(controller_admin)/admin/cluster/nodes" \
    -d "{\"id\":\"node-$n\",\"peer_addr\":\"$HOST:$(peer_port "$n")\",\"role\":\"$role\"}")" \
    || die "准入 node-$n 失败（controller 是否在跑？）"
  info "node-$n 准入：$resp"
}

cmd_join() {
  local n="${1:-}"
  [[ -n "$n" && "$n" =~ ^[0-9]+$ ]] || die "用法: $0 join <节点序号>"
  build_binary
  local node_role="$(node_n_role "$n")"
  if node_pid_alive "$n"; then
    info "node-$n 已在运行，跳过启动"
  else
    rm -f "$(pid_file "$n")"
    if [[ "$node_role" == "learner" ]]; then
      # 数据面节点：先准入（目录命令），再启动。它不报名 raft（不投票），
      # 槽表由控制面经 WatchLayout 分发；自己的 admin/client 地址由注册心跳公告。
      cmd_admit "$n" learner
      start_node "$n"
    else
      # 报名入口：指向 node-1 的 peer 端口（启动时至少 node-1 已在跑）。
      JOIN_TARGET="$HOST:$(peer_port 1)" start_node "$n"
    fi
  fi
  local deadline=$((SECONDS + READY_TIMEOUT))
  while (( SECONDS < deadline )); do
    local members
    members="$(curl -s --max-time 3 "$(base_url 1)/admin/cluster/nodes" 2>/dev/null)" || members=""
    if [[ "$node_role" == "learner" ]]; then
      # 数据面节点的判定是「它自报了 client 地址」：准入只是目录里多一条，
      # 进程起来并注册之后才真的能收数据。
      if printf '%s' "$members" | grep -o "{\"id\":\"node-$n\"[^}]*}" | grep -q '\"client_addr\":\"http'; then
        info "node-$n 已作为数据面节点上线（不投票；槽表由控制面分发）"
        cmd_members
        return 0
      fi
    elif printf '%s' "$members" | grep -q "\"id\":\"node-$n\""; then
      info "node-$n 已加入 raft 配置（成员变更自动 replan 槽表）"
      cmd_members
      return 0
    fi
    sleep 1
  done
  warn "等待 node-$n 加入超时；日志尾部："
  tail -n 10 "$(log_file "$n")" 2>/dev/null || warn "  （无日志）"
  return 1
}

# cmd_remove N：运行时摘掉一个节点。只允许移除离线节点：node-N 进程还在跑
# 时脚本先停掉它，再等 controller 的探活把它标成离线（连续 3 轮探测失败，
# 约数秒），最后提交 DELETE（必须发到 controller）。数据目录不动。
cmd_remove() {
  local n="${1:-}"
  [[ -n "$n" && "$n" =~ ^[0-9]+$ ]] || die "用法: $0 remove <节点序号>"
  if node_pid_alive "$n"; then
    info "node-$n 进程仍在运行：只允许移除离线节点，先停掉它的进程"
    stop_node "$n"
  fi
  local ctrl; ctrl="$(controller_admin)"
  # 等该成员在 controller 视角离线（down=true；目录里没有它或它从未注册也算离线）。
  # body 先压成一行：这个端点的 JSON 是缩进过的，逐行的 grep 只能看到条目的一部分，
  # 于是「条目里没有 client_addr」会立刻为真 —— 脚本会在 mark_down 落地前就去 DELETE，
  # 后端按「只允许移除离线成员」直接回 400（node 已停但还没被判离线的那 1~2 秒就是这窗口）。
  local deadline=$((SECONDS + 30)) body entry
  while (( SECONDS < deadline )); do
    body="$(curl -s --max-time 3 "http://$ctrl/admin/cluster/nodes" 2>/dev/null)" || body=""
    body="$(printf '%s' "$body" | tr -d ' \t\n')"
    if ! printf '%s' "$body" | grep -q "\"id\":\"node-$n\""; then
      break   # 已不在成员表：DELETE 会幂等成功
    fi
    entry="$(printf '%s' "$body" | grep -o "\"id\":\"node-$n\"[^}]*" | head -1)"
    if ! printf '%s' "$entry" | grep -q '"client_addr"'; then
      break   # 未注册过数据面地址：本就是离线
    fi
    if printf '%s' "$entry" | grep -q '"down":true'; then
      break
    fi
    sleep 1
  done
  # 「member is online」不是永久失败：node 进程已停，但 controller 判它离线（探活 3 轮，
  # 或有见证者上报）有一个 1~2s 窗口，DELETE 撞上它只是早了一步。等一拍再试，别把
  # 「node 还在跑」的错觉丢给运维。
  local resp code attempt=0
  while :; do
    attempt=$((attempt + 1))
    resp="$(curl -s -w '\n%{http_code}' -X DELETE --max-time 40 \
      "http://$ctrl/admin/cluster/nodes/node-$n" 2>/dev/null)"
    code="${resp##*$'\n'}"
    if [[ "$code" == "200" ]]; then
      info "node-$n 已从 raft 配置移除（槽主迁移 + 副本补齐已由 controller 接管）"
      break
    fi
    if (( attempt < 5 )) && printf '%s' "${resp%$'\n'*}" | grep -q 'online'; then
      info "node-$n 仍被判在线，等 controller 标记离线后重试（第 $attempt 次）"
      sleep 1
      continue
    fi
    die "移除 node-$n 失败（HTTP $code）：$(printf '%s' "${resp%$'\n'*}" | head -c 400)"
  done
  cmd_members
}

# cmd_members：列出当前 raft 成员（任一节点的本地视图）。
cmd_members() {
  local i body
  for i in $(seq 1 "$REPLICAS"); do
    body="$(curl -s --max-time 3 "$(base_url "$i")/admin/cluster/nodes" 2>/dev/null)" || continue
    [[ -n "$body" ]] || continue
    info "成员（自 node-$i 的视图）：$(json_field "$body" members 2>/dev/null)"
    printf '%s\n' "$body" | tr ',' '\n' | grep -o '"id":"[^"]*"' | sed 's/"id":"/  - /; s/"$//'
    return 0
  done
  die "没有节点响应 /admin/cluster/nodes"
}

# cluster_node_indexes 列出要呈现/体检的节点序号：REPLICAS 里的初始成员，加上
# 运行时扩进来的（node-N，N > REPLICAS，从成员表读）。写死 1..REPLICAS 会让 join
# 进来的节点在 status/smoke/slotcheck 里凭空消失。
cluster_node_indexes() {
  local extra=""
  # 只收「已自报 client 地址」的成员：没有 client 地址的条目（刚准入、进程还没起的
  # learner）不该被算出一个并不存在的 client 端口——按端口推算的前提是那个进程在跑。
  extra="$(curl -s --max-time 3 "$(base_url 1)/admin/cluster/nodes" 2>/dev/null \
    | grep -o '{"id":"node-[0-9]*"[^}]*}' | grep '"client_addr":"http' \
    | grep -o '"id":"node-[0-9]*"' | grep -o '[0-9]*')" || extra=""
  printf '%s\n' $(seq 1 "$REPLICAS") $extra | sort -n -u
}

cmd_status() {
  local i body state leader leaders migrating
  info "=== PushupES 集群（REPLICAS=$REPLICAS）==="
  for i in $(cluster_node_indexes); do
    body="$(curl -s --max-time 3 "$(base_url "$i")/admin/cluster/status" 2>/dev/null)"
    if [[ -z "$body" ]]; then
      info "  node-$i: 无响应（$(node_pid_alive "$i" && echo 进程存活 || echo 未运行)）"
      continue
    fi
    state="$(json_field "$body" state)"
    # 只数槽表里的 leader：raft 段的 "leader" 字段在 leader 自身会回填自己的
    # id，直接全串 grep 会让 raft leader 凭空多出 1 个槽——纯控制节点（不持槽）
    # 上这个偏差最刺眼，会把「0 个槽」显示成 1。
    leaders="$(printf '%s' "$body" | sed 's/.*"slots"://' | grep -o "\"leader\":\"node-$i\"" | wc -l | tr -d ' ')"
    migrating="$(printf '%s' "$body" | grep -o '"state":"migrating_' | wc -l | tr -d ' ')"
    # status 的 peers 是 map[string]Peer：取 node-$i 那条里的 role（还没自报
    # 角色的条目是 -peers 种子形态，属于控制面，按 voter 显示）。
    local role
    role="$(printf '%s' "$body" | grep -o "\"node-$i\":{[^}]*}" | grep -o '\"role\":\"[a-z]*\"' | head -1 | sed 's/.*:\"//; s/\"//')"
    info "  node-$i: raft=${state} role=${role:-voter} leader槽=${leaders} 迁移中槽=${migrating} admin=$(admin_port "$i") client=$(client_port "$i")"
  done
}

# 事件写入/查询只走 gRPC 数据面：冒烟直接调用 bin/grpccheck（MOVED 重定向 →
# v1/v2 写入 → 版本冲突 fail/1001 → 三节点回读 → 幂等 exists → by-command），
# 断言全部内建在程序里，退出码即结果。
cmd_smoke() {
  local addrs="" i
  [[ -x "$ROOT/bin/grpccheck" ]] || die "缺少 bin/grpccheck，先 BUILD=1 scripts/cluster.sh start"
  for i in $(cluster_node_indexes); do
    addrs+="${addrs:+,}$HOST:$(client_port "$i")"
  done
  "$ROOT/bin/grpccheck" -addrs "$addrs" || die "gRPC 冒烟失败（scripts/cluster.sh logs 查日志）"
  info "冒烟通过：路由/写入/幂等/版本校验/复制回读全部 OK（gRPC 数据面）"
}

cmd_logs() {
  local n="${1:-1}"
  [[ -f "$(log_file "$n")" ]] || die "没有 node-$n 的日志，先执行 '$0 start'"
  info "跟踪 $(log_file "$n")（Ctrl-C 退出）"
  tail -f "$(log_file "$n")"
}

cmd_stop() {
  local f pid name killed=0 skipped=0
  # 只停 RUN_DIR 下 node.pid 记录的进程：绝不按命令行特征扫描杀进程，
  # 否则会误伤用户手动启动的同名 pushupes 集群。
  shopt -s nullglob
  for f in "$RUN_DIR"/node-*/node.pid; do
    name=$(basename "$(dirname "$f")")
    pid=$(tr -d '[:space:]' < "$f")
    [[ "$pid" =~ ^[0-9]+$ ]] || { warn "$name 的 pid 文件内容非法（'$pid'），跳过"; rm -f "$f"; continue; }
    if ! kill -0 "$pid" 2>/dev/null; then
      info "$name 记录的进程 pid $pid 已不存在，清理 pid 文件"
      rm -f "$f"
      skipped=1
      continue
    fi
    # 该 pid 可能已被系统复用给别的进程：确认命令行确实是 pushupes 才杀。
    if ! tr '\0' ' ' < "/proc/$pid/cmdline" 2>/dev/null | grep -q 'pushupes'; then
      warn "$name 的 pid $pid 已不是 pushupes 进程（可能被复用），不杀，仅清理 pid 文件"
      rm -f "$f"
      continue
    fi
    kill "$pid" 2>/dev/null && killed=1
    local t=0
    while (( t < 50 )); do kill -0 "$pid" 2>/dev/null || break; sleep 0.1; t=$((t + 1)); done
    if kill -0 "$pid" 2>/dev/null; then
      warn "$name 未在 5 秒内退出，强杀 pid $pid"
      kill -9 "$pid" 2>/dev/null
    fi
    rm -f "$f"
  done
  shopt -u nullglob
  (( killed == 0 )) && (( skipped == 0 )) && info "未发现 $RUN_DIR 下任何 pid 文件（集群可能未在运行）"
  info "已停止"
}

cmd_clean() {
  cmd_stop
  if [[ -d "$RUN_DIR" ]]; then
    rm -rf "$RUN_DIR"
    info "已删除运行目录 $RUN_DIR（含 WAL 数据与 Raft 日志）"
  fi
}

usage() {
  awk 'NR > 1 { if ($0 !~ /^#/) exit; sub(/^# ?/, ""); print }' "${BASH_SOURCE[0]}"
}

case "${1:-}" in
  start)   cmd_start ;;
  stop)    cmd_stop ;;
  status)  cmd_status ;;
  admit)   shift; cmd_admit "$@" ;;
  join)    shift; cmd_join "$@" ;;
  remove)  shift; cmd_remove "$@" ;;
  members) cmd_members ;;
  smoke)   cmd_smoke ;;
  slotcheck) shift; cmd_slotcheck "$@" ;;
  logs)    cmd_logs "${2:-1}" ;;
  restart) cmd_stop; sleep 1; cmd_start ;;
  clean)   cmd_clean ;;
  ""|-h|--help|help) usage ;;
  *)       die "未知命令 '$1'（可用: start stop status join remove members smoke slotcheck logs restart clean）" ;;
esac
