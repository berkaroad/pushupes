#!/usr/bin/env bash
# PushupES 集群启停脚本
#
# 一条命令创建整个集群（控制面 voter + 数据面 learner）：
#
#   REPLICAS=5 VOTERS=3 scripts/cluster.sh start
#       → node-1..3 = voter（控制面，投票、不承载槽副本）
#       → node-4..5 = learner（数据面，承载槽副本、不投票）
#       冷启动顺序：先拉起 voter 等选主，再把 learner 逐个准入目录并启动，
#       最后等槽规划收敛。**就绪 = 连数据面也镜像完布局**（leader 规划完不等于
#       learner 追平，此刻冒烟会读到 learner 上的空槽）。创建时的形态（节点数、
#       每节点角色、端口）会写进 $RUN_DIR/cluster.env —— 之后 **裸跑
#       `restart` / `stop` / `status` / `smoke` 都按真实形态执行**，不需要再传
#       一遍角色。命令行显式传的变量优先于记录值。
#
#   scripts/cluster.sh start     启动集群（编译、按角色两段拉起、等待就绪；形态写入 cluster.env）
#   scripts/cluster.sh status    查看各节点 Raft 角色、Leader 槽数、迁移中槽数
#   scripts/cluster.sh join N    运行时扩一个节点：先拉起 node-N（voter 用 -join 指向现有成员，
#                                learner 先准入目录），扩出的序号与角色记回 cluster.env
#   scripts/cluster.sh remove N  运行时摘掉 node-N（只允许离线节点：进程在跑会先停，等探活标离线后再
#                                DELETE 到 controller；槽主自动迁到存活副本，副本集按新成员数重排）
#   scripts/cluster.sh admit N [learner]  把 node-N 准入目录（数据面节点：不投票、只承载槽副本）
#   scripts/cluster.sh members   列出当前 raft 成员（GET /admin/cluster/nodes）
#   scripts/cluster.sh smoke     端到端冒烟：MOVED 重定向 → v1/v2 写入 → 幂等 exists → 版本冲突 fail/1001 → 回读
#   scripts/cluster.sh slotcheck 副本一致性体检：逐槽比较 leader 与各副本摘要（有发散副本时退出码 1）
#   scripts/cluster.sh logs [N]  跟踪某个节点日志（默认 node-1）
#   scripts/cluster.sh restart   重启（保留数据，验证 WAL/Raft 崩溃恢复；裸跑即可，形态读自 cluster.env）
#   scripts/cluster.sh stop      停止集群（只按 pid 文件收割）
#   scripts/cluster.sh clean     stop 并删除运行目录（含数据与 cluster.env，慎用）
#
# 脚本可用环境变量覆盖（显式传入的值优先于 $RUN_DIR/cluster.env 的记录值）：
#   REPLICAS=3             节点总数。start 在集群已经存在时只拉起缺的，并把它们
#                          -join 进现有集群（REPLICAS=7 start 就是扩容）。未显式
#                          传入且没有记录值时，取运行目录里已有的最大 node-N 序号，
#                          保证裸 restart 不会漏掉上次拉起的节点
#   VOTERS=1               控制面节点数：node-1..VOTERS = voter，其余 = learner。
#                          最小部署 = 1 个 voter + 1 个 learner（默认值即如此）
#   ROLE_<n>=voter|learner  点名节点 n 的角色（优先于 VOTERS 的区间推导）。
#                          voter 只投票、不承载槽副本；learner 只承载副本、不投票，
#                          槽表由控制面经 WatchLayout 分发，不参与选举
#   HOST=127.0.0.1         绑定与广播地址
#   ADMIN_BASE=8091        节点 i 的 admin 管理端口（HTTP admin + pprof）= ADMIN_BASE + i - 1
#   PEER_BASE=8391         节点 i 的 peer 端口（Raft + peer gRPC，全部节点间通讯）= PEER_BASE + i - 1
#   CLIENT_BASE=8591       节点 i 的客户端(gRPC)端口 = CLIENT_BASE + i - 1
#   SEGMENT_BYTES=64MiB    段大小（默认 64MiB；须为 64MiB 的整数倍，最大 2GiB）
#   RAFT_ELECTION_TIMEOUT   raft 选举超时（默认 500ms，即节点默认值）：follower 等多久
#                          没收到心跳就发起选举。必须是 RAFT_HEARTBEAT_TIMEOUT 的
#                          至少 2 倍，否则一次迟到的心跳就会掀掉 leader（节点会拒绝启动）
#   RAFT_HEARTBEAT_TIMEOUT  raft 心跳间隔（默认 100ms）。宿主/容器 CPU 紧张导致心跳
#                          迟到时，把两个值一起放大（例：RAFT_HEARTBEAT_TIMEOUT=500ms
#                          RAFT_ELECTION_TIMEOUT=3s）
#   RUN_DIR=$ROOT/.cluster 运行目录（数据、日志、pid、cluster.env）
#   READY_TIMEOUT=90       等待就绪秒数
#   BUILD=1                start 前强制重新编译
#
# 客户端入口：任意节点都能收读写请求（非本地槽返回 err_id=1003 带目标节点
# 地址，smoke 里演示了跟随重定向）。
#
# 运行时成员变更（不重启、不丢数据）：
#   扩：REPLICAS=7 scripts/cluster.sh start   或   scripts/cluster.sh join 6
#       集群已经存在（有节点在跑，或数据目录里已有 raft 日志）时，缺的节点
#       自己报名加入（voter 按 -peers 成员轮转报名，learner 先准入目录）。
#       扩容后的序号与角色会记回 cluster.env。
#   缩：scripts/cluster.sh remove 2
#       只允许移除离线节点（与后端同规则）：node-2 进程还在跑时脚本先停掉它，
#       等 controller 探活把它标离线后提交 raft 配置删除；该节点收到配置项后才
#       停复制，槽主先迁到存活副本，副本集按剩余成员数重排（补环上的席位、
#       等留下的副本追平后回收多余席位）。数据目录仍在，可手动清理。

set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# 运行目录先定下来：cluster 形态的记录（cluster.env）就放在这里。
RUN_DIR="${RUN_DIR:-$ROOT/.cluster}"

# 集群形态装载优先级：本次命令行显式传的变量 > $RUN_DIR/cluster.env 的记录值
# （start/join 写下）> 默认值。有了这一步，一条命令建好集群之后，裸跑
# restart / stop / status / smoke 都按真实形态执行，不用再传一遍角色。
if [[ -f "$RUN_DIR/cluster.env" ]]; then
  while IFS='=' read -r k v; do
    [[ "$k" =~ ^(REPLICAS|VOTERS|HOST|ADMIN_BASE|PEER_BASE|CLIENT_BASE|SEGMENT_BYTES|RAFT_HEARTBEAT_TIMEOUT|RAFT_ELECTION_TIMEOUT|ROLE_[0-9]+)$ ]] || continue
    [[ -v "$k" ]] || printf -v "$k" '%s' "$v"
  done < "$RUN_DIR/cluster.env"
fi

# REPLICAS 既没显式传、cluster.env 也没有记录（老运行目录）时，取运行目录里
# 已有的最大 node-N 序号：裸 restart 绝不能漏掉上次拉起的节点。这种情况下角色
# 只能按默认推导，所以明确告警——显式传一次 ROLE_<n>/VOTERS 就会被记进
# cluster.env，之后裸跑不再需要传。
if [[ -z "${REPLICAS:-}" ]]; then
  REPLICAS=0
  shopt -s nullglob
  for d in "$RUN_DIR"/node-*/; do
    n="${d%/}"; n="${n##*node-}"
    [[ "$n" =~ ^[0-9]+$ ]] && (( n > REPLICAS )) && REPLICAS=$n
  done
  shopt -u nullglob
  if (( REPLICAS > 0 )) && [[ ! -f "$RUN_DIR/cluster.env" ]]; then
    printf '提示: %s 里有 %s 个节点但没有 cluster.env（老运行目录）：角色按默认推导（node-1..%s=voter，其余=learner）。\n' \
      "$RUN_DIR" "$REPLICAS" "${VOTERS:-1}" >&2
    printf '      显式传一次 ROLE_<n>/VOTERS（或 VOTERS=K）即可把形态记下来，之后裸跑 start/restart/stop 不需要再传。\n' >&2
  fi
  (( REPLICAS == 0 )) && REPLICAS=3
fi

REPLICAS="${REPLICAS:-3}"
VOTERS="${VOTERS:-1}"
HOST="${HOST:-127.0.0.1}"
ADMIN_BASE="${ADMIN_BASE:-8091}"
PEER_BASE="${PEER_BASE:-8391}"
CLIENT_BASE="${CLIENT_BASE:-8591}"
# 段大小：默认 256MiB（须为 64MiB 的整数倍，最大 2GiB；可写字节数或 256MiB/1GiB 带单位）
SEGMENT_BYTES="${SEGMENT_BYTES:-64MiB}"
seg_args=(-segment-bytes "$SEGMENT_BYTES")
# 副本策略不在此转发：PUSHUPES_REPLICA_POLICY 若已在环境里，子进程自然继承；
# 否则用节点自己的默认档（medium=每槽 2 份）。high=每槽 3 份。
# raft 的两个时间旋钮：只在显式设置时才转发，否则用节点自己的默认值（避免把默认值
# 复制到脚本里，节点改默认值时脚本不需要跟着改）。
RAFT_ELECTION_TIMEOUT="${RAFT_ELECTION_TIMEOUT:-}"
RAFT_HEARTBEAT_TIMEOUT="${RAFT_HEARTBEAT_TIMEOUT:-}"
raft_timing_args=()
[[ -n "$RAFT_ELECTION_TIMEOUT" ]] && raft_timing_args+=(-raft-election-timeout "$RAFT_ELECTION_TIMEOUT")
[[ -n "$RAFT_HEARTBEAT_TIMEOUT" ]] && raft_timing_args+=(-raft-heartbeat-timeout "$RAFT_HEARTBEAT_TIMEOUT")
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

# 节点数与控制面规模的合法性在这里判（die 已定义）。
[[ "$REPLICAS" =~ ^[0-9]+$ && "$VOTERS" =~ ^[0-9]+$ ]] || die "REPLICAS/VOTERS 必须是非负整数（REPLICAS=$REPLICAS VOTERS=$VOTERS）"

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
    (cd "$ROOT" && CGO_ENABLED=0 go build -o bin/pushupes ./cmd/pushupes && CGO_ENABLED=0 go build -o bin/grpccheck ./cmd/grpccheck && CGO_ENABLED=0 go build -o bin/slotcheck ./cmd/slotcheck) || die "编译失败"
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
  # 日志跨重启保留：start/restart 只动进程，绝不截断上一世的 node.log——
  # 排障要看的正是「宕机前最后写了什么」，截断会把证据抹掉（实测踩过：
  # 节点宕机后 restart，宕机现场日志荡然无存）。进程本身以 >> 追加，
  # 这里只打一行世代分隔符便于翻查。真正清日志的入口只有 clean。
  printf '\n===== cluster.sh start node-%s @ %s =====\n' "$n" "$(date '+%Y-%m-%d %H:%M:%S %z')" >> "$dir/node.log"
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
  # 形态里有没有数据面节点：决定「槽表 0 条」是纯控制面就绪，还是数据面还没注册。
  local want_learner=0
  for i in $(seq 1 "$REPLICAS"); do
    [[ "$(node_n_role "$i")" == learner ]] && want_learner=1
  done
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
          # 数据面节点是把元数据从控制面 feed 追过来的，比 leader 慢一拍：leader 的
          # 槽表规划完成不代表 learner 也镜像完了。此刻返回「就绪」，紧接着的 smoke
          # 会读到 learner 上的空槽（未追平时本地读答空），看起来像集群坏了。所以等
          # 每个节点（含数据面）都自报同一张非空槽表再宣布就绪。
          if (( want_learner == 1 )); then
            local lagging="" j jbody jplanned jtopo topo
            topo="$(json_field "$body" topology)"
            for j in $(seq 1 "$REPLICAS"); do
              jbody="$(curl -s --max-time 2 "$(base_url "$j")/admin/cluster/status")"
              jplanned="$(json_count "$jbody" epoch)"
              jtopo="$(json_field "$jbody" topology)"
              if [[ "$jplanned" -eq 0 || ( -n "$topo" && "$jtopo" != "$topo" ) ]]; then
                lagging+=" node-$j"
              fi
            done
            if [[ -n "$lagging" ]]; then
              sleep 1
              continue 3   # 回到 while 轮询（for j / for i 之上）
            fi
          fi
          local rf; rf="$(json_field "$body" replica_factor)"
          info "集群就绪：slot 规划 $planned 槽（每槽 ${rf:-?} 副本，按副本策略 × 成员数推导）"
          return 0
        fi
        # 纯控制面（形态里一个数据面节点都没有）就绪时槽表就是 0 条：承载池为空，
        # 控制器按设计不规划，写入由控制面明确拒绝。没有这一支，控制面专用拓扑每次
        # start 都会白等一个 READY_TIMEOUT 再报「未就绪」。
        # 判据是形态（本脚本启动的节点角色）而不是「目录里此刻有没有数据面成员」：
        # 3 控制 + 2 数据启动时，leader 先选出、learner 还没自报地址，那个瞬间目录里
        # 只有 voter——按目录判会把「数据面正在注册」误判成纯控制面，start 提前返回
        # 「就绪」而槽表是空的，随后的 smoke 直接失败。
        if [[ "${state,,}" == "leader" && "$planned" == "0" && "$want_learner" == "0" ]]; then
          info "集群就绪：纯控制面（形态里没有数据面节点），槽表 0 条；等数据面节点加入后自动规划"
          return 0
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
  # 形态先落盘：即使本次启动半途失败，下一次 restart/stop 也按真实形态执行。
  save_cluster_env

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
  # 两段启动：先控制面（voter），再数据面（learner）。数据面分两种：
  # 已在目录里注册过的（共识目录旁有 layout-feed 缓存；目录条目本身经 Raft
  # 持久化，重启不需要重新准入）跟着 voter 一并拉起——若也排队等 controller，
  # restart 窗口里数据面缺席数秒，探活就会把它名下所有槽的 leader 迁到环邻居
  # （实测形态，当时槽数 4096：重启后一个数据节点全部槽领走、另一个 0，回切要按每轮一次
  # 交接慢慢爬）。全新 learner 才需要「等 controller → 准入目录 → 启动」。
  local known=() fresh=()
  for i in "${learners[@]}"; do
    if [[ -s "$(node_dir "$i")/data/layout-feed.json" ]]; then known+=("$i"); else fresh+=("$i"); fi
  done
  for i in "${known[@]}"; do
    start_node "$i"
  done
  if (( ${#fresh[@]} > 0 )); then
    # 等 controller 就绪（选出 leader、controller_admin_addr 落表）。
    local deadline=$((SECONDS + READY_TIMEOUT)) ctrl=""
    while (( SECONDS < deadline )); do
      ctrl="$(controller_admin 2>/dev/null)" || ctrl=""
      [[ -n "$ctrl" ]] && break
      sleep 1
    done
    [[ -n "$ctrl" ]] || die "${READY_TIMEOUT}s 内没有选出 controller，无法准入数据面节点"
    for i in "${fresh[@]}"; do
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
# node_n_role N：节点 N 的角色。ROLE_<n> 点名优先；没点名时按 VOTERS 区间推导
# （node-1..VOTERS = voter 控制面，其余 = learner 数据面）。角色只有两种：
# voter 投票、不承载副本；learner 承载副本、不投票。最小部署 = 1 voter + 1 learner。
node_n_role() {
  local role_var="ROLE_$1"
  local r="${!role_var:-}"
  if [[ -z "$r" ]]; then
    if (( $1 <= VOTERS )); then r=voter; else r=learner; fi
  fi
  [[ "$r" == "voter" || "$r" == "learner" ]] || die "ROLE_$1=$r：角色只能是 voter 或 learner"
  printf '%s' "$r"
}

# save_cluster_env：把本次生效的集群形态（节点数、每节点角色、端口）记进
# $RUN_DIR/cluster.env。之后的裸 start / restart / status / smoke 从这里读回，
# 一条命令创建的集群不需要再传一遍环境变量；显式传入的变量始终优先。
save_cluster_env() {
  local i
  {
    printf '# cluster.sh 写下的集群形态；命令行显式传入的变量优先于这里。\n'
    printf 'REPLICAS=%s\n' "$REPLICAS"
    printf 'VOTERS=%s\n' "$VOTERS"
    printf 'HOST=%s\n' "$HOST"
    printf 'ADMIN_BASE=%s\n' "$ADMIN_BASE"
    printf 'PEER_BASE=%s\n' "$PEER_BASE"
    printf 'CLIENT_BASE=%s\n' "$CLIENT_BASE"
    printf 'SEGMENT_BYTES=%s\n' "$SEGMENT_BYTES"
    for ((i = 1; i <= REPLICAS; i++)); do
      printf 'ROLE_%d=%s\n' "$i" "$(node_n_role "$i")"
    done
  } > "$RUN_DIR/cluster.env"
}

# join_record N：join 成功后把扩出的序号与角色记回 cluster.env，后续裸 restart 不丢节点。
join_record() {
  local n="$1"
  (( n > REPLICAS )) && REPLICAS=$n
  save_cluster_env
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
      if printf '%s' "$members" | grep -o "{\"id\":\"node-$n\"[^}]*}" | grep -q '"client_addr":"http'; then
        info "node-$n 已作为数据面节点上线（不投票；槽表由控制面分发）"
        join_record "$n"
        cmd_members
        return 0
      fi
    elif printf '%s' "$members" | grep -q "\"id\":\"node-$n\""; then
      info "node-$n 已加入 raft 配置（成员变更自动 replan 槽表）"
      join_record "$n"
      cmd_members
      return 0
    fi
    sleep 1
  done
  warn "等待 node-$n 加入超时；日志尾部："
  tail -n 10 "$(log_file "$n")" 2>/dev/null || warn "  （无日志）"
  return 1
}

# cmd_remove N：一条 DELETE 摘掉 node-N。服务端自己走「先排干、再正式移除」：
# 排干标记让它退出布局（不再选它当 leader/席位/迁移目标），rebalancer 的排水轮
# 把它领导的槽按常规交接逐手移交（移交完成前它照常服务读写，运行中的客户端无
# 中断），排干的那一刻摘 raft 配置与目录条目——是否在线/离线服务端不问。
# 脚本只轮询同一条 DELETE 直到 200（202=排干中），**先摘后停**。数据目录不动。
cmd_remove() {
  local n="${1:-}"
  [[ -n "$n" && "$n" =~ ^[0-9]+$ ]] || die "用法: $0 remove <节点序号>"
  local ctrl; ctrl="$(controller_admin)"
  info "移除 node-$n：服务端先排干、再正式移除（客户端无中断）"
  local deadline=$((SECONDS + 600)) resp code attempt=0
  while :; do
    attempt=$((attempt + 1))
    resp="$(curl -s -w '\n%{http_code}' -X DELETE --max-time 40 \
      "http://$ctrl/admin/cluster/nodes/node-$n" 2>/dev/null)"
    code="${resp##*$'\n'}"
    if [[ "$code" == "200" ]]; then
      info "node-$n 已移除（raft 配置与目录条目已摘）"
      break
    fi
    if [[ "$code" == "202" ]]; then
      local led ifl
      led="$(printf '%s' "${resp%$'\n'*}" | grep -o '"leading":[0-9]*' | grep -o '[0-9]*')"
      ifl="$(printf '%s' "${resp%$'\n'*}" | grep -o '"inflight":[0-9]*' | grep -o '[0-9]*')"
      info "node-$n 排干中：leader槽=${led:-?} 在途=${ifl:-?}（第 $attempt 次轮询）"
    elif [[ "$code" == "000" || -z "$code" ]]; then
      info "controller（$ctrl）无应答，换当前 controller 重试"
      ctrl="$(controller_admin)"
    else
      die "移除 node-$n 失败（HTTP $code）：$(printf '%s' "${resp%$'\n'*}" | head -c 400)"
    fi
    if (( SECONDS >= deadline )); then
      die "node-$n 排干超时（仍领导槽未清空）；进程保持在线，排查后可重跑 remove"
    fi
    sleep 2
  done
  # 先摘后停：表里已经没有它、也没有任何槽指向它，此刻停进程对客户端无感。
  if node_pid_alive "$n"; then
    stop_node "$n"
    info "node-$n 进程已停止"
  fi
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
    role="$(printf '%s' "$body" | grep -o "\"node-$i\":{[^}]*}" | grep -o '"role":"[a-z]*"' | head -1 | sed 's/.*:"//; s/"//g')"
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
