#!/usr/bin/env bash
# PushupES 集群启停脚本
#
# 默认拉起 3 节点（成员集合静态：各节点启动时按 -peers 写入同一 voter 集，
# 无 bootstrap/join 差异），每个节点独立的
# admin / peer(Raft) / client(gRPC) 端口与数据目录，槽位自动均衡。
#
#   scripts/cluster.sh start     启动集群（编译、逐个拉起、等待选主与槽规划，并打印管理台多地址启动命令）
#   scripts/cluster.sh status    查看各节点 Raft 角色、Leader 槽数、迁移中槽数
#   scripts/cluster.sh smoke     端到端冒烟：MOVED 重定向 → v1/v2 写入 → 幂等 exists → 版本冲突 fail/1001 → 回读
#   scripts/cluster.sh slotcheck 副本一致性体检：逐槽比较 leader 与各副本摘要（有发散副本时退出码 1）
#   scripts/cluster.sh logs [N]  跟踪某个节点日志（默认 node-1）
#   scripts/cluster.sh restart   重启（保留数据，验证 WAL/Raft 崩溃恢复）
#   scripts/cluster.sh stop      停止集群
#   scripts/cluster.sh clean     stop 并删除运行目录（含数据，慎用）
#
# 脚本可用环境变量覆盖：
#   REPLICAS=3             节点数
#   HOST=127.0.0.1         绑定与广播地址
#   ADMIN_BASE=8091         节点 i 的 admin 管理端口（HTTP admin + pprof）= ADMIN_BASE + i - 1
#   PEER_BASE=8391         节点 i 的 peer 端口（Raft + peer gRPC，全部节点间通讯）= PEER_BASE + i - 1
#   CLIENT_BASE=8591         节点 i 的客户端(gRPC)端口 = CLIENT_BASE + i - 1
#   REPLICATION_FACTOR=2   每槽副本数（含 leader；改大后重启集群会自动补齐副本）
#   SEGMENT_BYTES=256MiB   段大小（默认 256MiB；须为 64MiB 的整数倍，最大 2GiB）
#   RUN_DIR=$ROOT/.cluster 运行目录（数据、日志、pid）
#   READY_TIMEOUT=90       等待就绪秒数
#   BUILD=1                start 前强制重新编译
#
# 客户端入口：任意节点都能收写请求（非 leader 返回 err_id=1003 带目标节点
# 地址，smoke 里演示了跟随重定向）；读走各节点本地 ≤HW 副本。

set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

REPLICAS="${REPLICAS:-3}"
HOST="${HOST:-127.0.0.1}"
ADMIN_BASE="${ADMIN_BASE:-8091}"
PEER_BASE="${PEER_BASE:-8391}"
CLIENT_BASE="${CLIENT_BASE:-8591}"
REPLICATION_FACTOR="${REPLICATION_FACTOR:-2}"
# 段大小：默认 256MiB（须为 64MiB 的整数倍，最大 2GiB；可写字节数或 256MiB/1GiB 带单位）
SEGMENT_BYTES="${SEGMENT_BYTES:-256MiB}"
seg_args=(-segment-bytes "$SEGMENT_BYTES")
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
peers_csv() {
  local list="" i
  for i in $(seq 1 "$REPLICAS"); do
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
  # -bootstrap 已废弃：成员集合静态，每个节点启动时按 -peers 写入同一 voter 集，
  # 不再有「先 bootstrap 再 join」的差异。

  cd "$dir" || die "无法进入 $dir"
  # setsid 让节点脱离本脚本会话；fd 全部重定向，否则管道调用永不返回
  if command -v setsid >/dev/null 2>&1; then
    setsid "$BIN" -node "node-$n" \
       -admin "$HOST:$(admin_port "$n")" -peer "$HOST:$(peer_port "$n")" \
      -client "$HOST:$(client_port "$n")" \
      -data ./data -peers "$peers" -replication-factor "$REPLICATION_FACTOR" \
      "${seg_args[@]}" "${extra[@]}" >> node.log 2>&1 < /dev/null &
  else
    nohup "$BIN" -node "node-$n" \
       -admin "$HOST:$(admin_port "$n")" -peer "$HOST:$(peer_port "$n")" \
      -client "$HOST:$(client_port "$n")" \
      -data ./data -peers "$peers" -replication-factor "$REPLICATION_FACTOR" \
      "${seg_args[@]}" "${extra[@]}" >> node.log 2>&1 < /dev/null &
  fi
  pid=$!
  echo "$pid" > node.pid
  cd "$ROOT"
  info "node-$n 启动中: admin=$(admin_port "$n") peer=$(peer_port "$n") client=$(client_port "$n") pid=$pid"
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
          info "集群就绪：slot 规划 $planned 槽（每槽 $REPLICATION_FACTOR 副本）"
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

  for i in "${to_start[@]}"; do start_node "$i"; done
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
  for i in $(seq 1 "$REPLICAS"); do list+="${list:+,}$HOST:$(admin_port "$i")"; done
  "$ROOT/bin/slotcheck" -admins "$list" "$@"
}

cmd_status() {
  local i body state leader leaders migrating
  info "=== PushupES 集群（REPLICAS=$REPLICAS）==="
  for i in $(seq 1 "$REPLICAS"); do
    body="$(curl -s --max-time 3 "$(base_url "$i")/admin/cluster/status" 2>/dev/null)"
    if [[ -z "$body" ]]; then
      info "  node-$i: 无响应（$(node_pid_alive "$i" && echo 进程存活 || echo 未运行)）"
      continue
    fi
    state="$(json_field "$body" state)"
    leaders="$(printf '%s' "$body" | grep -o "\"leader\":\"node-$i\"" | wc -l | tr -d ' ')"
    migrating="$(printf '%s' "$body" | grep -o '"state":"migrating_' | wc -l | tr -d ' ')"
    info "  node-$i: raft=${state} leader槽=${leaders} 迁移中槽=${migrating} admin=$(admin_port "$i") client=$(client_port "$i")"
  done
}

# 事件写入/查询只走 gRPC 数据面：冒烟直接调用 bin/grpccheck（MOVED 重定向 →
# v1/v2 写入 → 版本冲突 fail/1001 → 三节点回读 → 幂等 exists → by-command），
# 断言全部内建在程序里，退出码即结果。
cmd_smoke() {
  local addrs="" i
  [[ -x "$ROOT/bin/grpccheck" ]] || die "缺少 bin/grpccheck，先 BUILD=1 scripts/cluster.sh start"
  for i in $(seq 1 "$REPLICAS"); do
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
  smoke)   cmd_smoke ;;
  slotcheck) shift; cmd_slotcheck "$@" ;;
  logs)    cmd_logs "${2:-1}" ;;
  restart) cmd_stop; sleep 1; cmd_start ;;
  clean)   cmd_clean ;;
  ""|-h|--help|help) usage ;;
  *)       die "未知命令 '$1'（可用: start stop status smoke slotcheck logs restart clean）" ;;
esac
