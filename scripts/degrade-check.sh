#!/usr/bin/env bash
# 降级验收：保留数据面的槽数据，把控制面的共识状态整体重建，验证这条链路
#   世代不符 ⇒ 本地副本 pending（来源未证明）⇒ 写入 fail/1007 ⇒ 采纳 ⇒ 自愈
#
# 它做的事（沿用 cluster.sh 的一套环境变量与端口约定）：
#   1. 预检：所有节点在跑，数据面节点已有槽且 pending 为 0；
#   2. 停簇；只删「控制面节点」的 data/cluster（共识目录）——槽数据目录不动；
#   3. 起控制面（新世代）；此时槽表应为 0（承载池为空，不规划）；
#   4. 把数据面节点 join 回来（准入 + 启动 + 自报地址）；
#   5. 断言：数据面本地副本 pending > 0 且原因含「世代」；
#   6. 断言：写入被拒（smoke 必须因 err_id:1007 失败——未证明的副本不得服务）；
#   7. 采纳（每个数据面节点各一次，再补一次清掉再均衡带上来的残留）；
#   8. 断言：pending 归零、smoke 通过、slotcheck 无发散。
#
# 用法：
#   YES=1 RUN_DIR=<运行目录> PEER_BASE=... ADMIN_BASE=... CLIENT_BASE=... \
#     REPLICAS=<控制面节点数> ROLE_1=voter ROLE_2=voter ROLE_3=voter \
#     ROLE_4=learner ROLE_5=learner scripts/degrade-check.sh
#
# 环境变量：
#   YES=1            必须显式给出：脚本会删掉控制面节点的 data/cluster（含其 raft 日志
#                    与槽表）。数据面的槽数据不受影响，但这是不可逆操作，故不设默认。
#   CONTROL_NODES    要重建共识状态、保留槽数据的控制面节点序号（默认 = ROLE_<n> 为
#                    voter 的节点；一个都没有则报错退出）
#   DATA_NODES       数据面节点序号（默认 = ROLE_<n> 为 learner 的节点）
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CLUSTER="$ROOT/scripts/cluster.sh"
HOST="${HOST:-127.0.0.1}"
RUN_DIR="${RUN_DIR:-$ROOT/.cluster}"
REPLICAS="${REPLICAS:-3}"
ADMIN_BASE="${ADMIN_BASE:-8091}"
PEER_BASE="${PEER_BASE:-8391}"
CLIENT_BASE="${CLIENT_BASE:-8591}"
READY_TIMEOUT="${READY_TIMEOUT:-90}"

info() { printf '%s\n' "$*"; }
warn() { printf '%s\n' "$*" >&2; }
die()  { printf '错误: %s\n' "$*" >&2; exit 1; }

admin_port() { echo $((ADMIN_BASE + $1 - 1)); }
base_url()   { echo "http://$HOST:$(admin_port "$1")"; }
node_dir()   { echo "$RUN_DIR/node-$1"; }

# 每个节点的角色来自 ROLE_<n>（与 cluster.sh 同源：默认 node-1=voter、其余=learner）。
# 序号超过 REPLICAS 的是运行时加入者，没有显式 ROLE_<n> 就没有角色 —— 否则
# data_nodes 会把空序号也当成 learner 数进降级计划。
node_n_role() {
  local v="ROLE_$1"; local r="${!v:-}"
  if [[ -z "$r" && "$1" -le "$REPLICAS" ]]; then
    if [[ "$1" == "1" ]]; then r=voter; else r=learner; fi
  fi
  printf '%s' "$r"
}

control_nodes() {
  if [[ -n "${CONTROL_NODES:-}" ]]; then printf '%s\n' $CONTROL_NODES; return; fi
  local i
  for i in $(seq 1 "$REPLICAS"); do
    [[ "$(node_n_role "$i")" == "voter" ]] && printf '%s\n' "$i"
  done
}

data_nodes() {
  if [[ -n "${DATA_NODES:-}" ]]; then printf '%s\n' $DATA_NODES; return; fi
  local i n="${NODES_MAX:-$((REPLICAS + 8))}"
  for i in $(seq 1 "$n"); do
    [[ "$(node_n_role "$i")" == "learner" ]] && printf '%s\n' "$i"
  done
}

# pending_count N：该节点当前「离线待认领副本」条数。
pending_count() {
  curl -s --max-time 5 "$(base_url "$1")/admin/slots/pending" 2>/dev/null \
    | grep -o '"slot":' | wc -l | tr -d ' '
}

# pending_reasons N：pending 的原因文案（去重打印，便于确认是世代不符）。
pending_reasons() {
  curl -s --max-time 5 "$(base_url "$1")/admin/slots/pending" 2>/dev/null \
    | grep -o '"reason":"[^"]*"' | sed 's/"reason":"//; s/"$//' | sort -u
}

# layout_slots N：该节点表里「已分配」的槽数（-1 = 无响应）。数的是每条槽位记录的
# `"epoch"`——status 里只有 slots 段的条目带这个键（raft 段/peers 段都没有），所以不需要
# python/jq，与 cluster.sh 的口径一致。
layout_slots() {
  local body
  body="$(curl -s --max-time 5 "$(base_url "$1")/admin/cluster/status" 2>/dev/null)" || { echo -1; return; }
  [[ -n "$body" ]] || { echo -1; return; }
  printf '%s' "$body" | grep -o '"slot_count":[0-9]*' >/dev/null || { echo -1; return; }
  printf '%s' "$body" | grep -o '"epoch":[0-9]*' | wc -l | tr -d ' '
}

topology_of() { cat "$(node_dir "$1")/data/cluster/topology" 2>/dev/null; }

CONTROLS="$(control_nodes)"
DATAS="$(data_nodes)"
[[ -n "$CONTROLS" ]] || die "没有控制面节点：用 ROLE_<n>=voter 标注，或显式给 CONTROL_NODES"
[[ -n "$DATAS" ]] || die "没有数据面节点：用 ROLE_<n>=learner 标注，或显式给 DATA_NODES"
[[ "${YES:-}" == "1" ]] || die "这会删除控制面节点的 data/cluster（共识目录，不可逆）。确认无误后加 YES=1 重跑"

OLD_TOPOS=""
for n in $CONTROLS; do
  t="$(topology_of "$n")"
  OLD_TOPOS+="$n=$t "
done

info "=== 降级验收：重建控制面共识状态，保留数据面槽数据"
info "  控制面节点（将清空 data/cluster）：$CONTROLS"
info "  数据面节点（槽数据保留）：$DATAS"
info "  重建前的世代：$OLD_TOPOS"

# ---- 1) 预检 ----------------------------------------------------------------
for n in $CONTROLS $DATAS; do
  slots="$(layout_slots "$n")"
  [[ "$slots" -ge 0 ]] || die "node-$n 无响应（$(base_url "$n")）——先把集群跑起来"
done
for n in $DATAS; do
  slots="$(layout_slots "$n")"
  [[ "$slots" -gt 0 ]] || die "node-$n 当前未承载槽（slots=$slots）：这份验收要求数据面已有数据"
  p="$(pending_count "$n")"
  [[ "$p" == "0" ]] || die "node-$n 当前已有 $p 个 pending：起点不干净，先采纳再跑本脚本"
done
info "  预检通过：$(for n in $DATAS; do printf 'node-%s=%s槽 ' "$n" "$(layout_slots "$n")"; done)"

# ---- 2) 停簇，只清控制面的共识目录 ------------------------------------------
RUN_DIR="$RUN_DIR" PEER_BASE="$PEER_BASE" ADMIN_BASE="$ADMIN_BASE" CLIENT_BASE="$CLIENT_BASE" \
  REPLICAS="$REPLICAS" "$CLUSTER" stop >/dev/null 2>&1
for n in $CONTROLS; do
  rm -rf "$(node_dir "$n")/data/cluster" || die "删除 node-$n 的共识目录失败"
done
info "  已停簇并清空控制面共识目录（数据面目录未动）"

# ---- 3) 起控制面（新世代，槽表应为 0） --------------------------------------
# cluster.sh 的 start 等的是「槽表已规划」，而重建后的控制面按设计就是 0 槽（承载池
# 为空）⇒ 它的退出码在这里不代表失败。就绪自己判：每个控制面节点可达、世代文件已写出。
"$CLUSTER" start >/dev/null 2>&1 || true
first="$(printf '%s' "$CONTROLS" | head -1)"
deadline=$((SECONDS + READY_TIMEOUT))
while :; do
  if [[ -n "$(topology_of "$first")" && "$(layout_slots "$first")" != "-1" ]]; then
    break
  fi
  if (( SECONDS >= deadline )); then
    warn "控制面启动超时（${READY_TIMEOUT}s），日志尾部："
    tail -n 5 "$(node_dir "$first")/node.log" 2>/dev/null
    die "控制面未能启动"
  fi
  sleep 1
done
NEW_TOPO="$(topology_of "$first")"
[[ -n "$NEW_TOPO" ]] || die "读不到新世代 id（$(node_dir "$first")/data/cluster/topology）"
[[ "$NEW_TOPO" != "$(printf '%s' "$OLD_TOPOS" | sed -n "s/^$first=\([^ ]*\).*/\1/p")" ]] \
  || die "新世代 id 与重建前相同（$NEW_TOPO）：共识目录没被真正重建"
slots="$(layout_slots "$first")"
[[ "$slots" == "0" ]] || die "新建的控制面已有 $slots 个槽：承载池应为空（数据面还没回来）"
info "  控制面已就绪，新世代 $NEW_TOPO，槽表 0 条（符合预期）"

# ---- 4) 数据面带旧数据回来 --------------------------------------------------
for n in $DATAS; do
  if ! "$CLUSTER" join "$n" >/dev/null 2>&1; then
    warn "node-$n join 失败，日志尾部："
    tail -n 5 "$(node_dir "$n")/node.log" 2>/dev/null
    die "node-$n 未能作为数据面节点上线"
  fi
done
info "  数据面节点已上线：$(for n in $DATAS; do printf 'node-%s ' "$n"; done)"

# 围栏只在表里看得到槽时才判来源（没分配就没有「这个副本属于谁」的问题），而控制器
# 要等承载池稳定一拍才提交 plan——所以先等表真的规划出来，再去数 pending，否则会
# 把「还没规划」误读成「围栏失效」。
deadline=$((SECONDS + READY_TIMEOUT))
for n in $DATAS; do
  while :; do
    s="$(layout_slots "$n")"
    [[ "$s" -gt 0 ]] && break
    if (( SECONDS >= deadline )); then
      die "node-$n 的槽表仍为空：控制器没有规划（数据面节点注册了吗？）"
    fi
    sleep 1
  done
done
sleep 2 # 留一拍给围栏判定

# ---- 5) 副本来源未证明 ⇒ pending --------------------------------------------
# 口径：围栏判的是「以领导者身份服务」的副本（owner.go 开头那段：只有它会把自己的
# 序列空间混进槽里）。因此一个在本世代不领任何槽的数据面节点报 0 条 pending 是正确
# 的——它的旧副本由抓取/发散检测管，等哪天领导权落到它头上，那一拍才会被判 pending。
# 所以这里断言的是「集群里出现了以世代不符为原因的 pending」，逐节点数字只作信息打印。
total_pending=0
for n in $DATAS; do
  p="$(pending_count "$n")"
  total_pending=$((total_pending + p))
  info "  node-$n pending $p 条$([[ "$p" != "0" ]] && printf '，原因：%s' "$(pending_reasons "$n" | paste -sd' ; ' -)")"
done
[[ "$total_pending" -gt 0 ]] || die "没有任何数据面节点把旧副本判为 pending：世代围栏失效"
found_reason=0
for n in $DATAS; do
  pending_reasons "$n" | grep -q 'generation' && found_reason=1
done
(( found_reason == 1 )) || die "pending 的原因不是世代不符：$(for n in $DATAS; do pending_reasons "$n"; done)"
info "  合计 $total_pending 条 pending，原因均为「世代不符」" 

# ---- 6) 未证明的副本不得服务：写入必须被拒（fail/1007） --------------------
smoke_out="$("$CLUSTER" smoke 2>&1)"
if printf '%s' "$smoke_out" | grep -q '1007'; then
  info "  写入按预期被拒（fail/1007 SlotUnproven）"
else
  printf '%s\n' "$smoke_out" | tail -n 3 >&2
  die "采纳前的写入没有被拒：未证明的副本正在服务（期望 err_id:1007）"
fi

# ---- 7) 采纳 ----------------------------------------------------------------
for n in $DATAS; do
  resp="$(curl -s --max-time 30 -X POST "$(base_url "$n")/admin/slots/pending/adopt" -d '{}')"
  info "  node-$n 采纳：$resp"
done
sleep 2
for n in $DATAS; do
  # 再均衡把领导权交给某个副本时，它的记录还是旧世代 ⇒ 会再次 pending，补一次即可。
  p="$(pending_count "$n")"
  if [[ "$p" != "0" ]]; then
    resp="$(curl -s --max-time 30 -X POST "$(base_url "$n")/admin/slots/pending/adopt" -d '{}')"
    info "  node-$n 二次采纳（清残留 $p 条）：$resp"
    sleep 1
  fi
done

# ---- 8) 自愈后：pending 归零 + 冒烟 + 一致性 --------------------------------
fail=0
for n in $DATAS; do
  p="$(pending_count "$n")"
  if [[ "$p" != "0" ]]; then
    warn "node-$n 仍有 $p 条 pending"
    fail=1
  fi
done

smoke_out="$("$CLUSTER" smoke 2>&1)"
if printf '%s' "$smoke_out" | grep -q 'SMOKE PASS'; then
  info "  采纳后冒烟通过"
else
  printf '%s\n' "$smoke_out" | tail -n 3 >&2
  warn "采纳后冒烟未通过"
  fail=1
fi

check_out="$("$CLUSTER" slotcheck 2>&1)"
printf '%s\n' "$check_out" | tail -n 1
if printf '%s' "$check_out" | grep -q 'diverged: 0' && printf '%s' "$check_out" | grep -q 'behind: 0'; then
  info "  副本一致性：无发散、无落后"
else
  warn "副本一致性检查未通过"
  fail=1
fi

if (( fail == 0 )); then
  info "降级验收通过：数据面的槽数据在控制面整体重建后仍然可用（pending → fail/1007 → 采纳 → 自愈）"
  exit 0
fi
die "降级验收未通过（见上面的告警）"
