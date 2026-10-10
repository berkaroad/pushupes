#!/usr/bin/env bash
# 客户端契约守门：仓库内的 pushupes 客户端（事件面工具）只允许 import
# pkg/ 下的公开契约（pkg/client 路由算法与错误码、pkg/grpcapi 生成桩），
# 不得 import internal/（服务端实现细节）。新增客户端命令时把它加进
# CLIENT_CMDS 即可纳入守门。
set -uo pipefail
cd "$(dirname "$0")/.."

CLIENT_CMDS=(bench batchsmoke grpccheck seed slotcheck)

fail=0
for c in "${CLIENT_CMDS[@]}"; do
  if hits=$(grep -rn 'github.com/berkaroad/pushupes/internal' "cmd/$c" 2>/dev/null); then
    echo "FAIL: cmd/$c 是客户端，不得 import internal/："
    echo "$hits"
    fail=1
  fi
done

if [[ $fail -eq 0 ]]; then
  echo "OK: 客户端命令（${CLIENT_CMDS[*]}）只依赖 pkg/ 公开契约"
fi
exit $fail
