#!/usr/bin/env bash
# 暂停 update_insider_transactions --all 长时回填,避免周日 scheduled_update 的 SEC 步骤
# 跟回填并发触发 SEC 429 / 临时封禁(SEC 节流是进程内 8 req/s,两个进程就是 16 req/s 超
# 10 req/s 上限)。匹配进程名而不是 PID(回填每次重启换 PID,timer 不能依赖固定数字)。
set -u
DRY=0
[ "${1:-}" = '--dry-run' ] && DRY=1
TS="$(date -u '+%Y-%m-%d %H:%M:%S UTC')"
# ps args 严格前缀匹配:cmdline 必须以 .venv/bin/python main.py update_insider_transactions 开头
PIDS=$(ps -eo pid=,args= | awk '/^[ ]*[0-9]+ \.venv\/bin\/python main\.py update_insider_transactions/ {print $1}' | tr '\n' ' ')
PIDS=$(echo "$PIDS" | xargs)
if [ -z "$PIDS" ]; then
  echo "$TS no insider backfill process to STOP"
  exit 0
fi
if [ "$DRY" = 1 ]; then
  echo "$TS DRY-RUN would STOP pid(s): $PIDS"
else
  for pid in $PIDS; do kill -STOP "$pid"; done
  echo "$TS STOPPED insider backfill pid(s): $PIDS"
fi
