#!/usr/bin/env bash
# 暂停 SEC 长时回填进程(insider --all / 13F --quarter),避免周日 scheduled_update 的 SEC 步骤
# 跟回填并发触发 SEC 429 / 临时封禁(SEC 节流是进程内 8 req/s,两个进程就是 16 req/s 超
# 10 req/s 上限)。匹配进程名而不是 PID(回填每次重启换 PID,timer 不能依赖固定数字)。
set -u
DRY=0
[ "${1:-}" = '--dry-run' ] && DRY=1
TS="$(date -u '+%Y-%m-%d %H:%M:%S UTC')"
# python 可能以相对(.venv/bin/python)或绝对路径出现,用 [^ ]*python 前缀统配;
# awk 自身 cmdline 以 "awk" 开头,不会被该前缀误匹配。
PIDS=$(ps -eo pid=,args= | awk '/^[ ]*[0-9]+ [^ ]*python main\.py update_(insider_transactions|institutional_holdings)/ {print $1}' | tr '\n' ' ')
PIDS=$(echo "$PIDS" | xargs)
if [ -z "$PIDS" ]; then
  echo "$TS no SEC backfill process to STOP"
  exit 0
fi
if [ "$DRY" = 1 ]; then
  echo "$TS DRY-RUN would STOP pid(s): $PIDS"
else
  for pid in $PIDS; do kill -STOP "$pid"; done
  echo "$TS STOPPED SEC backfill pid(s): $PIDS"
fi
