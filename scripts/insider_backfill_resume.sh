#!/usr/bin/env bash
# 等 scheduled_update 真结束后再恢复回填,避免在 SEC 步骤还没跑完时就 CONT 重新撞节流。
# 最多等 4 小时然后强制 CONT(防 scheduled_update 卡死时回填永远不复活)。
set -u
DRY=0
[ "${1:-}" = '--dry-run' ] && DRY=1
TS_NOW() { date -u '+%Y-%m-%d %H:%M:%S UTC'; }
DEADLINE=$(( $(date +%s) + 4*3600 ))
sched_running() {
  ps -eo args= | awk '/^\.venv\/bin\/python main\.py scheduled_update/ {found=1} END{exit !found}'
}
while sched_running; do
  if [ "$(date +%s)" -gt "$DEADLINE" ]; then
    echo "$(TS_NOW) deadline reached, forcing CONT anyway"
    break
  fi
  echo "$(TS_NOW) scheduled_update still running, waiting 60s..."
  [ "$DRY" = 1 ] && break
  sleep 60
done
PIDS=$(ps -eo pid=,args= | awk '/^[ ]*[0-9]+ \.venv\/bin\/python main\.py update_insider_transactions/ {print $1}' | tr '\n' ' ')
PIDS=$(echo "$PIDS" | xargs)
if [ -z "$PIDS" ]; then
  echo "$(TS_NOW) insider backfill process gone — no CONT needed"
  exit 0
fi
if [ "$DRY" = 1 ]; then
  echo "$(TS_NOW) DRY-RUN would CONT pid(s): $PIDS"
else
  for pid in $PIDS; do kill -CONT "$pid"; done
  echo "$(TS_NOW) CONTINUED insider backfill pid(s): $PIDS"
fi
