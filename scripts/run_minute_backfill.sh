#!/usr/bin/env bash
# 分钟线 23 年回填编排器（在本机 Mac 上跑，把年包推给 253 逐年装载）。
#
# 流程：对每个 year=YYYY.tar.gz：
#   1. scp 到 253 的 $REMOTE_DATA（若上一轮预取已送达则跳过）
#   2. 后台预取下一年（scp 与装载流水线并行）
#   3. ssh 执行 import_minute_bars_clickhouse.py --tar
#   4. 成功则删除远端 tar，追加本地台账；失败即停（台账保证重跑从断点续传）
#
# 用法：
#   nohup bash scripts/run_minute_backfill.sh > logs/manual_backfill/minute_backfill_orchestrator.log 2>&1 &
set -uo pipefail

LOCAL_DIR="/Users/wenruifeng/Documents/WithVW/intraday_1m_vw"
REMOTE=home-debian
REMOTE_DATA=/home/wenruifeng/data/minute_vw
REMOTE_REPO=/home/wenruifeng/projects/stock
LEDGER="$(dirname "$0")/../logs/manual_backfill/minute_backfill_years_done.txt"
mkdir -p "$(dirname "$LEDGER")"; touch "$LEDGER"

ssh "$REMOTE" "mkdir -p $REMOTE_DATA /home/wenruifeng/data/minute_tmp && chown wenruifeng:wenruifeng /home/wenruifeng/data $REMOTE_DATA /home/wenruifeng/data/minute_tmp && chmod 755 $REMOTE_DATA"

years=$(ls "$LOCAL_DIR" | grep -oE 'year=[0-9]{4}' | grep -oE '[0-9]{4}' | sort)
prefetch_pid=""

for y in $years; do
  if grep -q "^$y\$" "$LEDGER"; then echo "[skip] $y 已完成"; continue; fi
  tarname="year=$y.tar.gz"

  # 等待预取（若是它在送这个年份），否则同步推送
  if [ -n "$prefetch_pid" ]; then wait "$prefetch_pid" || true; prefetch_pid=""; fi
  if ! ssh "$REMOTE" "test -s $REMOTE_DATA/$tarname"; then
    echo "[$(date +%T)] scp $tarname ..."
    scp -q "$LOCAL_DIR/$tarname" "$REMOTE:$REMOTE_DATA/" || { echo "scp $y 失败"; exit 1; }
  fi

  # 预取下一年
  next=$(echo "$years" | awk -v y="$y" '$0>y' | head -1)
  if [ -n "$next" ] && ! grep -q "^$next\$" "$LEDGER"; then
    if ! ssh "$REMOTE" "test -s $REMOTE_DATA/year=$next.tar.gz"; then
      scp -q "$LOCAL_DIR/year=$next.tar.gz" "$REMOTE:$REMOTE_DATA/" &
      prefetch_pid=$!
    fi
  fi

  echo "[$(date +%T)] 装载 $y ..."
  if ssh "$REMOTE" "cd $REMOTE_REPO && sudo -u wenruifeng .venv/bin/python scripts/import_minute_bars_clickhouse.py --tar $REMOTE_DATA/$tarname"; then
    ssh "$REMOTE" "rm -f $REMOTE_DATA/$tarname"
    echo "$y" >> "$LEDGER"
    echo "[$(date +%T)] $y 完成"
  else
    echo "[$(date +%T)] $y 装载失败，停止（重跑本脚本可断点续传）"; exit 1
  fi
done
echo "全部年份完成。"
