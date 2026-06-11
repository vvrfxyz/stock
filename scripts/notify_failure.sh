#!/usr/bin/env bash
# systemd OnFailure 钩子：stock-notify-failure@<unit>.service 调用本脚本。
# 行为：
#   1) 永远追加一条记录到 logs/failures.log（最低保障，无外部依赖）。
#   2) 若 .env 中配置了 STOCK_NOTIFY_URL，则 POST 一条文本消息（适配 ntfy / 自建 webhook，
#      Bark 可用 https://api.day.app/<key>/ 形式 — 消息会作为 POST body 发送）。
set -u

REPO_DIR="${STOCK_REPO_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
FAILED_UNIT="${1:-unknown-unit}"
HOST="$(hostname)"
NOW="$(date '+%Y-%m-%d %H:%M:%S %Z')"
MSG="[stock] ${HOST} 上 ${FAILED_UNIT} 执行失败 @ ${NOW}。排查: journalctl -u ${FAILED_UNIT} -n 100; tail logs/cron_daily_run.log"

mkdir -p "${REPO_DIR}/logs"
echo "${NOW} ${FAILED_UNIT} FAILED" >> "${REPO_DIR}/logs/failures.log"

# 从 .env 读取可选的通知地址（不 source 整个文件，避免副作用）
NOTIFY_URL=""
if [[ -f "${REPO_DIR}/.env" ]]; then
  NOTIFY_URL="$(grep -E '^STOCK_NOTIFY_URL=' "${REPO_DIR}/.env" | tail -1 | cut -d= -f2- | tr -d '"' || true)"
fi

if [[ -n "${NOTIFY_URL}" ]]; then
  curl -fsS -m 10 -X POST -H 'Content-Type: text/plain; charset=utf-8' \
    --data "${MSG}" "${NOTIFY_URL}" >/dev/null 2>&1 \
    || echo "${NOW} notify webhook failed" >> "${REPO_DIR}/logs/failures.log"
fi

exit 0
