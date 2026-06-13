#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="${STOCK_REPO_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
PYTHON_BIN="${STOCK_PYTHON_BIN:-$REPO_DIR/.venv/bin/python}"
MARKET="${STOCK_MARKET:-US}"
LOG_DIR="${STOCK_LOG_DIR:-$REPO_DIR/logs}"
LOG_FILE="${STOCK_LOG_FILE:-$LOG_DIR/cron_daily_run.log}"
LOCK_FILE="${STOCK_LOCK_FILE:-/tmp/stock_daily_run.lock}"

mkdir -p "$LOG_DIR"

timestamp() {
  /bin/date "+%Y-%m-%d %H:%M:%S %Z"
}

{
  if ! /usr/bin/flock -n 9; then
    echo "[$(timestamp)] scheduled_update skipped: another run is active"
    exit 0
  fi

  echo "[$(timestamp)] scheduled_update start: repo=$REPO_DIR market=$MARKET"

  if [[ ! -d "$REPO_DIR" ]]; then
    echo "[$(timestamp)] missing repo directory: $REPO_DIR"
    exit 1
  fi

  if [[ ! -x "$PYTHON_BIN" ]]; then
    echo "[$(timestamp)] missing python executable: $PYTHON_BIN"
    exit 1
  fi

  cd "$REPO_DIR"
  export PYTHONUNBUFFERED=1

  "$PYTHON_BIN" main.py scheduled_update --market "$MARKET"

  echo "[$(timestamp)] scheduled_update success"

  if [[ -n "${STOCK_HEALTHCHECK_URL:-}" ]]; then
    if /usr/bin/curl -fsS --max-time 10 --retry 3 "$STOCK_HEALTHCHECK_URL" >/dev/null; then
      echo "[$(timestamp)] healthcheck ping sent"
    else
      echo "[$(timestamp)] WARNING: healthcheck ping failed"
    fi
  fi
} 9>"$LOCK_FILE" >> "$LOG_FILE" 2>&1
