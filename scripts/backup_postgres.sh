#!/usr/bin/env bash
set -euo pipefail

# 生产库跑在 Docker 容器 stock-postgres 里，宿主机不一定装了匹配版本的 pg_dump，
# 因此默认走 `docker exec` 在容器内 dump（--format=custom 二进制流写到宿主机文件）。
# 运行用户可能不在 docker 组但有免密 sudo docker（253 即如此）：STOCK_DOCKER_SUDO=auto
# 时自动探测——能直连 docker 就直连，否则回退 `sudo -n docker`。
# 设 STOCK_BACKUP_MODE=host 可改用宿主机 pg_dump + DATABASE_URL（需自行装 postgresql-client）。

REPO_DIR="${STOCK_REPO_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
BACKUP_DIR="${STOCK_BACKUP_DIR:-$HOME/stock_backups/postgres}"
RETENTION_DAYS="${STOCK_BACKUP_RETENTION_DAYS:-30}"
BACKUP_MODE="${STOCK_BACKUP_MODE:-docker}"
PG_CONTAINER="${STOCK_PG_CONTAINER:-stock-postgres}"
DOCKER_SUDO="${STOCK_DOCKER_SUDO:-auto}"
PG_DUMP_BIN="${PG_DUMP_BIN:-pg_dump}"
TIMESTAMP="$(/bin/date -u '+%Y%m%dT%H%M%SZ')"

if [[ -f "$REPO_DIR/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "$REPO_DIR/.env"
  set +a
fi

POSTGRES_USER="${POSTGRES_USER:-postgres}"
POSTGRES_DB="${POSTGRES_DB:-stock}"

# 选定 docker 调用方式：直连 / sudo -n docker / 显式开关
resolve_docker() {
  case "$DOCKER_SUDO" in
    1|true|yes) DOCKER=(sudo -n docker) ;;
    0|false|no) DOCKER=(docker) ;;
    auto)
      if docker ps >/dev/null 2>&1; then
        DOCKER=(docker)
      elif sudo -n docker ps >/dev/null 2>&1; then
        DOCKER=(sudo -n docker)
      else
        echo "无法访问 docker（用户不在 docker 组且无免密 sudo docker）" >&2
        return 1
      fi
      ;;
    *) echo "unknown STOCK_DOCKER_SUDO='$DOCKER_SUDO' (use auto/1/0)" >&2; return 1 ;;
  esac
}

mkdir -p "$BACKUP_DIR"
chmod 700 "$BACKUP_DIR"

OUT="$BACKUP_DIR/stock_${TIMESTAMP}.dump"
TMP="$OUT.tmp"

dump_via_docker() {
  if ! command -v docker >/dev/null 2>&1; then
    echo "docker not found; cannot dump from container '$PG_CONTAINER'" >&2
    return 1
  fi
  resolve_docker || return 1
  if ! "${DOCKER[@]}" ps --format '{{.Names}}' | grep -qx "$PG_CONTAINER"; then
    echo "container '$PG_CONTAINER' is not running; cannot back up" >&2
    return 1
  fi
  "${DOCKER[@]}" exec -i "$PG_CONTAINER" pg_dump \
    -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
    --format=custom --no-owner --no-acl > "$TMP"
}

dump_via_host() {
  if ! command -v "$PG_DUMP_BIN" >/dev/null 2>&1; then
    echo "$PG_DUMP_BIN not found on host (install postgresql-client matching the server)" >&2
    return 1
  fi
  if [[ -z "${DATABASE_URL:-}" ]]; then
    echo "DATABASE_URL is required for host-mode backup (set it in .env)" >&2
    return 1
  fi
  "$PG_DUMP_BIN" --format=custom --no-owner --no-acl --file="$TMP" "$DATABASE_URL"
}

case "$BACKUP_MODE" in
  docker) dump_via_docker ;;
  host)   dump_via_host ;;
  *)      echo "unknown STOCK_BACKUP_MODE='$BACKUP_MODE' (use 'docker' or 'host')" >&2; exit 1 ;;
esac

# dump 失败时 set -e/ pipefail 已经退出；这里再防一手空文件
if [[ ! -s "$TMP" ]]; then
  echo "backup produced an empty file; aborting" >&2
  rm -f "$TMP"
  exit 1
fi

chmod 600 "$TMP"
mv "$TMP" "$OUT"

if [[ "$RETENTION_DAYS" =~ ^[0-9]+$ ]] && [[ "$RETENTION_DAYS" -gt 0 ]]; then
  /usr/bin/find "$BACKUP_DIR" -name 'stock_*.dump' -type f -mtime +"$RETENTION_DAYS" -delete
fi

if [[ -n "${STOCK_BACKUP_RSYNC_TARGET:-}" ]]; then
  rsync -a --chmod=F600,D700 "$OUT" "$STOCK_BACKUP_RSYNC_TARGET"
fi

echo "Backup written: $OUT"
echo "Restore drill example: createdb stock_restore_test && pg_restore --clean --if-exists --dbname=stock_restore_test $OUT"
