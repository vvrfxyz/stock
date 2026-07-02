#!/usr/bin/env bash
set -euo pipefail

# 生产库跑在 Docker 容器 stock-postgres 里，宿主机不一定装了匹配版本的 pg_dump，
# 因此默认走 `docker exec` 在容器内 dump（--format=custom 二进制流写到宿主机文件）。
# 运行用户可能不在 docker 组但有免密 sudo docker（253 即如此）：STOCK_DOCKER_SUDO=auto
# 时自动探测——能直连 docker 就直连，否则回退 `sudo -n docker`。
# 设 STOCK_BACKUP_MODE=host 可改用宿主机 pg_dump（需自行装 postgresql-client）；
# DATABASE_URL 会被解析成 PGHOST/PGPORT/PGUSER/PGPASSWORD/PGDATABASE 环境变量
# 供 pg_dump 读取，含口令的 URL 不进 argv（ps 全局可见）。

REPO_DIR="${STOCK_REPO_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
BACKUP_DIR="${STOCK_BACKUP_DIR:-$HOME/stock_backups/postgres}"
RETENTION_DAYS="${STOCK_BACKUP_RETENTION_DAYS:-30}"
BACKUP_MODE="${STOCK_BACKUP_MODE:-docker}"
PG_CONTAINER="${STOCK_PG_CONTAINER:-stock-postgres}"
DOCKER_SUDO="${STOCK_DOCKER_SUDO:-auto}"
PG_DUMP_BIN="${PG_DUMP_BIN:-pg_dump}"
TIMESTAMP="$(/bin/date -u '+%Y%m%dT%H%M%SZ')"

# 只从 .env 提取本脚本需要的键；不整体 source（set -a 会把全部秘密
# 导出进当前环境，并被 docker/pg_dump/rsync 等子进程继承）。
env_file_get() {
  local key="$1" line
  [[ -f "$REPO_DIR/.env" ]] || return 1
  line="$(grep -E "^[[:space:]]*(export[[:space:]]+)?${key}=" "$REPO_DIR/.env" | tail -n 1 || true)"
  [[ -n "$line" ]] || return 1
  line="${line#*=}"
  line="${line%$'\r'}"
  if [[ ${#line} -ge 2 && ( "$line" == \"*\" || "$line" == \'*\' ) ]]; then
    line="${line:1:${#line}-2}"
  fi
  printf '%s' "$line"
}

if _v="$(env_file_get DATABASE_URL)"; then DATABASE_URL="$_v"; fi
if _v="$(env_file_get POSTGRES_USER)"; then POSTGRES_USER="$_v"; fi
if _v="$(env_file_get POSTGRES_DB)"; then POSTGRES_DB="$_v"; fi
unset _v

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
  if ! command -v python3 >/dev/null 2>&1; then
    echo "python3 not found on host (needed to parse DATABASE_URL)" >&2
    return 1
  fi
  # 含口令的 URL 不进 pg_dump argv（ps 全局可见）：经环境变量传给 python
  # 解析成 libpq 的 PG* 变量，pg_dump 从子 shell 环境读取连接信息。
  local pg_env
  pg_env="$(STOCK_DB_URL="$DATABASE_URL" python3 - <<'PY'
import os
from urllib.parse import parse_qs, unquote, urlsplit

url = urlsplit(os.environ["STOCK_DB_URL"])
query = parse_qs(url.query)
pairs = {
    "PGHOST": unquote(url.hostname) if url.hostname else "",
    "PGPORT": str(url.port) if url.port else "",
    "PGUSER": unquote(url.username) if url.username else "",
    "PGPASSWORD": unquote(url.password) if url.password else "",
    "PGDATABASE": unquote(url.path.lstrip("/")),
    "PGSSLMODE": (query.get("sslmode") or [""])[0],
}
for key, value in pairs.items():
    if value:
        print(f"{key}={value}")
PY
)" || return 1
  (
    while IFS='=' read -r key value; do
      if [[ -n "$key" ]]; then export "$key=$value"; fi
    done <<<"$pg_env"
    exec "$PG_DUMP_BIN" --format=custom --no-owner --no-acl --file="$TMP"
  )
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
