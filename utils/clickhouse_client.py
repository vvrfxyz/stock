from __future__ import annotations

import json
import os
import threading
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable

import requests
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parents[1]
CLICKHOUSE_DDL_PATH = PROJECT_ROOT / "sql" / "clickhouse" / "polyglot_persistence.sql"


def _env_enabled(name: str, default: bool = True) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() not in {"0", "false", "no", "off"}


def _to_decimal_string(value: Any, scale: str = "1.000000") -> str | None:
    if value is None:
        return None
    try:
        decimal_value = Decimal(str(value))
        if not decimal_value.is_finite():
            return None
        return f"{decimal_value.quantize(Decimal(scale)):f}"
    except (InvalidOperation, TypeError, ValueError):
        return None


def _to_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _to_date_string(value: Any) -> str | None:
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, str) and value:
        return value[:10]
    return None


def _utc_now_string() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def _split_sql(sql: str) -> list[str]:
    statements: list[str] = []
    current: list[str] = []
    for line in sql.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("--"):
            continue
        current.append(line)
        if stripped.endswith(";"):
            statements.append("\n".join(current).rstrip(";").strip())
            current = []
    if current:
        statements.append("\n".join(current).strip())
    return statements


class ClickHouseClient:
    def __init__(
        self,
        url: str | None = None,
        database: str | None = None,
        user: str | None = None,
        password: str | None = None,
        enabled: bool | None = None,
        timeout: int = 30,
        strict: bool = False,
    ):
        self.url = (url or os.getenv("CLICKHOUSE_URL") or "http://localhost:8123").rstrip("/")
        self.database = database or os.getenv("CLICKHOUSE_DATABASE") or "stock"
        self.user = user or os.getenv("CLICKHOUSE_USER") or "default"
        self.password = password if password is not None else os.getenv("CLICKHOUSE_PASSWORD", "")
        self.enabled = _env_enabled("CLICKHOUSE_ENABLED", True) if enabled is None else enabled
        self.timeout = timeout
        # strict=True: 写失败直接抛异常（专用回填脚本需要硬失败）。
        # strict=False: 写失败降级为告警并在本进程内停写，避免拖垮 PostgreSQL 主管道。
        self.strict = strict
        self._degraded = False
        self._degraded_lock = threading.Lock()

    @classmethod
    def from_env(cls, strict: bool = False) -> "ClickHouseClient":
        return cls(strict=strict)

    def _params(self, database: str | None = None) -> dict[str, str]:
        params = {"user": self.user}
        if self.password:
            params["password"] = self.password
        if database:
            params["database"] = database
        return params

    def execute(self, query: str, *, database: str | None = None) -> str:
        if not self.enabled:
            return ""
        response = requests.post(
            self.url,
            params=self._params(database=database),
            data=query.encode("utf-8"),
            timeout=self.timeout,
        )
        response.raise_for_status()
        return response.text

    def insert_json_each_row(self, query: str, payload: str, *, database: str | None = None) -> str:
        if not self.enabled:
            return ""
        params = self._params(database=database)
        params["query"] = query
        response = requests.post(
            self.url,
            params=params,
            data=payload.encode("utf-8"),
            timeout=self.timeout,
        )
        response.raise_for_status()
        return response.text

    def ensure_schema(self, ddl_path: Path = CLICKHOUSE_DDL_PATH) -> None:
        if not self.enabled:
            logger.warning("CLICKHOUSE_ENABLED=false，跳过 ClickHouse schema 初始化。")
            return

        sql = ddl_path.read_text(encoding="utf-8")
        for statement in _split_sql(sql):
            upper = statement.upper()
            if upper.startswith("USE "):
                continue
            if "CREATE DICTIONARY" in upper and "{" in statement:
                logger.info("跳过包含占位符的 ClickHouse Dictionary DDL。")
                continue
            database = None if upper.startswith("CREATE DATABASE") else self.database
            self.execute(statement, database=database)
        logger.success("ClickHouse schema 初始化完成。")

    def ping(self) -> bool:
        try:
            return self.execute("SELECT 1", database=self.database).strip() == "1"
        except Exception:
            return False

    def query_scalar(self, query: str) -> str:
        return self.execute(query, database=self.database).strip()

    def _insert_json_each_row(self, table: str, rows: list[dict]) -> int:
        if not self.enabled or not rows or self._degraded:
            return 0
        payload = "\n".join(json.dumps(row, ensure_ascii=False, separators=(",", ":")) for row in rows)
        try:
            self.insert_json_each_row(f"INSERT INTO {table} FORMAT JSONEachRow", payload, database=self.database)
        except Exception as exc:
            if self.strict:
                raise
            with self._degraded_lock:
                first_failure = not self._degraded
                self._degraded = True
            if first_failure:
                logger.warning(
                    "ClickHouse 写入 {} 失败，本进程剩余 ClickHouse 写入将跳过（PostgreSQL 不受影响，"
                    "可稍后用 backfill_clickhouse_daily_bars 补齐）: {}",
                    table,
                    exc,
                )
            return 0
        return len(rows)

    def write_daily_bars(
        self,
        rows: Iterable[dict],
        *,
        source: str,
        vendor_symbol: str | None = None,
        write_canonical: bool = True,
    ) -> int:
        prepared_raw: list[dict] = []
        prepared_canonical: list[dict] = []
        timestamp = _utc_now_string()

        for row in rows:
            security_id = _to_int(row.get("security_id"))
            trade_date = _to_date_string(row.get("date"))
            open_value = _to_decimal_string(row.get("open"))
            high_value = _to_decimal_string(row.get("high"))
            low_value = _to_decimal_string(row.get("low"))
            close_value = _to_decimal_string(row.get("close"))

            if not all([security_id, trade_date, open_value, high_value, low_value, close_value]):
                continue

            volume = _to_int(row.get("volume"))
            vwap = _to_decimal_string(row.get("vwap"))
            trade_count = _to_int(row.get("trade_count"))
            symbol = (row.get("vendor_symbol") or vendor_symbol or "").upper()

            prepared_raw.append(
                {
                    "security_id": security_id,
                    "date": trade_date,
                    "source": source,
                    "vendor_symbol": symbol,
                    "open": open_value,
                    "high": high_value,
                    "low": low_value,
                    "close": close_value,
                    "volume": volume,
                    "vwap": vwap,
                    "trade_count": trade_count,
                    "otc": 1 if row.get("otc") is True else 0,
                    "ingested_at": timestamp,
                    "is_suspect": 0,
                }
            )
            prepared_canonical.append(
                {
                    "security_id": security_id,
                    "date": trade_date,
                    "selected_source": source,
                    "open": open_value,
                    "high": high_value,
                    "low": low_value,
                    "close": close_value,
                    "volume": volume,
                    "vwap": vwap,
                    "built_at": timestamp,
                }
            )

        inserted = self._insert_json_each_row("raw_daily_bars", prepared_raw)
        if write_canonical:
            self._insert_json_each_row("canonical_daily_bars", prepared_canonical)
        return inserted
