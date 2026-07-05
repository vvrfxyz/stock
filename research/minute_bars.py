"""ClickHouse 分钟线读取层（stock.minute_bars，2003+，含盘前盘后，未复权原始价）。

只读研究入口，走 ClickHouse HTTP 接口（无额外依赖）。连接优先
RESEARCH_CLICKHOUSE_URL（指向 253，如 http://192.168.1.253:8123），回退 CLICKHOUSE_URL。

复权与日线同口径：分钟价 × utils/adjusted_prices 的日级因子（分钟粒度不产生
额外复权语义——因子按 ex_date 日级生效）。本模块只出原始价。
"""
from __future__ import annotations

import io
import os
from datetime import date

import pandas as pd
import requests


def clickhouse_url(url: str | None = None) -> str:
    resolved = url or os.environ.get("RESEARCH_CLICKHOUSE_URL") or os.environ.get("CLICKHOUSE_URL")
    if not resolved:
        raise RuntimeError("需要 RESEARCH_CLICKHOUSE_URL 或 CLICKHOUSE_URL")
    return resolved.rstrip("/")


def query_df(sql: str, url: str | None = None) -> pd.DataFrame:
    """任意只读 SQL -> DataFrame（TSVWithNames 编解码）。"""
    response = requests.post(
        clickhouse_url(url),
        params={"default_format": "TabSeparatedWithNames"},
        data=sql.encode(),
        timeout=600,
    )
    if response.status_code != 200:
        raise RuntimeError(f"ClickHouse 查询失败: {response.text[:500]}")
    if not response.content:
        return pd.DataFrame()
    return pd.read_csv(io.BytesIO(response.content), sep="\t")


def load_minute_bars(
    security_ids: list[int],
    start: date,
    end: date,
    *,
    regular_session_only: bool = False,
    url: str | None = None,
) -> pd.DataFrame:
    """按 security_id 拉分钟线。[start, end] 为 ET 交易日闭区间。

    regular_session_only=True 只取 09:30-16:00 ET（含 09:30 开盘分钟，
    不含 16:00 收盘竞价之后）。
    """
    ids = ",".join(str(int(i)) for i in security_ids)
    session_filter = ""
    if regular_session_only:
        # 09:30 起（570 分）到 16:00 前（959 分），ET 口径
        session_filter = """
          AND (toHour(ts, 'America/New_York') * 60 + toMinute(ts, 'America/New_York'))
              BETWEEN 570 AND 959
        """
    sql = f"""
        SELECT security_id, ts, open, high, low, close, volume, vwap, trade_count
        FROM stock.minute_bars FINAL
        WHERE security_id IN ({ids})
          AND toDate(ts, 'America/New_York') >= '{start.isoformat()}'
          AND toDate(ts, 'America/New_York') <= '{end.isoformat()}'
          {session_filter}
        ORDER BY security_id, ts
    """
    frame = query_df(sql, url)
    if not frame.empty:
        frame["ts"] = pd.to_datetime(frame["ts"], utc=True)
    return frame
