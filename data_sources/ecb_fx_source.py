"""ECB 欧元每日参考汇率适配器。

数据源（免费、无 key）：
- https://www.ecb.europa.eu/stats/eurofxref/eurofxref-hist.zip  全历史 CSV（~700KB）

口径：1 EUR = rate 个 quote currency，仅 TARGET 工作日有行情；周末/假日由读取层
按"当日或之前最近发布日"回退（utils/fx_rates.py）。
"""
from __future__ import annotations

import csv
import io
import zipfile
from datetime import date
from decimal import Decimal, InvalidOperation

import requests

_ECB_HIST_URL = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-hist.zip"
_DEFAULT_TIMEOUT = 60


def fetch_ecb_fx_history(since: date | None = None, session: requests.Session | None = None) -> list[dict]:
    """下载并解析 ECB 全历史参考汇率，返回 fx_rates 行。

    - since: 只保留 rate_date >= since 的行（周度增量用，全量回填传 None）。
    - 'N/A'（停发币种如 HRK/RUB）跳过。
    """
    http = session or requests
    response = http.get(
        _ECB_HIST_URL,
        timeout=_DEFAULT_TIMEOUT,
        headers={"User-Agent": "stock-pipeline fx sync"},
    )
    response.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        csv_name = next(name for name in archive.namelist() if name.endswith(".csv"))
        text = archive.read(csv_name).decode("utf-8")
    return parse_ecb_fx_csv(text, since=since)


def parse_ecb_fx_csv(csv_text: str, since: date | None = None) -> list[dict]:
    rows = []
    reader = csv.DictReader(io.StringIO(csv_text))
    for record in reader:
        date_text = (record.get("Date") or "").strip()
        if not date_text:
            continue
        try:
            rate_date = date.fromisoformat(date_text)
        except ValueError:
            continue
        if since and rate_date < since:
            continue
        for currency, value in record.items():
            if currency is None or currency == "Date":
                continue
            currency = currency.strip()
            value = (value or "").strip()
            if not currency or not value or value.upper() == "N/A":
                continue
            try:
                rate = Decimal(value)
            except InvalidOperation:
                continue
            rows.append(
                {
                    "rate_date": rate_date,
                    "base_currency": "EUR",
                    "quote_currency": currency,
                    "source": "ECB",
                    "rate": rate,
                }
            )
    return rows
