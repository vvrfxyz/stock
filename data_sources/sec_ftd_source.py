"""SEC fails-to-deliver (FTD) 数据适配器。

用途不是做空数据（short_interests 已有 Massive 来源），而是把 FTD 文件里的
CUSIP|SYMBOL 对照抽出来，作为 CUSIP -> security_id 身份映射的免费官方来源
（13F holdings 回填 security_id 依赖它）。

数据源（免费、硬性要求自报含邮箱的 User-Agent，否则 403）：
- https://www.sec.gov/files/data/fails-deliver-data/cnsfails{YYYYMM}{a|b}.zip
  每月两个半月文件，约 T+1 月发布；单文件 ~5 万行、~12k 唯一 CUSIP/symbol 对。
"""
from __future__ import annotations

import io
import zipfile
from datetime import date

import requests

from utils.sec_config import get_sec_user_agent

_FTD_URL = "https://www.sec.gov/files/data/fails-deliver-data/cnsfails{yyyymm}{half}.zip"
_DEFAULT_TIMEOUT = 60


def ftd_periods(months_back: int, today: date | None = None) -> list[tuple[str, str]]:
    """返回最近 months_back 个月的 (yyyymm, half) 列表，新月在前。
    发布有 ~1 个月滞后，从上个月起往回数。"""
    today = today or date.today()
    periods = []
    year, month = today.year, today.month
    for _ in range(months_back):
        month -= 1
        if month == 0:
            year, month = year - 1, 12
        yyyymm = f"{year}{month:02d}"
        periods.extend([(yyyymm, "b"), (yyyymm, "a")])
    return periods


def fetch_ftd_cusip_symbol_pairs(
    yyyymm: str,
    half: str,
    session: requests.Session | None = None,
    user_agent: str | None = None,
) -> set[tuple[str, str]] | None:
    """下载单个 FTD 半月文件，返回 {(cusip, symbol_lower), ...}；未发布(404)返回 None。

    403 不吞——正确的自报 UA 下未发布只会 404，403 意味着 UA 配置问题，必须暴露。"""
    http = session or requests
    response = http.get(
        _FTD_URL.format(yyyymm=yyyymm, half=half),
        timeout=_DEFAULT_TIMEOUT,
        headers={"User-Agent": user_agent or get_sec_user_agent()},
    )
    if response.status_code == 404:
        return None
    response.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        name = next(n for n in archive.namelist() if n.lower().endswith(".txt"))
        text = archive.read(name).decode("latin-1")
    return parse_ftd_pairs(text)


def parse_ftd_pairs(text: str) -> set[tuple[str, str]]:
    """FTD 管道分隔文本 -> {(cusip, symbol_lower)}。symbol 保持 FTD 原样
    （无点号大写，如 BRKB），由调用方做与库内 symbol 的规范化匹配。"""
    pairs = set()
    for line in text.splitlines():
        parts = line.split("|")
        if len(parts) < 3:
            continue
        cusip = parts[1].strip().upper()
        symbol = parts[2].strip().lower()
        # 表头行与无 symbol 的行跳过；CUSIP 固定 9 位
        if len(cusip) != 9 or not symbol or cusip == "CUSIP":
            continue
        pairs.add((cusip, symbol))
    return pairs
