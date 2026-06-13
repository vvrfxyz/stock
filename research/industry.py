"""研究层行业映射：SIC -> Fama-French 12 行业。"""
from __future__ import annotations

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

_FF12_BUCKETS: tuple[str, ...] = (
    "NoDur",
    "Durbl",
    "Manuf",
    "Enrgy",
    "Chems",
    "BusEq",
    "Telcm",
    "Utils",
    "Shops",
    "Hlth",
    "Money",
    "Other",
)

_FF12_RANGES: tuple[tuple[int, int, str], ...] = (
    (100, 999, "NoDur"),
    (2000, 2399, "NoDur"),
    (2700, 2749, "NoDur"),
    (2770, 2799, "NoDur"),
    (3100, 3199, "NoDur"),
    (3940, 3989, "NoDur"),
    (2500, 2519, "Durbl"),
    (2590, 2599, "Durbl"),
    (3630, 3659, "Durbl"),
    (3710, 3711, "Durbl"),
    (3714, 3714, "Durbl"),
    (3716, 3716, "Durbl"),
    (3750, 3751, "Durbl"),
    (3792, 3792, "Durbl"),
    (3900, 3939, "Durbl"),
    (3990, 3999, "Durbl"),
    (2520, 2589, "Manuf"),
    (2600, 2699, "Manuf"),
    (2750, 2769, "Manuf"),
    (3000, 3099, "Manuf"),
    (3200, 3569, "Manuf"),
    (3580, 3629, "Manuf"),
    (3700, 3709, "Manuf"),
    (3712, 3713, "Manuf"),
    (3715, 3715, "Manuf"),
    (3717, 3749, "Manuf"),
    (3752, 3791, "Manuf"),
    (3793, 3799, "Manuf"),
    (3830, 3839, "Manuf"),
    (3860, 3899, "Manuf"),
    (1200, 1399, "Enrgy"),
    (2900, 2999, "Enrgy"),
    (2800, 2829, "Chems"),
    (2840, 2899, "Chems"),
    (3570, 3579, "BusEq"),
    (3660, 3692, "BusEq"),
    (3694, 3699, "BusEq"),
    (3810, 3829, "BusEq"),
    (7370, 7379, "BusEq"),
    (4800, 4899, "Telcm"),
    (4900, 4949, "Utils"),
    (5000, 5999, "Shops"),
    (7200, 7299, "Shops"),
    (7600, 7699, "Shops"),
    (2830, 2839, "Hlth"),
    (3693, 3693, "Hlth"),
    (3840, 3859, "Hlth"),
    (8000, 8099, "Hlth"),
    (6000, 6999, "Money"),
)

SIC_TO_FF12 = _FF12_RANGES

_NO_SIC_VALUES = {"", "N/A", "NA", "NONE", "NULL"}


def _is_missing_sic(sic_code: int | str | None) -> bool:
    return sic_code is None or (
        isinstance(sic_code, str) and sic_code.strip().upper() in _NO_SIC_VALUES
    )


def _parse_sic(sic_code: int | str | None) -> int | None:
    if _is_missing_sic(sic_code):
        return None
    if isinstance(sic_code, int):
        value = sic_code
    elif isinstance(sic_code, str):
        try:
            value = int(sic_code.strip())
        except ValueError:
            return None
    else:
        return None
    if value < 100 or value > 9999:
        return None
    return value


def sic_to_ff12(sic_code: int | str | None) -> str | None:
    """SIC 代码映射到 FF12 bucket；脏输入返回 None。"""
    value = _parse_sic(sic_code)
    if value is None:
        return None
    for start, end, ff12 in _FF12_RANGES:
        if start <= value <= end:
            return ff12
    return "Other"


def load_industry_panel(
    engine: Engine,
    *,
    security_ids: list[int] | None = None,
) -> pd.DataFrame:
    """从 securities 读取当前 SIC 并映射 FF12 长表。"""
    id_clause = "where id = any(:security_ids)" if security_ids else ""
    sql = text(
        f"""
        select id as security_id, sic_code, type, is_active
        from securities
        {id_clause}
        order by id
        """
    )
    params = {"security_ids": security_ids} if security_ids else None
    panel = pd.read_sql_query(sql, engine, params=params)
    panel["ff12"] = [sic_to_ff12(value) for value in panel["sic_code"]]
    panel["ff12_coverage_reason"] = [
        "no_sic"
        if _is_missing_sic(sic_code)
        else "mapped"
        if ff12 is not None
        else "unmapped_sic"
        for sic_code, ff12 in zip(panel["sic_code"], panel["ff12"], strict=True)
    ]
    return panel[["security_id", "sic_code", "ff12", "ff12_coverage_reason"]]


def coverage_report(panel: pd.DataFrame) -> dict:
    """汇总 FF12 覆盖率与分 bucket 样本数。"""
    total = int(len(panel))
    mapped = int((panel["ff12_coverage_reason"] == "mapped").sum())
    no_sic = int((panel["ff12_coverage_reason"] == "no_sic").sum())
    unmapped_sic = int((panel["ff12_coverage_reason"] == "unmapped_sic").sum())
    counts = panel["ff12"].dropna().value_counts().to_dict()
    return {
        "total_securities": total,
        "mapped": mapped,
        "mapped_pct": mapped / total if total else 0.0,
        "no_sic": no_sic,
        "unmapped_sic": unmapped_sic,
        "by_ff12": {bucket: int(counts.get(bucket, 0)) for bucket in _FF12_BUCKETS},
    }
