"""SecurityIdentityResolver 单元测试。

不依赖 PostgreSQL——直接构造 resolver 的内部索引，验证解析逻辑。
"""
from datetime import date

import pytest

from utils.security_identity import (
    ResolutionResult,
    SecurityIdentityResolver,
    _SecurityRow,
)


def _row(
    id: int,
    symbol: str,
    *,
    figi: str | None = None,
    cik: str | None = None,
    exchange: str | None = None,
    is_active: bool = True,
    current_symbol: str | None = None,
) -> _SecurityRow:
    return _SecurityRow(
        id=id,
        symbol=symbol,
        current_symbol=current_symbol or symbol,
        composite_figi=figi,
        share_class_figi=None,
        cik=cik,
        exchange=exchange,
        is_active=is_active,
    )


def _build(rows: list[_SecurityRow], history: dict | None = None) -> SecurityIdentityResolver:
    """从 _SecurityRow 列表构造一个 resolver，索引规则与 _load 保持一致。"""
    by_figi: dict = {}
    by_cik: dict = {}
    by_symbol: dict = {}
    by_symbol_all: dict = {}
    for row in rows:
        if row.composite_figi:
            existing = by_figi.get(row.composite_figi.upper())
            if existing is None or (not existing.is_active and row.is_active):
                by_figi[row.composite_figi.upper()] = row
        if row.cik:
            by_cik.setdefault(row.cik, []).append(row)
        sym = row.symbol.lower()
        by_symbol_all.setdefault(sym, []).append(row)
        if row.is_active:
            by_symbol[sym] = row
    return SecurityIdentityResolver._from_indexes(
        by_figi, by_cik, by_symbol, by_symbol_all, history or {}
    )


def test_figi_exact_match():
    r = _build([_row(1, "meta", figi="BBG000MM2P62", cik="1326801")])
    result = r.resolve(symbol="meta", composite_figi="BBG000MM2P62")
    assert result.security_id == 1
    assert result.resolution_type == "FIGI"
    assert result.confidence == "HIGH"
    assert result.matched_field == "composite_figi"
    assert result.is_rename is False
    assert result.is_recycle is False


def test_figi_match_with_rename():
    # 同一 FIGI（FB 已改名 META），symbol 不同 -> rename。
    r = _build([_row(1, "fb", figi="BBG000MM2P62")])
    result = r.resolve(symbol="META", composite_figi="bbg000mm2p62")
    assert result.security_id == 1
    assert result.resolution_type == "FIGI"
    assert result.is_rename is True
    assert result.is_recycle is False


def test_cik_unique_match():
    r = _build([_row(7, "aapl", cik="320193", exchange="XNAS")])
    result = r.resolve(symbol="aapl", cik="320193")
    assert result.security_id == 7
    assert result.resolution_type == "CIK"
    assert result.confidence == "HIGH"


def test_cik_ambiguous_falls_through_to_symbol():
    # 同一 CIK 两个 share class，无交易所消歧 -> 落到 symbol 匹配。
    rows = [
        _row(10, "googl", cik="1652044", exchange="XNAS"),
        _row(11, "goog", cik="1652044", exchange="XNAS"),
    ]
    r = _build(rows)
    result = r.resolve(symbol="goog", cik="1652044")
    assert result.security_id == 11
    assert result.resolution_type == "ACTIVE_SYMBOL"
    assert result.confidence == "HIGH"


def test_cik_ambiguous_disambiguated_by_exchange():
    rows = [
        _row(20, "shel.l", cik="1306965", exchange="XLON"),
        _row(21, "shel", cik="1306965", exchange="XNYS"),
    ]
    r = _build(rows)
    result = r.resolve(symbol="brandnew", cik="1306965", exchange="XNYS")
    assert result.security_id == 21
    assert result.resolution_type == "CIK"
    assert result.confidence == "MEDIUM"
    assert result.is_rename is True


def test_active_symbol_match():
    r = _build([_row(3, "tsla", figi="BBG000N9MNX3")])
    result = r.resolve(symbol="TSLA")
    assert result.security_id == 3
    assert result.resolution_type == "ACTIVE_SYMBOL"
    assert result.confidence == "HIGH"
    assert result.is_recycle is False


def test_symbol_match_with_identity_conflict_recycle():
    # 既有 symbol 的 FIGI 与 incoming FIGI 不同 -> symbol 被回收复用。
    r = _build([_row(5, "abcd", figi="BBG000OLDOLD")])
    result = r.resolve(symbol="abcd", composite_figi="BBG000NEWNEW")
    assert result.security_id == 5
    assert result.resolution_type == "ACTIVE_SYMBOL"
    assert result.confidence == "LOW"
    assert result.is_recycle is True


def test_history_symbol_match():
    sec = _row(9, "newco", figi="BBG000HISTORY")
    history = {"oldco": [(sec, date(2020, 1, 1), date(2022, 6, 30))]}
    # newco 当前活跃，oldco 只在历史里。
    r = _build([sec], history=history)
    result = r.resolve(symbol="oldco")
    assert result.security_id == 9
    assert result.resolution_type == "HISTORY_SYMBOL"
    assert result.confidence == "MEDIUM"


def test_no_match_new_listing():
    r = _build([_row(1, "meta", figi="BBG000MM2P62")])
    result = r.resolve(symbol="ipoco", composite_figi="BBG000BRANDNEW", cik="9999999")
    assert result.resolution_type == "NEW"
    assert result.confidence == "HIGH"
    assert result.security_id == -1


def test_resolve_batch():
    r = _build([_row(1, "meta", figi="BBG000MM2P62"), _row(2, "aapl", cik="320193")])
    rows = [
        {"symbol": "meta", "composite_figi": "BBG000MM2P62"},
        {"symbol": "aapl", "cik": "320193"},
        {"symbol": "ipoco"},
    ]
    results = r.resolve_batch(rows)
    assert len(results) == 3
    assert all(isinstance(x, ResolutionResult) for x in results)
    assert results[0].resolution_type == "FIGI"
    assert results[1].resolution_type == "CIK"
    assert results[2].resolution_type == "NEW"


def test_dry_run_report():
    r = _build(
        [
            _row(1, "fb", figi="BBG000MM2P62"),   # 将被 rename 命中
            _row(2, "abcd", figi="BBG000OLDOLD"),  # 将被 recycle 命中
            _row(3, "aapl", cik="320193"),
        ]
    )
    rows = [
        {"symbol": "meta", "composite_figi": "BBG000MM2P62"},          # rename
        {"symbol": "abcd", "composite_figi": "BBG000NEWNEW"},          # recycle
        {"symbol": "aapl", "cik": "320193"},                          # clean CIK
        {"symbol": "ipoco", "composite_figi": "BBG000BRANDNEW"},       # new
    ]
    report = r.dry_run_report(rows)
    assert report["total"] == 4
    assert report["by_type"]["FIGI"] == 1
    assert report["by_type"]["ACTIVE_SYMBOL"] == 1
    assert report["by_type"]["CIK"] == 1
    assert report["by_type"]["NEW"] == 1
    assert len(report["renames"]) == 1
    assert report["renames"][0][0] == "meta"
    assert report["renames"][0][1] == 1
    assert report["renames"][0][2] == "fb"
    assert len(report["recycles"]) == 1
    assert report["recycles"][0][0] == "abcd"
    assert report["new_listings"] == ["ipoco"]
    # recycle 已单列，不重复进 ambiguous。
    assert report["ambiguous"] == []
