"""sync_massive_universe 的 _classify_incoming 单元测试。

不依赖 PostgreSQL——直接构造 resolver 和 incoming rows。
"""
from scripts.sync_massive_universe import _classify_incoming
from utils.security_identity import SecurityIdentityResolver, _SecurityRow


def _row(id, symbol, *, figi=None, cik=None, exchange=None, is_active=True):
    return _SecurityRow(id=id, symbol=symbol, current_symbol=symbol,
                        composite_figi=figi, share_class_figi=None, cik=cik,
                        exchange=exchange, is_active=is_active)


def _build_resolver(rows, history=None):
    by_figi = {}
    by_cik = {}
    by_symbol = {}
    by_symbol_all = {}
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
    return SecurityIdentityResolver._from_indexes(by_figi, by_cik, by_symbol, by_symbol_all, history or {})


def test_classify_detects_rename():
    resolver = _build_resolver([_row(1, "fb", figi="BBG000MM2P62")])
    incoming = [{"symbol": "meta", "composite_figi": "BBG000MM2P62"}]
    rename, recycle, normal, _ = _classify_incoming(resolver, incoming)
    assert len(rename) == 1
    assert rename[0][1].security_id == 1
    assert rename[0][1].is_rename is True
    assert len(recycle) == 0
    assert len(normal) == 0


def test_classify_detects_recycle():
    resolver = _build_resolver([_row(1, "abcd", figi="BBG000OLD")])
    incoming = [{"symbol": "abcd", "composite_figi": "BBG000NEW"}]
    rename, recycle, normal, _ = _classify_incoming(resolver, incoming)
    assert len(recycle) == 1
    assert recycle[0][1].is_recycle is True
    assert len(rename) == 0
    assert len(normal) == 0


def test_classify_normal_match():
    resolver = _build_resolver([_row(1, "aapl", figi="BBG000B9XRY4")])
    incoming = [{"symbol": "aapl", "composite_figi": "BBG000B9XRY4"}]
    rename, recycle, normal, _ = _classify_incoming(resolver, incoming)
    assert len(rename) == 0
    assert len(recycle) == 0
    assert len(normal) == 1


def test_classify_new_listing():
    resolver = _build_resolver([_row(1, "aapl", figi="BBG000B9XRY4")])
    incoming = [{"symbol": "newco", "composite_figi": "BBG000BRAND_NEW"}]
    rename, recycle, normal, _ = _classify_incoming(resolver, incoming)
    assert len(rename) == 0
    assert len(recycle) == 0
    assert len(normal) == 1  # new listings go to normal for insert


def test_classify_mixed_batch():
    resolver = _build_resolver([
        _row(1, "fb", figi="BBG000MM2P62"),
        _row(2, "aapl", figi="BBG000B9XRY4"),
        _row(3, "abcd", figi="BBG000OLD"),
    ])
    incoming = [
        {"symbol": "meta", "composite_figi": "BBG000MM2P62"},   # rename
        {"symbol": "aapl", "composite_figi": "BBG000B9XRY4"},   # normal
        {"symbol": "abcd", "composite_figi": "BBG000NEW"},      # recycle
        {"symbol": "newco"},                                      # new
    ]
    rename, recycle, normal, results = _classify_incoming(resolver, incoming)
    assert len(rename) == 1
    assert len(recycle) == 1
    assert len(normal) == 2
    assert len(results) == 4
