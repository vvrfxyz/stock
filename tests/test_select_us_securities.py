"""utils.massive_task.select_us_securities 的查询分支测试（sqlite 内存库即可）。"""
from contextlib import contextmanager
from datetime import date, datetime
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from data_models.models import Security
from utils.massive_task import select_us_securities

import scripts.update_massive_prices as prices


class FakeDbManager:
    def __init__(self):
        self.engine = create_engine("sqlite:///:memory:")
        # 只建 securities：全 metadata 里有 sqlite 不支持的 ARRAY 列
        Security.__table__.create(self.engine)
        self._factory = sessionmaker(bind=self.engine)

    @contextmanager
    def get_session(self):
        session = self._factory()
        try:
            yield session
        finally:
            session.close()


def _args(symbols=(), market="US", limit=0):
    return SimpleNamespace(symbols=list(symbols), market=market, limit=limit)


@pytest.fixture()
def db():
    manager = FakeDbManager()
    rows = [
        # id, symbol, market, type, active, info_ts
        (1, "aapl", "US", "CS", True, datetime(2020, 1, 1)),
        (2, "msft", "US", "CS", True, None),
        (3, "spy", "US", "ETF", True, datetime(2026, 6, 11)),
        (4, "warrants", "US", "WARRANT", True, None),   # 非保留类型
        (5, "dead", "US", "CS", False, None),            # 已退市
    ]
    with manager.get_session() as session:
        for id_, symbol, market, type_, active, ts in rows:
            session.add(Security(
                id=id_, symbol=symbol, current_symbol=symbol, market=market,
                type=type_, is_active=active, full_refresh_interval=30,
                info_last_updated_at=ts,
            ))
        session.commit()
        # 构造函数里的 None 会被 server_default=now() 顶掉，必须显式置回 NULL
        null_ids = [id_ for id_, *_rest, ts in rows if ts is None]
        session.query(Security).filter(Security.id.in_(null_ids)).update(
            {"info_last_updated_at": None}, synchronize_session=False
        )
        session.commit()
    return manager


def _symbols(result):
    return [s.symbol for s in result]


def test_default_scope_filters_type_and_active(db):
    result = select_us_securities(db, _args())
    assert _symbols(result) == ["aapl", "msft", "spy"]  # 无排序列时按 symbol


def test_unless_symbols_scope_allows_explicit_offlist_symbols(db):
    # 显式指名时放开 type/active 过滤：warrant 和退市证券都可以被指名操作
    result = select_us_securities(
        db, _args(symbols=["warrants", "dead"]),
        type_scope="unless_symbols", active_scope="unless_symbols",
    )
    assert _symbols(result) == ["dead", "warrants"]


def test_always_scope_excludes_offlist_even_when_named(db):
    result = select_us_securities(db, _args(symbols=["warrants", "aapl"]))
    assert _symbols(result) == ["aapl"]


def test_staleness_filter_selects_null_and_stale_only(db):
    result = select_us_securities(
        db, _args(),
        staleness_column="info_last_updated_at", staleness_days=30,
    )
    # msft(NULL) 排最前，aapl(2020) 过期；spy(昨天) 不选
    assert _symbols(result) == ["msft", "aapl"]


def test_skip_staleness_returns_all(db):
    result = select_us_securities(
        db, _args(),
        staleness_column="info_last_updated_at", staleness_days=30, skip_staleness=True,
    )
    assert _symbols(result) == ["msft", "aapl", "spy"]  # 按 staleness 列排序, NULL 最前


def test_extra_filter_is_applied(db):
    result = select_us_securities(
        db, _args(),
        extra_filter=lambda q: q.filter(Security.type == "ETF"),
    )
    assert _symbols(result) == ["spy"]


def test_limit_caps_results(db):
    result = select_us_securities(db, _args(limit=1))
    assert len(result) == 1


def test_symbols_filter_is_case_insensitive(db):
    result = select_us_securities(db, _args(symbols=["AAPL"]))
    assert _symbols(result) == ["aapl"]


def test_non_us_market_rejected(db):
    with pytest.raises(ValueError):
        select_us_securities(db, _args(market="HK"))


def test_order_column_override(db):
    with db.get_session() as session:
        session.query(Security).filter(Security.id == 1).update(
            {"price_data_latest_date": datetime(2026, 6, 1).date()}
        )
        session.commit()
    result = select_us_securities(db, _args(), order_column="price_data_latest_date")
    # NULL 在前（msft/spy 按 symbol），有值的 aapl 最后
    assert _symbols(result)[-1] == "aapl"


# ---------------------------------------------------------------------------
# update_massive_prices 的选择语义：--include-inactive -> active_scope 映射
#（2025-08-01 截断队列修复入口：显式指名时才放开 is_active 过滤）
# ---------------------------------------------------------------------------

PRICES_END_DATE = date(2026, 7, 3)


def _prices_args(symbols=(), include_inactive=False, full_refresh=False, market="US", limit=0):
    return SimpleNamespace(
        symbols=list(symbols),
        include_inactive=include_inactive,
        full_refresh=full_refresh,
        market=market,
        limit=limit,
    )


def test_prices_default_excludes_inactive_even_when_named(db):
    # 不带 flag 时行为与历史完全一致：退市证券即便被指名也不选。
    result = prices.get_securities_to_update(
        db, _prices_args(symbols=["dead", "aapl"]), PRICES_END_DATE
    )
    assert _symbols(result) == ["aapl"]


def test_prices_include_inactive_lifts_active_filter_for_explicit_symbols(db):
    result = prices.get_securities_to_update(
        db, _prices_args(symbols=["dead", "aapl"], include_inactive=True), PRICES_END_DATE
    )
    assert _symbols(result) == ["aapl", "dead"]


def test_prices_include_inactive_without_symbols_keeps_active_filter(db):
    # run() 会在更早处拒绝无 symbols 的 --include-inactive；这里锁定选择层兜底：
    # unless_symbols 语义下无 symbols 时 is_active 过滤依然生效，不会全库扫退市。
    result = prices.get_securities_to_update(
        db, _prices_args(include_inactive=True), PRICES_END_DATE
    )
    assert _symbols(result) == ["aapl", "msft", "spy"]


def test_prices_include_inactive_keeps_type_filter(db):
    # 只放开 is_active，type 过滤保持 always：warrant 类仍被排除。
    result = prices.get_securities_to_update(
        db, _prices_args(symbols=["warrants", "dead"], include_inactive=True), PRICES_END_DATE
    )
    assert _symbols(result) == ["dead"]
