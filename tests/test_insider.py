"""Tests for insider net buy panel and builtin factor."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
from unittest.mock import MagicMock

import research.factors.protocol as _proto
from research.factors.protocol import FactorContext, get
from research.insider import load_insider_events, load_insider_net_buy_panel


# ---------------------------------------------------------------------------
# helpers / fixtures
# ---------------------------------------------------------------------------

def _insider_events(*rows) -> pd.DataFrame:
    """Build a tiny insider events DataFrame matching load_insider_events schema."""
    if not rows:
        return pd.DataFrame(
            {
                "security_id": pd.Series(dtype=np.int64),
                "visible_date": pd.Series(dtype="datetime64[ns]"),
                "transaction_date": pd.Series(dtype="datetime64[ns]"),
                "signed_shares": pd.Series(dtype=np.float64),
            }
        )
    df = pd.DataFrame(rows, columns=["security_id", "visible_date", "transaction_date", "signed_shares"])
    for col in ("visible_date", "transaction_date"):
        df[col] = pd.to_datetime(df[col]).astype("datetime64[ns]")
    df["security_id"] = df["security_id"].astype(np.int64)
    df["signed_shares"] = df["signed_shares"].astype(np.float64)
    return df


@pytest.fixture()
def _isolate_registry():
    """Save and restore the factor registry around each test."""
    saved = dict(_proto._REGISTRY)
    yield
    _proto._REGISTRY.clear()
    _proto._REGISTRY.update(saved)


def _patch_events(monkeypatch, events: pd.DataFrame):
    """Monkeypatch load_insider_events to return *events* regardless of args."""
    monkeypatch.setattr(
        "research.insider.load_insider_events",
        lambda engine, *, security_ids=None: events,
    )


# ---------------------------------------------------------------------------
# panel tests
# ---------------------------------------------------------------------------

def test_net_buy_basic(monkeypatch):
    """Buy 100 on day 1, sell 50 on day 10. At day 15: net = +50. Two securities."""
    events = _insider_events(
        (1, "2025-01-01", "2025-01-01", 100.0),
        (1, "2025-01-10", "2025-01-10", -50.0),
        (2, "2025-01-05", "2025-01-05", 200.0),
    )
    _patch_events(monkeypatch, events)

    panel = load_insider_net_buy_panel(
        MagicMock(),
        dates=pd.to_datetime(["2025-01-15"]),
        security_ids=[1, 2],
        visible_delay_days=1,
        window_days=90,
    )
    assert panel.shape == (1, 2)
    assert panel.loc[pd.Timestamp("2025-01-15"), 1] == pytest.approx(50.0)
    assert panel.loc[pd.Timestamp("2025-01-15"), 2] == pytest.approx(200.0)


def test_visible_delay(monkeypatch):
    """Event visible_date=Jan 15, delay=1. On Jan 15: NaN. On Jan 16: see the event."""
    events = _insider_events(
        (1, "2025-01-15", "2025-01-15", 100.0),
    )
    _patch_events(monkeypatch, events)

    panel = load_insider_net_buy_panel(
        MagicMock(),
        dates=pd.to_datetime(["2025-01-15", "2025-01-16"]),
        security_ids=[1],
        visible_delay_days=1,
        window_days=90,
    )
    # effective_visible_date = Jan 16; on Jan 15 the event is not yet visible
    assert pd.isna(panel.loc[pd.Timestamp("2025-01-15"), 1])
    assert panel.loc[pd.Timestamp("2025-01-16"), 1] == pytest.approx(100.0)


def test_window_expiry(monkeypatch):
    """Buy 100 on day 1, window=30 days. At day 31: still 100. At day 32: drops to 0.

    effective_visible_date = 2025-01-02 (delay=1).
    date=2025-01-31: upper <= Jan31 => 100; lower <= (Jan31-30d=Jan01) => NaN(0) => net=100.
    date=2025-02-01: upper <= Feb01 => 100; lower <= (Feb01-30d=Jan02) => 100 => net=0.
    """
    events = _insider_events(
        (1, "2025-01-01", "2025-01-01", 100.0),
    )
    _patch_events(monkeypatch, events)

    panel = load_insider_net_buy_panel(
        MagicMock(),
        dates=pd.to_datetime(["2025-01-31", "2025-02-01"]),
        security_ids=[1],
        visible_delay_days=1,
        window_days=30,
    )
    assert panel.loc[pd.Timestamp("2025-01-31"), 1] == pytest.approx(100.0)
    assert panel.loc[pd.Timestamp("2025-02-01"), 1] == pytest.approx(0.0)


def test_accumulation(monkeypatch):
    """Multiple buys within window sum up correctly."""
    events = _insider_events(
        (1, "2025-01-01", "2025-01-01", 100.0),
        (1, "2025-01-10", "2025-01-10", 50.0),
        (1, "2025-01-20", "2025-01-20", 75.0),
    )
    _patch_events(monkeypatch, events)

    panel = load_insider_net_buy_panel(
        MagicMock(),
        dates=pd.to_datetime(["2025-01-25"]),
        security_ids=[1],
        visible_delay_days=1,
        window_days=90,
    )
    assert panel.loc[pd.Timestamp("2025-01-25"), 1] == pytest.approx(225.0)


def test_buy_and_sell_cancel(monkeypatch):
    """Buy 100 + sell 100 within window -> net = 0."""
    events = _insider_events(
        (1, "2025-01-01", "2025-01-01", 100.0),
        (1, "2025-01-10", "2025-01-10", -100.0),
    )
    _patch_events(monkeypatch, events)

    panel = load_insider_net_buy_panel(
        MagicMock(),
        dates=pd.to_datetime(["2025-01-15"]),
        security_ids=[1],
        visible_delay_days=1,
        window_days=90,
    )
    assert panel.loc[pd.Timestamp("2025-01-15"), 1] == pytest.approx(0.0)


def test_no_events_for_security_is_nan(monkeypatch):
    """Security in universe but no insider events -> NaN, not 0."""
    events = _insider_events(
        (1, "2025-01-01", "2025-01-01", 100.0),
    )
    _patch_events(monkeypatch, events)

    panel = load_insider_net_buy_panel(
        MagicMock(),
        dates=pd.to_datetime(["2025-01-15"]),
        security_ids=[1, 999],
        visible_delay_days=1,
        window_days=90,
    )
    assert panel.shape == (1, 2)
    assert panel.loc[pd.Timestamp("2025-01-15"), 1] == pytest.approx(100.0)
    assert pd.isna(panel.loc[pd.Timestamp("2025-01-15"), 999])


def test_empty_events(monkeypatch):
    """No events at all -> all NaN."""
    _patch_events(monkeypatch, _insider_events())

    panel = load_insider_net_buy_panel(
        MagicMock(),
        dates=pd.to_datetime(["2025-01-15"]),
        security_ids=[1, 2],
        visible_delay_days=1,
        window_days=90,
    )
    assert panel.shape == (1, 2)
    assert pd.isna(panel.loc[pd.Timestamp("2025-01-15"), 1])
    assert pd.isna(panel.loc[pd.Timestamp("2025-01-15"), 2])


def test_empty_dates(monkeypatch):
    """dates=[] -> empty index."""
    events = _insider_events(
        (1, "2025-01-01", "2025-01-01", 100.0),
    )
    _patch_events(monkeypatch, events)

    panel = load_insider_net_buy_panel(
        MagicMock(),
        dates=pd.to_datetime([]),
        security_ids=[1],
        visible_delay_days=1,
        window_days=90,
    )
    assert len(panel) == 0
    assert 1 in panel.columns


def test_empty_security_ids(monkeypatch):
    """security_ids=[] -> empty columns."""
    _patch_events(monkeypatch, _insider_events())

    panel = load_insider_net_buy_panel(
        MagicMock(),
        dates=pd.to_datetime(["2025-01-15"]),
        security_ids=[],
        visible_delay_days=1,
        window_days=90,
    )
    assert panel.shape == (1, 0)


def test_security_ids_filter(monkeypatch):
    """Test None vs explicit list vs empty list for security_ids."""
    events_all = _insider_events(
        (1, "2025-01-01", "2025-01-01", 100.0),
        (2, "2025-01-01", "2025-01-01", 200.0),
        (3, "2025-01-01", "2025-01-01", 300.0),
    )

    calls: list = []

    def mock_load(engine, *, security_ids=None):
        calls.append(security_ids)
        if security_ids is None:
            return events_all
        return events_all[events_all["security_id"].isin(security_ids)].copy()

    monkeypatch.setattr("research.insider.load_insider_events", mock_load)

    dates = pd.to_datetime(["2025-01-15"])

    # None -> discover universe from events
    panel = load_insider_net_buy_panel(MagicMock(), dates=dates, security_ids=None)
    assert panel.shape[1] == 3
    assert calls[-1] is None

    # [2, 999]
    panel = load_insider_net_buy_panel(MagicMock(), dates=dates, security_ids=[2, 999])
    assert panel.shape[1] == 2
    assert set(calls[-1]) == {2, 999}
    assert panel.loc[pd.Timestamp("2025-01-15"), 2] == pytest.approx(200.0)
    assert pd.isna(panel.loc[pd.Timestamp("2025-01-15"), 999])

    # []
    panel = load_insider_net_buy_panel(MagicMock(), dates=dates, security_ids=[])
    assert panel.shape[1] == 0


def test_dedupes_security_ids(monkeypatch):
    """[2,2,999] -> deduplicated to [2,999]."""
    events = _insider_events(
        (2, "2025-01-01", "2025-01-01", 200.0),
    )

    calls: list = []

    def mock_load(engine, *, security_ids=None):
        calls.append(security_ids)
        return events

    monkeypatch.setattr("research.insider.load_insider_events", mock_load)

    panel = load_insider_net_buy_panel(
        MagicMock(),
        dates=pd.to_datetime(["2025-01-15"]),
        security_ids=[2, 2, 999],
    )
    assert panel.shape[1] == 2
    # load_insider_events should receive deduplicated list
    passed_ids = calls[-1]
    assert sorted(passed_ids) == [2, 999]


def test_multiple_events_same_filing_date(monkeypatch):
    """CEO and CFO both file on same day; their shares should accumulate."""
    events = _insider_events(
        (1, "2025-01-10", "2025-01-10", 100.0),  # CEO
        (1, "2025-01-10", "2025-01-10", 50.0),   # CFO
    )
    _patch_events(monkeypatch, events)

    panel = load_insider_net_buy_panel(
        MagicMock(),
        dates=pd.to_datetime(["2025-01-15"]),
        security_ids=[1],
        visible_delay_days=1,
        window_days=90,
    )
    assert panel.loc[pd.Timestamp("2025-01-15"), 1] == pytest.approx(150.0)


# ---------------------------------------------------------------------------
# builtin factor tests
# ---------------------------------------------------------------------------

def test_builtin_factor_shape(monkeypatch, _isolate_registry):
    """InsiderNetBuyFactor.compute returns the correct shape via FactorContext."""
    import research.factors.builtins.insider_net_buy as _mod

    dates = pd.DatetimeIndex(pd.to_datetime(["2025-01-15", "2025-01-16"]))
    universe = pd.Index([1, 2], dtype=np.int64)

    mock_panel = pd.DataFrame(
        [[10.0, 20.0], [30.0, 40.0]],
        index=dates,
        columns=universe,
        dtype=np.float64,
    )

    def fake_loader(engine, *, dates, security_ids):
        return mock_panel

    monkeypatch.setattr(_mod, "load_insider_net_buy_panel", fake_loader)

    from research.factors.builtins.insider_net_buy import InsiderNetBuyFactor

    # register in the isolated registry
    _proto._REGISTRY.pop("insider_net_buy", None)
    _proto.register(InsiderNetBuyFactor())

    ctx = FactorContext(engine=MagicMock(), dates=dates, security_universe=universe)
    factor = get("insider_net_buy")
    result = factor.compute(ctx)

    assert result.shape == (2, 2)
    pd.testing.assert_frame_equal(result, mock_panel)


def test_builtin_factor_registered(_isolate_registry):
    """get('insider_net_buy') returns InsiderNetBuyFactor."""
    from research.factors.builtins.insider_net_buy import InsiderNetBuyFactor

    _proto._REGISTRY.pop("insider_net_buy", None)
    _proto.register(InsiderNetBuyFactor())

    factor = get("insider_net_buy")
    assert isinstance(factor, InsiderNetBuyFactor)
    assert factor.name == "insider_net_buy"
    assert factor.lookback_days == 90
    assert factor.lag_days == 1
    assert factor.pit_guarantee is True


# ---------------------------------------------------------------------------
# SQL integration tests (load_insider_events against real PG)
# ---------------------------------------------------------------------------

def _insert_security(pg_db, security_id: int, symbol: str) -> None:
    from sqlalchemy import text

    with pg_db.engine.connect() as conn:
        conn.execute(
            text(
                """
                insert into securities
                    (id, symbol, current_symbol, market, type, is_active, full_refresh_interval)
                values
                    (:id, :symbol, :symbol, 'US', 'CS', true, 30)
                """
            ),
            {"id": security_id, "symbol": symbol},
        )
        conn.commit()


def _insert_insider_transaction(
    pg_db,
    *,
    row_id: int,
    security_id: int,
    accession: str,
    owner_name: str,
    security_type: str = "NON_DERIVATIVE",
    record_type: str = "TRANSACTION",
    filing_date: str = "2025-01-05",
    transaction_date: str = "2025-01-02",
    transaction_code: str = "P",
    acquired_disposed: str = "A",
    shares: float = 100.0,
    price: float | None = 10.0,
) -> None:
    from sqlalchemy import text

    with pg_db.engine.connect() as conn:
        conn.execute(
            text(
                """
                insert into insider_transactions
                    (id, security_id, source, accession_number, source_row_hash,
                     form_type, filing_date, owner_name,
                     security_type, record_type, transaction_date, transaction_code,
                     transaction_shares, transaction_price_per_share,
                     transaction_acquired_disposed)
                values
                    (:id, :security_id, 'SEC_EDGAR', :accession, :row_hash,
                     '4', :filing_date, :owner_name,
                     :security_type, :record_type, :transaction_date, :transaction_code,
                     :shares, :price, :acquired_disposed)
                """
            ),
            {
                "id": row_id,
                "security_id": security_id,
                "accession": accession,
                "row_hash": f"hash-{row_id}",
                "filing_date": filing_date,
                "owner_name": owner_name,
                "security_type": security_type,
                "record_type": record_type,
                "transaction_date": transaction_date,
                "transaction_code": transaction_code,
                "shares": shares,
                "price": price,
                "acquired_disposed": acquired_disposed,
            },
        )
        conn.commit()


@pytest.mark.integration
def test_multi_owner_copies_counted_once(pg_db):
    """联合申报同一 entry 按 owner 复制的行只计一次（取 min(id) 行）。"""
    _insert_security(pg_db, 1, "aapl")
    # 一笔 10 万股买入 × 3 个 reporting owner -> 解析层 3 行副本
    for i, owner in enumerate(["Fund GP LLC", "Fund LP", "Manager"]):
        _insert_insider_transaction(
            pg_db,
            row_id=10 + i,
            security_id=1,
            accession="0001-25-000001",
            owner_name=owner,
            shares=100000.0,
        )
    # 同一 accession 内另一条 entry（股数不同）不能被误并
    _insert_insider_transaction(
        pg_db,
        row_id=20,
        security_id=1,
        accession="0001-25-000001",
        owner_name="Fund GP LLC",
        shares=500.0,
    )

    events = load_insider_events(pg_db.engine)

    assert len(events) == 2
    assert sorted(events["signed_shares"].tolist()) == [500.0, 100000.0]


@pytest.mark.integration
def test_multi_owner_dedupe_scoped_by_accession(pg_db):
    """不同 accession 的字段全同 entry 是独立交易，不得跨 accession 去重。"""
    _insert_security(pg_db, 1, "aapl")
    _insert_insider_transaction(pg_db, row_id=1, security_id=1, accession="0001-25-000001", owner_name="CEO")
    _insert_insider_transaction(pg_db, row_id=2, security_id=1, accession="0001-25-000002", owner_name="CEO")

    events = load_insider_events(pg_db.engine)

    assert len(events) == 2
    assert events["signed_shares"].tolist() == [100.0, 100.0]


@pytest.mark.integration
def test_derivative_rows_excluded(pg_db):
    """DERIVATIVE 表的 P/S 行必须被排除——买 put 不得计为看多。"""
    _insert_security(pg_db, 1, "aapl")
    # 买入 put 期权：code='P'、acquired='A'，若混入会被当成 5 万股看多
    _insert_insider_transaction(
        pg_db,
        row_id=1,
        security_id=1,
        accession="0001-25-000001",
        owner_name="CEO",
        security_type="DERIVATIVE",
        shares=50000.0,
    )
    # 卖出 call：同样必须排除
    _insert_insider_transaction(
        pg_db,
        row_id=2,
        security_id=1,
        accession="0001-25-000002",
        owner_name="CEO",
        security_type="DERIVATIVE",
        transaction_code="S",
        acquired_disposed="D",
        shares=30000.0,
    )
    # 真正的普通股开放市场买入
    _insert_insider_transaction(
        pg_db,
        row_id=3,
        security_id=1,
        accession="0001-25-000003",
        owner_name="CEO",
        shares=100.0,
    )

    events = load_insider_events(pg_db.engine)

    assert len(events) == 1
    assert events["signed_shares"].tolist() == [100.0]


@pytest.mark.integration
def test_holding_rows_excluded(pg_db):
    """record_type='HOLDING' 的行不是交易，必须被排除。"""
    _insert_security(pg_db, 1, "aapl")
    _insert_insider_transaction(
        pg_db,
        row_id=1,
        security_id=1,
        accession="0001-25-000001",
        owner_name="CEO",
        record_type="HOLDING",
        shares=99999.0,
    )
    _insert_insider_transaction(
        pg_db,
        row_id=2,
        security_id=1,
        accession="0001-25-000002",
        owner_name="CEO",
        shares=100.0,
    )

    events = load_insider_events(pg_db.engine)

    assert len(events) == 1
    assert events["signed_shares"].tolist() == [100.0]


@pytest.mark.integration
def test_null_price_copies_deduped(pg_db):
    """price 为 NULL 的多 owner 副本同样只计一次（distinct 视 NULL 相等）。"""
    _insert_security(pg_db, 1, "aapl")
    for i, owner in enumerate(["Owner A", "Owner B"]):
        _insert_insider_transaction(
            pg_db,
            row_id=10 + i,
            security_id=1,
            accession="0001-25-000001",
            owner_name=owner,
            transaction_code="S",
            acquired_disposed="D",
            shares=2000.0,
            price=None,
        )

    events = load_insider_events(pg_db.engine)

    assert len(events) == 1
    assert events["signed_shares"].tolist() == [-2000.0]
