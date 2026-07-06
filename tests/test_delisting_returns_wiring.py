"""delisting_events.delisting_return 到研究层的接线测试。

覆盖两段（docs/todo_crsp_grade_2026-07.md 任务 1 步骤 4）：
- run_baselines.resolve_terminal_returns：纯函数决策——实测非空时 Series 优先、
  CLI 标量降级为 fallback；实测为空 / 显式 opt-out 时行为与旧口径逐位一致。
- data.load_delisting_returns：只取每证券最近一次退市事件的实测收益，NULL 行
  整体缺席（宁缺毋滥，由 fallback 兜底），绝不借用更早退市周期的收益。
"""
from datetime import date
from decimal import Decimal

import pandas as pd
import pytest

from research.run_baselines import parse_args, resolve_terminal_returns

EMPTY = pd.Series(dtype="float64")
REALIZED = pd.Series({1: -1.0, 2: 0.002})


# ---------------------------------------------------------------------------
# resolve_terminal_returns：纯函数，无 DB
# ---------------------------------------------------------------------------

class TestResolveTerminalReturns:
    def test_empty_realized_passes_scalar_through(self):
        """表未填充：CLI 标量原样直传、无 fallback——与旧口径完全一致。"""
        assert resolve_terminal_returns(EMPTY, -0.3) == (-0.3, None)

    def test_empty_realized_and_no_cli_value_stays_none(self):
        assert resolve_terminal_returns(EMPTY, None) == (None, None)

    def test_realized_series_wins_and_cli_scalar_becomes_fallback(self):
        terminal, fallback = resolve_terminal_returns(REALIZED, -0.3)
        assert terminal is REALIZED
        assert fallback == -0.3

    def test_realized_series_without_cli_value_has_no_fallback(self):
        terminal, fallback = resolve_terminal_returns(REALIZED, None)
        assert terminal is REALIZED
        assert fallback is None

    def test_opt_out_ignores_realized_series(self):
        """--no-delisting-returns：即使实测非空也只用 CLI 标量（复现旧运行）。"""
        assert resolve_terminal_returns(REALIZED, -0.3, use_realized=False) == (-0.3, None)

    def test_opt_out_without_cli_value(self):
        assert resolve_terminal_returns(REALIZED, None, use_realized=False) == (None, None)

    def test_zero_scalar_is_preserved_not_treated_as_missing(self):
        """0.0 是显式假设（退市赚 0%），不能被 falsy 误判成 None。"""
        assert resolve_terminal_returns(EMPTY, 0.0) == (0.0, None)
        assert resolve_terminal_returns(REALIZED, 0.0)[1] == 0.0


class TestParseArgsFlag:
    def test_flag_defaults_off(self):
        args = parse_args([])
        assert args.no_delisting_returns is False

    def test_flag_parses(self):
        args = parse_args(["--no-delisting-returns"])
        assert args.no_delisting_returns is True

    def test_long_window_guard_still_mandatory_with_flag(self):
        """--start < 2024-05-14 必须显式给 --terminal-return，opt-out 不豁免。"""
        with pytest.raises(SystemExit):
            parse_args(["--start", "2010-01-01", "--no-delisting-returns"])

    def test_long_window_with_explicit_terminal_return_passes(self):
        args = parse_args(["--start", "2010-01-01", "--terminal-return", "-0.3",
                           "--no-delisting-returns"])
        assert args.terminal_return == -0.3
        assert args.no_delisting_returns is True


def test_resolved_tuple_feeds_run_backtest_per_security():
    """接线端到端（无 DB）：resolve 出的 (terminal, fallback) 直接喂 run_backtest，
    实测覆盖的证券注入自己的收益，未覆盖的落 fallback。"""
    from research.backtest import run_backtest

    dates = pd.to_datetime(["2026-01-01", "2026-01-02", "2026-01-03", "2026-01-04"])
    prices = pd.DataFrame({1: [100.0, 110.0, None, None],
                           2: [50.0, 55.0, None, None]}, index=dates)
    weights = pd.DataFrame({1: [0.5, 0.5, 0.0, 0.0],
                            2: [0.5, 0.5, 0.0, 0.0]}, index=dates)

    terminal, fallback = resolve_terminal_returns(pd.Series({1: -1.0}), -0.3)
    result = run_backtest("wired", weights, prices, cost_bps=0.0,
                          terminal_return=terminal, terminal_return_fallback=fallback)
    # t=2 双双首个永久缺失日：sec1 实测 -1.0×0.5 + sec2 fallback -0.3×0.5 = -0.65
    assert abs(result.daily_returns.iloc[2] - (-0.65)) < 1e-9


# ---------------------------------------------------------------------------
# load_delisting_returns：PostgreSQL 集成
# ---------------------------------------------------------------------------

def _insert_security(pg_db, security_id, symbol):
    from data_models.models import Security

    with pg_db.get_session() as session:
        session.add(Security(
            id=security_id, symbol=symbol, current_symbol=symbol,
            market="US", type="CS", is_active=False, full_refresh_interval=30,
        ))
        session.commit()


def _event(security_id, delist_date, delisting_return, **extra):
    """upsert_delisting_events 是全量重建语义（缺省键=NULL），行给全一点。"""
    return {
        "security_id": security_id,
        "delist_date": delist_date,
        "reason_code": "ACQUISITION_CASH" if delisting_return is not None else "UNKNOWN",
        "reason_confidence": "HIGH" if delisting_return is not None else "LOW",
        "final_price": Decimal("10.00"),
        "final_price_date": delist_date,
        "delisting_return": delisting_return,
        "source": "8K" if delisting_return is not None else "PRICE_INFERRED",
        "evidence": "test fixture",
        **extra,
    }


@pytest.mark.integration
def test_load_delisting_returns_empty_table(pg_db):
    from research.data import load_delisting_returns

    got = load_delisting_returns(pg_db.engine)
    assert got.empty
    assert got.dtype == "float64"


@pytest.mark.integration
def test_load_delisting_returns_excludes_null_and_takes_latest_episode(pg_db):
    from research.data import load_delisting_returns

    for sid, sym in [(1, "dead1"), (2, "dead2"), (3, "twice"), (4, "zero")]:
        _insert_security(pg_db, sid, sym)

    written = pg_db.upsert_delisting_events([
        # 有实测收益
        _event(1, date(2020, 3, 2), Decimal("-1")),
        # 无实测收益（NULL）——必须整体缺席，由 fallback 兜底
        _event(2, date(2021, 6, 15), None),
        # 两次退市：更早一次有实测、最近一次 NULL——不得借用旧周期的收益
        _event(3, date(2015, 1, 5), Decimal("0.05")),
        _event(3, date(2024, 9, 30), None),
        # 实测恰为 0.0（现金并购对价≈终价的常态）不得被当成缺失丢掉
        _event(4, date(2022, 11, 1), Decimal("0")),
    ])
    assert written == 5

    got = load_delisting_returns(pg_db.engine)

    assert got.to_dict() == {1: -1.0, 4: 0.0}
    assert got.index.dtype == "int64"
    assert got.dtype == "float64"
    # run_backtest 需要唯一 index 才能 reindex 到面板列
    assert got.index.is_unique
