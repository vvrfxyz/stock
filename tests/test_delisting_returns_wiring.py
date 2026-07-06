"""delisting_events.delisting_return 到研究层的接线测试。

覆盖三段（docs/todo_crsp_grade_2026-07.md 任务 1 步骤 4）：
- run_baselines.resolve_terminal_returns：纯函数决策——实测非空时 Series 优先、
  CLI 标量降级为 fallback；实测为空 / 显式 opt-out 时行为与旧口径逐位一致。
- data.load_delisting_returns：只取每证券最近一次退市事件的实测收益，NULL 行
  整体缺席（宁缺毋滥，由 fallback 兜底），绝不借用更早退市周期的收益。
- fund_closure_par 开关：ETF 清盘（FUND_CLOSURE + final_price 在场 + 实测 NULL）
  在读取层合成 0.0（事实表纪律：无实据不写数值，经验值活在读取层）；
  opt-out 排除合成行；实测值永远优先于合成值。
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


# ---------------------------------------------------------------------------
# fund_closure_par：ETF 清盘平价合成（读取层经验值，事实表保持 NULL）
# ---------------------------------------------------------------------------

def _fund_closure_event(security_id, delist_date, *, final_price, delisting_return=None):
    return _event(
        security_id, delist_date, delisting_return,
        reason_code="FUND_CLOSURE",
        reason_confidence="MEDIUM",
        source="TICKER_EVENT",
        final_price=final_price,
        final_price_date=delist_date if final_price is not None else None,
    )


@pytest.mark.integration
def test_fund_closure_par_synthesizes_zero_at_read_time(pg_db):
    from research.data import load_delisting_returns

    for sid, sym in [(1, "detf"), (2, "noprc"), (3, "dead")]:
        _insert_security(pg_db, sid, sym)

    pg_db.upsert_delisting_events([
        # FUND_CLOSURE + final_price 在场 + 实测 NULL -> 默认合成 0.0
        _fund_closure_event(1, date(2023, 4, 14), final_price=Decimal("25.10")),
        # FUND_CLOSURE 但无 final_price：终价都没有，平价假设无锚点，不合成
        _fund_closure_event(2, date(2023, 4, 14), final_price=None),
        # 非 ETF 的 NULL 行照旧缺席
        _event(3, date(2023, 4, 14), None),
    ])

    got = load_delisting_returns(pg_db.engine)
    assert got.to_dict() == {1: 0.0}
    assert got.dtype == "float64"


@pytest.mark.integration
def test_fund_closure_par_opt_out_excludes_synthesized_rows(pg_db):
    from research.data import load_delisting_returns

    _insert_security(pg_db, 1, "detf")
    _insert_security(pg_db, 2, "dead")
    pg_db.upsert_delisting_events([
        _fund_closure_event(1, date(2023, 4, 14), final_price=Decimal("25.10")),
        _event(2, date(2020, 3, 2), Decimal("-1")),   # 实测行不受开关影响
    ])

    got = load_delisting_returns(pg_db.engine, fund_closure_par=False)
    assert got.to_dict() == {2: -1.0}


@pytest.mark.integration
def test_fund_closure_measured_return_never_overridden_by_par(pg_db):
    from research.data import load_delisting_returns

    _insert_security(pg_db, 1, "detf")
    # 罕见但可能：某 ETF 清盘拿到了实测收益（如清算分配实据）——绝不被 0.0 覆盖
    pg_db.upsert_delisting_events([
        _fund_closure_event(1, date(2023, 4, 14),
                            final_price=Decimal("25.10"),
                            delisting_return=Decimal("-0.013")),
    ])

    assert load_delisting_returns(pg_db.engine).to_dict() == {1: -0.013}
    assert load_delisting_returns(pg_db.engine, fund_closure_par=False).to_dict() == {1: -0.013}


@pytest.mark.integration
def test_fund_closure_par_respects_latest_episode_semantics(pg_db):
    from research.data import load_delisting_returns

    _insert_security(pg_db, 1, "retf")
    # 两次退市：更早一次是可合成的 FUND_CLOSURE，最近一次是无实据的 UNKNOWN
    # —— 面板终局是最近那次，不得借用旧周期的合成收益
    pg_db.upsert_delisting_events([
        _fund_closure_event(1, date(2015, 1, 5), final_price=Decimal("20.00")),
        _event(1, date(2024, 9, 30), None),
    ])

    assert load_delisting_returns(pg_db.engine).empty


def _redemption_event(security_id, delist_date, *, final_price, evidence, delisting_return=None):
    return {
        "security_id": security_id,
        "delist_date": delist_date,
        "reason_code": "LIQUIDATION",
        "reason_confidence": "HIGH",
        "final_price": final_price,
        "final_price_date": delist_date if final_price is not None else None,
        "delisting_return": delisting_return,
        "source": "FORM25",
        "evidence": evidence,
    }


@pytest.mark.integration
def test_redemption_par_synthesizes_zero_for_spac_redemptions(pg_db):
    """SPAC 赎回清算 par：LIQUIDATION + redemption_provision 证据 + final_price
    在场 → 读取时合成 0.0（与 ETF 清盘 par 同构）；无该证据的真清算不合成。"""
    from research.data import load_delisting_returns

    for sid, sym in [(1, "spac"), (2, "liqd"), (3, "nopx")]:
        _insert_security(pg_db, sid, sym)
    pg_db.upsert_delisting_events([
        _redemption_event(1, date(2023, 4, 14), final_price=Decimal("10.02"),
                          evidence="form25_rule=12d2-2(a)(1)|redemption_provision"),
        # 无 redemption_provision 标记的 LIQUIDATION（真清算）：不合成
        _redemption_event(2, date(2023, 4, 14), final_price=Decimal("3.10"),
                          evidence="form25_rule=12d2-2(a)(2)"),
        # 有标记但无 final_price：无锚点，不合成
        _redemption_event(3, date(2023, 4, 14), final_price=None,
                          evidence="form25_rule=12d2-2(a)(1)|redemption_provision"),
    ])

    got = load_delisting_returns(pg_db.engine)
    assert got.to_dict() == {1: 0.0}
    # 单独关 redemption par：spac 行消失
    assert load_delisting_returns(pg_db.engine, redemption_par=False).empty
