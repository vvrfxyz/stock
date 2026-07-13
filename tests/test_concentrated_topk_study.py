"""concentrated_topk 模拟核心语义锁定测试（纯合成数据，无 DB）。

锁定预注册的可验证不变量：t+1 收盘执行、退出优先级 cap>e1>e2、Chandelier 线、
SMA200 周检、退市结算（实测收益/0% 缺省/停牌跨末冻结）、冷却再入场、
无杠杆现金约束、成本恒等式、DTB3 现金计息、判据布尔逻辑与 E0-FAIL 不翻案。
"""
from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd
import pytest

from research.concentrated_topk_study import (
    EXIT_VARIANTS,
    SimConfig,
    SimInputs,
    compute_metrics,
    evaluate_exit_verdict,
    evaluate_selector_verdict,
    make_ranked_pick,
    make_random_pick,
    simulate,
)


def _dates(n: int) -> pd.DatetimeIndex:
    return pd.bdate_range("2020-01-06", periods=n)


def _weekly_pos(dates: pd.DatetimeIndex) -> np.ndarray:
    s = pd.Series(np.arange(len(dates)), index=dates)
    return s.groupby(dates.to_period("W")).last().to_numpy()


def _inputs(prices: np.ndarray, *, rf: float = 0.0, cost: float | None = None,
            delist: dict[int, float] | None = None,
            alive_beyond: list[bool] | None = None,
            sma: np.ndarray | None = None, atr: np.ndarray | None = None) -> SimInputs:
    T, N = prices.shape
    dates = _dates(T)
    finite = np.isfinite(prices)
    last_priced = np.where(finite.any(axis=0), T - 1 - np.argmax(finite[::-1], axis=0), -1)
    dr = np.full(N, np.nan)
    for col, r in (delist or {}).items():
        dr[col] = r
    cost_panel = None
    if cost is not None:
        cost_panel = np.full((T, N), cost, dtype="float32")
    return SimInputs(
        dates=dates, adj_close=prices.astype("float64"),
        weekly_pos=_weekly_pos(dates), rf=np.full(T, rf),
        sec_ids=np.arange(N, dtype="int64") + 100, last_priced=last_priced,
        alive_beyond=np.asarray(alive_beyond if alive_beyond is not None else [False] * N),
        delist_ret=dr, cost_bps=cost_panel, fallback_bps=40.0,
        sma200=sma, atr22=atr,
    )


def _cfg(k: int = 1, cap: int = 252, exit_name: str = "e0", start: int = 0) -> SimConfig:
    return SimConfig(k=k, cap_sessions=cap, exit=EXIT_VARIANTS[exit_name], start_week_idx=start)


def _flat(n: int, cols: int, price: float = 10.0) -> np.ndarray:
    return np.full((n, cols), price)


class TestExecutionTiming:
    def test_buy_executes_next_session_close(self):
        px = _flat(30, 1)
        px[:, 0] = np.linspace(10, 12, 30)  # 单调涨：入场价必须是信号次日的价
        si = _inputs(px, cost=0.0)
        res = simulate(si, _cfg(), make_ranked_pick([np.array([0])] * len(si.weekly_pos)))
        sp = res.spells.iloc[0]
        assert sp["entry_exec"] == si.weekly_pos[0] + 1
        assert sp["entry_price"] == pytest.approx(px[si.weekly_pos[0] + 1, 0])

    def test_halted_execution_postpones(self):
        px = _flat(30, 1)
        w0 = int(_weekly_pos(_dates(30))[0])
        px[w0 + 1, 0] = np.nan  # 信号次日停牌 → 顺延一日
        si = _inputs(px, cost=0.0)
        res = simulate(si, _cfg(), make_ranked_pick([np.array([0])] * len(si.weekly_pos)))
        assert res.spells.iloc[0]["entry_exec"] == w0 + 2
        assert res.counters["postponed_buys"] == 1

    def test_no_leverage_cash_floor(self):
        px = _flat(60, 3)
        si = _inputs(px, cost=0.0)
        ranked = [np.array([0, 1, 2])] * len(si.weekly_pos)
        res = simulate(si, SimConfig(k=3, cap_sessions=252, exit=EXIT_VARIANTS["e0"],
                                     start_week_idx=0), make_ranked_pick(ranked))
        assert (res.daily["cash"].to_numpy() >= -1e-9).all()
        assert len(res.spells) == 3


class TestExits:
    def test_cap_exit_at_252(self):
        px = _flat(300, 1)
        si = _inputs(px, cost=0.0)
        res = simulate(si, _cfg(cap=252), make_ranked_pick([np.array([0])] * len(si.weekly_pos)))
        first = res.spells.iloc[0]
        assert first["reason"] == "cap"
        assert first["exit_signal"] == first["entry_exec"] + 252
        assert first["exit_exec"] == first["exit_signal"] + 1

    def test_e2_chandelier_triggers(self):
        T = 60
        px = _flat(T, 1, 100.0)
        px[30:, 0] = 100.0 - 3.5 * 2.0  # 高点 100，ATR=2 → 线 94；跌到 93 触发
        atr = np.full((T, 1), 2.0)
        si = _inputs(px, cost=0.0, atr=atr)
        res = simulate(si, _cfg(exit_name="e2"), make_ranked_pick([np.array([0])] * len(si.weekly_pos)))
        sp = res.spells.iloc[0]
        assert sp["reason"] == "e2"
        assert sp["exit_signal"] == 30
        assert sp["exit_exec"] == 31

    def test_e1_sma_weekly_check_only(self):
        T = 80
        px = _flat(T, 1, 100.0)
        sma = np.full((T, 1), np.nan)
        sma[40:, 0] = 105.0  # 40 起 close<SMA；但只能在周度信号日触发
        si = _inputs(px, cost=0.0, sma=sma)
        res = simulate(si, _cfg(exit_name="e1"), make_ranked_pick([np.array([0])] * len(si.weekly_pos)))
        sp = res.spells.iloc[0]
        assert sp["reason"] == "e1"
        weekly = set(int(w) for w in si.weekly_pos)
        assert int(sp["exit_signal"]) in weekly
        assert sp["exit_signal"] >= 40

    def test_exit_priority_cap_over_e1_e2(self):
        # 同一日 cap 与 e1/e2 并发 → 取 cap 标签（预注册优先序）。
        # bdate 周度信号日在行号 4,9,14,...（每 5 日）；entry_exec=5，
        # 取 cap=254 使 cap_sig=259=4+5*51 恰落周度信号日。
        T = 300
        cap = 254
        px = _flat(T, 1, 100.0)
        w = _weekly_pos(_dates(T))
        entry_exec = int(w[0]) + 1
        cap_sig = entry_exec + cap
        sma = np.full((T, 1), np.nan)
        sma[cap_sig:, 0] = 200.0
        atr = np.full((T, 1), 1e9)  # e2 永不触发
        si = _inputs(px, cost=0.0, sma=sma, atr=atr)
        assert cap_sig in set(int(x) for x in si.weekly_pos)  # 构造保证并发
        res = simulate(si, _cfg(exit_name="e3", cap=cap),
                       make_ranked_pick([np.array([0])] * len(si.weekly_pos)))
        assert res.spells.iloc[0]["reason"] == "cap"

    def test_cooldown_blocks_immediate_reentry(self):
        T = 120
        px = _flat(T, 1, 100.0)
        atr = np.full((T, 1), 1.0)
        px[10:, 0] = 90.0  # 快速触发 e2
        px[12:, 0] = 100.0  # 反弹（价格又高于线，若无冷却会立刻回补）
        si = _inputs(px, cost=0.0, atr=atr)
        res = simulate(si, _cfg(exit_name="e2"), make_ranked_pick([np.array([0])] * len(si.weekly_pos)))
        s = res.spells.sort_values("entry_exec")
        assert len(s) >= 2
        first_exit_exec = int(s.iloc[0]["exit_exec"])
        wpos = si.weekly_pos
        j0 = int(np.searchsorted(wpos, first_exit_exec, side="left"))
        # 再入场信号日必须 > 冷却周（wpos[j0]）
        assert int(s.iloc[1]["signal_pos"]) > int(wpos[j0])


class TestDelisting:
    def test_measured_return_applied(self):
        T = 40
        px = _flat(T, 1, 10.0)
        px[25:, 0] = np.nan  # 24 为最后有价日
        si = _inputs(px, cost=0.0, delist={0: -0.5})
        res = simulate(si, _cfg(), make_ranked_pick([np.array([0])] * len(si.weekly_pos)))
        sp = res.spells.iloc[0]
        assert sp["reason"] == "delist"
        assert sp["proceeds"] == pytest.approx(sp["shares"] * 10.0 * 0.5)
        assert res.daily["nav"].iloc[-1] == pytest.approx(sp["proceeds"], rel=1e-9)

    def test_unresolved_defaults_zero_and_counted(self):
        T = 40
        px = _flat(T, 1, 10.0)
        px[25:, 0] = np.nan
        si = _inputs(px, cost=0.0)  # 无实测 → 0%
        res = simulate(si, _cfg(), make_ranked_pick([np.array([0])] * len(si.weekly_pos)))
        sp = res.spells.iloc[0]
        assert sp["proceeds"] == pytest.approx(sp["shares"] * 10.0)
        assert res.counters["unresolved_delist"] == 1

    def test_halt_beyond_window_is_frozen_not_delisted(self):
        T = 40
        px = _flat(T, 1, 10.0)
        px[25:, 0] = np.nan
        si = _inputs(px, cost=0.0, alive_beyond=[True])  # 全史后面还有价 → 停牌
        res = simulate(si, _cfg(), make_ranked_pick([np.array([0])] * len(si.weekly_pos)))
        sp = res.spells.iloc[0]
        assert sp["reason"] == "open"
        assert res.counters["frozen_at_end"] == 1
        # 冻结在末值 ffill：期末 NAV = 持仓按 10 元估值
        assert res.daily["pos_value"].iloc[-1] == pytest.approx(sp["shares"] * 10.0)


class TestCostsAndCash:
    def test_cost_identity_and_fallback_count(self):
        T = 40
        px = _flat(T, 1, 10.0)
        si = _inputs(px, cost=None)  # 无覆盖 → 全走 40bps fallback
        res = simulate(si, _cfg(cap=10), make_ranked_pick([np.array([0])] * len(si.weekly_pos)))
        sp = res.spells.iloc[0]
        frac = 40.0 / 1e4
        assert sp["entry_cost"] == pytest.approx(sp["gross"] * frac / (1 + frac))
        assert res.counters["fallback_trades"] >= 2
        assert sp["exit_cost"] == pytest.approx(sp["shares"] * 10.0 * frac)

    def test_cash_earns_rf(self):
        T = 30
        px = _flat(T, 1, 10.0)
        si = _inputs(px, rf=0.0001, cost=0.0)
        res = simulate(si, _cfg(), make_ranked_pick([np.array([], dtype=int)] * len(si.weekly_pos)))
        # 永不建仓：NAV = 纯现金复利
        n = len(res.daily) - 1
        assert res.daily["nav"].iloc[-1] == pytest.approx((1.0001) ** n, rel=1e-12)

    def test_zero_price_move_zero_cost_nav_flat(self):
        px = _flat(60, 2, 10.0)
        si = _inputs(px, cost=0.0)
        res = simulate(si, SimConfig(k=2, cap_sessions=252, exit=EXIT_VARIANTS["e0"],
                                     start_week_idx=0),
                       make_ranked_pick([np.array([0, 1])] * len(si.weekly_pos)))
        assert np.allclose(res.daily["nav"].to_numpy(), 1.0, atol=1e-12)


class TestVerdicts:
    def test_selector_verdict_all_pass(self):
        main = {"excess_annual": 0.05, "excess_ex_top_spell": 0.01,
                "max_drawdown": -0.30, "spy_max_drawdown": -0.25}
        v = evaluate_selector_verdict(main, [0.05, 0.04, 0.03, 0.02], 0.01, 0.95, 0.02)
        assert v["pass"] and not v["deploy_frozen"]

    def test_selector_verdict_phase_fail(self):
        main = {"excess_annual": 0.05, "excess_ex_top_spell": 0.01,
                "max_drawdown": -0.30, "spy_max_drawdown": -0.25}
        v = evaluate_selector_verdict(main, [0.05, 0.04, -0.03, 0.02], 0.01, 0.95, 0.02)
        assert not v["pass"] and not v["c2_phases"]

    def test_deploy_frozen_on_mdd_guard(self):
        main = {"excess_annual": 0.05, "excess_ex_top_spell": 0.01,
                "max_drawdown": -0.45, "spy_max_drawdown": -0.25}
        v = evaluate_selector_verdict(main, [0.05, 0.04, 0.03, 0.02], 0.01, 0.95, 0.02)
        assert v["pass"] and v["deploy_frozen"]

    def test_exit_verdict_not_judged_when_e0_fail(self):
        e0 = {"max_drawdown": -0.5, "sharpe": 0.2, "cagr": 0.05}
        e3 = {"max_drawdown": -0.2, "sharpe": 0.9, "cagr": 0.06}
        v = evaluate_exit_verdict(e0, e3, e0_passed=False)
        assert not v["judged"] and not v["pass"]  # 数值再好也不翻案
        assert v["c1_mdd_improve"] and v["c2_sharpe"] and v["c3_cagr_loss"]

    def test_exit_verdict_pass(self):
        e0 = {"max_drawdown": -0.40, "sharpe": 0.50, "cagr": 0.10}
        e3 = {"max_drawdown": -0.25, "sharpe": 0.65, "cagr": 0.09}
        v = evaluate_exit_verdict(e0, e3, e0_passed=True)
        assert v["judged"] and v["pass"]


class TestMetricsAndRandom:
    def test_metrics_match_manual(self):
        px = np.linspace(10, 20, 253).reshape(-1, 1)
        si = _inputs(px, cost=0.0)
        res = simulate(si, _cfg(cap=500), make_ranked_pick([np.array([0])] * len(si.weekly_pos)))
        spy = np.linspace(100, 110, 253)
        m = compute_metrics(res.daily, si.rf, spy, res.start_pos, res.spells)
        nav = res.daily["nav"].to_numpy()
        years = (len(nav) - 1) / 252.0
        assert m["cagr"] == pytest.approx((nav[-1] / nav[0]) ** (1 / years) - 1, rel=1e-12)
        assert m["excess_ex_top_spell"] <= m["excess_annual"]

    def test_random_pick_reproducible_and_no_blocked(self):
        pools = [np.arange(10)] * 5
        p1 = make_random_pick(pools, seed=42)
        p2 = make_random_pick(pools, seed=42)
        assert p1(0, 3, set()) == p2(0, 3, set())
        got = make_random_pick(pools, seed=7)(0, 5, {0, 1, 2, 3, 4})
        assert set(got).isdisjoint({0, 1, 2, 3, 4})

    def test_study_kind_registered(self):
        from research._trials_store import STUDY_KINDS
        assert "concentrated_topk" in STUDY_KINDS
