"""paper_portfolio_tracker 语义锁定测试（纯函数，无 DB）。

锁定：延续选仓（保留∩q5、按信号降序补足、上限 30）、换手成本（首期全买、
延续期只对进出计费、fallback 成本）、结算收益口径（复权比值、停牌冻结、
SPY 对照）。
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from research.paper_portfolio_tracker import (
    FALLBACK_COST_BPS,
    HOLDINGS,
    SPY_SECURITY_ID,
    pick_holdings,
    settle_previous,
    turnover_cost,
)


class TestPickHoldings:
    def test_inception_takes_top30(self):
        q5 = list(range(1, 101))
        assert pick_holdings(q5, []) == q5[:HOLDINGS]

    def test_continuity_keeps_survivors_fills_by_signal(self):
        q5 = [10, 11, 12, 13, 14] + list(range(100, 160))
        prev = [11, 13, 999]      # 999 已离场
        out = pick_holdings(q5, prev)
        assert out[:2] == [11, 13]            # 幸存者顺序保留
        assert 999 not in out
        assert out[2:] == [10, 12, 14] + list(range(100, 125))  # 按 q5 序补足
        assert len(out) == HOLDINGS

    def test_never_exceeds_holdings_cap(self):
        q5 = list(range(200))
        prev = list(range(50))    # 50 只旧仓全部幸存 → 只保留前 30
        out = pick_holdings(q5, prev)
        assert len(out) == HOLDINGS
        assert out == prev[:HOLDINGS]

    def test_short_q5_returns_all(self):
        q5 = [1, 2, 3]
        assert pick_holdings(q5, []) == [1, 2, 3]


class TestTurnoverCost:
    def test_inception_full_buy(self):
        ids = list(range(1, 31))
        cost = pd.Series(10.0, index=ids)   # 10bps 单边
        t = turnover_cost([], ids, cost)
        assert t["n_buys"] == 30 and t["n_sells"] == 0
        assert t["cost_frac"] == pytest.approx(30 * (1 / 30) * 10 / 1e4, rel=1e-9)

    def test_continuation_only_changes_charged(self):
        prev = [{"security_id": i, "weight": 1 / 30} for i in range(1, 31)]
        new = list(range(3, 33))            # 卖 1,2 买 31,32
        cost = pd.Series(20.0, index=range(1, 40))
        t = turnover_cost(prev, new, cost)
        assert t["n_buys"] == 2 and t["n_sells"] == 2
        assert t["cost_frac"] == pytest.approx(4 * (1 / 30) * 20 / 1e4, abs=5e-7)  # 落盘 round(6)

    def test_missing_cost_uses_fallback(self):
        t = turnover_cost([], [1], pd.Series(dtype="float64"))
        assert t["cost_frac"] == pytest.approx(FALLBACK_COST_BPS / 1e4, rel=1e-9)


class TestSettlement:
    def _snap(self):
        dates = pd.DatetimeIndex(["2026-06-02", "2026-06-30", "2026-07-01"])
        adj = pd.DataFrame({
            1: [10.0, 11.0, 12.0],           # +20%
            2: [50.0, np.nan, np.nan],        # 停牌：冻结 50 → 0%
            SPY_SECURITY_ID: [100.0, 104.0, 105.0],  # +5%
        }, index=dates)
        return {"adj_close": adj, "dates": dates, "exec_ts": pd.Timestamp("2026-07-01")}

    def _prev(self):
        return {"month": "2026-06", "execution_date": "2026-06-02",
                "holdings": [
                    {"security_id": 1, "symbol": "aaa", "weight": 0.5},
                    {"security_id": 2, "symbol": "bbb", "weight": 0.5},
                ]}

    def test_gross_and_spy_and_frozen(self):
        s = settle_previous(None, self._prev(), self._snap(), None)
        assert s["gross_ret"] == pytest.approx(0.5 * 0.20 + 0.5 * 0.0, rel=1e-9)
        assert s["spy_ret"] == pytest.approx(0.05, rel=1e-9)
        assert s["excess_vs_spy"] == pytest.approx(0.10 - 0.05, rel=1e-9)
        notes = {r["security_id"]: r["note"] for r in s["positions"]}
        assert notes[2] == "frozen_last_price"

    def test_pending_prev_exec_backfilled_from_formation(self):
        # 上月单 execution_date=None（pending）：回填为形成日后第一个交易日
        prev = self._prev()
        prev["execution_date"] = None
        prev["formation_date"] = "2026-06-01"
        s = settle_previous(None, prev, self._snap(), None)
        assert s["from"] == "2026-06-02"

    def test_pending_new_exec_uses_last_panel_date(self):
        snap = self._snap()
        snap["exec_ts"] = None
        s = settle_previous(None, self._prev(), snap, None)
        assert s["to"] == "2026-07-01"

    def test_empty_window_raises(self):
        snap = self._snap()
        snap["exec_ts"] = pd.Timestamp("2026-06-02")  # 等于上月执行日 → 空窗
        with pytest.raises(RuntimeError):
            settle_previous(None, self._prev(), snap, None)
