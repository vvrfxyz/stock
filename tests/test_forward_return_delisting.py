"""前向收益退市终局注入金测试（W0-P2：口径统一到带退市注入的 _forward_return）。

覆盖：
- 无退市（或退市列无实测值）合成面板：注入路径与旧 ffill 实现位级一致（1e-12）；
- 含退市面板：并入值 = 手算注入值（退市在 horizon 窗内/外、fallback 补洞、
  无实测无 fallback 保持旧行为、t_last 之后 NaN 不变）；
- 注入公式与 run_backtest 逐日复合恒等（含面板尾 horizon 越界行）；
- params_hash：--no-ic-delisting-returns 逃生舱翻转产生不同 hash；
- 三消费方（evaluate / factor_correlation / size_neutral_study）共用同一实现（import 同一性）。
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import numpy as np
import pandas as pd
import pytest

import research.evaluate as ev
from research.backtest import run_backtest
from research.evaluate import _forward_return
from research.factors.protocol import FactorContext


# ---------------------------------------------------------------------------
# 参照实现（旧 ffill 口径，重构前 _forward_return 原样拷贝）
# ---------------------------------------------------------------------------

def _forward_return_reference(adj_close: pd.DataFrame, horizon: int) -> pd.DataFrame:
    filled = adj_close.ffill()
    shifted = filled.shift(-horizon)
    valid_pair = adj_close.notna() & shifted.notna()
    return (shifted / filled - 1).where(valid_pair)


@pytest.fixture(scope="module")
def rich_panel() -> pd.DataFrame:
    """坑全覆盖：内部停牌段、并列、深历史，但**无退市尾巴**（所有列面板末日有价）。"""
    rng = np.random.default_rng(3)
    dates = pd.bdate_range("2022-01-03", periods=120)
    n = 60
    cols = pd.Index(range(1, n + 1), dtype="int64")
    px = pd.DataFrame(
        100 * np.exp(np.cumsum(rng.normal(0, 0.02, (len(dates), n)), axis=0)),
        index=dates, columns=cols)
    px.iloc[30:35, 4] = np.nan       # 内部停牌（会复牌，非退市）
    px.iloc[60:61, 9] = np.nan       # 单日停牌
    return px


# ---------------------------------------------------------------------------
# 无退市：注入路径位级等价旧实现
# ---------------------------------------------------------------------------

class TestNoDelistingEquivalence:
    @pytest.mark.parametrize("horizon", [1, 5, 21])
    def test_terminal_none_bitwise_equals_reference(self, rich_panel, horizon):
        new = _forward_return(rich_panel, horizon)
        ref = _forward_return_reference(rich_panel, horizon)
        pd.testing.assert_frame_equal(new, ref, rtol=1e-12, atol=1e-14)

    @pytest.mark.parametrize("horizon", [1, 5, 21])
    def test_series_without_matching_delisting_equals_reference(self, rich_panel, horizon):
        """传实测 Series 但面板内无实际退市列 → injectable 全 False，与旧实现位级一致。"""
        realized = pd.Series({99999: -0.5, 88888: -0.3})  # 不在宇宙里
        new = _forward_return(rich_panel, horizon, terminal_return=realized,
                              terminal_return_fallback=-0.3)
        ref = _forward_return_reference(rich_panel, horizon)
        pd.testing.assert_frame_equal(new, ref, rtol=1e-12, atol=1e-14)

    def test_survivor_column_untouched_when_others_delist(self, rich_panel):
        """有退市列注入时，未退市列（面板末日有价）逐位不变。"""
        px = rich_panel.copy()
        px.iloc[80:, 1] = np.nan  # col 1 退市
        new = _forward_return(px, 5, terminal_return=pd.Series({1: -1.0}))
        ref = _forward_return_reference(px, 5)
        survivors = [c for c in px.columns if c != 1]
        pd.testing.assert_frame_equal(new[survivors], ref[survivors], rtol=1e-12, atol=1e-14)


# ---------------------------------------------------------------------------
# 含退市：手算注入值
# ---------------------------------------------------------------------------

@pytest.fixture
def delist_panel() -> pd.DataFrame:
    """col 10 在 index 3（price 130）后永久缺失（t_last=3）；col 20 存活。"""
    dates = pd.bdate_range("2025-01-06", periods=6)
    cols = pd.Index([10, 20], dtype="int64")
    return pd.DataFrame(
        {10: [100.0, 110.0, 120.0, 130.0, np.nan, np.nan],
         20: [50.0, 51.0, 52.0, 53.0, 54.0, 55.0]},
        index=dates, columns=cols)


class TestDelistingInjection:
    def test_window_in_and_out_scalar(self, delist_panel):
        r_d = -0.40
        fwd = _forward_return(delist_panel, 2, terminal_return=pd.Series({10: r_d}))
        col = fwd[10].to_numpy()
        # 窗外（t+2<=t_last=3）：纯价格前向收益
        assert col[0] == pytest.approx(120.0 / 100.0 - 1)
        assert col[1] == pytest.approx(130.0 / 110.0 - 1)
        # 窗内（t_last=3 落在 [t, t+2)）：price[t_last]/price[t] × (1+r_d) − 1
        assert col[2] == pytest.approx(130.0 / 120.0 * (1 + r_d) - 1)
        assert col[3] == pytest.approx(130.0 / 130.0 * (1 + r_d) - 1)  # = r_d
        # t_last 之后（退市事件日及以后）保持 NaN
        assert np.isnan(col[4]) and np.isnan(col[5])

    def test_fallback_fills_when_series_missing(self, delist_panel):
        """退市列不在实测 Series 里 → 用 fallback 注入（与直接标量口径一致）。"""
        with_fallback = _forward_return(
            delist_panel, 2, terminal_return=pd.Series({999: 0.0}), terminal_return_fallback=-0.40)
        scalar = _forward_return(delist_panel, 2, terminal_return=pd.Series({10: -0.40}))
        pd.testing.assert_frame_equal(with_fallback, scalar)

    def test_no_realized_no_fallback_keeps_old_convention(self, delist_panel):
        """退市列无实测、无 fallback → 不注入，回退旧 ffill 口径（前向收益≈0%）。"""
        uncovered = _forward_return(delist_panel, 2, terminal_return=pd.Series({999: 0.0}))
        ref = _forward_return_reference(delist_panel, 2)
        pd.testing.assert_frame_equal(uncovered, ref, rtol=1e-12, atol=1e-14)

    def test_scalar_injects_all_delisted_columns(self):
        """标量口径对所有退市列统一注入；存活列不受影响。"""
        dates = pd.bdate_range("2025-01-06", periods=6)
        cols = pd.Index([10, 20, 30], dtype="int64")
        px = pd.DataFrame(
            {10: [100.0, 110.0, 120.0, np.nan, np.nan, np.nan],
             20: [50.0, 51.0, 52.0, 53.0, np.nan, np.nan],
             30: [10.0, 11.0, 12.0, 13.0, 14.0, 15.0]},
            index=dates, columns=cols)
        fwd = _forward_return(px, 2, terminal_return=-1.0)
        # col10 t_last=2（price120）：row1 窗内 = 120/110×0−1 = −1
        assert fwd[10].iloc[1] == pytest.approx(-1.0)
        assert fwd[10].iloc[2] == pytest.approx(-1.0)  # = r_d
        # col20 t_last=3：row2/row3 窗内 = −1
        assert fwd[20].iloc[2] == pytest.approx(-1.0)
        assert fwd[20].iloc[3] == pytest.approx(-1.0)
        # col30 存活，纯价格
        assert fwd[30].iloc[0] == pytest.approx(12.0 / 10.0 - 1)

    def test_delisting_within_panel_tail_still_injected(self):
        """退市落在面板尾 horizon 内（t+horizon 越界）仍注入——旧口径会丢这些观察。"""
        dates = pd.bdate_range("2025-01-06", periods=5)
        cols = pd.Index([10], dtype="int64")
        # t_last=3（price130），最后一天 NaN；horizon=3 → row2/row3 的 t+3 越界（shifted NaN）
        px = pd.DataFrame({10: [100.0, 110.0, 120.0, 130.0, np.nan]}, index=dates, columns=cols)
        ref = _forward_return_reference(px, 3)
        inj = _forward_return(px, 3, terminal_return=pd.Series({10: -0.5}))
        # 旧口径 row2/row3 越界丢失
        assert np.isnan(ref[10].iloc[2]) and np.isnan(ref[10].iloc[3])
        # 新口径注入（退市已提前锁定收益，window = [f_pos-3, f_pos) = rows 1,2,3）
        assert inj[10].iloc[2] == pytest.approx(130.0 / 120.0 * 0.5 - 1)
        assert inj[10].iloc[3] == pytest.approx(0.5 - 1)  # 130/130*0.5-1

    def test_halt_row_within_window_stays_nan(self):
        """停牌行（base 价 NaN）即使落在注入窗内也保持 NaN（不改变旧的停牌口径）。"""
        dates = pd.bdate_range("2025-01-06", periods=6)
        cols = pd.Index([10], dtype="int64")
        # row2 停牌（NaN 但复牌），row4/5 退市；t_last=3，horizon=2 → window = rows 2,3
        px = pd.DataFrame({10: [100.0, 110.0, np.nan, 130.0, np.nan, np.nan]},
                          index=dates, columns=cols)
        inj = _forward_return(px, 2, terminal_return=pd.Series({10: -0.4}))
        assert np.isnan(inj[10].iloc[2])                       # 停牌行（窗内）仍 NaN
        assert inj[10].iloc[1] == pytest.approx(130.0 / 110.0 - 1)  # 窗外，纯价格前向收益
        assert inj[10].iloc[3] == pytest.approx(-0.4)          # 窗内有价 → 注入 r_d


class TestRunBacktestEquivalence:
    """注入公式与 run_backtest 逐日复合恒等（终局注入语义同源）。

    审核 #6 修订（2026-07-08）：期望值必须**真调 run_backtest**（单证券全仓权重、
    cost_bps=0，取 daily_returns 逐日复合），不得在测试内手写第二份注入逻辑对拍
    第一份——那样 run_backtest 真实注入路径改动时测试照绿。容差 1e-12。
    """

    def test_matches_daily_compounding(self):
        dates = pd.bdate_range("2025-01-06", periods=8)
        cols = pd.Index([10, 20], dtype="int64")
        px = pd.DataFrame(
            {10: [100.0, 108.0, 115.0, 121.0, 130.0, np.nan, np.nan, np.nan],
             20: [50.0, 51.0, 52.0, 53.0, 54.0, 55.0, 56.0, 57.0]},
            index=dates, columns=cols)
        r_d = pd.Series({10: -0.35})
        # 期望值来源 = run_backtest 本尊：全仓持有 col10、零成本，其 daily_returns
        # 即"col10 的日频收益含终局注入"（含停牌冻结语义，本面板无停牌不触发）。
        w = pd.DataFrame(0.0, index=dates, columns=cols)
        w[10] = 1.0
        bt = run_backtest("equiv", w, px, cost_bps=0.0, hold_through_gaps=True,
                          terminal_return=r_d, terminal_return_fallback=None)
        daily = bt.daily_returns.to_numpy()
        for horizon in (1, 2, 3, 4):
            fwd = _forward_return(px, horizon, terminal_return=r_d)
            for t in range(len(dates)):
                got = fwd[10].iloc[t]
                if np.isnan(got):
                    continue
                seg = daily[t + 1:t + horizon + 1]
                expected = np.prod(1 + seg) - 1
                assert got == pytest.approx(expected, rel=1e-12, abs=1e-12), f"h={horizon} t={t}"


# ---------------------------------------------------------------------------
# params_hash：逃生舱翻转产生不同 hash（无 DB，monkeypatch 面板）
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ConstFactor:
    name = "const"

    def compute(self, ctx: FactorContext) -> pd.DataFrame:
        return pd.DataFrame(1.0, index=ctx.dates, columns=ctx.security_universe)


REALIZED = pd.Series({120: -1.0, 999: 0.05})


def _wire(monkeypatch):
    dates = pd.bdate_range("2025-01-02", periods=20)
    universe = pd.Index(range(1, 121), dtype="int64")
    panel = {
        "adj_close": pd.DataFrame(100.0, index=dates, columns=universe),
        "close": pd.DataFrame(100.0, index=dates, columns=universe),
        "dollar_volume": pd.DataFrame(10_000_000.0, index=dates, columns=universe),
    }
    monkeypatch.setattr(ev, "load_adjusted_panel", lambda *a, **k: panel)
    monkeypatch.setattr(ev, "securities_with_uncovered_events", lambda *a, **k: [])
    monkeypatch.setattr(ev, "load_delisting_returns", lambda *a, **k: REALIZED)
    return dates


def _run(dates, **kwargs):
    return ev.run_evaluation(
        ConstFactor(), engine=object(),
        start=dates.min().date(), end=dates.max().date(),
        horizons=(1,), eval_start=dates[2].date(),
        min_median_dollar_volume=1, eligibility_window=1,
        trials_path=None, risk_free_series=None, **kwargs)


class TestIcDelistingParamsHash:
    def test_escape_hatch_flips_params_hash(self, monkeypatch):
        """--no-ic-delisting-returns（ic_delisting_returns=False）产生不同 params_hash。"""
        dates = _wire(monkeypatch)
        new_ic = _run(dates)  # 默认新口径
        old_ic = _run(dates, ic_delisting_returns=False)
        assert new_ic.params_hash != old_ic.params_hash
        assert new_ic.config["ic_delisting_returns"] is True
        assert old_ic.config["ic_delisting_returns"] is False

    def test_no_key_when_no_terminal(self, monkeypatch):
        """无可注入终局（opt-out + 无标量）时不记 ic_delisting_returns 键——新旧口径
        IC 行为完全一致，多记键会让无退市 trial 无谓换 hash。"""
        dates = _wire(monkeypatch)
        none_default = _run(dates, use_delisting_returns=False)
        none_escape = _run(dates, use_delisting_returns=False, ic_delisting_returns=False)
        assert "ic_delisting_returns" not in none_default.config
        assert none_default.params_hash == none_escape.params_hash

    def test_default_is_new_convention(self, monkeypatch):
        dates = _wire(monkeypatch)
        assert ev.parse_args(["--factors", "size"]).no_ic_delisting_returns is False
        assert ev.parse_args(["--factors", "size", "--no-ic-delisting-returns"]).no_ic_delisting_returns is True


# ---------------------------------------------------------------------------
# 三消费方共用同一实现（防再分叉）
# ---------------------------------------------------------------------------

def test_three_consumers_share_same_forward_return():
    import research.factor_correlation as fc
    import research.size_neutral_study as sn

    assert fc._forward_return is ev._forward_return
    assert sn._forward_return is ev._forward_return
