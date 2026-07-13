"""concentrated_topk：owner 交易形状的集中 top-K 部署研究（预注册执行引擎）。

预注册 docs/concentrated_topk_hypotheses_2026-07.md（2026-07-13 冻结，先于任何结果提交）。
本脚本是 study（部署检验），不是发现级假设检验；写 trials 台账 study 行，绝不回写事实表。

核心语义（与预注册逐条对应，改动即违规）：
- 信号日 = 每周最后一个 XNYS 交易日收盘；一切成交在**信号后下一个有价交易日收盘**
  （t+1 收盘执行；执行日无价顺延并计数）。
- top-K：因子值降序、并列按 63 日中位美元成交额降序；K=10 主 / K=5 敏感。
- 入场金额 = 执行日组合净值 × 1/K，超出现金按现金全额（不加杠杆）；成本从
  金额内扣（net = gross/(1+cost)），现金永不为负。
- 退出：E0=仅 252 日上限+退市；E1=周频 close<SMA200；E2=日频 Chandelier
  （入场后最高收盘 − 3×ATR22）；E3=E1∨E2。同日并发按 cap > e1 > e2 取优先标签。
- 冷却：卖出执行后，须越过"首个 ≥ 执行日的周度信号日"，此后的信号日才可再入场。
- 成本：逐笔 PIT——执行日的 63 日 cs_spread 滚动中位（min_periods=20）/2×1.5，
  无覆盖 fallback 40bps 单边；退市结算不是交易、不收成本。
- 退市：价格序列在窗口内永久终结（全史最后有价日 ≤ 窗口末 且 < 全史面板末）→
  spell 在最后有价日终结、次日按 last_close×(1+实测 delisting_return，缺省 0%)
  结算入现金；停牌跨窗口末的持仓冻结在末值（计数披露）。
- 现金按 DTB3 日息（actual/360，utils.risk_free_rates 口径）逐日复利。
- 随机基线：同周同池（eligibility ∩ 因子非 NaN ∩ 有价 ∩ 窗口内还有下一个有价日）
  等概率抽 K，N=1000 路径，种子 42 起连续编号，同成本引擎、E0 形态。

非目标：不做参数网格（均线窗/ATR 倍数/止损百分比全部预注册钉死）；不做盘中执行；
不给 E0 FAIL 的选股器用退出规则翻案（引擎层面允许跑，判据层面只作诊断）。
"""
from __future__ import annotations

import argparse
import json
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Callable

import numpy as np
import pandas as pd

from research._trials_store import append_study
from research.backtest import rebalance_dates
from research.progress import Progress

OUTPUT_DIR = Path(__file__).resolve().parent / "output"
PREREG_DOC = "docs/concentrated_topk_hypotheses_2026-07.md"

SELECTORS = ("composite_v2", "f_score", "operating_profitability")
PANEL_START = date(2012, 1, 3)
PHASE_STARTS = (date(2016, 1, 1), date(2016, 4, 1), date(2016, 7, 1), date(2016, 10, 1))
STABILITY_START = date(2013, 7, 1)
STABILITY_END = date(2015, 12, 31)
SUBWINDOW_2021 = date(2021, 1, 1)

K_MAIN = 10
K_SENS = 5
CAP_MAIN = 252
CAP_SENS = 126
FALLBACK_BPS = 40.0
STRESS_MULT = 0.5
COST_MIN_PERIODS = 20
COST_WINDOW = 63
FIXED_COST_TIERS = (10.0, 25.0, 40.0)
N_RANDOM = 1000
RANDOM_SEED0 = 42
EXCHANGE_DROP_SENS = -0.30

MIN_PRICE = 3.0
MIN_MEDIAN_DOLLAR_VOLUME = 2_000_000.0
ELIGIBILITY_WINDOW = 63
SMA_WINDOW = 200
ATR_WINDOW = 22
ATR_MULT = 3.0

# 判据常数（预注册"判据"节）
CRIT_EXCESS_PP = 0.03
CRIT_WORST_PHASE_PP = -0.02
CRIT_RANDOM_PCTILE = 0.90
CRIT_MDD_GUARD_PP = 0.15
E3_MDD_IMPROVE_PP = 0.10
E3_SHARPE_IMPROVE = 0.10
E3_CAGR_LOSS_PP = 0.02

_EXIT_PRIORITY = {"cap": 0, "e1": 1, "e2": 2}


@dataclass(frozen=True)
class ExitConfig:
    name: str
    use_e1: bool
    use_e2: bool


EXIT_VARIANTS: dict[str, ExitConfig] = {
    "e0": ExitConfig("e0", False, False),
    "e1": ExitConfig("e1", True, False),
    "e2": ExitConfig("e2", False, True),
    "e3": ExitConfig("e3", True, True),
}


@dataclass
class SimInputs:
    """纯模拟核心的全部输入（无 DB 依赖，测试直接构造）。

    所有矩阵 shape=(T,N)、行对齐 dates、列对齐 sec_ids。cost_bps 为 None 时全部
    交易走 fallback_bps。sma200/atr22 仅在对应退出启用时必须提供。
    """

    dates: pd.DatetimeIndex
    adj_close: np.ndarray
    weekly_pos: np.ndarray          # 升序信号日行号
    rf: np.ndarray                  # 日简单收益，rf[t] 为 t-1→t
    sec_ids: np.ndarray
    last_priced: np.ndarray         # 每列在本窗口内最后有价行号（无价=-1）
    alive_beyond: np.ndarray        # bool：全史上该列在窗口末之后仍有价（停牌跨末≠退市）
    delist_ret: np.ndarray          # 实测退市收益（NaN=无实测→按 0%）
    cost_bps: np.ndarray | None = None
    fallback_bps: float = FALLBACK_BPS
    sma200: np.ndarray | None = None
    atr22: np.ndarray | None = None


@dataclass(frozen=True)
class SimConfig:
    k: int
    cap_sessions: int
    exit: ExitConfig
    start_week_idx: int


@dataclass
class SimResult:
    spells: pd.DataFrame
    daily: pd.DataFrame             # index=dates[s0:]，列 nav/cash/pos_value
    counters: dict
    start_pos: int


PickFn = Callable[[int, int, set[int]], list[int]]


def _next_priced(col: np.ndarray, pos: int, limit: int) -> int | None:
    """严格大于 pos、不超过 limit 的下一个有价行号。"""
    for p in range(pos + 1, limit + 1):
        if np.isfinite(col[p]):
            return p
    return None


def _ffill_value(col: np.ndarray, pos: int) -> float:
    """pos 处的 ffill 估值（入场价必有价，回扫有界）。"""
    p = pos
    while p >= 0:
        if np.isfinite(col[p]):
            return float(col[p])
        p -= 1
    return float("nan")


def _first_e1_trigger(adj: np.ndarray, sma: np.ndarray, weekly_pos: np.ndarray,
                      entry_exec: int, bound: int) -> int | None:
    """(entry_exec, bound] 内首个 close<SMA200 的周度信号行号；SMA NaN 不触发。"""
    lo = int(np.searchsorted(weekly_pos, entry_exec, side="right"))
    for wi in range(lo, len(weekly_pos)):
        w = int(weekly_pos[wi])
        if w > bound:
            break
        c, s = adj[w], sma[w]
        if np.isfinite(c) and np.isfinite(s) and c < s:
            return w
    return None


def _first_e2_trigger(adj: np.ndarray, atr: np.ndarray, entry_exec: int, bound: int) -> int | None:
    """(entry_exec, bound] 内首个 close < cummax(entry_exec..t) − 3×ATR22 的行号。

    cummax 只滚动有价日；close/ATR 任一 NaN 的日子不触发（停牌无法交易）。
    """
    run_max = adj[entry_exec]
    if not np.isfinite(run_max):  # 防御：入场价必有价
        return None
    for p in range(entry_exec + 1, bound + 1):
        c = adj[p]
        if not np.isfinite(c):
            continue
        if c > run_max:
            run_max = c
        a = atr[p]
        if np.isfinite(a) and c < run_max - ATR_MULT * a:
            return p
    return None


def _resolve_exit(si: SimInputs, cfg: SimConfig, col: int, entry_exec: int
                  ) -> tuple[int | None, int | None, str]:
    """入场即确定的退出三元组 (exit_signal_pos, exit_exec_pos, reason)。

    退出信号只依赖该列价格史与入场行号（与现金/其余持仓无关），故可在入场时
    确定性解析。reason ∈ {cap,e1,e2,delist,open}；open=持有至窗口末（含停牌跨末）。
    """
    T = len(si.dates)
    lastp = int(si.last_priced[col])
    cap_sig = entry_exec + cfg.cap_sessions
    bound = min(cap_sig, lastp)
    adj_col = si.adj_close[:, col]

    cands: list[tuple[int, int, str]] = []
    if cap_sig <= lastp:
        cands.append((cap_sig, _EXIT_PRIORITY["cap"], "cap"))
    if cfg.exit.use_e1:
        p1 = _first_e1_trigger(adj_col, si.sma200[:, col], si.weekly_pos, entry_exec, bound)
        if p1 is not None:
            cands.append((p1, _EXIT_PRIORITY["e1"], "e1"))
    if cfg.exit.use_e2:
        p2 = _first_e2_trigger(adj_col, si.atr22[:, col], entry_exec, bound)
        if p2 is not None:
            cands.append((p2, _EXIT_PRIORITY["e2"], "e2"))

    if cands:
        sig, _, reason = min(cands)
        exec_pos = _next_priced(adj_col, sig, lastp)
        if exec_pos is not None:
            return sig, exec_pos, reason
        # 信号后窗口内再无有价日：落入终局分支

    if lastp < T - 1 and not si.alive_beyond[col]:
        return lastp, lastp + 1, "delist"       # 结算日=最后有价日次日
    return None, None, "open"                    # 活到窗口末（或停牌冻结跨末）


def _cost_frac(si: SimInputs, pos: int, col: int) -> tuple[float, bool]:
    """执行日单边成本（小数）与是否 fallback。"""
    bps = float("nan")
    if si.cost_bps is not None:
        bps = float(si.cost_bps[pos, col])
    if not np.isfinite(bps):
        return si.fallback_bps / 1e4, True
    return bps / 1e4, False


def simulate(si: SimInputs, cfg: SimConfig, pick: PickFn) -> SimResult:
    """事件驱动模拟：周度选股/补位 + 入场即定的退出 + 逐日现金/净值。"""
    T, _ = si.adj_close.shape
    weekly = si.weekly_pos
    s0 = int(weekly[cfg.start_week_idx])
    growth = np.cumprod(1.0 + si.rf)             # C[t]；cash_at 用比值

    flows: list[tuple[int, float]] = [(s0, 1.0)]  # 初始资金在首个信号日收盘就位
    spells: list[dict] = []
    cooldown: dict[int, int] = {}                 # col -> 首个 ≥ exit_exec 的周下标
    counters = {"postponed_buys": 0, "fallback_trades": 0, "trades": 0,
                "skipped_no_cash": 0, "unresolved_delist": 0, "frozen_at_end": 0}

    def cash_at(pos: int) -> float:
        return float(sum(f * growth[pos] / growth[p] for p, f in flows if p <= pos))

    def nav_at(pos: int) -> float:
        total = cash_at(pos)
        for sp in spells:
            end = sp["value_end"]
            if sp["entry_exec"] <= pos and (end is None or pos <= end):
                total += sp["shares"] * _ffill_value(si.adj_close[:, sp["col"]], pos)
        return total

    for wi in range(cfg.start_week_idx, len(weekly)):
        w = int(weekly[wi])
        # 占位：退出信号未决（None）或晚于本信号日的 spell 仍占 slot
        occupied = sum(1 for sp in spells
                       if sp["exit_signal"] is None or sp["exit_signal"] > w)
        n_free = cfg.k - occupied
        if n_free <= 0:
            continue
        blocked: set[int] = set()
        for sp in spells:
            if sp["exit_exec"] is None or sp["exit_exec"] > w:
                blocked.add(sp["col"])            # 仍持有（含在途卖出）
        for col, j0 in cooldown.items():
            if wi <= j0:
                blocked.add(col)                  # 冷却：须越过首个 ≥exit_exec 的信号日

        chosen = pick(wi, n_free, blocked)
        buys: list[tuple[int, int]] = []          # (exec_pos, col)，按执行时序处理
        for col in chosen:
            exec_pos = _next_priced(si.adj_close[:, col], w, int(si.last_priced[col]))
            if exec_pos is None:
                continue
            if exec_pos > w + 1:
                counters["postponed_buys"] += 1
            buys.append((exec_pos, col))
        buys.sort()                               # 现金时序正确性：按执行日排序

        for exec_pos, col in buys:
            nav = nav_at(exec_pos)
            cash = cash_at(exec_pos)
            gross = min(nav / cfg.k, cash)
            if gross <= 1e-12:
                counters["skipped_no_cash"] += 1
                continue
            frac, fb = _cost_frac(si, exec_pos, col)
            counters["fallback_trades"] += int(fb)
            counters["trades"] += 1
            net = gross / (1.0 + frac)
            price = float(si.adj_close[exec_pos, col])
            shares = net / price
            flows.append((exec_pos, -gross))
            sig, xexec, reason = _resolve_exit(si, cfg, col, exec_pos)
            sp = {"col": col, "security_id": int(si.sec_ids[col]),
                  "signal_pos": w, "entry_exec": exec_pos, "entry_price": price,
                  "gross": gross, "entry_cost": gross - net, "shares": shares,
                  "entry_fallback": fb, "exit_signal": sig, "exit_exec": xexec,
                  "reason": reason, "exit_cost": 0.0, "proceeds": np.nan,
                  "exit_fallback": False, "value_end": None}
            # 结算/卖出流与估值窗终点
            if reason == "delist":
                lastp = int(si.last_priced[col])
                ret = si.delist_ret[col]
                if not np.isfinite(ret):
                    ret = 0.0
                    counters["unresolved_delist"] += 1
                last_price = float(si.adj_close[lastp, col])
                proceeds = shares * last_price * (1.0 + float(ret))
                flows.append((xexec, proceeds))   # 结算非交易：零成本
                sp["proceeds"] = proceeds
                sp["value_end"] = lastp
            elif xexec is not None:
                xfrac, xfb = _cost_frac(si, xexec, col)
                counters["fallback_trades"] += int(xfb)
                counters["trades"] += 1
                xprice = float(si.adj_close[xexec, col])
                proceeds = shares * xprice * (1.0 - xfrac)
                flows.append((xexec, proceeds))
                sp.update(proceeds=proceeds, exit_cost=shares * xprice * xfrac,
                          exit_fallback=xfb)
                sp["value_end"] = xexec - 1
            else:
                counters["frozen_at_end"] += int(si.last_priced[col] < T - 1)
                sp["value_end"] = None            # 持有至窗口末
            spells.append(sp)
            if xexec is not None:
                cooldown[col] = int(np.searchsorted(weekly, xexec, side="left"))

    # ---- 逐日 cash / 持仓值 / NAV ----
    flow_by_pos = np.zeros(T)
    for p, f in flows:
        flow_by_pos[p] += f
    cash = np.zeros(T)
    cash[s0] = flow_by_pos[s0]
    for t in range(s0 + 1, T):
        cash[t] = cash[t - 1] * (1.0 + si.rf[t]) + flow_by_pos[t]
    pos_value = np.zeros(T)
    for sp in spells:
        start = sp["entry_exec"]
        stop = sp["value_end"] if sp["value_end"] is not None else T - 1
        if stop < start:
            continue
        seg = pd.Series(si.adj_close[start:stop + 1, sp["col"]]).ffill().to_numpy()
        pos_value[start:stop + 1] += sp["shares"] * seg
        if sp["value_end"] is None:               # 期末标记价即终局收益（无成本）
            sp["proceeds"] = sp["shares"] * float(seg[-1])
    nav = cash + pos_value
    daily = pd.DataFrame({"nav": nav[s0:], "cash": cash[s0:], "pos_value": pos_value[s0:]},
                         index=si.dates[s0:])
    sdf = pd.DataFrame(spells)
    if not sdf.empty:
        # int/None 混合列显式转 float64（parquet 序列化 + 下游算术都吃 NaN 语义）
        for c in ("exit_signal", "exit_exec"):
            sdf[c] = pd.to_numeric(sdf[c], errors="coerce").astype("float64")
        sdf["pnl"] = sdf["proceeds"] - sdf["gross"]
        sdf["holding_sessions"] = (sdf["exit_exec"].fillna(T - 1) - sdf["entry_exec"]).astype("int64")
        sdf["entry_date"] = si.dates[sdf["entry_exec"].to_numpy(dtype=int)]
    return SimResult(spells=sdf, daily=daily, counters=counters, start_pos=s0)


def terminal_excess(si: SimInputs, cfg: SimConfig, pick: PickFn,
                    spy_adj: np.ndarray) -> float:
    """随机路径专用：只算全期年化几何超额（复用 simulate，不另写轻引擎）。"""
    res = simulate(si, cfg, pick)
    nav = res.daily["nav"].to_numpy()
    years = max((len(nav) - 1) / 252.0, 1e-9)
    strat = (nav[-1] / nav[0]) ** (1.0 / years) - 1.0
    spy = (spy_adj[-1] / spy_adj[res.start_pos]) ** (1.0 / years) - 1.0
    return float(strat - spy)


# ---------------------------------------------------------------- metrics ----

def compute_metrics(daily: pd.DataFrame, rf: np.ndarray, spy_adj: np.ndarray,
                    start_pos: int, spells: pd.DataFrame) -> dict:
    nav = daily["nav"].to_numpy()
    n = len(nav) - 1
    years = max(n / 252.0, 1e-9)
    ret = np.diff(nav) / nav[:-1]
    rf_w = rf[start_pos + 1: start_pos + 1 + n]
    spy = spy_adj[start_pos:start_pos + n + 1]
    spy_ret = np.diff(spy) / spy[:-1]
    cagr = (nav[-1] / nav[0]) ** (1.0 / years) - 1.0
    spy_cagr = (spy[-1] / spy[0]) ** (1.0 / years) - 1.0
    ex = ret - rf_w
    sharpe = float(np.mean(ex) / np.std(ex, ddof=1) * np.sqrt(252)) if np.std(ex, ddof=1) > 0 else float("nan")
    spy_ex = spy_ret - rf_w
    spy_sharpe = float(np.mean(spy_ex) / np.std(spy_ex, ddof=1) * np.sqrt(252)) if np.std(spy_ex, ddof=1) > 0 else float("nan")
    downside = ex[ex < 0]
    sortino = float(np.mean(ex) / np.sqrt(np.mean(downside ** 2)) * np.sqrt(252)) if len(downside) else float("nan")
    peak = np.maximum.accumulate(nav)
    mdd = float((nav / peak - 1.0).min())
    spy_peak = np.maximum.accumulate(spy)
    spy_mdd = float((spy / spy_peak - 1.0).min())
    out = {
        "n_sessions": int(n), "years": float(years),
        "cagr": float(cagr), "spy_cagr": float(spy_cagr),
        "excess_annual": float(cagr - spy_cagr),
        "sharpe": sharpe, "spy_sharpe": spy_sharpe, "sortino": sortino,
        "max_drawdown": mdd, "spy_max_drawdown": spy_mdd,
        "terminal_nav": float(nav[-1]),
    }
    if not spells.empty:
        closed = spells[spells["exit_exec"].notna()]
        sold = closed.loc[closed["reason"] != "delist", "proceeds"].fillna(0.0)
        traded = float(spells["gross"].sum() + sold.sum())  # 只计实际成交（结算/期末标记不计）
        out.update({
            "n_spells": int(len(spells)),
            "median_holding_sessions": float(spells["holding_sessions"].median()),
            "win_rate": float((closed["pnl"] > 0).mean()) if len(closed) else float("nan"),
            "total_costs": float(spells["entry_cost"].sum() + spells["exit_cost"].sum()),
            "annual_turnover": float(traded / max(np.mean(nav), 1e-12) / years),
            "max_spell_pnl": float(spells["pnl"].max()),
            "max_spell_security": int(spells.loc[spells["pnl"].idxmax(), "security_id"]),
        })
        # 判据 5：剔最大 spell 的名义 PnL（不追溯再投资复利，预注册文档已声明近似）
        adj_terminal = out["terminal_nav"] - max(out["max_spell_pnl"], 0.0)
        adj_cagr = (max(adj_terminal, 1e-9) / nav[0]) ** (1.0 / years) - 1.0
        out["excess_ex_top_spell"] = float(adj_cagr - spy_cagr)
    else:
        out.update({"n_spells": 0, "excess_ex_top_spell": float(cagr - spy_cagr)})
    return out


def window_excess(daily: pd.DataFrame, spy_adj: np.ndarray, dates: pd.DatetimeIndex,
                  start_pos: int, win_start: pd.Timestamp) -> float:
    """主相位日净值内 [win_start, end] 子段的年化几何超额。"""
    idx = daily.index
    sub = daily.loc[idx >= win_start, "nav"]
    if len(sub) < 2:
        return float("nan")
    p0 = int(dates.get_indexer([sub.index[0]])[0])
    years = max((len(sub) - 1) / 252.0, 1e-9)
    strat = (sub.iloc[-1] / sub.iloc[0]) ** (1.0 / years) - 1.0
    spy = (spy_adj[len(dates) - 1] / spy_adj[p0]) ** (1.0 / years) - 1.0
    return float(strat - spy)


# ---------------------------------------------------------------- verdicts ----

def evaluate_selector_verdict(main: dict, phases: list[float], sub2021: float,
                              random_pctile: float, k5_excess: float) -> dict:
    """预注册判据 1-6（E0/K=10/measured/252）。返回逐条布尔 + PASS。"""
    phase_arr = np.asarray(phases, dtype=float)
    c1 = bool(main["excess_annual"] >= CRIT_EXCESS_PP)
    c2 = bool(np.median(phase_arr) > 0 and phase_arr.min() > CRIT_WORST_PHASE_PP)
    c3 = bool(sub2021 >= 0)
    c4 = bool(random_pctile >= CRIT_RANDOM_PCTILE)
    c5 = bool(main["excess_ex_top_spell"] > 0)
    c6 = bool(np.sign(k5_excess) == np.sign(main["excess_annual"]) and k5_excess != 0)
    passed = all((c1, c2, c3, c4, c5, c6))
    deploy_frozen = bool(main["max_drawdown"] < main["spy_max_drawdown"] - CRIT_MDD_GUARD_PP)
    return {"c1_excess_3pp": c1, "c2_phases": c2, "c3_sub2021": c3,
            "c4_random_p90": c4, "c5_ex_top_spell": c5, "c6_k5_same_sign": c6,
            "pass": passed, "deploy_frozen": deploy_frozen}


def evaluate_exit_verdict(e0: dict, e3: dict, e0_passed: bool) -> dict:
    """退出层判据（E3 vs E0）；E0 FAIL 时 judged=False（诊断，不得翻案）。"""
    mdd_improve = e3["max_drawdown"] - e0["max_drawdown"]      # 两者皆负；≥+0.10 为改善
    sharpe_improve = e3["sharpe"] - e0["sharpe"]
    cagr_loss = e0["cagr"] - e3["cagr"]
    c1 = bool(mdd_improve >= E3_MDD_IMPROVE_PP)
    c2 = bool(sharpe_improve >= E3_SHARPE_IMPROVE)
    c3 = bool(cagr_loss <= E3_CAGR_LOSS_PP)
    return {"judged": bool(e0_passed), "c1_mdd_improve": c1, "c2_sharpe": c2,
            "c3_cagr_loss": c3, "pass": bool(e0_passed and c1 and c2 and c3),
            "mdd_improve": float(mdd_improve), "sharpe_improve": float(sharpe_improve),
            "cagr_loss": float(cagr_loss)}


# ------------------------------------------------------------- data build ----

def build_market_data(engine, *, end: date, ch_url: str | None, prog: Progress) -> dict:
    """装载全部面板并预计算 SMA/ATR/成本/资格。只读，无回写。"""
    from research.data import (
        apply_adjustment, load_adjusted_panel, load_delisting_returns,
        load_factor_events, load_price_long, securities_with_uncovered_events,
    )
    from research.market_regime_overlay_study import (
        SPY_SECURITY_ID, assert_spy_adjustment_coverage,
    )
    from utils.risk_free_rates import load_risk_free_daily_returns

    with prog.stage("复权面板装载"):
        panel = load_adjusted_panel(engine, start=PANEL_START, end=end)
        adj_close, close = panel["adj_close"], panel["close"]
        dollar_volume = panel["dollar_volume"]
    with prog.stage("未覆盖事件 gate"):
        drop = securities_with_uncovered_events(engine, start=PANEL_START, end=end)
        keep = [c for c in adj_close.columns if int(c) not in set(drop)]
        adj_close, close = adj_close[keep], close[keep]
        dollar_volume = dollar_volume[keep]
    dates = adj_close.index
    with prog.stage("SPY 总收益链"):
        assert_spy_adjustment_coverage(engine, start=PANEL_START, end=end)
        events = load_factor_events(engine, as_of=end)
        spy_long = load_price_long(engine, start=PANEL_START, end=end,
                                   types=("ETF",), security_ids=[SPY_SECURITY_ID],
                                   columns=("close",))
        spy_long = apply_adjustment(spy_long, events, as_of=end)
        spy = (spy_long.set_index("date")["adj_close"].reindex(dates).ffill())
        if spy.isna().any():
            raise RuntimeError("SPY 复权序列在研究窗口内有缺口")
    with prog.stage("高低价→ATR22"):
        # load_price_long 白名单无 high/low，走 price_cache 的自由列 COPY 通道；
        # 显式给 security_ids 时该函数不叠类型门（§E.6 语义），keep 已是 CS 过滤后宇宙。
        from research.factors.price_cache import load_price_long_fast
        hl = load_price_long_fast(engine, start=PANEL_START, end=end,
                                  columns="high, low",
                                  security_ids=[int(c) for c in keep])
        from research.data import to_wide
        high_w = to_wide(hl, "high").reindex(index=dates, columns=adj_close.columns)
        low_w = to_wide(hl, "low").reindex(index=dates, columns=adj_close.columns)
        del hl
        factor = (adj_close / close).astype("float64")
        a_high = high_w * factor
        a_low = low_w * factor
        del high_w, low_w, factor
        prev_c = adj_close.shift(1)
        # TR = max(H-L, |H-Cprev|, |L-Cprev|)；np.fmax 跳 NaN（首日退化为 H-L），
        # 单遍 numpy 避免三面板 concat 的 ~1GB 瞬时峰值（253 内存纪律）。
        tr_arr = np.fmax(
            np.fmax((a_high - a_low).to_numpy(), (a_high - prev_c).abs().to_numpy()),
            (a_low - prev_c).abs().to_numpy(),
        )
        tr = pd.DataFrame(tr_arr, index=dates, columns=adj_close.columns)
        del a_high, a_low, prev_c, tr_arr
        atr22 = tr.rolling(ATR_WINDOW, min_periods=ATR_WINDOW).mean().astype("float32")
        del tr
    with prog.stage("SMA200 / 资格 / 并列键"):
        sma200 = adj_close.rolling(SMA_WINDOW, min_periods=SMA_WINDOW).mean().astype("float32")
        med_dv = dollar_volume.rolling(ELIGIBILITY_WINDOW, min_periods=ELIGIBILITY_WINDOW).median()
        eligible = (med_dv >= MIN_MEDIAN_DOLLAR_VOLUME) & (close >= MIN_PRICE)
        med_dv = med_dv.astype("float32")
    with prog.stage("cs_spread 成本面板（ClickHouse）"):
        cost_bps = _load_cost_panel(dates, [int(c) for c in adj_close.columns], ch_url, prog)
    with prog.stage("退市 / 无风险利率"):
        delist_main = load_delisting_returns(engine)
        delist_sens = load_delisting_returns(engine, exchange_drop_fallback=EXCHANGE_DROP_SENS)
        rf = load_risk_free_daily_returns(engine, dates).to_numpy(dtype="float64")
    return {"adj_close": adj_close, "close": close, "eligible": eligible,
            "med_dv": med_dv, "sma200": sma200, "atr22": atr22, "cost_bps": cost_bps,
            "spy": spy.to_numpy(dtype="float64"), "rf": rf, "dates": dates,
            "delist_main": delist_main, "delist_sens": delist_sens}


def _load_cost_panel(dates: pd.DatetimeIndex, ids: list[int], ch_url: str | None,
                     prog: Progress, chunk: int = 1500) -> pd.DataFrame:
    """逐笔 PIT 单边成本面板（bps，float32）：63 日 cs_spread 滚动中位/2×(1+0.5)。"""
    from research.factors.minute_loader import load_minute_feature_panel
    parts: list[pd.DataFrame] = []
    for i in range(0, len(ids), chunk):
        batch = ids[i:i + chunk]
        feat = load_minute_feature_panel(dates, batch, ("cs_spread",),
                                         buffer_days=95, min_bars=100, url=ch_url)
        cs = feat["cs_spread"].reindex(index=dates)
        med = cs.rolling(COST_WINDOW, min_periods=COST_MIN_PERIODS).median()
        parts.append((med / 2.0 * (1.0 + STRESS_MULT) * 1e4).astype("float32"))
        prog.log(f"cs_spread {min(i + chunk, len(ids))}/{len(ids)}")
    out = pd.concat(parts, axis=1)
    return out.reindex(columns=pd.Index(ids))


def make_sim_inputs(md: dict, *, window_end: pd.Timestamp | None,
                    exit_cfg: ExitConfig, cost_mode: str = "measured",
                    fixed_bps: float = FALLBACK_BPS,
                    delist: str = "main") -> SimInputs:
    """从市场数据字典切窗构造 SimInputs（预热面板已全史计算后按行切）。"""
    dates: pd.DatetimeIndex = md["dates"]
    end_i = len(dates) - 1 if window_end is None else int(dates.get_indexer([window_end], method="pad")[0])
    sl = slice(0, end_i + 1)
    adj = md["adj_close"].iloc[sl]
    arr = adj.to_numpy(dtype="float64")
    T, N = arr.shape
    finite = np.isfinite(arr)
    last_priced = np.where(finite.any(axis=0), T - 1 - np.argmax(finite[::-1], axis=0), -1)
    full = md["adj_close"].to_numpy(dtype="float64")
    full_last = np.where(np.isfinite(full).any(axis=0),
                         full.shape[0] - 1 - np.argmax(np.isfinite(full)[::-1], axis=0), -1)
    alive_beyond = full_last > (end_i if end_i < full.shape[0] - 1 else full.shape[0] - 1)
    sec_ids = np.asarray([int(c) for c in adj.columns], dtype="int64")
    delist_series: pd.Series = md["delist_main" if delist == "main" else "delist_sens"]
    dr = delist_series.reindex(sec_ids).to_numpy(dtype="float64")
    weekly = rebalance_dates(adj.index, "W")
    weekly_pos = adj.index.get_indexer(weekly)
    cost = None
    if cost_mode == "measured":
        cost = md["cost_bps"].iloc[sl].to_numpy(dtype="float32")
    fallback = FALLBACK_BPS if cost_mode == "measured" else float(fixed_bps)
    return SimInputs(
        dates=adj.index, adj_close=arr, weekly_pos=np.asarray(weekly_pos, dtype=int),
        rf=md["rf"][sl], sec_ids=sec_ids, last_priced=last_priced,
        alive_beyond=np.asarray(alive_beyond, dtype=bool), delist_ret=dr,
        cost_bps=cost, fallback_bps=fallback,
        sma200=md["sma200"].iloc[sl].to_numpy(dtype="float32") if exit_cfg.use_e1 else None,
        atr22=md["atr22"].iloc[sl].to_numpy(dtype="float32") if exit_cfg.use_e2 else None,
    )


def build_week_candidates(md: dict, factor: pd.DataFrame, si: SimInputs) -> tuple[list[np.ndarray], list[np.ndarray]]:
    """每周 (排序候选, 无序池)。池 = eligible ∩ 因子非 NaN ∩ 当日有价 ∩ 窗口内有后续有价日。"""
    dates = si.dates
    fac = factor.reindex(index=dates, columns=[int(c) for c in md["adj_close"].columns])
    fac_arr = fac.to_numpy(dtype="float64")
    elig = md["eligible"].reindex(index=dates).to_numpy(dtype=bool)
    dv = md["med_dv"].reindex(index=dates).to_numpy(dtype="float32")
    ranked: list[np.ndarray] = []
    pools: list[np.ndarray] = []
    for w in si.weekly_pos:
        mask = (elig[w] & np.isfinite(fac_arr[w]) & np.isfinite(si.adj_close[w])
                & (si.last_priced > w))
        cols = np.flatnonzero(mask)
        pools.append(cols)
        if len(cols):
            order = np.lexsort((-np.nan_to_num(dv[w, cols], nan=-1.0), -fac_arr[w, cols]))
            ranked.append(cols[order])
        else:
            ranked.append(cols)
    return ranked, pools


def make_ranked_pick(ranked: list[np.ndarray]) -> PickFn:
    def pick(wi: int, n: int, blocked: set[int]) -> list[int]:
        out: list[int] = []
        for col in ranked[wi]:
            if int(col) not in blocked:
                out.append(int(col))
                if len(out) == n:
                    break
        return out
    return pick


def make_random_pick(pools: list[np.ndarray], seed: int) -> PickFn:
    rng = np.random.default_rng(seed)
    def pick(wi: int, n: int, blocked: set[int]) -> list[int]:
        pool = pools[wi]
        if blocked:
            pool = pool[~np.isin(pool, list(blocked))]
        if len(pool) == 0:
            return []
        take = min(n, len(pool))
        return [int(x) for x in rng.choice(pool, size=take, replace=False)]
    return pick


def first_week_on_or_after(si: SimInputs, day: date) -> int:
    ts = pd.Timestamp(day)
    for i, w in enumerate(si.weekly_pos):
        if si.dates[int(w)] >= ts:
            return i
    raise ValueError(f"no weekly signal on/after {day}")


# ---------------------------------------------------------------- driver ----

def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="concentrated_topk 部署研究（预注册引擎）")
    p.add_argument("--selectors", default=",".join(SELECTORS))
    p.add_argument("--end", type=date.fromisoformat, default=None,
                   help="窗口末（默认面板最新完整交易日）")
    p.add_argument("--n-random", type=int, default=N_RANDOM)
    p.add_argument("--no-study", action="store_true", help="不写 trials study 行（调试）")
    p.add_argument("--ch-url", default=None, help="ClickHouse HTTP URL 覆盖")
    p.add_argument("--output-dir", type=Path, default=OUTPUT_DIR)
    return p.parse_args(argv)


def run_selector(md: dict, selector: str, *, n_random: int, output_dir: Path,
                 write_study: bool, prog: Progress) -> dict:
    """一个选股器的全套预注册运行 → 报告 dict（含判据与审计）。"""
    from research.factors.protocol import FactorContext, get
    # 注册副作用：composite_v2 的成分（low_vol/high_52w/size/OP）在各自模块注册
    import research.factors.builtins.classic_pillars  # noqa: F401 low_vol/high_52w
    import research.factors.builtins.size  # noqa: F401
    import research.factors.builtins.operating_profitability  # noqa: F401
    import research.factors.builtins.composite_v2  # noqa: F401
    import research.factors.builtins.f_score  # noqa: F401

    dates: pd.DatetimeIndex = md["dates"]
    with prog.stage(f"{selector} 因子面板"):
        factor_obj = get(selector)
        ctx = FactorContext(engine=md["engine"], dates=dates,
                            security_universe=pd.Index([int(c) for c in md["adj_close"].columns]),
                            as_of=dates[-1])
        factor = factor_obj.compute(ctx)

    variants: dict[str, dict] = {}
    spy = md["spy"]

    def run_one(tag: str, *, window_end=None, exit_name="e0", k=K_MAIN, cap=CAP_MAIN,
                cost_mode="measured", fixed_bps=FALLBACK_BPS, delist="main",
                phase_start: date = PHASE_STARTS[0], keep_daily=False) -> dict:
        cfg_exit = EXIT_VARIANTS[exit_name]
        si = make_sim_inputs(md, window_end=window_end, exit_cfg=cfg_exit,
                             cost_mode=cost_mode, fixed_bps=fixed_bps, delist=delist)
        ranked, _ = build_week_candidates(md, factor, si)
        wi = first_week_on_or_after(si, phase_start)
        cfg = SimConfig(k=k, cap_sessions=cap, exit=cfg_exit, start_week_idx=wi)
        res = simulate(si, cfg, make_ranked_pick(ranked))
        spy_arr = spy[:len(si.dates)]
        m = compute_metrics(res.daily, si.rf, spy_arr, res.start_pos, res.spells)
        m["counters"] = res.counters
        entry = {"metrics": m, "config": {"exit": exit_name, "k": k, "cap": cap,
                                          "cost_mode": cost_mode, "fixed_bps": fixed_bps,
                                          "delist": delist, "phase_start": str(phase_start)}}
        if keep_daily:
            entry["_res"] = res
            entry["_si_dates"] = si.dates
            entry["_spy"] = spy_arr
        variants[tag] = entry
        prog.log(f"{selector}/{tag} excess={m['excess_annual']:+.4f} mdd={m['max_drawdown']:.3f}")
        return entry

    with prog.stage(f"{selector} 主相位 4 退出"):
        for ex in EXIT_VARIANTS:
            run_one(f"main_{ex}", exit_name=ex, keep_daily=(ex in ("e0", "e3")))
    with prog.stage(f"{selector} 相位腿"):
        for ph in PHASE_STARTS[1:]:
            run_one(f"phase_{ph.strftime('%Y%m')}_e0", phase_start=ph)
    with prog.stage(f"{selector} 敏感性腿"):
        run_one("k5_e0", k=K_SENS)
        run_one("cap126_e0", cap=CAP_SENS)
        run_one("cap126_e3", cap=CAP_SENS, exit_name="e3")
        for bps in FIXED_COST_TIERS:
            run_one(f"fixed{int(bps)}_e0", cost_mode="fixed", fixed_bps=bps)
        run_one("delist_sens_e0", delist="sens")
    with prog.stage(f"{selector} 稳定腿 2013H2-2015"):
        run_one("stability_e0", window_end=pd.Timestamp(STABILITY_END),
                phase_start=STABILITY_START)

    with prog.stage(f"{selector} 随机基线 {n_random} 路径"):
        cfg_exit = EXIT_VARIANTS["e0"]
        si = make_sim_inputs(md, window_end=None, exit_cfg=cfg_exit)
        _, pools = build_week_candidates(md, factor, si)
        wi = first_week_on_or_after(si, PHASE_STARTS[0])
        cfg = SimConfig(k=K_MAIN, cap_sessions=CAP_MAIN, exit=cfg_exit, start_week_idx=wi)
        rand_excess = np.empty(n_random)
        for j in range(n_random):
            rand_excess[j] = terminal_excess(si, cfg, make_random_pick(pools, RANDOM_SEED0 + j),
                                             spy[:len(si.dates)])
            if (j + 1) % max(1, n_random // 20) == 0:
                prog.log(f"random {j + 1}/{n_random}")

    main_e0 = variants["main_e0"]["metrics"]
    phases = [main_e0["excess_annual"]] + [
        variants[f"phase_{ph.strftime('%Y%m')}_e0"]["metrics"]["excess_annual"]
        for ph in PHASE_STARTS[1:]]
    res0: SimResult = variants["main_e0"]["_res"]
    sub2021 = window_excess(res0.daily, variants["main_e0"]["_spy"],
                            variants["main_e0"]["_si_dates"], res0.start_pos,
                            pd.Timestamp(SUBWINDOW_2021))
    random_pctile = float(np.mean(rand_excess <= main_e0["excess_annual"]))
    verdict = evaluate_selector_verdict(main_e0, phases, sub2021, random_pctile,
                                        variants["k5_e0"]["metrics"]["excess_annual"])
    exit_verdict = evaluate_exit_verdict(main_e0, variants["main_e3"]["metrics"],
                                         verdict["pass"])

    # ---- 产物落盘 + 独立复算审计 ----
    eval_start = str(res0.daily.index[0].date())
    eval_end = str(res0.daily.index[-1].date())
    stem = f"concentrated_topk_{selector}_{eval_start}_{eval_end}"
    output_dir.mkdir(parents=True, exist_ok=True)
    daily_path = output_dir / f"{stem}_daily.parquet"
    spells_path = output_dir / f"{stem}_spells.parquet"
    pd.concat({"e0": res0.daily, "e3": variants["main_e3"]["_res"].daily},
              names=["variant"]).to_parquet(daily_path)
    pd.concat({"e0": res0.spells, "e3": variants["main_e3"]["_res"].spells},
              names=["variant"]).drop(columns=["value_end"]).to_parquet(spells_path)
    rand_path = output_dir / f"{stem}_random.parquet"
    pd.DataFrame({"excess_annual": rand_excess}).to_parquet(rand_path)
    audit = _independent_audit(daily_path, spells_path, md["rf"], variants)
    report = {
        "prereg": PREREG_DOC, "selector": selector,
        "eval_start": eval_start, "eval_end": eval_end,
        "variants": {k: {"metrics": v["metrics"], "config": v["config"]}
                     for k, v in variants.items()},
        "phases_excess": phases, "sub2021_excess": sub2021,
        "random": {"n": int(n_random), "pctile_of_strategy": random_pctile,
                   "quantiles": {q: float(np.quantile(rand_excess, float(q)))
                                 for q in ("0.1", "0.5", "0.9", "0.95")}},
        "verdict_e0": verdict, "verdict_e3": exit_verdict, "audit": audit,
        "artifacts": {"daily": str(daily_path), "spells": str(spells_path),
                      "random": str(rand_path)},
    }
    json_path = output_dir / f"{stem}.json"
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str),
                         encoding="utf-8")
    _write_md(output_dir / f"{stem}.md", report)
    if write_study and audit["ok"]:
        _append_study_rows(report)
    elif write_study:
        prog.log(f"{selector} 审计未过（degraded），拒绝写 study 行")
    return report


def _independent_audit(daily_path: Path, spells_path: Path, rf: np.ndarray,
                       variants: dict) -> dict:
    """从落盘产物复算 CAGR/Sharpe/MDD + NAV/成本恒等式。误差>1e-8 → degraded。"""
    daily = pd.read_parquet(daily_path)
    spells = pd.read_parquet(spells_path)
    errs: dict[str, float] = {}
    for tag in ("e0", "e3"):
        d = daily.loc[tag]
        m = variants[f"main_{tag}"]["metrics"]
        nav = d["nav"].to_numpy()
        years = max((len(nav) - 1) / 252.0, 1e-9)
        errs[f"{tag}_cagr"] = abs(((nav[-1] / nav[0]) ** (1 / years) - 1) - m["cagr"])
        peak = np.maximum.accumulate(nav)
        errs[f"{tag}_mdd"] = abs(float((nav / peak - 1).min()) - m["max_drawdown"])
        errs[f"{tag}_nav_identity"] = float(
            np.abs(d["nav"] - d["cash"] - d["pos_value"]).max())
        sp = spells.loc[tag]
        errs[f"{tag}_cost_identity"] = abs(
            float(sp["entry_cost"].sum() + sp["exit_cost"].sum()) - m["total_costs"])
    ok = all(v <= 1e-8 for v in errs.values())
    return {"ok": bool(ok), "max_error": float(max(errs.values())), "errors": errs}


def _append_study_rows(report: dict) -> None:
    sel = report["selector"]
    base_params = {
        "prereg": PREREG_DOC, "k": K_MAIN, "cap": CAP_MAIN, "cost_mode": "measured",
        "fallback_bps": FALLBACK_BPS, "stress_mult": STRESS_MULT,
        "n_random": report["random"]["n"], "seed0": RANDOM_SEED0,
    }
    v0, v3 = report["verdict_e0"], report["verdict_e3"]
    m0 = report["variants"]["main_e0"]["metrics"]
    m3 = report["variants"]["main_e3"]["metrics"]
    append_study(
        study="concentrated_topk", factor_name=sel,
        verdict=bool(v0["pass"]),
        criteria="excess>=3pp & phases & sub2021>=0 & random p90 & ex-top-spell & k5 sign",
        params={**base_params, "leg": "selector_e0",
                "deploy_frozen": v0["deploy_frozen"]},
        eval_start=date.fromisoformat(report["eval_start"]),
        eval_end=date.fromisoformat(report["eval_end"]),
        report_path=report["artifacts"]["daily"],
        criterion_values={
            "excess_annual": m0["excess_annual"],
            "phase_median": float(np.median(report["phases_excess"])),
            "phase_worst": float(np.min(report["phases_excess"])),
            "sub2021_excess": report["sub2021_excess"],
            "random_pctile": report["random"]["pctile_of_strategy"],
            "excess_ex_top_spell": m0["excess_ex_top_spell"],
            "max_drawdown": m0["max_drawdown"],
        })
    append_study(
        study="concentrated_topk", factor_name=f"{sel}__exit_e3",
        verdict=bool(v3["pass"]),
        criteria="judged(E0 PASS) & mdd_improve>=10pp & sharpe+0.10 & cagr_loss<=2pp",
        params={**base_params, "leg": "exit_e3_vs_e0", "judged": v3["judged"]},
        eval_start=date.fromisoformat(report["eval_start"]),
        eval_end=date.fromisoformat(report["eval_end"]),
        report_path=report["artifacts"]["daily"],
        criterion_values={
            "mdd_improve": v3["mdd_improve"], "sharpe_improve": v3["sharpe_improve"],
            "cagr_loss": v3["cagr_loss"], "e3_excess_annual": m3["excess_annual"],
        })


def _write_md(path: Path, report: dict) -> None:
    lines = [f"# concentrated_topk：{report['selector']}",
             "", f"预注册：{report['prereg']}；窗口 {report['eval_start']} ~ {report['eval_end']}。", "",
             "| variant | excess/yr | cagr | sharpe | mdd | spells | med_hold | win% |",
             "| --- | --- | --- | --- | --- | --- | --- | --- |"]
    for tag, v in report["variants"].items():
        m = v["metrics"]
        lines.append(
            f"| {tag} | {m['excess_annual']:+.4f} | {m['cagr']:.4f} | {m.get('sharpe', float('nan')):.3f} "
            f"| {m['max_drawdown']:.3f} | {m.get('n_spells', 0)} "
            f"| {m.get('median_holding_sessions', float('nan')):.0f} | {m.get('win_rate', float('nan')):.2f} |")
    v0, v3, rnd = report["verdict_e0"], report["verdict_e3"], report["random"]
    lines += ["", f"随机基线：策略超额位于 {rnd['pctile_of_strategy']:.3f} 分位（n={rnd['n']}）；"
                  f"随机分位数 {rnd['quantiles']}", "",
              f"**E0 判据**：{v0}", "", f"**E3 vs E0**：{v3}", "",
              f"审计：{report['audit']['ok']}（max_err={report['audit']['max_error']:.2e}）"]
    path.write_text("\n".join(lines), encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    from dotenv import load_dotenv

    from research.data import research_engine
    load_dotenv(Path(__file__).resolve().parents[1] / ".env")  # systemd-run 洗净环境下 .env 是唯一连库配置
    args = parse_args(argv)
    prog = Progress("concentrated_topk", warn_gb=5.0)
    try:
        engine = research_engine()
        with prog.stage("市场数据装载"):
            end = args.end
            if end is None:
                end = pd.read_sql_query(
                    "select max(date) as d from daily_prices", engine)["d"].iloc[0]
                end = pd.Timestamp(end).date()
            md = build_market_data(engine, end=end, ch_url=args.ch_url, prog=prog)
            md["engine"] = engine
        for sel in [s.strip() for s in args.selectors.split(",") if s.strip()]:
            if sel not in SELECTORS:
                raise ValueError(f"未注册的选股器 {sel!r}（预注册只允许 {SELECTORS}）")
            run_selector(md, sel, n_random=args.n_random, output_dir=args.output_dir,
                         write_study=not args.no_study, prog=prog)
        return 0
    except Exception as e:  # noqa: BLE001
        from loguru import logger
        logger.opt(exception=e).error("concentrated_topk failed")
        return 1
    finally:
        prog.done()


if __name__ == "__main__":
    raise SystemExit(main())
