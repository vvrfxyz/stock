"""wave 17 same_month_seasonality：同日历月季节性（KLN 2016 稳健口径）。

预注册 docs/wave17_same_month_seasonality_hypotheses.md（2026-07-13 冻结，先于任何结果）。
study 写 trials 台账 calendar_technical 行；research/ 只读铁律不变。

核心口径（与预注册逐条对应）：
- 信号 same_month_1_10：持有月 m 的信号 = lag 1..10 年同日历月收益均值，min 3 观测。
- 月网格：复权日收盘 resample('ME').last() → 月收益；末尾不完整月剔除。
- momentum_12_1（月网格版）= P[m−2]/P[m−13] − 1（跳过最近整月，与 builtin 语义一致）。
- 资格：形成日（m−1 月最后交易日）原始收盘 ≥$3 且 63 日中位美元成交额 ≥$2M；
  当月有效横截面 <300 剔月计数。
- H1 逐月 Spearman IC；H2 信号秩对动量秩逐月 OLS 残差 → partial IC；
- H3 五分位 q5−q1 等权 LS 月频再平衡，成本 25bps 单边 × Σ|Δw|（漂移后权重差），
  披露 10/40bps；持有月内退市（整月无价）贡献 0 并全额换手。
- 判据：H1 t≥3 / H2 t≥2 / H3 净 t≥2 / H4 稳定腿 IC>0，全过才 PASS。

全部矩阵向量化（逐月循环只在 ~250 月的月网格上，符合"百量级再平衡行"豁免）。
"""
from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

from research._trials_store import append_study
from research.evaluate import _newey_west_t, default_nw_lag
from research.progress import Progress

OUTPUT_DIR = Path(__file__).resolve().parent / "output"
PREREG_DOC = "docs/wave17_same_month_seasonality_hypotheses.md"

PANEL_START = date(2003, 1, 2)
PRIMARY_START = pd.Period("2016-01", "M")
STABILITY_START = pd.Period("2007-01", "M")
STABILITY_END = pd.Period("2015-12", "M")

LAG_YEARS = tuple(range(1, 11))
MIN_LAG_OBS = 3
MIN_CROSS_SECTION = 300
MIN_PRICE = 3.0
MIN_MEDIAN_DOLLAR_VOLUME = 2_000_000.0
ELIGIBILITY_WINDOW = 63
N_QUANTILES = 5
COST_TIERS_BPS = (10.0, 25.0, 40.0)
MAIN_COST_BPS = 25.0

CRIT_H1_T = 3.0
CRIT_H2_T = 2.0
CRIT_H3_T = 2.0


# ------------------------------------------------------------------ math ----

def rank_pct(a: np.ndarray) -> np.ndarray:
    """行内百分位秩（NaN 保持 NaN；<2 个有效值全 NaN）。a: 1D。"""
    out = np.full(a.shape, np.nan)
    m = np.isfinite(a)
    n = int(m.sum())
    if n < 2:
        return out
    order = np.argsort(a[m], kind="stable")
    ranks = np.empty(n, dtype="float64")
    ranks[order] = np.arange(1, n + 1)
    # 并列取平均秩（与 scipy rankdata 'average' 一致）
    vals = a[m]
    sorted_vals = vals[order]
    ties = np.concatenate(([True], sorted_vals[1:] != sorted_vals[:-1]))
    group_id = np.cumsum(ties) - 1
    sums = np.bincount(group_id, weights=np.arange(1, n + 1))
    counts = np.bincount(group_id)
    avg = sums / counts
    ranks[order] = avg[group_id]
    out[m] = ranks / n
    return out


def spearman_ic(sig_row: np.ndarray, ret_row: np.ndarray) -> tuple[float, int]:
    """单月 Spearman rank IC（秩后 Pearson），返回 (ic, n)。"""
    m = np.isfinite(sig_row) & np.isfinite(ret_row)
    n = int(m.sum())
    if n < 3:
        return float("nan"), n
    rs = rank_pct(np.where(m, sig_row, np.nan))
    rr = rank_pct(np.where(m, ret_row, np.nan))
    a, b = rs[m], rr[m]
    sa, sb = a.std(), b.std()
    if sa == 0 or sb == 0:
        return float("nan"), n
    return float(np.corrcoef(a, b)[0, 1]), n


def residual_rank(sig_row: np.ndarray, ctrl_row: np.ndarray) -> np.ndarray:
    """信号秩对控制秩的横截面 OLS 残差（两者都有效的格子；其余 NaN）。"""
    m = np.isfinite(sig_row) & np.isfinite(ctrl_row)
    out = np.full(sig_row.shape, np.nan)
    if int(m.sum()) < 3:
        return out
    rs = rank_pct(np.where(m, sig_row, np.nan))[m]
    rc = rank_pct(np.where(m, ctrl_row, np.nan))[m]
    cd = rc - rc.mean()
    denom = float(cd @ cd)
    beta = float(cd @ (rs - rs.mean())) / denom if denom > 0 else 0.0
    out[m] = (rs - rs.mean()) - beta * cd
    return out


def quantile_ls_weights(sig_row: np.ndarray, n_q: int) -> np.ndarray:
    """q5−q1 等权多空权重（多腿 +1/n5，空腿 −1/n1；其余 0；无效格 0）。"""
    w = np.zeros(sig_row.shape)
    m = np.isfinite(sig_row)
    n = int(m.sum())
    if n < n_q:
        return w
    r = rank_pct(np.where(m, sig_row, np.nan))
    q_hi = r > 1.0 - 1.0 / n_q
    q_lo = r <= 1.0 / n_q
    n_hi, n_lo = int(np.nansum(q_hi)), int(np.nansum(q_lo))
    if n_hi == 0 or n_lo == 0:
        return w
    w[np.where(q_hi & m)] = 1.0 / n_hi
    w[np.where(q_lo & m)] = -1.0 / n_lo
    return w


@dataclass
class MonthlyResult:
    months: pd.PeriodIndex
    ic: np.ndarray
    partial_ic: np.ndarray
    ls_gross: np.ndarray
    turnover: np.ndarray          # Σ|Δw|（相对上月漂移后权重）
    n_stocks: np.ndarray
    skipped_thin: int


def run_monthly_engine(sig: np.ndarray, mom: np.ndarray, ret: np.ndarray,
                       eligible: np.ndarray, months: pd.PeriodIndex) -> MonthlyResult:
    """T_months × N 面板 → 逐月 IC / partial IC / LS 毛收益 / 换手。

    sig/mom 在行 t 是持有月 t 的信号（已对齐、已含滞后）；ret[t] 是持有月收益；
    eligible[t] 是形成日资格。月内退市（ret NaN 但曾持有）按 0% 处理在**收益侧**：
    LS 收益 = Σ w·ret（NaN→0），下月权重重算时该股自然出场（全额换手）。
    """
    T, _ = sig.shape
    ic = np.full(T, np.nan)
    pic = np.full(T, np.nan)
    gross = np.full(T, np.nan)
    tov = np.full(T, np.nan)
    n_st = np.zeros(T, dtype=int)
    skipped = 0
    prev_w_drifted: np.ndarray | None = None
    for t in range(T):
        s = np.where(eligible[t], sig[t], np.nan)
        c = np.where(eligible[t], mom[t], np.nan)
        r = ret[t]
        n = int((np.isfinite(s) & np.isfinite(r)).sum())
        n_st[t] = n
        if n < MIN_CROSS_SECTION:
            skipped += 1
            prev_w_drifted = None      # 断月后重启（保守：全额换手计入下一有效月）
            continue
        ic[t], _ = spearman_ic(s, r)
        resid = residual_rank(s, c)
        pic[t], _ = spearman_ic(resid, r)
        w = quantile_ls_weights(s, N_QUANTILES)
        r0 = np.where(np.isfinite(r), r, 0.0)  # 退市月贡献 0%
        gross[t] = float(w @ r0)
        if prev_w_drifted is None:
            tov[t] = float(np.abs(w).sum())    # 建仓月：全额
        else:
            tov[t] = float(np.abs(w - prev_w_drifted).sum())
        drifted = w * (1.0 + r0)
        gs = float(np.abs(drifted).sum())
        prev_w_drifted = drifted / gs * float(np.abs(w).sum()) if gs > 0 else None
    return MonthlyResult(months=months, ic=ic, partial_ic=pic, ls_gross=gross,
                         turnover=tov, n_stocks=n_st, skipped_thin=skipped)


def build_signal_panels(monthly_ret: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
    """月收益宽表 → (same_month_1_10 信号, momentum_12_1) 对齐持有月网格。

    行 t 是持有月：信号由 t−12k（k=1..10）行均值构成（min 3）；动量 =
    Π(1+ret[t−12..t−2]) − 1（跳过 t−1 整月——形成时 t−1 已完结、t 未开始）。
    """
    arr = monthly_ret.to_numpy(dtype="float64")
    T, N = arr.shape
    sig = np.full((T, N), np.nan)
    stack = np.full((len(LAG_YEARS), N), np.nan)
    for t in range(T):
        stack[:] = np.nan
        for i, k in enumerate(LAG_YEARS):
            j = t - 12 * k
            if j >= 0:
                stack[i] = arr[j]
        finite = np.isfinite(stack)
        cnt = finite.sum(axis=0)
        total = np.where(finite, stack, 0.0).sum(axis=0)
        with np.errstate(invalid="ignore", divide="ignore"):
            mean = np.where(cnt > 0, total / cnt, np.nan)
        sig[t] = np.where(cnt >= MIN_LAG_OBS, mean, np.nan)
    log1p = np.log1p(np.where(np.isfinite(arr), arr, np.nan))
    mom = np.full((T, N), np.nan)
    for t in range(T):
        lo, hi = t - 12, t - 1          # 行 t−12..t−2（切片右开）
        if lo < 0:
            continue
        window = log1p[lo:hi]
        cnt = np.isfinite(window).sum(axis=0)
        mom_t = np.expm1(np.nansum(np.where(np.isfinite(window), window, 0.0), axis=0))
        mom[t] = np.where(cnt == 11, mom_t, np.nan)   # 11 个完整月缺一不可
    return sig, mom


def stats_block(series: np.ndarray, months: pd.PeriodIndex, mask: np.ndarray) -> dict:
    x = series[mask]
    x = x[np.isfinite(x)]
    n = len(x)
    if n < 24:
        return {"mean": float("nan"), "nw_t": float("nan"), "n": n}
    lag = default_nw_lag(1, n)
    return {"mean": float(x.mean()), "nw_t": float(_newey_west_t(pd.Series(x), lag)),
            "n": n, "nw_lag": lag}


def evaluate_verdict(h1: dict, h2: dict, h3: dict, h4_mean: float) -> dict:
    c1 = bool(h1["mean"] > 0 and h1["nw_t"] >= CRIT_H1_T)
    c2 = bool(h2["mean"] > 0 and h2["nw_t"] >= CRIT_H2_T)
    c3 = bool(h3["mean"] > 0 and h3["nw_t"] >= CRIT_H3_T)
    c4 = bool(np.isfinite(h4_mean) and h4_mean > 0)
    return {"h1_ic": c1, "h2_partial": c2, "h3_net_ls": c3, "h4_stability": c4,
            "pass": bool(c1 and c2 and c3 and c4)}


# ------------------------------------------------------------- data build ----

def build_panels(engine, end: date, prog: Progress) -> dict:
    from research.data import load_adjusted_panel, securities_with_uncovered_events
    with prog.stage("复权面板装载"):
        panel = load_adjusted_panel(engine, start=PANEL_START, end=end)
        adj_close, close = panel["adj_close"], panel["close"]
        dollar_volume = panel["dollar_volume"]
    with prog.stage("未覆盖事件 gate"):
        drop = set(securities_with_uncovered_events(engine, start=PANEL_START, end=end))
        keep = [c for c in adj_close.columns if int(c) not in drop]
        adj_close, close, dollar_volume = adj_close[keep], close[keep], dollar_volume[keep]
    with prog.stage("月网格与资格"):
        month_end_price = adj_close.resample("ME").last()
        # 末尾不完整月剔除：最后价格日不是该月最后交易日的近似——用日历判断
        last_day = adj_close.index[-1]
        if (last_day + pd.offsets.BDay(1)).month == last_day.month:
            month_end_price = month_end_price.iloc[:-1]
        monthly_ret = month_end_price / month_end_price.shift(1) - 1.0
        med_dv = dollar_volume.rolling(ELIGIBILITY_WINDOW, min_periods=ELIGIBILITY_WINDOW).median()
        elig_daily = (med_dv >= MIN_MEDIAN_DOLLAR_VOLUME) & (close >= MIN_PRICE)
        elig_form = elig_daily.resample("ME").last()  # 形成日=月末交易日的资格
        elig_form = elig_form.reindex(month_end_price.index).shift(1)  # 持有月 m 用 m−1 月末
        elig = elig_form.fillna(False).to_numpy(dtype=bool)
    months = pd.PeriodIndex(monthly_ret.index, freq="M")
    return {"monthly_ret": monthly_ret, "eligible": elig, "months": months}


# ----------------------------------------------------------------- driver ----

def run_study(engine, *, end: date, output_dir: Path, write_study: bool,
              prog: Progress) -> dict:
    data = build_panels(engine, end, prog)
    monthly_ret: pd.DataFrame = data["monthly_ret"]
    months: pd.PeriodIndex = data["months"]
    with prog.stage("信号与动量面板"):
        sig, mom = build_signal_panels(monthly_ret)
    with prog.stage("逐月引擎"):
        res = run_monthly_engine(sig, mom, monthly_ret.to_numpy(dtype="float64"),
                                 data["eligible"], months)
    primary = np.asarray(months >= PRIMARY_START)
    stability = np.asarray((months >= STABILITY_START) & (months <= STABILITY_END))

    h1 = stats_block(res.ic, months, primary)
    h2 = stats_block(res.partial_ic, months, primary)
    nets = {}
    for bps in COST_TIERS_BPS:
        net = res.ls_gross - res.turnover * bps / 1e4
        nets[f"{bps:g}bps"] = stats_block(net, months, primary)
    h3 = nets[f"{MAIN_COST_BPS:g}bps"]
    h4 = stats_block(res.ic, months, stability)
    verdict = evaluate_verdict(h1, h2, h3, h4["mean"])

    monthly = pd.DataFrame({
        "month": months.astype(str), "ic": res.ic, "partial_ic": res.partial_ic,
        "ls_gross": res.ls_gross, "turnover": res.turnover, "n_stocks": res.n_stocks,
    }).set_index("month")
    monthly["ls_net_25bps"] = res.ls_gross - res.turnover * MAIN_COST_BPS / 1e4
    eval_start = str(months[np.argmax(primary)]) if primary.any() else "NA"
    stem = f"wave17_same_month_{eval_start}_{months[-1]}"
    output_dir.mkdir(parents=True, exist_ok=True)
    monthly_path = output_dir / f"{stem}_monthly.parquet"
    monthly.to_parquet(monthly_path)

    # 披露：逐年 IC / 1月 vs 非1月
    year = months.year.to_numpy()
    ic_series = pd.Series(res.ic)
    yearly_ic = {int(y): round(float(np.nanmean(res.ic[(year == y) & primary])), 4)
                 for y in sorted(set(year[primary])) if np.isfinite(res.ic[(year == y) & primary]).any()}
    jan = months.month.to_numpy() == 1
    disclosures = {
        "yearly_ic_primary": yearly_ic,
        "jan_ic_primary": float(np.nanmean(res.ic[primary & jan])),
        "nonjan_ic_primary": float(np.nanmean(res.ic[primary & ~jan])),
        "mean_turnover_primary": float(np.nanmean(res.turnover[primary])),
        "skipped_thin_months": int(res.skipped_thin),
        "gross_ls_primary": stats_block(res.ls_gross, months, primary),
        "net_tiers_primary": nets,
        "stability_h4": h4,
    }
    report = {"prereg": PREREG_DOC, "signal": "same_month_1_10",
              "eval_start": eval_start, "eval_end": str(months[-1]),
              "h1_ic": h1, "h2_partial_ic": h2, "h3_net_ls_25bps": h3,
              "h4_stability_ic_mean": h4["mean"], "verdict": verdict,
              "disclosures": disclosures,
              "artifacts": {"monthly": str(monthly_path)}}
    audit = _independent_audit(monthly_path, report, months, primary)
    report["audit"] = audit
    (output_dir / f"{stem}.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    _write_md(output_dir / f"{stem}.md", report)
    prog.log(f"H1 t={h1['nw_t']:+.2f} H2 t={h2['nw_t']:+.2f} "
             f"H3 t={h3['nw_t']:+.2f} H4 mean={h4['mean']:+.4f} pass={verdict['pass']}")
    if write_study and audit["ok"]:
        _append_study_row(report, months)
    elif write_study:
        prog.log("审计未过（degraded），拒绝写 study 行")
    return report


def _independent_audit(monthly_path: Path, report: dict, months: pd.PeriodIndex,
                       primary: np.ndarray) -> dict:
    df = pd.read_parquet(monthly_path)
    errs: dict[str, float] = {}
    pm = pd.PeriodIndex(df.index, freq="M") >= PRIMARY_START
    ic = df["ic"].to_numpy()[pm]
    ic = ic[np.isfinite(ic)]
    errs["h1_mean"] = abs(float(ic.mean()) - report["h1_ic"]["mean"])
    errs["h1_t"] = abs(float(_newey_west_t(pd.Series(ic), report["h1_ic"]["nw_lag"]))
                       - report["h1_ic"]["nw_t"])
    net = (df["ls_gross"] - df["turnover"] * MAIN_COST_BPS / 1e4).to_numpy()[pm]
    errs["net_identity"] = float(np.nanmax(np.abs(
        net - df["ls_net_25bps"].to_numpy()[pm])))
    netf = net[np.isfinite(net)]
    errs["h3_mean"] = abs(float(netf.mean()) - report["h3_net_ls_25bps"]["mean"])
    finite = [v for v in errs.values() if np.isfinite(v)]
    nan_cells = [k for k, v in errs.items() if not np.isfinite(v)]
    ok = bool(not nan_cells and finite and max(finite) <= 1e-10)
    return {"ok": ok, "max_error": float(max(finite)) if finite else float("nan"),
            "nan_cells": nan_cells, "errors": {k: float(v) for k, v in errs.items()}}


def _append_study_row(report: dict, months: pd.PeriodIndex) -> None:
    v = report["verdict"]
    append_study(
        study="calendar_technical", factor_name="same_month_1_10",
        verdict=bool(v["pass"]),
        criteria="H1 IC t>=3 & H2 partial(mom_12_1) t>=2 & H3 net25bps t>=2 & H4 stability>0",
        params={"prereg": PREREG_DOC, "lags": list(LAG_YEARS), "min_lag_obs": MIN_LAG_OBS,
                "n_quantiles": N_QUANTILES, "main_cost_bps": MAIN_COST_BPS,
                "min_cross_section": MIN_CROSS_SECTION},
        eval_start=pd.Period(report["eval_start"], "M").to_timestamp().date(),
        eval_end=pd.Period(report["eval_end"], "M").to_timestamp("M").date(),
        report_path=report["artifacts"]["monthly"],
        criterion_values={
            "h1_ic_mean": report["h1_ic"]["mean"], "h1_t": report["h1_ic"]["nw_t"],
            "h2_partial_mean": report["h2_partial_ic"]["mean"],
            "h2_t": report["h2_partial_ic"]["nw_t"],
            "h3_net_mean": report["h3_net_ls_25bps"]["mean"],
            "h3_t": report["h3_net_ls_25bps"]["nw_t"],
            "h4_stability_ic": report["h4_stability_ic_mean"],
        })


def _write_md(path: Path, report: dict) -> None:
    d = report["disclosures"]
    lines = [f"# wave17 same_month_seasonality：{report['signal']}", "",
             f"预注册：{report['prereg']}；主窗口 {report['eval_start']} ~ {report['eval_end']}。", "",
             "| 判据 | mean | nw_t | n | 过线 |", "| --- | --- | --- | --- | --- |",
             f"| H1 IC | {report['h1_ic']['mean']:+.4f} | {report['h1_ic']['nw_t']:+.2f} | {report['h1_ic']['n']} | {report['verdict']['h1_ic']} |",
             f"| H2 partial IC \\| mom_12_1 | {report['h2_partial_ic']['mean']:+.4f} | {report['h2_partial_ic']['nw_t']:+.2f} | {report['h2_partial_ic']['n']} | {report['verdict']['h2_partial']} |",
             f"| H3 LS 净 25bps/月 | {report['h3_net_ls_25bps']['mean']:+.5f} | {report['h3_net_ls_25bps']['nw_t']:+.2f} | {report['h3_net_ls_25bps']['n']} | {report['verdict']['h3_net_ls']} |",
             f"| H4 稳定腿 IC | {report['h4_stability_ic_mean']:+.4f} | — | {d['stability_h4']['n']} | {report['verdict']['h4_stability']} |",
             "", f"**PASS = {report['verdict']['pass']}**", "",
             f"毛 LS：{d['gross_ls_primary']['mean']:+.5f}/月 t={d['gross_ls_primary']['nw_t']:+.2f}；"
             f"净档：" + "；".join(f"{k} {v['mean']:+.5f}/t={v['nw_t']:+.2f}" for k, v in d['net_tiers_primary'].items()),
             "", f"月换手 Σ|Δw|={d['mean_turnover_primary']:.2f}；1月 IC {d['jan_ic_primary']:+.4f} vs 非1月 {d['nonjan_ic_primary']:+.4f}；"
             f"剔薄月 {d['skipped_thin_months']}", "",
             f"逐年 IC：{d['yearly_ic_primary']}", "",
             f"审计：{report['audit']['ok']}（max_err={report['audit']['max_error']:.2e}）"]
    path.write_text("\n".join(lines), encoding="utf-8")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="wave17 same_month_seasonality（预注册引擎）")
    p.add_argument("--end", type=date.fromisoformat, default=None)
    p.add_argument("--no-study", action="store_true")
    p.add_argument("--output-dir", type=Path, default=OUTPUT_DIR)
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    from dotenv import load_dotenv

    from research.data import research_engine
    load_dotenv(Path(__file__).resolve().parents[1] / ".env")
    args = parse_args(argv)
    prog = Progress("wave17_sms", warn_gb=5.0)
    try:
        engine = research_engine()
        end = args.end
        if end is None:
            end = pd.Timestamp(pd.read_sql_query(
                "select max(date) as d from daily_prices", engine)["d"].iloc[0]).date()
        run_study(engine, end=end, output_dir=args.output_dir,
                  write_study=not args.no_study, prog=prog)
        return 0
    except Exception as e:  # noqa: BLE001
        from loguru import logger
        logger.opt(exception=e).error("wave17_sms failed")
        return 1
    finally:
        prog.done()


if __name__ == "__main__":
    raise SystemExit(main())
