"""wave 16 market_intraday_momentum：市场同日日内动量（GHLZ 2018 复现+发表后检验）。

预注册 docs/wave16_market_intraday_momentum_hypotheses.md（2026-07-13 冻结，先于任何结果）。
study（部署前证据检验），写 trials 台账 study 行；research/ 只读铁律不变。

核心口径（与预注册逐条对应）：
- 资产两个：SPY（分钟锚点价）与 pit_cs_capw（全 CS、前一日总市值加权）。
- 主预测器 r1 = 昨收 → 当日 09:59 bar close（含隔夜；SPY 用原始价，个股隔夜段
  经日级复权因子链）；描述腿 r1_intraday = 09:30 open → 09:59（不裁决）。
- 目标 y = 15:29 bar close → 15:59 bar close（排除 16:00 收盘竞价）。
- 有效日 = SPY 三锚 bar（09:59/15:29/15:59）齐全；市场腿同用此掩码（早收盘是
  市场级事件，且特征表对缺窗口存哨兵 0，必须整日剔除）。
- 检验：OLS y~r1 的 NW(lag=10) β/t + sign(r1) 交易（1/2/5bps 单边，主档 2bps，
  双边计价）+ 剔 |净收益| 最大 10 日稳健性。判据 4 条见预注册；两资产独立裁决。
- 市场腿指数构造用"同集合"掩码：某股当日 r1 或 y 缺任一则两侧同时剔除——指数是
  研究对象（可交易主张由 SPY 腿承担），同集合保证 β 度量的是同一篮子的时序自相关。

非目标：不搜阈值、不做条件化、不换预测窗、不扩行业/个股；r1_intraday 再好看
也只是披露。
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
from research.evaluate import _newey_west_t
from research.progress import Progress

OUTPUT_DIR = Path(__file__).resolve().parent / "output"
PREREG_DOC = "docs/wave16_market_intraday_momentum_hypotheses.md"

SPY_SECURITY_ID = 3379
NW_LAG = 10
COST_TIERS_BPS = (1.0, 2.0, 5.0)
MAIN_COST_BPS = 2.0
N_DROP_ROBUST = 10
MIN_OBS = 100

SPY_REPL = (date(2004, 1, 2), date(2018, 12, 31))
MKT_REPL = (date(2010, 1, 4), date(2018, 12, 31))
POST_START = date(2019, 1, 2)

# 判据常数（预注册"判据"节）
CRIT_REPL_T = 3.0
CRIT_POST_T = 2.0
CRIT_TRADE_T = 2.0


# ------------------------------------------------------------------ core ----

def nw_beta_t(x: np.ndarray, y: np.ndarray, lag: int) -> tuple[float, float, int]:
    """y = α + β·x 的 (β, NW-t, n)。Bartlett 核，lag 截断；样本 < MIN_OBS 返回 NaN。"""
    m = np.isfinite(x) & np.isfinite(y)
    xs, ys = x[m].astype("float64"), y[m].astype("float64")
    n = len(xs)
    if n < MIN_OBS:
        return float("nan"), float("nan"), n
    xd = xs - xs.mean()
    yd = ys - ys.mean()
    sxx = float(xd @ xd)
    if sxx <= 0:
        return float("nan"), float("nan"), n
    beta = float(xd @ yd) / sxx
    u = xd * (yd - beta * xd)  # 矩条件序列
    lag_eff = min(max(int(lag), 0), n - 1)
    s = float(u @ u) / n
    for k in range(1, lag_eff + 1):
        s += 2.0 * (1.0 - k / (lag_eff + 1)) * float(u[k:] @ u[:-k]) / n
    if s <= 0:
        return beta, float("nan"), n
    var_beta = n * s / (sxx * sxx)
    return beta, beta / float(np.sqrt(var_beta)), n


def trading_net(r1: np.ndarray, y: np.ndarray, cost_one_side_bps: float
                ) -> tuple[np.ndarray, np.ndarray]:
    """sign(r1) 持有 y 的日净收益序列与 traded 掩码（r1/y 任一非有限的日子剔除）。

    sign=0 的日子不交易（净收益 0、不计成本，仍留在日历里保 NW 对齐）；
    交易日成本 = 双边 2×单边（15:30 进、15:59 出）。
    """
    m = np.isfinite(r1) & np.isfinite(y)
    sgn = np.sign(np.where(m, r1, 0.0))
    traded = (sgn != 0.0) & m
    net = np.where(m, sgn * np.where(m, y, 0.0) - traded * 2.0 * cost_one_side_bps / 1e4, np.nan)
    return net, traded


def series_stats(net: np.ndarray, lag: int) -> dict:
    """净收益日序列的均值（bps/日）、NW-t、年化 Sharpe、n。"""
    x = net[np.isfinite(net)]
    n = len(x)
    if n < MIN_OBS:
        return {"mean_bps": float("nan"), "nw_t": float("nan"), "sharpe": float("nan"), "n": n}
    t = _newey_west_t(pd.Series(x), lag)
    sd = float(np.std(x, ddof=1))
    sharpe = float(x.mean() / sd * np.sqrt(252)) if sd > 0 else float("nan")
    return {"mean_bps": float(x.mean() * 1e4), "nw_t": float(t), "sharpe": sharpe, "n": n}


def drop_extreme_mean_bps(net: np.ndarray, n_drop: int) -> float:
    """剔除 |净收益| 最大的 n_drop 日后的净均值（bps/日）——危机日稳健判据。"""
    x = net[np.isfinite(net)]
    if len(x) <= n_drop + MIN_OBS // 2:
        return float("nan")
    keep = np.argsort(np.abs(x))[: len(x) - n_drop]
    return float(x[keep].mean() * 1e4)


def cap_weighted_market(r1p: pd.DataFrame, yp: pd.DataFrame, w: pd.DataFrame) -> pd.DataFrame:
    """同集合市值加权聚合 → DataFrame(index=date, r1/y/n_names/cap_coverage)。

    单日某股 r1、y、w 任一缺 → 该股当日整体剔除（同集合掩码，docstring 论证）。
    cap_coverage = 参与聚合的权重 / 当日全部有限权重（诊断，不裁决）。
    """
    r1a, ya, wa = (df.to_numpy(dtype="float64") for df in (r1p, yp, w))
    valid = np.isfinite(r1a) & np.isfinite(ya) & np.isfinite(wa) & (wa > 0)
    wv = np.where(valid, wa, 0.0)
    denom = wv.sum(axis=1)
    with np.errstate(invalid="ignore", divide="ignore"):
        r1_mkt = np.where(denom > 0, (wv * np.where(valid, r1a, 0.0)).sum(axis=1) / denom, np.nan)
        y_mkt = np.where(denom > 0, (wv * np.where(valid, ya, 0.0)).sum(axis=1) / denom, np.nan)
        w_all = np.where(np.isfinite(wa) & (wa > 0), wa, 0.0).sum(axis=1)
        coverage = np.where(w_all > 0, denom / w_all, np.nan)
    return pd.DataFrame(
        {"r1": r1_mkt, "y": y_mkt, "n_names": valid.sum(axis=1), "cap_coverage": coverage},
        index=r1p.index,
    )


def evaluate_asset_verdict(repl_reg: dict, post_reg: dict, post_trade: dict,
                           robust_mean_bps: float) -> dict:
    """预注册判据 1-4。PASS = 全过。"""
    c1 = bool(repl_reg["beta"] > 0 and repl_reg["nw_t"] >= CRIT_REPL_T)
    c2 = bool(post_reg["beta"] > 0 and post_reg["nw_t"] >= CRIT_POST_T)
    c3 = bool(post_trade["mean_bps"] > 0 and post_trade["nw_t"] >= CRIT_TRADE_T)
    c4 = bool(robust_mean_bps > 0)
    return {"c1_repl_reg": c1, "c2_post_reg": c2, "c3_post_trade": c3,
            "c4_crisis_robust": c4, "pass": bool(c1 and c2 and c3 and c4)}


# ------------------------------------------------------------- data build ----

def load_spy_anchors(ch_url: str | None) -> pd.DataFrame:
    """SPY 分钟锚点（ET）：09:30 open、09:59/15:29/15:59 close，按日一行。"""
    from research.minute_bars import query_df
    sql = f"""
        SELECT toDate(ts, 'America/New_York') AS d,
               anyIf(open,  md = 570) AS o0930,
               anyIf(close, md = 599) AS c0959,
               anyIf(close, md = 929) AS c1529,
               anyIf(close, md = 959) AS c1559,
               countIf(md = 599) > 0 AND countIf(md = 929) > 0 AND countIf(md = 959) > 0 AS ok
        FROM (
            SELECT ts, open, close,
                   toHour(ts, 'America/New_York') * 60 + toMinute(ts, 'America/New_York') AS md
            FROM stock.minute_bars FINAL
            WHERE security_id = {SPY_SECURITY_ID}
        )
        GROUP BY d ORDER BY d
    """
    frame = query_df(sql, ch_url)
    frame["d"] = pd.to_datetime(frame["d"])
    frame = frame[frame["ok"].astype(bool) & (frame[["o0930", "c0959", "c1529", "c1559"]] > 0).all(axis=1)]
    return frame.set_index("d")


def build_spy_series(engine, ch_url: str | None, end: date) -> pd.DataFrame:
    """SPY 资产序列：index=有效日，列 r1/r1_intraday/y。r1 昨收用 PG 原始日收盘。"""
    anchors = load_spy_anchors(ch_url)
    daily = pd.read_sql_query(
        "select date, close from daily_prices where security_id = %(sid)s order by date",
        engine, params={"sid": SPY_SECURITY_ID}, parse_dates=["date"],
    ).set_index("date")["close"]
    prev_close = daily.shift(1)  # 官方日历上的前收，再对齐锚点日
    df = anchors.loc[anchors.index <= pd.Timestamp(end)].copy()
    df["prev_close"] = prev_close.reindex(df.index)
    df = df[df["prev_close"].notna() & (df["prev_close"] > 0)]
    out = pd.DataFrame(index=df.index)
    out["r1"] = df["c0959"] / df["prev_close"] - 1.0
    out["r1_intraday"] = df["c0959"] / df["o0930"] - 1.0
    out["y"] = df["c1559"] / df["c1529"] - 1.0
    return out


def build_market_series(engine, ch_url: str | None, *, start: date, end: date,
                        valid_days: pd.DatetimeIndex, prog: Progress,
                        chunk: int = 1500) -> pd.DataFrame:
    """pit_cs_capw 资产序列：index=有效日，r1/r1_intraday/y/n_names/cap_coverage。"""
    from research.data import (
        apply_adjustment, load_factor_events, securities_with_uncovered_events, to_wide,
    )
    from research.factors.minute_loader import load_minute_feature_panel
    from research.factors.price_cache import load_price_long_fast
    from research.market_cap import load_market_cap_panel

    with prog.stage("CS open/close 长表"):
        panel_start = date(start.year - 1, 12, 1)  # 前收暖机
        long = load_price_long_fast(engine, start=panel_start, end=end,
                                    columns="open, close", types=("CS",))
        events = load_factor_events(engine, as_of=end)
        long = apply_adjustment(long, events, as_of=end)
        close_w = to_wide(long, "close")
        adj_close_w = to_wide(long, "adj_close")
        open_w = to_wide(long, "open")
        del long, events
    with prog.stage("未覆盖事件 gate"):
        drop = set(securities_with_uncovered_events(engine, start=panel_start, end=end))
        keep = [c for c in close_w.columns if int(c) not in drop]
        close_w, adj_close_w, open_w = close_w[keep], adj_close_w[keep], open_w[keep]
    dates = close_w.index
    ids = [int(c) for c in close_w.columns]
    with prog.stage("隔夜段（复权 open/前收）"):
        with np.errstate(invalid="ignore", divide="ignore"):
            factor = adj_close_w / close_w
            adj_open = open_w * factor
            overnight = adj_open / adj_close_w.shift(1) - 1.0
        del factor, adj_open, open_w, close_w
    with prog.stage("分钟窗口特征（ClickHouse）"):
        first_parts, last_parts = [], []
        for i in range(0, len(ids), chunk):
            batch = ids[i:i + chunk]
            feat = load_minute_feature_panel(dates, batch, ("ret_first30", "ret_last30"),
                                             buffer_days=5, min_bars=100, url=ch_url)
            first_parts.append(feat["ret_first30"].reindex(index=dates).astype("float32"))
            last_parts.append(feat["ret_last30"].reindex(index=dates).astype("float32"))
            prog.log(f"minute features {min(i + chunk, len(ids))}/{len(ids)}")
        first30 = pd.concat(first_parts, axis=1).reindex(columns=pd.Index(ids))
        last30 = pd.concat(last_parts, axis=1).reindex(columns=pd.Index(ids))
        del first_parts, last_parts
    with prog.stage("PIT 市值面板"):
        cap = load_market_cap_panel(engine, dates=dates, security_ids=ids)
        cap = cap.reindex(index=dates, columns=pd.Index(ids))
        w = cap.shift(1)  # 前一交易日市值 = t 日晨已知
        del cap
    with prog.stage("市场聚合"):
        first30_f = first30.astype("float64")
        r1_panel = (1.0 + overnight.reindex(columns=first30.columns)) * (1.0 + first30_f) - 1.0
        y_panel = last30.astype("float64")
        del overnight, first30, last30
        mkt = cap_weighted_market(r1_panel, y_panel, w)
        # 描述腿：纯盘中首半小时（同集合掩码复用 r1 的 valid 语义即可近似——
        # 直接用同一权重面板对 first30 聚合，r1/y 缺失的股同样剔除）
        intra = cap_weighted_market(first30_f.where(r1_panel.notna()), y_panel, w)
        mkt["r1_intraday"] = intra["r1"]
        del r1_panel, y_panel, w, first30_f
    mkt = mkt.loc[mkt.index.isin(valid_days) & (mkt.index >= pd.Timestamp(start))
                  & (mkt.index <= pd.Timestamp(end))]
    return mkt[mkt["r1"].notna() & mkt["y"].notna()]


# ----------------------------------------------------------------- report ----

def run_asset(name: str, series: pd.DataFrame, repl: tuple[date, date], *,
              end: date, output_dir: Path, write_study: bool, prog: Progress) -> dict:
    idx = series.index
    repl_mask = (idx >= pd.Timestamp(repl[0])) & (idx <= pd.Timestamp(repl[1]))
    post_mask = idx >= pd.Timestamp(POST_START)
    windows = {"replication": series[repl_mask], "post_publication": series[post_mask]}

    result: dict = {"windows": {}}
    for wname, sub in windows.items():
        r1 = sub["r1"].to_numpy()
        r1i = sub["r1_intraday"].to_numpy()
        y = sub["y"].to_numpy()
        beta, t, n = nw_beta_t(r1, y, NW_LAG)
        beta_i, t_i, _ = nw_beta_t(r1i, y, NW_LAG)
        entry = {"n": n, "reg": {"beta": beta, "nw_t": t},
                 "reg_intraday_desc": {"beta": beta_i, "nw_t": t_i}, "trading": {}}
        for bps in COST_TIERS_BPS:
            net, traded = trading_net(r1, y, bps)
            st = series_stats(net, NW_LAG)
            st["n_traded"] = int(traded.sum())
            if bps == MAIN_COST_BPS:
                st["robust_drop10_mean_bps"] = drop_extreme_mean_bps(net, N_DROP_ROBUST)
                # 披露：多空日分解 + 逐年净均值
                sgn = np.sign(np.where(np.isfinite(r1), r1, 0.0))
                fin = np.isfinite(net)
                entry["long_days_mean_bps"] = float(np.nanmean(net[fin & (sgn > 0)]) * 1e4) if (fin & (sgn > 0)).any() else float("nan")
                entry["short_days_mean_bps"] = float(np.nanmean(net[fin & (sgn < 0)]) * 1e4) if (fin & (sgn < 0)).any() else float("nan")
                yearly = pd.Series(net, index=sub.index).groupby(sub.index.year).apply(
                    lambda s: float(np.nanmean(s) * 1e4))
                entry["yearly_net_mean_bps"] = {int(k): round(v, 3) for k, v in yearly.items()}
            entry["trading"][f"{bps:g}bps"] = st
        result["windows"][wname] = entry
        prog.log(f"{name}/{wname} beta={beta:+.4f} t={t:+.2f} "
                 f"net2bps={entry['trading']['2bps']['mean_bps']:+.2f}bps t={entry['trading']['2bps']['nw_t']:+.2f}")

    repl_e, post_e = result["windows"]["replication"], result["windows"]["post_publication"]
    verdict = evaluate_asset_verdict(
        {"beta": repl_e["reg"]["beta"], "nw_t": repl_e["reg"]["nw_t"]},
        {"beta": post_e["reg"]["beta"], "nw_t": post_e["reg"]["nw_t"]},
        post_e["trading"]["2bps"],
        post_e["trading"]["2bps"]["robust_drop10_mean_bps"],
    )
    eval_start, eval_end = str(idx[0].date()), str(idx[-1].date())
    stem = f"wave16_market_intraday_momentum_{name}_{eval_start}_{eval_end}"
    output_dir.mkdir(parents=True, exist_ok=True)
    daily_path = output_dir / f"{stem}_daily.parquet"
    series.to_parquet(daily_path)
    report = {"prereg": PREREG_DOC, "asset": name, "eval_start": eval_start,
              "eval_end": eval_end, "repl_window": [str(repl[0]), str(repl[1])],
              "post_start": str(POST_START), "nw_lag": NW_LAG, **result,
              "verdict": verdict, "artifacts": {"daily": str(daily_path)}}
    audit = _independent_audit(daily_path, report)
    report["audit"] = audit
    (output_dir / f"{stem}.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    _write_md(output_dir / f"{stem}.md", report)
    if write_study and audit["ok"]:
        _append_study_row(report)
    elif write_study:
        prog.log(f"{name} 审计未过（degraded），拒绝写 study 行")
    return report


def _independent_audit(daily_path: Path, report: dict) -> dict:
    """从落盘日度序列复算四判据数值，误差 >1e-10 → degraded。"""
    df = pd.read_parquet(daily_path)
    idx = df.index
    repl0, repl1 = (pd.Timestamp(x) for x in report["repl_window"])
    errs: dict[str, float] = {}
    for wname, mask in (("replication", (idx >= repl0) & (idx <= repl1)),
                        ("post_publication", idx >= pd.Timestamp(report["post_start"]))):
        sub = df[mask]
        beta, t, _ = nw_beta_t(sub["r1"].to_numpy(), sub["y"].to_numpy(), NW_LAG)
        e = report["windows"][wname]["reg"]
        errs[f"{wname}_beta"] = abs(beta - e["beta"])
        errs[f"{wname}_t"] = abs(t - e["nw_t"])
        net, _tr = trading_net(sub["r1"].to_numpy(), sub["y"].to_numpy(), MAIN_COST_BPS)
        st = series_stats(net, NW_LAG)
        et = report["windows"][wname]["trading"]["2bps"]
        errs[f"{wname}_net_mean"] = abs(st["mean_bps"] - et["mean_bps"])
        errs[f"{wname}_net_t"] = abs(st["nw_t"] - et["nw_t"])
        errs[f"{wname}_robust"] = abs(drop_extreme_mean_bps(net, N_DROP_ROBUST)
                                      - et["robust_drop10_mean_bps"])
    finite = [v for v in errs.values() if np.isfinite(v)]
    nan_cells = [k for k, v in errs.items() if not np.isfinite(v)]
    ok = bool(not nan_cells and finite and max(finite) <= 1e-10)
    return {"ok": ok, "max_error": float(max(finite)) if finite else float("nan"),
            "nan_cells": nan_cells, "errors": {k: float(v) for k, v in errs.items()}}


def _append_study_row(report: dict) -> None:
    post = report["windows"]["post_publication"]
    repl = report["windows"]["replication"]
    v = report["verdict"]
    append_study(
        study="market_intraday_momentum", factor_name=report["asset"],
        verdict=bool(v["pass"]),
        criteria="repl beta>0 t>=3 & post beta>0 t>=2 & post net(2bps)>0 t>=2 & drop10>0",
        params={"prereg": PREREG_DOC, "nw_lag": NW_LAG, "main_cost_bps": MAIN_COST_BPS,
                "repl_window": report["repl_window"], "post_start": report["post_start"],
                "target": "1529_to_1559_close", "predictor": "prev_close_to_0959"},
        eval_start=date.fromisoformat(report["eval_start"]),
        eval_end=date.fromisoformat(report["eval_end"]),
        report_path=report["artifacts"]["daily"],
        criterion_values={
            "repl_beta": repl["reg"]["beta"], "repl_t": repl["reg"]["nw_t"],
            "post_beta": post["reg"]["beta"], "post_t": post["reg"]["nw_t"],
            "post_net_mean_bps": post["trading"]["2bps"]["mean_bps"],
            "post_net_t": post["trading"]["2bps"]["nw_t"],
            "robust_drop10_mean_bps": post["trading"]["2bps"]["robust_drop10_mean_bps"],
        })


def _write_md(path: Path, report: dict) -> None:
    lines = [f"# wave16 market_intraday_momentum：{report['asset']}", "",
             f"预注册：{report['prereg']}；窗口 {report['eval_start']} ~ {report['eval_end']}；"
             f"复现 {report['repl_window'][0]}~{report['repl_window'][1]}，发表后 {report['post_start']}+。", "",
             "| window | n | beta | nw_t | beta_intra(desc) | net2bps | net_t | sharpe | drop10 |",
             "| --- | --- | --- | --- | --- | --- | --- | --- | --- |"]
    for wname, e in report["windows"].items():
        t2 = e["trading"]["2bps"]
        lines.append(
            f"| {wname} | {e['n']} | {e['reg']['beta']:+.4f} | {e['reg']['nw_t']:+.2f} "
            f"| {e['reg_intraday_desc']['beta']:+.4f}/{e['reg_intraday_desc']['nw_t']:+.2f} "
            f"| {t2['mean_bps']:+.2f}bps | {t2['nw_t']:+.2f} | {t2['sharpe']:+.2f} "
            f"| {t2['robust_drop10_mean_bps']:+.2f} |")
    post = report["windows"]["post_publication"]
    lines += ["", f"成本档（发表后窗）：" + "；".join(
        f"{k} 净 {v['mean_bps']:+.2f}bps/t={v['nw_t']:+.2f}" for k, v in post["trading"].items()),
        "", f"多/空日净均值（发表后 2bps）：{post['long_days_mean_bps']:+.2f} / {post['short_days_mean_bps']:+.2f} bps",
        "", f"逐年净均值 bps（发表后含复现末年）：{post['yearly_net_mean_bps']}",
        "", f"**判据**：{report['verdict']}", "",
        f"审计：{report['audit']['ok']}（max_err={report['audit']['max_error']:.2e}）"]
    path.write_text("\n".join(lines), encoding="utf-8")


# ----------------------------------------------------------------- driver ----

def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="wave16 market_intraday_momentum（预注册引擎）")
    p.add_argument("--end", type=date.fromisoformat, default=None)
    p.add_argument("--assets", default="spy,pit_cs_capw")
    p.add_argument("--ch-url", default=None)
    p.add_argument("--no-study", action="store_true")
    p.add_argument("--output-dir", type=Path, default=OUTPUT_DIR)
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    from dotenv import load_dotenv

    from research.data import research_engine
    load_dotenv(Path(__file__).resolve().parents[1] / ".env")
    args = parse_args(argv)
    prog = Progress("wave16_mim", warn_gb=5.0)
    try:
        engine = research_engine()
        end = args.end
        if end is None:
            end = pd.Timestamp(pd.read_sql_query(
                "select max(date) as d from daily_prices", engine)["d"].iloc[0]).date()
        assets = [a.strip() for a in args.assets.split(",") if a.strip()]
        with prog.stage("SPY 锚点序列"):
            spy = build_spy_series(engine, args.ch_url, end)
        valid_days = spy.index
        if "spy" in assets:
            run_asset("spy", spy.loc[spy.index >= pd.Timestamp(SPY_REPL[0])], SPY_REPL,
                      end=end, output_dir=args.output_dir,
                      write_study=not args.no_study, prog=prog)
        if "pit_cs_capw" in assets:
            with prog.stage("市场组合序列"):
                mkt = build_market_series(engine, args.ch_url, start=MKT_REPL[0], end=end,
                                          valid_days=valid_days, prog=prog)
            run_asset("pit_cs_capw", mkt, MKT_REPL, end=end, output_dir=args.output_dir,
                      write_study=not args.no_study, prog=prog)
        return 0
    except Exception as e:  # noqa: BLE001
        from loguru import logger
        logger.opt(exception=e).error("wave16_mim failed")
        return 1
    finally:
        prog.done()


if __name__ == "__main__":
    raise SystemExit(main())
