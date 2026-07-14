"""composite_v2 带星号纸面组合月度跟踪器（生成持仓单 + 结算上月）。

章程 docs/paper_portfolio_charter_2026-07.md（2026-07-14 冻结）。带星号=非部署：
零成本前瞻留痕练习，不进 trials 台账，不产生部署授权。

月度流程（每月第一个交易日后运行一次）：
1. 结算上月：用上月持仓单 + 本月最新价格，逐股实现收益（t+1 close 口径），
   毛/净双账 + SPY 对照，追加进 ledger.json。
2. 生成本月持仓单：形成日 = 最近完整交易日；小盘桶 q5 信号降序前 30（延续
   规则：旧持仓仍在 q5 则保留，缺额按信号降序补足）；执行日 = 形成日次一交易日，
   记账价 = 执行日收盘。执行日价格尚不存在时持仓单先落盘（pending），下月结算
   时回填执行价。

幂等与不可篡改：当月文件已存在且 --force 未给 → 拒绝覆盖；历史月份文件永不改写
（结算写进 ledger 与当月新文件，不回改上月持仓单）。

用法（253 上）：
    python -m research.paper_portfolio_tracker            # 例行月度
    python -m research.paper_portfolio_tracker --force    # 当月未提交前重生成
"""
from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
from sqlalchemy import text

from research.backtest import eligibility_mask
from research.data import securities_with_uncovered_events
from research.factors.price_cache import adjusted_close_panel, raw_bar_panels
from research.factors.protocol import FactorContext, get
from research.lowvol_monetization import _security_flags
from research.market_cap import load_market_cap_panel
from research.progress import Progress
from research.retail_reality_study import _measured_cost_bps
from research.size_neutral_study import _bucket_labels

# 注册副作用：composite_v2 及其成分
import research.factors.builtins.classic_pillars  # noqa: F401
import research.factors.builtins.composite_v1  # noqa: F401
import research.factors.builtins.composite_v2  # noqa: F401
import research.factors.builtins.operating_profitability  # noqa: F401
import research.factors.builtins.size  # noqa: F401

PORTFOLIO_DIR = Path(__file__).resolve().parent / "paper_portfolio"
CHARTER = "docs/paper_portfolio_charter_2026-07.md"

FACTOR = "composite_v2"
HOLDINGS = 30
SPY_SECURITY_ID = 3379
LOOKBACK_DAYS = 400          # 因子暖机（252 回看 + 63 资格窗 + 余量）
FALLBACK_COST_BPS = 40.0
STRESS_MULT = 0.5
MIN_PERIODS = 10             # measured mp10 档（章程冻结）


# ------------------------------------------------------------------ build ----

def build_formation(engine, form_end: date, prog: Progress) -> dict:
    """形成日快照：小盘桶 q5 成员（按信号降序）+ 全部所需记账数据。"""
    probe_start = (pd.Timestamp(form_end) - pd.tseries.offsets.BDay(LOOKBACK_DAYS)).date()
    probe_dates = pd.bdate_range(probe_start, form_end)
    with engine.connect() as conn:
        ids = [int(r[0]) for r in conn.execute(
            text("select id from securities where upper(type) = 'CS' order by id"))]
    flags = _security_flags(engine, ids)
    with prog.stage("bar/复权面板"):
        bars = raw_bar_panels(engine, dates=probe_dates, security_ids=ids,
                              columns=("close", "volume"), buffer_days=200)
        close = bars["close"]
        adj_close = adjusted_close_panel(engine, dates=probe_dates, security_ids=ids,
                                         buffer_days=200).reindex(
            index=close.index, columns=close.columns)
    universe = close.columns
    dates = close.index
    form_ts = dates[dates <= pd.Timestamp(form_end)][-1]
    with prog.stage("资格掩码"):
        eligible = eligibility_mask(close, close * bars["volume"])
        eligible = eligible & pd.Series(universe.isin(flags.index[flags["is_common"]]),
                                        index=universe)
        bad = securities_with_uncovered_events(engine, start=probe_start, end=form_end)
        if bad:
            eligible = eligible & ~pd.Series(universe.isin(bad), index=universe)
    with prog.stage(f"因子 {FACTOR}"):
        ctx = FactorContext(engine=engine, dates=dates, security_universe=universe,
                            as_of=form_ts)
        signal = get(FACTOR).compute(ctx)
    with prog.stage("市值面板"):
        mcap = load_market_cap_panel(engine, dates=pd.DatetimeIndex([form_ts]),
                                     security_ids=[int(c) for c in universe])
        mcap = mcap.reindex(columns=universe)
    sig_row = signal.loc[form_ts].to_numpy()
    mcap_row = mcap.iloc[0].to_numpy()
    cov_row = (eligible.loc[form_ts].to_numpy() & np.isfinite(sig_row)
               & np.isfinite(mcap_row))
    buckets = _bucket_labels(mcap_row, cov_row, 3)
    small = np.flatnonzero(buckets == 1)
    if len(small) < 100:
        raise RuntimeError(f"小盘桶只有 {len(small)} 只（<100），数据异常")
    order = sig_row[small].argsort(kind="stable")
    ranks = np.empty(len(small), dtype="int64")
    ranks[order] = np.arange(len(small))
    q = np.minimum(ranks * 5 // len(small), 4) + 1
    q5 = small[q == 5]
    # 信号降序、security_id 升序破并列（章程冻结的确定性序）
    sec_ids = np.array([int(c) for c in universe])
    q5_sorted = q5[np.lexsort((sec_ids[q5], -sig_row[q5]))]
    with prog.stage("执行日与价格"):
        after = dates[dates > form_ts]
        exec_ts = after[0] if len(after) else None  # 形成日=最新日 → 执行价待回填
        exec_prices = close.loc[exec_ts] if exec_ts is not None else None
    with prog.stage("measured 成本"):
        window_dates = dates[dates <= form_ts][-70:]
        cost_bps, cost_diag = _measured_cost_bps(
            window_dates, pd.Index([int(c) for c in universe]),
            stress_mult=STRESS_MULT, fallback_bps=FALLBACK_COST_BPS,
            min_periods=MIN_PERIODS)
    with engine.connect() as conn:
        rows = conn.execute(text(
            "select s.id, s.symbol, s.name from securities s where s.id = any(:ids)"),
            {"ids": [int(sec_ids[i]) for i in q5_sorted]}).fetchall()
    meta = {int(r[0]): {"symbol": r[1], "name": r[2]} for r in rows}
    return {
        "form_ts": form_ts, "exec_ts": exec_ts, "universe": universe,
        "q5_sorted_ids": [int(sec_ids[i]) for i in q5_sorted],
        "sig": {int(sec_ids[i]): float(sig_row[i]) for i in q5_sorted},
        "meta": meta, "exec_prices": exec_prices,
        "cost_bps": cost_bps, "cost_diag": cost_diag,
        "n_small": int(len(small)), "n_q5": int(len(q5)),
        "adj_close": adj_close, "close": close, "dates": dates,
    }


def pick_holdings(q5_sorted_ids: list[int], prev_holdings: list[int]) -> list[int]:
    """延续规则：旧持仓 ∩ 新 q5 保留，缺额按新 q5 信号降序补足（确定性）。"""
    q5_set = set(q5_sorted_ids)
    kept = [sid for sid in prev_holdings if sid in q5_set][:HOLDINGS]
    fill = [sid for sid in q5_sorted_ids if sid not in set(kept)]
    return kept + fill[: HOLDINGS - len(kept)]


# -------------------------------------------------------------- settlement ----

def settle_previous(engine, prev: dict, snap: dict, prog: Progress) -> dict:
    """结算上月持仓单：入场执行价（可能回填）→ 本月执行日的持有收益，毛/净双账。

    收益口径：复权收盘比值（adj_close[exec_new] / adj_close[exec_prev] − 1），
    自动吸收分红与拆股；退市/无价按最后可得复权价冻结（延迟到有实测退市收益的
    档案更新，不在纸面账里预估）。SPY 对照同两执行日。

    上月单执行价 pending（生成时形成日=面板最新日）时，在此回填：执行日 =
    新面板中严格晚于上月形成日的第一个交易日。历史持仓单文件不改写（章程），
    回填只进结算记录。
    """
    adj = snap["adj_close"]
    dates = snap["dates"]
    if prev.get("execution_date"):
        prev_exec = pd.Timestamp(prev["execution_date"])
    else:
        after_form = dates[dates > pd.Timestamp(prev["formation_date"])]
        if not len(after_form):
            raise RuntimeError("上月形成日后无交易日价格，无法回填执行价")
        prev_exec = after_form[0]
    new_exec = snap["exec_ts"]
    if new_exec is None:
        # 本月单也 pending：结算截点用面板最新日（下月回填差额不影响毛账口径——
        # 逐段收益按执行日几何衔接，见 ledger 复利公式）
        new_exec = dates[-1]
    if new_exec <= prev_exec:
        raise RuntimeError(f"结算窗口为空：{prev_exec.date()} → {new_exec.date()}")
    rows = []
    rets = []
    for h in prev["holdings"]:
        sid = h["security_id"]
        col = sid if sid in adj.columns else None
        if col is None:
            rets.append(0.0)
            rows.append({**h, "exit_price_adj": None, "ret": None, "note": "missing_from_panel"})
            continue
        series = adj[col]
        p0 = series.loc[prev_exec] if prev_exec in series.index else np.nan
        if not np.isfinite(p0):
            p0 = series.loc[:prev_exec].dropna().iloc[-1] if len(series.loc[:prev_exec].dropna()) else np.nan
        p1 = series.loc[new_exec] if new_exec in series.index else np.nan
        if not np.isfinite(p1):
            tail = series.loc[:new_exec].dropna()
            p1 = tail.iloc[-1] if len(tail) else np.nan
            note = "frozen_last_price"  # 停牌/退市：冻结最后价，等档案实测
        else:
            note = ""
        r = float(p1 / p0 - 1.0) if np.isfinite(p0) and np.isfinite(p1) and p0 > 0 else 0.0
        rets.append(r)
        rows.append({"security_id": sid, "symbol": h["symbol"], "weight": h["weight"],
                     "ret": round(r, 6), "note": note})
    w = np.array([h["weight"] for h in prev["holdings"]])
    r_arr = np.array(rets)
    gross = float(w @ r_arr)
    # 净账：本月换手成本在生成新单时计（entry+exit 逐股单边），结算只报毛-上月已计成本
    spy = adj[SPY_SECURITY_ID] if SPY_SECURITY_ID in adj.columns else None
    spy_ret = float(spy.loc[new_exec] / spy.loc[prev_exec] - 1.0) if spy is not None else float("nan")
    return {"period": prev["month"], "from": str(prev_exec.date()), "to": str(new_exec.date()),
            "gross_ret": round(gross, 6), "spy_ret": round(spy_ret, 6),
            "excess_vs_spy": round(gross - spy_ret, 6), "positions": rows}


def turnover_cost(prev_holdings: list[dict], new_ids: list[int],
                  cost_bps: pd.Series) -> dict:
    """换手成本（单边逐股）：卖出离场旧仓 + 买入新进仓位，等权 1/30 计。

    简化（章程口径）：按目标权重 1/N 计双向，不追漂移权重差——月度漂移对
    30 只等权的成本影响 <1bp，人读账目优先。首期全额买入。
    """
    prev_ids = {h["security_id"] for h in prev_holdings}
    new_set = set(new_ids)
    w = 1.0 / max(len(new_ids), 1)
    buys = [sid for sid in new_ids if sid not in prev_ids]
    sells = [sid for sid in prev_ids if sid not in new_set]
    def _cost(sids):
        tot = 0.0
        for sid in sids:
            bps = float(cost_bps.get(sid, FALLBACK_COST_BPS))
            tot += w * bps / 1e4
        return tot
    return {"n_buys": len(buys), "n_sells": len(sells),
            "cost_frac": round(_cost(buys) + _cost(sells), 6)}


# ----------------------------------------------------------------- driver ----

def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="composite_v2 纸面组合月度跟踪器")
    p.add_argument("--as-of", type=date.fromisoformat, default=None,
                   help="形成日上限（默认=库内最新交易日）")
    p.add_argument("--month", default=None, help="持仓单月份标签 YYYY-MM（默认=as-of 所在月）")
    p.add_argument("--force", action="store_true", help="覆盖当月未提交的持仓单")
    p.add_argument("--dir", type=Path, default=PORTFOLIO_DIR)
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    from dotenv import load_dotenv

    from research.data import research_engine
    load_dotenv(Path(__file__).resolve().parents[1] / ".env")
    args = parse_args(argv)
    prog = Progress("paper_portfolio", warn_gb=5.0)
    try:
        engine = research_engine()
        as_of = args.as_of
        if as_of is None:
            as_of = pd.Timestamp(pd.read_sql_query(
                "select max(date) as d from daily_prices", engine)["d"].iloc[0]).date()
        month = args.month or f"{as_of.year:04d}-{as_of.month:02d}"
        args.dir.mkdir(parents=True, exist_ok=True)
        out_json = args.dir / f"{month}.json"
        if out_json.exists() and not args.force:
            raise RuntimeError(f"{out_json} 已存在；未提交前重生成请加 --force")

        snap = build_formation(engine, as_of, prog)

        # 上月结算（若有）
        ledger_path = args.dir / "ledger.json"
        ledger = json.loads(ledger_path.read_text()) if ledger_path.exists() else {"periods": []}
        prev_files = sorted(p for p in args.dir.glob("2*.json") if p.name != out_json.name)
        settlement = None
        prev_holdings: list[dict] = []
        if prev_files:
            prev = json.loads(prev_files[-1].read_text())
            prev_holdings = prev["holdings"]
            with prog.stage("结算上月"):
                settlement = settle_previous(engine, prev, snap, prog)
                done = {p["period"] for p in ledger["periods"]}
                if settlement["period"] not in done:
                    ledger["periods"].append(settlement)

        # 本月持仓
        new_ids = pick_holdings(snap["q5_sorted_ids"], [h["security_id"] for h in prev_holdings])
        tcost = turnover_cost(prev_holdings, new_ids, snap["cost_bps"])
        w = round(1.0 / len(new_ids), 6)
        exec_ts = snap["exec_ts"]
        holdings = []
        for sid in new_ids:
            m = snap["meta"].get(sid, {})
            px = None
            if snap["exec_prices"] is not None and sid in snap["exec_prices"].index:
                v = snap["exec_prices"][sid]
                px = round(float(v), 4) if np.isfinite(v) else None
            holdings.append({
                "security_id": sid, "symbol": m.get("symbol"), "name": m.get("name"),
                "weight": w, "signal": round(snap["sig"][sid], 6),
                "exec_price_raw": px,
                "cost_bps_one_side": round(float(snap["cost_bps"].get(sid, FALLBACK_COST_BPS)), 2),
            })
        doc = {
            "charter": CHARTER, "factor": FACTOR, "month": month,
            "formation_date": str(snap["form_ts"].date()),
            "execution_date": str(exec_ts.date()) if exec_ts is not None else None,
            "execution_status": "priced" if exec_ts is not None else "pending_next_session",
            "n_small_bucket": snap["n_small"], "n_q5": snap["n_q5"],
            "turnover": tcost, "cost_diag": snap["cost_diag"],
            "holdings": holdings,
        }
        out_json.write_text(json.dumps(doc, ensure_ascii=False, indent=2), encoding="utf-8")
        ledger_path.write_text(json.dumps(ledger, ensure_ascii=False, indent=2), encoding="utf-8")
        _write_md(args.dir / f"{month}.md", doc, settlement, ledger)
        prog.log(f"{month} 持仓单落盘：{len(holdings)} 只，形成 {doc['formation_date']}，"
                 f"执行 {doc['execution_date']}（{doc['execution_status']}）")
        return 0
    except Exception as e:  # noqa: BLE001
        from loguru import logger
        logger.opt(exception=e).error("paper_portfolio failed")
        return 1
    finally:
        prog.done()


def _write_md(path: Path, doc: dict, settlement: dict | None, ledger: dict) -> None:
    lines = [f"# composite_v2 纸面组合 {doc['month']}（带星号：非部署练习）", "",
             f"章程：{doc['charter']}；形成 {doc['formation_date']}，执行 {doc['execution_date']}"
             f"（{doc['execution_status']}）；小盘桶 {doc['n_small_bucket']} 只 → q5 {doc['n_q5']} 只 → 持仓 {len(doc['holdings'])} 只。", "",
             f"换手：买 {doc['turnover']['n_buys']} / 卖 {doc['turnover']['n_sells']}，"
             f"单边成本合计 {doc['turnover']['cost_frac']*1e4:.1f}bps（measured 覆盖 {doc['cost_diag']['coverage']:.0%}）", "",
             "| # | symbol | name | signal | 执行价 | 成本 bps |", "| --- | --- | --- | --- | --- | --- |"]
    for i, h in enumerate(doc["holdings"], 1):
        lines.append(f"| {i} | {h['symbol']} | {(h['name'] or '')[:32]} | {h['signal']:.4f} "
                     f"| {h['exec_price_raw'] if h['exec_price_raw'] is not None else '待回填'} | {h['cost_bps_one_side']:.1f} |")
    if settlement:
        lines += ["", f"## 上月结算（{settlement['period']}：{settlement['from']} → {settlement['to']}）", "",
                  f"毛收益 **{settlement['gross_ret']:+.2%}** vs SPY {settlement['spy_ret']:+.2%}"
                  f"（超额 {settlement['excess_vs_spy']:+.2%}）"]
    if ledger["periods"]:
        cum = 1.0
        cum_spy = 1.0
        for p in ledger["periods"]:
            cum *= 1 + p["gross_ret"]
            cum_spy *= 1 + p["spy_ret"]
        lines += ["", f"## 累计（{len(ledger['periods'])} 期）",
                  "", f"组合 {cum-1:+.2%} vs SPY {cum_spy-1:+.2%}"]
    path.write_text("\n".join(lines), encoding="utf-8")


if __name__ == "__main__":
    raise SystemExit(main())
