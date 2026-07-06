"""技术分析基线回测入口。

数据现实约束（2026-07 时点，corporate-actions 20 年回填后）：
- computed_adjustment_factors 覆盖 ex_date >= 2003-01-01 的 MASSIVE 源事件
  （2026-07 归档导入，见 docs/corp_actions_archive_2026-07.md）；2003 前无价格
  也无事件，仍是硬地板。pre-2024-05-14 的链没有 vendor reference 可对账，
  只能靠价格跳变抽验兜底。
- 存在无因子覆盖事件的证券（值冲突挂起、POLYGON 孤行、归档漏抓、退市缺口）
  由 securities_with_uncovered_events 整体剔除，避免假跳空污染横截面。

用法（连 253 生产库，只读）：
    RESEARCH_DATABASE_URL=postgresql://...@192.168.1.253:5432/stock \
        .venv/bin/python -m research.run_baselines
"""
from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

from research.backtest import BacktestResult, run_backtest
from research.data import (
    apply_adjustment,
    load_adjusted_panel,
    load_delisting_returns,
    load_factor_events,
    load_price_long,
    research_engine,
    resolve_terminal_returns,
    securities_with_uncovered_events,
    to_wide,
)
from research.strategies import momentum_12_1, short_term_reversal, sma_trend
from research.universe import build_universe_mask

OUTPUT_DIR = Path(__file__).resolve().parent / "output"
FACTOR_TRUST_FLOOR = date(2003, 1, 1)
DEFAULT_PANEL_START = date(2024, 5, 14)  # 基线默认沿用原窗口；20 年面板显式传 --start
_FALLBACK_UNSET = object()  # --terminal-return-fallback 未提供时的哨兵（区分显式 none）


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="技术分析基线回测")
    parser.add_argument("--start", type=date.fromisoformat, default=DEFAULT_PANEL_START)
    parser.add_argument("--end", type=date.fromisoformat, default=date(2026, 6, 10))
    parser.add_argument("--eval-start", type=date.fromisoformat, default=date(2025, 6, 1),
                        help="指标起算日（之前为 warmup，不计入收益统计）")
    parser.add_argument("--cost-bps", type=float, default=10.0, help="单边成本（基点）")
    parser.add_argument("--min-dollar-volume", type=float, default=2_000_000.0)
    parser.add_argument("--min-price", type=float, default=3.0)
    parser.add_argument("--benchmark", default="SPY")
    parser.add_argument("--terminal-return", default=None,
                        help="退市持仓的终局收益假设（如 -1.0=归零、-0.3=CRSP 经验值、"
                             "none=显式沿用旧口径即退市赚 0%%）。面板起点早于 2024-05-14 时必填。"
                             "delisting_events 有实测收益时它降级为未覆盖证券的 fallback。")
    parser.add_argument("--no-delisting-returns", action="store_true",
                        help="不读 delisting_events 的逐证券实测退市收益，只用 --terminal-return "
                             "全局假设（复现旧口径运行）。")
    parser.add_argument("--terminal-return-fallback", default=_FALLBACK_UNSET,
                        help="实测 Series 口径下未覆盖证券的兜底收益（float，或 none=显式不兜底）。"
                             "缺省沿用 --terminal-return 标量作兜底（现状语义）；显式给出时覆盖之。")
    parser.add_argument("--no-fund-closure-par", action="store_true",
                        help="关闭 ETF 清盘平价合成（FUND_CLOSURE + final_price 在场的 NULL 实测行"
                             "读取时合成 0.0），只用纯实测行。")
    args = parser.parse_args(argv)
    # 退市终局强制显式化：长窗口面板含 6,000+ 退市股，默认"退市赚 0%"会系统性
    # 高估做多策略（评估 hard truth）。短窗口沿旧口径不变。
    if args.start < date(2024, 5, 14) and args.terminal_return is None:
        parser.error("--start 早于 2024-05-14 的长窗口回测必须显式给 --terminal-return"
                     "（-1.0 归零 / -0.3 CRSP 经验 / none 沿用退市赚 0%% 旧口径）")
    if isinstance(args.terminal_return, str):
        args.terminal_return = None if args.terminal_return.lower() == "none" else float(args.terminal_return)
    args.terminal_return_fallback_explicit = args.terminal_return_fallback is not _FALLBACK_UNSET
    if isinstance(args.terminal_return_fallback, str):
        args.terminal_return_fallback = (
            None if args.terminal_return_fallback.lower() == "none" else float(args.terminal_return_fallback)
        )
    elif not args.terminal_return_fallback_explicit:
        args.terminal_return_fallback = None
    return args


def trim(result: BacktestResult, eval_start: date) -> BacktestResult:
    ts = pd.Timestamp(eval_start)
    r = result.daily_returns.loc[result.daily_returns.index >= ts]
    return BacktestResult(
        name=result.name,
        daily_returns=r,
        equity=(1 + r).cumprod(),
        turnover=result.turnover.loc[result.turnover.index >= ts],
        avg_positions=result.avg_positions,
        terminal_missing_position_days=int(result.terminal_missing_position_days),
    )


def main(argv: list[str] | None = None) -> int:
    load_dotenv()
    args = parse_args(argv)
    if args.start < FACTOR_TRUST_FLOOR:
        print(f"警告: start={args.start} 早于因子可信窗口 {FACTOR_TRUST_FLOOR}，"
              "更早的价格未复权，结果不可信。")
    engine = research_engine()

    print(f"加载 CS 面板 {args.start} ~ {args.end} ...")
    panel = load_adjusted_panel(engine, start=args.start, end=args.end)
    adj_close = panel["adj_close"]

    bad_ids = securities_with_uncovered_events(engine, start=args.start, end=args.end)
    drop = [c for c in adj_close.columns if c in set(bad_ids)]
    if drop:
        for key in panel:
            panel[key] = panel[key].drop(columns=drop)
        adj_close = panel["adj_close"]
        print(f"剔除 {len(drop)} 只有未覆盖拆股事件的证券（因子缺口）")
    print(f"面板: {adj_close.shape[0]} 个交易日 × {adj_close.shape[1]} 只证券")

    universe = build_universe_mask(
        engine,
        start=args.start,
        end=args.end,
        adj_close=adj_close,
        close=panel["close"],
        dollar_volume=panel["dollar_volume"],
        min_price=args.min_price,
        min_median_dollar_volume=args.min_dollar_volume,
    )
    eligible = universe["eligible"]
    print(f"Universe hash: {universe['universe_hash']}")
    print(f"平均可交易标的数: {eligible.sum(axis=1).mean():.0f}")

    # 退市终局收益：优先 delisting_events 的逐证券实测 delisting_return，
    # 查无（表未填充 / --no-delisting-returns）再落回全局 CLI 假设（旧口径不变）。
    realized = (pd.Series(dtype="float64") if args.no_delisting_returns
                else load_delisting_returns(engine, fund_closure_par=not args.no_fund_closure_par))
    terminal_return, terminal_fallback = resolve_terminal_returns(
        realized, args.terminal_return, use_realized=not args.no_delisting_returns
    )
    if args.terminal_return_fallback_explicit and isinstance(terminal_return, pd.Series):
        # 显式 --terminal-return-fallback 覆盖"CLI 标量降级为兜底"的缺省语义；
        # 标量/None 口径下引擎不消费 fallback，不覆盖以保持打印口径一致。
        terminal_fallback = args.terminal_return_fallback
    if isinstance(terminal_return, pd.Series):
        in_panel = int(terminal_return.index.isin(adj_close.columns).sum())
        fallback_desc = ("无（未覆盖的退市持仓沿旧口径赚 0%）" if terminal_fallback is None
                         else f"全局假设 {terminal_fallback:+.1%}")
        print(f"退市收益模式: 实测 delisting_return {len(terminal_return)} 只"
              f"（本面板内 {in_panel} 只），未覆盖 fallback: {fallback_desc}")
    else:
        why = ("--no-delisting-returns 显式关闭实测" if args.no_delisting_returns
               else "delisting_events 无实测收益")
        assumption = ("无假设（退市持仓沿旧口径赚 0%）" if terminal_return is None
                      else f"全局假设 {terminal_return:+.1%}")
        print(f"退市收益模式: {why}，全部退市持仓使用: {assumption}")

    results = [
        run_backtest("momentum_12_1 (top10%, 月调)", momentum_12_1(adj_close, eligible), adj_close, cost_bps=args.cost_bps, terminal_return=terminal_return, terminal_return_fallback=terminal_fallback),
        run_backtest("sma_50_200 趋势 (周调)", sma_trend(adj_close, eligible), adj_close, cost_bps=args.cost_bps, terminal_return=terminal_return, terminal_return_fallback=terminal_fallback),
        run_backtest("5日反转 (bottom10%, 周调)", short_term_reversal(adj_close, eligible), adj_close, cost_bps=args.cost_bps, terminal_return=terminal_return, terminal_return_fallback=terminal_fallback),
    ]

    ew_weights = eligible.astype(float)
    ew_weights = ew_weights.div(ew_weights.sum(axis=1).where(lambda s: s > 0), axis=0).fillna(0.0)
    results.append(run_backtest("等权全样本（参考）", ew_weights, adj_close, cost_bps=args.cost_bps))

    results = [trim(r, args.eval_start) for r in results]

    rows = {r.name: r.metrics() for r in results}

    # 基准：SPY 买入持有
    bench_ids = _lookup_ids(engine, [args.benchmark])
    if bench_ids:
        bench = load_price_long(engine, start=args.start, end=args.end, types=("ETF",), security_ids=bench_ids)
        bench = apply_adjustment(bench, load_factor_events(engine, as_of=args.end), as_of=args.end)
        bench_ret = to_wide(bench, "adj_close").iloc[:, 0].pct_change(fill_method=None).reindex(adj_close.index)
        bench_result = trim(
            BacktestResult(
                name=f"{args.benchmark} 买入持有",
                daily_returns=bench_ret.fillna(0.0),
                equity=(1 + bench_ret.fillna(0.0)).cumprod(),
                turnover=pd.Series(0.0, index=adj_close.index),
                avg_positions=1,
            ),
            args.eval_start,
        )
        rows[bench_result.name] = bench_result.metrics()
        results.append(bench_result)
    else:
        print(f"警告: 基准 {args.benchmark} 未找到")

    table = pd.DataFrame(rows).T
    pd.set_option("display.float_format", lambda v: f"{v:,.3f}")
    print(f"\n=== 回测结果 {args.eval_start} ~ {args.end}（净值含双边成本 {args.cost_bps:.0f}bps）===")
    print(table.to_string())

    # terminal missing 敏感性：有退市持仓的策略自动跑 -100% 对比
    has_terminal = [r for r in results if r.terminal_missing_position_days > 0]
    if has_terminal:
        print(f"\n=== Terminal Missing 敏感性（{len(has_terminal)} 只策略受影响）===")
        sens_rows = {}
        for r in has_terminal:
            days_pct = r.terminal_missing_position_days / max(len(r.daily_returns) * r.avg_positions, 1)
            sens_rows[r.name] = {
                "terminal_days": r.terminal_missing_position_days,
                "terminal_days_pct": f"{days_pct:.4%}",
                "note": "退市持仓日收益假设为 0%，实际可能更差",
            }
        print(pd.DataFrame(sens_rows).T.to_string())
    else:
        print("\n无 terminal missing 持仓，敏感性分析跳过。")

    OUTPUT_DIR.mkdir(exist_ok=True)
    curves = pd.DataFrame({r.name: r.equity for r in results})
    out = OUTPUT_DIR / f"baselines_{args.eval_start}_{args.end}.csv"
    curves.to_csv(out)
    table.to_csv(OUTPUT_DIR / f"baselines_metrics_{args.eval_start}_{args.end}.csv")
    print(f"\n净值曲线已保存: {out}")
    return 0


def _lookup_ids(engine, symbols: list[str]) -> list[int]:
    from sqlalchemy import text
    with engine.connect() as conn:
        rows = conn.execute(
            text("select id from securities where upper(symbol) = any(:syms)"),
            {"syms": [s.upper() for s in symbols]},
        ).fetchall()
    return [r[0] for r in rows]


if __name__ == "__main__":
    raise SystemExit(main())
