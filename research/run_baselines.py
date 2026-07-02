"""技术分析基线回测入口。

数据现实约束（2026-06 时点）：
- computed_adjustment_factors 只覆盖 ex_date >= 2024-05-14 的事件（Massive 免费档
  730 天可信窗口；因子构建只对该窗口做 vendor 对账）。更早的拆股/分红没有因子，
  所以 **2024-05-14 之前的"复权价"并未真正复权**，绝不能用于回测。
- 因此默认面板从 2024-05-14 开始；带长 warmup 的策略（12-1 动量、SMA200）
  指标评估从 --eval-start（默认 2025-06-01）起算。
- 窗口内有 SPLIT 事件但无因子行的证券（因子构建只跑 active，退市股有缺口）
  会被整体剔除，避免假跳空污染横截面。

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
    load_factor_events,
    load_price_long,
    research_engine,
    securities_with_uncovered_events,
    to_wide,
)
from research.strategies import momentum_12_1, short_term_reversal, sma_trend
from research.universe import build_universe_mask

OUTPUT_DIR = Path(__file__).resolve().parent / "output"
FACTOR_TRUST_FLOOR = date(2024, 5, 14)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="技术分析基线回测")
    parser.add_argument("--start", type=date.fromisoformat, default=FACTOR_TRUST_FLOOR)
    parser.add_argument("--end", type=date.fromisoformat, default=date(2026, 6, 10))
    parser.add_argument("--eval-start", type=date.fromisoformat, default=date(2025, 6, 1),
                        help="指标起算日（之前为 warmup，不计入收益统计）")
    parser.add_argument("--cost-bps", type=float, default=10.0, help="单边成本（基点）")
    parser.add_argument("--min-dollar-volume", type=float, default=2_000_000.0)
    parser.add_argument("--min-price", type=float, default=3.0)
    parser.add_argument("--benchmark", default="SPY")
    return parser.parse_args(argv)


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

    results = [
        run_backtest("momentum_12_1 (top10%, 月调)", momentum_12_1(adj_close, eligible), adj_close, cost_bps=args.cost_bps),
        run_backtest("sma_50_200 趋势 (周调)", sma_trend(adj_close, eligible), adj_close, cost_bps=args.cost_bps),
        run_backtest("5日反转 (bottom10%, 周调)", short_term_reversal(adj_close, eligible), adj_close, cost_bps=args.cost_bps),
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
