"""Wave 13 路径质量条件研究：PRET × information discreteness 双排序。

主排序严格按 Da-Gurun-Warachka (2014)：先按 12-1 PRET 五分位，再在每个
PRET 组内按 ID 五分位。ID 越低，信息越连续。独立双排序只作稳健性诊断。

用法：
    python -m research.path_momentum_study --sample-role primary \
        --start 2016-01-04 --end 2026-07-02
    python -m research.path_momentum_study --sample-role stability \
        --start 2007-07-02 --end 2015-12-31
"""
from __future__ import annotations

import argparse
import json
import math
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from dotenv import load_dotenv

from research._trials_store import append_study
from research.backtest import eligibility_mask
from research.data import (
    DEFAULT_RESEARCH_TYPES,
    load_adjusted_panel,
    load_delisting_returns,
    research_engine,
    resolve_terminal_returns,
    securities_with_uncovered_events,
)
from research.evaluate import _forward_return, _markdown_table, _newey_west_t
from research.factors.builtins.path_momentum import information_discreteness_from_prices
from research.progress import Progress
from utils.trading_calendar import shift_trading_date

OUTPUT_DIR = Path(__file__).resolve().parent / "output"
DEFAULT_HORIZONS = (21, 63, 126)


def _parse_horizons(value: str) -> tuple[int, ...]:
    horizons = tuple(int(part.strip()) for part in value.split(",") if part.strip())
    if not horizons or any(h <= 0 for h in horizons):
        raise argparse.ArgumentTypeError("horizons must be positive comma-separated integers")
    return horizons


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--sample-role",
        choices=("primary", "stability"),
        default="primary",
    )
    parser.add_argument("--start", type=date.fromisoformat, default=date(2016, 1, 4))
    parser.add_argument("--end", type=date.fromisoformat, default=date(2026, 7, 2))
    parser.add_argument("--horizons", type=_parse_horizons, default=DEFAULT_HORIZONS)
    parser.add_argument("--n-quantiles", type=int, default=5)
    parser.add_argument("--min-cell-size", type=int, default=20)
    parser.add_argument("--min-price", type=float, default=3.0)
    parser.add_argument("--min-median-dollar-volume", type=float, default=2_000_000.0)
    return parser.parse_args(argv)


def _quantile_labels(
    values: np.ndarray,
    valid: np.ndarray,
    *,
    n_quantiles: int,
    min_per_quantile: int,
) -> np.ndarray:
    """稳定秩分桶；无效值为 0，有效桶为 1..n_quantiles。"""
    labels = np.zeros(len(values), dtype="int8")
    members = np.flatnonzero(valid & np.isfinite(values))
    if len(members) < n_quantiles * min_per_quantile:
        return labels
    order = np.argsort(values[members], kind="stable")
    ranked_members = members[order]
    buckets = np.minimum(
        np.arange(len(ranked_members)) * n_quantiles // len(ranked_members),
        n_quantiles - 1,
    ) + 1
    labels[ranked_members] = buckets.astype("int8")
    return labels


def _double_sort_labels(
    pret: np.ndarray,
    information_discreteness: np.ndarray,
    eligible: np.ndarray,
    *,
    mode: str,
    n_quantiles: int,
    min_cell_size: int,
) -> tuple[np.ndarray, np.ndarray]:
    valid = eligible & np.isfinite(pret) & np.isfinite(information_discreteness)
    pret_labels = _quantile_labels(
        pret,
        valid,
        n_quantiles=n_quantiles,
        min_per_quantile=n_quantiles * min_cell_size,
    )
    id_labels = np.zeros(len(pret), dtype="int8")
    if mode == "sequential":
        for pret_q in range(1, n_quantiles + 1):
            in_bucket = pret_labels == pret_q
            labels = _quantile_labels(
                information_discreteness,
                in_bucket,
                n_quantiles=n_quantiles,
                min_per_quantile=min_cell_size,
            )
            id_labels[in_bucket] = labels[in_bucket]
    elif mode == "independent":
        id_labels = _quantile_labels(
            information_discreteness,
            valid,
            n_quantiles=n_quantiles,
            min_per_quantile=n_quantiles * min_cell_size,
        )
    else:
        raise ValueError(f"unknown sort mode: {mode}")
    return pret_labels, id_labels


def monthly_double_sort_cells(
    pret: pd.DataFrame,
    information_discreteness: pd.DataFrame,
    eligible: pd.DataFrame,
    forward_returns: dict[int, pd.DataFrame],
    formation_dates: pd.DatetimeIndex,
    *,
    mode: str,
    n_quantiles: int = 5,
    min_cell_size: int = 20,
) -> pd.DataFrame:
    """返回 date × horizon × PRET quintile × ID quintile 的等权前向收益。"""
    rows: list[dict[str, object]] = []
    for formation_date in formation_dates:
        pret_row = pret.loc[formation_date].to_numpy(dtype="float64")
        id_row = information_discreteness.loc[formation_date].to_numpy(dtype="float64")
        eligible_row = eligible.loc[formation_date].to_numpy(dtype=bool)
        pret_labels, id_labels = _double_sort_labels(
            pret_row,
            id_row,
            eligible_row,
            mode=mode,
            n_quantiles=n_quantiles,
            min_cell_size=min_cell_size,
        )
        for horizon, panel in forward_returns.items():
            fwd = panel.loc[formation_date].to_numpy(dtype="float64")
            for pret_q in range(1, n_quantiles + 1):
                for id_q in range(1, n_quantiles + 1):
                    members = (
                        (pret_labels == pret_q)
                        & (id_labels == id_q)
                        & np.isfinite(fwd)
                    )
                    count = int(members.sum())
                    rows.append(
                        {
                            "date": formation_date,
                            "sort_mode": mode,
                            "horizon": int(horizon),
                            "pret_q": pret_q,
                            "id_q": id_q,
                            "mean_return": float(fwd[members].mean())
                            if count >= min_cell_size
                            else np.nan,
                            "n_stocks": count,
                        }
                    )
    return pd.DataFrame(rows)


def spread_series(cells: pd.DataFrame, *, n_quantiles: int = 5) -> pd.DataFrame:
    """由月度 5x5 单元格构造条件动量、赢家腿、输家腿和 FIP 交互。"""
    rows: list[dict[str, object]] = []
    for (sort_mode, horizon, formation_date), group in cells.groupby(
        ["sort_mode", "horizon", "date"], sort=True
    ):
        lookup = {
            (int(row.pret_q), int(row.id_q)): float(row.mean_return)
            for row in group.itertuples()
        }
        momentum = {
            id_q: lookup.get((n_quantiles, id_q), np.nan)
            - lookup.get((1, id_q), np.nan)
            for id_q in range(1, n_quantiles + 1)
        }
        winner_continuity = lookup.get((n_quantiles, 1), np.nan) - lookup.get(
            (n_quantiles, n_quantiles), np.nan
        )
        loser_continuity = lookup.get((1, n_quantiles), np.nan) - lookup.get((1, 1), np.nan)
        rows.append(
            {
                "date": formation_date,
                "sort_mode": sort_mode,
                "horizon": int(horizon),
                **{f"momentum_id_q{id_q}": momentum[id_q] for id_q in momentum},
                "winner_continuity": winner_continuity,
                "loser_continuity": loser_continuity,
                "fip_spread": momentum[1] - momentum[n_quantiles],
            }
        )
    return pd.DataFrame(rows)


def _monthly_nw_lag(horizon: int, n_obs: int) -> int:
    holding_months = math.ceil(horizon / 21)
    automatic = math.floor(4 * (n_obs / 100) ** (2 / 9)) if n_obs > 0 else 0
    return max(holding_months, automatic)


def summarize_spreads(spreads: pd.DataFrame, *, n_quantiles: int = 5) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for (sort_mode, horizon), group in spreads.groupby(["sort_mode", "horizon"], sort=True):
        fip = group["fip_spread"].dropna()
        winner = group["winner_continuity"].dropna()
        loser = group["loser_continuity"].dropna()
        lag = _monthly_nw_lag(int(horizon), len(fip))
        rows.append(
            {
                "sort_mode": sort_mode,
                "horizon": int(horizon),
                "n_months": len(fip),
                "nw_lag": lag,
                **{
                    f"momentum_id_q{id_q}": float(group[f"momentum_id_q{id_q}"].mean())
                    for id_q in range(1, n_quantiles + 1)
                },
                "winner_continuity": float(winner.mean()) if len(winner) else np.nan,
                "winner_nw_t": _newey_west_t(winner, lag),
                "loser_continuity": float(loser.mean()) if len(loser) else np.nan,
                "loser_nw_t": _newey_west_t(loser, lag),
                "fip_spread": float(fip.mean()) if len(fip) else np.nan,
                "fip_nw_t": _newey_west_t(fip, lag),
            }
        )
    return pd.DataFrame(rows).sort_values(["sort_mode", "horizon"]).reset_index(drop=True)


def _month_end_dates(index: pd.DatetimeIndex, *, start: date, end: date) -> pd.DatetimeIndex:
    dates = index[(index >= pd.Timestamp(start)) & (index <= pd.Timestamp(end))]
    if dates.empty:
        return dates
    series = pd.Series(dates, index=dates)
    return pd.DatetimeIndex(series.groupby(dates.to_period("M")).max().to_numpy())


def _json_records(frame: pd.DataFrame) -> list[dict[str, object]]:
    return json.loads(frame.to_json(orient="records", date_format="iso"))


def _window_verdict(summary: pd.DataFrame, *, sample_role: str) -> bool:
    seq = summary[summary["sort_mode"] == "sequential"].set_index("horizon")
    if sample_role == "stability":
        if 126 not in seq.index:
            return False
        return bool(seq.loc[126, "fip_spread"] > 0)
    if sample_role != "primary":
        raise ValueError(f"unknown sample role: {sample_role}")

    independent = summary[summary["sort_mode"] == "independent"].set_index("horizon")
    required = {21, 63, 126}
    if not required.issubset(seq.index) or 126 not in independent.index:
        return False
    primary = seq.loc[126]
    return bool(
        primary["fip_spread"] > 0
        and primary["fip_nw_t"] >= 3.0
        and seq.loc[21, "fip_spread"] > 0
        and seq.loc[63, "fip_spread"] > 0
        and primary["winner_continuity"] > 0
        and primary["loser_continuity"] > 0
        and independent.loc[126, "fip_spread"] > 0
    )


def main(argv: list[str] | None = None) -> int:
    load_dotenv()
    args = parse_args(argv)
    if args.start >= args.end:
        raise ValueError("start must be earlier than end")
    if args.n_quantiles != 5:
        raise ValueError("Wave 13 preregistration fixes n_quantiles=5")
    if args.min_cell_size != 20:
        raise ValueError("Wave 13 preregistration fixes min_cell_size=20")
    if tuple(args.horizons) != DEFAULT_HORIZONS:
        raise ValueError("Wave 13 preregistration fixes horizons=21,63,126")

    progress = Progress("path_quality")
    engine = research_engine()
    panel_start = args.start - timedelta(days=500)
    panel_end = shift_trading_date("US", args.end, max(args.horizons))
    with progress.stage("load adjusted panel"):
        panel = load_adjusted_panel(
            engine,
            start=panel_start,
            end=panel_end,
            types=DEFAULT_RESEARCH_TYPES,
            as_of=args.end,
        )
    bad = securities_with_uncovered_events(
        engine,
        start=args.start,
        end=panel_end,
        require_straddle=True,
    )
    if bad:
        for key in panel:
            panel[key] = panel[key].drop(columns=list(bad), errors="ignore")

    eligible = eligibility_mask(
        panel["close"],
        panel["dollar_volume"],
        min_price=args.min_price,
        min_median_dollar_volume=args.min_median_dollar_volume,
    )
    keep = eligible.any(axis=0)
    for key in panel:
        panel[key] = panel[key].loc[:, keep]
    eligible = eligible.loc[:, keep]
    adjusted_close = panel["adj_close"]
    formation_dates = _month_end_dates(adjusted_close.index, start=args.start, end=args.end)
    if formation_dates.empty:
        raise ValueError("no monthly formation dates in requested window")

    with progress.stage("compute PRET and information discreteness"):
        pret = adjusted_close.shift(21) / adjusted_close.shift(252) - 1
        information_discreteness = information_discreteness_from_prices(adjusted_close)

    realized = load_delisting_returns(engine)
    terminal_return, terminal_fallback = resolve_terminal_returns(realized, None, use_realized=True)
    with progress.stage("compute forward returns"):
        forward_returns = {
            horizon: _forward_return(
                adjusted_close,
                horizon,
                terminal_return=terminal_return,
                terminal_return_fallback=terminal_fallback,
            )
            for horizon in args.horizons
        }

    cells = []
    for mode in ("sequential", "independent"):
        with progress.stage(f"{mode} double sort"):
            cells.append(
                monthly_double_sort_cells(
                    pret,
                    information_discreteness,
                    eligible,
                    forward_returns,
                    formation_dates,
                    mode=mode,
                    n_quantiles=args.n_quantiles,
                    min_cell_size=args.min_cell_size,
                )
            )
    cell_frame = pd.concat(cells, ignore_index=True)
    spreads = spread_series(cell_frame, n_quantiles=args.n_quantiles)
    summary = summarize_spreads(spreads, n_quantiles=args.n_quantiles)
    cell_summary = (
        cell_frame.groupby(["sort_mode", "horizon", "pret_q", "id_q"], as_index=False)
        .agg(mean_return=("mean_return", "mean"), median_stocks=("n_stocks", "median"))
    )
    verdict = _window_verdict(summary, sample_role=args.sample_role)

    OUTPUT_DIR.mkdir(exist_ok=True)
    stem = f"path_quality_{args.sample_role}_{args.start}_{args.end}"
    markdown_path = OUTPUT_DIR / f"{stem}.md"
    json_path = OUTPUT_DIR / f"{stem}.json"
    payload = {
        "generated_at": pd.Timestamp.now(tz="UTC").isoformat(),
        "config": {
            "sample_role": args.sample_role,
            "start": str(args.start),
            "end": str(args.end),
            "horizons": list(args.horizons),
            "n_quantiles": args.n_quantiles,
            "min_cell_size": args.min_cell_size,
            "min_price": args.min_price,
            "min_median_dollar_volume": args.min_median_dollar_volume,
            "formation_dates": len(formation_dates),
            "universe_columns": len(adjusted_close.columns),
        },
        "window_verdict": verdict,
        "summary": _json_records(summary),
        "cell_summary": _json_records(cell_summary),
        "monthly_spreads": _json_records(spreads),
    }
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    markdown_path.write_text(
        "\n".join(
            [
                f"# 路径质量条件研究（{args.sample_role}）{args.start} ~ {args.end}",
                "",
                "主排序：先 PRET 五分位，再组内 ID 五分位；低 ID=连续，高 ID=离散。",
                f"形成月数={len(formation_dates)}，价格面板证券数={len(adjusted_close.columns)}。",
                "",
                "## 条件动量与 FIP 交互",
                "",
                _markdown_table(summary.round(4), include_index=False),
                "",
                "## 平均 5x5 单元格收益",
                "",
                _markdown_table(cell_summary.round(4), include_index=False),
                "",
                "## 预注册判据",
                "",
                (
                    "主样本：h126 FIP t>=3，并要求 h21/h63、赢家腿、输家腿和独立排序同号。"
                    if args.sample_role == "primary"
                    else "稳定性样本：顺序双排序 h126 FIP_spread 与主样本预期同号（>0）。"
                ),
                "",
                "PASS" if verdict else "FAIL",
                "",
                f"结构化结果：`{json_path.name}`",
            ]
        ),
        encoding="utf-8",
    )

    seq126 = summary[
        (summary["sort_mode"] == "sequential") & (summary["horizon"] == 126)
    ].iloc[0]
    append_study(
        study="path_quality",
        factor_name="information_discreteness_12_1",
        verdict=verdict,
        criteria=(
            "sequential h126 FIP t>=3; h21/h63同号; winner/loser双腿同号; "
            "independent h126同号"
            if args.sample_role == "primary"
            else "stability sequential h126 FIP spread > 0"
        ),
        params={
            "sample_role": args.sample_role,
            "horizons": args.horizons,
            "n_quantiles": args.n_quantiles,
            "min_cell_size": args.min_cell_size,
            "min_price": args.min_price,
            "min_median_dollar_volume": args.min_median_dollar_volume,
            "nw": "monthly max(holding_months, Andrews)",
        },
        eval_start=args.start,
        eval_end=args.end,
        report_path=str(markdown_path.relative_to(Path(__file__).resolve().parent.parent)),
        criterion_values={
            "fip_spread_h126": float(seq126["fip_spread"]),
            "fip_nw_t_h126": float(seq126["fip_nw_t"]),
            "winner_continuity_h126": float(seq126["winner_continuity"]),
            "loser_continuity_h126": float(seq126["loser_continuity"]),
        },
    )
    print(summary.round(4).to_string(index=False), flush=True)
    print(f"\nwindow verdict: {'PASS' if verdict else 'FAIL'}", flush=True)
    print(f"report: {markdown_path}\njson: {json_path}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
