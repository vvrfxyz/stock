"""Independently audit the frozen Wave 15 market-regime outputs."""
from __future__ import annotations

import hashlib
import json
import math
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

OUTPUT_DIR = Path(__file__).resolve().parent / "output"
V1_STEM = "market_regime_overlay_2007-07-02_2026-07-10"
V2_STEM = "market_regime_overlay_v2_2007-07-02_2026-07-10"
AUDIT_PATH = OUTPUT_DIR / "wave15_market_regime_overlay_independent_audit.json"
TRIALS_PATH = OUTPUT_DIR / "wave15_authoritative_trials_2026-07-12.parquet"

RULES = ("spy_10m_trend", "breadth_200d", "trend_and_breadth")
ASSETS = ("spy", "pit_cs_equal_weight")
PATHS = ("buy_and_hold", *RULES)
PRIMARY_COST_BPS = {"spy": 2.0, "pit_cs_equal_weight": 25.0}
SAMPLES = {
    "stability": (date(2007, 7, 2), date(2015, 12, 31)),
    "primary": (date(2016, 1, 4), date(2026, 7, 10)),
}
CRISIS_YEARS = (2008, 2020, 2022)
TRADING_DAYS = 252


def _read(suffix: str, *, stem: str = V2_STEM) -> pd.DataFrame:
    return pd.read_parquet(OUTPUT_DIR / f"{stem}_{suffix}.parquet")


def _records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    return json.loads(frame.to_json(orient="records", date_format="iso"))


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _path_metrics(frame: pd.DataFrame) -> dict[str, float]:
    returns = frame["net_return"].to_numpy(dtype="float64")
    excess = frame["net_return"] - frame["risk_free_return"]
    if len(returns) == 0 or (returns <= -1.0).any():
        raise ValueError("invalid return sample")
    equity = np.r_[1.0, np.cumprod(1.0 + returns)]
    drawdown = equity / np.maximum.accumulate(equity) - 1.0
    years = len(returns) / TRADING_DAYS
    return {
        "n_days": float(len(returns)),
        "cagr": float(equity[-1] ** (1.0 / years) - 1.0),
        "sharpe": float(
            excess.mean() / excess.std(ddof=1) * math.sqrt(TRADING_DAYS)
        ),
        "max_drawdown": float(drawdown.min()),
    }


def recompute_decision_cells(daily: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for asset in ASSETS:
        asset_frame = daily[daily["asset"] == asset]
        cost_bps = PRIMARY_COST_BPS[asset]
        for sample, (start, end) in SAMPLES.items():
            selected = asset_frame[
                (asset_frame["date"] >= pd.Timestamp(start))
                & (asset_frame["date"] <= pd.Timestamp(end))
            ].copy()
            selected["net_return"] = (
                selected["gross_return"]
                - selected["turnover"] * cost_bps / 10_000.0
            )
            baseline = _path_metrics(selected[selected["rule"] == "buy_and_hold"])
            for rule in RULES:
                metrics = _path_metrics(selected[selected["rule"] == rule])
                drawdown_improvement = (
                    metrics["max_drawdown"] - baseline["max_drawdown"]
                )
                sharpe_improvement = metrics["sharpe"] - baseline["sharpe"]
                cagr_loss = baseline["cagr"] - metrics["cagr"]
                passes_drawdown = drawdown_improvement >= 0.10
                passes_sharpe = sharpe_improvement >= 0.10
                passes_cagr = cagr_loss <= 0.02
                rows.append(
                    {
                        "asset": asset,
                        "sample": sample,
                        "rule": rule,
                        "cost_bps": cost_bps,
                        **metrics,
                        "baseline_cagr": baseline["cagr"],
                        "baseline_sharpe": baseline["sharpe"],
                        "baseline_max_drawdown": baseline["max_drawdown"],
                        "drawdown_improvement": drawdown_improvement,
                        "sharpe_improvement": sharpe_improvement,
                        "cagr_loss": cagr_loss,
                        "passes_drawdown": passes_drawdown,
                        "passes_sharpe": passes_sharpe,
                        "passes_cagr": passes_cagr,
                        "cell_pass": bool(
                            passes_drawdown and passes_sharpe and passes_cagr
                        ),
                    }
                )
    return pd.DataFrame(rows)


def recompute_crises(daily: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for asset in ASSETS:
        cost_bps = PRIMARY_COST_BPS[asset]
        for year in CRISIS_YEARS:
            for rule in PATHS:
                frame = daily[
                    (daily["asset"] == asset)
                    & (daily["rule"] == rule)
                    & (daily["date"].dt.year == year)
                ].copy()
                returns = (
                    frame["gross_return"]
                    - frame["turnover"] * cost_bps / 10_000.0
                ).to_numpy(dtype="float64")
                equity = np.r_[1.0, np.cumprod(1.0 + returns)]
                drawdown = equity / np.maximum.accumulate(equity) - 1.0
                rows.append(
                    {
                        "asset": asset,
                        "year": year,
                        "rule": rule,
                        "cost_bps": cost_bps,
                        "total_return": float(equity[-1] - 1.0),
                        "max_drawdown": float(drawdown.min()),
                        "avg_target_exposure": float(frame["target_exposure"].mean()),
                    }
                )
    return pd.DataFrame(rows)


def _weighted_quantile(
    values: pd.Series,
    weights: pd.Series,
    quantile: float,
) -> float:
    valid = values.notna() & weights.notna() & (weights > 0.0)
    ordered = pd.DataFrame(
        {"value": values.loc[valid], "weight": weights.loc[valid]}
    ).sort_values("value", kind="stable")
    cutoff = quantile * ordered["weight"].sum()
    position = int(
        np.searchsorted(ordered["weight"].cumsum().to_numpy(), cutoff, side="left")
    )
    return float(ordered["value"].iloc[min(position, len(ordered) - 1)])


def recompute_spread_summary(
    trades: pd.DataFrame,
    sessions: pd.DatetimeIndex,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for sample, (start, end) in SAMPLES.items():
        selected = trades[
            (trades["date"] >= pd.Timestamp(start))
            & (trades["date"] <= pd.Timestamp(end))
        ]
        years = int(
            ((sessions >= pd.Timestamp(start)) & (sessions <= pd.Timestamp(end))).sum()
        ) / TRADING_DAYS
        for rule, frame in selected.groupby("rule", sort=True):
            covered = frame["cost_bps"].notna()
            total_weight = float(frame["abs_weight"].sum())
            covered_weight = float(frame.loc[covered, "abs_weight"].sum())
            drag = float(
                (
                    frame.loc[covered, "abs_weight"]
                    * frame.loc[covered, "cost_bps"]
                    / 10_000.0
                ).sum()
            )
            rows.append(
                {
                    "sample": sample,
                    "rule": rule,
                    "total_trade_weight": total_weight,
                    "covered_trade_weight": covered_weight,
                    "trade_weight_coverage": covered_weight / total_weight,
                    "weighted_mean_cost_bps": float(
                        np.average(
                            frame.loc[covered, "cost_bps"],
                            weights=frame.loc[covered, "abs_weight"],
                        )
                    ),
                    "weighted_p25_cost_bps": _weighted_quantile(
                        frame.loc[covered, "cost_bps"],
                        frame.loc[covered, "abs_weight"],
                        0.25,
                    ),
                    "weighted_median_cost_bps": _weighted_quantile(
                        frame.loc[covered, "cost_bps"],
                        frame.loc[covered, "abs_weight"],
                        0.50,
                    ),
                    "weighted_p75_cost_bps": _weighted_quantile(
                        frame.loc[covered, "cost_bps"],
                        frame.loc[covered, "abs_weight"],
                        0.75,
                    ),
                    "covered_cost_drag_sum": drag,
                    "covered_cost_drag_ann_arithmetic": drag / years,
                }
            )
    return pd.DataFrame(rows)


def _max_errors(
    actual: pd.DataFrame,
    expected: pd.DataFrame,
    *,
    keys: list[str],
    fields: list[str],
) -> dict[str, float]:
    joined = actual.merge(
        expected[keys + fields],
        on=keys,
        suffixes=("_actual", "_expected"),
        validate="one_to_one",
    )
    return {
        field: float(
            (joined[f"{field}_actual"] - joined[f"{field}_expected"]).abs().max()
        )
        for field in fields
    }


def _ledger_rows() -> list[dict[str, Any]]:
    trials = pd.read_parquet(TRIALS_PATH)
    kind = trials["trial_kind"].astype(object).fillna("evaluate").astype(str)
    rows = trials[
        (kind == "study")
        & (trials["run_id"] == "market_regime_overlay")
        & (trials["metric"] == "study_verdict")
    ].copy()
    rows["study_version"] = rows["params_json"].map(
        lambda raw: json.loads(raw)["study_version"]
    )
    return _records(
        rows[
            ["trial_id", "created_at", "factor_name", "value", "study_version"]
        ].sort_values(["created_at", "factor_name"])
    )


def run() -> dict[str, Any]:
    daily = _read("daily")
    daily["date"] = pd.to_datetime(daily["date"])
    metrics = _read("metrics")
    crises = _read("crises")
    signals = _read("signals")
    signals["date"] = pd.to_datetime(signals["date"])
    trades = _read("measured_trades")
    trades["date"] = pd.to_datetime(trades["date"])
    spread_summary = _read("spread_summary")
    sensitivity = _read("delisting_sensitivity")
    with (OUTPUT_DIR / f"{V2_STEM}.json").open(encoding="utf-8") as handle:
        study_payload = json.load(handle)

    decision = recompute_decision_cells(daily)
    crisis_recomputed = recompute_crises(daily)
    spread_recomputed = recompute_spread_summary(
        trades,
        pd.DatetimeIndex(sorted(daily["date"].unique())),
    )

    primary_summary = metrics[
        metrics.apply(
            lambda row: row["cost_bps"] == PRIMARY_COST_BPS[row["asset"]],
            axis=1,
        )
    ]
    decision_errors = _max_errors(
        decision,
        primary_summary,
        keys=["asset", "sample", "rule", "cost_bps"],
        fields=[
            "cagr",
            "sharpe",
            "max_drawdown",
            "baseline_cagr",
            "baseline_sharpe",
            "baseline_max_drawdown",
            "drawdown_improvement",
            "sharpe_improvement",
            "cagr_loss",
        ],
    )
    crisis_errors = _max_errors(
        crisis_recomputed,
        crises,
        keys=["asset", "year", "rule", "cost_bps"],
        fields=["total_return", "max_drawdown", "avg_target_exposure"],
    )
    spread_errors = _max_errors(
        spread_recomputed,
        spread_summary,
        keys=["sample", "rule"],
        fields=[
            "total_trade_weight",
            "covered_trade_weight",
            "trade_weight_coverage",
            "weighted_mean_cost_bps",
            "weighted_p25_cost_bps",
            "weighted_median_cost_bps",
            "weighted_p75_cost_bps",
            "covered_cost_drag_sum",
            "covered_cost_drag_ann_arithmetic",
        ],
    )

    stock_main = primary_summary[primary_summary["asset"] == "pit_cs_equal_weight"]
    stock_sensitivity = sensitivity[
        (sensitivity["asset"] == "pit_cs_equal_weight")
        & (sensitivity["cost_bps"] == 25.0)
    ]
    sensitivity_join = stock_main.merge(
        stock_sensitivity,
        on=["asset", "sample", "rule", "cost_bps"],
        suffixes=("_main", "_sensitivity"),
        validate="one_to_one",
    )

    v1 = _read("metrics", stem=V1_STEM)
    v1_primary = v1[
        v1.apply(
            lambda row: row["cost_bps"] == PRIMARY_COST_BPS[row["asset"]],
            axis=1,
        )
    ]
    version_join = primary_summary.merge(
        v1_primary,
        on=["asset", "sample", "rule", "cost_bps"],
        suffixes=("_v2", "_v1"),
        validate="one_to_one",
    )

    numeric = daily.select_dtypes(include=[np.number])
    cost_identity_error = (
        daily["primary_net_return"]
        - (
            daily["gross_return"]
            - daily["turnover"] * daily["primary_cost_bps"] / 10_000.0
        )
    ).abs()
    first_breadth = pd.Timestamp(
        study_payload["audit"]["first_valid_signal_dates"]["breadth_200d"]
    )
    breadth_denominator = signals.loc[
        signals["date"] >= first_breadth,
        "breadth_denominator",
    ]
    expected_rows = daily["date"].nunique() * len(ASSETS) * len(PATHS)

    payload = {
        "generated_at": study_payload["generated_at"],
        "study_version": study_payload["config"]["study_version"],
        "verdicts": {
            rule: bool(decision.loc[decision["rule"] == rule, "cell_pass"].all())
            for rule in RULES
        },
        "decision_cells": _records(decision),
        "crisis_cells": _records(crisis_recomputed),
        "spread_summary": _records(spread_recomputed),
        "robustness": {
            "delisting_sensitivity_cells": _records(
                sensitivity_join[
                    [
                        "asset",
                        "sample",
                        "rule",
                        "cost_bps",
                        "cell_pass_main",
                        "cell_pass_sensitivity",
                        "cagr_main",
                        "cagr_sensitivity",
                        "sharpe_main",
                        "sharpe_sensitivity",
                        "max_drawdown_main",
                        "max_drawdown_sensitivity",
                    ]
                ]
            ),
            "delisting_pass_fail_changes": int(
                (
                    sensitivity_join["cell_pass_main"]
                    != sensitivity_join["cell_pass_sensitivity"]
                ).sum()
            ),
            "primary_unresolved_missing_weight": _records(
                daily[
                    (daily["asset"] == "pit_cs_equal_weight")
                    & (daily["date"] >= pd.Timestamp(SAMPLES["primary"][0]))
                ]
                .groupby("rule", as_index=False)
                .agg(
                    mean_unresolved_missing_weight=(
                        "unresolved_missing_weight",
                        "mean",
                    ),
                    max_unresolved_missing_weight=(
                        "unresolved_missing_weight",
                        "max",
                    ),
                )
            ),
            "v1_v2_max_abs_delta": {
                field: float(
                    (
                        version_join[f"{field}_v2"]
                        - version_join[f"{field}_v1"]
                    ).abs().max()
                )
                for field in (
                    "cagr",
                    "sharpe",
                    "max_drawdown",
                    "drawdown_improvement",
                    "sharpe_improvement",
                    "cagr_loss",
                )
            },
            "v1_v2_verdict_changes": int(
                (version_join["cell_pass_v2"] != version_join["cell_pass_v1"]).sum()
            ),
        },
        "data_quality": {
            "daily_rows": int(len(daily)),
            "expected_daily_rows": int(expected_rows),
            "daily_key_duplicates": int(
                daily.duplicated(["date", "asset", "rule"]).sum()
            ),
            "non_finite_numeric_values": int(
                (~np.isfinite(numeric.to_numpy(dtype="float64"))).sum()
            ),
            "negative_turnover_rows": int((daily["turnover"] < 0.0).sum()),
            "out_of_range_target_exposure_rows": int(
                ((daily["target_exposure"] < 0.0) | (daily["target_exposure"] > 1.0)).sum()
            ),
            "cost_identity_max_abs_error": float(cost_identity_error.max()),
            "signal_rows": int(len(signals)),
            "signal_date_duplicates": int(signals["date"].duplicated().sum()),
            "last_signal_date": str(signals["date"].max().date()),
            "breadth_denominator_min": int(breadth_denominator.min()),
            "breadth_denominator_median": float(breadth_denominator.median()),
            "breadth_denominator_max": int(breadth_denominator.max()),
            "minute_spread_feature_end": study_payload["audit"][
                "minute_spread_feature_end"
            ],
            "measured_spread_trade_match_end": study_payload["audit"][
                "measured_spread_trade_match_end"
            ],
        },
        "comparison_errors": {
            "decision_cells": decision_errors,
            "crisis_cells": crisis_errors,
            "spread_summary": spread_errors,
        },
        "ledger_verdicts": _ledger_rows(),
        "source_files": {
            path.name: _sha256(path)
            for path in sorted(OUTPUT_DIR.glob(f"{V2_STEM}*"))
            if path.is_file()
        },
    }
    AUDIT_PATH.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return payload


if __name__ == "__main__":
    result = run()
    print(json.dumps({
        "verdicts": result["verdicts"],
        "data_quality": result["data_quality"],
        "comparison_errors": result["comparison_errors"],
        "audit_path": str(AUDIT_PATH),
    }, ensure_ascii=False, indent=2))
