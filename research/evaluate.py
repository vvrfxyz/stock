from __future__ import annotations

import argparse
import hashlib
import inspect
import json
import math
from dataclasses import dataclass, replace
from datetime import date, timedelta
from functools import lru_cache
from pathlib import Path
from typing import Any, Callable, Mapping

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from loguru import logger
from sqlalchemy.engine import Engine

from research.backtest import eligibility_mask, hold_between_rebalances, run_backtest
from research.data import (
    ADR_TYPES,
    DEFAULT_RESEARCH_TYPES,
    FACTOR_TRUST_FLOOR,
    RESEARCH_TYPES_WITH_ADR,
    load_adjusted_panel,
    load_delisting_returns,
    research_engine,
    resolve_terminal_returns,
    securities_with_uncovered_events,
    uncovered_gate_version,
)
from research.factors.builtins import accruals as _accruals  # noqa: F401
from research.factors.builtins import bar_geometry as _bar_geometry  # noqa: F401
from research.factors.builtins import classic_pillars as _classic_pillars  # noqa: F401
from research.factors.builtins import classic_price as _classic_price  # noqa: F401
from research.factors.builtins import composite_v1 as _composite_v1  # noqa: F401
from research.factors.builtins import days_to_cover as _days_to_cover  # noqa: F401
from research.factors.builtins import delta_institutional_ownership as _delta_inst_own  # noqa: F401
from research.factors.builtins import earnings_yield as _earnings_yield  # noqa: F401
from research.factors.builtins import eod_pressure as _eod_pressure  # noqa: F401
from research.factors.builtins import gross_profitability as _gross_profitability  # noqa: F401
from research.factors.builtins import insider_cluster as _insider_cluster  # noqa: F401
from research.factors.builtins import insider_net_buy as _insider_net_buy  # noqa: F401
from research.factors.builtins import institutional_breadth as _institutional_breadth  # noqa: F401
from research.factors.builtins import intraday_flow as _intraday_flow  # noqa: F401
from research.factors.builtins import intraday_moments as _intraday_moments  # noqa: F401
from research.factors.builtins import ownership_concentration as _ownership_concentration  # noqa: F401
from research.factors.builtins import operating_profitability as _operating_profitability  # noqa: F401
from research.factors.builtins import residual_vol as _residual_vol  # noqa: F401
from research.factors.builtins import short_interest as _short_interest  # noqa: F401
from research.factors.builtins import short_volume as _short_volume  # noqa: F401
from research.factors.builtins import size as _size  # noqa: F401
from research.factors.builtins import ta_combo as _ta_combo  # noqa: F401
from research.factors.builtins import ta_zoo as _ta_zoo  # noqa: F401
from research.factors.protocol import Factor, FactorContext, get, list_factors
from research.progress import Progress
from utils.risk_free_rates import DEFAULT_SERIES_ID as DEFAULT_RISK_FREE_SERIES, load_risk_free_daily_returns
from utils.trading_calendar import shift_trading_date

OUTPUT_DIR = Path(__file__).resolve().parent / "output"
DEFAULT_TRIALS_PATH = OUTPUT_DIR / "trials.parquet"
DEFAULT_HORIZONS = (1, 5, 10, 21)
# 因子链已覆盖 2003+（FACTOR_TRUST_FLOOR），但 20 年全市场面板内存/耗时大，
# 默认评估窗口仍取原 2024-05-14；长窗口评估显式传 --start。
DEFAULT_EVAL_PANEL_START = date(2024, 5, 14)
NOISE_THRESHOLD = 3.0
MIN_OBS = 60


class FactorEvaluationError(Exception):
    pass


@lru_cache(maxsize=1)
def _engine_code_fingerprint() -> str:
    """回测/评估引擎源码指纹；git 树 dirty 时 code_git_sha 不变，指纹仍能区分引擎改动。"""
    import research.backtest as _backtest_module

    digests = []
    for module_file in (Path(_backtest_module.__file__), Path(__file__)):
        digests.append(hashlib.sha1(module_file.read_bytes()).hexdigest()[:12])
    return "-".join(digests)


def compute_trial_id(
    *,
    factor_name: str,
    factor_version: str,
    universe_hash: str,
    params_hash: str,
    eval_start: str,
    eval_end: str,
    as_of: str,
    code_git_sha: str | None,
) -> str:
    """trial_id 的唯一推导（EvaluationResult._trial_id_value 与 --skip-existing 共用）。

    注意 universe_hash 是面板装载 + gate 之后才有的量——skip 判定因此只能活在
    run_evaluation 内部（factor.compute 之前），做不到"跑之前"就构造 trial_id。
    """
    raw = "|".join(
        [
            factor_name,
            factor_version,
            universe_hash,
            params_hash,
            f"{eval_start}:{eval_end}",
            as_of,
            code_git_sha or "",
            _engine_code_fingerprint(),
        ]
    )
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def _normalize_dates(index: pd.Index) -> pd.DatetimeIndex:
    return pd.DatetimeIndex(pd.to_datetime(index)).astype("datetime64[ns]")


def _clean_json(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(k): _clean_json(v) for k, v in sorted(value.items(), key=lambda item: str(item[0]))}
    if isinstance(value, (list, tuple)):
        return [_clean_json(v) for v in value]
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, (date, np.datetime64)):
        return pd.Timestamp(value).date().isoformat()
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        value = float(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("params_hash refuses NaN/Inf")
        return repr(value)
    if value is None or isinstance(value, (str, bool, int)):
        return value
    return repr(value)


def _canonical_json(value: Mapping[str, Any]) -> str:
    return json.dumps(_clean_json(value), sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _params_without_note(config: Mapping[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in config.items() if k not in {"note", "run_id"}}


def _params_hash(config: Mapping[str, Any]) -> str:
    return hashlib.sha256(_canonical_json(_params_without_note(config)).encode("utf-8")).hexdigest()


def _terminal_return_config(
    terminal_return: float | pd.Series | None,
    terminal_return_fallback: float | None,
) -> dict[str, Any]:
    """退市终局口径的 config 快照（进 params_hash，区分 trials 新旧口径）。

    Series 本身不可哈希进 config：mode 记 'realized_series'，标量值只在标量口径下记录。
    run_evaluation 与 evaluate_factor 必须用同一个推导（config 覆盖合并时值须一致）。
    """
    if isinstance(terminal_return, pd.Series):
        mode = "realized_series"
        scalar = None
    elif terminal_return is not None:
        mode = "scalar"
        scalar = float(terminal_return)
    else:
        mode = "none"
        scalar = None
    return {
        "terminal_return_mode": mode,
        "terminal_return_scalar": scalar,
        "terminal_return_fallback": terminal_return_fallback,
    }


def _factor_params_snapshot(factor: Factor) -> dict[str, Any]:
    raw = getattr(factor, "__dict__", {}) or {}
    return {k: v for k, v in raw.items() if not k.startswith("_") and k != "engine"}


def _universe_hash(columns: pd.Index) -> str:
    values = sorted(int(c) for c in columns)
    return hashlib.sha1(json.dumps(values, separators=(",", ":")).encode("utf-8")).hexdigest()


def default_nw_lag(horizon: int, n_obs: int) -> int:
    if n_obs <= 0:
        return 0
    return int(max(horizon, math.floor(4 * (n_obs / 100) ** (2 / 9))))


def _newey_west_t(values: pd.Series, lag: int) -> float:
    x = values.dropna().astype(float).to_numpy()
    t = len(x)
    if t < 2:
        return np.nan
    demeaned = x - x.mean()
    effective_lag = min(max(int(lag), 0), t - 1)
    long_run_var = float(np.dot(demeaned, demeaned) / t)
    for k in range(1, effective_lag + 1):
        cov = float(np.dot(demeaned[k:], demeaned[:-k]) / t)
        long_run_var += 2 * (1 - k / (effective_lag + 1)) * cov
    if long_run_var <= 0:
        mean = float(x.mean())
        if mean == 0:
            return np.nan
        return math.copysign(np.inf, mean)
    return float(x.mean() / math.sqrt(long_run_var / t))


def _masked_rowwise_corr(
    x: np.ndarray, y: np.ndarray, valid: np.ndarray, min_coverage: int
) -> np.ndarray:
    """逐行 Pearson 相关（仅 valid 对参与；行内有效数 < min_coverage 或零方差 → NaN）。

    与旧参照实现（逐日 .loc 提取 + Series.corr）数值一致，但为单遍 numpy——
    2026-07 性能重构：旧路径每次调用做 T 次面板行提取（take_2d memmove 热点）。
    """
    xm = np.where(valid, x, np.nan)
    ym = np.where(valid, y, np.nan)
    n = valid.sum(axis=1).astype("float64")
    with np.errstate(invalid="ignore", divide="ignore"):
        mean_x = np.nansum(xm, axis=1) / n
        mean_y = np.nansum(ym, axis=1) / n
        dx = xm - mean_x[:, None]
        dy = ym - mean_y[:, None]
        sxx = np.nansum(dx * dx, axis=1)
        syy = np.nansum(dy * dy, axis=1)
        sxy = np.nansum(dx * dy, axis=1)
        corr = sxy / np.sqrt(sxx * syy)
    corr[(n < min_coverage) | (sxx == 0) | (syy == 0)] = np.nan
    return corr


def _rank_ic_series(factor: pd.DataFrame, forward_return: pd.DataFrame, min_coverage: int) -> pd.Series:
    aligned_factor, aligned_return = factor.align(forward_return, join="left", axis=None)
    valid = (aligned_factor.notna() & aligned_return.notna()).to_numpy()
    f_rank = aligned_factor.rank(axis=1, method="average", na_option="keep").to_numpy()
    r_rank = aligned_return.rank(axis=1, method="average", na_option="keep").to_numpy()
    corr = _masked_rowwise_corr(f_rank, r_rank, valid, min_coverage)
    return pd.Series(corr, index=aligned_factor.index, dtype="float64")


def _ic_decay_table(
    factor: pd.DataFrame,
    forward_returns: Mapping[int, pd.DataFrame],
    horizons: tuple[int, ...],
    min_coverage: int,
) -> pd.DataFrame:
    """IC 衰减表。性能关键：行平移与行内排名可交换（rank∘shift == shift∘rank），
    每个 horizon 只排名一次，lag 平移用 numpy 视图——旧实现按 (horizon, lag)
    重复整面板 shift+rank+逐日循环（max_lag+1 × horizons 次）。"""
    rows: list[dict[str, float | int]] = []
    max_lag = max(horizons) if horizons else 0
    f_rank = factor.rank(axis=1, method="average", na_option="keep").to_numpy()
    f_valid = factor.notna().to_numpy()
    total = len(factor.index)
    for horizon in horizons:
        returns = forward_returns[horizon].reindex(index=factor.index, columns=factor.columns)
        r_rank_full = returns.rank(axis=1, method="average", na_option="keep").to_numpy()
        r_valid_full = returns.notna().to_numpy()
        for lag in range(max_lag + 1):
            if lag == 0:
                r_rank, r_valid = r_rank_full, r_valid_full
            else:
                r_rank = np.full_like(r_rank_full, np.nan)
                r_rank[: total - lag] = r_rank_full[lag:]
                r_valid = np.zeros_like(r_valid_full)
                r_valid[: total - lag] = r_valid_full[lag:]
            corr = _masked_rowwise_corr(f_rank, r_rank, f_valid & r_valid, min_coverage)
            finite = corr[~np.isnan(corr)]
            ic = float(finite.mean()) if len(finite) else np.nan
            rows.append({"horizon": horizon, "lag": lag, "ic": ic})
    return pd.DataFrame(rows, columns=["horizon", "lag", "ic"])


def _decay_halflife(ic_decay: pd.DataFrame, horizon: int) -> float:
    series = ic_decay[ic_decay["horizon"] == horizon].sort_values("lag")["ic"].abs().reset_index(drop=True)
    if series.empty or pd.isna(series.iloc[0]) or series.iloc[0] == 0:
        return np.nan
    threshold = series.iloc[0] / 2
    hits = series[series <= threshold]
    return float(hits.index[0]) if not hits.empty else np.inf


def _forward_return(
    adj_close: pd.DataFrame,
    horizon: int,
    *,
    terminal_return: float | pd.Series | None = None,
    terminal_return_fallback: float | None = None,
) -> pd.DataFrame:
    """未来 horizon 交易日的价格前向收益（ffill 后按行移位；停牌/缺口跳空计入复牌日）。

    退市终局注入（terminal_return 非 None 时启用，语义与 run_backtest 逐位对齐）：
    设证券最后有效价日为 t_last（= backtest first_terminal 前一日）、退市实测收益 r_d。
    对满足 t <= t_last < t+horizon 的观察（退市落在前瞻窗内），前向收益并入 r_d：
        fwd(t) = (1 + price[t_last]/price[t] - 1) × (1 + r_d) − 1
               = price[t_last]/price[t] × (1 + r_d) − 1
    这与 run_backtest 的逐日复合恒等：t+1..t_last 走价格路径、t_last+1（退市事件日）
    这一天注入 r_d、其后各日 0。r_d 的解析与 run_backtest 完全同构：Series 按证券
    reindex、fallback 补洞后仍缺的列不注入；标量对所有退市列统一注入；None 不注入。
    价格路径分量用 t_last 的价（ffill 末值）直取，退市落在面板尾 horizon 内（t+horizon
    越界）时仍可注入——退市已提前锁定收益，不因面板不够长而丢观察。t_last 之后的行
    保持 NaN（退市后无观察，不改变旧行为）。

    默认 terminal_return=None 时行为与旧实现逐位一致（纯 ffill 前向收益）。
    """
    filled = adj_close.ffill()
    shifted = filled.shift(-horizon)
    valid_pair = adj_close.notna() & shifted.notna()
    fwd = (shifted / filled - 1).where(valid_pair)
    if terminal_return is None:
        return fwd

    columns = adj_close.columns
    if isinstance(terminal_return, pd.Series):
        per_security = terminal_return.reindex(columns).astype("float64")
        if terminal_return_fallback is not None:
            per_security = per_security.fillna(terminal_return_fallback)
        r_d = per_security.to_numpy()
    else:
        r_d = np.full(len(columns), float(terminal_return), dtype="float64")

    valid = adj_close.notna().to_numpy()
    n_rows = valid.shape[0]
    any_valid = valid.any(axis=0)
    # 每列最后有效价日 t_last（全 NaN 列记 -1）；退市事件日 f_pos = t_last + 1，恰等于
    # backtest 的 first_terminal（trailing 永久缺失块首日）——内部停牌不误判为退市。
    last_valid_pos = np.where(any_valid, n_rows - 1 - valid[::-1].argmax(axis=0), -1)
    f_pos = last_valid_pos + 1
    has_terminal = last_valid_pos < (n_rows - 1)          # 尾部转永久缺失才算退市
    injectable = has_terminal & ~np.isnan(r_d)            # 无实测/无 fallback 的列不注入
    if not injectable.any():
        return fwd

    row_idx = np.arange(n_rows)[:, None]
    within_window = (row_idx < f_pos[None, :]) & (row_idx >= (f_pos - horizon)[None, :])
    inject_mask = within_window & injectable[None, :]
    # 价格路径分量 = price[t_last] / price[t]；last 价取 ffill 末行（注入列 = t_last 价）。
    # base 用原始价（非 ffill）：停牌行 base=NaN -> 结果 NaN，与旧口径 halt 行一致。
    last_valid_price = filled.to_numpy()[-1]
    base_price = adj_close.to_numpy()
    r_d_row = np.where(injectable, r_d, 0.0)[None, :]
    with np.errstate(invalid="ignore", divide="ignore"):
        compounded = last_valid_price[None, :] / base_price * (1.0 + r_d_row) - 1.0
    out = np.where(inject_mask, compounded, fwd.to_numpy())
    return pd.DataFrame(out, index=adj_close.index, columns=columns)


def _quantile_weights_for_day(signal: pd.Series, eligible: pd.Series, n_quantiles: int) -> dict[str, pd.Series]:
    base = pd.Series(0.0, index=signal.index, dtype="float64")
    tradable = signal[eligible.fillna(False) & signal.notna()]
    if len(tradable) < 100:
        return {f"q{i}": base.copy() for i in range(1, n_quantiles + 1)} | {f"ls_q{n_quantiles}_q1": base.copy()}
    ranks = tradable.rank(method="first")
    labels = np.minimum(((ranks - 1) * n_quantiles // len(tradable)).astype(int) + 1, n_quantiles)
    out: dict[str, pd.Series] = {}
    for q in range(1, n_quantiles + 1):
        weights = base.copy()
        members = labels.index[labels == q]
        if len(members) > 0:
            weights.loc[members] = 1.0 / len(members)
        out[f"q{q}"] = weights
    long_short = base.copy()
    low = labels.index[labels == 1]
    high = labels.index[labels == n_quantiles]
    if len(low) > 0 and len(high) > 0:
        long_short.loc[high] = 0.5 / len(high)
        long_short.loc[low] = -0.5 / len(low)
    out[f"ls_q{n_quantiles}_q1"] = long_short
    return out


def _quantile_weight_matrices(
    factor_slice: pd.DataFrame,
    eligibility_slice: pd.DataFrame,
    n_quantiles: int,
) -> dict[str, np.ndarray]:
    """再平衡日全体的分位权重矩阵（向量化版 _quantile_weights_for_day）。

    与旧逐日实现语义逐位一致：rank(method='first') 平票按列序、
    tradable<100 的行全零、LS 头尾各 ±0.5 且任一侧为空则全零。"""
    tradable = eligibility_slice.fillna(False).astype(bool).to_numpy() & factor_slice.notna().to_numpy()
    counts = tradable.sum(axis=1)
    ranks = factor_slice.where(pd.DataFrame(
        tradable, index=factor_slice.index, columns=factor_slice.columns)).rank(
        axis=1, method="first").to_numpy()
    valid_row = counts >= 100
    denom = np.maximum(counts, 1)[:, None].astype("float64")
    with np.errstate(invalid="ignore"):
        labels = np.floor_divide((ranks - 1) * n_quantiles, denom) + 1
    labels = np.where(np.isnan(ranks), 0, np.minimum(labels, n_quantiles)).astype("int64")
    labels[~valid_row] = 0

    out: dict[str, np.ndarray] = {}
    for q in range(1, n_quantiles + 1):
        members = labels == q
        row_n = members.sum(axis=1)[:, None].astype("float64")
        with np.errstate(divide="ignore", invalid="ignore"):
            weights = np.where(members, 1.0 / np.maximum(row_n, 1), 0.0)
        weights[row_n[:, 0] == 0] = 0.0
        out[f"q{q}"] = weights
    high = labels == n_quantiles
    low = labels == 1
    n_high = high.sum(axis=1)[:, None].astype("float64")
    n_low = low.sum(axis=1)[:, None].astype("float64")
    both = (n_high[:, 0] > 0) & (n_low[:, 0] > 0)
    long_short = (np.where(high, 0.5 / np.maximum(n_high, 1), 0.0)
                  - np.where(low, 0.5 / np.maximum(n_low, 1), 0.0))
    long_short[~both] = 0.0
    out[f"ls_q{n_quantiles}_q1"] = long_short
    return out


def _quantile_metrics(
    factor: pd.DataFrame,
    eligibility: pd.DataFrame,
    adj_close: pd.DataFrame | None,
    horizons: tuple[int, ...],
    n_quantiles: int,
    cost_bps: float,
    risk_free_returns: pd.Series | None = None,
    terminal_return: float | pd.Series | None = None,
    terminal_return_fallback: float | None = None,
) -> pd.DataFrame:
    columns = ["ann_return", "ann_vol", "sharpe_gross", "sharpe_net", "ann_turnover", "max_drawdown"]
    if adj_close is None:
        index = pd.MultiIndex.from_tuples([], names=["horizon", "quantile_label"])
        return pd.DataFrame(columns=columns, index=index, dtype="float64")
    rf_aligned = (
        _align_risk_free_returns(risk_free_returns, adj_close.index)
        if risk_free_returns is not None
        else None
    )
    rows: list[dict[str, Any]] = []
    labels = [f"q{i}" for i in range(1, n_quantiles + 1)] + [f"ls_q{n_quantiles}_q1"]
    daily_values = adj_close.index.values
    for horizon in horizons:
        rebalance_index = factor.index[::horizon]
        weight_mats = _quantile_weight_matrices(
            factor.loc[rebalance_index], eligibility.loc[rebalance_index], n_quantiles)
        # 复刻 hold_between_rebalances(reindex(daily).ffill().fillna(0))：
        # 每个交易日取"最近一个不晚于它的再平衡日"的权重行，首个再平衡日前为 0。
        pos = np.searchsorted(rebalance_index.values, daily_values, side="right") - 1
        before_first = pos < 0
        pos_safe = np.clip(pos, 0, None)
        for label in labels:
            mat = weight_mats[label][pos_safe]
            if before_first.any():
                mat = mat.copy()
                mat[before_first] = 0.0
            weights = pd.DataFrame(mat, index=adj_close.index, columns=factor.columns)
            # 退市终局收益注入 gross/net 两跑都要传；注意引擎只对多头持仓
            # （held>0）注入——ls 组合空头腿的退市不注入（收益保守低估）。
            gross_result = run_backtest(
                f"{label}_h{horizon}_gross", weights, adj_close, cost_bps=0, hold_through_gaps=True,
                terminal_return=terminal_return, terminal_return_fallback=terminal_return_fallback,
            )
            net_result = run_backtest(
                f"{label}_h{horizon}_net", weights, adj_close, cost_bps=cost_bps, hold_through_gaps=True,
                terminal_return=terminal_return, terminal_return_fallback=terminal_return_fallback,
            )
            gross = gross_result.metrics()
            net = net_result.metrics()
            if rf_aligned is None:
                sharpe_gross = gross.get("sharpe", np.nan)
                sharpe_net = net.get("sharpe", np.nan)
            else:
                exposure = weights.shift(1).sum(axis=1).fillna(0.0)
                rf_drag = rf_aligned * exposure
                sharpe_gross = _annualized_sharpe(gross_result.daily_returns - rf_drag)
                sharpe_net = _annualized_sharpe(net_result.daily_returns - rf_drag)
            rows.append(
                {
                    "horizon": horizon,
                    "quantile_label": label,
                    "ann_return": net.get("cagr", np.nan),
                    "ann_vol": net.get("ann_vol", np.nan),
                    "sharpe_gross": sharpe_gross,
                    "sharpe_net": sharpe_net,
                    "ann_turnover": net.get("ann_turnover", np.nan),
                    "max_drawdown": net.get("max_drawdown", np.nan),
                }
            )
    return pd.DataFrame(rows).set_index(["horizon", "quantile_label"]).sort_index()[columns]


def _align_risk_free_returns(risk_free_returns: pd.Series, index: pd.Index) -> pd.Series:
    target = _normalize_dates(index)
    rf = pd.Series(
        pd.to_numeric(risk_free_returns.to_numpy(), errors="coerce"),
        index=_normalize_dates(risk_free_returns.index),
        name=getattr(risk_free_returns, "name", None),
    )
    if rf.index.has_duplicates:
        raise FactorEvaluationError("risk_free_returns index contains duplicate dates")
    aligned = rf.reindex(target)
    missing = aligned[aligned.isna()].index
    if len(missing) > 0:
        sample = ", ".join(str(ts.date()) for ts in missing[:5])
        suffix = "" if len(missing) <= 5 else ", ..."
        raise FactorEvaluationError(
            f"risk_free_returns missing {len(missing)} dates required for quantile backtest: {sample}{suffix}"
        )
    return aligned


def _annualized_sharpe(returns: pd.Series) -> float:
    r = returns.dropna()
    std = r.std()
    if r.empty or std <= 0 or pd.isna(std):
        return np.nan
    return float(r.mean() / std * np.sqrt(252))


def _coverage(
    factor: pd.DataFrame,
    forward_returns: Mapping[int, pd.DataFrame],
    eligibility: pd.DataFrame,
    as_of: pd.Timestamp | None,
) -> pd.DataFrame:
    eligible_counts = eligibility.sum(axis=1).replace(0, np.nan)
    factor_present = factor.notna() & eligibility
    raw_factor_counts = factor_present.sum(axis=1).astype("int64")
    factor_counts = raw_factor_counts.replace(0, np.nan)
    first_horizon = sorted(forward_returns)[0] if forward_returns else None
    if first_horizon is None:
        fwd_cov = pd.Series(np.nan, index=factor.index)
    else:
        fwd = forward_returns[first_horizon].reindex(index=factor.index, columns=factor.columns)
        fwd_cov = (factor_present & fwd.notna()).sum(axis=1) / factor_counts
    pit = pd.Series(0, index=factor.index, dtype="int64")
    if as_of is not None:
        pit.loc[pit.index > as_of] = factor.loc[factor.index > as_of].notna().sum(axis=1).astype("int64")
    coverage = pd.DataFrame(
        {
            "n_universe": eligibility.sum(axis=1).astype("int64"),
            "factor_count": raw_factor_counts,
            "factor_coverage": factor_present.sum(axis=1) / eligible_counts,
            "fwd_ret_coverage_given_factor": fwd_cov,
            "pit_violations": pit,
        },
        index=factor.index,
    )
    return coverage.dropna(subset=["factor_coverage"], how="all")


@dataclass(frozen=True)
class EvaluationResult:
    factor_name: str
    factor_version: str
    code_git_sha: str | None
    code_git_dirty: bool
    horizons: tuple[int, ...]
    eval_dates: pd.DatetimeIndex
    as_of: pd.Timestamp | None
    cost_bps: float
    n_quantiles: int
    universe_hash: str
    universe_size_mean: float
    universe_size_min: int
    params_hash: str
    config: Mapping[str, Any]
    ic_table: pd.DataFrame
    ic_decay: pd.DataFrame
    quantile_metrics: pd.DataFrame
    coverage: pd.DataFrame
    diagnostics: Mapping[str, Any]
    status: str = "ok"
    trial_id: str | None = None
    created_at: pd.Timestamp | None = None

    def is_noisy(self, t_threshold: float = NOISE_THRESHOLD, min_obs: int = MIN_OBS) -> dict[int, bool]:
        out: dict[int, bool] = {}
        for horizon, row in self.ic_table.iterrows():
            t_value = row.get("nw_t", np.nan)
            n_obs = row.get("n_obs", 0)
            out[int(horizon)] = bool(pd.isna(t_value) or abs(float(t_value)) < t_threshold or int(n_obs) < min_obs)
        return out

    def _trial_id_value(self) -> str:
        eval_start = self.eval_dates.min().date().isoformat() if len(self.eval_dates) else ""
        eval_end = self.eval_dates.max().date().isoformat() if len(self.eval_dates) else ""
        as_of = self.as_of.date().isoformat() if self.as_of is not None else ""
        return compute_trial_id(
            factor_name=self.factor_name,
            factor_version=self.factor_version,
            universe_hash=self.universe_hash,
            params_hash=self.params_hash,
            eval_start=eval_start,
            eval_end=eval_end,
            as_of=as_of,
            code_git_sha=self.code_git_sha,
        )

    def to_trial_rows(self) -> list[dict[str, Any]]:
        from research._trials_store import TRIALS_SCHEMA_VERSION

        trial_id = self.trial_id or self._trial_id_value()
        created_at = self.created_at or pd.Timestamp.now(tz="UTC")
        params_json = _canonical_json(_params_without_note(self.config))
        start = pd.Timestamp(self.config.get("start", self.eval_dates.min() if len(self.eval_dates) else pd.NaT))
        end = pd.Timestamp(self.config.get("end", self.eval_dates.max() if len(self.eval_dates) else pd.NaT))
        effective = pd.Timestamp(self.eval_dates.min()) if len(self.eval_dates) else pd.NaT
        base = {
            "trial_id": trial_id,
            "schema_version": TRIALS_SCHEMA_VERSION,
            "created_at": created_at,
            "run_id": self.config.get("run_id"),
            "factor_name": self.factor_name,
            "factor_version": self.factor_version,
            "code_git_sha": self.code_git_sha,
            "code_git_dirty": self.code_git_dirty,
            "eval_start": None if pd.isna(start) else start.date(),
            "eval_end": None if pd.isna(end) else end.date(),
            "eval_start_effective": None if pd.isna(effective) else effective.date(),
            "as_of": self.as_of.date() if self.as_of is not None else None,
            "universe_hash": self.universe_hash,
            "universe_size_mean": self.universe_size_mean,
            "universe_size_min": self.universe_size_min,
            "n_dates": len(self.eval_dates),
            "params_hash": self.params_hash,
            "params_json": params_json,
            "cost_bps": self.cost_bps,
            "n_quantiles": self.n_quantiles,
            "note": self.config.get("note"),
        }

        rows: list[dict[str, Any]] = []

        def add(horizon: int, metric: str, value: Any, metric_param: int | None = None) -> None:
            rows.append(
                base
                | {
                    "horizon": int(horizon),
                    "metric": metric,
                    "metric_param": metric_param,
                    "value": float(value) if pd.notna(value) else np.nan,
                    "is_noisy": self.is_noisy().get(int(horizon), False),
                }
            )

        metric_map = {
            "mean_ic": "ic_mean",
            "std_ic": "ic_std",
            "nw_t": "ic_nw_t",
            "nw_lag": "ic_nw_lag",
            "n_obs": "n_obs",
        }
        for horizon, row in self.ic_table.iterrows():
            for column, metric in metric_map.items():
                add(int(horizon), metric, row.get(column, np.nan))
        for row in self.ic_decay.itertuples(index=False):
            add(int(row.horizon), "ic_decay", row.ic, int(row.lag))

        q_metric_map = {
            "ann_return": "q_ann_return",
            "ann_vol": "q_ann_vol",
            "sharpe_gross": "q_sharpe_gross",
            "sharpe_net": "q_sharpe_net",
            "ann_turnover": "q_ann_turnover",
            "max_drawdown": "q_max_drawdown",
        }
        for (horizon, label), row in self.quantile_metrics.iterrows():
            metric_param = 0 if str(label).startswith("ls_") else int(str(label).removeprefix("q"))
            for column, metric in q_metric_map.items():
                add(int(horizon), metric, row.get(column, np.nan), metric_param)

        if not self.coverage.empty:
            add(0, "coverage_factor_mean", self.coverage["factor_coverage"].mean())
            add(0, "coverage_factor_p05", self.coverage["factor_coverage"].quantile(0.05))
            add(0, "coverage_fwd_given_factor_p05", self.coverage["fwd_ret_coverage_given_factor"].quantile(0.05))
            if "factor_count" in self.coverage:
                add(0, "coverage_factor_count_p05", self.coverage["factor_count"].quantile(0.05))
                add(0, "coverage_factor_count_median", self.coverage["factor_count"].median())
                add(0, "coverage_factor_count_max", self.coverage["factor_count"].max())
                min_coverage = int(self.config.get("min_coverage", 50))
                add(0, "coverage_days_below_min_coverage", (self.coverage["factor_count"] < min_coverage).sum())
            add(0, "n_universe_mean", self.coverage["n_universe"].mean())
            add(0, "n_universe_min", self.coverage["n_universe"].min())
        for metric in (
            "pit_regression_max_abs_diff",
            "pit_presence_violations",
            "factor_freshness_gap_days",
            "unexpected_coverage_jump_days",
        ):
            add(0, metric, self.diagnostics.get(metric, np.nan))
        for horizon in self.diagnostics.get("skipped_horizons", ()):
            add(int(horizon), "flag_horizon_skipped", 1.0)
        return rows


def evaluate_factor(
    factor_values: pd.DataFrame,
    forward_returns: dict[int, pd.DataFrame],
    *,
    eligibility: pd.DataFrame,
    horizons: tuple[int, ...] = DEFAULT_HORIZONS,
    n_quantiles: int = 5,
    cost_bps: float = 10.0,
    adj_close: pd.DataFrame | None = None,
    nw_lag_rule: Callable[[int, int], int] | None = None,
    min_coverage: int = 50,
    factor_name: str = "anonymous",
    risk_free_returns: pd.Series | None = None,
    terminal_return: float | pd.Series | None = None,
    terminal_return_fallback: float | None = None,
) -> EvaluationResult:
    factor = factor_values.copy().astype("float64")
    factor.index = _normalize_dates(factor.index)
    eligibility = eligibility.reindex(index=factor.index, columns=factor.columns).fillna(False).astype(bool)
    if factor.dropna(how="all", axis=0).empty:
        raise FactorEvaluationError(f"factor {factor_name!r} is empty or all NaN")
    aligned_returns = {h: forward_returns[h].reindex(index=factor.index, columns=factor.columns) for h in horizons}
    config = dict(factor.attrs.get("config", {})) | {
        "horizons": horizons,
        "n_quantiles": n_quantiles,
        "cost_bps": cost_bps,
        "risk_free_series": getattr(risk_free_returns, "name", None),
        "min_coverage": min_coverage,
        "noise_threshold": NOISE_THRESHOLD,
        "min_obs": MIN_OBS,
    } | _terminal_return_config(terminal_return, terminal_return_fallback)
    as_of = factor.attrs.get("as_of")
    as_of_ts = pd.Timestamp(as_of) if as_of is not None else None
    # IC/IC-decay 与分位回测、coverage 共用可投资横截面：ineligible 名字上的
    # 因子值不参与排名相关，min_coverage 计数口径与 coverage.factor_count 一致。
    investable_factor = factor.where(eligibility)
    lag_rule = nw_lag_rule or default_nw_lag
    ic_rows: list[dict[str, Any]] = []
    skipped: list[int] = []
    for horizon in horizons:
        fwd = aligned_returns[horizon]
        if fwd.dropna(how="all").empty:
            logger.warning("factor={} horizon={} skipped because forward returns are all NaN", factor_name, horizon)
            skipped.append(horizon)
            ic_rows.append({"horizon": horizon, "mean_ic": np.nan, "std_ic": np.nan, "nw_t": np.nan, "nw_lag": 0, "n_obs": 0, "is_noisy": True})
            continue
        ic = _rank_ic_series(investable_factor, fwd, min_coverage)
        n_obs = int(ic.notna().sum())
        requested_lag = int(lag_rule(horizon, n_obs)) if n_obs else 0
        effective_lag = min(requested_lag, max(n_obs - 1, 0))
        nw_t = _newey_west_t(ic, effective_lag) if n_obs else np.nan
        is_noisy = bool(pd.isna(nw_t) or abs(float(nw_t)) < NOISE_THRESHOLD or n_obs < MIN_OBS)
        ic_rows.append(
            {
                "horizon": horizon,
                "mean_ic": float(ic.mean()) if n_obs else np.nan,
                "std_ic": float(ic.std(ddof=1)) if n_obs > 1 else np.nan,
                "nw_t": nw_t,
                "nw_lag": effective_lag,
                "n_obs": n_obs,
                "is_noisy": is_noisy,
            }
        )
    ic_table = pd.DataFrame(ic_rows).set_index("horizon")
    ic_table["is_noisy"] = ic_table["is_noisy"].map(bool).astype(object)
    ic_decay = _ic_decay_table(investable_factor, aligned_returns, horizons, min_coverage)
    q_metrics = _quantile_metrics(
        factor, eligibility, adj_close, horizons, n_quantiles, cost_bps, risk_free_returns,
        terminal_return=terminal_return, terminal_return_fallback=terminal_return_fallback,
    )
    coverage = _coverage(factor, aligned_returns, eligibility, as_of_ts)
    non_nan_dates = factor.dropna(how="all").index
    freshness = (factor.index.max() - non_nan_dates.max()).days if len(non_nan_dates) else np.nan
    jumps = int((coverage["factor_coverage"].diff().abs() > 0.25).sum()) if not coverage.empty else 0
    halflife = _decay_halflife(ic_decay, min(horizons)) if horizons else np.nan
    decay_lookahead = bool(
        horizons
        and pd.notna(ic_table.loc[min(horizons), "mean_ic"])
        and ic_table.loc[min(horizons), "mean_ic"] > 0.5
        and pd.notna(halflife)
        and halflife < 2
    )
    pit_lookahead = bool(coverage["pit_violations"].max() > 0) if not coverage.empty else False
    diagnostics = {
        "pit_regression_max_abs_diff": np.nan,
        "pit_presence_violations": np.nan,
        "factor_freshness_gap_days": float(freshness) if pd.notna(freshness) else np.nan,
        "unexpected_coverage_jump_days": jumps,
        "skipped_horizons": tuple(skipped),
        "lookahead_suspect": bool(decay_lookahead or pit_lookahead),
        "ic_decay_halflife": halflife,
    }
    universe_sizes = eligibility.sum(axis=1)
    config.setdefault("factor_name", factor_name)
    params_hash = factor.attrs.get("params_hash") or _params_hash(config)
    return EvaluationResult(
        factor_name=factor_name,
        factor_version=factor.attrs.get("factor_version", "unknown"),
        code_git_sha=factor.attrs.get("code_git_sha"),
        code_git_dirty=bool(factor.attrs.get("code_git_dirty", False)),
        horizons=tuple(horizons),
        eval_dates=factor.index,
        as_of=as_of_ts,
        cost_bps=cost_bps,
        n_quantiles=n_quantiles,
        universe_hash=_universe_hash(factor.columns),
        universe_size_mean=float(universe_sizes.mean()) if len(universe_sizes) else 0.0,
        universe_size_min=int(universe_sizes.min()) if len(universe_sizes) else 0,
        params_hash=params_hash,
        config=config,
        ic_table=ic_table,
        ic_decay=ic_decay,
        quantile_metrics=q_metrics,
        coverage=coverage,
        diagnostics=diagnostics,
        status="skipped_all_nan" if len(skipped) == len(horizons) else "ok",
    )


def _buffered_end(end: date, max_horizon: int, market: str = "US") -> date:
    try:
        return shift_trading_date(market, end, max_horizon)
    except Exception as exc:
        logger.debug("trading calendar shift failed, using natural-day buffer: {}", exc)
        return end + timedelta(days=math.ceil(max_horizon * 7 / 5) + 5)


def _load_type_ids(engine: Engine, types: tuple[str, ...]) -> set[int]:
    """指定 type 集合的全部 security_id（adr_unsafe 门用，一次评估最多查一遍）。"""
    from sqlalchemy import text as _text
    with engine.connect() as conn:
        rows = conn.execute(
            _text("select id from securities where upper(type) = any(:types)"),
            {"types": [t.upper() for t in types]},
        )
        return {int(r[0]) for r in rows}


def _factor_version(factor: Factor, git_sha: str | None) -> str:
    source = inspect.getsourcefile(factor.__class__) or inspect.getsourcefile(factor)
    if source is None:
        return f"{git_sha or 'nogit'}:unknown"
    path = Path(source)
    digest = hashlib.sha1(path.read_bytes()).hexdigest()[:12]
    prefix = git_sha or "nogit"
    return f"{prefix}:{path.name}:{digest}"


def _pit_regression(
    factor_obj: Factor,
    live_values: pd.DataFrame,
    engine: Engine,
    eval_dates: pd.DatetimeIndex,
    universe: pd.Index,
) -> tuple[float, int]:
    if len(eval_dates) == 0:
        return np.nan, 0
    offsets = (60, 120, 180)
    sample_pos = sorted({max(len(eval_dates) - offset, 0) for offset in offsets if len(eval_dates) > offset})
    if not sample_pos:
        return np.nan, 0
    diffs: list[float] = []
    presence_violations = 0
    for pos in sample_pos:
        ts = eval_dates[pos]
        replay_dates = pd.DatetimeIndex(live_values.index[live_values.index <= ts])
        ctx = FactorContext(engine=engine, dates=replay_dates, security_universe=universe, as_of=ts)
        recomputed = factor_obj.compute(ctx).reindex(index=[ts], columns=universe)
        live = live_values.reindex(index=[ts], columns=universe)
        # presence 不匹配（live 有值而 as-of 重放为 NaN，或反之）本身就是 PIT 违规；
        # 值差取 nanmax 会跳过这些格子，必须单独计数。
        mismatch = recomputed.notna().to_numpy() != live.notna().to_numpy()
        presence_violations += int(mismatch.sum())
        diff = (recomputed - live).abs().to_numpy(dtype=float)
        if np.isfinite(diff).any():
            diffs.append(float(np.nanmax(diff)))
    return (max(diffs) if diffs else np.nan), presence_violations


def run_evaluation(
    factor: str | Factor,
    *,
    engine: Engine,
    start: date,
    end: date,
    as_of: date | None = None,
    horizons: tuple[int, ...] = DEFAULT_HORIZONS,
    n_quantiles: int = 5,
    cost_bps: float = 10.0,
    types: tuple[str, ...] = DEFAULT_RESEARCH_TYPES,
    min_price: float = 3.0,
    min_median_dollar_volume: float = 2_000_000.0,
    eligibility_window: int = 63,
    eval_start: date | None = None,
    extra_drop_ids: list[int] | None = None,
    trials_path: Path | None = DEFAULT_TRIALS_PATH,
    note: str | None = None,
    strict: bool = False,
    run_id: str | None = None,
    risk_free_returns: pd.Series | None = None,
    risk_free_series: str | None = DEFAULT_RISK_FREE_SERIES,
    terminal_return: float | None = None,
    use_delisting_returns: bool = True,
    ic_delisting_returns: bool = True,
    fund_closure_par: bool = True,
    exchange_drop_fallback: float | None = None,
    existing_trial_ids: frozenset[str] | None = None,
) -> EvaluationResult:
    from research._trials_store import _git_meta, append_trial

    effective_as_of = as_of or end
    buffered_end = _buffered_end(end, max(horizons))
    panel = load_adjusted_panel(engine, start=start, end=buffered_end, types=types, as_of=effective_as_of)
    # 剔除窗口与缓冲面板一致：前向收益会用到 end 之后 max(horizons) 个交易日的价格。
    # gate 口径单一事实源：行为（require_straddle 实参）与 params_hash 标签（uncovered_gate）
    # 都从这一个变量派生——将来加 legacy 对照旗标只改这里，不会出现"跑 legacy 宇宙、
    # 记 straddle_v2 标签"的错桶。
    uncovered_require_straddle = True
    bad = set(
        securities_with_uncovered_events(
            engine, start=start, end=buffered_end, require_straddle=uncovered_require_straddle
        )
    ) | set(extra_drop_ids or [])
    if bad:
        for key in panel:
            panel[key] = panel[key].drop(columns=[c for c in panel[key].columns if int(c) in bad], errors="ignore")
    eligible = eligibility_mask(
        panel["close"],
        panel["dollar_volume"],
        min_price=min_price,
        min_median_dollar_volume=min_median_dollar_volume,
        window=eligibility_window,
    )
    # 退市终局收益（口径同 run_baselines）：优先 delisting_events 逐证券实测，
    # CLI 标量降级为未覆盖证券的 fallback；opt-out / 表空时只用标量（旧口径）。
    realized = (
        load_delisting_returns(engine, fund_closure_par=fund_closure_par,
                               redemption_par=fund_closure_par,
                               exchange_drop_fallback=exchange_drop_fallback)
        if use_delisting_returns
        else pd.Series(dtype="float64")
    )
    resolved_terminal, resolved_fallback = resolve_terminal_returns(
        realized, terminal_return, use_realized=use_delisting_returns
    )
    keep = eligible.any(axis=0)
    for key in panel:
        panel[key] = panel[key].loc[:, keep]
    eligible = eligible.loc[:, keep]
    adj_close = panel["adj_close"]
    ctx_end = min(pd.Timestamp(end), pd.Timestamp(effective_as_of))
    ctx_dates = adj_close.index[(adj_close.index >= pd.Timestamp(start)) & (adj_close.index <= ctx_end)]
    universe = pd.Index(adj_close.columns, dtype="int64")
    from research.universe import universe_hash_from_ids
    u_hash = universe_hash_from_ids(universe.tolist(), start, end)
    factor_obj = get(factor) if isinstance(factor, str) else factor
    factor_params = _factor_params_snapshot(factor_obj)
    ctx = FactorContext(engine=engine, dates=ctx_dates, security_universe=universe, as_of=pd.Timestamp(effective_as_of))
    # ---- eval 窗口 / config / trial_id 全部前移到 factor.compute 之前 ----
    # 这些量只依赖面板与参数、不依赖因子值（纯重排序）；意义有二：
    # (1) 窗口不足类错误在昂贵的 compute 之前就抛（fail fast）；
    # (2) --skip-existing 能在 compute 之前用 compute_trial_id 判定"这一跑已有 trial"。
    default_eval_start = ctx_dates[252].date() if eval_start is None and len(ctx_dates) > 252 else None
    effective_eval_start = eval_start or default_eval_start
    if effective_eval_start is None:
        raise FactorEvaluationError("not enough trading dates for default 252-day warmup; pass eval_start")
    eval_dates = ctx_dates[ctx_dates >= pd.Timestamp(effective_eval_start)]
    if len(eval_dates) == 0:
        raise FactorEvaluationError("no evaluation dates after eval_start")
    quantile_adj_close = adj_close.reindex(index=ctx_dates, columns=universe).loc[pd.Timestamp(effective_eval_start):]
    if risk_free_returns is None and risk_free_series:
        risk_free_returns = load_risk_free_daily_returns(engine, quantile_adj_close.index, series_id=risk_free_series)
    code_git_sha, code_git_dirty = _git_meta()
    config = {
        "start": start,
        "end": end,
        "as_of": effective_as_of,
        "eval_start": effective_eval_start,
        "horizons": horizons,
        "n_quantiles": n_quantiles,
        "cost_bps": cost_bps,
        "risk_free_series": getattr(risk_free_returns, "name", None) or risk_free_series,
        "types": types,
        "min_price": min_price,
        "min_median_dollar_volume": min_median_dollar_volume,
        "eligibility_window": eligibility_window,
        "extra_drop_ids": sorted(int(x) for x in (extra_drop_ids or [])),
        # 未覆盖事件 gate 口径进 params_hash：straddle_v2（只剔跨立事件，2310->794）与
        # legacy_v1（任何未覆盖事件都剔）的 trial 必须可区分，先例同退市终局口径。
        # 标签与上方 gate 调用同源（uncovered_require_straddle），不得各写默认值。
        "uncovered_gate": uncovered_gate_version(uncovered_require_straddle),
        "factor_name": factor_obj.name,
        "factor_params": factor_params,
        "universe_hash": u_hash,
        "universe_size": len(universe),
        "factor_lookback_days": getattr(factor_obj, "lookback_days", None),
        "factor_lag_days": getattr(factor_obj, "lag_days", None),
        "factor_pit_guarantee": getattr(factor_obj, "pit_guarantee", None),
        # 退市终局口径进 params_hash：realized/scalar/none 三种口径的 trial 必须可区分，
        # 否则 trials.parquet 里新旧口径互相顶替（latest_only 读取会拿错）。
        **_terminal_return_config(resolved_terminal, resolved_fallback),
        # fund_closure_par 只在实测口径下起作用；其余口径归一为 None 避免无谓的 hash 分裂。
        "fund_closure_par": fund_closure_par if isinstance(resolved_terminal, pd.Series) else None,
        # redemption_par 与 fund_closure_par 同旗标控制（读取层 par 合成一体开关）
        "redemption_par": fund_closure_par if isinstance(resolved_terminal, pd.Series) else None,
        # EXCHANGE_DROP 无实测行的读取层经验合成（--exchange-drop-fallback，默认 None=旧口径
        # 即回测按 0% 处理）。同 fund_closure_par：只在实测口径下起作用，其余口径归一为
        # None 避免无谓的 hash 分裂；值进 params_hash，新旧口径 trial 不互相顶替。
        "exchange_drop_fallback": exchange_drop_fallback if isinstance(resolved_terminal, pd.Series) else None,
        "run_id": run_id,
        "note": note,
    }
    # IC/前向收益路径的退市终局注入口径进 params_hash（--no-ic-delisting-returns 回旧
    # ffill 口径，退市证券前向收益≈0%）。只在有可注入终局（resolved_terminal 非 None）时
    # 记键：否则 IC 行为新旧完全一致，多记键会让无退市 trial 无谓换 hash（同 fund_closure_par
    # 的"不起作用即归一"精神，这里更进一步——直接不记键，与本改动前的无退市 trial 位级同 hash）。
    if resolved_terminal is not None:
        config["ic_delisting_returns"] = ic_delisting_returns
    params_hash_value = _params_hash(config)
    factor_version_value = _factor_version(factor_obj, code_git_sha)
    prospective_trial_id = compute_trial_id(
        factor_name=factor_obj.name,
        factor_version=factor_version_value,
        universe_hash=_universe_hash(universe),
        params_hash=params_hash_value,
        eval_start=eval_dates.min().date().isoformat(),
        eval_end=eval_dates.max().date().isoformat(),
        as_of=pd.Timestamp(effective_as_of).date().isoformat(),
        code_git_sha=code_git_sha,
    )
    if existing_trial_ids is not None and prospective_trial_id in existing_trial_ids:
        logger.info(
            "factor={} trial 已存在（trial_id={}…），--skip-existing 跳过 compute/evaluate",
            factor_obj.name, prospective_trial_id[:12],
        )
        return EvaluationResult(
            factor_name=factor_obj.name,
            factor_version=factor_version_value,
            code_git_sha=code_git_sha,
            code_git_dirty=code_git_dirty,
            horizons=tuple(horizons),
            eval_dates=eval_dates,
            as_of=pd.Timestamp(effective_as_of),
            cost_bps=cost_bps,
            n_quantiles=n_quantiles,
            universe_hash=_universe_hash(universe),
            universe_size_mean=0.0,
            universe_size_min=0,
            params_hash=params_hash_value,
            config=config,
            ic_table=pd.DataFrame(),
            ic_decay=pd.DataFrame(columns=["horizon", "lag", "ic"]),
            quantile_metrics=pd.DataFrame(),
            coverage=pd.DataFrame(),
            diagnostics={"skipped_existing": True},
            status="skipped_existing",
            trial_id=prospective_trial_id,
        )
    factor_values = factor_obj.compute(ctx).reindex(index=ctx_dates, columns=universe).astype("float64")
    # §E.3 股本口径门：ADR 的 ADS/公司股本混杂，声明 adr_unsafe 的因子
    # （size/earnings_yield/short_interest_ratio）在 ADR opt-in 宇宙里对
    # ADR 列置 NaN——其余证券照常评估，禁入直至 ADS 比率归一化。
    adr_gated_count = 0
    if getattr(factor_obj, "adr_unsafe", False) and any(t in ADR_TYPES for t in types):
        adr_ids = _load_type_ids(engine, ADR_TYPES)
        gated_cols = [c for c in factor_values.columns if int(c) in adr_ids]
        if gated_cols:
            factor_values.loc[:, gated_cols] = np.nan
            adr_gated_count = len(gated_cols)
            logger.warning(
                "factor={} 声明 adr_unsafe：{} 只 ADR 列已置 NaN（股本口径未归一化，§E.3）",
                factor_obj.name, adr_gated_count,
            )
    if factor_values.dropna(how="all", axis=0).empty:
        raise FactorEvaluationError(f"factor {factor_obj.name!r} is empty or all NaN")
    # 前向收益退市终局注入（口径与分位路径 _quantile_metrics 同源 resolved_terminal/
    # resolved_fallback）；--no-ic-delisting-returns 回旧 ffill 口径（退市证券前向收益≈0%）。
    ic_terminal = resolved_terminal if ic_delisting_returns else None
    ic_fallback = resolved_fallback if ic_delisting_returns else None
    forward_returns = {
        h: _forward_return(
            adj_close, h, terminal_return=ic_terminal, terminal_return_fallback=ic_fallback
        ).reindex(index=eval_dates, columns=universe)
        for h in horizons
    }
    eval_start_ts = pd.Timestamp(effective_eval_start)
    for panel_returns in forward_returns.values():
        panel_returns.loc[panel_returns.index < eval_start_ts] = np.nan
    factor_eval = factor_values.reindex(index=eval_dates, columns=universe)
    factor_eval.attrs.update(
        {
            "as_of": pd.Timestamp(effective_as_of),
            "factor_version": factor_version_value,
            "code_git_sha": code_git_sha,
            "code_git_dirty": code_git_dirty,
            "config": config,
            "params_hash": params_hash_value,
        }
    )
    result = evaluate_factor(
        factor_eval,
        forward_returns,
        eligibility=eligible.reindex(index=eval_dates, columns=universe),
        horizons=horizons,
        n_quantiles=n_quantiles,
        cost_bps=cost_bps,
        adj_close=quantile_adj_close,
        min_coverage=50,
        factor_name=factor_obj.name,
        risk_free_returns=risk_free_returns,
        terminal_return=resolved_terminal,
        terminal_return_fallback=resolved_fallback,
    )
    pit_diff, pit_presence = _pit_regression(factor_obj, factor_values, engine, eval_dates, universe)
    diagnostics = dict(result.diagnostics)
    diagnostics["pit_regression_max_abs_diff"] = pit_diff
    diagnostics["pit_presence_violations"] = pit_presence
    diagnostics["adr_gated_columns"] = adr_gated_count
    diagnostics["lookahead_suspect"] = bool(
        diagnostics.get("lookahead_suspect", False)
        or (pd.notna(pit_diff) and pit_diff > 1e-6)
        or pit_presence > 0
    )
    result = replace(result, diagnostics=diagnostics)
    if pd.notna(pit_diff) and pit_diff > 1e-9:
        logger.warning("factor={} PIT regression max abs diff={}", factor_obj.name, pit_diff)
    if pit_presence > 0:
        logger.warning("factor={} PIT regression presence violations={}", factor_obj.name, pit_presence)
    if trials_path is not None:
        try:
            append_trial(result, trials_path)
        except Exception as exc:
            logger.opt(exception=exc).error("failed to append trial for factor={}", factor_obj.name)
            raise
    if strict and diagnostics["lookahead_suspect"]:
        raise FactorEvaluationError(f"factor {factor_obj.name!r} failed PIT regression")
    return result


def evaluate_all(
    *,
    engine: Engine,
    start: date,
    end: date,
    names: list[str] | None = None,
    skip_existing: bool = False,
    **kwargs: Any,
) -> list[EvaluationResult]:
    factor_names = names or list_factors()
    results: list[EvaluationResult] = []
    run_id = hashlib.sha1(f"{pd.Timestamp.now(tz='UTC').isoformat()}:{factor_names}".encode()).hexdigest()
    prog = Progress("evaluate", total=len(factor_names))
    # --skip-existing 断点续跑：完成集就是 trials.parquet 本身（trial_id 全指纹幂等），
    # 不引入新台账文件。OOM 死在第 7 个因子后，同一条命令 + --skip-existing 只重跑 7、8；
    # 每个因子仍要付面板装载（进程内缓存只付一次），省掉的是逐因子 compute/evaluate 大头。
    existing_trial_ids: frozenset[str] | None = None
    if skip_existing:
        trials_path = kwargs.get("trials_path", DEFAULT_TRIALS_PATH)
        if trials_path is None:
            logger.warning("--skip-existing 在 --no-persist 下无台账可查，忽略跳过")
        else:
            from research._trials_store import load_trials
            trials = load_trials(Path(trials_path))
            existing_trial_ids = (
                frozenset(trials["trial_id"].dropna().astype(str)) if not trials.empty else frozenset()
            )
            prog.log(f"--skip-existing: 台账已有 {len(existing_trial_ids)} 个 trial_id")
    skipped = 0
    for i, name in enumerate(factor_names, 1):
        try:
            with prog.stage(f"因子 {name}", item=i):
                result = run_evaluation(
                    name, engine=engine, start=start, end=end, run_id=run_id,
                    existing_trial_ids=existing_trial_ids, **kwargs,
                )
            if result.status == "skipped_existing":
                skipped += 1
                prog.log(f"skip {name}: trial 已存在（{(result.trial_id or '')[:12]}…）", item=i)
            results.append(result)
        except Exception as exc:
            if kwargs.get("strict"):
                raise
            logger.opt(exception=exc).error("factor={} evaluation failed", name)
            results.append(
                EvaluationResult(
                    factor_name=name,
                    factor_version="unknown",
                    code_git_sha=None,
                    code_git_dirty=False,
                    horizons=tuple(kwargs.get("horizons", DEFAULT_HORIZONS)),
                    eval_dates=pd.DatetimeIndex([]),
                    as_of=pd.Timestamp(kwargs.get("as_of")) if kwargs.get("as_of") is not None else pd.Timestamp(end),
                    cost_bps=float(kwargs.get("cost_bps", 10.0)),
                    n_quantiles=int(kwargs.get("n_quantiles", 5)),
                    universe_hash="",
                    universe_size_mean=0.0,
                    universe_size_min=0,
                    params_hash="",
                    config={"start": start, "end": end, "run_id": run_id, "error": repr(exc)},
                    ic_table=pd.DataFrame(),
                    ic_decay=pd.DataFrame(columns=["horizon", "lag", "ic"]),
                    quantile_metrics=pd.DataFrame(),
                    coverage=pd.DataFrame(),
                    diagnostics={"error": repr(exc)},
                    status="failed",
                )
            )
    if skip_existing and existing_trial_ids:
        prog.log(f"--skip-existing 命中 {skipped}/{len(factor_names)}")
        if skipped == 0:
            logger.warning(
                "--skip-existing 零命中：trial_id 含源码指纹/参数/宇宙，任一漂移都会全量重跑——"
                "确认这是有意的（如改了 evaluate/backtest/因子源码）"
            )
    return results


def _parse_csv_ints(value: str) -> tuple[int, ...]:
    return tuple(int(part.strip()) for part in value.split(",") if part.strip())


def _result_summary(result: EvaluationResult) -> pd.DataFrame:
    rows = []
    factor_count_p05 = np.nan
    factor_count_median = np.nan
    factor_count_max = np.nan
    days_below_min_coverage = np.nan
    if not result.coverage.empty and "factor_count" in result.coverage:
        counts = result.coverage["factor_count"]
        factor_count_p05 = counts.quantile(0.05)
        factor_count_median = counts.median()
        factor_count_max = counts.max()
        min_coverage = int(result.config.get("min_coverage", 50))
        days_below_min_coverage = int((counts < min_coverage).sum())
    for horizon, row in result.ic_table.iterrows():
        label = f"{row['nw_t']:.3f}{'*' if bool(row['is_noisy']) else ''}" if pd.notna(row["nw_t"]) else "nan*"
        q_label = f"ls_q{result.n_quantiles}_q1"
        q_sharpe = np.nan
        if not result.quantile_metrics.empty and (horizon, q_label) in result.quantile_metrics.index:
            q_sharpe = result.quantile_metrics.loc[(horizon, q_label), "sharpe_net"]
        rows.append(
            {
                "horizon": horizon,
                "ic_mean": row["mean_ic"],
                "nw_t": label,
                "q_ls_sharpe_net": q_sharpe,
                "coverage_p05": result.coverage["factor_coverage"].quantile(0.05) if not result.coverage.empty else np.nan,
                "factor_count_p05": factor_count_p05,
                "factor_count_median": factor_count_median,
                "factor_count_max": factor_count_max,
                "days_below_min_coverage": days_below_min_coverage,
                "pit_violations_max": result.coverage["pit_violations"].max() if not result.coverage.empty else np.nan,
                "n_obs": row["n_obs"],
            }
        )
    return pd.DataFrame(rows).set_index("horizon")


def _markdown_table(df: pd.DataFrame, *, include_index: bool = True) -> str:
    table = df.reset_index() if include_index else df.copy()
    if table.empty:
        return "(empty)"
    headers = [str(col) for col in table.columns]
    body = []
    for row in table.itertuples(index=False):
        body.append(["" if pd.isna(value) else str(value) for value in row])
    lines = ["| " + " | ".join(headers) + " |", "| " + " | ".join(["---"] * len(headers)) + " |"]
    lines.extend("| " + " | ".join(row) + " |" for row in body)
    return "\n".join(lines)


def _write_markdown_report(result: EvaluationResult, output_dir: Path) -> Path:
    output_dir.mkdir(exist_ok=True)
    start = result.eval_dates.min().date() if len(result.eval_dates) else "empty"
    end = result.eval_dates.max().date() if len(result.eval_dates) else "empty"
    path = output_dir / f"evaluate_{result.factor_name}_{start}_{end}.md"
    terminal_mode = str(result.config.get("terminal_return_mode", "none"))
    notes = [
        f"- Terminal-return mode: `{terminal_mode}`"
        f" (scalar={result.config.get('terminal_return_scalar')},"
        f" fallback={result.config.get('terminal_return_fallback')},"
        f" fund_closure_par={result.config.get('fund_closure_par')},"
        f" exchange_drop_fallback={result.config.get('exchange_drop_fallback')}).",
    ]
    if terminal_mode != "none":
        notes.append(
            "- Terminal-return injection only covers long legs (held > 0): short-leg delistings in "
            "`ls_*` portfolios are not injected, so long-short returns are conservatively understated."
        )
    parts = [
        f"# Factor evaluation: {result.factor_name}",
        "",
        "## Summary",
        _markdown_table(_result_summary(result)),
        "",
        f"Long-short convention: `ls_q{result.n_quantiles}_q1` is long q{result.n_quantiles} (highest factor values) and short q1 (lowest factor values).",
        "",
        "## IC decay",
        _markdown_table(result.ic_decay, include_index=False),
        "",
        "## Quantile metrics",
        _markdown_table(result.quantile_metrics.reset_index(), include_index=False),
        "",
        "## PIT diagnostics",
        _markdown_table(pd.Series(result.diagnostics, name="value").to_frame()),
        "",
        "## Notes",
        *notes,
    ]
    path.write_text("\n".join(parts), encoding="utf-8")
    return path


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="因子评估层")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--factors", help="逗号分隔因子名")
    group.add_argument("--all", action="store_true", help="评估所有注册因子")
    parser.add_argument("--start", type=date.fromisoformat, default=DEFAULT_EVAL_PANEL_START)
    parser.add_argument("--end", type=date.fromisoformat, default=date.today())
    parser.add_argument("--as-of", type=date.fromisoformat, default=None)
    parser.add_argument("--eval-start", type=date.fromisoformat, default=None)
    parser.add_argument("--horizons", type=_parse_csv_ints, default=DEFAULT_HORIZONS)
    parser.add_argument("--n-quantiles", type=int, default=5)
    parser.add_argument("--cost-bps", type=float, default=10.0)
    parser.add_argument("--include-adr", action="store_true",
                        help="宇宙并入 ADR 家族（ADRC/ADRP/ADRR）。默认 CS-only 零污染；"
                             "adr_unsafe 因子（size/earnings_yield/short_interest_ratio）"
                             "的 ADR 列自动置 NaN（股本口径未归一化，§E.3）。")
    parser.add_argument("--risk-free-series", default=DEFAULT_RISK_FREE_SERIES, help="risk_free_rates series_id；默认 DTB3。")
    parser.add_argument("--no-risk-free", action="store_true", help="复现旧口径：Sharpe/IR 不扣 risk-free。")
    parser.add_argument("--terminal-return", default=None,
                        help="退市持仓的终局收益假设（如 -1.0=归零、-0.3=CRSP 经验值、"
                             "none=显式旧口径即退市赚 0%%）。语义同 run_baselines：delisting_events "
                             "有实测收益时它降级为未覆盖证券的 fallback。")
    parser.add_argument("--no-delisting-returns", action="store_true",
                        help="不读 delisting_events 的逐证券实测退市收益，只用 --terminal-return "
                             "全局假设（复现旧口径运行）。")
    parser.add_argument("--no-ic-delisting-returns", action="store_true",
                        help="逃生舱：IC/前向收益路径回旧 ffill 口径（退市证券前向收益≈0%%），"
                             "不注入实测/标量退市终局。默认新口径与分位路径同源注入退市收益。"
                             "分位回测路径不受此旗标影响（始终按 --terminal-return/实测口径）。"
                             "口径进 params_hash，新旧口径 trial 不互相顶替。")
    parser.add_argument("--no-fund-closure-par", action="store_true",
                        help="关闭读取层 par 合成（ETF 清盘 FUND_CLOSURE 与 SPAC 赎回 LIQUIDATION+"
                             "redemption_provision 的 NULL 实测行合成 0.0），只用纯实测行。")
    parser.add_argument("--exchange-drop-fallback", type=float, default=None,
                        help="EXCHANGE_DROP（摘牌转 OTC 等 1,194 只）无实测退市收益行的读取层"
                             "经验合成值（如 -0.30，CRSP 风格假设）。默认不合成=旧口径，回测按 "
                             "0%% 处理会虚增小盘 q5；实测值永远优先。值进 params_hash。")
    persist_group = parser.add_mutually_exclusive_group()
    persist_group.add_argument("--trials-path", type=Path, default=None)
    persist_group.add_argument("--no-persist", action="store_true")
    parser.add_argument("--skip-existing", action="store_true",
                        help="断点续跑：trial_id（因子版本+参数+宇宙+窗口全指纹）已在 trials 台账里的"
                             "因子跳过 compute/evaluate（OOM 死在第 7 个因子后，重跑只算 7、8）。"
                             "任何源码/参数/宇宙漂移都会导致零命中并全量重跑（有 WARN 提示）。")
    parser.add_argument("--note")
    parser.add_argument("--strict", action="store_true")
    args = parser.parse_args(argv)
    if args.start < FACTOR_TRUST_FLOOR:
        parser.error(f"--start must be >= {FACTOR_TRUST_FLOOR}")
    if isinstance(args.terminal_return, str):
        args.terminal_return = None if args.terminal_return.lower() == "none" else float(args.terminal_return)
    if args.trials_path is None and not args.no_persist:
        args.trials_path = DEFAULT_TRIALS_PATH
    return args


def main(argv: list[str] | None = None) -> int:
    load_dotenv()
    args = parse_args(argv)
    if args.no_persist:
        args.trials_path = None
    engine = research_engine()
    names = None if args.all else [part.strip() for part in args.factors.split(",") if part.strip()]
    results = evaluate_all(
        engine=engine,
        start=args.start,
        end=args.end,
        names=names,
        as_of=args.as_of,
        eval_start=args.eval_start,
        horizons=args.horizons,
        n_quantiles=args.n_quantiles,
        cost_bps=args.cost_bps,
        types=RESEARCH_TYPES_WITH_ADR if args.include_adr else DEFAULT_RESEARCH_TYPES,
        risk_free_series=None if args.no_risk_free else args.risk_free_series,
        terminal_return=args.terminal_return,
        use_delisting_returns=not args.no_delisting_returns,
        ic_delisting_returns=not args.no_ic_delisting_returns,
        fund_closure_par=not args.no_fund_closure_par,
        exchange_drop_fallback=args.exchange_drop_fallback,
        trials_path=args.trials_path,
        note=args.note,
        strict=args.strict,
        skip_existing=args.skip_existing,
    )
    for result in results:
        if result.status == "failed":
            print(f"## {result.factor_name}\n\nfailed: {result.diagnostics.get('error')}")
            continue
        if result.status == "skipped_existing":
            print(f"## {result.factor_name}\n\nskipped: trial 已存在（--skip-existing，trial_id={(result.trial_id or '')[:12]}…）")
            continue
        print(f"## {result.factor_name}")
        print(_markdown_table(_result_summary(result)))
        report = _write_markdown_report(result, OUTPUT_DIR)
        print(f"\nreport: {report}\n")
    return 0 if all(result.status != "failed" for result in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
