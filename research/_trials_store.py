from __future__ import annotations

import fcntl
import os
import subprocess
from contextlib import contextmanager
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING, Any, Iterator

import pandas as pd
from loguru import logger

if TYPE_CHECKING:
    from research.evaluate import EvaluationResult

TRIALS_SCHEMA_VERSION = 1

TRIALS_SCHEMA: tuple[tuple[str, str], ...] = (
    ("trial_id", "string"),
    ("schema_version", "int16"),
    ("created_at", "timestamp[ns, UTC]"),
    ("run_id", "string"),
    ("factor_name", "string"),
    ("factor_version", "string"),
    ("code_git_sha", "string"),
    ("code_git_dirty", "bool"),
    ("eval_start", "date32"),
    ("eval_end", "date32"),
    ("eval_start_effective", "date32"),
    ("as_of", "date32"),
    ("horizon", "int16"),
    ("metric", "dictionary<string>"),
    ("metric_param", "int32"),
    ("value", "float64"),
    ("universe_hash", "string"),
    ("universe_size_mean", "float64"),
    ("universe_size_min", "int32"),
    ("n_dates", "int32"),
    ("params_hash", "string"),
    ("params_json", "string"),
    ("cost_bps", "float64"),
    ("n_quantiles", "int16"),
    ("note", "string"),
    ("is_noisy", "bool"),
)

METRIC_NAMES: frozenset[str] = frozenset(
    {
        "ic_mean",
        "ic_std",
        "ic_nw_t",
        "ic_nw_lag",
        "ic_decay",
        "n_obs",
        "q_ann_return",
        "q_ann_vol",
        "q_sharpe_gross",
        "q_sharpe_net",
        "q_ann_turnover",
        "q_max_drawdown",
        "coverage_factor_mean",
        "coverage_factor_p05",
        "coverage_fwd_given_factor_p05",
        "coverage_factor_count_p05",
        "coverage_factor_count_median",
        "coverage_factor_count_max",
        "coverage_days_below_min_coverage",
        "n_universe_mean",
        "n_universe_min",
        "pit_regression_max_abs_diff",
        "pit_presence_violations",
        "factor_freshness_gap_days",
        "unexpected_coverage_jump_days",
        "flag_horizon_skipped",
    }
)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


@lru_cache(maxsize=1)
def _git_meta() -> tuple[str | None, bool]:
    try:
        root = _repo_root()
        sha = subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=root, timeout=2, check=False, capture_output=True, text=True
        )
        if sha.returncode != 0:
            return None, False
        status = subprocess.run(
            ["git", "status", "--porcelain"], cwd=root, timeout=2, check=False, capture_output=True, text=True
        )
        dirty = bool(status.stdout.strip()) if status.returncode == 0 else False
        return sha.stdout.strip() or None, dirty
    except Exception:
        return None, False


def _pyarrow():
    import pyarrow as pa
    import pyarrow.parquet as pq

    return pa, pq


def _type_from_string(value: str):
    pa, _ = _pyarrow()
    if value == "string":
        return pa.string()
    if value == "int16":
        return pa.int16()
    if value == "int32":
        return pa.int32()
    if value == "float64":
        return pa.float64()
    if value == "bool":
        return pa.bool_()
    if value == "date32":
        return pa.date32()
    if value == "timestamp[ns, UTC]":
        return pa.timestamp("ns", tz="UTC")
    if value == "dictionary<string>":
        return pa.dictionary(pa.int32(), pa.string())
    raise ValueError(f"unsupported trials schema type: {value}")


def _arrow_schema():
    pa, _ = _pyarrow()
    return pa.schema([pa.field(name, _type_from_string(kind)) for name, kind in TRIALS_SCHEMA])


def _schema_columns() -> list[str]:
    return [name for name, _ in TRIALS_SCHEMA]


def _empty_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=_schema_columns())


def _read_frame(path: Path) -> pd.DataFrame:
    if not path.exists():
        return _empty_frame()
    _, pq = _pyarrow()
    table = pq.read_table(path)
    df = table.to_pandas()
    return df.reindex(columns=_schema_columns())


def _coerce_frame(df: pd.DataFrame) -> pd.DataFrame:
    out = df.reindex(columns=_schema_columns()).copy()
    for col in ("eval_start", "eval_end", "eval_start_effective", "as_of"):
        out[col] = pd.to_datetime(out[col], errors="coerce").dt.date
    out["created_at"] = pd.to_datetime(out["created_at"], utc=True, errors="coerce")
    for col in ("schema_version", "horizon", "n_quantiles"):
        out[col] = pd.to_numeric(out[col], errors="coerce").astype("Int16")
    for col in ("metric_param", "universe_size_min", "n_dates"):
        out[col] = pd.to_numeric(out[col], errors="coerce").astype("Int32")
    for col in ("value", "universe_size_mean", "cost_bps"):
        out[col] = pd.to_numeric(out[col], errors="coerce").astype("float64")
    for col in ("code_git_dirty", "is_noisy"):
        out[col] = out[col].astype("boolean")
    return out


def _write_frame(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    coerced = _coerce_frame(df)
    rows: list[dict[str, Any]] = []
    for row in coerced.to_dict("records"):
        clean: dict[str, Any] = {}
        for key, value in row.items():
            clean[key] = None if pd.isna(value) else value
        rows.append(clean)
    pa, pq = _pyarrow()
    table = pa.Table.from_pylist(rows, schema=_arrow_schema())
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        pq.write_table(table, tmp)
        os.replace(tmp, path)
    except Exception:
        try:
            tmp.unlink()
        except FileNotFoundError:
            pass
        raise


@contextmanager
def _exclusive_lock(path: Path) -> Iterator[None]:
    """.lock 旁文件上的 fcntl 排他锁，串行化并发 append 的 read→concat→replace。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = path.with_name(f"{path.name}.lock")
    with open(lock_path, "w") as lock_file:
        fcntl.flock(lock_file, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock_file, fcntl.LOCK_UN)


def append_trial(result: "EvaluationResult", path: Path) -> str:
    rows = result.to_trial_rows()
    if not rows:
        raise ValueError("EvaluationResult produced no trial rows")
    trial_id = rows[0]["trial_id"]
    new = pd.DataFrame(rows)
    unknown = set(new["metric"].dropna()) - METRIC_NAMES
    if unknown:
        raise ValueError(f"unknown trial metrics: {sorted(unknown)}")
    with _exclusive_lock(path):
        old = _read_frame(path)
        if not old.empty and trial_id in set(old["trial_id"].astype(str)):
            logger.info("trial_id={} already exists in {}, skipping", trial_id, path)
            object.__setattr__(result, "trial_id", trial_id)
            return trial_id
        combined = pd.concat([old, new.reindex(columns=_schema_columns())], ignore_index=True)
        if len(combined) > 100_000:
            logger.warning("{} has {} rows; consider manual archive", path, len(combined))
        _write_frame(combined, path)
    object.__setattr__(result, "trial_id", trial_id)
    created = pd.to_datetime(rows[0]["created_at"], utc=True)
    object.__setattr__(result, "created_at", created)
    return trial_id


def load_trials(path: Path = Path("research/output/trials.parquet"), *, latest_only: bool = False) -> pd.DataFrame:
    df = _read_frame(path)
    if df.empty or not latest_only:
        return df
    keys = ["factor_name", "factor_version", "horizon", "metric", "metric_param", "params_hash"]
    ordered = df.copy()
    created = pd.to_datetime(ordered["created_at"], utc=True, errors="coerce")
    sort_created = created.fillna(pd.Timestamp("1970-01-01", tz="UTC"))
    ordered = ordered.assign(created_at=created, _created_at_sort=sort_created).sort_values("_created_at_sort")
    latest_idx = ordered.groupby(keys, dropna=False)["_created_at_sort"].idxmax()
    latest = ordered.loc[latest_idx].drop(columns=["_created_at_sort"]).sort_values(keys).reset_index(drop=True)
    dropped = sorted(set(ordered["trial_id"].dropna()) - set(latest["trial_id"].dropna()))
    if dropped:
        logger.warning("latest_only collapsed {} trial_ids (kept {})", len(dropped), len(latest))
        logger.debug("latest_only dropped trial_ids={}", dropped)
    return latest
