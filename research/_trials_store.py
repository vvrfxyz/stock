from __future__ import annotations

import fcntl
import hashlib
import json
import os
import subprocess
from contextlib import contextmanager
from datetime import date, datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING, Any, Iterator

import pandas as pd
from loguru import logger

if TYPE_CHECKING:
    from research.evaluate import EvaluationResult

# v2（2026-07-08，W0-P3）：新增 trial_kind 列区分两类行——
#   'evaluate'：假设检验型 trial（evaluate 自动写入；历史行 trial_kind 为 NULL，
#               读取侧一律按 'evaluate' 解释）。
#   'study'   ：变现/部署类 study 的判据与结局（retail_reality / composite_study /
#               size_neutral_study 收尾写入，append_study()）。
# 分母口径（预注册，roadmap_2026-07_researchline.md §1 P3）：study 行**不计入**
# Bonferroni 分母——分母只数假设检验型 trial。两层标准：发现级结论用动态
# Bonferroni（trials report），部署级 study 判据用各自预注册阈值（如 t>=2）。
TRIALS_SCHEMA_VERSION = 2

TRIALS_SCHEMA: tuple[tuple[str, str], ...] = (
    ("trial_id", "string"),
    ("schema_version", "int16"),
    ("trial_kind", "dictionary<string>"),
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
        # trial_kind='study' 专用（W0-P3）：结局 + 任意数值判据组件
        "study_verdict",
        "study_criterion",
    }
)

STUDY_KINDS: frozenset[str] = frozenset({"retail_reality", "composite_study", "size_neutral"})


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
    if "trial_kind" not in new.columns:
        new["trial_kind"] = "evaluate"
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


def default_trials_path() -> Path:
    """写/读共用的台账锚点（__file__ 绝对路径，与 load_trials 默认一致）。"""
    return Path(__file__).resolve().parent / "output" / "trials.parquet"


def append_study(
    *,
    study: str,
    factor_name: str,
    verdict: bool,
    criteria: str,
    params: dict[str, Any],
    eval_start: "date",
    eval_end: "date",
    report_path: str | None = None,
    criterion_values: dict[str, float] | None = None,
    path: Path | None = None,
) -> str:
    """变现/部署类 study 的结局入台账（trial_kind='study'，W0-P3）。

    与 append_trial 的分工：evaluate 写假设检验型 trial（计入 Bonferroni 分母）；
    study 行只记"这次部署判定看过什么、判了什么"——**不计入分母**（trials report
    读取侧按 trial_kind 剔除），但让 garden-of-forking-paths 有机器账可查：
    同一因子反复过 retail_reality 直到 PASS 的痕迹在此一览无余。

    口径键语义（对抗审核 #3/#8/#10 修订，2026-07-08）：
    - params_hash 的哈希输入含 eval_start/eval_end——与 evaluate 的 config 含窗口
      对齐，保住 load_trials(latest_only=True) 折叠键的不变量（同 params 不同窗口
      是两条账，不互相折叠）。
    - 幂等键 trial_id = hash(study|factor|窗口|params_hash|code_git_sha)：同代码
      同口径同窗口重跑视为同一次 study；代码变了（引擎语义改动）自动成新行。
    - 命中重复但 verdict 漂移（同代码下只可能是数据变了，如 delisting_events
      重建）：不静默吞——logger.warning + 以带时间戳的新 trial_id 追加新行，
      新旧结局都留在台账上供对账。verdict 一致才静默跳过。
    verdict 存 metric='study_verdict'（1.0=PASS/0.0=FAIL)，数值判据组件存
    metric='study_criterion' 行（note=组件名）。
    """
    if study not in STUDY_KINDS:
        raise ValueError(f"unknown study kind: {study!r}（须在 STUDY_KINDS 中登记）")
    if path is None:
        path = default_trials_path()
    params_json = json.dumps(params, sort_keys=True, ensure_ascii=False, default=str)
    hash_input = json.dumps(
        {"params": params, "eval_start": str(eval_start), "eval_end": str(eval_end)},
        sort_keys=True, ensure_ascii=False, default=str,
    )
    params_hash = hashlib.sha1(hash_input.encode("utf-8")).hexdigest()[:12]
    sha, dirty = _git_meta()
    trial_id = hashlib.sha1(
        f"study|{study}|{factor_name}|{eval_start}|{eval_end}|{params_hash}|{sha}".encode("utf-8")
    ).hexdigest()[:32]
    created_at = datetime.now(timezone.utc)
    base: dict[str, Any] = {
        "trial_id": trial_id,
        "schema_version": TRIALS_SCHEMA_VERSION,
        "trial_kind": "study",
        "created_at": created_at,
        "run_id": f"{study}",
        "factor_name": factor_name,
        "factor_version": study,
        "code_git_sha": sha,
        "code_git_dirty": dirty,
        "eval_start": eval_start,
        "eval_end": eval_end,
        "params_hash": params_hash,
        "params_json": json.dumps(
            {**params, "report_path": report_path}, sort_keys=True, ensure_ascii=False, default=str
        ),
        "is_noisy": False,
    }
    rows = [{**base, "metric": "study_verdict", "value": 1.0 if verdict else 0.0, "note": criteria}]
    for i, (name, value) in enumerate(sorted((criterion_values or {}).items())):
        rows.append({**base, "metric": "study_criterion", "metric_param": i, "value": float(value), "note": name})
    with _exclusive_lock(path):
        old = _read_frame(path)
        if not old.empty and trial_id in set(old["trial_id"].astype(str)):
            prior = old[(old["trial_id"].astype(str) == trial_id) & (old["metric"] == "study_verdict")]
            prior_verdict = bool(prior["value"].iloc[0] == 1.0) if not prior.empty else None
            if prior_verdict == verdict:
                logger.info("study trial_id={} already exists in {} (verdict 一致), skipping", trial_id, path)
                return trial_id
            # 同代码同口径同窗口 verdict 漂移 = 数据变了（如 delisting_events 重建）。
            # 不静默吞：告警 + 带时间戳的新 trial_id 追加，新旧结局都留账（审核 #8/#10）。
            stamped = hashlib.sha1(
                f"{trial_id}|{created_at.isoformat()}".encode("utf-8")
            ).hexdigest()[:32]
            logger.warning(
                "study {} {} verdict 漂移（旧={} 新={}，同代码同口径——数据变了？）；"
                "以新 trial_id={} 追加，旧行保留",
                study, factor_name, prior_verdict, verdict, stamped,
            )
            trial_id = stamped
            for row in rows:
                row["trial_id"] = stamped
        combined = pd.concat(
            [old, pd.DataFrame(rows).reindex(columns=_schema_columns())], ignore_index=True
        )
        _write_frame(combined, path)
    logger.info("study 行已入台账: {} {} verdict={} trial_id={}", study, factor_name, verdict, trial_id)
    return trial_id


def load_trials(path: Path | None = None, *, latest_only: bool = False) -> pd.DataFrame:
    if path is None:
        # 与 evaluate.py 写侧同锚（__file__ 绝对路径）：cwd 相对默认值会在错目录下
        # 把有账读成空帧（_read_frame 对不存在路径静默返回空）。
        path = Path(__file__).resolve().parent / "output" / "trials.parquet"
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
