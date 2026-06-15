# Task #7 — `research/evaluate.py` 因子评估层

**来源**: `docs/audits/2026-06-13_deep_review_and_roadmap.md` 路线 Now 第 7 项

**依赖**: #6a + #6b 已 merged (commit `14ebc9d`),`research.factors.protocol.Factor` 协议 + `research.factors.builtins.size` / `earnings_yield` 可用。

## 背景

路线图原话: "research/evaluate.py——多 horizon 日频 rank-IC + Newey-West t、IC 衰减、分位组合换手调整 IR、覆盖率诊断;所有试验 append-only 落盘 trials.parquet,短样本下 NW t<3 视为噪音。"

任务: 给 `#6b` 的 `Factor` 协议补一个独立评估入口,产出多 horizon rank-IC + NW t + IC 衰减 + 5 分位组合 (复用 `run_backtest`) 换手调整 IR + 4 维覆盖率诊断 + PIT 漏检防回归,试验落 append-only `trials.parquet`。不改 `run_baselines.py`。

`run_baselines.py` 是策略基线 (signal → backtest 一条线),`evaluate.py` 是因子评估 (factor → IC + 分位 IR + 诊断),两者共享 `research.data` + `research.backtest` + `research.factors.protocol`,**禁止互相 import**。

## 作用域

### 新增文件

```
research/evaluate.py             # 主模块: evaluate_factor + run_evaluation + evaluate_all + EvaluationResult + main
research/_trials_store.py        # trials.parquet 长表 schema + append_trial + load_trials + _git_meta
tests/test_evaluate.py           # ≥ 11 个纯单测 (合成面板, 不连 PG)
tests/test_trials_store.py       # ≥ 7 个 trials_store 单测 (tmpdir + pyarrow)
tests/test_evaluate_integration.py  # ≥ 2 个集成测试 (pytestmark integration, pg_db)
```

### 修改文件

```
requirements.txt                 # 加 pyarrow>=14.0
docs/codex_tasks/README.md       # 加 #7 入任务清单, 加 pyarrow 到 "不引入新依赖" 豁免清单
```

### 不动文件 (反需求)

```
research/run_baselines.py        # 独立入口, 保留
research/backtest.py             # 已稳定, 复用 run_backtest / eligibility_mask
research/strategies.py
research/factors/*               # #6a + #6b 产物
research/fundamentals.py / market_cap.py / data.py
db_manager / alembic / db schema / data_sources
```

## 契约

### `research.evaluate.evaluate_factor` (纯计算, 零 IO)

```python
def evaluate_factor(
    factor_values: pd.DataFrame,                 # index = dates, columns = security_ids
    forward_returns: dict[int, pd.DataFrame],    # {horizon: panel}; 索引列同 factor_values
    *,
    eligibility: pd.DataFrame,                   # bool, 同形
    horizons: tuple[int, ...] = (1, 5, 10, 21),
    n_quantiles: int = 5,
    cost_bps: float = 10.0,
    adj_close: pd.DataFrame | None = None,       # 复用 run_backtest 算分位 IR 时用
    nw_lag_rule: Callable[[int, int], int] | None = None,  # None → 默认 Newey-West 1994 plug-in
    min_coverage: int = 50,
    factor_name: str = "anonymous",
) -> "EvaluationResult":
```

**纯函数**: 不读 DB,不写文件,不调 git。所有 IO 在 caller (`run_evaluation`) 完成。

### `research.evaluate.run_evaluation` (取数 + 评估 + 可选落盘)

```python
def run_evaluation(
    factor: str | Factor,
    *,
    engine: Engine,
    start: date,
    end: date,
    as_of: date | None = None,                   # 默认 = end (固定 as_of, v1 简化)
    horizons: tuple[int, ...] = (1, 5, 10, 21),
    n_quantiles: int = 5,
    cost_bps: float = 10.0,
    types: tuple[str, ...] = ("CS",),
    min_price: float = 3.0,
    min_median_dollar_volume: float = 2_000_000.0,
    eligibility_window: int = 63,
    eval_start: date | None = None,              # 默认 start + 252 交易日 warmup
    extra_drop_ids: list[int] | None = None,
    trials_path: Path | None = Path("research/output/trials.parquet"),
    note: str | None = None,
    strict: bool = False,                        # CLI --strict 时 True, PIT break 或 lookahead suspect raise
) -> "EvaluationResult":
```

**流程**:
1. `load_adjusted_panel(engine, start, end + buffer, types=types, as_of=as_of or end)` — `buffer = max(horizons)` 个交易日 (用 trading_calendars 推,否则 `ceil(max_h * 7 / 5) + 5` 自然日兜底)
2. 剔除 `securities_with_uncovered_events(engine, start, end) + (extra_drop_ids or [])`
3. `eligible = eligibility_mask(panel['close'], panel['dollar_volume'], min_price=, min_median_dollar_volume=, window=eligibility_window)`
4. 构造 `ctx = FactorContext(engine=engine, dates=panel_dates_in_window, security_universe=universe_after_drop, as_of=pd.Timestamp(as_of or end))`
5. `factor_obj = get(factor) if isinstance(factor, str) else factor`
6. `factor_values = factor_obj.compute(ctx)`
7. 自算 `forward_returns = {h: adj_close.ffill().shift(-h) / adj_close.ffill() - 1, masked by adj_close.notna() & adj_close.ffill().shift(-h).notna()}` for each `h in horizons`
8. `result = evaluate_factor(factor_values, forward_returns, eligibility=eligible.loc[ctx.dates], horizons=, ..., factor_name=factor_obj.name)`
9. PIT 漏检三件套 (见下方"诊断"小节)
10. 若 `trials_path is not None`: `append_trial(result, trials_path)` → 设置 `result.trial_id`

### `research.evaluate.evaluate_all` (多因子循环)

```python
def evaluate_all(
    *,
    engine: Engine,
    start: date,
    end: date,
    names: list[str] | None = None,
    **kwargs,
) -> list["EvaluationResult"]:
```

`names=None` → `list_factors()`。单因子 `raise` → `logger.opt(exception=...).error(...)` + 继续,返回 list 含 `status="failed"` 占位 `EvaluationResult`。`strict=True` 时单因子失败立即 raise。

### `research.evaluate.EvaluationResult` (frozen dataclass)

```python
@dataclass(frozen=True)
class EvaluationResult:
    factor_name: str
    factor_version: str                          # builtin: git rev-parse HEAD + getsourcefile; 外部: 模块源文件 sha
    code_git_sha: str | None
    code_git_dirty: bool
    horizons: tuple[int, ...]
    eval_dates: pd.DatetimeIndex
    as_of: pd.Timestamp | None
    cost_bps: float
    n_quantiles: int
    universe_hash: str                           # sha1(sorted(security_universe.tolist()))
    universe_size_mean: float
    universe_size_min: int
    params_hash: str                             # 见 pinned decisions 第 8 项
    config: Mapping[str, Any]                    # 完整入参快照
    ic_table: pd.DataFrame                       # rows=horizon, cols=[mean_ic, std_ic, nw_t, nw_lag, n_obs, is_noisy]
    ic_decay: pd.DataFrame                       # long: cols=[horizon, lag, ic]; lag ∈ [0, max(horizons)]
    quantile_metrics: pd.DataFrame               # rows=(horizon, quantile_label) where label ∈ {q1..q5, ls_q5_q1}, cols=[ann_return, ann_vol, sharpe_gross, sharpe_net, ann_turnover, max_drawdown]
    coverage: pd.DataFrame                       # rows=date, cols=[n_universe, factor_coverage, fwd_ret_coverage_given_factor, pit_violations, n_active, n_delisted]
    diagnostics: Mapping[str, Any]               # pit_regression_max_abs_diff, factor_freshness_gap_days, unexpected_coverage_jump_days, skipped_horizons, lookahead_suspect
    status: str = "ok"                           # "ok" | "skipped_all_nan" | "failed"
    trial_id: str | None = None                  # append_trial 设置
    created_at: pd.Timestamp | None = None       # UTC

    def is_noisy(self, t_threshold: float = 3.0, min_obs: int = 60) -> dict[int, bool]:
        """派生方法; 阈值后期可调, 不进 trial schema."""
        ...

    def to_trial_rows(self) -> list[dict]:
        """展开成 long-form rows 喂给 _trials_store.append_trial."""
        ...
```

### `research._trials_store` 模块

```python
TRIALS_SCHEMA_VERSION: int = 1                   # 加列时 bump, 禁止 delete / rename

# long-form: 一个 trial 多行, 每行一个 (horizon, metric) 组合
TRIALS_SCHEMA: tuple[tuple[str, str], ...] = (
    ("trial_id", "string"),
    ("schema_version", "int16"),
    ("created_at", "timestamp[ns, UTC]"),
    ("run_id", "string"),                        # 同次 batch (evaluate_all) 共享一个 run_id
    ("factor_name", "string"),
    ("factor_version", "string"),
    ("code_git_sha", "string"),                  # nullable
    ("code_git_dirty", "bool"),
    ("eval_start", "date32"),
    ("eval_end", "date32"),
    ("eval_start_effective", "date32"),          # 含 warmup 后真正进 IC 的首日
    ("as_of", "date32"),                         # nullable
    ("horizon", "int16"),
    ("metric", "dictionary<string>"),            # 枚举见下
    ("metric_param", "int32"),                   # nullable; ic_decay 装 lag, quantile_* 装 q 标签编码 (1..n, 0=long-short)
    ("value", "float64"),
    ("universe_hash", "string"),
    ("universe_size_mean", "float64"),
    ("universe_size_min", "int32"),
    ("n_dates", "int32"),
    ("params_hash", "string"),
    ("params_json", "string"),                   # canonical_json
    ("cost_bps", "float64"),
    ("n_quantiles", "int16"),
    ("note", "string"),                          # nullable
    ("is_noisy", "bool"),
)

# metric 枚举集合
METRIC_NAMES: frozenset[str] = frozenset({
    "ic_mean", "ic_std", "ic_nw_t", "ic_nw_lag", "ic_decay", "n_obs",
    "q_ann_return", "q_ann_vol", "q_sharpe_gross", "q_sharpe_net", "q_ann_turnover", "q_max_drawdown",
    "coverage_factor_mean", "coverage_factor_p05",
    "coverage_fwd_given_factor_p05", "n_universe_mean", "n_universe_min",
    "pit_regression_max_abs_diff", "factor_freshness_gap_days", "unexpected_coverage_jump_days",
    "flag_horizon_skipped",
})

def append_trial(result: EvaluationResult, path: Path) -> str:
    """单写者约定 (调用方串行, 无 fcntl). 读旧 parquet → reindex(columns=CURRENT_SCHEMA) →
    concat new rows → 写 tmp → os.replace 原子替换. 返回 trial_id.
    同 trial_id 已存在: skip + return existing (幂等).
    row 数 > 100_000 时 logger.warning 建议归档 trials_archive_YYYY.parquet."""

def load_trials(
    path: Path = Path("research/output/trials.parquet"),
    *,
    latest_only: bool = False,
) -> pd.DataFrame:
    """读 trials.parquet. latest_only=True 时按
    (factor_name, factor_version, horizon, metric, metric_param, params_hash)
    取 created_at 最大行, 折叠丢行时 logger.warning 列出被丢的 trial_id."""

def _git_meta() -> tuple[str | None, bool]:
    """subprocess.run(['git','rev-parse','HEAD'], cwd=repo_root, timeout=2, check=False).
    git status --porcelain 非空 → dirty=True. 进程内 LRU 缓存一次. 失败返回 (None, False)."""
```

### `research.evaluate.main` (CLI)

```bash
python -m research.evaluate \
  --factors size,earnings_yield  \           # 或 --all
  --start 2025-01-01 --end 2026-06-15 \
  [--as-of YYYY-MM-DD]            \           # 默认 = --end
  [--eval-start YYYY-MM-DD]       \           # 默认 = start + 252 交易日
  [--horizons 1,5,10,21]          \
  [--n-quantiles 5]               \
  [--cost-bps 10.0]               \
  [--trials-path PATH]            \           # 默认 research/output/trials.parquet
  [--no-persist]                  \           # 与 --trials-path 互斥
  [--note STR]                    \
  [--strict]                                  # PIT break 或 lookahead suspect raise
```

`--factors` 与 `--all` 互斥。`--start` 默认且强制 ≥ `FACTOR_TRUST_FLOOR = 2024-05-14` (因子可信下限,违反 `raise`,与 `run_baselines` 同硬规)。`stdout` 打印 markdown 表 (`horizon × [ic_mean, nw_t(*=noisy), q_ls_sharpe_net, coverage_p05, pit_violations_max, n_obs]`,`*` 表示 `is_noisy`),同步落 `research/output/evaluate_{factor}_{start}_{end}.md` 含 IC 衰减 + 5 分位明细 + PIT 检查明细。

## 关键不变量 (必须测试锁)

### 数值方法

1. **rank-IC 是 Spearman**: `factor.rank(axis=1) ↔ forward_return.rank(axis=1)` 的日截面 Pearson。`method="average"`,`na_option="keep"`。**不引入 scipy**,测试用硬编码常数对拍 `scipy.stats.spearmanr` 已知结果。
2. **NW lag 默认公式**: `nw_lag(h, T) = max(h, floor(4 * (T / 100) ** (2 / 9)))`。Newey-West 1994 plug-in 与 horizon overlap 修正取大。**不引入 statsmodels**,测试 T=100→max(h,4) / T=520→5 / T=1000→max(h,6) 数值锁。
3. **forward return 定义**: `fwd_h[t, i] = adj_close.ffill().shift(-h)[t, i] / adj_close.ffill()[t, i] - 1`,屏蔽 `valid_pair = adj_close.notna() & adj_close.ffill().shift(-h).notna()`。与 `backtest._returns_with_gap_recovery` 同口径。
4. **noisy 判定**: `is_noisy = (abs(nw_t) < 3.0) | pd.isna(nw_t) | (n_obs < 60)`。**原始 nw_t 不擦**,只额外加 `is_noisy` bool 列。阈值参数化 `noise_threshold=3.0`、`min_obs=60` 落 `config` 列以便日后重判。

### 分位组合

5. **n_quantiles=5**: 主指标 `q5_minus_q1` 等权多空 (top `+0.5/N_top`,bottom `-0.5/N_bot`,gross=1.0 net=0.0);同时报 `q5_long_only` (gross=1.0,`1/N_top`)、`q1_long_only`。
6. **再平衡间隔 = horizon 个交易日**: `index[::horizon]`,**不用 `rebalance_dates('M')`**。每个 rebalance 日先按 `eligibility[t]` mask 再分桶,有效 N<100 当日整组空。NaN 信号名字从分位分配剔除。
7. **IR 复用 `run_backtest`**: 三组权重 (`q5_long_only`, `q1_long_only`, `q5_minus_q1`) 各跑两轮 `run_backtest(name, weights, adj_close, cost_bps=10.0, hold_through_gaps=True)` 与 `cost_bps=0`,取 `.metrics()['sharpe']` 作 `sharpe_net` / `sharpe_gross`。long-only 额外报 `sharpe_vs_equal_universe_basis` (basis = 等权 eligible universe `run_backtest` 的 `daily_returns`)。

### 覆盖率 4 维

8. **逐日 4 列**:
   - (a) `factor_coverage[t] = (factor.notna() & eligible).sum(axis=1) / eligible.sum(axis=1)`,分母=0 当日剔除
   - (b) `fwd_ret_coverage_given_factor[t] = (factor.notna() & fwd_ret.notna() & eligible).sum(axis=1) / (factor.notna() & eligible).sum(axis=1)`
   - (c) `by_listing_status`: `n_active`、`n_delisted` 两列 (按 `securities.is_active`)
   - (d) `pit_violations[t]` = factor 在 `date > ctx.as_of` 的非 NaN cell 数,期望 0 (主防线)
9. **聚合落 trials**: `coverage_factor_mean / coverage_factor_p05 / coverage_fwd_given_factor_p05 / n_universe_mean / n_universe_min`。`fwd_ret_coverage_given_factor_p5_floor = 0.95` 是 spec 阈值,**仅 flag 不擦数据**。

### PIT 漏检防回归

10. **三件套**:
    - `pit_violations[t]` (上方 8d) — 主防线
    - `pit_regression_max_abs_diff`: `run_evaluation` 内抽 3 个历史 t (eval_end 往回 60/120/180 交易日),用 `FactorContext(as_of=t)` 重算因子,与 live-run 在该 t 的值 `abs diff` 取 max。**>1e-9 `logger.warning`,>1e-6 `lookahead_suspect=True`** (落 trials,`strict=True` 时 raise)
    - `factor_freshness_gap_days = max(ctx.dates) - max(non-NaN factor date)`,纯陈旧度

### 边界 / 异常

11. **空因子 / 全 NaN**:
    - `factor.reindex(ctx.dates × ctx.security_universe).dropna(how='all', axis=0).empty` → `raise FactorEvaluationError`
    - 单 horizon 全 NaN → 跳过该 horizon + `logger.warning` + trials 行 `metric="flag_horizon_skipped"` value=1
    - 日截面 N < `min_coverage` (默认 50) 整日 IC 置 NaN
12. **`as_of` 固定 = `end`**: v1 简化,**spec 注明已知限制**,用 `pit_regression_max_abs_diff` 抽样兜底。fundamentals.asof_panel 加入 vintage as_of 入参后升级到滚动 (留给后续任务)。

### trials.parquet

13. **trial_id 内容寻址**: `sha1(factor_name + factor_version + universe_hash + params_hash + eval_window + as_of + code_git_sha).hexdigest()`。`append_trial` 检测同 trial_id 已存在则 skip + 返回 existing (幂等)。
14. **params_hash 范围**: `sha256(canonical_json sort_keys=True separators=(',',':') ensure_ascii=False)` 含 **全部 eval params + factor params + universe filter**,**排除 `note`**。`float` 用 `repr`,拒绝 NaN/Inf (raise)。
15. **append 实现**: 单文件 `read+concat+atomic os.replace`,**单写者约定** (无 fcntl)。schema 演进通过 `TRIALS_SCHEMA_VERSION` bump + `reindex` 旧行 NaN;**禁止 delete/rename 列**。row count > 100_000 logger.warning 建议归档,不自动滚动。不分区 (MVP)。
16. **trials.parquet 入 `.gitignore`**: 二进制 + 频繁 append 会膨胀历史。spec 顺便写一份 `research/output/trials_README.md` 说明导出协议 (csv 跨机器搬运)。
17. **`load_trials(latest_only=True)`**: 按 `(factor_name, factor_version, horizon, metric, metric_param, params_hash)` 取 `created_at` max,折叠丢行时 `logger.warning` 列出被折叠的 `trial_id`。

### universe 过滤管道 (在 run_evaluation 中)

18. **顺序**:
    1. `panel = load_adjusted_panel(engine, start, end + buffer, types=types, as_of=as_of or end)`
    2. `bad = set(securities_with_uncovered_events(engine, start, end)) | set(extra_drop_ids or [])`
    3. drop `bad` 列
    4. `eligible = eligibility_mask(panel['close'], panel['dollar_volume'], min_price=, min_median_dollar_volume=, window=eligibility_window)`
    5. `universe = panel['adj_close'].columns`
    6. `eligibility.any(axis=0) == False` 的列再剔
19. **因子 vs forward return 口径分层**: `evaluate` **不约束**因子用 adjusted 还是 raw — 因子自管 PIT 与口径 (`SizeFactor` 用 raw_close × PIT shares 是正确的)。`evaluate` 只强制 forward return 用 `research.data.load_adjusted_panel` 出的 `adj_close`,**不允许临时 SQL**。

## 测试 (≥ 20 个新增)

### `tests/test_evaluate.py` — 至少 11 个纯单测 (合成面板)

1. `test_perfect_ic_yields_high_nw_t` — factor = forward_return 1-day 完美相关,assert `nw_t > 10` 且 `ic_mean > 0.99`
2. `test_pure_noise_factor_is_noisy` — factor = random normal,assert `|ic_mean| < 0.02`、`|nw_t| < 1.5`、`is_noisy == True`
3. `test_lookahead_detection` — factor = `forward_returns[1].shift(0)` (用了未来),assert `ic_mean > 0.5` 且 `ic_decay_halflife < 2` → `diagnostics["lookahead_suspect"] == True`
4. `test_ic_decay_monotonic_on_ar1` — 合成 AR(1) k=5 因子,assert IC 衰减序列对 horizon 单调下降
5. `test_rank_ic_matches_spearmanr_hardcoded` — 3 个手算 case,与 `scipy.stats.spearmanr` 已知结果 ±1e-10 相符
6. `test_nw_lag_default_formula` — `T ∈ {100, 252, 520, 1000}` × `h ∈ {1, 5, 21}` 数值锁
7. `test_quantile_ir_matches_run_backtest_directly` — 同样 weights / adj_close,`evaluate_factor` 算的 q5 sharpe 与直接 `run_backtest` 同源 (严格相等)
8. `test_coverage_diagnostic` — eligible 全 True 但 factor 半数 NaN,assert `factor_coverage.mean ≈ 0.5 ± 0.05`
9. `test_empty_factor_raises_and_single_horizon_all_nan_skips` — 全 NaN factor raise `FactorEvaluationError`;单 horizon 全 NaN warn + skip + trials 行 `flag_horizon_skipped`
10. `test_factor_context_as_of_truncates_dates` — `as_of` 比 `end` 早,assert factor.compute 只看到 `dates <= as_of`
11. `test_pit_regression_triggers_lookahead_suspect` — 注入一个"今天能看到明天"的合成 factor,assert `pit_regression_max_abs_diff > 1e-6` 且 `lookahead_suspect == True`

### `tests/test_trials_store.py` — 至少 7 个 (tmpdir + pyarrow)

1. `test_append_creates_parquet_when_path_does_not_exist`
2. `test_append_twice_accumulates_rows` — 同 result append 2 次,第二次 skip (幂等),total 行数不变
3. `test_append_atomic_on_error` — 模拟 write 中断,assert 旧 parquet 完整 (os.replace 原子)
4. `test_schema_version_reindex_old_rows_nan` — 模拟旧 schema 缺一列,assert 读出来该列为 NaN 不抛
5. `test_git_meta_returns_none_outside_git_repo` — tmpdir 不是 git 仓库,assert `(None, False)`
6. `test_trial_id_is_content_addressed` — 同内容两次构造 EvaluationResult,trial_id 严格相等
7. `test_params_hash_excludes_note` — 同参数不同 note → 同 params_hash

### `tests/test_evaluate_integration.py` — 至少 2 个 (`pytestmark = pytest.mark.integration`, pg_db)

1. `test_earnings_yield_full_pipeline` — pg_db 注入合成 securities/shares/prices/NetIncomeLoss,跑 `run_evaluation(EarningsYieldFactor, ...)`,assert IC 表 shape 正确、PIT 边界 (fundamentals.filed_date+1 天) 守住、落 trials.parquet 路径有效
2. `test_size_full_pipeline_with_run_baselines_share_dir` — 跑 `run_evaluation(SizeFactor, ..., trials_path=tmp/trials.parquet)`,assert 与 `run_baselines.py` 的 `research/output/` 同目录共存无冲突

## 验收

```bash
# 1. 单测
python -m pytest tests/test_evaluate.py -q -m "not integration"

# 2. trials_store 单测
python -m pytest tests/test_trials_store.py -q

# 3. integration (本机需 PG; sandbox 会 skip)
python -m pytest tests/test_evaluate_integration.py -q

# 4. 全套无回归 — 至少 302 + 20 = 322 passed
python -m pytest tests/ -q
```

`tests/test_evaluate.py` ≥ 11 unit。`tests/test_trials_store.py` ≥ 7。`tests/test_evaluate_integration.py` ≥ 2 (sandbox skipped, 本机过)。**全套 ≥ 322 passed**。

## 反需求 (绝不能做)

1. **不要**改 `run_baselines.py` (独立入口保留)
2. **不要**在 evaluate 里 import `run_baselines.py`,反之亦然
3. **不要**改 `research/factors/*` (#6a + #6b 产物已稳定)
4. **不要**改 `research/fundamentals.py` / `research/market_cap.py` / `research/data.py` / `research/backtest.py`
5. **不要**改 db schema / alembic / db_manager / data_sources
6. **不要**做行业中性化 (留给后续 #8)
7. **不要**同时报 Pearson level-IC,只报 Spearman rank-IC
8. **不要**引入 scipy / statsmodels (NW + Spearman 用 numpy + pandas.rank 自实现)
9. **不要**用 `rebalance_dates('M')`,分位再平衡间隔严格 = `horizon` 个交易日 (`index[::horizon]`)
10. **不要**把 `ctx.as_of` 接到 v1 的滚动 loop (v1 固定 `as_of=end`)
11. **不要**自动滚动归档 trials.parquet (row > 100k 仅 logger.warning,手工归档)
12. **不要**捕异常 swallow — 让上层处理,单因子在 `evaluate_all` 中失败 log + 继续是显式策略不是 swallow

## 实现建议

- `evaluate_factor` 是纯函数,所有 IO (数据库 / 文件 / git) 都在 `run_evaluation` 中
- `FactorEvaluationError` 自定义异常,继承 `Exception` 即可
- `EvaluationResult.to_trial_rows()` 把标量 metrics + ic_decay 长格式 + quantile_metrics 长格式 + coverage 聚合后展开成 `list[dict]`,每条 dict key 对齐 `TRIALS_SCHEMA`
- `_git_meta()` 用 `functools.lru_cache(maxsize=1)` 进程内缓存
- pyarrow 用 `pa.Table.from_pylist + pa.parquet.write_table`,读用 `pa.parquet.read_table().to_pandas()`,**不要**用 `pd.read_parquet/to_parquet` (无法精确控 dictionary 类型)
- README 同步加 pyarrow 豁免:措辞 "pyarrow 限定用于 research/ 列式 I/O,不得渗透进 db_manager / data_sources / utils"

## 工作时长估算

12-18 小时 (数值方法精细、测试覆盖宽、长 schema 设计)。
