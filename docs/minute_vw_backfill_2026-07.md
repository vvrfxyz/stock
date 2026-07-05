# WithVW 回填：daily_vw VWAP 补齐 + 分钟线入 ClickHouse（2026-07）

## 数据源

`~/Documents/WithVW/`（本机 Mac，2026-07-05/06 下载）：

- `daily_vw/`（1.6G）：US/year=YYYY/month=MM/data.parquet，2003-09 起逐月日线，
  列 date/symbol/OHLC/volume/**vw**/transactions；`补缺/year={2021,2022,2023,2026}`
  为主下载缺失日期的补齐件；download_progress json 按日期记录状态。
- `intraday_1m_vw/`（135G）：year=YYYY.tar.gz（2003-2026），内含 UTC 月对齐的
  US/year/month/data.parquet 分钟线（datetime UTC/symbol/OHLC/volume/vw/transactions），
  含盘前盘后（约 4:00-20:00 ET），未复权原始价。2003-09 单月即 1269 万行，
  全量估约 40-50 亿行。

## 方案

### Part A：daily_vw → PostgreSQL `daily_prices.vwap`（`scripts/import_daily_vw.py`）

背景：flat files（SIP）时代的 22,680,574 行日线 vwap 为 NULL（三指纹之一：
vwap NULL + trade_count 有）。daily_vw 与 flat files 同为 SIP 汇总口径，
close/volume 应位级一致（审计验证），其 vw 可安全嫁接。

规则：**只 UPDATE 不 INSERT**（bar 存在性以 flat files 为 truth）；守卫
`vwap IS NULL AND trade_count IS NOT NULL AND date < 2024-01-01`——yfinance
双 NULL 行与 Massive 行物理上碰不到；任期归属复用 import_day_aggs（同正则、
同 ambiguous 守卫）；TEMP 表 COPY + UPDATE JOIN，每文件一事务，幂等；
记账断言两级（输入去向 + join 分桶）。

指纹体系影响（已核实代码只有两处依赖）：import_day_aggs 的 Massive 保护边界
是 `max(min vwap 日, era_start)`，回填只会把 min 拉早、被 era_start 钳住，无行为
变化；purge 只认双 NULL。指纹口径更新为：**flat files = trade_count 有
（vwap 2026-07 起已回填，分源判定用 date < 2024 + trade_count）；Massive = vwap 有
且 date >= 2023H2；yfinance = 双 NULL（不变）**。

### Part B：intraday_1m_vw → ClickHouse `stock.minute_bars`

依据 `docs/archive/polyglot_persistence_architecture.md` 的分钟级预留设计执行；
253 上 `stock-clickhouse` 容器（24.12）仍在运行，DDL 从 git 历史恢复
（`sql/clickhouse/polyglot_persistence.sql`）+ 新增 `sql/clickhouse/minute_bars.sql`。

表设计：ReplacingMergeTree(ingested_at)，PARTITION BY toYYYYMM(ts)（UTC），
ORDER BY (security_id, ts)，ZSTD 压缩；`security_id` 仍是全局锚点。

管线（转换全部在 ClickHouse 内完成，Python 不逐行搬数十亿行）：

1. `--refresh-tenures`：从 PG 导出代码任期（复用 `import_day_aggs.build_tenures`），
   Python 侧消解同 symbol 重叠区间（重叠段双方出局 = ambiguous 守卫同语义），
   全量替换 `stock.symbol_tenures`。
2. 每月：DROP PARTITION（幂等）→ parquet 原生 `FORMAT Parquet` 灌 staging →
   `INSERT SELECT` 联任期表转换：`^[A-Z][A-Z0-9.]*$` 先过滤（防 'AAp'→'aap' 撞
   真 ticker），ET 交易日（`toDate(ts,'America/New_York')`）落在任期半开区间才入库
   → TRUNCATE staging。
3. 记账：staged = inserted + suffix_class + unmapped，不平即停；台账
   `logs/manual_backfill/minute_bars_ledger.tsv`。
4. 编排：`scripts/run_minute_backfill.sh`（本机跑）逐年 scp → 装载 → 删远端 tar，
   预取下一年与装载流水线并行；年级台账断点续传；**Mac 上的源归档保留不删**。

读取层：`research/minute_bars.py`（HTTP 接口，无新依赖；`load_minute_bars`
按 security_id + ET 日期窗口 + 可选常规时段过滤）。复权口径：分钟价 × 日级因子
（因子按 ex_date 日级生效，分钟粒度无额外语义）；本层只出原始价。

### 弃选项与理由

- 分钟线进 PostgreSQL：数十亿行的扫描负载与 31.6M 行的日线表不在一个量级，
  归档架构文档明确否定。
- vwap 以新增 `source` 列重构指纹：本可更干净，但现有指纹的代码依赖仅两处且
  均不受影响，为 2268 万行回填做全表 schema 变更不划算；留给未来真正需要
  多源日线时再做。

## 上线序列

1. 审计 workflow（5 路：daily 覆盖/一致性/回填模拟、intraday 完整性/映射率）go 后执行。
2. Part A：scp daily_vw 到 253 → dry-run 复核 → 正式回填（预期 ~22M 行 UPDATE）。
3. Part B：init DDL → refresh tenures → 编排器后台跑（估 8-14 小时）→
   台账对账 + AAPL 分钟重构日线抽验。
4. CLAUDE.md（指纹口径 + ClickHouse 复活说明）+ commit + 部署。

## 上线记录

（执行后回填）
