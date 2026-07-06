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

### 导入前审计（2026-07-06，5 路并行 + 裁决，双 go-with-conditions）

- **daily_vw 同血统实锤**：三个抽样月 299,715 对 join，close **位级一致 99.7357%**，
  其余 0.26% 全部溯源到 40 个改名/回收 symbol 的裸 join 假象（任期归属正好消除）。
  volume/transactions 有 37.5% 行不同但**严格单向**（daily_vw ≥ prod，中位 +0.43% /
  +2 笔）——daily_vw 是同一 SIP 汇总的更晚切片，只嫁接 vw 不动 volume/tc。
  vw∈[low,high] 含率 97.3%（越界中位仅 0.13%，盘前盘后/碎股纳入所致）。
- **覆盖**：主树 2003-09-10 ~ 2026-04-22 共 47.3M 行；2021 缺 1-2 月、2022/2023 缺
  1 月、2026 年 1-2 月——全部由 补缺 件补齐（补缺是超集，85% 行与主树重复，
  处理顺序主树在前使重复自然落 already_has_vwap 桶）。补齐后零缺口。
- **回填模拟（2014-03 基线）**：updatable 86,686 / already 538 / yfinance 2,312 /
  no_pg_row 69,961（43.9% 的 daily_vw 行是 OTC/后缀类，PG 本就无 bar，只计数）。
- **分钟线质量**：三个月 6705 万行抽样，OHLC 合法率 99.99975%（167 行零价，
  与日线 sub-penny 下溢同类，装载时过滤），**(symbol, datetime) 零重复**；
  ET 时段 04:00-20:00（2003 年代无 04:00 盘前，盘后占比随年代升至 6.2%）；
  AAPL 常规时段重构日线 **O/H/L 15/15 精确命中** prod 原始价（未复权确认过
  2014 年 7:1 拆分前后价位）；close 与官方收盘竞价差 $0.00-0.16、volume 低
  1.8%-23.3%（竞价与合并量不在分钟条内）——**永远禁止用分钟加总回填日线**。
- **总量估算**：~65 亿行 ±15%。
- **映射率**：现 symbol 直配下 2003 年代 66.8% 行 / 81.9% 美元成交额，2024 年代
  88.0% / 95.5%；任期归属会进一步提高 2003 年代（死票在 history 里的部分）。
- 裁决的两个 "blocking" 前提已被实测推翻：253 实际空闲 288G（30G 硬约束是
  13F 时代旧状态）；stock-clickhouse 容器健在（24.12，2026-06 只删了代码层）。

### 条件落实

- A1 同实体守卫：UPDATE 谓词加 `dp.close = t.close`（列精度 cast），归属错误行
  落 entity_mismatch 桶绝不写入——审计证明同实体 close 位级相等，零覆盖损失。
- A2 已普查：全库 137,488 行 pre-2023H2 的 vwap（6,306 只，最早 1993）是早期
  Massive 抓取的 vwap 在 day-aggs 重导时被"保留既有 vwap"的 upsert 语义留下的
  真 vendor 值，保留；守卫使其落 already_has_vwap 桶。
- A3 指纹口径与导入同 commit 更新（CLAUDE.md）；代码依赖仅 import_day_aggs 两处，
  一处被 era_start 钳制、一处只认双 NULL，均不受影响。
- B5 日期普查内置装载台账（每月记 ET 日期数），装载完与 trading_calendars 对账，
  有洞如实报告（补数据另立项）。
- B7 验收门：每年抽样重构 O/H/L 精确比对 daily_prices（close/volume 明确除外）。
- B8 零价过滤 + tz 处理（CH 原生读 tz-aware UTC parquet，无 pandas 双重 localize 风险）。

### 执行记录

**Part A（2026-07-06 完成，26 分 40 秒）**：输入 56,286,400 行（含补缺件），
实际 UPDATE **22,313,394 行** vwap；flat 时代 vwap NULL 由 22,680,574 降至
367,180（**覆盖率 98.4%**），残余为 daily_vw 缺 bar / vw 无效（148,845）/
同实体守卫挡下的可疑归属（entity_mismatch 8,307，零写入）。
yfinance_untouchable = 0 是正确结果——day-aggs 导入时的 --purge-remnants 已把
重叠的 yfinance 幽灵行清掉，审计模拟里的 2,312 行是其裸 symbol join 的假象。
补缺重叠段在 live 下按序落 already_has_vwap 桶（5.70M vs dry-run 1.59M），
幂等性即由此验证。

**Part B（2026-07-06 完成，全程约 2 小时 45 分）**：24 个年包逐年流水线
（scp 预取与装载并行，单年 4-12 分钟），272 个月分区全部入库：

- `stock.minute_bars`：**5,056,578,492 行 / 76.4 GiB**（ZSTD 后 16.2 B/行），
  17,006 只证券，2003-09-10 ~ 2026-04-23。
- 台账逐月记账全平：staged 7,354,678,604 = inserted 5,056,578,492 +
  suffix 71,521,428 + unmapped 2,226,569,870 + zero_price 8,814。
  unmapped（30%）以 2003 年代死票身份缺口为主（审计预告：行占比高、
  美元成交额占比 2003 年代仅 ~18%、2024 年代 ~4.5%），源 parquet 保留在
  Mac `~/Documents/WithVW/`，身份修复扩展后可回收。
- B5 日期普查：分钟数据 distinct ET 日 5,691，对照 daily_prices 5,710——
  差异 19 天全部是节假日（元旦/MLK/感恩节/圣诞、2012-10-29/30 飓风 Sandy
  休市）上 daily 侧的 yfinance 双 NULL 幽灵行（每天 1-3 行）。
  **分钟数据零缺日**；trading_calendars 只覆盖 2010+，不作基准。
- B7 验收门：AAPL(2003)/MSFT(2010)/SPY(2016)/TSLA(2021)/NVDA(2024) 各 5 天
  常规时段重构 O/H/L vs daily_prices：**19/20 位级命中**。唯一差异为
  NVDA 2024-06-10（10:1 拆分生效首日）daily 侧 high=195.95 在当日
  117-123 区间中是脏数据（拆分日污染），分钟侧 123.10 正确——
  待修：将该行 high 订正或走 vendor 复核（记 follow-up）。

发现与遗留：

- ~~daily_prices 在 19 个节假日上共 24 行 yfinance 幽灵 bar~~ **已清理（2026-07-06）**：
  按"该日期全表只有双 NULL 行"识别 19 个休市日（含 2012 飓风 Sandy、2011-01-01
  周六），删除 24 行并重算水位线；此后 daily_prices 与分钟线的日期宇宙精确一致
  （2003-09-10 起同为 5,691 个交易日），全库周末/休市日行归零。
- 其余 yfinance 双 NULL 行（264.7 万）**保留不删**：pre-2003 深历史 162 万行是
  库内唯一的 2003 前日线；post-2003 的 101 万行是已覆盖证券的 OTC 填缝
  （(security_id, date) 主键保证它们所在的日子没有任何其他来源的 bar，
  vwap 回填实测零重叠）+ 27 只 yfinance-only 证券——删除等于丢失唯一数据，
  不是去冗余。读取层按双 NULL 指纹过滤即可。
- NVDA 2024-06-10 high 脏值（daily 侧，vendor 数据），follow-up 订正。
- 编排器中途发现两处环境问题并已修复：wenruifeng 无 docker 组权限 →
  装载器全面改走 ClickHouse HTTP（8123，与读取层一致，无 docker 依赖）；
  DDL 解析对注释开头语句块的过滤 bug。
