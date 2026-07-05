# Corporate Actions 20 年回填（2026-07，来源：fundamentals_massive_new.tgz）

## 背景与定位

20 年日线回填（见 `day_aggs_backfill_2026-07.md`）完成后，复权因子仍受制于
Massive 免费档 730 天公司行动窗口：`corporate_actions` 中 MASSIVE 源事件最早
ex_date = 2024-05-14，`computed_adjustment_factors` 可信下限由此而来，所有回测
被卡在约 2 年窗口。

本项目用 `~/Documents/fundamentals_massive_new.tgz`（2026-04-19/20 对 Massive
的一次性快照抓取，44,320 ticker）中的 `corporate_actions/US/splits.parquet`
（26,710 行，1978-2026）与 `dividends.parquet`（699,947 行，2003-2027）补齐
2003 起的真 vendor 事件。事件 id 与 live sync 的 `source_event_id` 同源（E 前缀），
可幂等 upsert，与 2024+ 存量零重复。

关键存量事实（导入设计的前提）：

- prod `corporate_actions` 235,717 行中 171,414 行是 **source='POLYGON' 的
  legacy 深历史**（id 为 `massive-dividend:%`/`massive-split:%` 合成格式，
  2002 起）；64,303 行是 source='MASSIVE' 真 vendor id（2024-05-14 起）。
- 因子构建（`update_adjustment_factors._load_actions_and_prices`）按
  `upper(source)='MASSIVE'` 过滤——**POLYGON 行从不参与因子链**。所以归档以
  source=MASSIVE 入库后，因子重建自动向前延伸到 2003，legacy 行零干扰。

## 导入前审计（2026-07-05，六路并行对账）

1. **著名拆分召回**：2003 后 21/22 命中（日期与比例全部精确）。唯一确认缺失：
   **GOOGL 2014-04-03 C 类股拆分**（vendor 盲区，以 stock dividend 形式执行）——
   须在因子重建前人工补 MANUAL 事件，否则 GOOGL 2004-2014 复权价错 2 倍。
2. **稳定分红连续性**：15 只长期派息股 2004-2025 无缺季（O 的月度跨年错位是
   日历边界假象）。副产品发现 vendor 双发脏数据：CVX 28 对、T 21 对、MO 7 对
   完全重复分红（不同 E-id）；**CVX 2004-09-13 的 2:1 拆股也双发**——vendor 自己的
   `split_adjusted_cash_amount`/`historical_adjustment_factor` 因此被除 4 而非除 2，
   **两列已证实损坏，一律不读不写**。
3. **E-id 重叠段对账**（2024+ 双方都有真 id）：快照覆盖范围内 id 匹配率 99.98%，
   金额/日期字段一致率 ≥99.98%（差异均为外币 ADR 亚分位修订）。
4. **合成行覆盖对账**：pre-2024 窗口内 prod POLYGON 合成行与归档按
   (symbol, ex_date, 类型, 值) 匹配 ~99%（165,394/165,395 金额位级一致——
   证实归档金额与存量同为申报时名义值口径）；~1,242 行合成行归档缺失
   （tpl/sf 等已证实归档确有少量漏漏，删除策略须保守）；496 行合成行在任期外
   （z/sbc/rpc/xtkg 等 122 只证券的回收污染，prod 侧既有问题，转 repair_identity）。
5. **退市覆盖**：归档对退市证券的事件覆盖良好（ATVI/FRC/XLNX 等抽查通过），
   vendor 按 symbol 返回全历史，不限于快照时活跃的 ticker。
6. **金额口径**：cash_amount = 申报时名义值（AAPL 2.65→3.29→0.47→0.205 序列证实），
   与因子公式 `(prev_close - cash) / prev_close` 所需口径一致。

裁决：**go-with-conditions**，20 条确定性清洗规则（R1-R20），全部实现于
`scripts/import_corporate_actions_archive.py`（对审计规则的三处有意偏离已在
脚本 docstring 声明：R4 沿用 day_aggs 更严的大写 ticker 正则；R14 缺币种按 USD
兜底而非存 NULL——upsert 会丢 NULL 币种行；R15 因默认 cutoff 而无未来事件）。

## 导入脚本与防线

`scripts/import_corporate_actions_archive.py`，规则详见其 docstring。防线自内向外：

1. 窗口 [2003-01-01, 2024-05-14)：上界起归 live sync。
2. 归档内清洗：精确重复保留（prod 已有 id 优先，否则最小 id）；同日比例矛盾
   全组隔离（352 行，含审计点名的 NYC/LADR/SLG/ALEX 全部在此拦截）；
   P 前缀 spinoff 伪拆分隔离（156 行，IBM/Kyndryl、MMM/Solventum）；
   比例量级不过滤（极端值抽样证实是真实 OTC 反向拆分，仅示警）。
3. 任期归属：复用 `import_day_aggs.build_tenures`（symbol history 优先 +
   list_date/退市上界裁剪 + 链式推断），0 命中/多命中一律隔离——绝不猜。
4. 值冲突挂起（R13）：与 prod 既有行（任意 source）同证券同类型同 ex_date 但值
   不一致的归档事件不入库，输出 mismatch 报告。比较须把两边量化到列精度
   Numeric(20,10) 且用 ROUND_HALF_UP（PG numeric 舍入口径），否则全精度归档值
   vs 10 位存量会产生大批伪冲突（实测 75 → 10 条真冲突）。**机器强制**由
   `research.data.securities_with_uncovered_events` 承担（见下）。
5. 结构性只插入：归属后跳过 (security_id, source_event_id) 已存在于 prod 的行
   ——upsert 是 update_on_conflict 语义且无保护列，若放行，误用 `--cutoff none`
   或 vendor 事后修订 ex_date 跨越窗口边界时，2026-04-19 快照旧值会逐字段冲掉
   更新鲜的 live 行；跳过即防线。
6. R19 记账断言：清洗后每行必有唯一去向，对不上即中止。

输出三份报告：`corp_actions_archive_quarantine.tsv`（ticker×原因聚合）、
`corp_actions_archive_quarantine_detail.tsv`（可恢复类别的行级明细：
out_of_tenure/ambiguous/conflicting_split/spinoff/before_min_date 带 date+id+值，
清算分红等可按明细人工补录；unmapped_no_symbol 与 cutoff 后事件只聚合，是对审计
R5 侧车要求的有意收窄）、`corp_actions_archive_mismatch.tsv`（值冲突双方值）。

**值冲突的机器强制**（评审 F1 修复）：`securities_with_uncovered_events` 新增分支
——非 MASSIVE 事件若同日没有同类型 MASSIVE 行即视为复权链上的洞，证券自动剔出
研究面板。挂起证券（争议日只剩 POLYGON 孤行）、未确认保留的合成行、归档漏抓的
证券全部被同一机制覆盖；人工裁决落库（更正/删除 POLYGON 行或补录正确 MASSIVE
事件）后 gate 自动放行，不依赖任何名单文件或人工记忆。

`--retire-synthetic`（导入后单独跑）：删除已被位级一致 MASSIVE E-id 行确认的
POLYGON 合成行（同证券同类型同 ex_date；分红金额+币种精确相等、拆股比例
rtol 1e-6），落实 CLAUDE.md"真 vendor id 出现时清理合成 id"规则；无 E 对应的
合成行保留（归档确有少量缺漏，删了会让真实除权日失配）。

## 评审记录（2026-07-06，3 维评审 + 逐条对抗验证）

12 个 agent、9 个原始 findings、5 个确认（4 个被对抗验证驳回），全部已修复：

- F1(major) R13 排除名单只有 TSV+人工记忆 → gate 分支机器强制（见上）。
- F2(minor) 隔离报告丢失 date/id/值 → 行级明细报告。
- F3(major) `--cutoff none` 时 upsert 可冲掉 live 行 → 结构性只插入。
- F4(minor) vendor 修订 ex_date 跨界的残余冲写 → 同 F3 的 pair 判重覆盖。
- F5(minor) retire-synthetic 计数缺 DISTINCT 虚报 → SELECT DISTINCT。

## Dry-run 基线（2026-07-06，只读连 253 生产）

- 窗口内：分红 596,520 / 拆股 23,300 条
- 入选：**分红 239,780 + 拆股 3,337 条，8,446 只证券**
- 剔除去向：unmapped_no_symbol 287,031（OTC 外国线/基金/权证，不在 CS/ETF
  universe，预期内）；suffix_class 36,057（优先股/权证小写后缀）；
  out_of_tenure 32,706 + ambiguous 946（回收防护）；重复 2,176+27；
  比例矛盾 352；spinoff 156；值冲突挂起 10 条（CVI 2021 金额、CNHI 的
  EUR 申报 vs USD 折算 8 条、FBL 2023 疑似 59 倍错值——3 只证券待人工裁决）
- 与审计独立预测精确吻合：2,176 / 27 / 352 / 156 / 缺币种 5

## 上线步骤（apply runbook）

1. 同步代码到 253；scp 两个 parquet 到 `/home/wenruifeng/data/fundamentals/corporate_actions/US/`。
2. 253 上 `--dry-run` 复核计数与本地基线一致；备份库（备份保留 7 天，磁盘 30G 硬约束注意）。
3. 正式导入（预期 +243,107 行 corporate_actions：239,770 分红 + 3,337 拆股）。
4. `--retire-synthetic`（预期删除 ~165k POLYGON 合成行；先 dry-run 看计数）。
5. GOOGL 2014-04-03 MANUAL 拆股事件人工补录（1:2.002，Class C 分发）。
6. 值冲突 3 只证券（CVI/CNHI/FBL）人工裁决：把正确值落库（更正/删除 POLYGON 行
   或补录正确 MASSIVE 事件）后 `securities_with_uncovered_events` 自动放行；
   裁决前这些证券被 gate 自动剔出研究面板，不阻塞整体上线。
7. `update_adjustment_factors --all`（methodology_version 仍为 raw_actions_v1：
   输入事件集本身变了，因子全量重算；pre-2024 无 vendor reference 属预期，
   对账状态为 SUCCESS_NO_VENDOR_REFERENCE）。
8. 移动研究层数据边界：`research/data.py` 的 2024-05-14 面板下限 → 2003-01-01，
   重算 `securities_with_uncovered_events`；跑 `check_data_integrity`、
   `health_report`、全量 pytest（含复权一致性测试）。
9. 抽样验证 ~50 只证券的重建因子链 vs ex-date 前后价格跳变比例
   （pre-2024 唯一可用的独立验证）。

## 残余风险（审计原文摘要）

- GOOGL 式 vendor 盲区（以 stock dividend 执行的类股拆分）在小盘/OTC 上未测量，
  只能靠价格跳变异常扫描兜底。
- 快照缺漏：已证实存在少量真实事件缺失（tpl/sf 模式）；pre-2024 无 live 数据
  兜底，缺失不可检测（除价格跳变启发式）。
- pre-2024 无 vendor factor reference，因子链只能内部验证。
- symbol_history 完整性决定归属上限：未记录的改名会让真实历史滞留 unresolved。
- 快照无 per-row 可见性时间戳：as-of 2024 前的复权查询内嵌 2026 年知识
  （对因子水平可接受，严格 PIT 回测须知晓）。
- pre-2003 仍是硬地板，yfinance 时代更早价格保持未复权。
