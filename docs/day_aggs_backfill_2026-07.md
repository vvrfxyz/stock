# 20 年日线回填工程记录（2026-07-05）

一次性工程：将购买的 Polygon `day_aggs_v1` 日线 flat files（2003-09-10 ~ 2026-04-15）
灌入 `daily_prices`，并为此先补齐了退市证券 universe。本文是完整的过程记录、
数据口径定义和踩坑备忘。终态：**`daily_prices` 3,164 万行 / 17,699 只证券 /
最早 1972 年，`check_data_integrity` 全绿**。

## 1. 数据源与文件格式

- 文件：`day_aggs_v1_YYYY.tgz` × 24（2003-2026），253 上位于 `/home/wenruifeng/data/day_aggs/`。
- **格式坑**：扩展名 .tgz 但实为**未压缩 tar**，内嵌 `YYYY/MM/YYYY-MM-DD.csv.gz`
  （每交易日一件，真 gzip），且混有 macOS 拷贝产生的 `._` AppleDouble 垃圾成员。
  `import_day_aggs.iter_day_files` 用 `r:*` 自动探测 + 跳过 `._` + gzip 魔数判断解压。
- 列：`ticker,volume,open,close,high,low,window_start,transactions`；**未复权**原始价（QC 验证过）。
- ticker 大小写语义：内含小写字母 = 优先股(p)/权证(w)/认购权(r)/单位(u)等后缀类
  （如 `AAp`），非 CS/ETF，导入前用 `^[A-Z][A-Z0-9.]*$` 过滤——绝不能盲目
  lowercase（`AAp`→`aap` 会撞真 ticker AAP）。点号为股份类别（BRK.A）。
- 2026 年文件的 volume 有小数（碎股），导入时 `int(float(v))`。

## 2. 数据来源边界口径（用户拍板）

`daily_prices` 三个来源靠字段指纹区分（表内无 source 列）：

| 来源 | 指纹 | 覆盖 |
|---|---|---|
| flat files (SIP) | `vwap IS NULL AND trade_count IS NOT NULL` | 2003-09-10 ~ 2023-12-31（主体，2,268 万行） |
| Massive API | `vwap IS NOT NULL` | 2024-01-01 起（+少量深历史，632 万行） |
| yfinance 遗留 | `vwap IS NULL AND trade_count IS NULL` | 2003-09-10 前的深历史 + OTC 填缝（265 万行，见 §7） |

- **Massive 时代按 2023 年底一刀切**：每证券保护边界 =
  `max(该证券最早 vwap 日, 2024-01-01)`（`import_day_aggs --massive-era-start`）。
  边界前的行一律以 flat 为准（upsert 只换 OHLCV/trade_count，既有 vwap 保留）；
  边界后归 Massive 不碰。
- **例外**：从未被 Massive 覆盖的证券（2024 年后退市、退市补录行）无上界，
  flat 数据收到任期终点——否则该段永远无源可补。
- 背景事实：vwap 行 2023 年前仅 50 只零星（最早 1993-08-12），主力 11,071 只的
  最早 vwap 日中位数为 2023-06-29（730 天窗口所致）。

## 3. 退市 universe 补齐（`sync_delisted_universe`）

先补名单再回填（避免二次导入）。vendor 退市名单共 23,234 条（US），
其中明确 CS/ETF 7,913 条、**无类型 6,761 条**、其他类型 8,328 条。

- 适配器 `massive_source.list_delisted_tickers`：`active=false` 名单查询
  **不能带 sort 参数**（vendor 端返回空结果）。
- 匹配优先级：FIGI → CIK+symbol → symbol+退市日邻近（±35 天，仅退市行）。
  命中活跃行 = 改名幽灵（FB→META 后 vendor 把 FB 记 delisted、FIGI 同 META），跳过。
  命中退市行只补 NULL 字段，绝不改写既有值。
- 新退市行走 `db_manager.insert_backfilled_securities` **纯插入**路径（与
  symbol 冲突键的 upsert 隔离——那条路会把死票合并进现任持有者）；`symbol`
  的部分唯一索引只约束活跃行，死票同 symbol 多条合法。附带写 NEW_LISTING
  身份事件（details 含 `massive_delisted_backfill` 标记，可整体回溯）+ CIK 身份行。
- **差一天陷阱（重要）**：时点详情 `?date=退市日` 必然 NOT_FOUND——vendor 的
  PIT 视图当天已摘牌，**必须查退市前一日**（-1，早停牌回退 -7）；PIT 活跃视图
  响应不带 `delisted_utc` 且 `active=true`。第一轮 1.3 万次查询因此静默全灭
  （全部记为 no_data），差点误判为 vendor 物理限制。`get_security_info` 的
  fallback_date 老路径可能有同类隐患，未审。
- 结果：**7,500 只退市 CS/ETF 在库**（6,768 新插）；富化拿到真实 list_date
  2,569 只，vendor 档案无 list_date 4,075 只；无类型 6,761 条**定案放弃**
  （查对日期后 93% 有档案但 type=None——vendor 自己没分类，按"只收 CS/ETF"
  规矩拒收；抽样见过真普通股 OmniVision/Sirenza 混在 ADR/权证中，将来可走
  SEC 渠道鉴定，另立项）。
- 副产品：死票 CIK（覆盖 88.9%）/FIGI（46%）入库，直接提升 13F 历史持仓映射率。

## 4. 任期映射与链式推断（`import_day_aggs`）

- (ticker, date) 按代码任期挂靠 security_id：symbol_history 的 ticker_change
  事件 + 现行 symbol，裁剪到 [list_date, 退市上界]；恰好一个任期才写入，
  0 个记 unmapped、多个记 ambiguous（宁缺毋滥）。
- 退市补录行（list_date NULL + delist_date）走**链式推断**：开口起点段，
  起点 = 同 symbol 中结束不晚于本段终点的最近一段终点（无前任取地板
  2003-01-01），终点被更晚起跑的显式段截断；推断段与显式段的重叠日由
  ambiguous 守卫跳过。
- 富化到手的 2,569 个真实 list_date 直接拦下了 ~35 万根"上市前冒名 bar"
  （dry-run v2→v3 的 rows_to_write 差值，全部正确转入 unmapped_out_of_tenure）。

## 5. 执行记录与统计

三轮 dry-run 对比（全量 4,846 万根 bar 点名）：

| 指标 | 基线（补名单前） | 最终 v3（补名单+修 bug 后） |
|---|---|---|
| mapped | 1,905 万 | 2,835 万 |
| **rows_to_write** | **1,453 万** | **2,381 万** |
| unmapped_no_symbol | 2,194 万 | 1,225 万 |
| unmapped_out_of_tenure | 491 万 | 515 万 |
| ambiguous | 2.3 万 | 16.7 万 |
| skipped_massive_window | 452 万 | 454 万 |
| skipped_suffix_class | 253 万 | 253 万 |
| 无主 ticker 数 | 23,848 | 17,305 |

正式导入（`--purge-remnants`，52 分钟）：写入 23,812,611 行（与 dry-run 一致），
purge 删 yfinance 残留 1,636,213 行（13,417 只），水位校准 6,578 只。
日志：253 `logs/manual_backfill/day_aggs_import.log`（含收尾清理审计行）、
`day_aggs_dryrun_{baseline,final,final_run1}.log`、`delisted_sync*.log`、
未映射清单 `day_aggs_unmapped.tsv`。

## 6. 导入后清理（三道保险，均留审计）

1. **长间隙修剪**：4,286 只"猜任期"死票中 365 只的行情存在 >180 天空窗，
   空窗前 295,035 行删除——样本验证全部该删（nsh：空窗后是 2020 年 SPAC
   NavSight，前段是 2018 年被收购的 NuStar GP；bbx：2017 重组前旧实体；
   upl：Ultra Petroleum 破产重整，旧股 2016 清零与新股不可拼接）。
2. **零价 bar**：sub-penny 精度下溢的全零 OHLC 共 121 行删除（112 flat +
   9 条 Massive 原生的 elox 坏数据）；导入器已加 `skipped_zero_price` 过滤防复发。
3. **完整性体检**：`check_data_integrity` 从 91 项阻塞 → 全部通过。

## 7. 已知边界与遗留

- **无主 1,225 万根 bar 定案不导**：约半数是 vendor 连类型都没有的古早退市票
  （档案 type=None，物理极限）；其余为活着的封闭式基金/ADR（universe 只收
  CS/ETF，设计内排除）及 2004-2015 年退市名单偏薄的缺口。
- **yfinance 遗留 265 万行保留**（用户确认）：162 万行是 2003-09-10 前深历史
  孤本（1,235 只），103 万行是 flat/Massive 都没有的填缝（多为退市转 OTC
  时期；SIP 文件不含 OTC，yfinance 含）。双 NULL 指纹可随时在读取层过滤。
  没有任何证券完全依赖 yfinance 数据。
- ~~20 年复权尚缺公司行动~~ **已解决（2026-07-06）**：corporate_actions 归档
  回填补齐 2003+ 真 vendor 事件，`computed_adjustment_factors` 可信下限已移至
  2003-01-01（见 `corp_actions_archive_2026-07.md`）。
- 死票 CIK 在 `sec_filings`/`insider_transactions` 的覆盖仅 26%（当年只按
  活跃 universe 抓取），SEC 侧死票申报回填可另立项。

## 8. 运维备忘

- Massive key 现共 30 把（2026-07-05 +10），`activation_value.txt` 两端一致，
  聚合限流 150 请求/分钟，且为"整分钟窗突发"模式（瞬时可到 25 req/s 然后停顿）。
- **跨进程限流不协调**：`KeyRateLimiter` 只在进程内共享，两个 Massive 进程
  并发必互相 429——Massive 任务必须串行；SEC、纯 DB、纯本地任务可与之并行。
- 监听进程用 pgrep 匹配命令名时注意自匹配（模式文本落在父 shell cmdline 里
  会死锁等待），改用结果文件标记或把脚本落盘执行。
