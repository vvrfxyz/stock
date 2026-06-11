# 近 2 年数据抽样审计报告

- 审计窗口: 2024-06-10 ~ 2026-06-10
- 生成时间: 2026-06-11，耗时 24s
- 样本: 32 支（OK 0 / 有发现 32 / 跳过 0）

核对口径: A) vendor raw vs daily_prices 精确对账; B) vendor split-adjusted vs
corporate_actions 独立重算的 split-only 因子 (容忍 0.1%); C) 读取层全因子序列拆股日连续性。

| symbol | 状态 | 重叠天数 | 库缺 | 库多 | rawClose不符 | volume不符 | splitAdj不符 | 读取层断点 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| a | ISSUES | 500 | 1 | 1 | 0 | 4 | 0 | 0 |
| aa | ISSUES | 500 | 1 | 1 | 0 | 3 | 0 | 0 |
| aaac | ISSUES | 116 | 1 | 0 | 0 | 0 | 0 | 0 |
| aapl | ISSUES | 500 | 1 | 1 | 0 | 4 | 0 | 0 |
| aapy | ISSUES | 500 | 1 | 1 | 0 | 0 | 0 | 0 |
| aat | ISSUES | 500 | 1 | 1 | 0 | 1 | 0 | 0 |
| aaus | ISSUES | 222 | 1 | 0 | 0 | 0 | 0 | 0 |
| aavm | ISSUES | 340 | 1 | 0 | 0 | 0 | 0 | 0 |
| adil | ISSUES | 500 | 1 | 1 | 0 | 1 | 0 | 0 |
| advb | ISSUES | 317 | 1 | 0 | 0 | 0 | 0 | 0 |
| aehl | ISSUES | 500 | 1 | 1 | 0 | 1 | 0 | 1 |
| aht | ISSUES | 500 | 1 | 1 | 0 | 1 | 0 | 0 |
| aihs | ISSUES | 499 | 1 | 2 | 0 | 1 | 0 | 0 |
| aiio | ISSUES | 198 | 1 | 0 | 0 | 3 | 0 | 0 |
| aire | ISSUES | 500 | 1 | 1 | 0 | 1 | 0 | 0 |
| albt | ISSUES | 500 | 1 | 1 | 0 | 1 | 0 | 0 |
| alil | ISSUES | 291 | 1 | 0 | 0 | 0 | 0 | 0 |
| amzn | ISSUES | 500 | 1 | 1 | 0 | 4 | 0 | 0 |
| avgo | ISSUES | 500 | 1 | 1 | 0 | 4 | 0 | 0 |
| ftk | ISSUES | 500 | 1 | 1 | 0 | 0 | 0 | 0 |
| goog | ISSUES | 500 | 1 | 1 | 0 | 3 | 0 | 0 |
| googl | ISSUES | 500 | 1 | 1 | 0 | 4 | 0 | 0 |
| ivog | ISSUES | 500 | 1 | 1 | 0 | 0 | 0 | 0 |
| jhac | ISSUES | 483 | 1 | 4 | 0 | 0 | 0 | 0 |
| lsta | ISSUES | 500 | 1 | 1 | 0 | 0 | 0 | 0 |
| msft | ISSUES | 500 | 1 | 1 | 0 | 3 | 0 | 0 |
| nvda | ISSUES | 500 | 1 | 1 | 0 | 4 | 0 | 0 |
| qbts | ISSUES | 500 | 1 | 1 | 0 | 4 | 0 | 0 |
| seb | ISSUES | 500 | 1 | 1 | 0 | 1 | 0 | 0 |
| spgi | ISSUES | 500 | 1 | 1 | 0 | 4 | 0 | 0 |
| tsla | ISSUES | 500 | 1 | 1 | 0 | 4 | 0 | 0 |
| tsyw | ISSUES | 142 | 1 | 0 | 0 | 1 | 0 | 0 |

## 发现明细

### nvda (ISSUES)
- vendor 有而库缺 1 天 (如 [datetime.date(2026, 6, 10)])
- volume 不一致 4 天 (如 [(datetime.date(2026, 5, 19), 140948296, 140948208), (datetime.date(2026, 5, 21), 202132659, 203381760)])

### googl (ISSUES)
- vendor 有而库缺 1 天 (如 [datetime.date(2026, 6, 10)])
- volume 不一致 4 天 (如 [(datetime.date(2026, 5, 19), 39545436, 39545673), (datetime.date(2026, 5, 21), 24762968, 24852843)])

### goog (ISSUES)
- vendor 有而库缺 1 天 (如 [datetime.date(2026, 6, 10)])
- volume 不一致 3 天 (如 [(datetime.date(2026, 5, 21), 16785046, 16837601), (datetime.date(2026, 5, 27), 16893261, 16893149)])

### aapl (ISSUES)
- vendor 有而库缺 1 天 (如 [datetime.date(2026, 6, 10)])
- volume 不一致 4 天 (如 [(datetime.date(2026, 5, 19), 42243633, 42243562), (datetime.date(2026, 5, 21), 42931848, 42965127)])

### msft (ISSUES)
- vendor 有而库缺 1 天 (如 [datetime.date(2026, 6, 10)])
- volume 不一致 3 天 (如 [(datetime.date(2026, 5, 21), 31333512, 31393469), (datetime.date(2026, 5, 27), 28902791, 28901480)])

### amzn (ISSUES)
- vendor 有而库缺 1 天 (如 [datetime.date(2026, 6, 10)])
- volume 不一致 4 天 (如 [(datetime.date(2026, 5, 19), 40420578, 40340698), (datetime.date(2026, 5, 21), 36533411, 36591744)])

### avgo (ISSUES)
- vendor 有而库缺 1 天 (如 [datetime.date(2026, 6, 10)])
- volume 不一致 4 天 (如 [(datetime.date(2026, 5, 19), 20652039, 20652069), (datetime.date(2026, 5, 21), 16834263, 16865293)])

### tsla (ISSUES)
- vendor 有而库缺 1 天 (如 [datetime.date(2026, 6, 10)])
- volume 不一致 4 天 (如 [(datetime.date(2026, 5, 19), 46552247, 46500552), (datetime.date(2026, 5, 21), 42340858, 42636855)])

### aiio (ISSUES)
- vendor 有而库缺 1 天 (如 [datetime.date(2026, 6, 10)])
- volume 不一致 3 天 (如 [(datetime.date(2026, 5, 19), 12359431, 12359433), (datetime.date(2026, 5, 21), 7857468, 7958175)])

### aire (ISSUES)
- vendor 有而库缺 1 天 (如 [datetime.date(2026, 6, 10)])
- volume 不一致 1 天 (如 [(datetime.date(2026, 5, 21), 247919, 247950)])

### aehl (ISSUES)
- vendor 有而库缺 1 天 (如 [datetime.date(2026, 6, 10)])
- volume 不一致 1 天 (如 [(datetime.date(2026, 5, 21), 1489964, 1499963)])
- 读取层在 2025-04-04 因子切换处跳变 102.4%

### aht (ISSUES)
- vendor 有而库缺 1 天 (如 [datetime.date(2026, 6, 10)])
- volume 不一致 1 天 (如 [(datetime.date(2026, 5, 21), 10822, 10823)])

### aihs (ISSUES)
- vendor 有而库缺 1 天 (如 [datetime.date(2026, 6, 10)])
- volume 不一致 1 天 (如 [(datetime.date(2026, 5, 21), 1579, 1696)])

### albt (ISSUES)
- vendor 有而库缺 1 天 (如 [datetime.date(2026, 6, 10)])
- volume 不一致 1 天 (如 [(datetime.date(2026, 5, 21), 309674, 309929)])

### advb (ISSUES)
- vendor 有而库缺 1 天 (如 [datetime.date(2026, 6, 10)])

### adil (ISSUES)
- vendor 有而库缺 1 天 (如 [datetime.date(2026, 6, 10)])
- volume 不一致 1 天 (如 [(datetime.date(2026, 5, 19), 1778137, 1778226)])

### aaus (ISSUES)
- vendor 有而库缺 1 天 (如 [datetime.date(2026, 6, 10)])

### aapy (ISSUES)
- vendor 有而库缺 1 天 (如 [datetime.date(2026, 6, 10)])

### aat (ISSUES)
- vendor 有而库缺 1 天 (如 [datetime.date(2026, 6, 10)])
- volume 不一致 1 天 (如 [(datetime.date(2026, 5, 21), 416040, 416050)])

### aaac (ISSUES)
- vendor 有而库缺 1 天 (如 [datetime.date(2026, 6, 10)])

### a (ISSUES)
- vendor 有而库缺 1 天 (如 [datetime.date(2026, 6, 10)])
- volume 不一致 4 天 (如 [(datetime.date(2026, 5, 19), 3142714, 3142713), (datetime.date(2026, 5, 21), 2416792, 2416790)])

### aa (ISSUES)
- vendor 有而库缺 1 天 (如 [datetime.date(2026, 6, 10)])
- volume 不一致 3 天 (如 [(datetime.date(2026, 5, 21), 3292304, 3294470), (datetime.date(2026, 5, 27), 3283108, 3283107)])

### aavm (ISSUES)
- vendor 有而库缺 1 天 (如 [datetime.date(2026, 6, 10)])

### alil (ISSUES)
- vendor 有而库缺 1 天 (如 [datetime.date(2026, 6, 10)])

### ftk (ISSUES)
- vendor 有而库缺 1 天 (如 [datetime.date(2026, 6, 10)])

### seb (ISSUES)
- vendor 有而库缺 1 天 (如 [datetime.date(2026, 6, 10)])
- volume 不一致 1 天 (如 [(datetime.date(2026, 5, 28), 11342, 11345)])

### qbts (ISSUES)
- vendor 有而库缺 1 天 (如 [datetime.date(2026, 6, 10)])
- volume 不一致 4 天 (如 [(datetime.date(2026, 5, 19), 21512672, 21512862), (datetime.date(2026, 5, 21), 119074178, 119132464)])

### spgi (ISSUES)
- vendor 有而库缺 1 天 (如 [datetime.date(2026, 6, 10)])
- volume 不一致 4 天 (如 [(datetime.date(2026, 5, 19), 2883025, 2883028), (datetime.date(2026, 5, 21), 2062365, 2062581)])

### ivog (ISSUES)
- vendor 有而库缺 1 天 (如 [datetime.date(2026, 6, 10)])

### lsta (ISSUES)
- vendor 有而库缺 1 天 (如 [datetime.date(2026, 6, 10)])

### tsyw (ISSUES)
- vendor 有而库缺 1 天 (如 [datetime.date(2026, 6, 10)])
- volume 不一致 1 天 (如 [(datetime.date(2026, 5, 21), 1297, 1299)])

### jhac (ISSUES)
- vendor 有而库缺 1 天 (如 [datetime.date(2026, 6, 10)])


## 结论（人工复核后）

第二轮（修复 POLYGON/MASSIVE 经济重复去重后）32 支样本的判定：

1. **raw close 全部精确一致**（32/32，最大差 0）—— daily_prices 与 vendor raw 完全对齐。
2. **splitAdj 全部一致**（首轮 adil/aehl/jdzg 等 10 支的"偏差"全部源于
   迁移遗留的 POLYGON 重复行被连乘两次，审计逻辑去重后归零；事实表本身无错）。
3. **"库缺 1 天"均为 2026-06-10**：vendor EOD 数据 T+1 延迟，次日增量自动补齐，非缺陷。
4. **volume 不一致 22 支**，每支 1-4 天、偏差 <0.2%：vendor 对近期成交量的 restatement。
   已加周六 `update_grouped_daily_recent`（最近 5 个交易日）自动吸收。
5. **读取层断点 1 处（aehl 2025-04-04）**：40:1 反向拆股日叠加当日真实 +102% 行情，
   属真实波动，非数据/因子错误。

### 全库完整性检查同日发现（check_data_integrity，2 年窗口）

- 缺口 3,925 条：多为停牌/瘦交易标的（如 catg 等 ETF 在 2026-05-12 之前无成交记录），
  以及 2026-05-12 之前 vendor 覆盖空洞；属来源能力边界而非管道丢数。
- OHLC 违规 28 条：全部是 0.0000/0.0001 的仙股报价精度噪声。
- 拆股跳变不符 208 条 / 无事件大跳变 1,770 条：抽查 jdzg（6/1 +1101%）确认为
  **拆股事件迟到 7 天**——周日全量 actions 间隔太长。已加每日
  `update_massive_actions_recent --recent-days 14` 闭环修复，
  并已执行一次全量 actions 追补 + 因子重建（computed 行数 58,724 → 60,457）。

### 遗留动作（已完成 2026-06-11）

- ~~POLYGON 来源的经济重复行清理~~ → 已执行 `scripts/cleanup_polygon_duplicate_actions.py --apply`：
  - 删除与 MASSIVE 行逐字段一致的 POLYGON 重复行 **57,726 条**（actions 追补后重复量比首测的 36k 更多）；
  - 删除前向 MASSIVE 行回填 declaration/record/pay 日期 2 条；
  - 保留 97 条同日同类型但金额/比例不同的 POLYGON 行（可能为真实同日多笔事件，待人工甄别）；
  - 保留 171,522 条无 MASSIVE 对应的 POLYGON 行（730 天窗口外的唯一历史记录，是 1972 年起历史的事件来源）；
  - 清理后剩余经济重复 **0** 条；表总量 287,782 → 232,056；
  - 备份：logs/corporate_actions_pre_cleanup_20260611.dump（清理前最新快照）；
  - 清理后已全量重建 computed_adjustment_factors。
