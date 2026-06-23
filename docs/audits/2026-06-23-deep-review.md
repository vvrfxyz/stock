## 总体结论

这个仓库已经不是“脚本堆叠型”项目了，更像一个**以 PostgreSQL 为核心的美股日线数据基础设施**：事实层坚持 raw facts，复权、研究面板、SEC、股本、short data、新闻等都开始分层，整体方向是对的。README 明确写了两个关键原则：主存储是 PostgreSQL，身份以 `security_id` 为锚点；`daily_prices` 只存 raw bar，复权和其他派生值不进入事实表。

我对当前 `main` 分支做的是**静态深度审查**，没有在真实 PostgreSQL + API key 环境里跑完整集成测试。结论是：**数据正确性工程已经明显推进，但项目下一阶段的最大风险仍是“证券身份生命周期”和“运行态质量闭环”。** 如果把它定位为个人研究/量化数据底座，现在已经很有价值；如果要演进成长期可靠的生产级数据平台，优先级应从“继续加数据源”转向“身份、审计、失败语义、可观测性、研究有效性边界”。

---

## 当前做得很好的地方

**1. raw-facts-only 的建模纪律是核心优势。**
`DailyPrice` 用 `(security_id, date)` 作为复合主键，只保存 open/high/low/close/volume/VWAP/trade_count/盘前盘后等原始字段；复权因子被拆到 `VendorAdjustmentFactor` 和 `ComputedAdjustmentFactor`，并带 `methodology_version`、`factor_key`、`event_hash` 等字段。这比直接把 adj close 写入价格表更适合回测、重算和追责。

**2. 调度入口已经从“脚本串联”进化到“任务编排”。**
`scheduled_update` 每日现在会跑 universe 同步、价格、short data、最近公司行动、因子增量重建、盘前盘后和数据一致性检查；周末还会跑 shares、grouped daily、FX、risk-free、全量 actions、全量因子、SEC filing/fundamentals/insider/13F 等任务。失败后继续执行后续步骤，但最终非零退出，这个设计适合 cron/systemd。

**3. 复权 as-of 语义已从旧风险点修到正确方向。**
`utils.adjusted_prices` 明确采用 `C(第一个 ex_date > bar_date) / C(第一个 ex_date > as_of)` 的归一化口径，用完整事件链读取、再在读取层消除未来事件污染。测试里也覆盖了“未来 20:1 缩股污染旧 15:1 缩股链”的案例。

**4. 研究层比普通量化原型更严谨。**
回测引擎已经不再简单把 NaN 收益当 0，而是用价格前向填充恢复复牌跳空收益，并在停牌/缺口期间冻结持仓暴露；基本面读取默认 `visible_delay_days=1`，避免用 `filed_date` 当日盘后申报污染当天收盘建仓。

**5. 外部 API 层有较强的工程保护。**
`MassiveSource` 使用线程本地 session、连接池、超时、重试、429 key block、`Retry-After` 处理，并且对 URL 和异常文本中的 `apiKey` 做脱敏。这个细节对长期无人值守跑数很重要。

**6. 测试体系已经覆盖不少关键语义。**
仓库有 pytest 配置和 integration marker；已有测试覆盖调度任务顺序、失败后继续执行、`execute_script` 非零返回转异常、复权 as-of、防未来函数、身份审计等。

---

## P0：最优先修的问题

### 1. 证券身份生命周期仍未闭环

README 的承诺是“所有身份流转以 `security_id` 为锚点，`symbol` 只是当前或历史属性”。当前 schema 已经比旧版好：`symbol` 和 `(current_symbol, exchange)` 都是 active-only 唯一索引，不再是全局永久唯一。

但实现层仍然没有真正把 ticker rename / ticker recycle 做成完整状态机。`upsert_securities_by_symbol` 现在只按**同一个 active symbol** 查询旧行，然后比较 `composite_figi` / `cik`，发现冲突就 quarantine 并跳过。这能阻止一部分 ticker 回收造成的静默合并，但不能处理“同一 FIGI/CIK 换了新 symbol”的改名场景。

更关键的是，`update_massive_events` 只把 ticker change 写进 `security_symbol_history`，没有消费这些事件去更新 `Security.current_symbol`、重连旧 `security_id`、迁移价格或阻止重复 identity。 同时，`sync_massive_universe` 仍然是“拉 active ticker list → 按 symbol upsert → 不在 active list 的旧 symbol 标 inactive”。这会自然产生“旧 id inactive + 新 symbol 新 id active”的身份分裂形态。

你已经写了 `audit_security_identity.py`，它自己也承认当前修法只能阻止“今后”的 ticker 回收静默合并，无法修复或避免所有历史分裂/回收/映射歧义。脚本检查同一 FIGI/CIK 多个 `security_id`、同一 symbol/current_symbol 多行但身份不一致、CUSIP/CIK/FIGI 歧义、symbol history 可重连等问题，并在发现 advisory 问题时非零退出。

**建议修法：**
下一阶段不要继续把 `upsert_securities_by_symbol` 加条件补丁，而是做一个显式的 `SecurityIdentityResolver`：

* incoming reference row 先按 `composite_figi` / `share_class_figi` / SEC CIK / exchange 识别身份，symbol 只作为 fallback。
* 如果同 FIGI/CIK 但 symbol 变了：更新同一 `security_id` 的 `current_symbol`，写 symbol history，不插新 security。
* 如果同 symbol 但 FIGI/CIK 不同：确认为 ticker recycle，旧 row 保持 inactive，新 row 插入新 `security_id`，并产出审计事件。
* 所有 price/action/SEC/news/short 写入前统一走 resolver，而不是各脚本自己用 symbol 查。
* 把 `audit_security_identity` 加入月度或周度调度，至少在提高 universe 同步频率前必须通过。

这是项目最值得投入的 P0，因为身份错了，复权、基本面、13F、insider、回测全部都会被污染。

---

### 2. `rebuild_massive_dataset` 不重建内部复权因子

`scheduled_update` 已经做对了：每日最近 actions 后跑 `update_adjustment_factors --changed-since 3`，周日 full actions 后跑 `update_adjustment_factors --all`。

但 `rebuild_massive_dataset` 的路径是：sync universe、details、actions、prices、grouped daily、shares、可选 open/close；没有在 actions 和 prices 后重建 `computed_adjustment_factors`。 这意味着用户按 README 推荐的“Massive 免费层能力范围内全量重建”跑完后，raw facts 可能是新的，但复权读取层依赖的 cache 不一定存在或同步。README 也把 `rebuild_massive_dataset` 列为常用命令。

**建议修法：**
在 `run_rebuild_massive_dataset` 末尾强制追加：

```bash
python main.py update_adjustment_factors --market US --all
python main.py check_data_integrity --window-days 730
```

如果想更严格，再加 `audit_security_identity`。这条修复改动小、收益大，应排在第一批 PR。

---

## P1：高优先级问题

### 3. 因子 vendor mismatch 只告警，不会让调度失败

`update_adjustment_factors` 会对 computed 因子和 vendor 因子做比较，并记录 mismatch 样例。但主函数最后无论有无 mismatch 都 `return 0`，除非脚本级异常。

这适合探索阶段，不适合生产阶段。因为复权因子一旦错，研究收益、因子排序、回测夏普都会被系统性污染。既然你已经建立了 comparison 和 status 机制，就应该有一个可配置的失败策略。

**建议修法：**

* 默认仍允许 mismatch warning，避免供应商口径噪声中断日更。
* 增加 `--fail-on-vendor-mismatch` 或 `--max-mismatch-rate`。
* 调度里日更用 warning，周日 full rebuild 用严格模式。
* 把 mismatch 计数写入 `pipeline_task_runs` 或至少结构化 JSON log，方便趋势监控。

---

### 4. 子脚本失败语义还没有完全统一

`main.execute_script` 的合约是：子脚本返回非零 int 或抛 `SystemExit(nonzero)`，调度层才视为失败。这个合约本身有测试覆盖。

问题是子脚本对“部分失败”的定义还不一致。例如 `update_sec_fundamentals` 只有在所有处理对象都失败时才返回 1；部分 CIK 拉取失败仍返回 0。  对数据平台来说，部分失败不一定要阻断全局调度，但至少应该有统一的失败预算和可观测状态。

**建议修法：**

* 所有脚本统一返回语义：`0 = 完全成功或无工作`，`1 = 有错误但可恢复/需告警`，`2 = 阻塞性数据风险`。
* 支持 `--allow-partial-failure` 或 `--max-failures N`，调度显式决定是否容忍。
* 在数据库建 `pipeline_runs` / `pipeline_task_runs`，记录 started_at、ended_at、status、processed、written、failed、warning_count、error_sample。
* `scheduled_update` 最后不是只输出 failed step 名称，而是给出每个任务的行数、错误数、最新水位线。

---

### 5. CLI / README 的调度说明已经落后于代码

README 中 `scheduled_update` 的说明只列了每日 prices/short/open-close、周六 shares、周日 actions/SEC、月度 events/details。 但当前代码每日还会跑 `sync_massive_universe`、`update_massive_actions_recent`、`update_adjustment_factors`、`check_data_integrity`。

这类文档漂移会导致你以后排障时误判调度行为，尤其是 identity 问题还未闭环时，`sync_massive_universe` 是否每日执行非常关键。

**建议修法：**
让 README 的调度说明由测试锁定，或在 `main.py` 中提供 `python main.py scheduled_update --dry-run-plan` 输出真实 step list；README 只引用这个命令，不手写完整列表。

---

### 6. 研究层还缺“终止/退市收益”的保守处理

回测层已经解决了停牌期间收益被吞掉的问题，并报告 `terminal_missing_position_days`。 但如果某个持仓之后价格永久缺失，当前更像是“暴露风险指标”，不是 P&L 层的保守估计。对小盘、退市、暴跌反转策略，这会继续造成收益上偏。

**建议修法：**

* 对 terminal missing 的持仓支持策略参数：`terminal_return_policy = ignore | zero | delist_table | conservative_minus_100`。
* 如果没有 CRSP delisting return，就至少在研究报告中把 terminal missing 作为必报指标，并提供敏感性测试。
* 建一个 `security_lifecycle_events` 表，记录 delist_date、last_trade_date、delist_reason、terminal_price_source。

---

### 7. SEC 基本面 PIT 还可以更精确

当前研究读取层通过 `visible_delay_days=1` 默认后移一天，避免 `filed_date` 当天盘后披露被当天收盘建仓提前看到，这是正确的保守修复。 但 schema 里 `sec_filings` 已经有 `accepted_at`，而 `sec_fundamental_facts` 只有 `filed_date`，没有直接携带 accepted timestamp。

**建议修法：**

* 在 `sec_fundamental_facts` 增加 `accepted_at`，或者读取层按 `accession_number` join `sec_filings.accepted_at`。
* 对日频收盘策略：`accepted_at <= market_close_at` 才可当天可见，否则下一交易日可见。
* 对不同交易所/半日市使用 `trading_calendars.close_at`，不要写死 16:00。

---

## P2：中低优先级但值得修

### 8. `sync_massive_universe` 没有关闭 MassiveSource

脚本创建了 `MassiveSource`，但 `finally` 里只关闭了 `db_manager`。  对单次 cron 进程影响不大，但在长进程或测试里会造成 session 资源泄漏。直接把 `source = None` 放到 try 前，并在 finally `source.close()` 即可。

### 9. open/close 回填只在两个字段都为空时才候选

`update_open_close_summary` 在非 overwrite 模式下筛选 `pre_market IS NULL AND after_hours IS NULL`。 如果某行只有 `pre_market` 有值、`after_hours` 缺失，就不会被补。建议改成 `OR`，并在 upsert 时只覆盖缺失字段，或保留 `--overwrite` 的强覆盖语义。写入 payload 本身已经分别支持两个字段。

### 10. `upsert_daily_prices` 对混合 key-set 批次不够健壮

`upsert_daily_prices` 直接对 `price_data` 做 `pg_insert(DailyPrice).values(price_data)`，没有像其他 helper 那样按 key set 分组。 当前价格脚本和 open/close 脚本通常传入同构 rows，所以大概率没问题；但一旦未来混合 raw OHLC 行和只含盘前盘后字段的行，同一批可能触发 SQLAlchemy compile 问题或更新语义不清。你已经有 `_group_rows_by_key_set` 专门解决这个问题。

### 11. `load_price_long` 空结果会 `pd.concat` 失败

`research.data.load_price_long` 对 `pd.read_sql_query(..., chunksize=...)` 结果直接 `pd.concat(chunks, ignore_index=True)`。 如果研究窗口没有任何价格行，应该返回空 DataFrame，而不是抛 `No objects to concatenate`。这是小 bug，但会影响 notebook/批量研究体验。

### 12. docker compose 默认监听地址建议更保守

`docker-compose.yml` 默认 `${POSTGRES_BIND:-0.0.0.0}`，而 `.env.example` 建议本机部署改成 `127.0.0.1`。  安全默认值建议反过来：compose 默认 `127.0.0.1`，需要远程访问时用户显式改成 `0.0.0.0`。

---

## 发展路线图

### 第一阶段：1–2 周，先把“不会静默错”做好

目标：解决 P0/P1 中能快速落地的正确性问题。

应做的 PR：

1. `rebuild_massive_dataset` 末尾强制跑 `update_adjustment_factors --all`，再跑 `check_data_integrity`。
2. `update_adjustment_factors` 增加 `--fail-on-vendor-mismatch` / `--max-mismatch-rate`。
3. `sync_massive_universe` 关闭 `MassiveSource`。
4. `update_open_close_summary` 用 `pre_market IS NULL OR after_hours IS NULL` 找候选，并避免覆盖已有非空值。
5. `load_price_long` 空结果返回空 DataFrame。
6. README / CLI help 同步当前真实 `scheduled_update` step list。

验收标准：
跑一次 `python main.py rebuild_massive_dataset --market US` 后，`computed_adjustment_factors` 对所有有 corporate actions 的证券完整；`scheduled_update` 的 README 描述与 `build_scheduled_update_steps()` 测试一致；任何关键 step 失败都能在最终退出码体现。

---

### 第二阶段：2–6 周，建立证券身份状态机

目标：把 `security_id` 变成真正的持久身份，而不是“多数时候稳定”。

应做的模块：

1. `security_identity_resolver.py`

   * 输入：vendor ticker row、FIGI、CIK、exchange、as-of date。
   * 输出：`security_id`、resolution_type、confidence、action。
   * 处理 rename、recycle、ambiguous、new listing、missing identifiers。

2. `security_identity_events` 表

   * 记录 rename、recycle、manual_merge、manual_split、identifier_conflict。
   * 每次自动处理或 quarantine 都写事件。

3. 存量修复工具

   * 基于 `audit_security_identity` 输出生成修复 plan。
   * 支持 dry-run：合并同 FIGI/CIK 的 split identity，或拆分 recycle identity。
   * 不直接自动破坏历史数据，先生成 SQL plan 和人工确认清单。

4. 读取层改造

   * `resolve_security_id()` 不应只按 `Security.symbol` 查；应支持 current symbol、历史 symbol、as-of date、exchange。
   * 研究层 symbol map 应区分 `security_id -> current_symbol` 和 `security_id -> symbol_history`。

验收标准：
FB→META 类 ticker rename 不再产生两个 `security_id`；ticker recycle 不再把两家公司拼接；`audit_security_identity` 在干净库上返回 0；symbol history 能被真实消费，而不只是落库。

---

### 第三阶段：1–3 个月，建立数据质量和运行态闭环

目标：从“脚本成功跑完”升级为“知道每个数据域是否可信”。

应做的基础设施：

1. `pipeline_runs` / `pipeline_task_runs`

   * 每个脚本写 processed/written/skipped/failed/warnings/watermark。
   * 统一返回码和失败预算。

2. 数据质量分层

   * P0 blocking：identity collision、price latest date mismatch、OHLC invalid、split mismatch。
   * P1 warning：vendor factor mismatch、unexplained jumps、SEC partial failures、missing open/close fields。
   * P2 advisory：stale details、stale shares、stale fundamentals。

3. 周报/日报命令

   * `python main.py health_report --market US`
   * 输出每个数据域的 freshness、coverage、error trend、最近失败样例。

当前已经有 `check_data_integrity` 检查 latest date、symbol lowercase、OHLC、日历缺口、拆股跳变、无事件大跳变，并在 blocking issues > 0 时非零退出，这是很好的起点。

---

### 第四阶段：3–6 个月，研究平台化

目标：让这个仓库从“数据管道”变成“可复现实验平台”。

应做方向：

1. Universe snapshot

   * 每个交易日保存可交易 universe。
   * 区分 active today、listed by date、has price by date、meets liquidity。
   * 避免研究时用今天的 universe 回看历史。

2. Delisting / terminal return model

   * 退市、长期停牌、永久缺价都有可配置收益假设。
   * 研究报告默认输出 terminal missing 敏感性。

3. PIT fundamentals 精确化

   * 用 `accepted_at` 和交易所 close time 控制可见性。
   * 对 10-K/A、10-Q/A、restatement 建事件时间线。

4. 因子注册与评估

   * 因子函数有 metadata：lookback、lag、required fields、PIT guarantee。
   * 自动跑 IC、turnover、coverage、decay、long-short、capacity proxy。
   * 每次因子评估绑定数据版本、methodology_version 和 universe 版本。

---

### 第五阶段：6 个月以后，扩展数据广度和性能

只有当前面 identity / quality / observability 稳了，再扩数据源和性能层。

优先顺序：

1. 行业分类：SIC → FF12 / GICS-like mapping。
2. earnings calendar / event study。
3. short interest / short volume 因子。
4. institutional holdings / insider transaction 因子。
5. Parquet export 或 read-only analytics schema。
6. 分钟线需求真实出现后，再考虑 ClickHouse/Timescale/partitioned PostgreSQL。

README 里已经说明 ClickHouse 矩阵读取层在 2026-06 移除，等待分钟级数据需求出现后再重建；这个取舍是对的。现在过早引入列式/多存储，会把复杂度加在还没闭环的 identity 和质量问题上。

---

## 建议的 PR 顺序

1. **PR-1：rebuild/factor 一致性修复**
   给 `rebuild_massive_dataset` 加 `update_adjustment_factors --all`；给 vendor mismatch 增加严格模式。

2. **PR-2：身份 resolver 设计骨架**
   先不大迁移，先做只读 resolver + dry-run report，输出每个 incoming ticker 会命中哪个 `security_id`。

3. **PR-3：sync universe 改用 resolver**
   不再直接按 symbol upsert；rename 更新 `current_symbol`，recycle 插新 id，ambiguous quarantine。

4. **PR-4：运行态 task_runs 表**
   所有脚本统一记录 processed/written/failed/warnings，并标准化退出码。

5. **PR-5：研究有效性补丁**
   terminal return policy、empty DataFrame bug、accepted_at join。

6. **PR-6：文档和 CI/测试矩阵**
   README 调度说明、identity lifecycle 文档、数据质量 runbook、集成测试执行说明。

---

## 一句话判断

这个项目的底层方向是正确的：raw facts、`security_id`、复权 cache、PIT 研究层、调度和测试都已经成型。下一步最不该做的是“继续堆更多数据源”；最该做的是把 **证券身份解析、复权/调度一致性、失败语义、数据质量报告** 做成闭环。只要身份生命周期打通，这个仓库就可以从个人量化数据管道升级成一个长期可维护的美股研究数据平台。
