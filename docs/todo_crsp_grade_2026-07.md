# TODO：通往公司级 CRSP 的五个工程（2026-07-06 立项）

> 本文档是给**并行研究窗口**的完整交接件：what / why / how / 验收 / 坑。
> 背景阅读顺序：`data_infra_assessment_2026-07.md`（评估与差距地图）→
> `corp_actions_archive_2026-07.md` + `minute_vw_backfill_2026-07.md`（本周刚落地的两大件）
> → 本文。术语：PIT = point-in-time（回测时只能用当时已公开的信息）。

## 并行开发须知（与主窗口的协作约定）

- 主窗口剩余工作只有分钟线接缝补拉（写 ClickHouse `minute_bars`，无代码改动）与
  运行监控——**与本清单无冲突**。
- 共享文件协调：`data_models/models.py`、`main.py`、`CLAUDE.md`、`db_manager/` 本清单
  会新增内容，主窗口已停止编辑它们；**每次开工前 `git pull`，部署 253 前先 pull 再推**
  （部署流程见 CLAUDE.md Deployment 节：GitHub push + `_sync_main` SSH push 双轨）。
- 避开 02:00 UTC（10:00 北京时间）±1h 的重 DB 写入——每日调度在跑。
- 253 环境铁律：`wenruifeng` 无 docker 组权限，一切 ClickHouse 交互走 **HTTP 8123**
  （参考 `scripts/import_minute_bars_clickhouse.py` 的 `ch()`）；PG 交互经
  `DatabaseManager`；跑脚本用 `sudo -u wenruifeng`。
- 新表须走 alembic migration + `tests/test_db_manager_pg.py` 风格的集成测试；
  脚本遵循 `main(argv)->int` + `utils/script_logging` 约定（CLAUDE.md Code Conventions）。
- 归属/清洗纪律沿用全仓口径：**宁缺毋滥**——归属不确定的行进隔离报告，绝不猜。

---

## 任务 1：`delisting_events` 表——退市结局（头号工程，CRSP 的招牌拼图）

### What

新表记录每只退市证券的**结局**：为什么退市、持有人最后拿到了什么。

```
delisting_events
  security_id        BIGINT FK securities.id, 主键之一
  delist_date        DATE NOT NULL
  reason_code        VARCHAR(20)   -- MERGER / ACQUISITION_CASH / ACQUISITION_STOCK /
                                   -- BANKRUPTCY / LIQUIDATION / EXCHANGE_DROP（不达标）/
                                   -- VOLUNTARY / FUND_CLOSURE（ETF 清盘）/ UNKNOWN
  reason_confidence  VARCHAR(10)   -- HIGH / MEDIUM / LOW（分类来源决定）
  acquirer_name      VARCHAR(255)  -- 并购类：收购方
  consideration_cash NUMERIC(20,6) -- 每股现金对价
  consideration_stock_ratio NUMERIC(20,10) -- 换股比（对价含股票时）
  final_price        NUMERIC(19,6) -- 退市前最后可靠成交价（含 OTC 尾巴）
  final_price_date   DATE
  delisting_return   NUMERIC(12,8) -- 实测退市收益 =（实际所得-final_price）/final_price；
                                   -- 无实据时置 NULL，绝不填经验值（经验值是读取层的事）
  source             VARCHAR(30)   -- FORM25 / 8K / TICKER_EVENT / PRICE_INFERRED / MANUAL
  evidence           TEXT          -- accession number / 事件 id / 推断依据
  唯一键 (security_id, delist_date)
```

### Why

- CRSP 与我们之间**最大的结构性差距**。没有它，长仓回测里退市持仓的处理只能靠
  全局假设（`run_baselines --terminal-return`），而破产归零（-100%）和溢价并购
  （+30%）的差别足以翻转一个 20 年策略的结论。
- 学术可复现性：著名的"退市偏差"（Shumway 1997）就是因为早年 CRSP 缺退市收益，
  小盘反转/规模因子全被高估。我们现在正处在同样的坑里。
- 原料 90% 已在库内（见 How），免费。

### How

分四步，每步独立可验收：

1. **补 Form 25 原料**（半天）：`update_sec_filings` 目前抓不到 Form 25/25-NSE
   （实测 sec_filings 中为 0 行；8-K 有 50 万份）。检查
   `scripts/update_sec_filings.py` 的 form type 过滤（或 EDGAR full-index 解析范围），
   放开 `25`/`25-NSE`，对退市证券的 CIK 回拉。Form 25 = 交易所/发行人提交的正式
   退市通知，是 reason 分类的最强证据。注意：退市股 CIK 覆盖率仅 ~26%（已知身份债，
   任务 4 会改善），先用已有 CIK 的做。
2. **final_price 提取**（1 天）：对全部 `is_active=false AND delist_date IS NOT NULL`
   的证券，取 `daily_prices` 中 `delist_date` 前后最后一根可靠 bar（**含 yfinance
   双 NULL 的 OTC 尾巴——这批"填缝"数据在此处是唯一来源**；2026-07-06 实测 6,491 只
   在 ±5 天内有终价，覆盖 ~90%）。零价/停牌远早于退市日的（496 只截尾 + 338 只
   零价，评估已计数）final_price 置 NULL 并记 evidence。
3. **reason 分类器**（3-5 天，核心工作量）：按证据强度分层——
   - HIGH：Form 25 理由字段；8-K 并购完成公告（item 2.01）在退市日 ±30 天内且
     公司名匹配 → MERGER/ACQUISITION，同时从 8-K/DEFM14A 抽对价（现金每股金额
     正则相当可靠，换股比难一些，抽不出就只填 acquirer）；
   - MEDIUM：`security_identity_events` 与 ticker_events 的 MERGE 记录；ETF 发行人
     清盘公告模式（ETF 全部归 FUND_CLOSURE，价格无跳变即平价清算）；
   - LOW（PRICE_INFERRED）：终价形态推断——终价 < $1 且持续阴跌 → 疑似
     EXCHANGE_DROP/BANKRUPTCY；终价稳定在整数附近且成交萎缩 → 疑似现金并购
     （对价 ≈ 终价）。LOW 层只定性不定量，delisting_return 置 NULL。
   - 分类器写成 `scripts/build_delisting_events.py`，幂等重跑，产出分层统计 +
     UNKNOWN 清单（供人工与未来数据源迭代）。
4. **delisting_return 计算 + 读取层接线**（2 天）：
   - 现金并购：`(consideration_cash - final_price)/final_price`（通常≈0，因为终价
     已收敛到对价——这正是校验分类正确性的自检：现金并购类的 return 应聚在 0 附近）；
   - 破产/清盘无残值证据：return = -1 只在有法院文件/Form 25 佐证时写，否则 NULL；
   - `research/backtest.py` 的 terminal_return 机制升级：优先查 `delisting_events.
     delisting_return`，查无再落到全局 CLI 假设（保持向后兼容与显式性）。

### 验收

- 6,059 只退市 CS + 1,446 只退市 ETF 中：reason 非 UNKNOWN 覆盖 ≥70%，
  delisting_return 非 NULL 覆盖 ≥50%；现金并购类 return 分布以 0 为众数（自检）；
- 集成测试锁定表的 upsert 语义；`run_baselines --start 2010-01-01
  --terminal-return none` 与升级后（逐只实测）的 20 年动量结果对比写进工程记录；
- 新增 health_report 探针：延迟 90 天以上仍 UNKNOWN 的新退市数量。

### 坑

- 并购里的**要约收购部分成交**、**破产但老股保留 OTC 交易多年**（LEHMQ 类）——
  delist_date 不等于价值归零日，evidence 里写清口径：我们记"交易所退市时点结局"。
- 8-K 公司名匹配须走 CIK 而非名字模糊匹配（改名/母子公司陷阱）。
- ETF 清盘的 final NAV 分配常在退市后数周，final_price 已含预期，return≈0 是对的。

---

## 任务 2：公司实体表（PERMCO 等价物）

### What

```
companies
  id            BIGSERIAL PK          -- 我们的 PERMCO，永不回收
  cik           VARCHAR(20) UNIQUE    -- 主锚（可 NULL：无 SEC 申报的 ETF 发行主体不建）
  name          VARCHAR(255)
  created_at / updated_at
securities 增列 company_id BIGINT NULL FK companies.id
company_events（可选，第二期）：并购世系边表 (predecessor_company_id, successor_company_id,
  event_date, event_type MERGER/SPINOFF/RENAME, evidence)
```

### Why

- 双类股（GOOG/GOOGL、BRK.A/B）现在是两只无关证券：**合并市值算不出来**、
  基本面 join 按 CIK 撒到多 ticker 时会重复计数（13F 审计发现过 3,056 个多 ticker
  CIK，最多 86 个）。
- 并购世系（DWDP→DD/DOW/CTVA 这类）是任务 1 reason 分类和任务 4 身份史的共同骨架。
- CRSP 的 PERMNO/PERMCO 双层正是为此存在。

### How

1. migration 建表 + 加列（半天）；
2. 初始归组（1 天）：`INSERT INTO companies (cik, name) SELECT DISTINCT ON (cik) ...`
   来自 securities（cik 非空者）；`UPDATE securities SET company_id = ...` 按 cik join。
   cik 为 NULL 的证券留空（多为 ETF/老退市——任务 4 补 CIK 后自然入组）；
3. 冲突消解（1-2 天）：同 CIK 下 active 证券的 name 差异极大者（历史 CIK 回收极罕见
   但存在）出报告人工过目；ETF 不强行归组（发行人 CIK 和基金实体是两回事，
   第一期只做 CS）；
4. 读取层收益兑现（1 天）：`research/market_cap.py`（size 因子依赖链）提供
   company 级合并市值选项；`research/fundamentals.py` 的 CIK join 文档化
   "per-company 去重先经 companies"。

### 验收

- 活跃 CS 的 company_id 覆盖 ≥95%；GOOG+GOOGL、BRK.A+BRK.B 各归一 company 且
  合并市值与外部参考一致（±2%）；
- 每公司多证券的清单产出（这就是双类股名录），行数与 13F 审计的 3,056 量级吻合。

### 坑

- **CIK ≠ 永久公司**：重组会换 CIK（Alphabet 2015 新 CIK，Google 旧 CIK 退役）。
  第一期接受"CIK 世系断点"（Alphabet 案已知，见 fundamentals-massive-archive 记忆的
  价格断层遗留），company_events 边表留给第二期缝合世系。
- 不要试图用名字模糊匹配归组——只认 CIK 与人工确认。

---

## 任务 3：PIT 股本面板（historical_shares 推深到 2009+）

### What

读取层（不回写事实表！）：`research/shares.py` 提供
`load_pit_shares(engine, start, end) -> as-of 面板`，数据源 = `sec_fundamental_facts`
里已有的股本类 XBRL 概念，与 `historical_shares`（2024-06+ 的 vendor 段）拼接，
vendor 段优先。

### Why

- `size`/`earnings_yield` 等市值分母因子目前只有 2 年历史 vs 23 年价格——评估判定
  的"全栈杠杆最高的一件事"（差距地图 sev5）。
- 原料已在库：12.9M 条 XBRL 事实里的 `dei:EntityCommonStockSharesOutstanding`
  （封面披露，每份 10-K/10-Q 都有）与 `us-gaap` 股本概念；`filed_date` 就是 PIT 边界。

### How

1. 概念普查（半天）：`utils/sec_concepts.py` 白名单里核对已入库的股本概念覆盖率
   （按年 × 证券统计非空率）；不足则把缺的概念加进白名单重跑
   `update_sec_fundamentals --bulk-zip`（回填路径现成）；
2. 面板构建（1-2 天）：仿 `research/fundamentals.py` 的 vintage 口径——as-of t 取
   `filed_date <= t` 的最新申报值；**拆股调整陷阱**见"坑"；
3. 与 vendor 段对拍（1 天）：2024-06+ 双源重叠期，XBRL 推的 vs `historical_shares`
   的相对误差分布，>5% 的样本列出归因（B 类股、库存股口径、申报滞后）；
4. `research/factors/builtins/size.py` 依赖链切换 + `--start 2010-01-01` 重评估。

### 验收

- 活跃 CS 的 as-of 股本覆盖：2010 年起 ≥85%；重叠期与 vendor 中位相对误差 <2%；
- size 因子 2010+ 评估报告落 `research/output/`，IC 序列无 2024-06 断点跳变
  （有跳变=两段口径没接平）。

### 坑（这个任务坑最深，认真读）

- **XBRL 股本是"申报日时点值"，不随拆股回溯调整**：AAPL 2020-08 拆 4:1，
  2020-07 申报的股本是拆前口径。as-of 面板直接用会让拆股当周市值× 4 错位。
  解法：市值 = shares(as-of, 申报时口径) × **未复权原始价**（同时点同口径相乘，
  市值天然正确）——绝不要用复权价乘未调整股本。这与 `research/data.py` 的
  panel 口径要显式对齐，写测试锁死（用 AAPL 2020-08 那周做金样本）。
- dei 概念可能一司多值（多类股各报一行）——先按 CIK 聚合再经任务 2 的 company
  归组分摊，第一期可只做单类股证券（覆盖率仍够验收线）。
- 270 天新鲜度门槛沿 fundamentals.py 惯例（退市/停报后的股本不能永远沿用）。

---

## 任务 4：身份史增厚（改名事件溯源）

### What

给 `security_symbol_history` 的 11,555 条改名记录补事件溯源（当前仅 19 条 RENAME
进了 `security_identity_events`），并给 `security_identifiers` 物化 PIT FIGI/CUSIP 行。

### Why

- 任期归属的精度上限 = 身份史的完整度：2.23B 行分钟数据 unmapped、29.1M 行 13F
  未映射、2003 年代 ~33% 行找不到主，根子都在这里；
- 任务 1（8-K 按 CIK 匹配）与任务 2（公司世系）都消费这份溯源。

### How

1. 存量考据（2 天）：对 history 里无事件的 (security_id, symbol, start_date)，
   交叉三源定证据——Massive ticker_events（`update_massive_events` 已入库的
   ticker_change）、fundamentals 归档的 `ticker_events.parquet`（30,431 行，在
   `/tmp/fundamentals_massive`，Mac 重启丢失则从 `~/Documents/fundamentals_massive_new.tgz`
   重解）、SEC filings 的公司名变更；三源一致 → 写 RENAME 事件（confidence HIGH）；
2. FIGI 补链（1 天跑批）：`sync_openfigi_identifiers` 扩到退市股（评估：退市 FIGI
   仅 43.5%；OpenFIGI 免费，`OPENFIGI_API_KEY` 提速）；
3. 回收再归属（2 天）：身份史增厚后重跑 `import_day_aggs --dry-run` 与分钟
   `--refresh-tenures`，量化 unmapped 回收量；确有大改善再实跑（分钟侧从 Mac
   归档重放对应月份）；
4. 157 组重复 FIGI 与 19 组待人工合并（deep-review 遗留）借 `repair_identity`
   工具过一遍。

### 验收

- RENAME 事件覆盖率 19 → ≥60%（三源可证部分）；退市 FIGI 43.5% → ≥70%；
- 13F NULL security_id 行数下降量化（当前 29.1M）；分钟 unmapped 回收量化报告。

### 坑

- ticker_events 的 46.6% epoch 哨兵日期（1969-12-31）直接丢弃；
- 归档 ticker_events 是 2026-04 快照，与库内 live 版本冲突时 live 胜（FBL 教训：
  快照 id/字段会被 vendor 事后挪用）。

---

## 任务 5：securities 表身份/详情分离（先设计后动手）

### What

**第一期只交设计文档**（`docs/securities_split_design.md`），不动表。目标形态：

```
securities          -- 身份核心：symbol/current_symbol/type/market/exchange/
                    -- list_date/delist_date/is_active/cik/figi/company_id + 时间戳
security_details    -- vendor 易变快照：name/address/branding/market_cap/
                    -- shares_outstanding/description/... + snapshot 时间
```

### Why

- 2026-07-06 的 list_date 全舰队抹除事故（见 `data_infra_assessment_2026-07.md`
  整改记录）就是身份字段与 vendor 快照字段混居一张表、共用一条 upsert 通道的代价。
  探针（integrity 阻塞项）只是止血带，结构分离才是根治。

### How（设计文档要回答的问题清单）

1. 全量盘点 `securities` 的 47 列 → 身份/详情/时间戳三类归属表；
2. 写路径矩阵：sync_universe（by-symbol）/update_details（by-symbol+id）/
   sync_delisted / repair_identity 各自允许写哪张表哪些列——**身份表只允许
   details/identity 两条通道写，universe 同步只可读**；
3. 迁移策略：视图兼容期（`securities` 变视图 join 两表）vs 双写期，评估 40+ 处
   `Security` ORM 引用的破坏面（grep 统计每个字段的读者）；
4. 回滚方案与分阶段验收门。

**不要在没有设计评审前动 models.py**——这张表被 40+ 处引用，是全仓最高危变更。

### 验收（第一期）

- 设计文档含：列归属表、写路径矩阵、迁移三阶段、每阶段测试清单、预估工时。

---

## 建议执行顺序与依赖

```
任务3（独立，2-4 天，兑现最快）──────────────┐
任务1 步骤1-2（Form25+终价，独立可先行）──┐   ├→ 因子层立即受益
任务2（3-5 天）→ 任务1 步骤3-4（分类器要用公司名/CIK 匹配）
任务4（1 周，随时可插队跑批部分）→ 反哺任务1 覆盖率与分钟/13F 回收
任务5（设计文档，穿插做）
```

预计总量：3-4 周串行；两窗口并行可压到 ~2 周。每完成一个任务：工程记录追加到
本文档对应节 + 更新 `data_infra_assessment_2026-07.md` 差距地图 + CLAUDE.md 表清单。

---

## 工程记录（2026-07-06 执行窗口，multi-agent workflow 一日落地主体）

提交链 `732ea80..53d0d99`，全部部署 253，migration head `a2b3c4d5e6f7`。
研究阶段对本文档假设的订正与各任务实际结果如下。

### 任务 1（delisting_events）——步骤 1-3 完成，步骤 4 部分完成

- **假设订正**：退市股 CIK 覆盖实测 95.3%（CS，`securities.cik` 兜底路径），
  不是本文写的 ~26%——Form 25 回拉未等任务 4 即全量执行。真正缺的是退市股的
  8-K 历史（抽样仅 8% 有退市日 ±30 天 8-K，因 `--all` 只同步活跃），已与 Form 25
  同一轮回拉（form 过滤是客户端行为，零额外请求）。
- `sec_filings.items` 新列：EDGAR submissions 自带 item codes 被 adapter 丢弃，
  补列后 Item 2.01 判定变纯 SQL。回拉 9,576 CIK/98.9 万行/55 分钟零失败；
  Form 25 族 0 → 8,435 行（25-NSE 6,915 + 25 1,395 + /A 125），84.3 万行 8-K 带 items。
- **2025-08-01 截断队列根因**（本文未预见，评估的"496 截尾"已恶化到 674）：
  管道休眠期（2025-08-02→2026-05-13）内退市的 417 只，复活时已 inactive 被
  `update_massive_prices` 的 is_active 过滤永久挡住补拉；day-aggs 导入又按
  massive-era 守卫跳过该段。修复：`--include-inactive`（仅显式 symbols）+
  退市日上钳（回收防护镜像 list_date 下钳），497 只在 730 天窗口内重拉后
  冻结 409→3、停滞 674→206（残余在窗口外，final_price=NULL+evidence）。
- 分类器 `build_delisting_events.py`：终价 ±5 天窗（含 yfinance 双 NULL OTC 尾巴），
  四个失败桶 evidence 编码；HIGH=同 CIK 8-K item 2.01 / Form 25 12d2-2 规则段解析
  （`--fetch-form25-docs`），MEDIUM=identity MERGE/ETF 清盘，LOW=价格形态只定性；
  全量重建 upsert，MANUAL 行保护；health_report 新增退市结局 P1 探针。
- **最终验收（三轮迭代后，2026-07-06 晚）**：
  - **reason 非 UNKNOWN 76.2%（5,711/7,494）✓ 过 70% 线**；final_price 92.8% ✓。
    三轮迭代的三个杠杆：① Form 25 文档解析修复（25-NSE 的 primary_document_url
    是 xslF25X0N 渲染器路径，剥掉后是带 `<ruleProvision>` 的原始 XML；HTML 版靠
    ☒/☐ 勾选框检测——首版把"模板列出全部条款"误判为不确定，2,255 份只解析出
    1 份）；② ADS/MLP 类守卫例外（文档类别的非普通股标记词 ⊆ 证券自身名称标记词
    时放行——ADR 的 Form 25 类别就是 "American Depositary Shares"）；③ 8-K item
    3.01 退市通知层（582 只 UNKNOWN 有交易所不达标/摘牌通知，纯 SQL MEDIUM 证据）。
  - **现金并购 return 自检 ✓**：n=710，p10=-4.8% / p50=+0.02% / p90=+0.22%——
    如文档预言聚在 0（终价已收敛到对价），抽取链正确性数据自证。
  - 对价抽取漏斗：现金 742 / 换股比 173 / 收购方 369 / 门控剔除 32 / 歧义 40
    （sanity gate [0.2x, 5x] final_price）。
  - **delisting_return 覆盖未达 50% 线，如实记录**：表内实测 710（9.5%，全部
    现金独占并购）+ 读取层 ETF 清盘 par 合成 ~1,071（`load_delisting_returns
    (fund_closure_par=True)`，经验值按 schema 纪律放读取层不进事实表）≈ 23.7%。
    缺口主体=换股/混合对价并购的 return（需收购方价格面板估 stock leg）与
    无残值证据的破产（-1 只在有法院文件时写）。下迭代路径已在代码 followup 注明。
  - UNKNOWN 1,783 只清单在 `logs/manual_backfill/delisting_unknown_final3.csv`
    （其中 source=FORM25 的攥着证据定性不了——wrong_class 782 多为同 CIK 票据/
    优先股类 Form 25，indeterminate 257 为无勾选标记的老文档）。
- **步骤 4 的 20 年动量对比（验收实验，2026-07-06 晚完成）**：面板 2010-01-01~
  2026-06-10（9,127 只，评估窗 2011-01-01 起，双边 10bps）。
  旧口径（`--terminal-return none --no-delisting-returns`，退市持仓赚 0%）vs
  实测口径（delisting_events 1,781 只，面板内 671 只，未覆盖仍沿旧口径）：
  momentum_12_1 总收益 **733.4% → 714.0%**（CAGR 14.8%→14.6%，Sharpe
  0.643→0.637），sma_50_200 428.9%→423.2%，5 日反转 30.3%→30.0%。方向与
  Shumway 退市偏差一致——旧口径系统性高估；且本对比是**下界**（面板内实测仅
  671 只、未覆盖者仍按 0% 处理，对价抽取覆盖扩大后修正会更大）。
  momentum 的退市持仓日 4,057 天（0.53% 持仓日）。

### 任务 2（companies）——完成

- apply 结果：9,339 公司、10,635 只 CS 挂接（活跃 CS 99.85%、全 CS 97.34%）、
  0 改挂；GOOG+GOOGL、brk.a+brk.b 验收探针过。报告五件套在
  `logs/manual_backfill/companies_*.tsv`（双类股名录/工具行误标/改名世系）。
- **假设订正**：基本面无重复计数问题（入库即锚每 CIK 最小 id），真实问题是反面
  ——非主类股基本面为零、earnings_yield 分母低估；"3,056 多 ticker CIK"是 13F
  侧口径，securities 侧全 CS 多证券组实测 1,077（活跃 CS 86 组，多为 baby
  bonds/优先股误标 CS）。合并市值必须过 `is_common_equity_name` 普通股过滤。
- **合并市值口径**：我们=上市类别之和。Berkshire 与外部一致到 0.1%
  （1,095.2 vs 1,095B）；Alphabet 4,039 vs 外部 4,346B，差额恰为未上市 B 类
  0.86B 股——外部把 B 类按上市价计入。±2% 验收在"上市类合并"口径下成立。
- **关键修复**：`update_sec_fundamentals --include-inactive` 的 CIK 锚定改为
  活跃优先（`is_active DESC, id ASC`），否则 26 个 CIK 的 1,290 万行事实会
  翻锚到低 id 退市证券、活跃 ticker 基本面变 NaN（已加锁定测试）。

### 任务 3（PIT 股本）——完成（验收评估 2026-07-06 晚收官）

- **验收结果**：双源重叠期对拍中位相对误差 **0.00%**（2,001 只，p90 3.3%，>5% 者
  9.2%）✓ 过 <2% 线；as-of 覆盖（当年年中当时活跃 CS）2012 62.6% / 2016 71.1% /
  2020 79.0% / 2024 82.5%——**低于 ≥85% 线，如实记录**：缺口=ADR/IFRS 挂起缺口 +
  信托/LP 误标 CS + pre-2009 退市物理无 XBRL（数据存在性天花板，非实现缺陷）；
  vendor 段（2024-06-30+）覆盖接近全量。
- **size 因子 2011-2026 评估**（`research/output/evaluate_size_2011-01-03_
  2026-07-02.md`）：IC 0.015（1d）→0.030（21d），NW t 3.8-6.5，PIT 违规 0，
  覆盖低分位 74%。**2024-06 接缝检查过**：IC 接缝前 6 月均值 0.094 vs 后 7 月
  0.081（连续无断点）；覆盖数接缝处 +11.5%（3,934→4,386）系 vendor 段补上
  无 XBRL 的 ETF/小报告方——覆盖改善而非口径断裂。

- **假设订正 ×3**：(a) 6 个股本概念全部已在白名单且已入库（2009-04 起，dei 概念
  16.9 万行/4,459 家）——概念普查零工作量；(b) `research/market_cap.py` 已实现
  本文"坑"里要求的整套方案（事件流→as-of→拆股前滚×原始价），任务缩水为给
  `load_shares_events` 缝 XBRL 段；(c) 真正的瓶颈是幸存者偏差：退市 CS 仅
  45/6,059 有 XBRL 事实，根因 `resolve_cik_map` 只选活跃——bulk-zip 重跑
  （--include-inactive，命中 8,588 家 vs 原先约一半）是实际回填杠杆。
- **坑的补充**（AAPL 金样本验证）：`市值=股本×未复权价` 不充分——拆股落在
  period_end 与下一次申报之间时（AAPL 2020-08-31 拆 4:1，下一份 10-K 10-30 才
  filed）有 9 周窗口 as-of 股本是拆前口径。拆股前滚必须**锚 period_end**
  （XBRL 股本是该时点计量值），vendor 段锚 visible_date（400 天）不变；
  接缝以每事件 stale_after/split_anchor 列表达，旧路径位级一致由既有测试锁定。
- 2010 年 XBRL phase-in 只有 773 家——"2010 起 ≥85%" 实际从 2011-12 起算。

### 任务 4（身份史）——主体完成

- **分母订正**："11,555 条改名记录"实为任期行，真实改名 873 次/831 只——
  验收覆盖率按此分母计。
- apply 结果：RENAME 事件 19→633（HIGH 284/MEDIUM 330），覆盖率 68.1%（>60% 线）；
  PIT FIGI 12,704 行物化进 security_identifiers（此前 FIGI 行为零，id_type='FIGI'
  快照语义）；185 条退市任期行（source=MASSIVE_ARCHIVE）；幂等重跑 0 写验证过。
- 报告桶待人工：tail_mismatch 200（快照后改名，live wins 隔离）、cik_ambiguous
  475、figi_ambiguous 154、unresolved 3,747（多为 universe 外 OTC/权证）。
- **退市 FIGI 43.5%→43.7%，距 ≥70% 目标缺口是硬约束**：parquet 只能唯一归属 11 只；
  CUSIP→FIGI 路线候选仅 35 只（FIGI 缺失的退市股绝大多数也没有 CUSIP 识别符）。
  需要新数据源（如 OpenFIGI ticker+exchange+日期 组合查询不可行——非 PIT 有回收
  污染）。157/19 组重复 FIGI 合并经查 2026-07-02 已完成（deep-review），残余
  82 个 'UNKNOWN' 字面量 FIGI 待清理。
- 13F 映射经另一项目收割至 77.5%，剩余为设计内排除品种，不再追。

### 任务 5（拆表设计）——第一期交付完成

- `docs/securities_split_design.md`：实测 50 列（本文写 47，现含 company_id 51）；
  三桶=身份 15/详情 25/同步状态 9 + 死列 2（sector、base_currency_name 直接删）；
  写路径=7 条 db_manager 通道 + 8 个绕行写方；**规则重述**："universe 同步只可读"
  与现实矛盾（rename/停用/隔离只有它在写）——改为"universe 同步不得经通用 upsert
  写身份属性，只可调用一等身份操作 API"。迁移三阶段：绕行写方收口→双写+对账探针→
  读方改指+旧列下线；生产零视图/触发器，跑道干净。估 8-11 人日。

### 通用教训

- `stmt.excluded.<col>` 的 getattr 访问在列名撞 ColumnCollection 字典方法
  （items/keys/values）时返回 bound method，psycopg2 报 can't adapt——共享
  `_build_upsert_statement` 已改 `excluded[key]` 索引访问；新列名命名时注意。
- `upsert_delisting_events` 是全量重建语义（冲突覆盖全部 payload 列，缺 key 置
  NULL），绝不可当局部更新用——已在 docstring 与测试锁死。
- 并行窗口同树协作：分钟因子文件（research/factors/builtins/bar_geometry 等）
  属主窗口在途工作，本窗口 commit 时须显式排除。
