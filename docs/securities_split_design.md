# securities 表身份/详情分离设计（任务 5 第一期，2026-07-06）

> 本文是设计评审件，**不动表、不动代码**。对应 `docs/todo_crsp_grade_2026-07.md`
> 任务 5 的验收清单：列归属表、写路径矩阵、迁移三阶段、每阶段测试清单、预估工时。
> 所有 file:line 引用以 2026-07-06 并行工作树为准（含任务 1/2 当日刚落地的
> models/db_manager 增量；并行窗口仍在动这两个文件，行号可能继续漂移，
> **函数名/列名才是稳定锚点**）；prod 侧事实经 253 只读 psql 复核。

## 1. 背景与事故动机

`securities` 是全仓最高危的一张表：durable 身份（`security_id` 锚定 16 条
存量外键链，并行任务 1 的 `delisting_events` 刚加了第 17 条）、
vendor 易变快照（名称/地址/品牌/市值）、pipeline 调度状态（7 个 watermark 时间
戳 + 价格水位线）混居一处，共用同一批 upsert 通道。

**2026-07-06 list_date 全舰队抹除事故**是这次设计的直接动机：

- 根因：`/v3/reference/tickers` 列表响应不带 `list_date` 字段 → payload 携带
  None 原样下发 → `upsert_securities_by_symbol` 的 `SET list_date = excluded.
  list_date` 把 details 辛苦回填的值批量置 NULL，全舰队 10,531 行被抹，
  防回收 clamp（价格回填以 `securities.list_date` 为下界）随之全体失效。
- 现有防线（三道，全部是**止血带**而非根治）：
  1. payload None 剥离：`data_sources/massive_source.py:342-345`
     （"除 symbol 外 None 一律剥离"）；
  2. 回归测试：`tests/test_massive_source.py:404-418`
     （`test_reference_payload_strips_none_fields`）；
  3. integrity 阻塞探针：`scripts/check_data_integrity.py:167-187`
     （活跃 US CS/ETF 中 list_date NULL > 50 即阻塞）。
- 为什么止血带不够：by-symbol 通道的结构没变——它对"payload 里出现的任何
  非 protected 列"执行 `SET col = excluded.col`（`db_manager/securities.py:
  249-270`），protected 集合只覆盖 watermark 与 symbol（:167-179）。任何一个
  **身份列**（`is_active`/`delist_date`/`exchange`/`type`/`cik`/FIGI...）只要
  vendor 响应形态再变一次（字段消失、字段值异常），同样的抹除可以在任何一列
  重演。探针只能保证"抹了之后 24 小时内变红"，不能保证"抹不动"。

结构性根治 = 把"谁可以写什么"从**运行时约定**（protected_fields 集合、payload
剥离纪律）升级为**表边界**：身份列放进只接受受控操作的身份表，vendor 快照放进
可以随便整行覆盖的详情表，调度水位放进第三张状态表。这与 CLAUDE.md 的既有
架构纪律（"security_id 是 durable 身份，symbol 是易变属性"）是同一思想在
表结构上的落地。

## 2. 现状盘点

### 2.1 列数与读者面（证据）

- `Security` 模型共 **50 列**（`data_models/models.py:64-160`；253 prod
  `information_schema.columns` 计数同为 50）。TODO 文档写的"47 列"是过时数字，
  以 50 为准。另：并行任务 2 当日已在工作树加上第 51 列 `company_id`
  （models.py:92-93，migration 未上 prod），本设计已将其计入身份桶。
- 破坏面：全仓 **42 个 .py 文件**经 ORM 引用 `Security`（25 个非测试 + 17 个
  测试），另有 **14 个非 alembic 文件**只用 raw SQL 摸 `securities` 表
  （`research/data.py`、`research/fundamentals.py`、`research/industry.py`、
  `research/run_baselines.py`、`research/universe.py`、`scripts/audit_recent_
  data.py`、`scripts/audit_security_identity.py`、`scripts/health_report.py`、
  `scripts/import_day_aggs.py`、`scripts/repair_identity.py`、`scripts/repair_
  ohlc_violations.py`、`scripts/run_massive_shares_chunks.py`、`scripts/sync_
  delisted_universe.py`、`scripts/sync_openfigi_identifiers.py`），合计约
  56 个文件（另两个 alembic 历史 migration 不需要改）。
- 读者高度偏科：类级 `Security.<col>` 非测试引用计数——`symbol` 36、
  `is_active` 19、`price_data_latest_date` 17、`cik` 7；而**纯详情列
  （`market_cap`/`sic_code`/`sector`/`description`/`logo_url`/`round_lot` 等）
  的非测试 ORM 读全部为 0**——它们只出现在 `data_sources/massive_source.py`
  的 payload 构造里（写方向）。唯一例外是 `sic_code` 有一处 raw SQL 读者：
  `research/industry.py:133`（`select id as security_id, sic_code, type,
  is_active from securities`）。
- 结论：**详情表几乎是免费拆的**（读者面 ≈ 1 处 raw SQL + payload 构造器）；
  身份列与 watermark 才是硬表面。
- 身份解析器（`utils/security_identity.py:100-108`）恰好只读 8 列：
  `id / symbol / current_symbol / composite_figi / share_class_figi / cik /
  exchange / is_active`。**注意 `share_class_figi` 在解析链里**——TODO 任务 5
  的目标形态草图漏了它，本设计把它明确归入身份表。
- prod 兼容跑道干净：253 上 `securities` **零视图、零 matview、零触发器**
  依赖（2026-07-06 只读 psql 复核），"视图兼容期"没有存量包袱；但 14 个
  raw-SQL 文件与多处 ORM `update()` 写者意味着**不可更新视图会打碎写路径**
  （见 §5 的策略取舍）。
- 部分唯一索引 `_active_symbol_uc` 与 `_active_current_symbol_exchange_uc`
  （`data_models/models.py:145-160`）约束的全是身份列（symbol/current_symbol/
  exchange/is_active），必须随身份表走。
- 任务 2 已给 securities 增列 `company_id`（models.py:92-93，FK companies.id）
  ——它是身份归属，本设计把它计入身份表；拆表 migration 照单迁移即可，无冲突。

### 2.2 写路径盘点（7 条 db_manager 通道 + 7 处旁路）

db_manager 通道（`db_manager/securities.py`）：

| # | 通道 | 位置 | 冲突键 | 今日可写范围 |
|---|------|------|--------|--------------|
| W1 | `upsert_security_info` | :64 | `id` | 除 protected（:92-103：watermark+symbol+full_refresh_interval）外任意列；恒 bump `info_last_updated_at`（:111） |
| W2 | `upsert_securities_by_symbol` | :131 | 部分唯一索引 `symbol WHERE is_active` | 除 protected（:167-179）外任意列；带 FIGI/CIK 冲突隔离（:192-240 写 QUARANTINE 事件并跳行） |
| W3 | `update_security_timestamps` | :280 | `id` | 白名单 7 个 `*_last_updated_at`（:282-290） |
| W4 | `update_security_price_latest_date` | :305 | `id` | `price_data_latest_date` (+`full_data_last_updated_at`) |
| W5 | `ensure_security_price_latest_date_at_least` | :323 | `id` | `price_data_latest_date`（只前进不后退） |
| W6 | `rename_security` | :348 | `id` | `symbol`/`current_symbol` + symbol_history 区间维护 + 占用防御 |
| W7 | `insert_backfilled_securities` | :440 | 纯插入 | 整行（强制 `is_active=False`，退市补录专用） |

旁路写者（绕过上表直接 UPDATE/DELETE `securities`）：

| # | 旁路 | 位置 | 写什么 |
|---|------|------|--------|
| B1 | `sync_massive_universe` missing→inactive | `scripts/sync_massive_universe.py:282-291` | ORM `update()` 批量 `is_active=False` |
| B2 | `sync_delisted_universe` NULL 守卫回填 | `scripts/sync_delisted_universe.py:196-203` | raw UPDATE，`col IS NULL` 守卫，只补空 |
| B3 | `sync_delisted_universe` 富化 | `scripts/sync_delisted_universe.py:333-338` | raw UPDATE `COALESCE(col, :col)`，只补空 |
| B4 | `sync_sec_identifiers` cik 回填 | `scripts/sync_sec_identifiers.py:80-88` | ORM `update()`，`cik IS NULL` 守卫 |
| B5 | `repair_identity` 合并收尾 | `scripts/repair_identity.py:207-220` | raw UPDATE `price_data_latest_date` 重算 + `is_active=false` |
| B6 | `calibrate_price_latest_date` | `scripts/calibrate_price_latest_date.py:99-106` | ORM 批量 `update()` 水位校准 |
| B7 | `import_day_aggs` 水位自愈 | `scripts/import_day_aggs.py:355-361` | raw UPDATE 水位重算 |
| B8 | `cleanup_us_universe` | `scripts/cleanup_us_universe.py:112` | ORM `delete(Security)`（先手工删 10 张子表再删主表，:102-112） |

关键观察：

- **`price_data_latest_date` 一列就有 6 条独立写路径**（W4/W5 经
  `scripts/update_massive_prices.py:55,80` 与 `scripts/update_grouped_daily.
  py:183`，加 B5/B6/B7）——这不是详情，也不是身份，是**调度/水位状态**，
  有自己的白名单通道（W3 的 allowed_fields 就是既有先例）。二分法
  （身份/详情）装不下它，必须给第三桶。
- 旁路 B2/B3/B4 全部自带 NULL-only 守卫——它们事实上已经在执行"身份列只许
  补空不许覆盖"的纪律，只是散落在各脚本的 raw SQL 里。拆表设计把这条纪律
  收编为 db_manager 的一等 API（§4）。

## 3. 50 列全量归属表

目标形态三张表（保留 `securities` 原表名的取舍见 §5 开头）：

```
securities            -- 身份核心，保留原表名与原 id 序列，16 条 FK 不动
security_details      -- vendor 易变快照，1:1 FK securities.id，可整行覆盖
security_sync_state   -- pipeline 调度状态，1:1 FK securities.id，白名单写
```

逐列归属（50 列全量，√=无争议，※=歧义列见下方裁决，†=有轻微争议已裁决）：

### 3.1 身份表 `securities`（原 50 列中留 14 列 + 任务 2 的 company_id = 15）

| 列 | 归属理由 |
|----|----------|
| `id` √ | durable 主键，16 条 FK 与序列锚点（`_sync_model_id_sequence` 目标） |
| `symbol` √ | 解析器输入；rename 一等操作的对象 |
| `current_symbol` √ | 解析器输入；与 exchange 组成部分唯一索引 |
| `market` √ | universe 筛选主键之一（`select_us_securities`，utils/massive_task.py:91-93） |
| `type` √ | 同上（CS/ETF 铁律过滤） |
| `exchange` √ | 解析器输入 + 部分唯一索引成员 |
| `cik` √ | 解析器输入 + QUARANTINE 判定字段（db_manager/securities.py:22-28） |
| `composite_figi` √ | 同上 |
| `share_class_figi` √ | **解析器输入（utils/security_identity.py:105）——TODO 草图漏列，此处显式纠正** |
| `is_active` √ | 解析器输入 + 两个部分唯一索引的 WHERE 条件 + 生命周期状态 |
| `list_date` √ | 防回收 clamp 下界；本次事故主角 |
| `delist_date` √ | 生命周期边界；任务 1 delisting_events 的 join 锚 |
| `company_id`（任务 2） √ | 公司归属是身份 |
| `created_at` / `updated_at` √ | 行级审计（三张表各自保留一对） |

歧义列裁决（4 列，全部判给**详情表**）：

- `vendor_market` ※ → 详情。理由：Massive 的 `market` 字段快照（"stocks"），
  只有 payload 构造器写（massive_source.py:332,362），全仓**零非测试读者**；
  universe 过滤走我们自己的 `market`/`type`，不走它。它是 vendor 分类快照，
  不是我们的身份判定输入。
- `locale` ※ → 详情。同上：vendor 快照，零非测试读者。
- `ticker_root` / `ticker_suffix` ※ → 详情。理由：可由 symbol 重算的 vendor
  派生描述字段；且 `rename_security`（W6）今天改名时**并不同步维护它们**——
  改名后即 stale。若判给身份表，就得给 W6 加维护义务，徒增身份写面；判给
  详情表则由下一次 details 刷新自然覆盖，语义自洽。

### 3.2 详情表 `security_details`（迁入 25 列 + security_id + 自有审计对）

`security_id` PK/FK → securities.id（1:1，ON DELETE CASCADE，见 §7 风险 R3）。

| 列 | 备注 |
|----|------|
| `name` † | 判详情：解析器不读 name；QUARANTINE 事件里 name 只是 details JSON 的取证快照（db_manager/securities.py:208）。改名审计的真身在 `security_symbol_history` 与 identity_events，不靠这列 |
| `currency` † | 判详情：交易货币是 vendor 描述性字段，非测试 ORM 读者为 0（分红折算走 corporate_actions.currency + fx_rates，不读这列） |
| `currency_symbol` √ | vendor 快照 |
| `base_currency_symbol` √ | vendor 快照 |
| `vendor_market` ※ / `locale` ※ / `ticker_root` ※ / `ticker_suffix` ※ | 裁决见 §3.1 |
| `round_lot` √ | vendor 快照 |
| `share_class_shares_outstanding` √ | vendor 时点值；PIT 股本走 `historical_shares`（任务 3 另有 XBRL 面板），这列只是"最新快照" |
| `weighted_shares_outstanding` √ | 同上 |
| `market_cap` √ | vendor 最新市值快照（读取层市值另算） |
| `phone_number` / `description` / `homepage_url` / `total_employees` √ | 纯详情 |
| `sic_code` √ | 详情，但有一个 raw SQL 读者（research/industry.py:133）须在阶段 2 repoint |
| `industry` √ | SIC 描述文本 |
| `address_line1` / `city` / `state` / `postal_code` √ | 地址 |
| `logo_url` / `icon_url` √ | 品牌 |
| `vendor_last_updated_at` † | 判详情：这是"Massive reference 数据自身的更新时间"（models.py:124），即**快照的来源时间戳**，属于快照本体，不是我们的调度水位 |
| `created_at` / `updated_at` √ | 新表自有（不从原表迁移）；`updated_at` 即事实上的 snapshot 时间 |

（校验：身份留 14 + 详情迁 25 + 水位迁 9 + 死列 2 = 原 50 列，无一遗漏。）

### 3.3 状态表 `security_sync_state`（9 列 + security_id）

`security_id` PK/FK → securities.id（1:1，ON DELETE CASCADE）。

| 列 | 备注 |
|----|------|
| `price_data_latest_date` | 6 条写路径的汇聚点（§2.2）；integrity 检查"须等于 daily_prices.max(date)"的对象 |
| `full_data_last_updated_at` | W4 的 full-refresh 分支 |
| `info_last_updated_at` | details 刷新水位（注意：它衡量的是"详情多久没刷"，是调度状态，不是详情本体——details 表自己的 `updated_at` 是快照时间，两者语义不同，都保留） |
| `actions_last_updated_at` / `events_last_updated_at` / `shares_last_updated_at` / `short_data_last_updated_at` / `news_last_updated_at` | W3 白名单成员（db_manager/securities.py:282-290） |
| `full_refresh_interval` | 调度参数（随机 25-40 天），与 watermark 同生命周期 |

第三桶的存在理由（对 TODO 二分草图的修正）：这 9 列有**独立的写者群**（W3/W4/
W5 + B5/B6/B7）、独立的写频率（每天每证券多次 vs 详情的按周）、独立的一致性
契约（水位=事实表聚合，可随时重算，丢了不心疼）。塞进身份表会让最高危的表
承受最高频的写；塞进详情表会让"整行覆盖快照"的简单语义被 protected 集合
重新污染——protected_fields 机制存在的唯一原因就是 watermark 和快照同表。
**分出去之后，详情表可以没有任何 protected 列**，upsert 语义退化为最简单的
"整行以新代旧"，事故面即消失。

### 3.4 死列（拆表时 drop，不迁移）

- `sector`：零写者、prod 全表 0 非空（2026-07-06 复核）。
- `base_currency_name`：prod 全表 0 非空（同上）。

两列在阶段 3 的 migration 里直接 DROP，down-revision 补回可空列即可（无数据
可恢复，回滚零成本）。

## 4. 写路径矩阵（目标态）

### 4.1 规则的重述（解决 TODO 与现实的矛盾）

TODO 任务 5 原话："身份表只允许 details/identity 两条通道写，**universe 同步
只可读**"（todo_crsp_grade_2026-07.md:271-272）。这条规则照字面执行会立即
瘫痪身份维护——`sync_massive_universe` 是全库**唯一**的自动化身份写者：
改名走 `rename_security`（scripts/sync_massive_universe.py:199-202 改名后再
补元数据）、RECYCLE/QUARANTINE 事件、missing→inactive 摘牌（B1）全在它手里。
禁写等于身份表从此只能人工维护。

重述为可执行的形式：

> **universe 同步不得经通用 upsert 写身份属性；它只能调用身份写服务里的
> 一等身份操作（rename / deactivate / reactivate / quarantine / enrich-null /
> insert-new-listing）。**

即：禁的不是"universe 同步写身份表"，而是"`SET col = excluded.col` 这种
无差别属性覆盖到达身份列"。一等操作各自带前置校验（rename 的占用防御、
quarantine 的 FIGI/CIK 冲突判定、enrich 的 NULL-only 守卫），每条操作显式、
可审计（配套 identity_events）、且**结构上写不了它职责之外的列**。

### 4.2 目标态矩阵

身份表列按写语义分四组：`INSERT-only`（id/list_date/company_id 初值）、
`OP-only`（symbol/current_symbol/is_active/delist_date——只有一等操作可改）、
`ENRICH-only`（cik/figi 族/list_date——非 NULL 后冻结，冲突走 QUARANTINE）、
`FROZEN`（id 永不改）。

| 写者 | securities（身份） | security_details | security_sync_state |
|------|--------------------|------------------|---------------------|
| W1 `upsert_security_details`（原 upsert_security_info，update_massive_details 用） | ENRICH-only：cik/figi/list_date 补空；冲突→QUARANTINE 事件，不覆盖 | **整行覆盖**（无 protected 列） | bump `info_last_updated_at` |
| W2' `sync_universe` 新上市插入 | INSERT 整行（新 id） | INSERT 快照行 | INSERT 空水位行 |
| W2'' `sync_universe` 存量刷新 | 禁止（属性覆盖不可达身份列） | 整行覆盖 | — |
| W2''' `sync_universe` 身份操作 | `rename_security`（W6）/ `deactivate_missing`（收编 B1）/ QUARANTINE（既有 :192-240 逻辑内聚到身份服务） | — | — |
| W3 `update_security_timestamps` | — | — | 白名单 7 时间戳 |
| W4/W5 价格水位 | — | — | `price_data_latest_date` (+full_data_last_updated_at) |
| W6 `rename_security` | OP：symbol/current_symbol + history 区间 | —（ticker_root/suffix 留给下次详情刷新） | — |
| W7 `insert_backfilled_securities` | INSERT（is_active=False 强制） | INSERT 快照行 | INSERT 空水位行 |
| B1 → 收编为 `deactivate_missing_securities` API | OP：is_active=False | — | — |
| B2/B3 → 收编为 `enrich_security_identity`（NULL-only）+ `upsert_security_details` | ENRICH-only（保持既有 `col IS NULL` / COALESCE 守卫语义） | 富化字段中的详情列走详情覆盖 | — |
| B4 → 并入 `enrich_security_identity` | ENRICH-only：cik 补空 | — | — |
| B5 `repair_identity` | OP：is_active=False（合并败方摘牌）——修复工具保留直写特权，但阶段 2 起改走 API | — | 水位重算（改走 W4/W5 或新 `recalibrate` API） |
| B6/B7 水位校准/自愈 | — | — | 水位重算 |
| B8 `cleanup_us_universe` | DELETE（子表清完后删身份行；details/sync_state 靠 ON DELETE CASCADE 或显式删除，见 §7 R3） | CASCADE | CASCADE |

矩阵的不变量（阶段 3 后可用权限/触发器或代码评审锁定）：

1. 任何 `ON CONFLICT DO UPDATE SET col = excluded.col` 都到达不了身份表的
   OP-only/ENRICH-only 列——这就是 list_date 事故的结构性根治：**vendor
   响应再怎么变形，最多污染详情表**，详情表整行覆盖本来就是预期语义。
2. `security_sync_state` 只接受白名单列名的 UPDATE（沿用 W3 的
   allowed_fields 模式扩展到 9 列）。
3. 身份表的每次 OP 写配套 `security_identity_events` 审计行（既有惯例）。

## 5. 迁移三阶段

总体取舍先说结论：**推荐"写者收口 + 双写期 + 读者兼容视图"，不推荐
"securities 变视图 + INSTEAD OF 触发器"**。理由：

- prod 零视图/零触发器的干净跑道（§2.1）是留给**读者**的：给读者建兼容视图
  无历史包袱。但写者侧有 14 个 raw-SQL 文件和多处 ORM `update()`——
  `securities` 若变成 join 视图，这些写者全部立刻碎裂（非可更新视图），
  INSTEAD OF 触发器虽能救，但等于把写路由逻辑藏进数据库层，与本仓"写入
  语义在 db_manager 集中测试锁定"的纪律（tests/test_db_manager_pg.py）相悖，
  且触发器语义（rowcount、RETURNING、ON CONFLICT 穿透）与 SQLAlchemy 的
  交互坑多。
- 保留 `securities` 作为**物理身份表**（只 DROP 移走的列）还有三重红利：
  16 条 FK 与 id 序列原地不动；部分唯一索引（models.py:131-145）原地不动；
  身份解析器（utils/security_identity.py:100-108）读的 8 列全部存活，
  **零改动**。

### 阶段 1：写者收口 + 建新表 + 双写（预计 4-5 天）

**1a 写者收口（先于任何 schema 变更）**：把 B1-B7 旁路全部收编为 db_manager
API（§4.2 矩阵右列的 API 名）。这是纯代码重构，不改 schema、不改行为，
逐个旁路可独立上线。B8（cleanup）保留脚本形态但删除顺序文档化。
完成判据：`grep -rn "UPDATE securities\|update(Security)" scripts/` 只剩
repair_identity（修复工具特权，注明阶段 2 收口）。

**1b 建表 + 双写**：alembic migration 建 `security_details` +
`security_sync_state`（含回填 INSERT...SELECT，行数 = securities 行数，
校验计数相等）；db_manager 各通道在**同一事务**内双写旧列与新表
（详情/水位写两份，身份列只写旧表——旧表本来就是身份表，无需双写）。
1a 完成后所有写者都在 db_manager 里，双写覆盖率由构造保证，无旁路泄漏。
`check_data_integrity` 加对账探针：旧列 vs 新表逐列 IS DISTINCT FROM 计数，
>0 即阻塞。

**回滚**：1a 可逐 API revert（行为等价重构）；1b 直接 DROP 两张新表 + revert
db_manager 双写代码，旧表全程是 source of truth，零数据风险。

**部署注意**：253 上 alembic upgrade 避开 02:00 UTC ±1h 的 daily run
（todo_crsp_grade_2026-07.md:15）；表拷贝仅数万行，秒级，磁盘 30G 约束无压力。

### 阶段 2：读者 repoint（预计 3-4 天）

新表数据经阶段 1 对账探针连续多日全绿后开始。逐文件把详情/水位的**读**指向
新表；身份列读者不用动（还在原表）。需要改的读者全量清单：

- **raw SQL（14 个非 alembic 文件，§2.1 列表）**：逐个 grep 其 SQL 里引用的
  列，只有摸了详情/水位列的才需要 join 新表。已知必改：
  `research/industry.py:133`（sic_code → join security_details）、
  `scripts/health_report.py` / `scripts/audit_security_identity.py` /
  `scripts/audit_recent_data.py`（水位与详情探针改 join）、
  `scripts/import_day_aggs.py` / `scripts/repair_identity.py` /
  `scripts/sync_delisted_universe.py`（写侧已在 1a 收口，此处只剩读侧）、
  `research/data.py` / `research/universe.py` / `research/run_baselines.py` /
  `research/fundamentals.py` / `scripts/repair_ohlc_violations.py` /
  `scripts/run_massive_shares_chunks.py` / `scripts/sync_openfigi_identifiers.py`
  （多数只读身份列，逐个确认后大部分零改动）。
- **ORM（25 个非测试文件）**：核心是两处公共设施——
  `utils/massive_task.py:71-118` 的 `select_us_securities`（staleness 过滤与
  排序读 watermark 列，:101-113 的 `getattr(Security, staleness_column)` 改为
  join `SecuritySyncState`；市场/类型/活跃过滤读身份列不动）与
  `scripts/update_massive_prices.py:44-83`（水位对齐读写）。其余 update_massive_*
  脚本经 massive_task 间接受益，自身多为零改动。
- **兼容视图（可选但推荐）**：建只读视图 `securities_full` = 三表 join，供
  ad-hoc psql 与未来忘改的长尾查询；**不叫 securities**，避免任何写者误写。

**回滚**：读是无副作用的，逐文件 revert 即可；视图 DROP 即回滚。

### 阶段 3：断开旧列（预计 1-2 天 + 一周观察期）

- db_manager 停止双写（详情/水位只写新表）；
- alembic migration：DROP `securities` 上已移走的 25 详情列 + 9 水位列 +
  2 死列（sector/base_currency_name），`Security` 模型同步瘦身为 §3.1 的
  15 列；`upsert_security_info` 更名/重写为 §4.2 的 W1 语义（protected_fields
  集合随 watermark 出表而**整体消亡**）；
- down-revision 必须可执行：重加列（可空）+ 从新表 INSERT...SELECT 回填
  ——数据在新表里全程保留，所以阶段 3 也可回滚，只是窗口内新写入需要
  重放（回滚脚本按 `updated_at` 增量补）。

**验收门（每阶段独立）**：阶段 1 = 对账探针连续 7 个 daily run 全绿；
阶段 2 = 全量 pytest + `health_report` 退出码 0 + 一次完整 `scheduled_update`
在 253 无 step failure；阶段 3 = 同阶段 2 + `check_data_integrity` 全绿 +
alembic downgrade/upgrade 往返在测试库演练通过。

## 6. 每阶段测试清单（点名既有文件扩展）

**阶段 1a（写者收口）**：

- `tests/test_db_manager_pg.py`：新 API 各加集成用例——`deactivate_missing_
  securities`（B1 语义：只摘活跃 US CS/ETF）、`enrich_security_identity`
  （NULL-only：已有值绝不覆盖、冲突计数返回）、水位重算 API（B5/B6/B7 语义：
  IS DISTINCT FROM 才写）。现有 :59 起的
  `test_insert_then_update_preserves_protected_watermarks` 系列全程保持绿——
  它锁定的 protected 语义在阶段 3 前不变。
- `tests/test_sync_massive_universe.py` / `tests/test_script_runs.py`：
  mock db_manager 断言脚本改调新 API 而非直写。
- `tests/test_repair_identity.py`：合并收尾走 API 后语义不变。

**阶段 1b（双写）**：

- `tests/test_db_manager_pg.py`：每条写通道加"写后三表状态"断言（详情/水位
  两表与旧列一致）；`insert_backfilled_securities` / 新上市插入加**同事务**
  断言（人为让第二张表插入失败，验证整体回滚，见 §7 R1）。
- `tests/test_health_report.py` 或 integrity 测试：对账探针的红/绿两态。
- `tests/test_massive_source.py:404-418` 原样保留（None 剥离防线在双写期
  仍是活防线）。
- migration 回填的行数校验写进 migration 本身（RAISE if count mismatch）。

**阶段 2（读者 repoint）**：

- `tests/test_select_us_securities.py`：sqlite fixture 目前只建 securities 表
  （CLAUDE.md：全 metadata 含 sqlite 不支持的 ARRAY 列）——staleness join 后
  需同时建 `security_sync_state`；这是该文件的结构性改动，先改测试再改
  `select_us_securities`。
- `tests/test_research_data.py` / `tests/test_adjusted_prices.py`：研究层读
  路径回归。
- `research/industry.py` 的 FF12 测试（若有）+ 手工对拍 sic_code join 前后
  行数一致。
- 全量 `python -m pytest tests/ -q`（含 PG 集成，/tmp 一次性集群）。

**阶段 3（断开旧列）**：

- `tests/test_db_manager_pg.py`：protected-watermark 用例改写为新语义
  （watermark 不在 securities 上了，改断言 sync_state 不被详情 upsert 触碰）；
  详情表"整行覆盖、无 protected"新用例。
- alembic downgrade → upgrade 往返在 `TEST_DATABASE_URL` 演练。
- 253 部署后连续 7 天 `journalctl -u stock-daily-run.service` + `health_report`
  观察。

## 7. 预估工时

| 阶段 | 内容 | 估时 |
|------|------|------|
| 1a | 7 处旁路收口为 db_manager API + 测试 | 2-3 天 |
| 1b | 两张新表 migration + 双写 + 对账探针 | 2 天 |
| — | 阶段 1 观察期（7 个 daily run） | 日历 1 周（无人力占用） |
| 2 | 读者 repoint（14 raw-SQL 逐个甄别 + massive_task/update_massive_prices + 测试 fixture） | 3-4 天 |
| 3 | 断列 migration + 模型瘦身 + protected 语义迁移 + 往返演练 | 1-2 天 |
| — | 阶段 3 观察期 | 日历 1 周 |

合计 **8-11 个工作日**，日历跨度约 4 周（两段观察期与其它任务并行，实际
人力占用 2 周内）。

## 8. 风险清单

- **R1 两表写的部分失败**：新上市/退市补录要在一次调用里写身份 + 详情 +
  水位三行。现各通道自开连接（`with self.engine.connect()`），必须保证三写
  同事务（INSERT 身份 RETURNING id → 同 conn 插详情/水位），任何一步失败
  整体回滚——否则出现"有身份无详情"的孤儿行。测试用例见 §6 阶段 1b。
  双写期同理：旧列与新表必须同事务，防止对账探针把网络抖动误报成漂移。
- **R2 id 序列与锁**：`_lock_model_sequence_sync` / `_sync_model_id_sequence`
  的目标保持在身份表 `securities`（id 唯一来源）；details/sync_state 以
  security_id 为 PK，**无自有序列**，天然免疫序列漂移一类的老问题。
- **R3 cleanup_us_universe 的 DELETE 级联**：脚本现在手工按 10 张子表逐删再删
  securities（scripts/cleanup_us_universe.py:102-112）。details/sync_state 用
  `ON DELETE CASCADE` 让该脚本零改动即正确；但要意识到这是**全仓第一处
  CASCADE FK**（既有 16 条 FK 都无级联）——若不愿开先例，就在脚本显式加两行
  delete，并在 §6 阶段 1b 测试锁定"删身份行必须先删/同时删两张子行"。
- **R4 select_us_securities 的 join 性能**：staleness 过滤改 join 后，
  `security_sync_state` 需要在各 watermark 列上建索引（现 `price_data_latest_
  date` 在 securities 上有索引，models.py:127）；1:1 join 数万行本身无压力，
  但 `ORDER BY watermark NULLS FIRST` 要确认走索引。
- **R5 双写期的探针误报**：`info_last_updated_at` 由 `func.now()` 服务端求值，
  双写两条语句若分别求值会差微秒——双写实现要么单语句 CTE 写两表，要么
  客户端取一次时间戳两处复用；对账探针对 timestamp 列用秒级容差。
- **R6 raw-SQL 长尾**：14 个文件之外，`docs/` 里的 runbook、备忘 SQL、
  以及研究 notebook 可能还有手写 `FROM securities` 摸详情列的长尾。兼容视图
  `securities_full`（§5 阶段 2）+ 阶段 3 观察期的 `pg_stat_statements` 抽查
  兜底；`securities` 表名不变保证只摸身份列的长尾查询永不碎。
- **R7 与并行任务的合流**：任务 2 的 `company_id` 增列、任务 1 的
  delisting_events FK 都锚 `securities.id`——本设计身份表保名保 id，两任务
  先后落地均无冲突；但 models.py 是共享文件，阶段 3 的模型瘦身须与并行窗口
  按 TODO 协作约定先 pull 再动。
- **R8 事故防线的时序**：massive_source 的 None 剥离（:342-345）与 integrity
  探针（check_data_integrity.py:167-187）在阶段 3 完成前**一条都不能拆**——
  它们保护的是旧表上仍然存活的通用 upsert。阶段 3 后 None 剥离降级为数据
  卫生（防止详情表被 NULL 覆盖有值快照——这仍然值得保留），list_date 探针
  永久保留（它检验的是事实状态，与表结构无关）。
