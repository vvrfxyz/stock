# TODO：ADR 二期（2026-07-07 立项，接全域入库收官）

> 本文档是给**下一个执行窗口**的交接件。一期（ADR 全域入库：Phase 0 类型门 +
> runbook 11 步 + 8 项裁决 + §E.6 研究层 opt-in）已于 2026-07-06/07 两天内全部
> 落地并部署 253，工程实况、执行修订、裁决记录见
> `adr_expansion_plan_2026-07.md` 的 §G/§H/§I 三节。本文只写**还没做的**和
> **新解锁的**。
>
> 阅读顺序：`adr_expansion_plan_2026-07.md`（§A 不变量必读，§G 修订、§I 裁决先例）
> → 本文。记忆索引另有 `adr-expansion-execution` 条目（会话级细节）。

## 开工前置（每次都要）

- `git pull` 双侧确认同头（一期收官时 GitHub 与 253 = `a62fe14`，alembic 无新迁移）。
  **同一工作树常有并行窗口在途文件**（07-07 是 CRSP 二期 + 研究 wave），
  commit 必须逐文件精确 add；共享文件（`research/data.py`/`evaluate.py`）改动
  先 `git diff` 分清归属。
- 避开 02:00 UTC ±1h（每日调度）。手动 Massive 任务与 nightly 不能并发
  （跨进程限流不协调，双侧 429）。
- **一期踩过的三个坑，二期照样会踩**：
  1. `update_massive_details` 没有 `--all` 旗标，且 universe sync 插入行自带
     info 时间戳（30 天新鲜度过滤失效）——**新证券定向更新一律走显式 symbol
     清单**（模式：SQL 拉 symbol 到 `/tmp/xxx.txt`，`main.py update_massive_details
     $(cat ...) --force`）。
  2. 价格顺序铁律：Massive 730 天窗口**先行**，flat 导入后跑（水位保护）。
  3. `securities.type` 原样存 vendor 类型；名称启发式不能推翻 vendor 口径
     （§I.7 先例：163 只 ADR 名称的 CS 行，vendor 时点视图 151 只即标 CS，
     已重鉴定关闭，**勿再立项**）。
- 归档公司行动重跑后 `mismatch.tsv` 会**常态回显 3 条已裁决项**
  （PCRB/PHYD 精度修订保 prod、FBL 拒归档错值，§I.4）——不是新问题，勿重查。

## 一期验收快照（基线数字，二期对照用）

| 指标 | 一期终值 | 备注 |
|---|---|---|
| ADR universe | 活跃 ADRC 376 + 退市家族 275 + SHEL 谱系 3 段 | type 原样存 |
| ADR 日线 | 151.7 万根（2003-09 起，620 只） | TSM 5,739 / BABA 2,963 / SHEL 1,108+4,158 |
| ADR 公司行动 | 分红 7,723 + 拆股 506，因子链全建 | TSM 双配股裁决先例 §I.1 |
| 13F 挂链率 | 80.8% → **87.1%**（+830 万行） | share-class 回退消歧 `0305602` |
| ifrs-full 基本面 | 全库 13.99 万行（TSM 29 概念） | 存储层；读取层 USD-only |
| FX 汇率源 | ECB 21.9 万行 + FRED DEXTAUS 10,426 行（1983 起） | **已就位，任务 B 直接用** |
| 研究 opt-in 宇宙 | 5,277 → 5,656（+379） | `--include-adr`，默认 CS-only 不动 |
| adr_unsafe 门 | size 实测门掉 249 只 ADR 列 | 任务 A 的解锁目标 |

---

## 任务 A：ADS 比率归一化——解锁三个 adr_unsafe 因子（头号工程）

### What

给每只 ADR 建立 `ads_ratio`（1 ADS = N 股普通股），把市值/股本类计算归一到
公司口径，然后取消 `size` / `earnings_yield` / `short_interest_ratio` 三个因子
类上的 `adr_unsafe=True` 标记（`research/factors/builtins/`，evaluate 层的门
在 `research/evaluate.py` `_load_type_ids`/`adr_gated_columns` 一带）。

### Why

- opt-in 宇宙里 ~249 只活跃 ADR 在这三个因子下是 NaN 列——BABA/TSM 这类
  权重标的进不了 size/价值类研究，opt-in 的研究价值打了对折。
- 不归一直接算的后果已量化：TSM 1 ADS = 5 普通股，市值错 5 倍。

### How（建议路径，未预注册）

1. **先做双源对拍定口径**（半天，纯只读）：对活跃 ADR 逐只对比
   `historical_shares`（vendor）与 XBRL `shares_outstanding`
   （`research/fundamentals.py` METRICS，dei 概念）——比值分布若聚在整数/半整数
   （2/5/8/10…），vendor 存的是 ADS 数、XBRL 是公司普通股数，比值即 ads_ratio；
   若聚在 1，说明 vendor 已是公司口径，任务退化为"确认 + 标记摘除"。
2. 比值不整的杂例（增发/回购窗口错位、多类股）落人工清单，头部 ~50 只
   （13F 权重排序）手工核对 20-F 封面（"each ADS represents X ordinary shares"）。
3. 存储：**不回写事实表**（研究层只读铁律）。ads_ratio 放
   `research/` 读取层常量表或 `security_identifiers` 新 id_type=ADS_RATIO
   （后者要过 db_manager 测试，成本高；第一版建议读取层 CSV/表内嵌）。
4. `research/market_cap.py` / `research/shares.py` 挂 ratio 归一，金样本测试
   锁定 TSM（对拍 2330.TW 公司市值量级）。

### 坑

- XBRL 股本是**申报口径不随拆股回溯**（`research/shares.py` 拆股前滚逻辑已处理，
  别绕过它直接除）。
- ADS ratio 会变（TSM 没变过，但存在 ratio change 公司行动的品种）——第一版
  接受"当前 ratio 全史外推"，在 docstring 声明；ratio 变更史是二期的二期。
- `short_interest_ratio` 的分子（FINRA 空头仓位）对 ADR 报的是 ADS 数还是
  普通股数需单独验证（大概率 ADS 数，与 vendor shares 同口径则比率自洽，
  可能根本不用归一——先对拍再动手）。

### 验收

- TSM/BABA 的 size 值与公司市值外部源（如 companiesmarketcap）量级一致（±5%）；
- 三个因子摘除 `adr_unsafe` 后 `tests/test_research_adr_optin.py` 的标记锁定
  测试同步更新（改预期集合为空或删除该门）；
- `--include-adr` 跑 size 的 `adr_gated_columns=0`。

---

## 任务 B：基本面 FX 归一化——解锁 TWD/EUR 申报 FPI（数据源已就位）

### What

`research/fundamentals.py` 的金额事实目前只取 `unit='USD'`（混币种防线，
`tests/test_research_adr_optin.py::test_amount_metrics_still_usd_only` 锁定）。
放开为：非 USD 单位的事实按报告期汇率折算成 USD 再进事件流。

### Why

- TSM（台币申报）的 revenue/net_income 面板现在全 NaN——ifrs-full 概念白名单
  已让数据入库（13.99 万行），差的只是读取层折算。
- 汇率源**一期已备齐**：`fx_rates` 有 ECB 全史（EUR 基准交叉）+ FRED DEXTAUS
  台币全史（1983 起），`utils/fx_rates.UsdFxConverter` 已有 USD 基直连回退
  （方向测试锁定，1000 TWD @32.5 → 30.77 USD）。

### How

1. `load_fundamental_facts` 放开 unit 过滤（改为 unit 白名单 = USD + fx_rates
   有覆盖的币种），SQL 带回 unit 列。
2. 事件流构造处（`build_metric_events` 前）按 **period_end 汇率**折算
   （市值可比性口径；filed_date 汇率会把汇率波动混进重述事件流）。
   folder：`UsdFxConverter` 是 db_manager 接口，研究层用 engine——需要一个
   读取层等价物（`fx_rates` 表直读 + bisect as-of，30 行以内）。
3. MetricSpec 增 `fx_converted: bool` 记账（进 config/params_hash，
   新旧口径 trial 不互相顶替——先例见退市终局口径）。

### 坑

- flow 指标 TTM 三分量若跨大幅汇率波动期，折算日口径（各自 period_end）与
  "全年平均汇率"口径会有差——第一版用 period_end 点汇率，docstring 声明。
- unit 字符串脏值（"TWD" vs "NT$"？）先 `select distinct unit` 摸底。
- ARS/CLP/PEN 无汇率源（§E.4 接受跳过），放开 unit 过滤时白名单显式排除。

### 验收

- TSM `revenue_ttm` 出值，与 20-F 的 USD 便利折算列量级一致（±10%，
  汇率日口径差异属预期）；
- 纯 US filer 的面板位级不变（回归测试）。

---

## 任务 C：分钟线 ClickHouse ADR 增补（§E.5 原文挂起项）

### What

`stock.minute_bars`（50.6 亿行）补 ADR 的分钟线：归档 `flatfiles_1m` 里的
ADR ticker 增补导入 + `massive_1m` 周度增量确认自动接管。

### How / 坑

- `scripts/update_minute_bars.py` 的类型门 Phase 0 已改共享常量（ADR 自动放行），
  周度增量应已自动接管**活跃** ADR——先查 ClickHouse 里 tsm/baba 最近分钟数据
  是否已在流入（可能什么都不用做），再决定归档回补范围。
- ClickHouse 只走 HTTP 8123（`research/minute_bars.py`），253 上 wenruifeng
  **无 docker 组**，别试 `docker exec clickhouse-client`。
- **绝不用分钟加总回填日线**（收盘竞价与合并成交量在分钟条之外）。
- 归档 1m 文件量大，跑之前看磁盘（07-07 时 211G 可用）并避开 nightly。

### 验收

- `research/minute_bars.py` 能拉出 TSM 任一交易日完整分钟条；
- 分钟因子（`intraday_*`/`bar_geometry`）`--include-adr` 评估不再全 NaN。

---

## 任务 D：FTD 20 年回收误链审计（§E.1，一期显式挂起）

### What

`sync_cusip_identifiers --months 240` 用**当前活跃 symbol 快照**匹配了 20 年的
FTD 文件，身份事件仅覆盖 2026-06 后——存在把旧公司 CUSIP 错链给现任 symbol
持有者的理论风险。做一次"FTD 观测期 ∈ 证券任期"校验：对每条 CUSIP 映射，
其 FTD 出现日期段应落在挂链证券的 [list_date, delist_date] 内，越界的降级/删除
并回滚对应 `institutional_holdings.security_id`。

### 坑

- 任期数据源：`securities` + `security_symbol_history`（bt/enia/rds.a 手术后
  history 语义可靠，先例 §I.5）；list_date NULL 的行用首 bar 日代理。
- 修复动作是删 `security_identifiers` 行 + 置 NULL 持仓外键——写 dry-run 报告
  先看规模，预期误链是小众（回收 symbol 且两任都有 13F 持仓才会中招）。

---

## 挂起项（有意不做，防止误立项）

| 项 | 状态 | 依据 |
|---|---|---|
| 163 只 ADR 名称的退市 CS 行改类型 | **已裁决关闭** | vendor 时点视图即 CS（§I.7），type 原样存铁律 |
| 退市 ADR 大面积无 CIK/list_date | 接受 | vendor 时点视图缺档（§E.2），与退市基本面 0.9% 缺口同类 |
| 157 组同 FIGI 多退市行（含 13F 跳过的 71 组） | 移交 | CRSP 二期"四清单裁决"（`todo_crsp_phase2_2026-07.md`），非 ADR 专属 |
| 同日多笔现金分红乘法 vs 加法（EURN 1.1% 偏差） | 方法论待审 | §I.2；vendor 同约定、对账 MATCHED；改口径 = methodology_version 升版工程 |
| ARS/CLP/PEN 分红 256 行 | 接受跳过 | 无汇率源（§E.4），任务 B 白名单显式排除 |
| SHEL shares/新鲜度 | 无需动作 | nightly 自然接管中，health_report P2 自动消化 |

## 常态运维口径变化（一期引入，值班须知）

- 每日串行道 +376 只 ADR（prices/shorts/news/open_close 各步 ≈ +10 分钟）。
- 新增巡检工件：`logs/split_conflict_quarantine.tsv`（live 拆股冲突裁决队列，
  出现新行 = 需人工，裁决先例 §I.1）；`logs/delisted_match_audit.tsv`
  （退市名单跨类型吸收审计，`matched_cross_type>0` 的 WARNING 要看）。
- 周日 `update_fx_rates` 增量已含 FRED 分支（DEXTAUS 周度跟进）；无
  FRED_API_KEY 的环境用 `--skip-fred`。
- OpenFIGI AMBIGUOUS 消歧重跑**花 API**（缓存不存候选 payload）：
  精确重查用"老化 queried_at + 默认 refresh-days"模式（§G.2 与 §I 实操），
  别用 `--refresh-days 0`（会连 8.3 万 NOT_FOUND 一起重查）。
