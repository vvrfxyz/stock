# 深度代码审查与发展路线分析（2026-06-13）

> 方法：10 个维度并行深度审查（schema/写入路径/Massive 管道/SEC 管道/复权与 FX/研究层/测试/运维/性能/代码质量与安全），
> 每条 critical/high/medium 发现由 1–2 个独立对抗验证 agent 试图反驳或复现后才计入；
> 另有 4 个角度的路线分析（数据广度/研究能力/工程基建/运维自动化）。
> 共 84 个 agent。结果：**48 条确认 / 4 条被否决 / 2 条未验证 / 23 条低危**。

---

## 总体评价

**代码纪律明显领先于运维纪律。** 多个维度的审查者独立得出同样结论：

- 写入层 13 个 upsert 的冲突键与唯一约束逐一对得上，protected 字段语义有 PG 集成测试锁定；
- 全库金额/股本用 Numeric/BigInteger，事实表干净遵守 raw-facts-only，models.py 与 alembic 头无漂移；
- Massive HTTP 层超时/退避/分页中断防护齐全，截断响应不会当完整数据落库，apiKey 日志脱敏一致；
- SEC filed_date 口径一致（从未误用 period end），companyfacts API 与 bulk-zip 共用解析器，解析层有教科书级 golden-XML 测试；
- 复权因子的分红/拆股公式、FX 交叉汇率方向、vendor 对账设计经逐项验算正确；
- 基本面 PIT（vintage/TTM 三分量锁定/营收族 coalesce）是 Compustat PIT 级别的设计，16 个语义测试全过；
- 无 SQL 注入面、API key 从未入库（git 历史核验）、177 个非集成单测 1.3 秒全绿。

但存在 **2 个 critical、约 13 个 high 的结构性问题**，集中在三条线上：
1. **身份模型的核心承诺未实现**——symbol 事实上是持久键；
2. **复权链的 as_of 语义有数学缺陷**——未来事件污染整条链；
3. **调度内容有系统性缺口**——因子不重建、universe 不同步、部分脚本失败不报错。

---

## 一、确认的问题（按修复优先级）

### Critical

#### C1. symbol 是事实上的持久键：ticker 回收会合并两家公司、ticker 变更会分裂身份
`data_models/models.py:131-134`、`db_manager/securities.py:123-143`、`scripts/sync_massive_universe.py:69-89`

CLAUDE.md 第一条规则是"security_id 是持久身份，symbol 只是可变属性"，但实现层系统性违反：
- `UniqueConstraint('symbol')` 让 symbol 全局永久唯一；`upsert_securities_by_symbol` 以 `index_elements=['symbol']` 做 ON CONFLICT UPDATE，更新集含 name/cik/composite_figi/list_date/is_active 全部身份属性，无任何 FIGI/CIK 一致性校验。
- **合并场景（ticker 回收）**：A 公司退市仅标 is_active=False、symbol 永不改写；B 公司用同一 ticker 上市后，universe 同步命中 A 的旧行，把身份字段整体改写成 B 并重新激活，而 price_data_latest_date 受保护保留旧值——B 的 K 线从增量起点直接接在 A 的历史后面，两家公司的价格/公司行动/基本面被静默拼接成一条序列，复权与回测在拼接点产生完全错误的收益。
- **分裂场景（ticker 变更，如 FB→META）**：新 ticker 无冲突 INSERT 出全新 security_id 并重复下载全部历史，旧行被 missing→inactive 扫描误标退市——同一证券裂成两个 id。美股每年数十起改名，分裂场景几乎确定已在库内发生。
- `current_symbol` 机制（`_current_symbol_exchange_uc`）有约束无实现：全仓库该字段恒等于 symbol；`update_massive_events` 写入的 symbol_history 没有任何消费者用来重连身份。

验证：两个独立验证 agent 均确认（一个维持 critical，一个因 730 天窗口限界下调 high）。

**修复方向**：upsert 命中 symbol 冲突时先比对 composite_figi/cik，不一致判定为 ticker 回收——旧行 symbol 退役改名、新公司插新行；symbol 全局唯一约束改为部分唯一（`WHERE is_active`）；消费 ticker_change 事件真正实现 current_symbol；写一个 FIGI/CIK 对账脚本检测存量已发生的合并/分裂。

#### C2. cumulative_factor 链包含 as_of 之后（含未来已宣告）事件，读取层 as_of 过滤无法消除污染
`scripts/update_adjustment_factors.py:253-331`、`utils/adjusted_prices.py:69-93`、`research/data.py:86-116`

因子构建对 corporate_actions 不做 ex_date 过滤，把 ex_date 在未来的已宣告事件也纳入后缀积；读取层 as_of 只过滤事件**行**，保留行里已被未来事件污染的 cumulative_factor 原样使用。验证 agent 用内存复现证实：过去 15:1 缩股 + 已宣告未来 20:1 缩股时，as_of 读取在历史边界日的复权收益被扭曲 **20 倍**（幻影 -95% 单日）。`tests/test_adjustment_factors.py` 的 xxii 案例证明"未来拆股已宣告未生效"是生产中真实形态——**该污染当下就作用于研究面板**：任何有已宣告未到 ex 日分红的股票，其最近一次历史 ex 日复权收益带约 1% 误差；待生效拆股则是数倍至数十倍。"一致性测试"只锁因子应用语义不锁装载语义，两个读取层是**一致地错**。

**修复方向**（数学上精确且改动小）：读取层改归一化语义——`factor(d, as_of) = C(第一个 ex_date > d) / C(第一个 ex_date > as_of)`（无事件取 1）。因为链是后缀积，该式对任意 as_of 精确还原"当时可见"的因子。`utils/adjusted_prices.factor_for_date` 与 `research/data.apply_adjustment` 同步修改，补"保留事件+排除事件并存"的回归测试。

### High

**研究有效性类：**

- **H1. 回测 NaN 收益置 0：退市/停牌头寸按陈旧价格"无损逃逸"**（`research/backtest.py:57-64`）。已复现：100→90 后退市（真实归零）的股票回测只实现 -10%；停牌序列 [100,NaN,NaN,40] 的复牌 -60% 跳空对 P&L 贡献为 0。对专买暴跌股的 5 日反转基线构成方向性向上污染，`research/output/` 下已有据此产出的指标。修复：对曾持仓列 ffill 后再 pct_change 补回跨缺口收益；退市股施加保守退市收益（CRSP 惯例），或至少在结果中报告受影响持仓数。
- **H2. 基本面 filed_date 当日可见 × t 日收盘建仓 = 对盘后申报财报的一日前视**（`research/fundamentals.py:431-438`，schema 层根因：`sec_fundamental_facts` 缺 accepted_at，`data_models/models.py:525`）。EDGAR 16:00–17:30 ET 接受的申报 filed_date 仍是当日，但 t 收盘时信息尚未公开；基本面策略会系统性吃到财报隔夜跳空（PEAD 方向恰好向上）。当前只跑技术基线尚未触发，一旦跑基本面策略即为 critical。修复：可见性后移一天（`allow_exact_matches=False`），或回填 accepted_at（`sec_filings.accepted_at` 已存在，可经 accession join）。
- **H3. 分红因子缺口系统性留在面板：退市股分红永不建因子、被跳过分红静默消失，而 `securities_with_uncovered_events` 只查 SPLIT**（`scripts/update_adjustment_factors.py:337-341`、`research/data.py:166-181`）。ex 日 0.3%~10% 的幻影负收益集中在退市/濒退市名字上，恰好叠加在"纳入退市股以减幸存者偏差"最想保护的样本上（验证者评 medium，但与 H1 同向叠加污染反转/动量基线）。修复：因子重建去掉 is_active=True 限制（纯本地计算无 API 成本）；剔除函数纳入 DIVIDEND（可设金额阈值）。

**调度缺口类：**

- **H4. scheduled_update 从不重建 computed_adjustment_factors**（`main.py:133-275`）。每日/周日 actions 步骤持续写入新拆股/分红，但两条复权读取路径只消费因子缓存——新拆股落库后整段历史复权序列立即按拆股倍数失真，直到手动重建。佐证：`update_massive_actions.process_batch` 专门返回 `changed` 证券列表，`run()` 第 248 行用 `_changed` 丢弃——本应触发重建的钩子是死代码。修复：调度追加因子重建步骤（至少周日全量 actions 后），或消费 `changed` 做增量重建；读取层发现 corporate_actions 晚于缓存最新因子日期时告警。
- **H5. sync_massive_universe 不在任何调度中：新 IPO 永不入库、退市股永不下线**（`main.py:133-275`）。所有采集脚本只从已有 securities 行选取；缺口随时间单调恶化且无告警，研究横截面带系统性新股缺失。修复：加入调度（每日或每周），输出新增/转 inactive 计数。注意：**修复 C1 应在前或同时**——否则更频繁的 universe 同步会更快触发 ticker 回收合并。
- **H6. update_open_close_summary 失败既不返回非零也永不自愈**（`scripts/update_open_close_summary.py:138-201`）。main() 无返回值（隐式 None，execute_script 视为成功）；调度只回填 end_trading_date 单日，当日失败 = 盘前/盘后字段永久空洞。同主题：`update_massive_prices`/`update_massive_actions` 失败也 return 0，而 short_data/shares 失败返回 1——**同一调度内错误语义不一致**，OnFailure 告警链对一半脚本形同虚设。修复：统一"ERROR>0 → 非零退出"约定；open/close 改为扫描最近 N 日缺失组合使其可自愈。

**数据归属类：**

- **H7. Form 3/4/5 的 security_id 可能错挂到申报方而非发行人**（`scripts/update_insider_transactions.py:99`、`scripts/update_sec_filings.py:140-141`）。公司型 10% 股东场景（如 Berkshire 申报 OXY）：同一 accession 出现在两个 CIK 的 submissions 里，归属取决于处理顺序且必有一方向错；insider 写库直接信任 filing.security_id，从不与已解析的 issuer_cik 核对，默认模式跳过已解析 accession 使错误永久固化。修复：写库前用 issuer_cik 反查决定 security_id；存量数据可用一条 UPDATE…FROM 修复。

**工程类：**

- **H8. update_insider_transactions 用 NOT IN (子查询) 做去重，千万行后准平方级**（`scripts/update_insider_transactions.py:79-82`）。每周日生产路径踩中；PG 对 NOT IN 不做 anti-join 转换，DISTINCT 物化集超出 work_mem 后逐行线性扫描。修复：改 NOT EXISTS（可走 hash anti-join），或在 sec_filings 上维护 parsed_at 标记。
- **H9. migrate_database.py 三重缺陷可静默丢数据**（`scripts/migrate_database.py`）：无 ORDER BY 的 OFFSET 分页（行序不稳可漏/重）+ 迁移表清单缺 9 张现役表仍报"全部成功" + 失败退出码恒为 0。修复：keyset 分页 + 从 Base.metadata 派生表清单 + 正确退出码；若已不用建议连同 `main.py migrate` 子命令删除。
- **H10. 回测引擎与研究面板 SQL 层测试覆盖为 0**：`research/backtest.py` 0%（t/t+1 shift 这条防前视生命线无任何测试锁定），`research/data.py` 的 `date <= :as_of` SQL 谓词从未被执行——删掉它现有测试全绿。CLAUDE.md 宣称的"一致性测试锁定"实际只锁内层向量化函数。修复：合成数据单测锁 shift/turnover/metrics 语义；用现有 pg_db 基建加一个集成测试，同库同时跑两条读取路径多 as_of 断言相等。
- **H11. --recent-days 路径绕过 history-floor 兜底并每日盖戳**（`scripts/update_massive_actions.py:64-82,218`；验证者下调为 medium）：新证券第二天就被盖时间戳但只拉了 14 天事件，730 天窗口内历史事件缺失且 90 天 staleness 兜底永久失效，唯一治愈靠周日 force（与 SEC 任务同日竞争限速）。修复：recent-days 路径对无时间戳证券回退 floor，或不为其盖戳。

### Medium（按主题归组，全部经对抗验证确认）

**PIT 粒度与语义：**
- short_interests 无发布日字段，settlement_date 当可见边界有约 7 个交易日前视（读取层应硬编码保守滞后）；
- historical_shares.filing_date 实为 vendor 快照日，混存三种语义；
- 13F-HR/A 未解析 amendmentType/No、Form 4/A 与原件并存无关联字段——按 security_id 聚合会重复计数；
- FTD 历史 CUSIP 对照拿"当前 symbol"匹配（又一处 symbol-as-key），symbol 回收会写入永久错误身份映射；13F 写入路径 load_cusip_map 缺歧义保护（与回填 SQL 的 HAVING 不一致）。

**写入防御：**
- market_data/corporate_actions 系 upsert 缺批内冲突键去重（SEC 系新路径已有，未回补），vendor 重复行触发 CardinalityViolation 整批失败；
- historical_shares 重跑可把已有 float_shares 用 NULL 覆盖（缺 COALESCE）；
- 合成事件清理要求金额严格相等，vendor 修订金额后合成行残留 → 分红在因子链中双计；
- insert_missing_security_identifiers 非原子 check-then-insert。

**调度与运维：**
- check_data_integrity 未接入任何调度且无 heartbeat——exit 0 的坏数据无人发现；
- 周/月任务按实际运行日门控，systemd Persistent 补跑会静默丢掉整周/整月任务；
- **单机生产库零备份**——730 天窗口外数据 vendor 侧不可再生，单盘故障即永久损失（多个维度独立列为最高风险）；
- SEC 节流仅进程内，回填与周日调度并行时无共享限速（MEMORY.md 里的限速冲突即症状）；
- sync_massive_universe 的 missing→inactive 扫描无最小覆盖率保险丝，vendor 局部返回会批量误标退市；
- requirements.txt 全部未锁版本；单证券错误不传导退出码且口径不一致（同 H6）。

**性能：**
- 多行 VALUES 分片策略不一致，存在无上界单语句批（现仅靠 psycopg2 客户端插值不触发 65535 参数上限）；
- insider/13F 两张高增长表挂 15/12 个单列索引，多数无消费查询，纯写放大。

**研究与测试：**
- 幸存者偏差残留：未覆盖拆股证券按全窗整体剔除是以未来事件为条件，且集中移除困境退市股；
- 2024-05-14 因子可信下限只是 run_baselines 默认值+警告，装载层不强制（应 start 越界直接抛异常）；
- strategies/run_baselines 0% 覆盖、trading_calendar 主路径无测试、SEC 编排 run 层无测试、cleanup_us_universe（唯一批量删除脚本）零覆盖。

### 被对抗验证否决的发现（4 条，不必处理）

1. "vendor 对账不进退出码"——该脚本只在手动场景运行，mismatch 在统计摘要与 warning 中醒目可见；
2. "advisory lock + setval 串行化写入"——机制属实，但 Massive 限速（约 1.6 req/s 聚合）才是瓶颈，串行通道利用率 <1%；
3. "研究面板 chunksize 假流式"——全量物化本就是该函数契约，流式不改变存活量级（真正的出路是 Parquet 导出，见路线）；
4. "13F 回填 upper(cusip) 索引失效"——该查询形态下计划器选 hash join，本就不用连接键索引。

### Low（23 条，择要）

symbol_history 唯一键含可空 start_date（NULLS DISTINCT 陷阱）、news_insights 缺外键、限流器单 key 429 会 block_all 整 scope、13F 多 otherManager 只留第一个、无 XML 的 Form 永留 pending 集合、同日拆股+分红的口径歧义、metrics 漏首日收益、Sharpe 未扣无风险利率、同日多 accession 行序不定、research_engine 无只读保障、flock 占用静默 exit 0、日志无轮转、PG 绑定 0.0.0.0、线程池(24)>连接池(15)、xml.etree 无实体扩张防护、216KB 截图与 710KB CSV 在工作树、test_polyglot_architecture.py 命名过时（但并非死代码，是现行 schema 契约测试）。

---

## 二、发展路线（4 角度分析的综合）

### 各角度的核心判断

**数据广度**：当前最大瓶颈**不是广度而是历史深度**——730 天窗口把因子可信下限钉在 2024-05-14，扣除 warmup 后有效评估期约 1 年，覆盖不到任何一次体制切换，任何因子结论统计上立不住。月调仓约 25 个独立观察、Sharpe 标准误约 0.7。
**研究能力**：地基质量高，但只是"能跑基线的脚本"，离因子研究平台差评估、因子组织、组合构建三层。当前唯一有统计功率的研究风格是**日频横截面 rank-IC**（约 520 个日截面 × 3000+ 名字）；时序/趋势择时类在此样本量下不可能区分技能与运气。实查生产库的数据实况：securities.sector 全空（0/5274）、sic_code 73% 覆盖；新闻全库仅 36 篇文章（新闻因子不可行）；insider 仅 4 个月历史（等回填）；**13F 是隐藏富矿**（720 万行、2008Q1 起、88% 已映射）。
**工程基建**：ClickHouse 移除是正确决策——全库 18GB、daily_prices 2150 万行/2.6GB，PG 单机完全够用；重建矩阵读取层的唯一硬触发器是分钟线落地，且届时 Parquet+DuckDB 可能比 CH 更合适。pg_stat 行数估计失真 200 倍提示 ANALYZE 长期未跟上。
**运维自动化**：成熟度是"硬失败可感知"，盲区是沉默型故障（timer 失效、主机宕机）与"成功但数据错"。单操作者规模下明确**不建**：K8s、Prometheus 全家桶、Airflow/Dagster、流复制 HA、PgBouncer——运维栈上限就是 systemd + webhook + pg_dump。

### Now（修完 critical 后立刻做，多为 small/medium）

1. **每晚 pg_dump + 每周离机副本 + 恢复演练**——全局最大不可逆风险，复用现有 systemd timer 模式与 SSH 通道，方向是 Mac 拉取。
2. **dead-man's switch**——run_daily_cron.sh 成功路径末尾 curl ping healthchecks.io，3 行 shell 把告警语义从"失败才响"翻转为"不响就是出事"。
3. **check_data_integrity 接入调度收尾** + 结果落库（data_quality_runs 表）做 run-over-run 行数 delta 检查；阻断级非零退出走现有 OnFailure。先清存量问题再接入，防告警疲劳。
4. **评估 Massive 付费档报价**，对比 Sharadar/Norgate 等含退市股 EOD 源——把可信窗口从 2 年推到 5 年+是全项目单笔回报最高的支出；替代源若只给复权价则放独立研究表，绝不混入 daily_prices。
5. **因子构建覆盖非活跃证券**——修自己挖的幸存者偏差（同 H3），是任何历史回填的前置卫生条件。
6. **research/factors 因子库骨架**——把 fundamentals.asof_panel 抽成通用"事件表→as-of 面板"工具（insider/13F/short/股本全部复用同一条防未来管线），因子统一 compute(ctx)→宽表协议+注册表。
7. **research/evaluate.py**——多 horizon 日频 rank-IC + Newey-West t、IC 衰减、分位组合换手调整 IR、覆盖率诊断；所有试验 append-only 落盘 trials.parquet，短样本下 NW t<3 视为噪音。
8. **中性化原料**——SIC→FF12 行业静态映射（73% 覆盖，缺口记录在案）+ PIT 市值面板（close × shares_outstanding，原料全在库内）。
9. **财报事件日历**——从 sec_filings（含 accepted_at 精确时间戳）派生，零新数据解锁 PEAD/公告日研究；事件研究靠横截面事件数堆样本，是短窗口下少数统计可行的方向。
10. **部署脚本化 + alembic 版本守卫**——deploy.sh 串起 5 步手动 checklist；启动时比对 alembic current vs head，快速失败替代次日 10:00 才爆。

### Next（一季度内）

- **回测引擎升级**：分位多空资金中性 → 对 SPY 滚动 beta 中性 → EWMA 波动率目标；风险模型止步 Barra-lite（市场+log 市值+FF12 截面回归+对角特异波动），样本量撑不起更复杂的。
- **第一梯队另类因子**：做空数据（short_volumes 430 万行与复权窗口完全重合）与 13F（持有广度/ΔIO/HHI/拥挤度）优先；insider 等 06-17 回填完成；新闻因子从路线删除或先决定是否日更新闻摄取。
- **无风险利率**：照 update_fx_rates 模式接 FRED DTB3，半天的活，修 Sharpe 虚高。
- **PIT 指数成分**：前向每日存档 iShares ETF 持仓 CSV（成本近零，从今天起积累真 PIT 成分）+ EDGAR N-PORT 回填（与 13F 管线同构，复用 CUSIP 映射）。
- **退市处置事件层**：delisting_events 表区分并购/破产/转板/自愿四类终局（来源 Form 25/8-K/S-4，sec_filings 已索引），让回测对退市收益做有依据假设——与 H1 修复联动。
- **回填作业标准化**：ingestion_jobs 表（游标/状态/计数）+ 基于文件锁的跨进程 token bucket（SEC 与 Massive 各一 scope）——解决 MEMORY.md 里活生生的限速冲突；13F 的 SUCCESS_EMPTY 漏洞（0 行 filing 不留痕迹，pending 永不收敛）一并修。
- **研究读路径切 Parquet + DuckDB**：scheduled_update 末尾按年分区导出 daily_prices/因子/基本面事实，研究层改读文件——解除直连生产库耦合，免费拿列式扫描速度，也是未来分钟线的基础设施。PG 侧只补 daily_prices(date) 的 BRIN 索引。
- **编排保持线性**，但补 scheduled_runs 步骤记录表 + --steps/--skip-steps 选择性重跑 + depends_on 声明（cusip 失败则跳过 13F，fx 失败则跳过因子重建）。DAG 框架升级阈值写死：多主机、或步骤 >30 且有条件分支。
- **每日数据健康报告**：每表新鲜度 vs SLO、行数 delta、失败步骤、配额燃烧率，POST 到现有 STOCK_NOTIFY_URL，每天一条。
- **KeyRateLimiter 加计数器**：per-key 请求数/429 数/等待时长，run 收尾输出——撞 12h 超时墙前几周就能看到趋势。

### Later（半年+/触发式）

- **纸面交易通路**：先补两个口径裂缝——回测加 t+1 开盘执行变体（open 列已有）；SEC 数据周更 vs 回测日可见的 7 天裂缝（SEC 增量改每日，或回测 visible_date 推到下个周日）。然后每日 signal job + 目标权重 append-only 快照 + 实现/模拟收益漂移跟踪——检验整条 PIT 管线最诚实的方式。
- **宏观/利率层**：FRED+ALFRED vintage（宏观修订严重，不用 vintage 就是未来函数），等历史深度解决后体制条件化才有样本；T-bill 单序列可提前。
- **FTD 数量落库**：下载解析代码已存在（sync_cusip_identifiers），加一张表留下 fails 数量，与 short 系列组成完整做空压力特征族。
- **audit_recent_data 月度自动化**（小样本，月初周一分支）。
- **分钟数据决策预案**：一页文档（到达模式/保留期/查询形状/年增量），届时 Parquet+DuckDB vs CH 重新评估，避免压力下凭直觉重建刚拆掉的东西。

### 明确不做清单（与做同等重要）

分钟线（矩阵读取层回归前只是无法消费的存储负担）、期权链、分析师预期（无便宜的 PIT vintage 源，免费"当前预期"全是后视偏差）、实时行情、IFRS/ADR 扩展（维持已有搁置决策）；K8s/Prometheus 全家桶/Airflow/流复制 HA/PgBouncer/自建监控 UI。

---

## 三、建议执行顺序

**第一批（数据正确性，1–2 周）**：C2 复权链归一化（数学清晰、改动小、当下就在污染面板）→ C1 身份模型（先加"symbol 冲突但 FIGI 不同"检测与告警+存量对账脚本，再改 upsert 语义）→ H4/H5 调度补因子重建与 universe 同步（注意先有 C1 的检测再提频 universe 同步）→ H1/H2/H3 研究层三件（NaN 逃逸、filed_date 滞后一天、分红缺口剔除）→ H10 补回测引擎与 as_of SQL 的测试锁定（防止以上修复回归）。

**第二批（止损与防丢，并行可做）**：pg_dump 备份 + dead-man's switch + integrity 接入调度 + 退出码语义统一（H6）+ H7 insider 归属修复与存量回填。

**第三批（研究平台化，一季度）**：因子库骨架 → evaluate.py → 中性化原料 → 做空/13F 因子 → 回测引擎多空升级；同时推进 Massive 付费档评估（历史深度是研究价值的真正上限）。

---

*审查方法说明：所有 critical/high 发现经两个独立 agent（一个反驳立场、一个复现立场）验证；medium 经一个反驳立场 agent 验证；4 条经不起反驳的发现已剔除并在文中注明否决理由。低危项未经独立验证，采用时请先复核。*
