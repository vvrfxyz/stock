# 2026-07-02 深度审查报告（11 维度多智能体审查）

## 方法与规模

- 11 个审查维度并行深挖：身份与 PIT、复权链、因子库、回测评估、DB 写路径、调度运维、数据源适配器、测试质量、未提交改动、文档一致性、安全密钥。
- 审查前先汇编 6 份历史审计（06-11 / 06-12 / 06-13 / 06-23 / 06-30×2）+ 2 项有意搁置项（H9、部署脚本 #10）为「已知问题清单」，所有发现均排除已知项。
- 每个发现按严重级经 1~3 名独立对抗核查员反驳（过半反驳即淘汰）；完整性批判员提出的 5 条漏网线索再各经调查员 + 2 名反驳员核查。
- 总计约 160 个 agent、~10M tokens；因缓存恢复机制，部分核查员做了两轮独立抽样，两轮判定不一致的 9 项单独列为「边缘项」。
- 重点覆盖：2026-06-13 深审之后的新代码（第二批 5 因子、health_report）与当前未提交的 working-tree 改动。

**结论概览：高置信确认约 40 个独立问题（high 16 / medium 9 / low 14+），边缘待复核 9 项，查伪淘汰约 15 项。**
最集中的两个爆发点：**06-24 新增的 5 因子数据加载层**（4/5 有实质缺陷，评估结论多数不可信）与**未提交的 update_grouped_daily 改动**（2 个高危回归，不可原样提交）。

---

## 一、立即行动项（今天就该做）

### A1. 归档.zip 含全部线上 Massive API key，且未被 gitignore ⚠️
- 位置：仓库根目录 `归档.zip`（71MB，390 文件，约 06-23 的整仓快照）
- `unzip -l` 确认内含 `activation_value.txt`（659B，解析出 20 个 key）+ 约 190 个 `logs/*.log`。
- `.gitignore` 只忽略裸文件 `activation_value.txt`/`.env`/`logs/`，无任何 `*.zip` 规则——**一次 `git add .` 即把密钥永久写入 git 历史并随 push 公开**。git 历史已核查（149 commits，`--all --diff-filter=A`）：目前尚未发生。
- 处理：把 zip 移出仓库（或删除）；`.gitignore` 加 `*.zip` 兜底；长期建议 pre-commit 加密钥扫描（gitleaks 或简单 grep）。

### A2. 未提交的 update_grouped_daily 改动：先别提交，含 2 个高危回归
详见 §二 H2/H3。asof.py / trading_calendar / evaluate 等其余未提交改动经查是正确修复（§四），可拆分提交。

### A3. 非集成测试套件当前红灯（9 failed）
`pytest -m "not integration"` 当前 9 failed / 386 passed，全部集中在 `tests/test_insider.py`——pandas 3 的 ns/us 精度问题（同类修复已在 test_institutional/asof.py 落地，insider 漏掉）。红灯不修，CLAUDE.md 承诺的单元测试门禁失效，真回归会被噪声掩盖。
修法：fixture 中 `pd.to_datetime(...).astype("datetime64[ns]")`（同 tests/test_factors_asof.py:15 写法）；防御性地在 `load_insider_net_buy_panel` 内把 `effective_visible_date` cast 到 ns。

---

## 二、高置信 HIGH（两轮一致确认，多数经 3/3 反驳投票）

### 因子库 / 研究层

**H1. days_to_cover 前视泄漏 ~7-9 个交易日**（3 个维度独立发现）
`research/days_to_cover.py:69` 默认 `visible_delay_days=1`，而同一张 `short_interests` 表、同一个 `load_short_interest_events`（visible_date=settlement_date），`short_interest_ratio` 用的是 14 天兜底（`research/short_interest.py:134`，因 FINRA 半月报 BD+8 才公布）。新因子绕开了已落地的缓解措施；docstring "结算日 T+1 可见" 与 `pit_guarantee=True` 声明失实；docs/factors.md 第 70/72 行自相矛盾。**该因子全部已跑评估结论作废。**
修法：默认改 14（与 short_interest_ratio 共享同一常量），修 docstring/文档，重跑评估。

**H2. 13F 聚合 SQL 去重键错误，16% 持仓组被静默截断**
`research/institutional.py:59-84`：`distinct on (security_id, period, filer_cik)` 的键与下游 `per_filer` 的 group by 键相同 → 每组只剩 1 行，"per-filer 先求和"的 CTE 是死代码（已人工复核 SQL 确认）。13F informationTable 同一 CUSIP 按 discretion/voting authority 拆多行是常态：生产库实测 12.2M 组中 2.0M 组（16.4%）有多行，最大 1,226 行。total_shares/total_value 系统性低估、HHI 权重错、delta 在行拆分方式变化时产生虚假环比。**三个 13F 因子的值全部受污染**，且该 SQL 无集成测试（test_institutional.py 全 mock）。
修法：修正件去重改为 accession 级（先按 (filer_cik, period) 取最新 accession，再对 accession 内全部行求和），补 `-m integration` 测试。

**H3. 13F visible_date = 全体 filer 的 max(filing_date)：58-76% 季度聚合永不进面板 + 旧季度遮蔽新季度**
`research/institutional.py:87`：一份迟到数月的 13F-HR/A 会把整个季度聚合的可见日推到 amendment 日。后果：(a) merge_asof 按可见日选最新事件，旧季度可见日晚于新季度时面板回退到旧值（fundamentals.py:418-420 对同类问题有 period 单调守卫，这里缺）；(b) 生产库实测 2025Q1/Q2/Q3 分别有 73%/58%/76% 的聚合 visible_date−period ≥ 200 天，在 staleness=200 下**可见当天即过期，永远进不了面板**——这解释了 13F 因子覆盖率结构性偏低；(c) delta 因子的上季基数含未来修正值（前视）。
修法：改事件流语义（每份 filing 到达即发一条"截至该日的聚合"事件），或至少排除 amendment / 截断 visible_date 至 period+75d，并加 period 单调守卫。

**H4. insider_net_buy 多 owner 合并申报重复计数：32% 输入行是重复副本**
`research/insider.py:44-64`：解析层按 reporting owner 复制行（一笔联合申报 × N 个 owner），loader 直接 sum 全部 P/S 行无 accession 内去重。生产库实测：9.4% 的交易条目组有多 owner，贡献 32% 的行，单组最多 10 个 owner；重复恰集中在大股东基金的大额交易上，对 90 天滚动净买入是乘数级污染。
修法：读取层按 (accession, entry 字段组) 取 min(id) 去重；根治是解析层持久化 entry_index。

**H5. market_cap 拆股窗口错误传导进 size 与 earnings_yield**（批判员线索查实，量化后从 medium 升级）
`research/market_cap.py:132-143`：PIT 市值 = raw close × as-of 股本快照，无任何 SPLIT 校正。拆股 ex 日价格立即跳变、股本快照要等下一次 filing 才更新——期间市值恰错一个拆股比。生产库量化（回测窗口内）：629 起拆股落在市值非 NaN 的证券上，中位错误比例 **10x**（多为反向拆股），污染窗口中位 38 天、p90 83 天、132 起直到 400 天 staleness 才转 NaN。实证：DBGI 1:50 反拆后市值虚增 ~50x 持续 291 天。size/earnings_yield 直接消费该面板；反拆集中于困境微盘股，且恰在 ex 日越过 $3 门槛进入可交易集——偏差是系统性的。**size / earnings_yield 历史评估同样需要重跑。**
修法：用 corporate_actions 的 SPLIT 事件把股本快照滚动到观测日（shares × ∏ split_to/split_from）；update_massive_shares 增加"快照后有 SPLIT 则强制刷新"分支；补拆股跨快照的回归测试。

### 身份体系

**H6. 身份解析 CIK 分支缺 FIGI 冲突检查：同 CIK 新证券被自动判为 rename，劫持既有证券历史**
`utils/security_identity.py:211-240`：CIK 单候选（HIGH）与 exchange 消歧（MEDIUM）路径命中后从不校验 incoming FIGI 与既有行 FIGI 是否冲突（`_identity_conflict` 只在 ACTIVE_SYMBOL 分支用）。公司新上市第二类股 / 同 trust CIK 新发 ETF → 判 rename → `sync_massive_universe` 自动 `rename_security` + `upsert_security_info` 覆盖 composite_figi（不在 protected_fields）→ 既有证券全部历史被挂到新证券名下、原 FIGI 被抹掉，事后审计不可见（audit 只查同 FIGI/CIK 多行分裂）。每日调度自动路径。`test_cik_ambiguous_disambiguated_by_exchange` 把 symbol='brandnew' 判 rename 锁成了预期行为。
修法：CIK/HISTORY_SYMBOL 分支命中后调用 `_identity_conflict`；FIGI 冲突时降级为 NEW 或 ambiguous 交人工。

**H7. 13F CUSIP 身份桥无 PIT 语义：历史 FTD symbol 用当前 symbol 解析，错链永久且每周自动再生产**（批判员线索查实）
`scripts/sync_cusip_identifiers.py:60-114`：FTD 半月文件里的 (CUSIP, symbol) 是最多 ~3 个月前的历史观测，却用**当前** `securities.symbol` 快照解析（无 is_active 过滤、无时间维度、exact map 对重复 symbol 静默 last-wins，与 dotless 路径的歧义剔除不一致）。三个失败场景：改名+回收错链；**回收隔离期**（sync_massive_universe 只 quarantine 新公司、旧行保持 active 占 symbol，每周日新 FTD 把新公司 CUSIP 链到旧身份）；退市保号碰撞。现有三道守卫全部只防"一 CUSIP→多 id"，对"干净链到单个错 id"失明；`db_manager/reference_data.py:262-287` 只回填 NULL 行、错链永不重算；`update_institutional_holdings.py:97-104` 的写时映射连歧义守卫都没有。量级 <0.5%/年但错误无声、永久、恰集中在身份变动股上。
修法：FTD 匹配改 as-of（用 security_symbol_history）；剔除处于 RECYCLE quarantine 的 symbol；SEC_FTD 身份行写 start_date；audit 加反向校验 + 坏行修复脚本。

### 未提交改动（update_grouped_daily）

**H8. 按"当前 symbol→id 映射"upsert 任意历史日线：改名/回收证券的 bar 写进错误身份**（3 个维度独立发现）
未提交 diff 删掉了"只更新已存在行"守卫，改为无条件 `upsert_daily_prices`；`symbol_to_id_map`（:139-146）不过滤 is_active、无 ORDER BY、dict last-wins。回收场景（含 quarantine 期间）会把新公司行情插进退市旧身份并抬高其 price_data_latest_date；历史回填按今天的 symbol 归属历史日期。直接违反"symbol 绝不当持久键"。新增测试反而把守卫移除钉成规范，危险分支（同 symbol active+inactive、历史归属）零覆盖。
修法：映射至少限定 is_active=True + 重复 symbol 告警剔除；历史回填用 security_symbol_history 做按日 PIT 解析（注意先修 H10）；补两条危险分支测试。

**H9. 对 price_data_latest_date 为 NULL 的证券盖戳，永久关闭全量回填通道**
`ensure_security_price_latest_date_at_least` 的 WHERE 含 `IS NULL` 分支；零历史证券一旦被 grouped daily 先写入几天并盖戳，update_massive_prices 的"NULL → 从 history_floor 全量回填"唯一自动入口永不触发，730 天窗口留永久缺口，只能人工 --full-refresh。
修法：对 latest_date 为 NULL 的证券跳过盖戳（或给函数加 only_if_not_null 语义）。

### 数据源适配器（SEC EDGAR）

**H10. 13F daily form index 把 SEC 限流 403 当"非工作日"静默跳过整天**
`data_sources/sec_edgar_source.py:277`：403（限流封禁/UA 不合规）与 404 一并返回 None，不重试（403 不在 Retry status_forcelist）、无 warning、exit 0。周日 --since 只回看 14 天，一次封禁跨两次周跑即**永久静默丢失**这些天的 13F filing 发现。与 06-30 覆盖事故同类，覆盖健康检查至今未落地。
修法：只把 404 当"未发布"；403 检查响应体限流签名，命中则退避重试或失败退出；对交易日返回 None 记 warning + 与 trading_calendars 对账。

**H11. 13F primary_doc 解析失败 → period=NULL 入库，filing 永久不可见且不再重试**
`sec_edgar_source.py:641`：primary_doc XML 损坏而 informationTable 正常时，行以 period=NULL 写库；filter_pending 按 accession 判"已完成"永不重解析；消费端 SQL 硬过滤 `period is not null`。三重静默（写入成功、exit 0、pending 收敛），比已知 SUCCESS_EMPTY 更糟。已本机复现。
修法：period 缺失时从 SGML 头 `CONFORMED PERIOD OF REPORT` 回填（零额外请求）；仍缺则记失败不写库，留在 pending。

### 回测评估

**H12. run_baselines 终局敏感性死代码必然 TypeError 崩溃**
`research/run_baselines.py:149`：`run_backtest(r.name, r.equity * 0 + 1, ...)` 把 equity Series 当权重矩阵传入，`weights.reindex(columns=...)` 对 Series 直接 TypeError（pandas 3.0.3 实测）。include_inactive=True 下 has_terminal 非空是常态路径 → main 在打印回测表后、保存 CSV 前崩溃。
修法：删除该行死代码（stress 变量本就从未使用）。

**H13. _pit_regression 对"值出现/消失"型前视完全失明**
`research/evaluate.py:609`：presence 不匹配（live 有值、as-of 重放应为 NaN）产生 NaN diff 被 nanmax 跳过 → pit_diff=0.0 完美通过。--strict 与 lookahead_suspect 对"数据提前可见"类泄漏系统性假阴性——恰是 06-30 13F 事故那类 bug 想防的场景。已实测复现。
修法：`recomputed.notna() != live.notna()` 显式计入违规（pit_presence_violations 计数）。

### 安全与密钥（均已实测复现）

**H14. loguru 默认 diagnose=True：traceback 变量值标注泄漏密钥**
`utils/script_logging.py:30-35,44`：两个 sink 均未传 diagnose=False（loguru 官方明确警告生产环境须关）。异常穿过 `massive_source._request_json` 栈帧时，`request_params -> {'apiKey': '<明文>'}` 直接进日志；db_url（含密码 DSN）、fred params 同理。全仓 30+ 处 `logger.opt(exception=e)` 都是出口。
修法：两处 `logger.add` 显式 `diagnose=False`（backtrace 可酌情保留）。

**H15. 异常链绕过 apiKey 掩码：`raise ... from exc` 携带未脱敏原始异常**
`data_sources/massive_source.py:244`：重试耗尽后新 RuntimeError 消息已掩码，但链上的原始 ConnectionError 消息含 `?apiKey=<明文>`，traceback 渲染（与 diagnose 无关）即落日志。另外 except 面只有 (ConnectionError, Timeout)，其他 RequestException 完全不经掩码。
修法：`from None`（或净化 exc.args 后再链）；except 面扩到 RequestException。

---

## 三、高置信 MEDIUM / LOW（摘要）

**Medium：**
1. `research/insider.py:56` — insider_net_buy 未过滤 DERIVATIVE 表：26k 行期权/权证 P/S 按"股数"混入，买 put 计为看多。修法：`security_type='NON_DERIVATIVE'`。
2. `research/institutional.py:87` — 13F 聚合用最终 vintage 而非 as-of vintage，缺 fundamentals.py 同款 period 单调守卫（与 H3 同根）。
3. `db_manager/securities.py:332` — security_symbol_history 区间语义损坏：两套矛盾的 start_date 写法、end_date 全表永不闭合、resolver 命中多行历史 symbol 时任取第一行。**注意：这是 H8 正确修法（PIT symbol 解析）的前置依赖。**
4. `scripts/health_report.py:247-266`（未提交）— 13F 覆盖检查对在途季度必然误报 P1，每季度约 6 周退出码常红；EDGAR 畸形 period 会以 filings=1 永久占据 LIMIT 4 名额。新测试恰未覆盖这两种场景。修法：只对申报截止已过（period+60d）的季度施加阈值。
5. `research/evaluate.py:502` — IC/IC-decay 不做 eligibility 过滤，与分位回测横截面系统性不一致——已记录的"IC 与 q5-q1 方向冲突"的一个具体候选机制。
6. `tests/test_health_report.py:20` — 假 Session 不执行 SQL，防不住本仓已两次复发的"health_report 列名错误"缺陷类；建议补一条 pg integration 用例。
7. `AGENTS.md` — 指导 agent 用不存在的命令（daily_run/update_details）、宣称无测试套件；按它行事的 agent 会绕开最重要的回归防线。
8. `README.debian.md:21` 等 4 处 — scheduled_update 步骤描述与实现漂移（含"每天全量重建复权因子"的方向性错误）；仅根 README.md 准确。
9. `docs/README.md:8,42` — 因子数停在 4 个；"金融报表不抓取"与 sec_fundamental_facts 链路矛盾。

**Low（点列）：**
- `scripts/sync_massive_universe.py:102` — rename 链按 feed 顺序处理，占用冲突抛 ValueError 使同步半提交中止且每日复现。
- `research/data.py:193` — uncovered events 按日期级匹配，同日另一事件的因子行掩盖被跳过事件。
- `research/evaluate.py:642` — 剔除窗口 [start,end] 与缓冲面板 [start,end+horizon] 不一致（仅历史窗口评估暴露）。
- `db_manager/securities.py:115` — is_active 显式 None 时 setdefault 失效，NULL 行绕过部分唯一索引反复插入（PG 已复现）。
- `utils/massive_task.py:168` — stats 从未写入 pipeline_task_runs，观测链路断开。
- `scripts/health_report.py:74` — except 分支 `issues += 1` 写两遍，P1 虚高一倍（ab7527f 残留）。
- `main.py:94` — 不支持的市场打 CRITICAL 后退出码 0。
- `data_sources/massive_source.py:310` — HTTPError 日志输出未脱敏 response.text。
- `tests/test_trading_calendar.py:35` — 依赖缺失时静默 return 通过而非 pytest.skip，唯一的时区回归锁会静默失效。
- `scripts/update_grouped_daily.py:49`（未提交）— --end-date 未钳制到最近已完成交易日。
- `scripts/backup_postgres.sh:83` — DATABASE_URL 放 pg_dump 命令行参数（ps 可见）；set -a source .env 导出全部秘密进子进程环境。
- `CLAUDE.md:57` — 表清单缺 risk_free_rates（22/23）、命令缺 update_risk_free_rates；identity 事件类型三处文档口径不一致（5/6/7 种）。
- `docs/factors.md:94` — institutional 加载器描述与代码不符（不存在的 dict 结构与缓存复用；实际同一重 SQL 被执行 3 次）。
- `README.md:87` — 引用不存在的测试路径 test_scheduled_update_steps_*；deployment.md 测试计数停在 67（实际 478）。
- `scripts/update_adjustment_factors.py:367-375` — 未来 ex_date 分红因子按公告价落库本身无害（as-of 归一化精确相消 + 每日重触发自愈），**但自愈依赖"upsert 无条件刷 updated_at"这一与 --changed-since 文档意图相反的意外行为**——若日后给 upsert 加变更检测，此处会变成真 bug。建议把选取条件扩为 `updated_at >= cutoff OR ex_date 刚过`。

---

## 四、边缘项（两轮核查判定翻转，建议人工定夺）

| # | 发现 | 位置 | 两轮判定 | 倾向 |
|---|------|------|---------|------|
| B1 | upsert_securities_by_symbol：identity event 失败毒化共享事务（声称 PG 端到端复现） | db_manager/securities.py:181 | 确认→反驳 | 复现声明具体，倾向真；修法便宜（SAVEPOINT，同 d84b213） |
| B2 | 同上：缺批内 symbol 去重，大小写变体 CardinalityViolation（声称 PG 复现） | db_manager/securities.py:202 | 确认→反驳 | 同上，fbee3ff 是先例 |
| B3 | trials store 并发追加无锁 + 固定 tmp 名可写坏 parquet | research/_trials_store.py:197 | 确认→反驳 | 单人单进程用法下概率低；tmp 名加 pid 是一行修 |
| B4 | trial_id 去重不含 code_git_dirty：脏树改引擎重跑，新结果被静默丢弃 | research/evaluate.py:356 | 反驳→确认 | 当前工作区正是 dirty 状态，值得修 |
| B5 | 被 kill 的 pipeline 步骤永久 RUNNING，health_report 视为健康 | scripts/health_report.py:175 | 反驳→确认 | 修法便宜：RUNNING 超过 N 小时计 P1 |
| B6 | days_to_cover 分子分母拆股口径错位（SI 旧股数 vs 成交量新股数） | research/days_to_cover.py:97 | 确认→反驳 | 机制真实存在；量级评估分歧。修 H5 时可顺带 |
| B7 | FRED api_key 无掩码进 critical 日志 | data_sources/fred_source.py:44 | 确认→反驳 | 低价值凭证；修 H14/H15 时顺带共享掩码 util |
| B8 | main.py update_adjustment_factors 未暴露 --changed-since 等 3 旗标 | main.py:897 | 确认→反驳 | 文档-入口不一致属实，定级之争 |
| B9 | CHANGELOG.md 停更于 06-23 | CHANGELOG.md | 确认→反驳 | 事实属实，是否算问题看维护策略 |

## 五、查伪/澄清（重要的"好消息"）

1. **未提交的 asof.py 改动是正确修复，不是回归**——与 06-30 postmortem 第 96-105 行的修复方案一一对应；旧语义（并列时"因子值最大者胜"）才是缺陷。可放心提交。
2. **未提交的 trading_calendar.py 改动正确**——修掉"上海时间凌晨把未收盘的美股当日算已完成"的前视，且有针对性测试（但见测试的静默 pass 问题）。
3. **institutional_holdings.value 的 2023-01 千美元/美元单位断点当前不触发**——n_holders 是计数、HHI 同期内标度不变、delta 用 shares，且 2024-05-14 信任下限使窗口内 period 全在换制后。专项核查过，无需处理。
4. **insider P/S 分层本身正确**（A/G/F 未混入）；旧 4 因子除 market_cap 传导外未发现新缺陷。
5. **未来 ex_date 因子落库对读取无害**（as-of 归一化分子分母精确相消），仅存脆弱耦合（见 Low 最后一条）。
6. **universe.py type 过滤 PIT 盲**为潜伏项：机制属实但 evaluate 主管线不用 universe.py，价格驱动掩码独立覆盖幸存者偏差。记录备查。
7. 复权链主干、db_manager 冲突键（8 表逐一对照）、调度失败隔离、限流器、integration 标记纪律——均验证健康。

## 六、建议修复顺序

1. **今天**：A1（归档.zip）→ A3（测试红灯）→ H12（一行删除）→ H14/H15（安全，各一两行）。
2. **提交前**：拆分当前工作区为三个提交（asof/evaluate/trading_calendar 修复 ✅；grouped_daily 需先修 H8/H9；health_report 需先修在途季度误报）。
3. **本周（因子可信度）**：H1（一行改默认值）→ H2（重写聚合 SQL）→ H4（去重）→ insider 衍生品过滤 → H3（事件流语义，工作量最大）→ H5（拆股校正）。**修完后 days_to_cover、3 个 13F 因子、size、earnings_yield 的历史 trials 全部标记作废重跑。**
4. **本周（身份与数据完整性）**：H6 → H7 → H10/H11 → symbol_history 区间语义（M3，为 H8 正确修法铺路）。
5. **随后**：Medium 文档批次（AGENTS.md 优先，它会误导自动化 agent）、Low 批次、边缘项逐个定夺。

## 七、与历史审计的关系

- 06-13 深审的 2 项有意搁置（H9 migrate_database、部署脚本化）未重复审查，维持搁置。
- 本次全部发现均不与 6 份历史审计重复（核查员逐条比对）；唯一判为已知重复的 1 项（同日拆股+分红复合基准）已排除。
- 值得注意的模式：**本次 16 个 high 里有 10 个出自 06-13 之后的新代码或未提交改动**——存量代码经三轮审计已相当干净，新增代码是缺陷主要来源。建议新因子/新脚本合入前跑一次专项审查（尤其 PIT 可见性与身份归属两类）。
