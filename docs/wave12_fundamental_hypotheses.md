# Wave-12 预注册假设清单：基本面质量/盈利族（2026-07-08，跑数前写死）

**纪律**：每因子先写"为什么该有效"+ 预注册方向 + 构造式 + 死刑标准，全跑 2012-01-01 ~
2026-07-02 长窗（XBRL 2009+，留 TTM+270 天新鲜度暖机；panel start 2011-01-01），
family=wave12_fundamentals 统一记账。显著性门槛用跑数当日 `trials report` 的**动态
Bonferroni z 阈值**（W0-P3 口径，study 行不计分母）。过线者进三关卡（size/low_vol/high_52w
partial IC，任一关衰减 >2/3 判马甲）+ 栖息地诊断（size_neutral 桶内 IC）；全部存活才排
retail_reality 散户复审。禁止事后在网格里挑格子；单一预注册参数，不调参。

评估口径：P2 新退市口径（`ic_delisting_returns` 默认开，IC 与分位路径同注入实测
delisting_return）；宇宙 CS-only 默认；horizons (1,5,10,21)。

## 为什么现在打这一族（出发点）

- ledger 开放问题：价格族 wave 1-11 完整结案——排序信息真实但散户不可收割
  （residual_vol、low_vol、composite_v1 三连 FAIL 同一死法）；earnings_yield 是唯一
  长窗过线的非价格因子（2011+ h1 t=5.31，探索性 trial）。质量/盈利族与价格族口径
  不同源，是没动过的矿。
- 数据前提已摸底（2026-07-07）：5 个候选所需 XBRL 概念在 `utils/sec_concepts.py`
  白名单 100% 就绪，**不触发 bulk-zip 回填**；`fundamentals.METRICS` 现成面板覆盖大半。
- 文献先验：这一族在美股的发表后衰减普遍比价格异象温和（Novy-Marx 2013 的 GP 与
  FF-2015 的 RMW 至今活在因子模型里），但 2012+ 大盘制度下小盘价值/质量普遍承压——
  诚实的先验是"存活 1-2 个、其余死"。

## H1 gross profitability（Novy-Marx 2013）——毛利是最干净的盈利测量

- **构造**：GP/AT = gross_profit_ttm / assets。分子优先直报 `gross_profit_ttm`；
  缺失时用 `revenue_ttm − cost_of_revenue_ttm` 兜底（**跨 metric 对齐规则**：仅当两腿
  as-of 事件的 period_end 一致才做减法，否则置 NaN 回退——TTM 窗口错位的差不是毛利）。
  分母 assets 取 as-of 最新时点值。
- **预注册方向**：正（高毛利 → 高后续收益）。
- **预测**：文献效应在质量族里最强韧；若 2012+ 美股仍有质量溢价，此处最先看到。
- **死刑**：全 horizon |t| 低于动态 Bonferroni 阈值，或方向为负。

## H2 accruals（Sloan 1996）——应计项是盈利质量的反指标

- **构造**：现金流量表口径 (net_income_ttm − operating_cash_flow_ttm) / assets。
  三个量全部现成 METRICS；NI 与 CFO 两腿同为 TTM，锚同一 as-of 面板。
- **预注册方向**：**负**（高应计 → 低后续收益；盈余里"纸面"成分高的公司随后变脸）。
- **预测**：Sloan 效应发表最早、被套利最久，2012+ 大概率衰减；但其空头腿住在小盘
  ——恰好是我们的栖息地，桶内 IC 可能比全截面强。
- **死刑**：同 H1（注意方向为负——正显著同样算死，那是"应计溢价"翻转，不入账为发现）。

## H3 operating profitability（EBIT 口径，Fama-French 2015 变体）

- **构造**：operating_income_ttm / assets。**明确非 FF-2015 原口径**（原口径要扣利息
  费用，`InterestExpense` 不在白名单——本轮不扩白名单不回填，EBIT 口径绕开）。
- **预注册方向**：正。
- **预测**：与 H1 高相关（都是盈利能力测量）；若 H1/H3 双过线，三关对照之外互做
  partial——留一个即可，预注册优先留 H1（测量更干净，Novy-Marx 的原始论证）。
- **死刑**：同 H1。

## H4 asset growth（Cooper-Gulen-Schill 2008）——扩张的公司随后跑输

- **构造**：Assets_t / Assets_{t-1y} − 1，事件层 YoY（**PIT 规格预注册**：两腿锁同
  concept；YoY 事件 visible_date = max(两腿 filed_date)；任一腿新 vintage（重述）即
  重发事件；不用"as-of 面板取 t−252"近似——那混入申报时点噪声）。需要先做"同 metric
  上一年度期值"事件层机制（本战役唯一非平凡工程件），带重述金样本测试
  （仿 tests/test_shares_pit.py 的 AAPL 拆股锚定写法）。
- **预注册方向**：**负**（高资产增长 → 低后续收益）。
- **预测**：文献里横截面最强的"投资"因子；但它与 size 的相关未知，三关对照的 size
  关是主要生死关。
- **死刑**：同 H1；若 size partial 衰减 >2/3，记"size/成长制度马甲"。

## H5 F-score 子集（Piotroski 2000 组件化）——排最后，条件启动

- **构造**：9 组件中先量化覆盖率，取覆盖 >80% 的子集等权求和（组件符号按原文）。
  Δ 类组件（ΔROA/Δ杠杆/Δ毛利率/Δ周转）复用 H4 的上一期值机制。
- **预注册方向**：正。
- **启动条件**：仅当 H1-H4 至少一个过线才做（复合分的前提是成分有肉；全灭时 F-score
  大概率也是零，省下窗口）。组件子集在跑数前根据覆盖率钉死并提交，不做子集搜索。
- **死刑**：同 H1。

## 逐因子流水（每个都走完才动下一个）

evaluate（2012+，P2 新口径，family 记账）→ 过动态阈值 → factor_correlation 三关
（size/low_vol/high_52w partial IC）→ size_neutral 栖息地诊断 → 回写 ledger。
全部存活者进 composite_v2 成分池（W5-6）；**全灭也是合法结局**——ledger 记
"质量族在本市场 2012+ 无横截面增量"，纸面组合排期照走（用现有调味料成分）。

## 不做的（防网格挖矿）

- 不扫参数（TTM 窗口、标准化分母 assets vs equity、winsorize 档位——全部单一预注册值）。
- 不做行业中性化版本（另立项才做；先看裸信号）。
- 不做 earnings_yield × 质量的交互（composite_v2 阶段的事，且须以两者独立存活为前提）。
- 不扩 `InterestExpense` 白名单（FF-2015 原口径 operating profitability 冻结，
  解冻条件 = H3 EBIT 版过线且三关后仍需更干净的口径分辨）。
- ADR 不进宇宙（CS-only；分子分母同源 sec_fundamental_facts 的因子不设 adr_unsafe，
  但宇宙口径不变）。
