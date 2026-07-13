# 技术分析下一阶段研究路线图（2026-07-12）

> **状态更新（2026-07-13）**：Wave 13/14/15 已按本文预注册执行完毕并全部 FAIL 结案
> （裁决入 docs/research_ledger.md 因子表；停止条件生效，禁近义变体复活）。
> 本文其余内容保留为立项时的事前分析记录；执行顺序表中 1-3 已完成，
> 下一条技术线为 Wave 16，但当前主攻已切换为 owner 交易形状的
> concentrated_topk 部署研究（见 ledger 开放问题 #1）。§"条件路线：强基本面
> 候选池中的技术 timing"的解锁前提（Wave 13/15 存活信号）已确认不成立。

## 技术摘要

结论不是“技术分析无效”，而是研究对象必须换一层：

1. **停止继续扩经典指标动物园。** RSI、MFI、MACD、布林、OBV、A/D、唐奇安、SMA gap、
   ATR 归一化趋势，以及它们的反转集成、量能确认和锚定门控，已经在 2016-2026 长窗中
   完成裁决。经典指标作为**日频横截面选股分数**没有新的独立信息。
2. **横截面 rank-IC 不是技术分析的唯一正确问题。** 当前尚未充分研究的是：
   - 时序择时与市场暴露管理；
   - 价格路径质量、市场残差趋势，而非终点收益或单个指标；
   - 事件触发型结构，如财报缺口、放量确认、突破后的条件收益；
   - 同日盘中结构，而非用收盘到收盘收益稀释分钟信号。
3. **优先开三条线：**
   - Wave 13：残差动量 + 趋势路径质量；
   - Wave 14：财报缺口 × 成交量确认的事件研究；
   - Wave 15：市场趋势 + breadth 的风险暴露覆盖层。
4. **先补一项方法论红线。** 当前 `trials report` 按单因子的 trial 数计算动态
   Bonferroni，新因子只跑一次时阈值为 1.96，但一次 trial 同时检查 4 个 horizon，技术规则
   还会跨多个变体。新研究应按预注册的 `family × variant × horizon` 计数；技术因子发现级
   结论暂以 `|NW t| >= 3.0` 为最低门槛，策略 P&L 另做 White Reality Check / Hansen SPA
   或等价的全规则校正。

## 本报告要回答的问题

目标不是再列一批“可以算的指标”，而是决定下一轮研究资源投向哪里：

- 哪些方向没有被现有 12 波研究覆盖；
- 哪些方向有较强文献先验，同时能被当前数据严格验证；
- 哪些方向符合技术分析偏好，但不会重复 RSI/MACD 型研究；
- 每条线的最小可证伪假设、成本口径、基线和停止条件是什么。

优先级评分是研究决策启发式，不是收益预测。评分考虑五项：文献证据、与已死亡因子的
正交性、数据就绪度、交易成本可实现性、技术分析契合度。

## 现有研究边界

### 已经做得足够多

`home-debian`（253）截至 2026-07-09 的机器台账包含：

| 项目 | 数量 |
|---|---:|
| 注册并评估的因子 | 42 |
| evaluate trial | 87 |
| horizon 级 IC 检验 | 348 |
| 部署/变现 study verdict | 30 |
| trials 明细行 | 23,128 |

现有结果已经覆盖：

- 经典趋势：12-1 动量、52 周高、SMA gap、MACD、Donchian、ATR trend；
- 经典反转：21 日反转、RSI、MFI、Bollinger、反转集成、放量确认反转；
- 量价：OBV、A/D、MFI、收盘-VWAP 压力；
- K 线/高阶矩：影线不对称、MAX、已实现偏度、signed jump；
- 日内结构：首尾半小时、尾盘压力、尾盘量占比、隔夜/盘中分解；
- 价格族冗余：size、low-vol、52 周高三关，以及栖息地和真实成本裁决。

这批结果支持一个稳定结论：**技术指标最有价值的用途更可能是状态、事件和执行，
而不是继续作为无条件横截面排序分数。**

### 尚未被当前结论关闭的边界

| 未覆盖问题 | 为什么现有负结果不能替代它 |
|---|---|
| 市场/个股时序择时 | `evaluate.py` 测横截面 IC；它没有回答“同一资产何时持有或降仓” |
| 残差动量与路径质量 | 现有动量看 12-1 终点收益，高点因子看距 52 周高；都没有刻画残差路径或信息连续性 |
| 财报缺口事件 | 日频 rank 会把稀疏事件摊薄；事件研究应在公告样本上直接测 CAR 和 gap-fill |
| 同日市场 intraday momentum | 现有日内因子预测次日横截面收益，不是首半小时预测尾半小时的市场时序问题 |
| 市场 breadth 风险覆盖层 | 13F `institutional_breadth` 是持有人数因子，和价格 breadth 完全不是一件事 |
| 季节性 | 现有研究没有 same-month、turn-of-month 或 overnight/intraday calendar structure |
| 行业内 lead-lag | 只登记为开放问题，且历史行业分类仍不满足 PIT 研究要求 |

## 数据与工程可行性

截至 2026-07-12 的只读核对：

| 数据资产 | 覆盖 | 对下一轮的意义 |
|---|---|---|
| 日线 raw bars | 33.61M 行、18,371 只证券；研究可信下限 2003 | 所有日频路径、breadth、gap、seasonality 可直接做 |
| 普通股 universe | 11,367 只 CS，其中 5,308 只活跃 | 可构建 PIT 上市/退市 universe，避免当前成分回看 |
| 分钟 bars | 5.150B 行、18,112 只证券，2003-09 至 2026-07 | 可做 market intraday、opening/closing execution 研究 |
| 分钟日聚合特征 | 28.39M 行、17,696 只证券 | 已有首尾半小时收益、尾盘量、RV、偏度、有效价差 |
| 8-K Item 2.02 业绩公告 | 235,335 个事件、6,574 只证券，2004-08 起，全部有 `accepted_at` | 财报缺口的首选事件锚；比 10-K/10-Q 更接近信息首次公开时点 |
| 10-K/10-Q filing | 182,955 个事件、4,455 只证券，全部有 `accepted_at` | 用于确认财期和报告类型，不默认等同于业绩首次公告 |
| 无风险利率 | 1954 至 2026 | 时序策略空仓可按 DTB3 计息，而不是默认现金收益为 0 |
| SPY 日线 | 2001 至 2026 | 市场趋势、CAPM、危机期和暴露覆盖层基准就绪 |
| 当前 SIC | 2016-2026 有价 CS 仅 49.8% 覆盖，且读取当前分类 | 行业 lead-lag 暂不能给出无幸存者/PIT 结论 |
| 新闻 | 仅 2026-05 起 | 不能用于长窗新闻过滤或文本技术因子 |

一个需要明确记录的来源冲突：Mac 本地 `trials.parquet` 仍是 73 trial 的旧快照；
`home-debian`（253）上的权威台账是 87 trial。后续裁决应继续以远端机器账为唯一台账，
避免本地旧文件低估多重检验分母。

## 研究优先级

| 排名 | 方向 | 优先指数 | 状态 | 主要理由 |
|---:|---|---:|---|---|
| 1 | 残差动量 + 趋势路径质量 | 90 | 立即启动 | 文献强、数据全、与“终点动量/52 周高”不是同一测量 |
| 2 | 财报缺口 × 成交量确认 | 88 | 立即启动 | 事件工具和 23.5 万个 8-K 2.02 事件已就绪，低频、可直接测可交易路径 |
| 3 | 市场趋势 + price breadth 覆盖层 | 85 | 立即启动 | 技术分析更稳健的角色是风险管理；可用 2007+ 危机样本检验 |
| 4 | 市场同日 intraday momentum | 76 | 第二批 | 与现有次日横截面研究正交，但需要专用分钟回测和严格成本 |
| 5 | 横截面季节性 | 72 | 第二批 | 长历史、低换手、与价格主干可能正交，但更接近日历效应 |
| 6 | F-score/OP 候选池 + 技术入场/退出 | 68 | 条件启动 | 先用强基本面缩小候选，再让 TA 做 timing；须防止事后救因子 |
| 7 | 行业内大盘领先小盘 | 61 | 数据阻塞 | 文献先验强，但当前 SIC 只有约半覆盖且不是 PIT |
| 8 | 波动收缩后的突破事件 | 50 | 探索性 | event 口径未测，但现有 Donchian/ATR 负结果使先验偏低 |
| 9 | ML 自动组合 TA 特征 | 32 | 暂缓 | 当前样本与试验纪律下，非线性搜索最容易重新制造指标动物园 |

## Wave 13：残差动量与路径质量

### 为什么排第一

当前 `momentum_12_1` 被 `high_52w` 吸收，只说明“总收益终点”没有增量。文献提出了两个
不同问题：

- **Residual momentum**：先去掉市场或共同因子收益，再看个股自身残差是否持续；
- **Frog-in-the-pan / information discreteness**：相同累计收益下，缓慢连续形成的趋势
  和少数跳跃形成的趋势，后续持续性不同。

它们都可能在 `high_52w` 之外留下信息，也都可能被现有三关直接证伪。

### 预注册最小设计

**Family：`path_momentum`，只允许两个变体。**

1. `residual_momentum_12_1`
   - 用过去 252 日滚动市场模型得到日残差；
   - 累计 `t-252` 到 `t-21` 的残差收益；
   - 月度更新，方向为正。
2. `information_continuity_12_1`
   - 严格复现 Da-Gurun-Warachka 的 information discreteness 定义；
   - 在相同 12-1 formation return 内比较连续形成与跳跃形成的趋势；
   - 不额外搜索窗口、阈值或替代定义。

**样本：** 2007-07 至 2026-07；2016-2026 为主裁决窗，2007-2015 作为前期稳定性腿。

**基线与关卡：**

- 原始 `momentum_12_1`、`high_52w`、`low_vol`；
- partial IC 必过 size / low_vol / high_52w；
- 栖息地按市值三桶；
- 月频 q5 长腿和 q5-q1，成本 10/25/40bps 单边压力档；
- 主结果必须在 2016-2026 与至少两个独立子期同号。

**发现判据：** 预注册方向 `NW t >= 3.0`，且对三关中最强吸收者残差化后至少保留原始
IC 的 1/3；否则记为已知价格主干的马甲。

**停止条件：** 两个变体均不过发现关，或都被 `high_52w/low_vol` 吸收，则整个
“趋势路径修饰”家族结案，不继续尝试 slope、R2、efficiency ratio 等同义变体。

## Wave 14：财报缺口与放量确认

### 为什么它比继续测形态更合理

稀疏事件用全市场每日 rank-IC 会被大量无事件证券稀释。仓库已经有精确到秒的 SEC
`accepted_at`、8-K Item 2.02 标签和事件对齐工具，可以把问题改成：**市场对业绩公告
产生缺口后，什么样的价格/成交量结构继续漂移，什么样的结构回补？**

这里不需要分析师一致预期。开盘缺口本身是市场对新信息的聚合反应，成交量用于区分
信息确认和流动性噪声。

### 预注册最小设计

**Family：`earnings_gap`，两个预注册交互。**

1. `gap_atr`
   - 事件可见后的首个交易日；
   - `gap = open / previous_close - 1`，除以 20 日 ATR；
   - 正缺口预测正向 CAR，负缺口预测负向 CAR。
2. `gap_atr_volume_confirmed`
   - 在 `gap_atr` 上乘事件日成交量 / 20 日中位成交量，封顶 3；
   - 不搜索 1.5x、2x 等离散门槛。

**事件样本：** 8-K/8-K-A 中 Item 2.02 为主事件，修正件单列，不和原件混合；10-K/10-Q
只用于确认财期和报告类型，不重复登记为独立公告。当前库有 235,335 个 2.02 事件，明显
多于 182,955 个 10-K/10-Q filing。事件可见时点必须改用项目交易日历，不能继续使用
`events.py` 的 weekday 简化后直接终审。

**结果变量：**

- 公告日 close、t+1、t+5、t+20 的市场调整 CAR；
- gap-fill 概率、最大有利/不利路径；
- 事件 gap 与同幅度非事件 gap 的匹配对照；
- 大小盘、流动性、盘前/盘中受理时点分层。

**可交易口径：** 第一版只允许事件日收盘建仓、持有 1/5/20 日，避免声称捕获已经发生的
开盘跳空。全样本报告毛 CAR 和 10/25/40bps 单边固定成本敏感性；有 measured spread 的
子样本另报逐股净结果。缺失价差不做静默填补，并披露实测子样本相对全样本的覆盖偏差。

**判据：** volume-confirmed 变体必须在全部主要 horizon 同向增强，且净后 CAR 相对裸 gap
至少提升 25%；只在某一个 bucket 或某一个 horizon 成立则不算机制确认。

**停止条件：** 事件 gap 与匹配非事件 gap 无差异，或所有增量只来自公告日当日而收盘后
不可交易，则该方向转为执行研究，不登记为持有期 alpha。

## Wave 15：市场趋势与 price breadth 覆盖层

### 研究目标

这条线不以“发现新的选股因子”为目标，而是回答：**简单、透明的技术状态能否降低现有
组合的尾部风险，并在现金计息和完整成本后提高风险调整收益？**

现有 `sma_50_200` 基线是在个股层筛选后等权持有，不是市场择时。2011-2026 它的 CAGR
约 11.3%、Sharpe 0.656、最大回撤 -41.4%，同期 SPY 为 14.0%、0.855、-33.7%。这组结果
已经否决“个股 50/200 均线等权就是答案”，但没有检验市场级 exposure overlay。

### 预注册最小设计

**Family：`market_regime_overlay`，三个规则，不优化参数。**

1. `spy_10m_trend`：SPY 月末总收益价高于 10 月均线时满仓，否则现金；
2. `breadth_200d`：PIT eligible CS 中高于 200 日均线的比例 >50% 时满仓，否则现金；
   不足 200 个有效交易日的新股暂不进入分母；
3. `trend_and_breadth`：前两者同时成立时满仓，否则 50% 组合 + 50% 现金。

**底层资产：** 先测 SPY、PIT CS 等权基准；只有规则在两者都稳定，才叠加到未来纸面组合。

**现金与成本：** 现金赚 DTB3；月末调仓；ETF 成本 1/2/5bps 单边；股票组合按实际换手
报告 10/25/40bps 固定成本敏感性，实测价差覆盖样本另报，不填补缺失值。

**评价：** CAGR、Sharpe、Sortino、最大回撤、Expected Shortfall、down-capture、错杀天数、
换手，以及 2008、2020、2022 的独立危机表现。风险覆盖层不以 rank-IC 裁决。

**PASS：** 相对满仓基线最大回撤至少改善 10 个百分点，Sharpe 至少提高 0.10，且 CAGR
损失不超过 2 个百分点；三个条件须在 2007-2015 和 2016-2026 两段均不出现符号翻转。

**停止条件：** 仅靠 2008 单次危机改善，或 2016+ 长期现金拖累吞掉全部效用，则简单趋势
覆盖层结案，不升级 HMM、change-point 或机器学习 regime。

## Wave 16：市场同日 Intraday Momentum

现有 `last30_persistence`、`eod_reversal` 研究的是个股横截面信号对次日收益的关系；
Gao-Han-Li-Zhou 的问题是**市场首半小时收益是否预测同日尾半小时收益**，两者不重合。

最小研究应只从 SPY 和一个 PIT 市值加权市场组合开始：

- 09:30-10:00 的收益决定 15:30-15:59 的方向；
- 主检验使用连续信号，交易检验只用 `sign(first30)`，不搜索阈值；
- 2003-2018 做复现，2019-2026 做真正的发表后检验；
- 成本按 1/2/5bps 单边，并明确分钟 bar 不含 16:00 auction print；
- 只有 SPY 与市场组合都成立，才扩展到行业或个股。

这条线有较高研究价值，但不应先做全市场逐股 intraday scalping：当前没有 quote 级 bid/ask，
bar-based spread 适合成本校准，不足以支持对极薄的分钟 alpha 做强结论。

## Wave 17：低换手价格季节性

同月季节性是价格历史中的周期结构，不是基本面信号。它与当前的 trend/reversal 指标家族
可能正交，并且月频换手天然适合小本金。

约束：

- 第一版严格复现 Heston-Sadka 的 same-calendar-month 定义；
- 只允许一个形成期，不做 3/5/10 年窗口网格；
- 2016-2026 主裁决，2007-2015 稳定性；
- 三关 partial IC + size 栖息地 + 月频净后组合；
- 和普通 12-1 momentum 同时回归，防止把动量季节性重新命名。

如果不显著，turn-of-month、day-of-week 等短周期规则不自动获得续命资格；它们须另写假设
文档并计入同一 `calendar_technical` family。

## 条件路线：强基本面候选池中的技术 timing

F-score 和 operating profitability 是当前最强的独立排序信号，但纯多头散户出口失败。
技术分析在这里更合理的用途不是再和基本面做一次等权平均，而是做**层级式决策**：

1. 基本面只负责“买什么”：F-score/OP 选候选池；
2. 技术状态负责“什么时候持有”：只使用 Wave 13 或 Wave 15 已独立存活的信号；
3. 退出规则负责控制路径：固定持有期与一个 canonical trend exit 对拍。

在 Wave 13/15 之前不启动这条线。否则它会变成用技术门控事后挽救已失败的 F-score
散户 study，违反现有预注册纪律。

## 数据阻塞路线：行业内 lead-lag

行业动量和大盘股向小盘股的信息扩散有很强文献先验，也是总账已有的开放问题。但当前
`research.industry.load_industry_panel()` 读取 `securities.sic_code` 的**当前值**：

- 2016-2026 有价普通股只有 49.8% 有 SIC；
- 2007-2015 只有 38.0%；
- 当前分类回看历史会引入幸存者与重分类偏差。

因此不应直接开跑。解锁条件是建立通用的 `security_classification_history`：

- 主键锚定 `security_id`；
- 分类体系、代码、`valid_from/valid_to`、来源和可见时点分离；
- 历史 SEC filing header 的 SIC 可作为回填来源；
- 覆盖率和变更率通过后，才跑 FF12 内大盘领先小盘的 1/5/20 日扩散。

若不愿先补分类历史，可改做滚动历史相关网络，但那是另一条“动态 peer”假设，不能把
全样本聚类后回看历史。

## 暂缓方向

### 继续扩 RSI/MACD/均线参数

不做。已有结果已经表明经典指标是低波动、52 周高或弱反转的马甲。改变 14/20/50/200
窗口只会增加搜索空间，不会形成新机制。

### 大规模 K 线形态与 VCP/旗形/头肩形态库

暂缓。若要做，只允许一个预注册的“波动收缩后 55 日突破 + 连续成交量确认”事件定义，
用 matched-event CAR 裁决。不得同时上线几十种形态后挑赢家。

### ML 自动组合全部 TA 特征

暂缓。当前没有稳定的独立技术信号作为输入基线；直接上树模型、神经网络或符号回归，
只会把 42 因子的试验花园变成不可审计的超参数花园。解锁条件：至少两个新 family 在
2016+ 独立过线，并建立 purged walk-forward 与完整预测日志。

### 个股分钟 scalping

暂缓。分钟数据量足够，但 quote 级 spread、queue position、auction print 和 market impact
均缺失。先做 SPY/市场组合的低维 intraday 复现，确认毛 edge 明显大于成本后再扩展。

## W0：新一轮开始前的最小方法论修补

不需要重写研究框架，只补四件事：

1. **family 级多重检验。** 假设文档写死 variant 和 horizon 数；发现级最低 `|t|>=3.0`，
   边缘结果再用 family Bonferroni。策略规则集合用 Reality Check / SPA，不看单条最佳 Sharpe。
2. **按问题选择评估器。** 横截面因子用 IC；事件用 CAR；市场覆盖层用相对效用与 drawdown；
   intraday 用逐笔可交易时点后的净收益。不要强迫所有技术假设进入 `evaluate.py`。
3. **真正的时间外检验。** 规则定义固定后，至少报告 2007-2015 / 2016-2026；有明确发表年
   的研究再报告发表前/发表后。任何参数选择只在训练段完成。
4. **策略也入总账。** `run_baselines`、事件研究和 market overlay 至少写 `study` 行，记录
   family、规则、窗口、成本、PASS/FAIL、代码 SHA 和数据 as-of；不能只留下 CSV。

额外守卫：

- 空仓现金按 DTB3 计息；
- 长窗继续注入实测退市收益；缺失退市结果单列敏感性，不做静默填补；
- measured spread 覆盖不足时不降低 `n_bars` 门槛；
- 报告换手前的权重漂移误差，必要时再升级 backtest，不提前重构；
- 使用 placebo 日期、延迟一日信号或随机证券映射作为负对照。

## 建议执行顺序

| 顺序 | 交付 | 预计工程量 | 是否需要新数据 |
|---:|---|---:|---|
| 0 | family 级判据 + strategy study 登记约定 | 0.5-1 天 | 否 |
| 1 | Wave 13：两个 path momentum 因子 + 标准流水 | 1-2 天 + 一夜跑数 | 否 |
| 2 | Wave 14：earnings gap event study | 2-3 天 | 否；先接真实交易日历 |
| 3 | Wave 15：market trend/breadth overlay | 1-2 天 | 否 |
| 4 | Wave 16：SPY/市场 intraday momentum | 2-4 天 | 否 |
| 5 | Wave 17：same-month seasonality | 1-2 天 | 否 |
| 6 | F-score/OP + 已存活技术信号 timing | 仅在 Wave 13/15 PASS 后 | 否 |
| 7 | 行业 lead-lag | 先补 classification history | 是，需历史分类回填 |

如果只选一条立刻开工，选 **Wave 13**。如果更在意尽快得到可交易、低换手且容易解释的
结果，选 **Wave 14**。如果目标是先改善未来组合的回撤和持有体验，选 **Wave 15**。

## 关键文献

- Moskowitz, Ooi, Pedersen, [Time Series Momentum](https://doi.org/10.1016/j.jfineco.2011.11.003), 2012.
- Hurst, Ooi, Pedersen, [A Century of Evidence on Trend-Following Investing](https://doi.org/10.3905/jpm.2017.44.1.015), 2017.
- Blitz, Huij, Martens, [Residual Momentum](https://doi.org/10.1016/j.jempfin.2011.01.003), 2011.
- Da, Gurun, Warachka, [Frog in the Pan: Continuous Information and Momentum](https://doi.org/10.1093/rfs/hhu003), 2014.
- Moskowitz, Grinblatt, [Do Industries Explain Momentum?](https://doi.org/10.1111/0022-1082.00146), 1999.
- Hou, [Industry Information Diffusion and the Lead-lag Effect in Stock Returns](https://doi.org/10.1093/revfin/hhm003), 2007.
- Cooper, Gutierrez, Hameed, [Market States and Momentum](https://doi.org/10.1111/j.1540-6261.2004.00665.x), 2004.
- Daniel, Moskowitz, [Momentum Crashes](https://doi.org/10.1016/j.jfineco.2015.12.002), 2016.
- Barroso, Santa-Clara, [Momentum has its moments](https://doi.org/10.1016/j.jfineco.2014.11.010), 2015.
- Lee, Swaminathan, [Price Momentum and Trading Volume](https://doi.org/10.1111/0022-1082.00280), 2000.
- Bernard, Thomas, [Post-Earnings-Announcement Drift: Delayed Price Response or Risk Premium?](https://doi.org/10.2307/2491062), 1989.
- Gao, Han, Li, Zhou, [Market intraday momentum](https://doi.org/10.1016/j.jfineco.2018.05.009), 2018.
- Lou, Polk, Skouras, [A tug of war: Overnight versus intraday expected returns](https://doi.org/10.1016/j.jfineco.2019.03.011), 2019.
- Heston, Sadka, [Seasonality in the cross-section of stock returns](https://doi.org/10.1016/j.jfineco.2007.02.003), 2008.
- Brock, Lakonishok, LeBaron, [Simple Technical Trading Rules and the Stochastic Properties of Stock Returns](https://doi.org/10.1111/j.1540-6261.1992.tb04681.x), 1992.
- Sullivan, Timmermann, White, [Data-Snooping, Technical Trading Rule Performance, and the Bootstrap](https://doi.org/10.1111/0022-1082.00163), 1999.
- Bajgrowicz, Scaillet, [Technical trading revisited: False discoveries, persistence tests, and transaction costs](https://doi.org/10.1016/j.jfineco.2012.06.001), 2012.
- Harvey, Liu, Zhu, [... and the Cross-Section of Expected Returns](https://doi.org/10.1093/rfs/hhv059), 2016.
- McLean, Pontiff, [Does Academic Research Destroy Stock Return Predictability?](https://doi.org/10.1111/jofi.12365), 2016.
- Hou, Xue, Zhang, [Replicating Anomalies](https://doi.org/10.1093/rfs/hhy131), 2020.

## 仓库证据

- `docs/research_ledger.md`：因子与方法论裁决总账。
- `docs/research_technical_factors_2026-07.md`：wave 1-11 的技术研究过程与结果。
- `research/output/trials.parquet`：机器 trial 台账；权威快照在 `home-debian`（253）。
- `research/output/baselines_metrics_2011-01-01_2026-06-10.csv`：技术策略长窗基线。
- `research/events.py`：SEC 财报事件可见时点与收益对齐工具。
- `research/industry.py`：当前 SIC -> FF12 映射；尚非 PIT 分类历史。
- `research/backtest.py`：当前日频回测语义与已知未建模项。
