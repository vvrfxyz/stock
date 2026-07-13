# Wave 15 预注册：市场趋势与 price breadth 风险覆盖层

> 冻结日期：2026-07-12。本文在任何 Wave 15 主样本或稳定性样本结果运行之前写入。
> Family：`market_regime_overlay`。只允许下述三个规则；失败后不追加均线窗口、breadth
> 阈值、连续仓位函数、HMM、change-point 或机器学习 regime 变体。

## 结果前执行审计

正式脚本的前两次 253 启动都在任何固定成本指标、报告文件或台账 verdict 生成之前由
数据 gate 退出，未读取策略结果：第一次暴露稳定性开头尚未积满 200 个会话；第二次暴露
把 SPY 总收益起点同时误用为 CS 流动性/breadth 预热起点。相应修正仅限本节下方已经明确
的时间边界：leading warm-up 状态保持 100% 基线暴露；CS 从 2006-01-03 预热，SPY 10 月
趋势从 2007-03-16 计数，模拟从 2007-03-16 开始。三个规则、阈值、成本、样本裁决窗和
PASS/停止条件均未改变。两次退出的远端 `market_regime_overlay_*` 输出和 Wave 15 台账行
均为零。

### 执行后合规勘误（2026-07-12）

首次成功运行 `wave15_market_regime_overlay_v1` 后的输出审计发现，脚本将运行截止日
2026-07-10 误当成 2026 年 7 月的月末信号日。原因是月末函数只看到了截至当日的价格
索引，而没有用完整 XNYS 自然月日历确认 7 月最后一个交易日实际为 2026-07-31。这一行
没有后续持有收益，却产生了额外调仓成本，因此属于时点合规错误，不能作为最终裁决。

修正版登记为 `wave15_market_regime_overlay_v2`：信号日先从覆盖完整自然月的 XNYS 日历
确定，再只保留不晚于收益截止日的月末；收益仍延伸到 2026-07-10，并沿用 2026-06-30
形成的状态。v1 输出与三条 FAIL 台账行保留为审计痕迹，不删除、不覆盖；v2 使用独立输出
文件名并写入独立 study 行。三个冻结规则、窗口、阈值、仓位、成本、样本和 PASS/停止
条件均未改变。

同时，v1 的 `measured_spread_end` 实际表示“最后一个能匹配到历史价差估计的成交日”，
并不表示分钟特征事实表的新鲜度。v2 将其拆成 `minute_spread_feature_end` 与
`measured_spread_trade_match_end`，最终报告按两者各自语义披露。

## 研究问题

Wave 15 不寻找新的横截面选股分数。它检验一个更窄的问题：只用月末可见的市场总收益
趋势和普通股价格 breadth，能否在现金按 DTB3 计息、真实权重漂移和完整换手成本后，
降低 SPY 与 PIT 普通股等权组合的尾部风险，同时不牺牲过多长期复利。

这是描述性策略检验，不是因果研究。PASS 只表示透明风险覆盖层在冻结样本和成本口径下
提供稳定的相对效用，不表示未来危机一定重复。

## 数据、窗口与时点

- 日线事实来自 `daily_prices`；复权收益来自研究层 `raw_actions_v1` 调整因子，不向事实表
  写入调整价或指标。
- 市场代理为 SPY（`security_id=3379`）总收益价格。脚本必须在运行前验证 SPY 分红调整
  因子覆盖，缺失时硬失败。
- 股票底层为 `type='CS'` 的 PIT universe。每日上市状态使用 `list_date/delist_date`，并要求
  当日有价格；标准可交易门槛固定为原始收盘价至少 3 美元、过去 63 个 XNYS 交易日的
  原始美元成交额中位数至少 200 万美元。
- 存在跨立价格历史但没有调整因子覆盖的公司行动证券，按项目标准
  `straddle_v2` gate 整体剔除。
- 交易日只使用 `trading_calendars` 的 XNYS 开市日。信号在月末收盘后形成，从下一 XNYS
  交易日收益开始生效；月末当日收益仍属于上一个状态。
- CS 价格与流动性形成历史从 2006-01-03 加载，供 63 日 eligibility 和 200 日 breadth
  预热；这段只提供个股形成数据，不进入收益裁决。SPY 10 月趋势的计数时钟从可验证的
  SPY 总收益链首个分红调整日 2007-03-16 开始，之前的 SPY 月末价格不进入 10 月均线。
  组合从 2007-03-16 后首个月末建立。稳定性样本为 2007-07-02 至 2015-12-31；主样本为
  2016-01-04 至运行时最新完整且有 SPY 日线的 XNYS 交易日。
- 10 个月或 200 日窗口尚未完整时，规则保持 100% 基线风险暴露；缺失状态不得解释为
  risk-off。首个有效信号及其之前的基线月份进入审计，不能因后来看到 2008 而裁剪。

## 三个冻结规则

所有移动平均都包含信号月末当日收盘，信号只能作用于下一交易日。

### H1：`spy_10m_trend`

取每个自然月最后一个 XNYS 交易日的 SPY 总收益价格。至少有 10 个完整月末价格后，
若当前月末价格严格高于包含当前月的最近 10 个自然月末价格算术平均，则下月目标风险
暴露为 100%；否则为 0%，其余资金持有 DTB3 现金。

不允许改成 9/12 月均线、日频 200 日均线、月末缓冲带或连续仓位。

### H2：`breadth_200d`

对每只 PIT eligible 普通股计算包含月末当日的 200 个 XNYS 会话简单移动平均。过去
200 个会话必须全部有有效复权价格；不足 200 个完整会话的新股或存在窗口内价格缺口的
证券不进入当月分母，也不做前向填补。

`breadth = count(P_i > SMA200_i) / count(valid_200d_i)`。

若 breadth 严格大于 50%，下月目标风险暴露为 100%；否则为 0%。分母为零是数据错误，
不得以默认 risk-on/risk-off 兜底。

不允许搜索 40%/45%/55%/60% 阈值、100/150/250 日窗口或按市值加权 breadth。

### H3：`trend_and_breadth`

若 H1 与 H2 在同一月末都为 risk-on，下月目标风险暴露为 100%；否则为 50%，另外 50%
持有 DTB3。该规则没有第三种状态，也不允许事后改成 OR、0/100 或按 breadth 连续缩放。

## 两个底层资产与持仓语义

### SPY 底层

风险资产只有 SPY。状态变化时在月末收盘调整 SPY 权重；成本按股票权重变化绝对值计算。
满仓基线在预热期建仓后保持 100% SPY。

### PIT 普通股等权底层

每个月末在当时 eligible 的普通股中等权配置目标风险暴露。持有期内权重随个股与现金
收益真实漂移；下个月末先按当日收益更新漂移后权重，再与新目标权重比较，换手定义为：

`turnover_t = sum_i(abs(target_weight_i - pretrade_weight_i))`。

无当日价格的已有持仓不假定可以交易：它保持冻结，剩余可配置风险预算才分给当月
eligible 且有价证券。复牌收益一次性补回跨缺口路径。

退市收益优先使用 `delisting_events` 的逐证券实测值。实测退市结局在首次永久缺价日注入，
剩余价值转为现金并从下一日赚 DTB3。无实测结局的永久缺价持仓在主口径中不填收益、
保持冻结并单列权重日；另做 `-30%` 显式终局收益敏感性。敏感性不能把主口径 FAIL 改为
PASS；若主口径 PASS 而敏感性破坏门槛，只能标为数据风险下不可部署。

## 现金与交易成本

现金使用 FRED DTB3 年化 discount-basis 百分比，按 actual/360 转成与 XNYS 会话对齐的
简单日收益。缺少新鲜 DTB3 行是硬错误，现金不得默认为 0。

固定单边成本档位：

- SPY：1、2、5 bps；主裁决为 2 bps。
- PIT 普通股：10、25、40 bps；主裁决为 25 bps。

成本为 `sum(abs(delta stock weight)) * one_side_bps / 10,000`。现金权重变化不另收费；
股票交易的买入和卖出分别通过绝对权重变化进入换手，因此一次完整进出自然支付两边。

分钟特征 `cs_spread` 只用于股票腿的覆盖诊断：每个月末取过去 63 个 XNYS 会话中至少
20 个有效日的 Corwin-Schultz 全宽中位数，单边成本为全宽的一半。`n_bars < 100` 或
病理负估计已经在特征层置空。只对成本非空的实际成交权重报告覆盖率、加权成本分布和
已覆盖部分的成本拖累；不填补缺失交易，也不从部分覆盖成本推导完整净收益或 PASS。

## 指标定义

每个规则、底层资产、样本时期和固定成本档分别报告：

- CAGR：净日收益复利，按 252 交易日年化；
- Sharpe：`strategy_return - DTB3` 的日均值除以日标准差，再乘 `sqrt(252)`；
- Sortino：同一超额收益的年化均值除以负向半方差平方根；
- 最大回撤：样本起点净值 1.0 也进入高水位；
- 95% Expected Shortfall：最差 5% 净日收益的均值；
- down-capture：满仓基线为负的交易日中，策略平均收益除以基线平均收益；
- 年化实际换手、平均目标风险暴露；
- 错杀天数：目标风险暴露小于 100%，且同日满仓风险资产毛收益高于 DTB3 的交易日数；
  同时报告这些日子的未捕获正超额收益之和：
  `sum((1 - target_exposure) * (baseline_gross_return - DTB3))`；
- 2008、2020、2022 三个完整自然年的累计收益、最大回撤和平均目标风险暴露。

危机年份是冻结的完整自然年，不允许跑后改成更有利的局部高低点窗口。

## PASS、FAIL 与停止规则

对每个规则分别裁决。主成本下，每个 `asset x sample` 单元相对同资产、同样本、同成本的
100% 满仓基线必须同时满足：

1. 最大回撤改善至少 10 个百分点：
   `overlay_max_drawdown - baseline_max_drawdown >= 0.10`；
2. Sharpe 至少提高 0.10；
3. CAGR 损失不超过 2 个百分点：`baseline_cagr - overlay_cagr <= 0.02`。

一条规则只有在 SPY/普通股等权两个底层资产、2007-2015/2016-2026 两个时期共四个单元
全部通过时才 PASS。任一单元失败即该规则 FAIL；不得用全样本、单一资产或 2008 的结果
替代。10/40bps、1/5bps、退市敏感性、危机年份和实测价差覆盖均为稳健性/部署诊断，
不能复活主成本失败规则。

- 三条规则全部 FAIL：关闭 `market_regime_overlay` 简单规则家族，不升级 HMM、
  change-point、波动目标或机器学习 regime。
- 仅 H1 或 H2 PASS：只保留通过的单规则，不搜索另一个窗口或阈值。
- H3 PASS 而单规则 FAIL：只保留冻结的 50% 组合规则，不据此搜索 25%/75% 仓位。
- 结果仅在 2008 改善、而主样本 2016+ 被现金拖累至门槛失败：按 FAIL 结案。
- 任一规则 PASS 但股票腿对无实测退市结局的 `-30%` 敏感性不稳：研究发现可保留，
  部署状态必须冻结，直到退市结局覆盖改善。

## 预定输出与审计

- 月末信号：SPY 价格/10 月均线、breadth、breadth 分母、三规则目标暴露；
- 每日毛/净收益、换手、目标暴露和 DTB3；
- 主/稳定性全成本指标与四单元裁决；
- 2008/2020/2022 危机表；
- 退市覆盖、冻结仓位和 `-30%` 敏感性；
- 实测价差成交权重覆盖诊断，不做缺失填补；
- 三条规则分别写入 `trials.parquet` 的 `study` verdict。

独立复算必须从保存的每日收益与换手重新得到 CAGR、Sharpe、最大回撤和成本恒等式；
报告不得只转述研究脚本的汇总 JSON。

## 文献锚

- Brock, Lakonishok, and LeBaron (1992), *Simple Technical Trading Rules and the
  Stochastic Properties of Stock Returns*, Journal of Finance.
- Moskowitz, Ooi, and Pedersen (2012), *Time Series Momentum*, Journal of Financial
  Economics.
- Hurst, Ooi, and Pedersen (2017), *A Century of Evidence on Trend-Following Investing*,
  Journal of Portfolio Management.
- Corwin and Schultz (2012), *A Simple Way to Estimate Bid-Ask Spreads from Daily
  High and Low Prices*, Journal of Finance.
- Sullivan, Timmermann, and White (1999), *Data-Snooping, Technical Trading Rule
  Performance, and the Bootstrap*, Journal of Finance.
