# Wave 13 预注册：残差动量与趋势路径质量

> 冻结日期：2026-07-12。本文在任何 Wave 13 样本结果运行之前写入。
> Family：`path_momentum`。只允许下述两个预注册变体；失败后不追加 slope、R2、
> efficiency ratio、替代窗口或参数网格。

## 研究问题

现有 `momentum_12_1` 只描述 12-1 窗口的总收益终点，`high_52w` 只描述价格相对
年度高点的位置。Wave 13 检验两个尚未覆盖的问题：

1. 去掉滚动市场暴露后，个股自身的异常收益路径是否仍有持续性；
2. 在累计 12-1 收益相近时，连续形成的趋势是否比少数跳跃形成的趋势更持久。

本研究不把结果解释为因果效应。它只检验可重复的条件预测关系。

## 文献锚与本项目口径

- Blitz、Huij、Martens（2011）使用过去 36 个月的 Fama-French 三因子回归，按过去
  12 个月跳过最近 1 个月的残差收益排序，并用同窗残差波动标准化。
- Da、Gurun、Warachka（2014）定义 information discreteness：

  `ID = sign(PRET) * (% negative days - % positive days)`

  其中 `PRET` 是过去 12 个月跳过最近 1 个月的累计收益。低 ID 表示趋势由更多与
  `PRET` 同方向的小收益日连续形成；高 ID 表示趋势更离散。论文的主检验是先按
  `PRET`、再在每个 `PRET` 组内按 ID 的顺序双排序，而不是对 ID 做无条件排序。

本项目没有完整 PIT 的日频 SMB/HML 因子，因此 H1 是日频单因子市场模型的事前
创新版本，不声称直接复制 Blitz-Huij-Martens。H2 严格复现 Da-Gurun-Warachka 的
基准 ID 公式和条件排序逻辑。

## H1：`residual_momentum_12_1`

### 精确定义

使用研究层复权收盘价，仅在 `research/` 内计算，不写回 `daily_prices`。

1. 简单日收益：`r_i,t = P_i,t / P_i,t-1 - 1`。
2. 市场代理：当日全 CS 价格面板可得收益的横截面等权均值 `r_m,t`。市场代理不受
   当日 eligibility 掩码影响，与现有 `residual_vol` 口径一致。
3. 对每个证券和日期 t，仅用 t 之前最多 252 个配对有效日估计：

   `r_i,s = alpha_i,t-1 + beta_i,t-1 * r_m,s + error_i,s`

   当前收益不进入估计窗；有效观测少于 126 或市场方差为零时，当日残差为 NaN。
4. 一步外推残差：

   `e_i,t = r_i,t - alpha_i,t-1 - beta_i,t-1 * r_m,t`
5. 信号：

   `residual_momentum_12_1(i,t) = sum(e_i,s), s = t-251 ... t-21`

   该区间恰含 231 个日收益，对应 `P[t-21] / P[t-252] - 1` 的 12-1 窗口。231 个
   残差必须全部有效；不对缺失天数缩放，不用后续数据补洞，不做残差波动标准化。

元属性：`lookback_days=504`、`lag_days=1`、`pit_guarantee=True`，预期方向为正。

### 主判据

- 主样本：2016-01-04 至最新完整交易日；主 horizon 为 21 个交易日。
- 发现关：h=21 的平均 rank-IC 为正且 Newey-West t >= 3.0。
- 方向稳定：h=1/5/10 的 IC 均为正；2007-07-02 至 2015-12-31 的 h=21 IC 同号。
- 正交关：若发现关通过，运行 `factor_correlation` 对
  `size,low_vol,high_52w`；对最强吸收者的 partial IC 至少保留原始 IC 的 1/3。
- 栖息地关：若发现关通过，运行 `size_neutral_study`，报告三市值桶 IC 和现有
  size-neutral 部署判据。部署失败不改写发现结论，但会阻止策略化。

## H2：`information_discreteness_12_1`

### 精确定义

1. `PRET = P[t-21] / P[t-252] - 1`。
2. 形成期日收益为 `t-251 ... t-21`，共 231 日；231 日必须全部有效。
3. `n_pos`、`n_neg`、`n_zero` 分别统计正、负、零收益日，分母固定为 231：

   `ID = sign(PRET) * (n_neg - n_pos) / 231`

   `sign(0)=0`。低 ID 表示连续信息，高 ID 表示离散信息。

ID 不是无条件单调因子：低 ID 同时包含连续赢家和连续输家。因此不运行普通
`research.evaluate`，只运行专用条件研究。

### 条件双排序

- 每个自然月最后一个交易日形成组合，使用当日收盘可得信息预测其后收益。
- 使用项目标准 CS eligibility：价格 >= 3 美元、63 日中位美元成交额 >= 200 万美元，
  并沿用未覆盖公司行动剔除和实测退市收益注入。
- 主排序为顺序双排序：先按 PRET 五分位，再在每个 PRET 五分位内按 ID 五分位。
  每格至少 20 只证券；低 ID 为连续组，高 ID 为离散组。
- 前向窗口：21、63、126 个交易日。主判据窗口为 126 日，对应原论文六个月持有期。
- 月度收益序列的 Newey-West lag 固定为
  `max(ceil(horizon/21), floor(4*(n/100)^(2/9)))`，同时覆盖持有期重叠与自动带宽。
- 稳健性排序：独立按 PRET 和 ID 各自五分位；它不改变主判据，只检查结果是否依赖
  顺序分桶。

对每个 ID 分位 k，定义条件动量：

`MOM_k = mean(fwd | PRET_Q5, ID_Qk) - mean(fwd | PRET_Q1, ID_Qk)`

核心交互：

`FIP_spread = MOM_low_ID - MOM_high_ID`

并分别报告：

- 赢家连续性：`winner_low_ID - winner_high_ID`，预期 > 0；
- 输家连续性：`loser_high_ID - loser_low_ID`，预期 > 0。

### 主判据

- 2016-2026 顺序双排序的 126 日 `FIP_spread` > 0 且 Newey-West t >= 3.0。
- 21 日和 63 日 `FIP_spread` 同为正。
- 126 日赢家连续性和输家连续性两个分量均为正。
- 独立双排序的 126 日 `FIP_spread` 为正。
- 2007-07-02 至 2015-12-31 顺序双排序的 126 日 `FIP_spread` 同号。

## 多重检验与停止条件

Family 只有两个主检验：H1 的 h=21 rank-IC、H2 的 126 日条件交互。其他 horizon、
双排序方式、三关和市值桶都是稳定性或机制诊断，不允许替代失败的主判据。

- H1 和 H2 均失败：`path_momentum` 家族结案，不追加近义变体。
- 仅一个通过：只保留通过的定义，失败定义不调参复活。
- 通过发现关但正交关失败：记为已知价格主干的重表达，不进入组合研究。
- 任何结论必须同时报告主样本和稳定性样本；不得只展示较好窗口。

## 预定输出

- 标准 evaluate 报告：H1 的四个 horizon IC、NW t、分位收益、覆盖率和 PIT 诊断。
- 条件研究报告：H2 的 5x5 结构、三 horizon 交互、赢家/输家分量、顺序/独立排序。
- 条件通过后的相关性、partial IC 和市值栖息地诊断。
- 最终技术报告给出明确 PASS/FAIL、限制、可复现命令和下一步，不以单个漂亮数字
  替代完整裁决。

## 参考文献

- Blitz, D., Huij, J., & Martens, M. (2011). Residual Momentum. Journal of Empirical
  Finance, 18(3), 506-521. https://doi.org/10.1016/j.jempfin.2011.01.003
- Da, Z., Gurun, U. G., & Warachka, M. (2014). Frog in the Pan: Continuous Information
  and Momentum. Review of Financial Studies, 27(7), 2171-2218.
  https://doi.org/10.1093/rfs/hhu003
