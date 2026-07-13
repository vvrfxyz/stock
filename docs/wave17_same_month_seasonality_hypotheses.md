# Wave 17 预注册：same_month_seasonality（同日历月季节性）

> 冻结日期：2026-07-13。本文在任何 Wave 17 样本结果运行之前写入并提交。
> Family：`calendar_technical`。信号定义、样本、控制、成本、判据与停止条件全部
> 在此写死；失败后不做形成窗网格、不换单 lag 变体、不给 turn-of-month /
> day-of-week 自动续命（它们属同 family，须另写假设文档并计入分母）。

## 研究问题（Heston-Sadka 2008；Keloharju-Linnainmaa-Nyberg 2016）

股票在同一个日历月的历史收益是否预测它本月的横截面相对收益？（"每年 1 月强的
股票，下一个 1 月还强"）。这与 12-1 动量、路径动量（wave 13 已死）不同：季节性
信号只看同月历史，跨月不看。

## 信号定义（一个，写死）

**`same_month_1_10`**：对持有月 m，信号 = 该股在 m−12、m−24、…、m−120（年 lag
1..10 的同日历月）的月收益均值，**至少 3 个有效观测**，否则 NaN。

- 取 KLN 2016 的稳健口径（多年均值）而非 HS 单 lag：这是文献的强形式主张，
  单月形成噪声大。lag 上限 10 = 2003 数据地板 + 2016 主窗口的最大可用深度；
  min 3 = 均值降噪下限。两常数事前冻结，非网格。
- **单 lag-12 变体本轮不跑**；若均值版死，单 lag 不获自动续命（防"换定义复活"）。
- 月网格 = 日历月：复权日收盘 `resample('ME').last()` → 月收益。持有月 m 的信号
  在 m−1 月末形成时全部可见（最近成分是 12 个月前的已完结月），PIT 平凡成立。

## 样本与宇宙

- 面板：`load_adjusted_panel` 2003-01-02 起（FACTOR_TRUST_FLOOR）至最新完整交易日；
  末尾不完整日历月剔除。
- **主裁决：持有月 2016-01 ~ 最新完整月**；**稳定腿：2007-01 ~ 2015-12**（早段
  lag 深度 3-11 个，预先声明为弱证据腿，只检符号）。
- 宇宙：CS、`straddle_v2` 未覆盖事件 gate 剔除；资格（形成日=m−1 月最后交易日）：
  原始收盘 ≥ $3 且 63 日中位美元成交额 ≥ $200 万。
- 单月有效横截面 < 300 只 → 该月剔除并计数。
- 退市：持有月内整月无价的持仓贡献 0%（现金），次月全额换手退出；不注入实测
  退市收益（多空两腿对称受影响，披露为局限）。

## 检验与判据（PASS 须全部满足）

对照/回归全部秩空间；NW lag = `default_nw_lag(1, n_months)`。

1. **H1 主窗口预测力**：逐月 Spearman rank IC（信号 vs 当月收益）序列，
   均值 > 0 且 NW t ≥ **3**。
2. **H2 非动量重命名**（roadmap 强制）：逐月把信号秩对 `momentum_12_1` 秩
   （同月网格：P[m−2]/P[m−13]−1）横截面 OLS 残差化，partial IC 均值 > 0 且
   NW t ≥ **2**。
3. **H3 月频净后组合**：资格内按信号五分位，q5−q1 等权多空、月频再平衡，
   成本 **25bps 单边 × Σ|Δw|**（两腿、含漂移）；净月均 > 0 且 NW t ≥ **2**。
   披露档 10/40bps 不裁决。
4. **H4 稳定腿**：2007-2015 的 H1 均值 IC > 0（只检符号）。

## 披露（不裁决）

逐年 IC 均值；市值三分位（`load_market_cap_panel` 形成日市值）桶内 IC；
LS 毛/净、多空腿相对宇宙均值的分解；月换手率；1 月/非 1 月 IC 分解
（税损卖压节点，解释用）。

## 停止条件

任一判据 FAIL → `same_month_1_10` 死亡、结案回写；不做形成窗/加权/单 lag
变体，不做行业内版本。turn-of-month、day-of-week 等短周期日历规则不获自动
续命——须全新假设文档、同 family 计数。四判据 = 本轮 family 主检验计数。

## 输出与台账

- 逐月序列 parquet（ic / partial_ic / ls_gross / ls_net / turnover / n_stocks）+
  指标 JSON/MD + 逐年表。
- 独立复算审计：从落盘月序列复算均值/NW t/净收益恒等式（net = gross −
  25bps×turnover），误差 > 1e-10 拒写 study 行。
- `append_study(study="calendar_technical", factor_name="same_month_1_10")`，
  criterion_values 含四判据数值。

## 文献锚

- Heston, Sadka (2008), *Seasonality in the cross-section of stock returns*, JFE 87(2)。
- Keloharju, Linnainmaa, Nyberg (2016), *Return Seasonalities*, JF 71(4)。
- Sullivan, Timmermann, White (1999)（禁网格的依据，同 concentrated_topk）。
