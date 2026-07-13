# Wave 16 预注册：market_intraday_momentum（市场同日日内动量）

> 冻结日期：2026-07-13。本文在任何 Wave 16 样本结果运行之前写入并提交。
> Family：`market_intraday_momentum`。资产、预测器、目标窗、样本切分、成本、
> 判据与停止条件全部在此写死；失败后不做阈值搜索、不加条件化（VIX/量能/星期）、
> 不试其他日内窗口、不扩展行业或个股。

## 研究问题（Gao-Han-Li-Zhou 2018, JFE）

市场首半小时收益是否预测同日尾半小时同方向收益？2019 年后（论文发表后）是否仍然
存在、且净后可交易？这与 wave 1-15 的次日横截面研究正交：它是**市场级时序**问题。

## 资产（只做两个，写死）

1. **SPY**（security_id=3379）：分钟 bar 直接取窗口锚点价。
2. **PIT CS 总市值加权市场组合**（`pit_cs_capw`）：全 CS 宇宙（straddle_v2 未覆盖
   事件 gate 剔除），逐股取分钟特征窗口收益，按**前一交易日总市值**（
   `load_market_cap_panel` include_xbrl 默认档）加权。权重裁决（wave-15 报告
   开放问题 #1）：总市值而非流通市值——PIT 股本机器 2009+ 稳健，float 史弱得多。
   不加 $3/$2M eligibility 门（市场组合应代表市场，市值加权天然压低垃圾股；
   分钟质量由 n_bars≥100 门把守）。

## 预测器与目标（写死）

- **主预测器 r1（GHLZ 文献原口径）**：昨日收盘 → 当日 10:00（09:59 bar close），
  **含隔夜跳空**。SPY 用原始价（ETF 除息日 ~4 天/年的失真接受并记录）；个股腿
  隔夜段用日级复权因子链调整（factor = adj_close/close 面板比值），防拆股假跳空。
- **描述腿 r1_intraday**：09:30 bar open → 10:00。只报告不裁决，**不得救援主腿**
  （roadmap 草案曾以此为主问题；预注册在结果前改为文献原口径以使"复现"名副其实，
  改动方向=可检验性更强，此处留痕）。
- **目标 y**：15:29 bar close → 15:59 bar close。**排除 16:00 收盘竞价**（wave-15
  报告开放问题 #2 的裁决：可测量的诚实版本；与 GHLZ 的"至收盘"口径偏差记录在案）。
- 有效日掩码：SPY 当日 09:59、15:29、15:59 三个锚 bar 齐全（早收盘日自然剔除）。
- 个股窗口收益取 `minute_daily_features.ret_first30/ret_last30`（n_bars≥100 门；
  该表对缺窗口存哨兵 0——n_bars 门后残余哨兵与真实零收益均为 ~0 贡献，接受并记录）。

## 样本切分（写死）

| 腿 | 复现窗（文献内） | 发表后窗（主裁决） |
|---|---|---|
| SPY | 2004-01-02 ~ 2018-12-31 | 2019-01-02 ~ 最新完整交易日 |
| pit_cs_capw | 2010-01-04 ~ 2018-12-31（XBRL 股本边界） | 同上 |

## 检验（每资产独立裁决）

1. **预测回归**：`y_t = α + β·r1_t + ε_t`（日频 OLS，Newey-West lag=10）。
2. **交易检验**：每日 `sign(r1)` 满仓持有 15:30→15:59，**不搜索阈值**；
   日净收益 = `sign(r1)·y − 2×成本档`。成本档 1/2/5 bps 单边，**主档 2bps**
   （SPY 实际半价差 <1bp，2bps 已保守；5bps=压力披露）。
- 披露（不裁决）：多空日分解、逐年均值、r1_intraday 全套对照、Sharpe。

## 判据（PASS 须全部满足，逐资产）

1. 复现窗 β>0 且 NW t ≥ **3**（复现失败=实现/数据存疑，直接结案）；
2. 发表后窗 β>0 且 NW t ≥ **2**；
3. 发表后窗交易净收益（2bps 档）均值 >0 且 NW t ≥ **2**；
4. 危机日稳健：发表后窗剔除 |策略日收益| 最大的 10 天后，2bps 净均值仍 >0。

**扩展门**（行业/个股，本轮不做）：两资产都 PASS 才允许立项。
**部署候选门**：SPY 腿 PASS 才谈纸面组合（owner 形状为持仓组合，本策略最多
作为独立小仓位日内轮，须另行部署检验）。

## 停止条件

任一资产任一判据 FAIL → 该资产结案；两资产都 FAIL → **family 关闭**：
不做 |r1| 阈值/分位过滤，不做波动/量能/VIX 条件化，不换 12:00-15:30 等其他
预测窗，不做第二天延续版，不扩展行业 ETF 或个股。r1_intraday 描述腿结果
无论多好看都不构成新假设的立项理由（须全新预注册且计入 family 分母）。

## 输出与台账

- 逐资产日度序列 parquet（date/r1/r1_intraday/y/valid、市场腿另存覆盖诊断
  n_names/cap_coverage）、指标 JSON/MD、逐年表。
- 独立复算审计：从落盘日度序列复算 β/t/交易均值，误差 >1e-10 拒写 study 行。
- `append_study(study="market_intraday_momentum", factor_name=<asset>)` 一资产
  一行，criterion_values 含四判据数值；trial_kind='study' 不进 Bonferroni 分母，
  但预注册检验计数在此声明：主检验 = 2 资产 × 4 判据（回归 2 + 交易 1 + 稳健 1）。

## 文献锚

- Gao, Han, Li, Zhou (2018), *Market intraday momentum*, JFE 129(2)。
- Lou, Polk, Skouras (2019), *A tug of war: Overnight versus intraday expected
  returns*, JFE（隔夜/盘中分解先验——r1 含隔夜的动机）。
- Bogousslavsky (2016), *Infrequent Rebalancing, Return Autocorrelation, and
  Seasonality*, JF（日内 U 型再平衡机制先验）。
