# 技术因子研究备忘录：日内微观结构 vs K 线几何（2026-07-06 启动）

> 研究授权：owner 全权委托的自由探索（"从技术角度入手"）。
> 纪律：所有试验经 `research/evaluate.py` 登记进 trials.parquet（git sha/参数/
> 宇宙哈希），多重检验按族收敛；本备忘录同步记录死因子——不发表偏差。

## 研究问题

1. **分钟数据买来了多少 alpha？** 同一个"彩票偏好/日内压力"信号，用
   5.1B 行分钟数据精确计算（realized_skew, signed_jump）vs 用日线 K 线几何
   近似（shadow_asymmetry），IC 差值就是分钟数据在该信号族的定价。
2. **分钟独占信号是否存在？** 尾盘半小时收益持续性（last30_persistence）、
   聪明钱缺口（smart_money_gap）——这两个日线根本造不出来。
3. 经典价格异象（max_lottery, short_term_reversal）作为已知基线对照。

## 基础设施（本轮新建）

- `stock.minute_daily_features`（ClickHouse）：2,839 万行日频微观结构特征，
  50 亿分钟条单遍聚合，全史重建 4 分钟。5 分钟子采样抗噪（Liu-Patton-Sheppard
  口径），n_bars<100 判无效日。
- 8 个 builtin 因子（4 族）+ 加载器 + 10 单测。PIT：全部 t 日收盘可得。

## Wave 1 结果（评估窗 2025-05-16 ~ 2026-07-02，282 交易日，日均 ~2,850 只）

| 因子 | 族 | 最佳 IC | NW t | 净后 LS Sharpe | 判定 |
|---|---|---|---|---|---|
| shadow_asymmetry | K线几何 | .0154 @5d | **2.00 @1d** | +0.71 @21d | 全场唯一 t≥2 |
| last30_persistence | 分钟流 | .0153 @21d | 1.05 | **+1.58 @21d** | IC 弱、组合强 |
| smart_money_gap | 分钟流 | .0158 @10d | 1.01 | +1.04 @21d | 同上 |
| signed_jump | 分钟矩量 | .0144 @21d | 1.90 @21d | 净后负 | 边缘 |
| realized_skew | 分钟矩量 | .0098 @5d | 1.67 @1d | 净后负 | 弱于日线代理 |
| max_lottery | 经典 | ~0 | <1 | 负 | 本窗死 |
| short_term_reversal | 经典 | ~0 | <1 | 负 | 本窗死 |
| close_vwap_pressure | K线几何 | ~0 | <1 | 负 | 死，退出后续轮 |

**初步反转**：短窗里日线影线近似 ≥ 分钟精确偏度——"偏度族"信号在 t+1 起的
日频 horizon 上可能不需要分钟精度（分钟优势或在更短 horizon/更快衰减段，
ic_decay_halflife 3 天与此一致）。分钟数据的真实价值目前体现在**只能用它构造**
的资金流族（净后 Sharpe 1.0-1.6）。

**诚实声明**：14 个月 × 8 因子 × 4 horizon = 32 次检验，t=2.0 不过 Bonferroni
（~2.73）；两个"死"的经典异象（MAX/反转）文献里是强效应，本窗死更说明窗口
太短而非因子无效。一切以长窗为准。

## Wave 2（进行中）

- 长窗评估：2015-01-01 起（11.5 年），7 个存活因子（剔 close_vwap_pressure）。
- 待办：因子间相关矩阵与增量 IC（shadow_asymmetry 对 realized_skew 的替代率）；
  分组 by 流动性（微观结构效应常聚于小盘——但那里成本也高，须看净后）；
  earnings 周掩蔽稳健性（signed_jump 可能只是财报跳空代理，用 sec_filings 验）。
- 已知工程债：evaluate 各因子独立拉面板（4 个日线因子重复加载 4 遍 11 年面板），
  值得加共享面板缓存后再扩因子数。

## 结果登记

- trials.parquet：wave-1 全部 32 (factor, horizon) 试验已登记。
- 报告：research/output/evaluate_<factor>_2025-05-16_2026-07-02.md。
