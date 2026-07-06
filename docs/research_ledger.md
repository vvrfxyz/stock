# 研究总账（Research Ledger）

**目的**：防止重复研究。所有已裁决的研究问题（因子、假设、方法论）在此各占一行/一节，
记录**结论、为什么、去哪看细节**。开新研究前先查此表；每轮研究结束必须回写此表。

规矩：

- 结论分四档：**可用**（进入生产候选）/ **调味料**（自身不可变现，但可做执行叠加或复合信号成分）/
  **蚊子腿**（真实但净后不可变现，已结案）/ **死亡**（统计上不存在或被长窗证伪）。
- 只登已裁决的问题；进行中的挂在"开放问题"节。
- 机器可读的完整试验记录在 `research/output/trials.parquet`（防 p-hacking 的原始台账，
  6,000+ 行，每次 evaluate 自动追加）；此文档是它的人读摘要层。
- 显著性口径：Newey-West t，长窗（10 年+）Bonferroni 门槛 ≈ 2.9；短窗结论一律标注"待长窗确认"。

---

## 因子裁决表

| 因子 / 假设 | 数据源 | 长窗最佳结果 | 裁决 | 一句话原因 | 细节 |
|---|---|---|---|---|---|
| max_lottery（彩票股 MAX 效应，Bali 2011） | 日线 | IC .034，t=+5.03，全 horizon ≥2.9（2016-2026） | **调味料**（变现待验证） | 统计碾压级但效应在难做空的空头腿+日频高换手，等权 LS 净后亏；**月频低换手变体未验证**（开放问题#1） | [wave-2 终审](research_technical_factors_2026-07.md) |
| eod_reversal / eod_reversal_flow（尾盘位移次日反转，流量条件化） | 分钟 | flow 版 t=3.74 @h5 全过 Bonferroni；隔夜腿 t=+9.1 | **蚊子腿**（已结案 2026-07-06） | 反转 100% 在隔夜，次日盘中续行精确抵消（t=-5.4），close-to-close 零；隔夜单腿扣成本剩 2-4 bps/日且含未剔除的弹跳。可做执行择时叠加（调仓单挑尾盘放量被砸时点收盘成交，白捡隔夜反弹） | [wave-3](research_technical_factors_2026-07.md)、eod_decomposition 报告 |
| last30_persistence（尾盘动量续行，HKS 2010） | 分钟 | IC -.006，t=-2.57 @h1（**符号与文献相反**） | 死亡（原假设）；反向版即 eod_reversal（见上） | 发表后反转：2016+ 尾盘强者次日回吐，疑与收盘竞价流崛起有关 | 同上 |
| short_term_reversal（21 日反转） | 日线 | IC .011，t=+1.94 @h10 | 死亡 | 长窗不显著；经典效应在近十年美股已弱化 | wave-2 终审 |
| signed_jump（有符号跳跃 RSJ） | 分钟 | t=+1.73 @h21 | 死亡 | 发表后衰减殆尽 | 同上 |
| realized_skew（已实现偏度，Amaya 2015） | 分钟 | t=+1.51 @h10 | 死亡 | 同上；且分钟精确版不优于日线影线代理 | 同上 |
| shadow_asymmetry（K 线影线不对称） | 日线 | t=+1.30 @h5 | 死亡 | 短窗 t=2.0 被长窗证伪——短窗显著性教训的标本 | 同上 |
| smart_money_gap（尾盘-开盘收益差） | 分钟 | t<1 | 死亡 | 无信号 | 同上 |
| close_vwap_pressure（收盘对 vwap 偏离） | 日线 | t<1（短窗即死） | 死亡 | wave-1 即淘汰，未进长窗 | wave-1 |
| institutional_breadth（13F 持仓机构数） | SEC 13F | 存活（2026-07-02 重评估） | **可用候选** | 13F 族重评估后存活的两因子之一 | deep-review 2026-07-02、trials |
| delta_institutional_ownership（季度 IO 变化） | SEC 13F | 存活（同上） | **可用候选** | 同上 | 同上 |
| days_to_cover（空头回补天数） | FINRA 空头 | 死亡（2026-07-02 重评估） | 死亡 | 重评估未存活 | 同上 |
| size / earnings_yield / short_interest_ratio / short_volume_ratio / ownership_concentration / insider_net_buy | 各 PIT 源 | 基线因子，未做长窗攻坚 | 基线 | 作为框架验证与对照基线维护 | docs/factors.md |

## 方法论裁决（同样防止重复踩坑）

- **短窗（约 14 个月）显著性不可信**：shadow_asymmetry 短窗 t=2.0 长窗 1.3、wave-1 资金流族
  "净后 Sharpe 1.0-1.6" 全被长窗证伪。**任何新因子结论必须以 10 年+ 窗口为准。**
- **分钟数据在"偏度/矩量族"没有增量**：分钟精确偏度 ≤ 日线影线代理。分钟数据的真实
  价值在**只能用它构造**的变量（尾盘量占比、日内分段收益）与执行层。
- **IC 显著 ≠ 可变现**：必须看净后 LS + 换手；对隔夜/日内敏感的信号必须跑归因分解
  （`research/eod_decomposition.py` 可复用，成分收益矩阵 + evaluate 同款分位权重机器）。
- **评估引擎已向量化**（2026-07-06，commit 6052c84，金测试锁位级等价）：单因子全指标
  63s（2,890×7,000），面板装载走 COPY + 进程内缓存（`research/factors/price_cache.py`）。
  写新研究脚本时禁止逐日 Python 循环，复用 `_quantile_weight_matrices`/`_masked_rowwise_corr`。
- **多重检验记账**：变体族用统一 family 前缀（如 eod_pressure），全部试验进 trials.parquet；
  评估过的 (factor, horizon) 网格已 68+ 组，单次 t=2 出头的"发现"先默认是噪音。

## 开放问题（下一轮候选，按预期肉厚排序）

1. **max_lottery 月频变现变体**：月频调仓（换手 383→~12 倍）+ 只做多低 MAX 倾斜，净后能否转正。t=5 的信号值得一次认真的变现尝试。
2. **eod 隔夜腿精修**（低优先级，只改墓志铭不改结论）：开盘后 30 分钟 vwap 锚定剔除弹跳；财报日掩蔽（用 `sec_filings`）。
3. **因子相关矩阵与复合打分**：存活因子（max_lottery、13F 两因子、eod 调味料）间相关性与增量 IC，评估合成价值。
4. **signed_jump 财报跳空掩蔽**：验证其残存信号是否只是财报事件代理（wave-2 遗留，优先级低）。
5. **流动性分桶稳健性**：微观结构效应常聚于小盘，但小盘成本也高——存活因子按 dollar-volume 分桶重跑净后（wave-2 遗留）。

## 登记流程（每轮研究收尾必做）

1. evaluate 自动写 trials.parquet（勿手工编辑）。
2. 研究备忘录写详细过程（如 `research_technical_factors_2026-07.md`）。
3. **回写本表**：因子裁决表加行（或改档），方法论有新教训加条，开放问题增删。
4. 提交 git（只 add 自己的文件）。
