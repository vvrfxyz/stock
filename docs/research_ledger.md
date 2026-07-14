# 研究总账（Research Ledger）

**目的**：防止重复研究。所有已裁决的研究问题（因子、假设、方法论）在此各占一行/一节，
记录**结论、为什么、去哪看细节**。开新研究前先查此表；每轮研究结束必须回写此表。

规矩：

- 结论分四档：**可用**（进入生产候选）/ **调味料**（自身不可变现，但可做执行叠加或复合信号成分）/
  **蚊子腿**（真实但净后不可变现，已结案）/ **死亡**（统计上不存在或被长窗证伪）。
- 只登已裁决的问题；进行中的挂在"开放问题"节。
- 机器可读的完整试验记录在 `research/output/trials.parquet`（防 p-hacking 的原始台账，
  6,000+ 行，每次 evaluate 自动追加）；此文档是它的人读摘要层。
- 显著性口径（2026-07-08 更新，W0-P3）：Newey-West t，门槛用 `trials report` 的**动态
  Bonferroni z 阈值**（分母 = 该因子假设检验型 trial 数；study 行/部署判定不计入分母）。
  两层标准：发现级结论过动态 Bonferroni；变现/部署类 study 用各自预注册阈值（如 t>=2）。
  动态阈值仅适用于 2026-07-08 起的新 trial，**既往裁决不因阈值切换重开**（旧行按当时
  钉死的 ≈2.9 口径理解）。短窗结论一律标注"待长窗确认"。

---

## 因子裁决表

| 因子 / 假设 | 数据源 | 长窗最佳结果 | 裁决 | 一句话原因 | 细节 |
|---|---|---|---|---|---|
| **residual_vol（特质波动，对市场单因子残差）** | 日线 | 全 horizon 过 Bonferroni；**wave-9 终审：效应只住在小盘桶**（桶内 IC 小盘 .0343/t=4.84、中盘 t=1.4、大盘 t=-0.2） | **结案：真实但不可收割（2026-07-07，散户口径复核维持）** | size 中性化部署检验 FAIL（对桶匹配基准 alpha 毛 +1.9%/年 t=1.0）。**散户口径（$20k/30 只/40bps，wave-10b）复核仍 FAIL 且方向恶化**：小盘桶内 q5 年化 10.7% vs 桶等权基准 13.1%——低波动 q5 长腿在小盘跑输桶均值 2.4pp/年（效应的肉在躲 q1 不在抱 q5）；30 只持仓延续子组合中位 8.6%（集中度损耗 ~2pp 实测）。**vol 族排序信息统计铁证、经济无出口——机构和散户口径双重结案** | wave-5/9/10b |
| low_vol（低波动溢价，Ang 2006 总波动版） | 日线 | IC t=4.38/3.13 @h1/h5；**变现：2016-2026 对 spy CAPM alpha -0.9%/年（t=-0.39）** | **调味料（2026-07-07 降档：排序主干，非独立 alpha）** | 预注册变现研究判死：q5 纯多头年化 10.6% 但对 spy 超额几何 **-4.0%/年**、全部 5 相位 alpha 皆负、对基准水下 2517/2638 天；2007-2015 伪样本外 +2.65%/年（t=1.69，含 2008，防御属性 down_capture 0.66-0.72 真实）——与文献"低波动黄金期在 2010 前、此后拥挤化"一致。**IC 排序信息保留**（复合成分价值），独立多头部署否决 | wave-5、lowvol_monetization 报告×2 |
| max_lottery（彩票股 MAX 效应，Bali 2011） | 日线 | IC .034，t=+5.03，全 horizon ≥2.9（2016-2026） | **调味料**（降档 2026-07-06） | wave-4 相关研究揭示其 74% 是波动效应马甲：对 low_vol 正交化后 partial IC .0271→.0075；独立变现价值转移给 low_vol，仅剩小分支残差 | wave-2 终审、wave-4 |
| high_52w（52 周新高锚定，George-Hwang 2004） | 日线 | IC .015-.028，t=3.78 @h1 过 Bonferroni | **调味料** | 吸收动量（GH2004 原文结论在我们数据复现）但自身又大半被 low_vol 吸收（partial .0057）；LS 净后负 | wave-4 |
| momentum_12_1（12-1 动量，Jegadeesh-Titman 1993） | 日线 | IC .013，t=3.85 @h1；t 随 horizon 快速衰减 | **死亡（被吸收）** | 显著但对 high_52w 正交化后 partial IC 归零（.0133→.0021）——"美股最强异象"在 2016+ 的横截面增量为零；若做动量敞口，用 high_52w 或 low_vol 表达更优 | wave-4 |
| eod_reversal / eod_reversal_flow（尾盘位移次日反转，流量条件化） | 分钟 | flow 版 t=3.74 @h5 全过 Bonferroni；隔夜腿 t=+9.1 | **蚊子腿**（已结案 2026-07-06） | 反转 100% 在隔夜，次日盘中续行精确抵消（t=-5.4），close-to-close 零；隔夜单腿扣成本剩 2-4 bps/日且含未剔除的弹跳。可做执行择时叠加（调仓单挑尾盘放量被砸时点收盘成交，白捡隔夜反弹）。**wave-4 佐证：与全部日线因子秩相关 |r|<0.03，作为复合成分完全正交** | [wave-3](research_technical_factors_2026-07.md)、eod_decomposition 报告 |
| last30_persistence（尾盘动量续行，HKS 2010） | 分钟 | IC -.006，t=-2.57 @h1（**符号与文献相反**） | 死亡（原假设）；反向版即 eod_reversal（见上） | 发表后反转：2016+ 尾盘强者次日回吐，疑与收盘竞价流崛起有关 | 同上 |
| short_term_reversal（21 日反转） | 日线 | IC .011，t=+1.94 @h10 | 死亡 | 长窗不显著；经典效应在近十年美股已弱化 | wave-2 终审 |
| signed_jump（有符号跳跃 RSJ） | 分钟 | t=+1.73 @h21 | 死亡 | 发表后衰减殆尽 | 同上 |
| realized_skew（已实现偏度，Amaya 2015） | 分钟 | t=+1.51 @h10 | 死亡 | 同上；且分钟精确版不优于日线影线代理 | 同上 |
| shadow_asymmetry（K 线影线不对称） | 日线 | t=+1.30 @h5 | 死亡 | 短窗 t=2.0 被长窗证伪——短窗显著性教训的标本 | 同上 |
| smart_money_gap（尾盘-开盘收益差） | 分钟 | t<1 | 死亡 | 无信号 | 同上 |
| close_vwap_pressure（收盘对 vwap 偏离） | 日线 | t<1（短窗即死） | 死亡 | wave-1 即淘汰，未进长窗 | wave-1 |
| **composite_v2（low_vol + high_52w 残差 + size + operating_profitability，0.5 中性填补）** | 日线+PIT 市值+XBRL | §7 双条件 FAIL（q5 净 Sharpe 0.734<low_vol 0.743）；fixed 散户终审 FAIL（40bps t=1.458）；**measured 终裁（2026-07-09，min_periods 双档）：t=2.130/2.165 双档判据 PASS、双档 q5 覆盖 63.5%/66.8%<70% 资格线**。同批对照：v1 出现档间 verdict 翻转（1.969 FAIL→2.001 PASS，刀刃敏感）而 **v2 双档稳过不随杠杆晃**；OP/EY 双档皆 FAIL——翻案严格限于 v2 | **终局：翻案候选、数据边界拦截（2026-07-09 定格）——判据 PASS×资格线未达，纸面组合不启动** | measured 重审=§6 预注册路线（判据零改动、只换实测成本；q5 实测单边中位 8.9-9.3bps vs fixed 单边 40 高估 4 倍余）。覆盖缺口经四类拆解为结构性（join bug=0、无分钟史 6-7%、thin 交易+新票短史为主体；n_bars 门 100 永久不降——thin 段 cs 向下偏 2.4×，降门=造翻案；min_periods 合法杠杆用尽 64→67%）。**解锁=数据自然累积**（分钟史逐年增厚/q5 栖息地流动性漂移），届时同口径重跑即终裁——悬案经得起任何复查，无一步调参。是否带星号做小规模纸面练习属 owner 决策非研究判据 | measured 终裁报告×4（双档）、study 行×8（2026-07-09）、trials 台账 |
| **composite_v1（3 信号：low_vol + high_52w 逐日残差 + size，0.5 中性填补）** | 日线+PIT 市值 | size 版 IC .0292 t=3.79 @h5；q5 净 Sharpe h21 0.718；**散户复审（2026-07-08，口径 v2）：40bps alpha t=0.36、IR 全成本档为负、30 只子组合中位 8.65% vs 桶基准 12.63%（−4pp/年）** | **调味料（2026-07-08 降档：排序信息真实、散户不可收割）** | 排序双条件 PASS（wave-8 size 换脚重审维持）后，**retail_reality 散户口径复审双口径 FAIL**（判据 40bps alpha t>=2 且子组合中位超额>0，两半边都不过：alpha t 0.36 远低于 2，中位超额 −4pp）；EXCHANGE_DROP −0.30 敏感性口径数字几乎不动（alpha t 0.362 vs 0.356）——结论对退市口径稳健。注意口径 v2（引擎统一 run_backtest + 退市实测注入）比 wave-10b 旧快循环更严：残差 vol 时代虚增的小盘 q5 收益已挤出。**与 residual_vol/low_vol 同一死法：小盘桶 q5 的抱团肉不够厚，跑不赢桶等权基准**。IC 排序信息保留（复合成分/LS/择时出口未验）；按预注册分叉，主攻权重全转 earnings_yield 与基本面族，**不做参数挽救**。4 信号版 FAIL 是符号反转成分的直接实证 | wave-6/8、composite_v1_sizeswap 报告、retail_reality_composite_v1_*_v2 报告×2、trials study 行×2 |
| insider_cluster（多内部人非例行集群买入，CMP 例行过滤） | SEC Form 4 | IC 负向弱（t=-0.98~-1.37，全部 noisy） | **死亡（横截面 IC 口径，2026-07-07）** | 侦察队 40-50% 基率落空：条件横截面太薄（~294 只/日）且方向不符；其预警的"事件稀疏杀日频 rank IC"命中。事件研究（CAR）口径未测试——挂低优先级开放问题 | wave-7 |
| institutional_breadth（13F 持仓机构数） | SEC 13F | IC t=5.82~3.81 全过 Bonferroni，**但对 size 正交化后 partial IC = .0015（归零）** | **死亡（size 马甲，2026-07-07 当夜降档）** | 与 size 秩相关 **0.893**；"全项目最强单因子"称号存活 3 小时即被冗余关卡斩落——13F 机构数 ≈ 盘子大小，其全部 IC 是 2016-2026 大盘制度。变现实测同判（q5 对 spy t=-0.5；ex_q1 +0.76% t=1.4 不显著）。size 反向仍存活（size\|breadth=.0108）→ 吸收方向 size ⊃ breadth | wave-6/8 |
| **ta_combo 四假设**（wave-11：反转集成 / 放量确认反转 / 锚定门控 / ATR 归一化超级趋势） | 日线量价 | H2 放量确认反转 t=2.88/2.41/2.28/2.18——**全 4 horizon 单调强于裸版**（2.55/2.23/2.04/1.97）但最高 2.88<2.9 不过线；H1 集成 t=2.24 反而低于最好成员；H3 锚定半区差 **精确为零**（t=0.0007）；H4 ATR 趋势 \|t\|<0.6 | **全族死亡/不过线（2026-07-07）** | 预注册四假设三死一悬：量能条件化机制获**第二次独立验证**（继 wave-3 日内版后，日频版全 horizon 单调增强——机制真实，但基础信号太弱托不过 Bonferroni）；"四指标共潜因子"假设证伪（集成加噪不减噪=成员噪声相关）；锚定不调制反转（但半区内分层 IC .0148 t=3.3-3.9 双双高于全截面——分层去噪的事后观察，未预注册不入账）；超级趋势/ATR 族整族结案。**日频反转在本市场的天花板 ≈ t2.9，怎么修饰都差一口气；量能条件化机制的家在日内** | wave-11、docs/wave11_hypotheses.md |
| **ta_zoo 八指标**（obv_slope / adline_slope / mfi_14 / rsi_14 / macd_hist / bollinger_b / donchian_pos / sma_gap_50） | 日线量价 | 2016-2026 全网格：无一以预注册方向过 Bonferroni（最接近: bollinger_b +2.55@h1、macd_hist **-2.95**@h1 但与预注册趋势方向相反=又一个反转皮） | **死亡（全族，2026-07-07）** | 用户点名补测的经典技术指标动物园：量价族（OBV/AD/MFI）——唯一真没测过的方向——同样死（OBV 甚至符号反）；振荡器族弱反转味（t 1.3-2.5 不过关）；趋势族（MACD/SMA gap/唐奇安）在日频横截面口径为零或反向。预注册假设（"大多是已裁决因子的马甲"）被证实。**此后任何"XX 指标要不要试"先查此行** | wave-10 |
| size（log 市值，方向=大盘为高分） | PIT 市值 | 2016-2026 IC .027 t=4.85；**2008-2015 弱化到 t=2.1@h1 且随 horizon 衰减到 0.14** | **制度因子（非 alpha）** | "大盘碾压小盘"是 2016 后的制度而非持久异象；它是本窗口一切"强因子"的隐藏主轴（吸收 breadth，解释等权组合对 spy 的系统性拖累）。部署无意义（=买超大盘）；价值在**作为冗余关卡的默认对照**与风险归因 | wave-8 |
| delta_institutional_ownership（季度 IO 变化） | SEC 13F | 长窗 IC **为负**（t=-2.25~-2.44，均不过 Bonferroni） | **死亡（2026-07-07 降档：符号不稳）** | 短窗（2026-07-02 重评估）"存活"被长窗翻案且符号相反——2016-2026 机构增持预示**低**收益；作为复合成分实测拖垮 4 信号版（预注册判据 FAIL 的直接原因） | wave-6 |
| days_to_cover（空头回补天数） | FINRA 空头 | 死亡（2026-07-02 重评估） | 死亡 | 重评估未存活 | 同上 |
| **earnings_yield（盈利收益率，NI_TTM/公司级市值）** | XBRL+PIT 市值 | 2012+ 正式重跑（P2 退市注入新口径）：IC .0126-.0269，**nw_t h1 5.59 / h5 3.88 / h10 3.64 / h21 3.29 全过动态阈值且 IC 随 horizon 递增**；三关对照全过（\|size 衰减 24%、\|low_vol 53%、\|high_52w 17%，与 size 秩相关仅 0.200）；栖息地=小盘独占（桶内 IC 小盘 .0241/t=4.89）；size 中性化部署 FAIL（桶匹配 alpha 0.7-0.9%/年 t≈0.4）；**散户复审双口径 FAIL（2026-07-08 终审）**：40bps alpha **+2.76%/年 t=1.08**（<2 不过显著性关），子组合中位超额 **+4.1pp/年为正**（第二判据过）——EXCHANGE_DROP 敏感性≈0 | **调味料（composite_v2 核心候选；2026-07-08 终审）** | 项目至今最厚的非价格因子、**第一个失败形态为"正而不显著"的散户复审**（composite_v1/residual_vol 是负超额，此处是 alpha 点估计 +2.8%、中位超额 +4.1pp 但 t=1.08 差显著性）。三关全过=与价格族正交的独立信息维度，按预注册分叉进 composite_v2 成分池——若正交合成把 alpha t 抬过 2，在 v2 的 retail_reality 终审过关（那是预注册路径不是挽救）。独立部署按纪律否决。partial 为序列级近似（尾注同前） | wave-12 前哨；2012 正式 trial、三关/栖息地报告、retail_reality \*_v2 报告×2 + study 行×2（2026-07-08） |
| **wave-12 基本面族四因子**（gross_profitability / accruals / operating_profitability / asset_growth，预注册 docs/wave12_fundamental_hypotheses.md） | XBRL 2012+ | **OP（EBIT/AT）h1 t=7.21 全 horizon 全过 + 三关全过**（\|size −32%、\|low_vol −45%、\|high_52w −22%）+ **栖息地全桶显著**（小盘 t=4.91/中盘 2.65/大盘 2.05——全项目独一份）+ size 中性 alpha 1.5-1.8%/年 t 1.25-1.47；**散户复审双口径 FAIL（2026-07-08 终审）：40bps alpha +3.03%/年 t=1.49**（历史最接近显著性关）、子组合中位超额 +0.5pp 薄正；EXCHANGE_DROP 敏感性≈0；GP 仅 h1 过且 GP\|OP=−.0015 完全吸收；accruals 符号反；asset_growth 零 | **OP 调味料（composite_v2 基本面腿；2026-07-08 终审）；GP 死亡（被 OP 吸收）；accruals/asset_growth 死亡（预注册死刑）** | 诚实先验命中。**OP 是新科最强因子且格局质变**：首个全桶显著、三关全过；EY\|OP=−75% 而 OP\|EY −38%——**OP ⊃ EY，基本面腿由 OP 接管，EY 退候补**。单因子散户复审失败梯队 composite_v1(t=0.36,负超额)→EY(1.08,正)→**OP(1.49,正)**——距显著性关逐级逼近但皆差一口气，正是 composite_v2 正交合成的存在理由（终审进行中）。accruals 之死是 Sloan 套利殆尽教科书案例 | wave-12 battery/三关/栖息地/retail 报告（2026-07-08）、study 行×2 |
| **f_score（Piotroski 5 组件子集：ROA/CFO/ACCRUAL/ΔROA/EQ_OFFER，partial/k≥4）** | XBRL 2012+ | **h1 t=8.22 全项目历史最高**、全 horizon 4.94-8.22 碾压过线、IC 随期限递增、**LS 净 Sharpe 全 horizon 为正（全项目唯一）**；三关全过（\|OP 衰减 43%、\|size 28%、\|low_vol 41%、\|high_52w 23%——与 OP 秩相关 0.542 但未被吸收，反向 OP\|fs 衰 48% 互不吞并）；**栖息地全桶显著**（T1 4.78/T2 3.75/T3 4.06，第二例）；size 中性 FAIL（alpha 1.2-1.6% t 0.99-1.31）；**散户终审双模式 FAIL（2026-07-09）：fixed 40bps t=0.454、measured 双档 t=0.70/0.72（q5 覆盖 59/62% 亦不足）** | **调味料（2026-07-09 终审；排序王者、散户出口同样不开）** | wave-12 最后一格的反转剧：预注册悲观预期（"OP 近亲速死"）被打脸——三关全过、与 OP 互为独立维度；**但散户关死得比谁都干脆（t 0.45-0.72，连 v2 的 2.13 都摸不到）**：F-score 的 q5 是"财报健康的正经公司"，在小盘桶内恰好是**最不便宜**的那批（q5 年化 13.0% vs 桶基准 12.1%，抱团溢价薄）——排序强度与 q5 绝对肉厚是两回事的最极端标本（"IC≠超额"第四次也是最响一次应验）。composite 家族出口（fs×价格族复合）与 LS 出口（唯一净后为正的 LS）挂开放问题 | wave-12 H5、三关/栖息地/retail 报告、study 行×3+trial（2026-07-09） |
| **wave-13 path_momentum 双假设**（residual_momentum_12_1 / information_discreteness_12_1，预注册 docs/wave13_path_momentum_hypotheses.md） | 日线 2007+ | H1 主判据 h21 IC .0015/t=0.204（门槛 t≥3；h1 t=2.21 但随 horizon 速衰），稳定腿 h21 t=0.235、LS 净 Sharpe 四 horizon 全负；H2 FIP 主样本 126d spread −1.5%/t=−0.499（预期为正）、21/63d 同负、loser 分量 −2.2%，稳定腿 ≈0（t=−0.012） | **全族死亡（2026-07-12）** | 残差路径与信息连续性在本市场横截面均无增量——"动量路径修饰"整族关闭，按停止条件不试 slope/R²/efficiency ratio 近义变体；市场代理与预注册"当日全 CS"有轻微口径偏差（evaluator 宇宙均值）已记录，不影响量级 | research/output/wave13_2026-07-12/ 全套报告+JSON+trials 快照、预注册文档 |
| **wave-14 earnings_gap 双假设**（gap_atr / gap_atr_volume_confirmed，8-K Item 2.02 + accepted_at 事件锚，预注册 docs/wave14_earnings_gap_hypotheses.md） | SEC 8-K + 日线 | 主样本 75,945 完整事件/2,495 cohort：25bps 单边净日均 h20 −2.15bps/t=−3.21 与 −2.08/t=−3.00（1/5/20 日全负）；10bps 档 h20 仍负；h20 毛事件 CAR 仅 4.4-6.0bps（盖不住一次便宜往返）；稳定腿 h20 净负 | **全族死亡（2026-07-12）** | 收盘后建仓的财报缺口 continuation 净后不存在；量能确认只是负值间相对改善（+0.07bps/日），不留调味料。事件时点/财期去重/日历时间组合基建可复用于未来新事件源（如 insider CAR）；本地未保留冻结原始输出（artifact 内嵌 36 行独立复算，误差<5e-11）——provenance 缺口见方法论节 | wave14 report artifact（research/output/）、预注册文档 |
| **wave-15 market_regime_overlay 三规则**（spy_10m_trend / breadth_200d / trend_and_breadth，预注册 docs/wave15_market_regime_overlay_hypotheses.md 含 v1→v2 勘误） | SPY+PIT CS 2007+ | 四单元（SPY/CS 等权 × 2007-15/2016-26）判据全 FAIL：主样本最好 Sharpe 改善 +0.046（门槛 +0.10，trend_and_breadth/SPY）；2008 防御真实但 2016+ 现金拖累精确命中预注册 FAIL 条款；v1→v2 仅修 7 月末时点（最大 Sharpe 差 1.1e-4、0 verdict 变化）；独立审计 12 裁决单元/24 危机单元/8 价差摘要复算误差全 0 | **全族死亡（2026-07-12）** | 简单月频趋势/breadth 覆盖层过不了"回撤改善≥10pp × Sharpe+0.10 × 复利损失≤2pp"三门槛；按停止条件不升级 HMM/change-point/波动目标/ML regime | market_regime_overlay_v2_* JSON/MD + wave15_market_regime_overlay_independent_audit.json + v1 审计痕迹 |
| **concentrated_topk 部署研究**（composite_v2 / f_score / operating_profitability × top-K 集中多头 + 双速退出，预注册 docs/concentrated_topk_hypotheses_2026-07.md，clean-SHA 56bc31a 跑数） | 日线+XBRL+cs_spread 2012+ | 三腿 E0（K=10/252 日/measured/2016-01-08~2026-07-10）判据全 FAIL：**composite_v2 超额 −4.4%/年**（随机池 0.48 分位、sub2021 −6.2%、剔最大 spell −4.8%）；**f_score −1.7%/年**（唯一相位判据过：4 相位中位 +1.98%，但随机 0.66 分位、sub2021 −3.4%）；**OP −9.9%/年**（随机 0.09 分位——比抽签还差）。E3 双速退出全部 judged=False：fs/v2 的 E2 Chandelier 确有防御形态（Sharpe +0.10~0.14、MDD 改善 5-6pp<10pp 门槛）但不得翻案；OP 的 E3 反而更糟（Sharpe −0.14）。稳定腿分裂：v2 2013H2-15 +3.0%/年 vs f_score −3.8% | **全族死亡（2026-07-13，跑数当日结案）** | **顶端凸性假设被证伪、且方向反转**：q5 分位有排序信息（f_score t=8.22）不等于 top-10 有肉——**top-10 尖端反而系统性弱于分位组合**（fs q5 曾 +0.9%/年 vs top-10 −1.7%）；离散分数 + 流动性并列打破实际选中"最高分段里的大流动性票"，凸性被并列键稀释是候选解释（但按停止条件不开"换并列键"变体）。win_rate 66-76% × 负超额 = 长持有小赢频繁、错过的大赢家在买不到的那部分。等权持有漂移 + 252 日锁仓也锁死了动量收割。owner 交易形状（5-10 只半年-一年）在这三个信号上**没有可部署出口**；E0 全败停止条件生效——不换第四选股器、不调 K/持有期/并列键/退出参数 | 三选股器 JSON/MD/daily/spells/random parquet（双机 research/output/）、study 行×6（253 台账，code_git_dirty=False）、独立复算审计 max_err<9e-16 |
| **wave-16 market_intraday_momentum**（GHLZ 2018 复现+发表后检验：SPY + PIT CS 总市值加权组合，r1=昨收→10:00 预测 y=15:29→15:59，预注册 docs/wave16_market_intraday_momentum_hypotheses.md，clean-SHA 68b08b6 跑数） | SPY 分钟 2004+ / CS 2010+ | 两资产判据 0/4 全 FAIL：**SPY 复现窗 β=+0.073/t=2.92<3**（方向与文献一致但强度不足）、发表后 β=+0.020/t=0.64（衰减 73%）、sign 交易 2bps 净 −4.06bps/日（t=−4.9，1bps 档仍 −2.06）、剔 10 极端日 −4.36；**市场组合复现窗即无效**（β t=0.20），发表后 t=0.83；两资产逐年净收益 2019-2026 **无一年为正**；毛信号本身太薄（|y| 日均 ~几 bp，β·σ(r1) ≈ 0.5bp/日）盖不住任何成本档 | **全族死亡（2026-07-13，跑数当日结案）** | 文献效应在本数据上"方向存在、可交易性为零"：复现窗方向为正说明实现无错，但 GHLZ 的样本（1993-2013，SPY 早期高波动+高自相关段）之后效应已经衰减到 t<1；发表后八年逐年全负是干净的发表后衰减标本。按停止条件不做阈值/条件化/其他窗口/行业个股扩展；r1_intraday 描述腿两资产皆弱（β 甚至为负）不构成新假设 | 两资产 JSON/MD/daily parquet（双机 research/output/）、study 行×2（dirty=False）、独立复算审计 max_err=0 |
| **wave-17 same_month_seasonality**（same_month_1_10 = lag 1-10 年同月收益均值 min 3 观测，KLN 2016 稳健口径，预注册 docs/wave17_same_month_seasonality_hypotheses.md，clean-SHA e506f18 跑数） | 日线 2003+ 月网格 | 主窗口（持有月 2016-01~2026-06，126 月）判据 1/4：**H1 IC −0.0079/t=−1.35（符号即反）**、H2 对 momentum_12_1 残差化 partial −0.0094/t=−1.60、H3 LS 净 25bps **−1.03%/月 t=−4.65**（毛 −0.24%/月就为负，10bps 档 t=−2.52——不是成本问题）；仅 H4 稳定腿（2007-2015）IC +0.0142 符号为正；逐年 IC 2016+ 11 年中 8 年为负；1 月 IC −0.043 最差（税损卖压反向）；月换手 Σ\|Δw\|=3.13 极高 | **全族死亡（2026-07-13，跑数当日结案）** | 与 wave-16 同构的**发表后/样本外衰减+反转**标本：2007-2015 弱正（+0.014，量级仅文献一半）→ 2016+ 反号——季节性在近十年美股不但衰减而且**反向**（拥挤反转的典型形态）。calendar_technical family 关闭：按停止条件不做形成窗/单 lag/加权变体；turn-of-month/day-of-week 不获自动续命（须全新假设+同族分母）。**技术线五连败（wave 13-17）定弧** | 月序列 parquet + JSON/MD（双机 research/output/）、study 行×1（dirty=False）、独立复算审计 max_err=0 |
| size / short_interest_ratio / short_volume_ratio / ownership_concentration / insider_net_buy | 各 PIT 源 | 基线因子，未做长窗攻坚（size 另有制度因子行） | 基线 | 作为框架验证与对照基线维护 | docs/factors.md |

## 方法论裁决（同样防止重复踩坑）

- **thin-bar CS 价差估计向下偏，n_bars 门 100 不降（2026-07-09 裁决，永久结案）**：
  日内 50-99 bar 段的 cs_spread 中位比 100+ 段**系统性低 2.4×**（5.2 vs 13.4bps@2020、
  3.1 vs 7.3@2025——bar 稀释相邻桶极差估计，不是真便宜）且负估计剔除率高 6-8pp
  （噪声代理），该段恰好压在 q5 缺覆盖票上。降门冲覆盖率 = 用系统性低估的成本
  "造"翻案，与 measured 立意正对冲——**覆盖资格线宁缺毋滥**，thin 票走 40bps
  fallback 是保守正确。读取层 min_periods 宽容（20→10）是唯一合法覆盖杠杆
  （吃"有厚数据但窗口天数不足"的干净票），按口径变体纪律双档并排入台账。
- **cost_bps 语义 = 单边每单位换手成本（2026-07-09 澄清，B 口径裁决）**：引擎
  cost = turnover(Σ|Δw|) × cost_bps，开满仓 turnover=1 收一次 cost_bps——旧文档把
  20/40/80 档叫"往返"是**误称**，实收单边（40 档 = 往返 80bps）。含义：历史散户复审
  四连 FAIL 的实际成本假设比名义更严苛（单边 40 vs 小盘实测单边中位 6-10bps，高估
  4-6 倍）。fixed 档数字不动（存量 trial 连续性），measured 模式按同一单边口径对齐
  （cs_spread/2 × (1+0.5)）。
- **wave-13/14/15 首轮结果产自未提交/dirty 工作区（2026-07-13 记录）**：三轮预注册、
  实现与结果先于"实现冻结提交"存在，台账 code_git_sha 指向不含研究代码的旧 HEAD
  （append_study 的 trial_id 含 SHA 但不含脏源码内容哈希）。数字经独立复算无翻案
  风险（wave-15 零误差、wave-14 内嵌 36 行复算 <5e-11），但 Git 无法唯一恢复当时
  执行的代码——**教训：任何 study 跑数前先做实现冻结提交**；事后 clean-SHA 重放
  只算可复现性重放，不算新样本外证据。wave-14 另有证据打包缺口：冻结原始
  JSON/MD/Parquet 未保留在本地 checkout，仅存自包含 artifact。
- **短窗（约 14 个月）显著性不可信**：shadow_asymmetry 短窗 t=2.0 长窗 1.3、wave-1 资金流族
  "净后 Sharpe 1.0-1.6" 全被长窗证伪。**任何新因子结论必须以 10 年+ 窗口为准。**
- **分钟数据在"偏度/矩量族"没有增量**：分钟精确偏度 ≤ 日线影线代理。分钟数据的真实
  价值在**只能用它构造**的变量（尾盘量占比、日内分段收益）与执行层。
- **IC 显著 ≠ 基准超额（wave-5 升级版教训）**：IC 是横截面**排序**信息；纯多头分位组合
  的对基准 alpha 是**水平**问题——low_vol/residual_vol 全 horizon 过 Bonferroni 的排序强度
  与 2016-2026 对 spy alpha≈0 并存（beta 0.73 的组合在大牛市 excess_geo -4%/年）。
  变现判定必须走预注册的超额空间 CAPM（`research/lowvol_monetization.py` 模板：
  超额空间回归防 (1-β)·rf 假 alpha、spy 总收益断言、普通股过滤、相位错峰、成本压力档、
  伪样本外腿）。**排序信息的变现出口是 LS/复合/择时，不是裸多头分位。**
- **spy 分红因子链 2007-03 前缺失**：更早窗口的 spy 总收益是纯价格口径，任何
  benchmark-relative 研究下限 2007-07（脚本内断言已防）。
- **评估引擎已向量化**（2026-07-06，commit 6052c84，金测试锁位级等价）：单因子全指标
  63s（2,890×7,000），面板装载走 COPY + 进程内缓存（`research/factors/price_cache.py`）。
  写新研究脚本时禁止逐日 Python 循环，复用 `_quantile_weight_matrices`/`_masked_rowwise_corr`。
- **多重检验记账**：变体族用统一 family 前缀（如 eod_pressure），全部试验进 trials.parquet；
  评估过的 (factor, horizon) 网格已 80+ 组，单次 t=2 出头的"发现"先默认是噪音。
- **异象的"栖息地诊断"必须做（wave-9 定式）**：桶内 IC 定位效应住在哪个市值层——
  vol 族整族效应只在小盘桶（IC .034 t=4.8）、中盘弱、大盘零。凡效应只住在小盘的，
  成本假设按 30-80bps 现实档重审，"IC 显著"不再自动进入变现研究。
  `research/size_neutral_study.py` 是模板（桶匹配基准按构造扣掉 size 暴露）。
- **价格族冗余结构已裁决（2026-07-06，wave-4）**：六个价格系因子 ≈ **2.5 个独立信号**。
  low_vol 是主干（吸收 max_lottery r=.74/partial .0075，大半吸收 high_52w），
  momentum_12_1 ⊂ high_52w（partial .0021，GH2004 复现），eod_reversal_flow 完全正交
  （与全部因子 |r|<0.03）。**任何新因子（不限价格族）登记"新发现"前，必须过三关对照的
  partial IC：size、low_vol、high_52w**（`research/factor_correlation.py` 十分钟跑完；
  其 partial IC 是序列级近似，正式结论需逐日截面回归残差确认）。
  战果记录：该关卡当夜斩落 institutional_breadth（对 size 归零）——凡"强得可疑"的
  新因子，先问它是不是 size/vol/动量制度的马甲。

## 开放问题（下一轮候选，按预期肉厚排序）

技术/价格族研究弧（wave 1-17）已完整结案：日频横截面（wave 1-11）、基本面族
（wave 12）、路径动量/财报缺口/市场覆盖层（wave 13-15）、市场日内动量（wave 16）、
同月季节性（wave 17）全部裁决完毕。~~composite_v1 散户口径复审~~（2026-07-08
FAIL 结案）；~~基本面族长窗攻坚~~（wave-12 收官：OP/EY 调味料、f_score 排序王者、
GP/accruals/asset_growth 死亡）；~~liquidity_lambda~~（已兑现为价差面 2003-2026
全量 + measured 成本档基建）；~~concentrated_topk~~（2026-07-13 三腿 E0 全 FAIL
结案——owner 交易形状在现有三信号上无部署出口，"顶端凸性"反向证伪）；
~~Wave 16 market_intraday_momentum~~（2026-07-13 两资产 0/4 全 FAIL）；
~~Wave 17 same_month_seasonality~~（2026-07-13 判据 1/4 FAIL——2016+ IC 反号，
拥挤反转形态）。**技术线五连败（wave 13-17）定弧：公开价格/日历结构里的
可收割 alpha 在 2016+ 美股已系统性磨平——新技术假设的立项门槛自此抬高
（须有 2019+ 仍存活的独立文献证据或全新数据源，不再接受纯"经典异象复现"）。**

1. **fs×价格族复合 + f_score LS 出口**（唯一净后全 horizon 为正的 LS）——
   f_score 残余价值所在（新 family，须另行预注册；不是 top-K 的变体复活）。
   LS 出口对 2 万美元账户有做空可行性门槛，立项前先做可行性勘察（融券
   可得性/成本近似）。
2. **composite_v2 覆盖率年检**（数据自然累积解锁）：q5 measured 覆盖过 70%
   资格线即重跑 retail_reality 终审——当前唯一"判据已过、只差数据资格"的
   部署候选。
3. peer_lead_lag（数据阻塞：需 PIT classification history）；insider_cluster
   CAR——均挂起。

## 工程债（研究基建）

- **研究脚本一律在 253 本机跑（owner 指令，2026-07-07）**：跨网冷拉 GB 级面板
  在 I/O 争抢下可达 10-15 分钟，本地 socket 几十秒。Mac 只做编码/提交，
  评估/研究一律 SSH 到 253 执行（`.env` 的 DATABASE_URL 即本地库，evaluate 的
  load_dotenv 自动生效）。parquet 磁盘快照缓存作为后备优化保留在册。
- ta_zoo 教训固化：新因子模块的 buffer_days 走 price_cache 量化档（{200,420}），
  不要自定义中间值。

## 登记流程（每轮研究收尾必做）

1. evaluate 自动写 trials.parquet（勿手工编辑）。
2. 研究备忘录写详细过程（如 `research_technical_factors_2026-07.md`）。
3. **回写本表**：因子裁决表加行（或改档），方法论有新教训加条，开放问题增删。
4. 提交 git（只 add 自己的文件）。
