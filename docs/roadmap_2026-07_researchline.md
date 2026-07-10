# 研究主线路线图（2026-07-07 立项，第 1-6 周）v1.1

> 来源：`docs/comprehensive_review_2026-07-07.md` 第四节，经 owner 修订后固化为执行计划。
> v1.1（2026-07-08）：经 26-agent 对抗审核（4 维度审查 + 逐发现对抗核实），
> 21 条确认发现全部吸收；变更要点——composite_v1 注册为 builtin 因子（新增 P0）、
> P2 消费方圈定三工具、OOM 缓解升为 W1 前置、价差 CH 聚合加分块约束、
> 安全包豁免范围显式化、kw 案补录（经核实为真实案例非笔误）。
>
> 2026-07-10 统一整改：**备份仍挂起**（等待异机介质）；其余安全包已解除豁免并执行，
> 包括 PG/CH 密钥轮换、数据库端口 loopback 绑定、CH native 端口下线和 SSH 密码登录关闭。
>
> 纪律红线（全程有效，违反即返工）：
> 1. 判据**预注册**——写死在脚本/假设文档 docstring，先提交 git 再跑数；
> 2. 全部 trial 进 `research/output/trials.parquet`（253 为唯一台账），显著性看
>    `trials report` 的**动态 Bonferroni 阈值**——该阈值仅适用于本计划起的新 trial，
>    **既往裁决不因阈值切换重开**；
> 3. 变现判定一律**散户口径**（$20k、月频、20-40 只、成本=价差一半、小盘栖息地是优势）；
> 4. 研究/评估作业一律 253 上 `scripts/run_research.sh <tag> -- <cmd>` 拉起；
> 5. 每项收尾**回写 `docs/research_ledger.md`**；
> 6. 不偏离路线：降级令清单（§9）里的事项不做，除非解冻条件触发并经 owner 批准。

## 0. 总览与依赖图

```
W0 前置（工程四件套 + OOM 缓解，先于一切复审）
  ├─ P0 composite_v1 注册为 builtin 因子 ──┐
  ├─ P1 retail_reality 双引擎口径差修复 ───┼──> W1 composite_v1 散户复审（双退市口径）
  ├─ P3 study 脚本入 trials 台账 ──────────┘
  ├─ P2 evaluate/三关/栖息地前向收益退市口径统一 ──> W1-2 earnings_yield（三关+栖息地+散户）
  │                                                └─> W2-4 基本面族 evaluate
  └─ OPS0 run_research.sh 内存帽下调 + PG/CH 容器内存上限（半小时件，先于一切长窗作业）
W2 插缝 EXCHANGE_DROP 新旧口径对比（半天；composite_v1 部分已并入 W1 双口径跑）
W2-4 基本面族扩张（3-5 因子，假设文档先行）
W3-5 分钟线逐股有效价差（成本面基建，与上并行）
                                                          v
W5-6 composite_v2 + 30 只月频纸面组合（唯一汇合点）
```

运维盯梢（穿插，不占研究窗口）：见 §8 表。

## 1. W0 前置：工程四件套 + OOM 缓解（先修船再出海）

审阅报告点名的口径缝 + 对抗审核补出的缺件。不修就复审 composite_v1
等于用有分歧的尺子量压线因子。

### P0 composite_v1 注册为 builtin 因子（对抗审核新增，阻塞 W1/W2/W5-6）
- 问题（审核发现，经对抗核实确认）：composite_v1 **不是注册因子**——信号只内联在
  `research/composite_study.py:96-110` 的秩聚合里，`retail_reality_study.py:104` 的
  `get(args.factor)` 与 evaluate CLI 都无法消费它；且 composite_study 的 COMPONENTS
  默认值（`:41`）仍是含已裁决死亡的 breadth/delta_IO 的 wave-6 旧 4 信号版，
  与定案骨架（low_vol+high_52w+size）不一致——存在误用旧默认污染唯一可用候选的风险。
- 方案：新建 `research/factors/builtins/composite_v1.py`——把定案骨架
  （eligible 内排名 → high_52w 逐日 OLS 残差化 → 0.5 中性填补 → low_vol 在场门）
  从 composite_study 抽为注册因子，成分**写死**为 low_vol+high_52w+size；
  composite_study 保留为实验场，默认 COMPONENTS 同步订正 + docstring 标注定案出处。
- 验收：金测试——注册因子输出与 composite_study 同成分跑批位级一致（同一窗口合成面板）；
  `evaluate --factors composite_v1` 与 `retail_reality --factor composite_v1` 都能跑通。

### P1 retail_reality 双引擎口径差（阻塞 composite_v1 复审）
- 问题（已摸底钉到行）：`retail_reality_study.py:162-173` 的子组合 sim 是 numpy 快循环，
  比照 `backtest.run_backtest` 缺**三个**语义——①`:159` `pct_change(fill_method=None)`
  不 ffill，跨停牌缺口收益整段丢失；②无停牌冻结（`backtest.py:159-165` 的 gap_entry
  权重冻结）；③无退市注入（`backtest.py:144-157`），`nan_to_num(0.0)` = 退市赚 0%——
  小盘 q5 退市密集，子组合收益系统性高估，且判据第二半边（`:182-183`）直接拿
  快循环数字减 run_backtest 数字。
- **额外发现**：本脚本三处 `run_backtest` 调用（`:147-150`/`:175-176`）都没传
  `terminal_return`——整分位/基准腿同样退市赚 0%，一并修。
- 方案（定案）：**子组合直接复用 run_backtest**，删手写引擎——
  `members0.any(axis=0)` 裁列（q5 成员并集 ~2-4k 列），1000 次 sim 传同一个 `adj_sub`
  面板对象吃 `_DERIVED_CACHE`（容量 2、按 id() 缓存：整分位 adj_for_bt + sims adj_sub
  两键恰好保住，勿引入第三面板）。三处 run_backtest 全部补
  `terminal_return=load_delisting_returns(engine)`（用法照抄 `evaluate.py:872-881`）。
  `_pick_with_continuity` 不动（有测试锁定）。
  耗时预期按审核修正为**同量级（5-15 分钟）**；若嫌慢可做审核建议的正确性等价优化
  （退市注入只依赖价格面板，对 adj_sub 一次性预注入生成共享 returns，再喂 1000 次 sim），
  金测试照锁位级一致。
- 验收：合成面板（含停牌段+退市尾巴）金测试，sim 路径与 run_backtest 直接调用位级一致；
  判据数字会变——docstring 预注册段留痕新口径，回写 ledger。

### P2 前向收益退市口径统一（消费方圈定三工具，阻塞 earnings_yield 与基本面族）
- 问题（已钉到行）：`evaluate.py:288-292` `_forward_return` 用 `adj_close.ffill()` 后
  `shift(-horizon)`——退市后价格被 ffill 复制，前向收益=0%；而分位路径在
  `evaluate.py:400-407` 已注入实测 `delisting_return`。同一 EvaluationResult 两种终局口径。
- **审核圈定的完整消费方清单**（不止 evaluate）：`factor_correlation.py` 有自己的
  内联前向收益复制体（ffill 口径）、`size_neutral_study.py` 直接调 `_forward_return`
  且不传退市参数。三关对照与栖息地诊断若不同步，earnings_yield 的三步会口径分裂。
- 方案：三处前向收益**统一收口到同一个带退市注入的 `_forward_return`**
  （factor_correlation 删内联复制体改 import）；退市并入逻辑复用 `:872-881` 已解析的
  `resolved_terminal`；口径开关进 config dict（`:910-948`，仿 `_terminal_return_config`
  在 `:937` 的摊入方式）——params_hash 变化，新旧 trial 不互相顶替。
- 验收：无退市证券的合成面板上新旧位级一致；有退市面板上并入值 = 手算注入值；
  跑 size 确认产生新 trial 而非覆盖旧行；factor_correlation/size_neutral 消费同一实现
  的 import 断言（防再分叉）。

### P3 变现类 study 脚本入 trials 台账
- 问题：retail_reality / composite_study / size_neutral_study 只出 markdown，
  不进机器台账——garden-of-forking-paths 靠自觉。
- 方案：`research/_trials_store.py` 加 study 行类型（trial_kind='study'，
  记 factor/family、判据摘要、PASS/FAIL、报告路径）；三个 study 脚本收尾各加一次写入。
- **分母口径（预注册，审核要求前置到规划期定死）**：study 行记录判据与结局，
  **不计入** evaluate trial 的 Bonferroni 分母——分母只数假设检验型 trial；
  两层标准：发现级结论用动态 Bonferroni，部署级 study 判据用各自预注册阈值（如 t>=2）。
  该规则写进 trials.py docstring，`ledger:13` 显著性口径行同步更新列入本项验收。
- 验收：跑任一 study 后 `trials report --factor <name>` 能看到 study 行且分母不变；
  ledger 口径行已更新。

### OPS0 253 内存缓解（半小时件，先于一切长窗作业）
- 源报告运维节点名：07-07 上午 5 次 global OOM 击杀研究进程（当时 RSS 5.7-6.4G / 11G 主机
  与 Plex 共存）；run_research.sh 的 MemoryMax=7G 帽子太高——全局 OOM 先于硬帽触发。
- 终态（2026-07-10）：主机已扩至 19GiB RAM + 16GiB swap，run_research.sh 按实测保留
  **MemoryHigh=8G / MemoryMax=9G**；PG/CH 容器硬帽 2.5G/4G 已持久化进 compose。
- 扩容后统一使用 8G/9G 研究帽；长窗作业仍须**错峰跑**（同一时刻只跑一个，且避开
  每日 10:00 跑批窗口）。帽杀（Result=oom-kill）可验尸，发生时记录峰值后再单独裁决，
  不保留扩容前 5.5G/6.5G 的临时白名单口径。

### 排序与工时
P0/P1/P2/P3/OPS0 相互独立可并行；合计 2-3 天。
**P0+P1+P3 完成解锁 composite_v1 复审**；P2 完成解锁 earnings_yield 三步与基本面族
（它们是退市敏感评估，别用旧口径跑完再返工）。

## 2. W1 主线：composite_v1 散户口径复审（可用候选的收尾关）

- 前提：size 关卡重审已 PASS（2026-07-07，594569c），骨架定为 low_vol+high_52w+size；
  P0 已把该骨架注册为 builtin（成分写死，防 composite_study 旧默认污染）。
- 内容：`retail_reality_study.py` 过 $20k / 20-40 只现实档（成本三档 20/40/80bps，
  判据锚 40bps 档——与工具实际档位一致），双判据预注册（沿用 wave-10b 判据结构，
  具体阈值在修复后的脚本 docstring 写死并先提交）。
- **双退市口径跑**（吸收审核发现：口径敏感性检查不能排在裁决之后）：
  默认口径 + `exchange_drop_fallback=-0.30` 口径各跑一次，**判定以保守口径为准**；
  两口径分歧本身记入报告（顺带完成 §4 对 composite_v1 的那一腿）。
- 分叉（预注册）：
  - **PASS** → composite_v1 升"可用"，进 W5-6 纸面组合；
  - **FAIL** → composite_v1 记入 ledger（真实但散户不可收割），主攻权重全转 earnings_yield
    与基本面族——不做参数挽救（wave-10b 教训：FAIL 后调参数=叉路花园）。
- 253 跑，`run_research.sh composite_v1_retail -- ...`。

## 3. W1-2：earnings_yield 三关对照 + 栖息地诊断 + 散户复审

当前唯一长窗过线的非价格因子（2011+ h1 t=5.31 / h21 t=3.24，trial 49f0d58b2957——
**探索性 run，未回写 ledger；开工前先补一笔 ledger 登记其出处/窗口/分母口径，
正式结论以 P2 新口径重跑为准**），月频低换手天然适配散户。三步全用现成模板：

1. **三关对照**（`research/factor_correlation.py`，十分钟件，P2 后与 evaluate 同口径）：
   对 size / low_vol / high_52w 的 partial IC——institutional_breadth 就是死在这一关。
   预注册判据：任一关 partial IC 相对原 IC 衰减 >2/3 即判马甲，
   正式结论需逐日截面回归残差确认。
2. **栖息地诊断**（`research/size_neutral_study.py` 模板，wave-9 定式）：桶内 IC 定位
   效应住在哪个市值层。若只住小盘 → 成本压测按工具三档（20/40/80bps）的高档解读；
   若全桶 → 更好。
3. **散户复审**（`retail_reality_study.py`，等 P1 修复后）：同 §2 判据结构。
- 长窗窗口 2012-01-01 起（XBRL 2009+，留 TTM+270 天新鲜度暖机），显式传 `--eval-start`。
- 分叉：三关全过+散户 PASS → composite_v2 核心成分；三关过但散户 FAIL →
  复合成分保留（排序信息真实）；三关折戟 → ledger 记马甲、基本面族战役照打
  （earnings_yield 死不代表质量族死——两族口径不同源）。

## 4. W2 插缝：EXCHANGE_DROP fallback 新旧口径对比（半天）

- `--exchange-drop-fallback -0.30` 已实现（8a72d57，opt-in、进 params_hash）。
- 内容：对小盘栖息地已裁决因子重跑 fallback 口径对比 q5 年化——
  **residual_vol、low_vol 走 evaluate CLI**；composite_v1 的对比已并入 §2 的双口径跑
  （P0 注册后它也能走 CLI，若 §2 已产出双口径数字则不重跑）。
- 预注册判据：**q5 年化差 >2pp/年** → 相应 ledger 行标注"EXCHANGE_DROP 口径复核"；
  composite_v1 若差 >2pp 且方向不利 → §2 散户判定按 fallback 口径重跑重判（先于裁决生效）；
  ≤2pp → 记录后关闭，此后默认口径不变。
- 明确非目标：不翻案 residual_vol（三档全 FAIL 是符号级失败，口径精化救不了）。

## 5. W2-4：基本面族扩张战役（没动过的矿）

ledger 开放问题#4，XBRL 2009+ PIT 就绪，单因子评估 15-20 分钟，边际成本极低。

**数据前提已摸底（2026-07-07）**：5 个候选因子所需 XBRL 概念在 `utils/sec_concepts.py`
白名单层面 100% 就绪（唯一缺口 `InterestExpense`——仅 FF-2015 口径 operating profitability
需要，本轮改用 EBIT 口径绕开，**不触发白名单扩充与 bulk-zip 回填**——白名单过滤在写入层，
扩概念必须重跑摄取）。`fundamentals.METRICS` 已有 `gross_profit_ttm`/`net_income_ttm`/
`operating_income_ttm`/`operating_cash_flow_ttm`/`assets`/`equity` 现成面板；TTM 机制对
concept 通用，新增指标 = METRICS 加一条 MetricSpec。

- **假设文档先行**：`docs/wave12_fundamental_hypotheses.md`（仿 wave11 格式），
  每因子写死：学术出处、预注册方向、构造式、判据（动态 Bonferroni + 三关对照 + 栖息地），
  先提交 git 再写代码。评估全部用 P2 新退市口径。
- 候选池（按工程就绪度排序）：
  - **gross profitability**（Novy-Marx 2013：毛利/总资产）——现成 `gross_profit_ttm`+`assets`
    即可写；`revenue−cogs` 兜底覆盖不直报 GrossProfit 的公司，但**跨 metric 对齐规则预注册**
    （审核发现：TTM 窗口可能错位）：仅当 revenue 与 cogs 的 as-of 事件 period_end 一致时
    才做减法，否则置 NaN 回退直报 GrossProfit（coalesce 不能跨概念做算术，减法在因子
    compute 层做）。
  - **accruals**（Sloan 1996 现金流口径：NI−CFO 标准化，预注册方向为负）——概念全现成。
  - **operating profitability**（EBIT/资产口径）——概念全现成。
  - **asset growth**（Cooper 2008，预注册方向为负）——需先做"同 metric 上一年度期值"
    事件层机制，**PIT 规格预注册**（审核补全）：YoY 事件 visible_date = max(两腿 filed_date)、
    任一腿新 vintage 重发事件、两腿锁同 concept；仿 `tests/test_research_fundamentals.py`
    加重述金样本测试。不用"as-of 面板取 t−252"近似（混入申报时点噪声）。
  - **F-score 子集版**（Piotroski，9 组件概念全在）——Δ 类组件同样依赖上一期值机制，
    排 asset growth 之后；组件覆盖率先量化再定子集。
- 非平凡工程件共**两个**：上一期值机制、gross profitability 跨 metric 窗口对齐。
- 工程：每因子一个 `research/factors/builtins/*.py`（`@dataclass(frozen=True)` + register，
  骨架抄 `earnings_yield.py`）。分母为 assets 的因子**不需要**公司级市值合并那套——
  分子分母同源 `sec_fundamental_facts`、同挂 CIK 锚证券，按 company_id 广播即可；
  不含股本/市值口径的因子（gross profitability/accruals）不设 `adr_unsafe`。
- PIT 红线：`filed_date` 可见性边界先于一切。
- 逐因子流水：evaluate（2012+ 长窗，P2 新口径）→ 三关对照 → 栖息地诊断 →
  回写 ledger。存活者进 composite_v2 成分池；全灭也是合法结局（ledger 记"质量族在本市场
  2012+ 无横截面增量"，纸面组合只用价格族照走）。

## 6. W3-5：分钟线逐股有效价差估计（散户成本面基建）

分钟线独有资产的已证实价值在执行层——这是把 20/40/80bps 拍脑袋档换成实测面的基建件。

**数据前提已摸底（2026-07-07）**：`stock.minute_bars` 列为
open/high/low/close/volume/vwap/trade_count——**无任何报价（bid/ask）数据**，全仓也无
quotes 表与现成价差代码（当前成本 100% 是外生假设常数）。只能走 bar-based 估计器。

- 方法：OHLC 型估计器两法对拍——Corwin-Schultz（high-low）+ Roll（子采样收益一阶自协方差，
  数组在 minute_features 聚合里现成）；日内口径天然安全（分钟价未复权，跨日估计器会
  跨除权断点——如做日级 Roll 必须先说明断点处理）。量级校准用近月大票已知价差
  （SPY/AAPL ~1-2bps）。
- **病理值处理预注册**（审核点名：CS 负估计正是 residual_vol clip(0) 同型陷阱）：
  CS 负估计与 Roll 正自协方差一律**置 NaN，不 clip 不填 0**；覆盖率损失并入
  "无分钟覆盖票"fallback 通道统计。写死在实现 docstring 后再跑数。
- 落地通道（唯一正规通道，不另起写库脚本）：`research/minute_features.py` 的
  `EXTRACT_SQL_TEMPLATE` 加列 + `ALTER TABLE stock.minute_daily_features ADD COLUMN` +
  逐年幂等重跑；因子层消费走 `minute_loader.FEATURE_COLUMNS` 加列。
  **CH 聚合内存约束**（审核实measure：groupArrayIf 收全 RTH bar 的 (ts,high,low)
  在近年分区 ~3 亿元组/年，估 6-12GB 聚合态，超 11G 共享主机）：内层先做
  **5 分钟桶 max(high)/min(low) 预聚合**再收数组（量级回到 bipower 同级），
  或近年分区按月分块 INSERT；**首个大年份跑通 + CH 峰值内存记录**为中途检查点。
- **既有列金测试**（审核补全，与 P1/P2 标准对齐）：模板改动前对若干年份/证券抽样
  导出既有 15 列基线，改后重跑同样本断言位级一致；新列另测正确性。
- 接入：retail_reality 成本档从三固定档改为"逐股实测价差 ×（1 + 压力乘数）"，
  压力乘数取值与依据写进预注册 docstring；固定档保留为对照与 fallback
  （无分钟覆盖的票——OTC/yfinance 填缝票约 6-12%）。
- 价值定位（预注册，防翻案冲动）：服务**未来压线因子的判定精度**；
  已有符号级 FAIL 的裁决（residual_vol）不因成本精化重开。
- 验收（审核修正：不依赖可能为空的 PASS 集合）：价差面板与文献量级横截面一致
  （大盘 1-5bps、小盘几十-几百 bps）；对既有 **FAIL** 案例（residual_vol 三档）重跑
  断言判据方向一致（FAIL 仍 FAIL）；合成小面板金测试（已知价差注入→已知成本拖累）；
  若届时存在 PASS 案例则同样断言不改判（改判则说明档位设定原本就错，单独报告）。

## 7. W5-6：composite_v2 + 30 只月频纸面组合（汇合点）

- 成分池 = composite_v1 存活成分（low_vol+high_52w+size）+ 基本面族存活者。
- composite_v2 判据沿 composite_v1 双条件结构预注册（IC IR 与 q5 净 Sharpe 均须
  严格优于最优单成分与 v1）；对照基线（单成分与 v1 在 P2 新口径同窗口的 trial）
  在 W2-4 逐因子流水中自然产出，无需单独排产。
- retail_reality 双判据（用 W3-5 实测价差面）PASS → **30 只月频纸面组合**：
  - 调仓执行叠加用 eod_reversal_flow（已裁决的调味料：尾盘放量被砸时点收盘成交）；
  - 纸面跑 1-2 个月核对成交假设（成交价 vs 假设成本、缺票率、月频换手实测）；
  - 之后 $2-5k 实盘分批回填成本模型（owner 决定时点，不在本计划内）。
- FAIL → ledger 记账，回到基本面族/价差面找下一代成分——不硬上实盘。

## 8. 运维盯梢（穿插件）

| 事项 | 时点 | 动作 | 升级条件 |
|---|---|---|---|
| OPS0 内存缓解 | 已完成 07-10 | 主机扩容后研究帽 8G/9G；PG/CH 2.5G/4G 硬帽持久化 | — |
| 日线补齐 | 已完成 07-10 | `daily_prices max(date)=2026-07-08`，相对最近完整交易日 07-09 在 1 session 容忍内 | — |
| 分钟线周度首跑 | 已完成 07-10 | 10,954 只全量增量；10,730 有数据、222 无数据/窗口外，2 只网络失败定向重试成功；最新 07-09 | — |
| fxho rename 终裁 | 已完成 07-08 | security_id 570 收口 fxho；sync_massive_universe 后续跑批成功 | — |
| ipw/kw 重叠裁决 | 已完成 07-08 | ipw 污染段删除、kw 单行归还 9335；同符号价格重叠归零 | — |
| API key 历史日志 | 已完成 07-10 | 当前 key 全量扫描无日志命中 | — |
| chmod 600 activation_value.txt | 已完成 07-10 | 两端均为 600 | — |
| 安全加固 | 已完成 07-10 | PG/CH 密钥轮换；5432/8123 loopback；9000 下线；SSH key-only | — |

> 数据库远程访问统一走 SSH 隧道，见 `docs/deployment.md`；不再开放 LAN 写端口。

## 9. 显式降级令（防执行惯性，冻结进 backlog）

以下与小盘栖息地战略正交，本计划期内**不做**；解冻条件写死：

| 冻结事项 | 解冻条件 |
|---|---|
| ADR 二期四任务（ADS 归一化/基本面 FX/分钟增补/FTD 审计） | 某存活因子实证需要 ADR 样本 |
| securities 拆表阶段 2/3 | 再发 securities 直写事故 |
| tail_mismatch 207 身份手术 | 影响到某因子 PIT 正确性 |
| 13F FTD 任期审计 | 同 ADR 二期 |
| liquidity_lambda 机构口径原设计（Kyle-λ 冲击） | 改造为价差面（§6 已吸收其散户价值）后再排 |
| 异机备份 | owner 亲手做（硬盘到位后）；不进本计划 |

## 10. 第二梯队（登记勿丢，勿抢窗口）

- delta_IO 反向利用：**ledger 已裁决死亡（长窗 t=−2.25~−2.44 均不过 Bonferroni、
  短长窗符号相反）**——此处非重开，仅登记"若未来预注册新口径/新数据源假设文档、
  且不以推翻'不过线'事实为前提，可重立项"的解冻条款。
- insider CAR 事件研究口径（现只测过横截面 IC）。
- 13F filer 条件化（小盘桶内高集中度基金行为）。

## 11. 经济账（如实记录，防自我麻醉）

$20k 本金 × 年化 10% 净 alpha = $2,000/年，低于任何合理时间成本定价。
本计划的理性辩护：a) 数据资产本身（20 年无幸存者偏差退市面板、51.4 亿分钟条、
13F/insider PIT 链）价值可能超过交易变现价值；b) 系统与技能随本金增长复用。
这正是把纸面组合钉死在 W5-6 的理由——**先变现一个信号，再谈堆更多工程**。
