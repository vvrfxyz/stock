# TODO：CRSP-grade 二期（2026-07-06 晚立项，接一期收官）

> 本文档是给**下一个执行窗口**的交接件。一期（五工程：delisting_events / companies /
> PIT 股本 / 身份史 / 拆表设计）已于 2026-07-06 当日落地并部署 253，工程记录见
> `todo_crsp_grade_2026-07.md` 末节。本文只写**还没做完的**和**新解锁的**。
>
> 阅读顺序：`todo_crsp_grade_2026-07.md` 工程记录（一期实况与假设订正）→
> `data_infra_assessment_2026-07.md` 差距地图更新节 → 本文。

## 开工前置（每次都要）

- `git pull`（GitHub 与 253 双侧确认同头；一期收官时两侧= `4c23833`，
  alembic head `a2b3c4d5e6f7`）。**同一工作树可能有并行窗口在途文件**
  （2026-07-06 晚是 ADR 扩展 + 分钟因子），commit 必须逐文件精确 add。
- 避开 02:00 UTC ±1h（每日调度）。253 交互：PG 走
  `ssh home-debian 'docker exec stock-postgres psql -U postgres -d stock -tAc "..."'`，
  跑脚本 `sudo -u wenruifeng`，ClickHouse 只走 HTTP 8123。
- 部署流程照旧：GitHub push + `_sync_main` SSH push 双轨，remote reset 后 chown。
- **`run_baselines` 的坑**：`--eval-start` 默认 2025-06-01，`--start` 只管面板
  加载——长窗口回测两个都要显式传（一期踩过）。

## 一期验收快照（基线数字，二期对照用）

| 指标 | 一期终值 | 验收线 | 状态 |
|---|---|---|---|
| delisting reason 非 UNKNOWN | 76.2%（5,711/7,494） | ≥70% | ✓ |
| final_price 覆盖 | 92.8% | — | ✓ |
| 现金并购 return 自检 | n=710, p50=+0.02% | 聚 0 | ✓ |
| **delisting_return 覆盖** | **23.7%（读取层口径）** | **≥50%** | **✗ 二期主攻** |
| RENAME 事件覆盖 | 68.1%（633 条） | ≥60% | ✓ |
| **退市 FIGI** | **43.7%** | **≥70%** | **✗ 数据源硬约束** |
| companies 活跃 CS 覆盖 | 99.85% | ≥95% | ✓ |
| 股本双源对拍中位误差 | 0.00% | <2% | ✓ |
| as-of 股本覆盖 | 2012 62.6% → 2024 82.5% | ≥85% | ✗ 存在性天花板 |
| 动量 Shumway 修正 | 733%→714%（16 年，下界） | 对比入记录 | ✓ |

---

## 任务 A：对价抽取二期——把 delisting_return 推过 50% 线（头号工程）

### What

补齐一期没抽出来的并购对价，让退市收益覆盖过验收线：

1. **换股对价估值（stock leg）**：ACQUISITION_STOCK 142 只 + 混合对价者，
   `return = (ratio × acquirer_close(final_price_date) - final_price) / final_price`。
   需要收购方 ticker 解析（acquirer_name → securities join，一期已抽出 369 个
   acquirer 名）+ 收购方在 final_price_date 的原始收盘价。
2. **DEFM14A 深解析**：一期只用其做现金 regex 兜底；委托书里 "Merger
   Consideration" 章节结构稳定，可抽混合对价的两条腿。
3. **现金模式扩充**：cash_ambiguous 40 + 归因成功但无对价的 MERGER ~1,400 只，
   按 UNKNOWN 清单里的 consideration_note 桶迭代 regex。
4. **破产 -1 的证据路径**：Form 25 rule (b) + 终价 <$0.1 + 8-K item 1.03
   （破产申请公告，items 列现成）三证齐才写 -1，否则维持 NULL。item 1.03
   判定是纯 SQL（一期已把 items 列铺满 84 万行 8-K）。

### Why

- 23.7% → 50%+ 是任务 1 验收唯一没过的线；每多一只实测 return，长窗口回测的
  Shumway 修正就少一分下界性（一期动量对比只有面板内 671 只实测）。

### How / 入口

- 全部改动收敛在 `scripts/build_delisting_events.py`（对价抽取层已模块化：
  `extract_consideration` / `ConsiderationExtraction`）+ `research/data.py`
  （acquirer 价格查询可复用 `_load_raw_close_wide` 思路）。
- 跑法照旧：`--fetch-form25-docs --fetch-8k-docs --apply` 幂等全量重建，
  完整三阶段在 253 约 55 分钟。
- 自检沿用：现金并购 p50 应保持 ≈0；换股并购的 return 分布预期略负
  （announcement premium 已在终价里）。

### 验收

- delisting_return 非 NULL（含读取层 ETF par）≥50%；换股类 return 分布写进
  工程记录；动量对比实验重跑一次（预期修正幅度大于一期的 -19pp）。

### 坑

- acquirer_name → security 解析必须走 companies.name/CIK 模糊匹配 + 人工白名单，
  收购方是私募/外国主体时无价格面板——这类只填 acquirer 不算 return（宁缺毋滥）。
- 混合对价里 election 结构（股东可选现金或股票）没有唯一 return——取
  加权/默认腿并在 evidence 记 `election_structure`，或干脆 NULL。
- ratio 方向陷阱："0.5 shares of X for each share"（乘）vs "each share
  converted into..."（同）vs 反向表述——一期 `extract_stock_ratios` 只收
  唯一小数模式，扩展时保持唯一性纪律。

---

## 任务 B：companies 兑现研究收益——earnings_yield 公司级分母 + 多类股分摊

### What

1. `research/fundamentals.py` / `earnings_yield.py`：分母从"锚定证券自己的市值"
   换成"公司级合并市值"（`research/company_market_cap.py` 的
   `load_company_market_cap_panel` 一期已就位），非主类股（goog/brk.b）从
   NaN 变为可计算。
2. 普通股分类器二期：`is_common_equity_name` 的 name-regex 换成结构化证据
   （share_class_figi / ticker_root+suffix / SEC 封面类别），消除已锁定的
   误伤（Preferred Bank、Unit Corporation）。
3. company_events 边表（任务 2 二期设计遗留）：并购世系
   (predecessor/successor/event_date/type/evidence)，先只缝 Alphabet 2015
   CIK 断层 + delisting_events 里 acquirer 可解析的并购边。

### Why

- 这是建 companies 表的原始动机：13F 审计发现的多类股问题，一期只建了骨架；
  earnings_yield 分母低估在 GOOGL/BRK 这类权重股上是系统性偏差。
- company_events 反哺任务 A（收购方解析）与基本面世系连续性。

### 验收

- earnings_yield 2011+ 重评估：goog/brk.b 类非主类股因子值非 NaN；IC 对比
  修正前后写记录。分类器二期：两个已知误伤案例翻正，双类股名录行数不变或
  有解释。

### 坑

- fundamentals 事实锚定在 min-id 证券（`resolve_cik_map` 活跃优先，一期修过
  翻锚 bug）——公司级分摊在读取层做 join，**绝不回写事实表**。
- 市值口径注意一期结论：我们的合并市值=上市类之和（Alphabet 差外部 7% =
  未上市 B 类）；earnings_yield 分母用上市类口径与分子（公司级净利）有轻微
  错配，记录口径即可，第一版不追求完美。

---

## 任务 C：securities 拆表实施（设计已评审即可动工）

### What

按 `docs/securities_split_design.md` 三阶段走。**阶段 1a 可独立先行**：把 8 个
绕行写方（sync_delisted_universe 的裸 SQL、repair_identity、import_day_aggs、
calibrate_price_latest_date 等）收口进 db_manager API——纯重构、不动 schema、
独立可部署，是后续一切的前置。

### Why / 验收 / 坑

见设计文档（列归属、写路径矩阵、每阶段测试清单、回滚方案齐备；估 8-11 人日）。
核心动机不变：2026-07-06 list_date 抹除事故的结构性根治。
坑之首：`tests/test_db_manager_pg.py` 锁定的 protected-fields 语义一行不能破。

---

## 任务 D：裁决积压——四个人工/半自动清单

| 清单 | 量 | 位置 | 建议处理 |
|---|---|---|---|
| delisting UNKNOWN | 1,783 | `logs/manual_backfill/delisting_unknown_final3.csv`（253） | 按 evidence 分桶：source=FORM25 的 782 wrong_class（多为同 CIK 票据类，正确拒绝，少数 ADR 变体可扩守卫）、257 indeterminate（老文档无勾选标记，可试 OCR/宽松规则）、source=None 1,576 里 CIK 缺失者等任务 4 补链后重跑 |
| cik_ambiguous（rename 考据） | 475 | `logs/backfill_rename_events_report_*.json`（253） | 多类股共 CIK 所致，companies 表落地后可用 company_id 消歧重跑 |
| tail_mismatch（rename 考据） | 200 | 同上 | 快照后改名，对这批 symbol 跑一次 `update_massive_events` 刷新 live 再重考据 |
| UNKNOWN 字面量 FIGI | 82 | securities.composite_figi='UNKNOWN' | 清 NULL + 记事件，半小时脚本 |

## 任务 E：因子长窗口重评估战役（新市值面板解锁）

- 9 个 builtin 因子全部 `--start 2011-01-01` 重评估（size 一期已跑：IC 0.015-0.030
  显著）；earnings_yield 等 B 完成后跑。输出统一落 `research/output/`，
  用 `--terminal-return`+realized 口径。
- 注意内存：长窗口评估在 Mac 跑（253 只有 11G）；直连
  `RESEARCH_DATABASE_URL=postgresql://...@192.168.1.253:5432/stock`（密码在
  253 `.env`），断网会杀作业。

## 杂项（半天件，插缝做）

- `update_massive_actions` / `update_massive_short_data` 补退市日上钳
  （镜像一期 prices 的 clamp；两脚本已有 `unless_symbols` 语义）。
- `run_baselines` 加 `--terminal-return-fallback` 与 `--no-fund-closure-par`
  CLI 透传（引擎参数已在，纯 parser 件）。
- `research/short_interest.py` 切换到缝合股本流（`load_shares_events` 的
  vendor-only → stitched，short_interest_ratio 历史随之延长到 2009+）。
- `research/evaluate.py` 因子回测接 realized terminal returns（run_baselines
  已接，evaluate 未接）。
- 陈年脏数据：NVDA 拆分日脏 high（半天）、706 行 OHLC 违规断言进 integrity。
- health_report 的 delisting P1 现值 1,783——每轮任务 A/D 迭代后应可见下降，
  把它当二期的进度条。

## 挂起项（显式不做，有据）

- **退市 FIGI ≥70%**：parquet 唯一归属仅 +11，CUSIP 路线候选仅 35——现有数据源
  物理到顶。等新识别符数据源（或接受 43.7%）。OpenFIGI ticker 查询**禁用**
  （非 PIT，回收污染）。
- **as-of 股本 ≥85%（2012-2020 段）**：pre-2009 退市无 XBRL + ADR/IFRS 挂起
  缺口（`fundamentals-coverage-gaps` 记忆）——存在性天花板，除非引入
  Compustat 类付费源。
- 13F 映射剩余（ADR/商品信托）：设计内排除，不再追（一期已确认）。

## 建议执行顺序

```
任务 A（对价二期，3-5 天）────────┐
任务 B1（earnings_yield 分母，1-2 天）├→ 任务 E（因子战役，2-3 天）→ 动量对比重跑
任务 C 阶段 1a（绕行写方收口，2 天）──→ C 阶段 2/3（评审后，6-9 天）
任务 D（裁决积压，插缝）反哺 A 的 UNKNOWN 下降
```

预计 2-3 周串行；A 与 B/C 可双窗口并行（共享文件冲突面小：A 在
build_delisting_events，B 在 research/，C 在 db_manager/scripts）。
每完成一项：工程记录追加 + 差距地图更新 + health_report P1 数字入记录。

---

## 工程记录（2026-07-06 深夜 ~ 07-07 凌晨执行窗口，二期主体落地）

提交链 `3a15a7f..53e1e7a`（+ 收尾 docs 提交），全部部署 253，migration head 不变
（`a2b3c4d5e6f7`，零 schema 变更窗口）。执行形态：多代理并行实现（文件所有权隔离）
+ 两轮对抗审查（16 项发现 15 项确认全修）+ 生产幂等重建。

### 开工即发现的生产事故（起点订正）

**一期的 23.7% 起点在生产中已不存在。** 2026-07-06 21:07 CST，ADR 扩容并行窗口跑了
一次**无 fetch 旗标**的 `build_delisting_events --apply`（45 秒，7,776 行），
full-rebuild 语义把对价四列（acquirer_name/consideration_cash/consideration_stock_ratio/
delisting_return）全部置 NULL，Form 25 规则层分类同步丢失（UNKNOWN 1,783→2,606）。
这正是交接件"坑"节警告过的语义陷阱第一次真实咬人。结构性根治见任务 A 的保险丝。
另：人口从 7,494 涨到 7,776（+281 只 ADR 退市证券入册 + 自然新增），验收按
CS+ETF（7,501）与全人口双口径报。

### 任务 A：对价抽取二期（commit 53e1e7a）

四抓手全部落地于 `scripts/build_delisting_events.py`（1,603→2,186 行，tests 222→237）：

1. **股票腿估值**：acquirer 名归一（小写去标点循环剥公司后缀）后在 securities.name +
   companies.name 双通道精确匹配，合并候选**唯一**才采信（companies 命中经成员收敛到
   唯一活跃 CS）；acquirer_close 当日缺失回看 3 天；换股独占
   `return=(ratio×acquirer_close−final_price)/final_price`，混合再加现金腿，同款
   [0.2x,5x] 闸门；election 结构跳过股票/混合腿。
2. **破产 −1 三证**：Form 25 rule (b) + 终价 <$0.1 + 同 CIK 8-K item 1.03
   （−400d..+30d 窗）→ BANKRUPTCY/HIGH，delisting_return=−1；ETF 豁免
   （1940 法案基金资产隔离）；8-K 2.01 分支先行短路（20 只 MERGER∩1.03 不动）。
3. **现金正则 +5**（tender net-to-seller / consideration of / amount equal to
   子句锚定 + accrued/unpaid/dividend 负守卫 / purchase price of / tender offer at）；
   aggregate 回看守卫扩词 total|entire|combined|overall；无 per-share 候选 >$10,000
   直接丢弃（总价位于百万级，每股价永不过四位数）。
4. **DEFM14A 深解析 + 候选扩圈**：候选闸从"仅 8-K 2.01"放宽到 form25 rule
   a/a3/a4 与 identity-merge 的 MERGER 族（这批只有 DEFM14A 可抓，排序 DEFM14A
   优先）；"Merger Consideration"章节切片取**首个有产出**的窗口（末次出现会落进
   Annex A 法律件），现金/换股比/election 检测都在切片内跑。

**对抗审查修正的关键设计错误**（写进决策表测试）：
- 身份 MERGE 的 keep 侧是**同一实体的存续身份**（repair_identity 只合并重复身份），
  绝不可当收购方——曾用它给股票腿估值等于拿被并方自身价格算 return（恒 ~ratio−1 的伪值）。
- 混合对价的现金腿不过单腿 [0.2x,5x] 闸门（现金腿远小于终价是常态），合腿闸门在
  classify；否则 $2.50 现金+0.5 股的真混合会被误升 ACQUISITION_STOCK 且 return 反号。
- return 只写并购族 reason_code（MERGER/ACQUISITION_CASH/ACQUISITION_STOCK）+
  BANKRUPTCY −1——扩圈后 form25 rule b/c/a1 也可能带对价抽取，绝不给
  EXCHANGE_DROP/VOLUNTARY/LIQUIDATION 写 return。

**降级重建保险丝**（结构性根治开篇事故）：--apply 且存量表带对价/回报数据（非 MANUAL）
时，缺任一 fetch 旗标 → 拒绝写库退出 1；两个网络阶段任一**终端降级**
（fetch_unavailable：SEC_USER_AGENT 缺失等 getter 不可用；offline_abort：连续 5 失败
离线中止）同样拒绝；`--allow-degraded-rebuild` 显式豁免。已部署 253，任何后续无旗标
重跑（含并行窗口）都会被挡下。

### 任务 B：companies 研究收益兑现（commit 18c6b88）

- **B1 earnings_yield 公司级分子/分母**（读取层 join，绝不回写）：分子按 company_id
  对成员列取首个非 NaN（security_id 升序镜像 resolve_cik_map 决胜；锚翻转过渡窗
  gliba/gncma 型两代锚同时新鲜时绝不加总双计——审查确认的 latent bug，已修+金测试）；
  分母 = 公司级合并市值广播回成员列；无 company_id 或公司无任何 common-equity 成员
  （退市 LP common units 误伤型，实测 14 只有基本面的证券）回退证券级旧口径逐位不变。
  生产冒烟：goog/brk.b 从 NaN 变可算（googl=goog≈4.0%、brk.a=brk.b≈6.8%——旧口径
  brk.a 分母低估致 19.3% 的系统性偏差被修正）。
- **B2 分类器结构化证据**：生产探针定稿——share_class_figi 非空是唯一可靠正证据
  （两例误伤 pfbc/unt 均携带；真工具行 rilyg/oxlcg/tmusi/bhfan 无一携带）；
  share_class_shares_outstanding 被证实污染不可用（vendor 对 depositary preferred
  也填，36 行反例）；ticker_suffix 双边都有不可判别。规则：FIGI 非空→普通股，
  否则退名称启发式（证据缺失绝不作负证据——退市段 FIGI 覆盖仅 ~44%）。全 CS
  21 行 False→True（逐行核验均为真普通股/LP common units/ADS），0 行 True→False。
  253 已重跑 `build_companies --apply`：631 行新挂（含 ADR 扩容新证券），0 改挂，
  活跃 CS 99.96%；onbpo（全库唯一"持基本面事实但无 company_id"的锚）挂入
  company 821，onb 的 earnings_yield 随之解锁。
- **B3 company_events 边表：挂起**（下窗口件）——本窗口时间预算给了 A/E；
  Alphabet 2015 CIK 断层缝合与并购世系边表原样保留在任务清单。

### 任务 C 阶段 1a：绕行写方收口（commit 3a15a7f）

七个旁路（B1/B2/B3/B4/B6/B7/B9）收编为 db_manager 三个新 API；语义定案与豁免清单
已回写 `docs/securities_split_design.md` 阶段 1a 节。protected-fields 既有 80 用例
一行未动全绿，新增 17 个集成用例。

### 任务 D：裁决积压（部分）

- **82 只 'UNKNOWN' 字面量 FIGI**：`scripts/cleanup_unknown_figi.py`（commit 71b17af）
  已在 253 执行——82 行置 NULL + 82 条 MANUAL 身份事件，幂等重跑零事件。
- UNKNOWN 分桶清单随本次重建重新导出（`logs/manual_backfill/delisting_unknown_phase2.csv`，
  253）；cik_ambiguous 475（需 backfill_rename_events 接 company_id 消歧的代码件）与
  tail_mismatch 200（需 Massive API 配额窗口）**未做**，留下窗口。

### 任务 E 前置：evaluate 接线 + 长窗口性能（commit 2d7ca2c）

- evaluate 接 realized 退市收益（默认口径与 run_baselines 一致，`--no-delisting-returns`
  复现旧口径；口径四键进 params_hash，新旧 trial 不互相顶替；报告 Notes 节记多头腿
  caveat——引擎只对 held>0 注入，多空组合空头腿退市不注入，ls 收益保守低估）。
- run_baselines 补 `--terminal-return-fallback` / `--no-fund-closure-par` 纯透传。
- 性能（32GB Mac 长窗口实测）：面板进程内缓存（9 因子 evaluate_all 省 8 次 31M 行
  COPY，重复装载 9.9s→0.0s）、评估路径只拉 close/volume（长表 −36%）、to_wide 换
  pivot、峰值 RSS −26%；16 年 run_baselines 全程约 5 分钟。
- （_rank_ic_series 向量化与 ic_decay 因子 rank 提升在本窗口开工前已由并行窗口
  commit 6052c84 落地，带逐位金测试。）

### 验收数字（生产重建后实测）

### 验收数字（生产 r4 重建后实测，2026-07-07 04:49 CST）

| 指标 | 二期终值 | 一期终值 | 验收线 | 状态 |
|---|---|---|---|---|
| reason 非 UNKNOWN（CS+ETF 口径） | 76.2%（5,713/7,498） | 76.2% | ≥70% | ✓ 持平（分母含 ADR 后全口径 73.8%） |
| 表内实测 delisting_return | **1,104 只**（现金 1,060 + 换股 18 + 混合 2 + 破产 24） | 710（重建事故后归零） | — | ✓ 破千 |
| **读取层 return 覆盖（CS+ETF）** | **31.6%**（实测 + ETF par 1,071 + SPAC 赎回 par 193） | 23.7% | **≥50%** | **✗ 未达，+7.9pp** |
| 现金并购自检 | n=1,060, p50=+0.0002, p10=−6.4% | n=710, p50=+0.02% | 聚 0 | ✓ |
| 换股并购自检 | n=18, p50=−0.05%, p10=−1.5% | 0 | 预期略负 | ✓ |
| BANKRUPTCY 三证 −1 | 24 只 | 0 | 宁缺毋滥 | ✓ |
| health_report 退市 P1 | 1,985（全口径，CS+ETF 1,785） | 1,783（CS+ETF） | 进度条 | ≈ 持平（本轮主攻 return 非 UNKNOWN） |

**50% 线未达的缺口分解**（CS+ETF 3,749 只的差距 ≈1,390 只，全部有据可查）：

1. **MERGER 1,718 只无对价**（最大池）：598 只 no_primary_document_url（老 filing
   索引无 URL，需 EDGAR index 补链）+ 1,120 只文档抓到但对价没抽出/被闸门拦下
   （DEFM14A 章节切片对老式委托书命中率低，正则迭代空间还在）。
2. **紧闸门的代价**：r3→r4 现金 return 从 1,168 降到 1,060——108 只被 [0.6x,1.5x]
   拦下的多为混合 deal 漏抽腿的伪值（mrtx −79.6% 型），砍掉它们让 p10 从
   −26.8% 收敛到 −6.4%。**质量优先于覆盖率是本轮的明确取舍**。
3. EXCHANGE_DROP 1,194 / LIQUIDATION 非赎回 / VOLUNTARY 117 本质上无并购对价，
   覆盖它们需要经验假设（CRSP −30% 型），属读取层 fallback 的事，不是抽取的事。
4. UNKNOWN 2,041（CS+ETF 1,785）——新分桶（证券级，delisting_unknown_phase2.csv）：
   no_evidence 1,247 / form25_wrong_class 393 / no_price_history 206 /
   form25_indeterminate 69。任务 4 CIK 补链后重跑可吃掉 no_evidence 的一部分。

**下一轮的三个明确杠杆**（按预期收益排序）：no_doc_url 598 只的 EDGAR index
补链；DEFM14A 老式文档的章节启发式迭代（1,120 只池）；EXCHANGE_DROP+极低终价
的读取层经验 fallback（−1 近似，参照 168 只 final_price<\$0.1 分布）。


### 教训（新增，防三踩）

- **贫数据重建抹富数据**不是假设风险，是已发生两次的事故路径（一期收官后 + 本窗口
  开工前）；凡 full-rebuild 语义的表，写方脚本必须自带降级保险丝，靠人记旗标不可靠。
- zsh 里 `git push host $SHA:refs/...` 的 `:r` 被当变量修饰符吞掉——refspec 必须
  `"${SHA}:refs/..."` 带引号带花括号。
- 同树多窗口并行时（本夜三个窗口交错 commit），逐文件精确 add 纪律有效；commit 前
  `git log --oneline -3` 确认没把别窗口的在途提交当自己的基。

### 动量对比重跑（同面板双跑，2026-07-07 凌晨）

面板 2010-01-01~2026-06-10（9,245 只，剔除 1,105 只未覆盖事件证券），评估窗
2011-01-01 起，双边 10bps。**同日同面板双跑**（一期的 733.4%→714.0% 是跨面板
测量，当日 corporate-actions 归档在途，不可比）：

| 口径 | momentum_12_1 | sma_50_200 | 5 日反转 | 等权参考 |
|---|---|---|---|---|
| 旧口径（退市赚 0%） | 729.5% | 423.4% | 32.5% | 345.2% |
| 实测口径（1,060 只面板内实测注入） | **720.2%（−9.3pp）** | 419.7% | 32.2% | 345.4% |

- 修正幅度 −9.3pp **小于**一期记录的 −19.4pp，且这是更可信的测量：①同面板
  双跑消除了数据漂移；②紧闸门砍掉了 108 只漏腿伪深负值（一期宽闸门下这类
  伪值会人为放大修正）；③实测池以并购类为主（return≈0，对动量组合本来就
  近中性），破产 −1 只有 24 只——动量持仓里真正的归零型退市大多仍在
  EXCHANGE_DROP/UNKNOWN（无实测，沿 0%），修正仍是**下界**。
- 产物：`research/output/baselines*_2011-01-01_2026-06-10*.csv`（旧口径带
  `.old-caliber-20260706` 后缀，实测口径为当前无后缀文件；一期实测产物存档为
  `.realized-v1-20260706`）。

### 任务 E：因子长窗口重评估战役（realized 口径，2026-07-07 05:00-07:15）

全部经 evaluate 新接的 realized terminal returns 口径（实测 2,368 只 + 双 par，
口径四键进 params_hash），面板 2011-01-01 起、评估窗 2012-01-03~2026-07-02
（252 日 warmup），报告落 `research/output/evaluate_*_2012-01-03_2026-07-02.md`，
trials.parquet 台账同步。h=21 汇总（Rank IC / Newey-West t / 多空净 Sharpe）：

| 因子 | 窗口 | IC | NW t | ls_sharpe_net | 裁决 |
|---|---|---|---|---|---|
| institutional_breadth | 2012+ | **+0.033** | **4.30** | +0.09 | 显著存活（13F 长窗口首验） |
| size | 2012+ | **+0.032** | **3.79** | −0.06 | 显著（复核一期样板，realized 口径） |
| earnings_yield | 2012+ | **+0.027** | **3.24** | +0.04 | 显著（公司级分母首个长基线） |
| delta_institutional_ownership | 2012+ | −0.009 | −2.36* | −0.23 | 负显著带噪声标记——与短窗口正 IC 反号，需专项研究 |
| insider_net_buy | 2012+ | −0.003 | −0.55* | −0.33 | 死 |
| ownership_concentration | 2012+ | +0.000 | 0.01* | −0.21 | 死 |
| short_interest_ratio | 2025+短窗 | −0.009 | −0.44* | +1.04 | IC 噪声（数据下限 2024-05，长窗口无意义） |
| short_volume_ratio | 2025+短窗 | +0.007 | 0.73* | −0.76 | 噪声 |
| days_to_cover | 2025+短窗 | +0.004 | 0.39* | +0.40 | 噪声（一期"死亡"裁决维持） |

- earnings_yield 的 IC 修正前后对比：修正前无长窗口基线可比（旧口径 goog/brk.b
  恒 NaN、锚证券分母系统性低估，2011+ 评估在旧代码下从未跑通过）——本表即
  公司级口径的首个长基线；生产冒烟已单点验证分母修正（brk.a 旧 19.3% → 6.8%）。
- 短窗口三个 short 因子按数据下限跑 2024-05-14 起（对照一期裁决，全维持）。
- 性能（perf 改造后实测）：9 因子战役全程约 2 小时 15 分（32GB Mac，面板缓存
  命中后单因子边际 ~15-20 分钟）；长表 COPY 只拉 close/volume 两列。

### 杂项裁决订正：short_interest 缝合切换挂起（2026-07-07 上午）

杂项条目"`research/short_interest.py` 切换到缝合股本流（short_interest_ratio
历史随之延长到 2009+）"的前提**不成立**：`short_interests` 分子数据本身
2024-05-15 起（FINRA 接入时点，生产实测 min(settlement_date)），分母股本缝到
2009 也没有分子可除。切换只影响现有 2024-05+ 窗口内少数缺 vendor 股本的证券，
而实现须完整移植 stale_after/split 前滚语义（fundamentals 调研报告 §7 的风险
警告）——收益/风险比不成立，**挂起**；若未来回填 FINRA 历史空头数据再启。

### 任务 D 收尾 + B3 落地（2026-07-07 上午）

- **cik_ambiguous 475 → 462**（commit 40d756f，已 --apply 于 253）：companies 就位后
  同族收窄双硬条件（链尾唯一持有者 + 全链 live history 佐证）裁决 30 组，
  +74 RENAME 事件（36 HIGH + 38 MEDIUM）+205 tenure，覆盖 68.1%→**72.1%**。
  残组诚实分解：421 single_ticker（优先股票据噪声，正确拒绝）+ 8 tail_not_unique
  + 33 chain_uncorroborated——**此清单就此收案**，剩余是设计内不可裁。
- **tail_mismatch 200→207**：全量清单已由新 --dump-buckets 旗标导出
  （/tmp/tail_mismatch_symbols.txt，已复制到 253:/tmp）；等每日调度让开后
  `update_massive_events $(cat /tmp/tail_mismatch_symbols.txt) --force` 刷新
  live 再重考据（见执行日志）。
- **B3 company_events**（commit e03b54f，migration b3c4d5e6f7a8 已 upgrade 于 253）：
  世系边表 + 播种脚本。dry-run 计划：Alphabet 2015 CIK_CHANGE 1 边（老 Google
  CIK 补建 companies 行）+ 并购边 29 条（r4 表的 acquirer_security token 解析，
  33 候选中 2 无 successor company、2 重复去重）。r5 重建完成后 --apply。
- **OHLC 探针**（commit aeafe0a）：全史七类不变量 ratchet 探针进 check_data_integrity，
  基线 0（repair_ohlc_violations 与 NVDA 脏 high 均已被 c08f968 窗口订正，
  生产实测零违规——杂项条目'706 行'系订正前旧数，就此收案）。
- **short_interest 缝合切换**：裁决为挂起（分子 FINRA 数据 2024-05 起，缝分母
  无意义），见上节订正。
