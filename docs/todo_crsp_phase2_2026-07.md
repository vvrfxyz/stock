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
