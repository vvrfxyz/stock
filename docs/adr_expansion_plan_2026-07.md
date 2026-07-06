# ADR 全量入库方案（2026-07-06 v2，经 3 路对抗审查修订）

目标：ADR 家族证券（活跃+退市）全数据域入库；研究层默认保持 CS-only 零污染。
v1 → v2 修订来源：3 个对抗审查代理攻出 14 个带 file:line 证据的缺陷，本版全部吸收。
规模（实测）：活跃 ADRC **378 只**、退市 ADR 家族 **329 条**（+无类型池中的隐藏 ADR）；
day_aggs 归档待认领 **663 ticker / 168.5 万 bar**；公司行动归档含全部 ADR 2003+ 历史；
13F 未映射 ADR 持仓 **~390 万行**（AMBIGUOUS 池 378 万 + MATCHED 积压 9.9 万）。

## A. 危险排序不变量（违反任何一条 = 数据事故）

1. **先改代码后灌数据**：`cleanup_us_universe.py:61` 会物理 DELETE 白名单外类型及 10 张域表。
2. **`securities.type` 原样存 vendor 类型，绝不写成 CS**（研究层 7 处 `type=any(('CS',))`
   默认因此天然隔离）。加测试断言。
3. **名单入库后、任何价格/行动回填前，必须先跑 `update_massive_details`**（v1 致命漏洞）：
   - tickers 列表响应**不含 list_date**（massive_source.py:342-345），新行 list_date=NULL；
   - list_date NULL ⇒ 防回收 clamp 变 no-op（update_massive_prices.py:167 /
     update_massive_actions.py:109 均以 `if security.list_date` 为前提）⇒ 730 天回填
     会拉入回收 symbol 前任实体的数据 = **gogl/lazr 事故复现路径**；
   - import_day_aggs 跳过 NULL list_date 活跃行 ⇒ 110 万根活跃 ADR bar 认领数为 0；
   - check_data_integrity 的 NULL list_date>50 阻塞必触发（378≫50）。
   details 只在每月第一个周三自动跑（main.py:376-383）——**必须手动跑，不能等**。
4. **部署 Phase 0 当晚 nightly 就会开始摄入 ADR**（universe sync 是每日第一步）——
   Phase 0 部署与不变量 3 的 details 必须在同一个白天内完成，赶在 02:00 UTC 前。
5. **价格顺序：先 Massive 后 flat**（v1 顺序会永久丢失 2024-07~2026-04 的 vwap）：
   flat 先跑会把水位推到 2026-04-15，nightly 增量只拉水位+1 之后（update_massive_prices.py:155-161），
   Massive 时代窗口永不回填。正确顺序：details → 手动/夜跑 `update_massive_prices`
   （水位 NULL → 自动全量 730 天）→ 再 flat 导入（massive_min≈2024-07 自动成为上界，
   与既有 CS 口径完全一致）。
6. 手动 Massive 串行道任务**避开 02:00 UTC nightly 窗口**（跨进程限流不协调，双侧 429）。

## B. Phase 0：代码改造（一个 PR，逐文件逐行清单）

| 文件 | 行号 | 动作 |
|---|---|---|
| utils/massive_config.py | :10 | `ALLOWED_US_SECURITY_TYPES += ("ADRC","ADRP","ADRR")`；8 个引用方原子生效；**新增测试锁定常量内容** |
| data_sources/massive_source.py | :412 | 去掉冗余 `is_supported_us_security_type` 双重门 |
| scripts/sync_delisted_universe.py | :42,:156,:269,:212 | SUPPORTED_TYPES 改 import 共享常量；:212 富化 SQL 同步 |
| scripts/import_day_aggs.py | :165 | load_tenures SQL 用共享常量渲染（同时解锁归档公司行动导入，:82 复用） |
| scripts/update_minute_bars.py | :85 | 硬编码 ["CS","ETF"] → 共享常量 |
| scripts/check_data_integrity.py | :179,:254,:366 | 三处全改共享常量 |
| scripts/health_report.py | :231,:353 | 两处改；**:85 是有意 CS-only 的 KPI 口径，保持不动并注释** |
| scripts/audit_recent_data.py | :70,:76,:81 | 三处改；**:65 有意 CS-only 采样，保持不动** |
| scripts/build_companies.py | :86,:198,:266,:277,:306 | **不得用共享常量**（ETF 隔离是该脚本不变量，tests/test_build_companies.py:185 锁定）。新建 `COMPANY_EQUITY_TYPES=("CS","ADRC","ADRP","ADRR")`，永不含 ETF |
| scripts/sync_massive_universe.py | NEW 路径 | 补写 NEW_LISTING 身份事件（含 origin），修复回滚叙述（现状只写 RENAME/RECYCLE/QUARANTINE） |
| utils/sec_concepts.py | — | 新增 `ifrs-full` 概念块（TSM 实测 334 个概念现被静默丢弃；schema 已有 taxonomy 列） |
| scripts/update_sec_filings.py | :32-40 | DEFAULT_FORMS 补 `6-K/A`、`40-F/A` |
| scripts/import_corporate_actions_archive.py | :97,:308-319 | **inactive 证券 per-security 上界 = min(全局 cutoff, delist_date+1)**——否则退市证券在 [cutoff, delist] 的事件三不管（live actions 无 --include-inactive 且只选活跃，实测现存 877 条退市行已有此洞） |
| scripts/update_massive_actions.py | live 路径 | 同日冲突拆股守卫（归档 R10 有、live 缺；TSM 实测 vendor 双发不一致比率） |
| FX 小工程（非一行改动，v1 低估） | utils/fx_rates.py:59-63 等 | UsdFxConverter 硬过滤 source='ECB' base='EUR'，FRED DEXTAIUS 是 USD 基直连——需三件套：①update_fx_rates 增 FRED 分支写 (base=USD, quote=TWD, source=FRED) 行；②converter 增直连 USD 基回退（1/rate，ECB 优先）；③方向测试。否则 735 行 TWD 分红（TSM）照旧被 SKIP 静默丢弃 |
| scripts/sync_openfigi_identifiers.py | _merge_candidates :210-216 | **必要路径（v1 误标可选）**：AMBIGUOUS 多 FIGI 时取 exchCode=US 的 composite——退市旗舰 ADR（LFC/PTR/DIDI）无法走 FTD（sync_cusip_identifiers.py:186 只匹配活跃行），这是它们 13F 挂链的唯一通道 |
| 研究层 | 7 处默认 ('CS',) | 本期不动（隔离即保护）；opt-in 另立工程 |

## C. 执行 Runbook（部署日一气呵成，顺序即不变量）

```
白天（02:00 UTC nightly 之前完成 1-5）：
1. 部署 Phase 0 → 全量测试过 → 253 alembic 无需迁移
2. python main.py sync_massive_universe            # 378 活跃 ADRC 入库
3. python main.py update_massive_details --market US --all   # ★list_date/CIK/FIGI，防回收 clamp 上膛
4. sync_delisted_universe --dry-run 审计（重点看 matched_filled 有无跨类型吸收、
   new_delisted≈ADR 增量）→ 正式跑（含无类型池重鉴定 ~1.5-2h + 阶段 B 富化）
5. python scripts/build_delisting_events.py --apply  # 否则 health_report 因退市结局缺失每日 P1
6. update_massive_prices（手动定向新 ADR 或等当晚 nightly）  # 730 天 Massive 窗口先行
7. import_day_aggs 重跑，**不带 --purge-remnants**（vendor 个别行 vw/n 双缺会被指纹误删；
   07-05 已清过残留）。注意：全量重跑=同值重写 2380 万行（~50 分钟 WAL 消耗），避开 nightly
8. update_massive_actions（活跃 ADR 730 天）→ 归档重跑 --cutoff=今天-730（含 per-security
   退市上界）→ TSM 三日期冲突拆股人工裁决（quarantine_detail.tsv）→
   update_adjustment_factors --all（~7 分钟）
9. sync_sec_identifiers 手动跑（不等周日档；活跃 ADR 自动得 CIK；退市 ADR 无 CIK 接受）
   → update_sec_filings（活跃自动；退市加 --include-inactive）→ update_sec_fundamentals
   （ADR CIK 定向，ifrs-full 生效）
10. 13F 挂链：sync_cusip_identifiers 重跑（FTD 给活跃旗舰挂链；--months 沿用但知晓
    2017-07 前文件 404、20 年 lookback 的回收误链风险，见 §E 审计项）→
    openfigi resolve_links 零 API 重跑（MATCHED 积压 9.9 万行）→
    AMBIGUOUS 消歧新路径跑一遍（BABA/TSM/Novartis…378 万行主奖池）
11. 验证：check_data_integrity + health_report（§D 预期整改清单）→
    TSM/BABA 复权价对外部源抽验 → 存储/统计终报
```

## D. 预期整改清单（跑体检前先知道会红什么）

- NULL list_date：富化失败的退市 ADR 残留（阈值 50 内应可控；活跃侧 details 已解决）
- 归档重跑放行 [2024-05-14, 今天-730) 新窗口后，vendor 快照后修订过的事件触发 R13
  mismatch 挂起（对应证券自动剔出研究面板待人工裁决）——预期一小批
- health_report 退市结局：build_delisting_events 跑过即消
- 常态成本：每日串行道 +378 只 ≈ +10 分钟（prices/shorts/news/open_close 各步）

## E. 已知残留风险与后续（不阻塞本方案）

1. **FTD 20 年 lookback 的回收误链审计**（今晨 --months 240 已跑，同类风险已存在）：
   FTD 匹配用当前活跃 symbol 快照，身份事件仅覆盖 2026-06 后——建议后续做一次
   "FTD 观测期 ∈ 证券任期"校验审计，越界映射降级/删除。
2. 退市 ADR 无 CIK（sync_sec_identifiers 只喂活跃行）→ 其 SEC filings/基本面缺失，
   与既有"退市基本面 0.9%"缺口同类（见 infra 评估）。
3. ADR 股本 ADS/公司口径混杂：size/earnings_yield/short_interest_ratio 禁入 ADR
   直至归一化（研究层默认关闭已挡）；historical_shares 照写，用时甄别。
4. ARS/CLP/PEN 分红（256 行）无汇率源，接受跳过入监控口径。
5. 分钟线 ClickHouse 侧 ADR 增补：类型门开后另批执行。
6. 研究层 opt-in 工程：集中 DEFAULT_RESEARCH_TYPES → --include-adr CLI →
   修双重过滤（fundamentals.py:188、classic_price.py:28）→ ADS 比率归一化 →
   MetricSpec 增 IFRS 概念名。

## F. 回滚

锚点 = `securities.type IN ('ADRC','ADRP','ADRR')`（类型原样存储保证可靠；v1 的
NEW_LISTING 事件锚点对活跃 ADR 不成立，Phase 0 补写后双保险）。价格/行动/身份行
按该 security_id 集合整体可删；07-06 pg dump 兜底。
