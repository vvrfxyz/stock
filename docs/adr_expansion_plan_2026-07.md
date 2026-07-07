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
| FX 小工程（非一行改动，v1 低估） | utils/fx_rates.py:59-63 等 | UsdFxConverter 硬过滤 source='ECB' base='EUR'，FRED DEXTAUS 是 USD 基直连——需三件套：①update_fx_rates 增 FRED 分支写 (base=USD, quote=TWD, source=FRED) 行；②converter 增直连 USD 基回退（1/rate，ECB 优先）；③方向测试。否则 735 行 TWD 分红（TSM）照旧被 SKIP 静默丢弃 |
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

## G. 执行期修订（2026-07-06 实现 + 对抗审查后定案）

实现阶段与 4 镜头对抗审查（13 发现 → 3 路反驳裁决 7 确认）对 §B/§C 的修订：

1. **归档退市上界公式勘误**：§B 行 13 的 `min(全局 cutoff, delist_date+1)` 是笔误，
   正确实现为 **inactive → max(全局 cutoff, delist_date+1)**（exclusive；min 对
   cutoff 后退市的证券恰好保不住洞）。active 严格维持全局 cutoff（§A.5）。回收
   symbol 场景有 resolve 层逐证券兜底，活跃后继者绝不吸收其 Massive-live 窗口内
   的归档事件。NULL delist_date 的 inactive 行回退全局 cutoff（保守残留）。
2. **OpenFIGI 消歧文件勘误 + 成本修正**：`_merge_candidates` 实际在
   `data_sources/openfigi_source.py`（§B 行 16 文件名有误）。AMBIGUOUS 缓存行
   **不存候选 payload**，消歧重跑必须 `--refresh-days 0` 重新查 API——§C 步骤 10
   的"AMBIGUOUS 消歧跑一遍"**不是零 API 本地重算**（零 API 的只有 MATCHED 积压的
   resolve_links 重跑）。
3. **★步骤 5.5 新增（P1）：手动全史 FX 回填**。计划内唯一的 update_fx_rates 调用
   是周日 `--since=run_date-30d`，FRED DEXTAUS 全史永远不会被回填——步骤 8 因子
   重建前必须手动跑一次 `python main.py update_fx_rates`（无 --since = 全史，
   含 FRED 分支），否则 735 行 TSM 台币分红照旧 SKIP，FX 三件套白做。
4. **新增审计工件**：退市名单同步的吸收匹配明细落
   `logs/delisted_match_audit.tsv`（vendor/db 类型对照 + matched_via；
   `matched_cross_type>0` 时 WARNING）——§C 步骤 4 的"跨类型吸收"人工审计以此为准
   （此前 dry-run 只有聚合 Counter，审计实际不可执行）。live 拆股冲突隔离追加写
   `logs/split_conflict_quarantine.tsv`（镜像归档 quarantine_detail.tsv 的人工
   裁决队列；此前只有滚动日志 WARNING，无持久工件）。
5. **残留硬编码收口**：`scripts/run_massive_shares_chunks.py`（shares 全量刷新
   编排器，§B 六脚本之外）的 `('CS','ETF')` 过滤已改共享常量。
6. **main.py 补 `--skip-fred` 透传**（无 FRED_API_KEY 环境的逃生口；253 有 key
   不受影响）。
7. **NEW_LISTING 锚点缺口已知残留**：步骤 4（securities upsert）与 4b（事件写入）
   分属两个事务，若 4b 失败，重跑因 resolver 判 ACTIVE_SYMBOL 不会补发事件——
   部署日首跑若在 4/4b 之间失败，须人工补事件后再依赖 §F 事件锚（type 白名单锚
   不受影响）。同日已加 pg 集成测试锁定反查的 is_active 过滤（DEAD_TICKER_RECYCLE
   错锚防御）。

## H. 执行终报（2026-07-06 深夜收官，runbook 全 11 步完成）

- **规模落地**：活跃 ADRC 376（vendor 实际数，方案预估 378）+ 退市 ADR 家族 275
  入库；ADR 日线 **151.7 万根**（2003-09-10 起，620 只）；TSM 5,739 根全史 /
  BABA 2,963 根全史；ADR 分红 7,723 + 拆股 506；ADR 基本面 35.2 万事实行
  （ifrs-full 全库 13.99 万行）；SEC filings +14.1 万行。
- **13F 挂链 80.8% → 87.1%**（+750 万行，远超预估 390 万）。两级消歧：
  US-composite（BABA/TSM 类）+ 执行中新增的 **share-class FIGI 回退**
  （`0305602`）——LFC/PTR 实测：退市旗舰 ADR 的 exchCode=US 行从 OpenFIGI
  消失，US-composite 永远命不中；但全部候选共享唯一 shareClassFIGI 且等于
  securities.share_class_figi，以此挂链（LFC 6,241 行、PTR 7,483 行救回）。
- **勘误：'735 行 TWD 分红（TSM）'** 实为台湾 F-share OTC 票（TSMWF/HNHAF 等
  非 ADR 类型，不在本期 universe）；TSM ADR 的 vendor 分红全史即 USD（45 笔
  2004-2026 完整）。FX 三件套仍然成立：CAD 2,444 行等经转换 2,410 笔进因子链，
  DEXTAUS 10,426 行（1983 起）备好待 F-share 期。另一执行期修复：FRED 系列 ID
  是 **DEXTAUS** 而非 DEXTAIUS（`a3963fb`）。
- **验证**：check_data_integrity 通过；health_report P0=0（执行中清了一个
  vendor 错标 FIGI 的 P0——cdzip id=10819 挂着 CDZI 普通股的 composite，
  已清 FIGI+写 MANUAL 事件）；TSM/BABA 原始收盘对外部记录抽验一致，复权链
  vendor 对账 MATCHED。
- **遗留裁决**（不阻塞）：bt/enia/rds.a 三组跨类型 FIGI 谱系
  （logs/delisted_match_audit.tsv，均 noop）；71 组 share_class 多退市歧义；
  退市 ADR 大面积无 CIK/list_date（vendor 时点视图缺档）；存量老退市 CS 行
  实为 ADR 的重鉴定；EURN/FBL 等 6 条 R13 值冲突挂起（mismatch.tsv，对应证券
  自动剔出研究面板）。

## I. 裁决记录（2026-07-07 凌晨，遗留清单全部处置完毕）

1. **TSM 三日期"冲突"拆股 → 全部恢复**：两两配对乘积均精确 = 1.005（台湾
   盈余转增资 0.3% + 资本公积转增资 0.2% 同日分开除权），yfinance 独立记录
   2009-07-15 = 1.005 佐证。6 行以真实 vendor id 入库（同日多真实 id 引擎按
   Ford 先例全保留相乘），复合因子实测 0.99502 = 1/1.005，vendor 对账 MATCHED。
2. **EURN 2024-05-22 → 归档两行补入**：除权跳空 21.08→16.49（-4.59）≈ 外部记录
   总派息 $4.57 = prod 3.49 + 归档 0.27 + 0.81——三笔同日分派成分并存，非冲突。
   **顺带发现方法论既有约定**：同日多笔现金分红按"各对同一前收盘算因子再相乘"
   （EURN 此日 0.7921 vs 精确加法 0.7832，偏差 1.1%；Ford 案例仅 0.02% 故从未
   暴露；vendor 参考因子同一约定，对账 MATCHED）——列入方法论待审，不动引擎。
3. **FBL 2023-12-27 → 保留 prod、拒绝归档**：跳空 118.51→78.54 精确吻合 prod
   40.02228；yfinance 8.0044 = 40.022/5（拆股调整口径）；归档 0.67416 为快照
   期错值（vendor id 挪用案的另一面）。R13 挡下归档是正确行为。
4. **ASET → 归档真实 id 替换合成行**：live 路径不覆盖退市证券取不到权威值，
   按"合成 ID 在真实 vendor ID 可用时清理"政策，MASSIVE 归档行（0.333）入库、
   POLYGON 合成行（0.333016，差 1.6e-5 舍入级）退役。PCRB/PHYD 为 vendor
   快照后修订（同 id 精度差），保留 prod 修订值——这三条会在每次归档重跑的
   mismatch.tsv 回显，属已裁决常态。
5. **bt/enia/rds.a → 改名接续手术**：BTY→BT(2004-11-15)、ENI→ENIA(2016-04-27)、
   RDS.AW→RDS.A(2005-07-26) 同实体单行 + symbol_history 双段 + RENAME 事件 +
   delist_date 更新（2019-09-16 / 2022-06-21 / 2022-01-31）+ type 依 vendor
   订正 ADRC。day_aggs 重跑精确认领 9,439 根 bar（bt 4,031 / enia 4,727 /
   rds.a 4,158 全史成立），归档行动 +拆分 5 笔、因子 145 行，退市结局重建并
   清理 3 条旧失配残行。**副产品：SHEL 解锁**——Shell 现役 ADR 此前因 vendor
   shel 条目撞 288239 退市壳行被 resolver 逐日隔离（07-06 sync 日志可见），
   288239 改名 rds.a 后 symbol 冲突消失，下一次 nightly universe sync 将自然
   以 NEW 上市入库（其 2022+ 行情由 730 天窗口 + 下次归档重跑补齐——待验证）。
6. **cdzip(id=10819) → 清错误 FIGI**：vendor 把 CDZI 普通股 composite 塞给了
   存托优先股行（OpenFIGI 实查 CDZIP 无 US 行），已清 + MANUAL 事件。扩面扫描：
   全库 157 组同 FIGI 多行**零跨类型/零活跃组**（cdzip 是孤例），全部为多退市
   行身份合并积压（含 13F 挂链跳过的 71 组）→ 移交 CRSP 二期"四清单裁决"。
7. **163 只名称含 ADR/ADS 的退市 CS 行 → 已重鉴定关闭（2026-07-07 晨）**：
   vendor 时点详情逐只查证（退市前一日，NOT_FOUND 回退一周，163 请求）——
   151 只 vendor 即标 CS、12 只无数据、**零 ADR 类型**。vendor 历史口径本就把
   这批存托凭证归 CS，按 type 原样存储铁律维持不动，此项非缺陷。
8. 复查：check_data_integrity 通过、health_report P0=0。
