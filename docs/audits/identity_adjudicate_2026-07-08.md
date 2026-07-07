# 身份裁决三案执行记录（2026-07-08，roadmap §8 插缝件）

执行前提：只读调查（8 轮 SELECT）→ 承重证据亲验（同 CIK/重复行数/逐位相等/缺口互补）→
定向备份 → 单事务执行 → 终态验证。备份与 SQL 原件：
253 `logs/manual_backfill/identity_adjudicate_20260708/`（2,816 价格行 + 全元数据 CSV）。

## 案 A：fxho / UTime 三方分裂身份（QUARANTINE 447 终裁）

同 CIK 0001789299 三只：570（wto，全史 1,316 行）/ 289426（utme，608 行 **0 独有天**
全被 570 覆盖）/ 265869（fxho，07-02 sync 误建，仅 1 行）。**裁决：570 为 durable
主记录（utme→wto→fxho），两 husk 留 inactive**。执行：265869 唯一行迁 570、289426
608 重复行删除、两条错误退市事件删除、570 收口活跃 fxho + 符号史 + RENAME/MERGE
审计事件。**07-05 起 sync_massive_universe 每日非零退出的根因就此消除**（07-08 跑批验证）。
遗留：570 的 2026-07-02 行 close 7.20 vs vwap 17.59 背离（疑反向拆股过渡脏行），
待下次价格核验；**未做**：Massive 端 fxho 历史 refetch。

## 案 B1：ipw 回收污染 + 错挂 CIK

286364（massive_delisted_backfill husk）持两段：2008-2017 真实老 IPW 实体（2,106 行 +
36 条公司行动，身份未解）+ 2021 年 iPower(9303) 污染段 100 行（99/100 close 逐位同）。
且错挂 International Paper CIK 0000051434 与活跃 6375 撞车。**裁决：删污染段、clamp 至
2017-07-24、摘 CIK（securities + security_identifiers）、删污染派生退市事件、
名字改 UNKNOWN 标记**。证据不足项：2008-2017 真实发行人需 vendor 端补证后再定名。

## 案 B2：kw 单行错挂

289946（backfill husk）唯一行 2010-03-19(10.00) 实为 Kennedy-Wilson(9335) 缺失交易日
（9335 有 03-18=10.00 / 03-22=9.85，值连续）；husk 错挂 Kellanova CIK 0000055067（撞活跃
7743）+ 错 FIGI + 错退市事件（2023-10-02 MERGER 属 Kellanova）。**裁决：行归还 9335、
错身份/事件全摘、husk 清空留 inactive**。

## 终态验证（全过）

570 活跃 fxho 水位一致（max(date)=price_data_latest_date=2026-07-02）；9335 拿回
2010-03-19；CIK 撞车 6375/7743 各归零；三案同符号价格重叠归零。

## 系统性根因（登记，本轮不扩栈）

三 husk 全部 origin=massive_delisted_backfill：退市回填对被回收 ticker 有两类伤——
①符号键把新持有人价格污染进老 husk；②enrich 把不相关活跃公司 CIK 错嫁接造撞车
（污染 resolver 与基本面 join）。权威清单应以 `check_data_integrity` 同符号重叠探针
为准（CIK-collision 全库扫描噪声大：de-SPAC 壳共享 CIK 2,179 例属合法）。
**建议后续插缝**：跑全量同符号重叠清单 + husk 的 CIK-撞活跃券探针，按 B1/B2 模板
逐只裁决——登记进 backlog，不抢研究主线窗口。
