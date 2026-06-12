# POLYGON 同日同类型不同金额行甄别（2026-06-12）

承接 [2026-06-11 审计](./2026-06-11-recent-data-audit.md) 遗留事项：清理经济重复后保留的
**97 条**"同日同类型但金额/比例不同"的 POLYGON 行（当时判定可能为真实同日多笔事件，待人工甄别）。

## 甄别方法

对每条 POLYGON 行 join 同 security、同 action_type、同 ex_date 的 MASSIVE 行，
按金额差异分桶，并对实质性分歧逐一核查上下文（分红节奏、事件 ID 形态、申报时间线）。

## 结论：97 条全部是 MASSIVE 已维护事件的陈旧/冗余快照，无一为真实同日第二笔事件

| 分类 | 条数 | 判定 |
| --- | --- | --- |
| 舍入噪声（相对差 <0.1%） | 83 | 同一事件，vendor 精度不同（如 aipi 1.3550320 vs 1.3550） |
| 小幅分歧（<5%，多为小额标的的舍入 + ADR 汇率折算口径） | 8 | 同一事件：flag/guru/itan/stxk/thro 为舍入；tte/phg 为 EUR ADR 不同日汇率折算；dlo 为申报估计被修订 |
| 跨币种同事件 | 1 | dec：POLYGON 0.29 USD ≈ MASSIVE 0.21321 GBP，同一笔分红的两种币种口径 |
| 多事件合计 | 1 | ty 2025-06-16：POLYGON 1.052 = MASSIVE 两笔真实事件 0.29 + 0.762 之和（2025-12-12 同形态佐证） |
| 实质性分歧 | 4 | 见下表，均为 POLYGON 路径停更后的冻结值，MASSIVE 侧持续刷新 |

实质性分歧 4 条的逐一核查：

| symbol | ex_date | POLYGON | MASSIVE | 判定 |
| --- | --- | --- | --- | --- |
| oxlc | 2025-09-16 | 0.09 | 0.45 | OXLC 自 2025-09 起从月派 0.09 改季派 0.40-0.45（2025-10/11/12 三个月 0.40 佐证）；POLYGON 是改制前的冻结月派申报 |
| nvd | 2024-12-27 | 2.60797 | 3.00511 | 杠杆 ETF 年末分派金额被 restate；POLYGON 行是合成 ID 的迁移残留快照，MASSIVE 行持真实 vendor ID 且持续刷新 |
| bpre | 2026-06-17 | 0.1208 | 0.1371 | 未来事件（ex 日未到），申报金额被修订；POLYGON 冻结值不会再更新 |
| cw | 2026-06-15 | 0.24 | 0.26 | 同上，未来事件申报修订 |

## 建议处置

全部 97 条删除。理由：

1. 每条都有同日同类型的 MASSIVE 对应行，且 MASSIVE 是当前唯一持续刷新的来源
   （每日 recent-14d + 周日全量）；POLYGON 路径已停更，这些行只会越来越陈旧。
2. 因子管线只读 MASSIVE 行（`_load_actions_and_prices` 按 source 过滤），
   这些行不影响 computed_adjustment_factors，但污染事件 truth 表。
3. 730 天窗口外的 171,522 条 POLYGON 唯一历史行**不受影响**——删除谓词要求存在
   同日 MASSIVE 对应行，窗口外的行没有对应行，天然排除。

执行步骤（生产 253，先备份再删）：

```bash
ssh home-debian "docker exec stock-postgres pg_dump -U postgres -d stock -t corporate_actions -Fc" \
  > logs/corporate_actions_pre_cleanup_20260612.dump

ssh home-debian "docker exec stock-postgres psql -U postgres -d stock -c \"
DELETE FROM corporate_actions p
WHERE p.source = 'POLYGON'
  AND EXISTS (
    SELECT 1 FROM corporate_actions m
    WHERE m.security_id = p.security_id
      AND m.action_type = p.action_type
      AND m.ex_date = p.ex_date
      AND m.source = 'MASSIVE')\""
```

删除后无需重建因子（POLYGON 行本就不进因子链）。
