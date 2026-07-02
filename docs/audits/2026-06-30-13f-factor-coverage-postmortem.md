# 13F 因子覆盖异常尸检报告

- 日期: 2026-06-30
- 状态: 已关闭
- 类型: 研究数据质量事故
- 严重度: P1, 影响研究判断，不影响原始价格事实表
- 影响范围: `institutional_breadth`, `ownership_concentration`, `delta_institutional_ownership` 三个 13F 因子的评估结果

## 摘要

13F 因子评估曾出现“信号极强但覆盖率异常”的组合：

- `institutional_breadth` h=10 IC 约 +0.132, NW t 约 6.97, 但 `coverage_p05` 显示 0%, `n_obs` 只有 28。
- `ownership_concentration` h=10 IC 约 -0.157, NW t 约 -8.24, 但同样覆盖异常。
- `delta_institutional_ownership` 有效样本更少。
- PIT 检查为 0 违规，所以问题不是典型 lookahead，而是数据覆盖和 as-of 选择问题。

最终确认这是两个问题叠加：

1. 历史 13F filing-quarter 回填不完整，导致 2025Q1-Q3 reporting period 在研究窗口内长期稀疏。
2. `event_table_to_asof_panel` 在同一可见日存在多条事件时，曾按因子值参与去重，可能让旧 reporting period 覆盖新 reporting period。

处理后，13F reporting period 覆盖恢复到 8k+ filings 量级，评估 `n_obs` 恢复到 200+。原先极端 IC/t 值消失，结论从“极强信号”降级为“弱到中等迹象，仍需更长样本验证”。

## 影响

受影响的是研究结论可信度，不是生产事实表污染：

- `institutional_holdings` 中历史 filing-quarter 不完整，导致研究层 13F 因子有效截面不足。
- as-of 面板在同可见日多事件场景下存在选择错误风险，影响所有复用该 helper 的事件型因子。
- 原始 `daily_prices`、复权因子事实层、SEC 原始 filing 文本抓取逻辑不在本次影响范围内。

主要风险是把小样本和覆盖稀疏导致的极端统计量误读为可交易信号。

## 根因

### 1. 历史回填缺口

13F 是季度申报。日频评估窗口为 2025-05-16 到 2026-06-26，但窗口内依赖的 2025Q1-Q4 filing-quarter 历史回填不完整。

事故前的表现：

- 13F 因子 `coverage_p05` 显示 0%。
- 13F 因子 `n_obs` 只有几十天，而日频因子有约 278 天。
- 极端 t 值来自很少的有效截面，不可直接采信。

### 2. as-of 同可见日事件选择错误

`research/factors/asof.py` 旧逻辑对 `(security_id, effective_visible_date)` 做值相关去重。对 13F 来说，同一证券在同一可见日可能同时出现多个 reporting period 或 amended filing 聚合结果。

正确语义应该是：

- `effective_visible_date <= eval_date`。
- 在可见日相同或多个候选都已可见时，优先选择 `staleness_anchor_column` 最新的记录。
- 因子值大小不能参与“哪条事件更新”的判断。

旧逻辑会在新季度因子值更小的情况下错误保留旧季度值，尤其会扭曲 `ownership_concentration` 和 `delta_institutional_ownership`。

### 3. 报告指标可读性不足

`coverage_p05` 是 `factor_coverage = factor_present / eligible_universe` 的比例，不是 13F filing 覆盖率，也不是有效 IC 天数。比例被格式化为百分比后容易显示为 0%，掩盖了两件不同的事：

- filing-quarter 是否完整入库。
- 当日 eligible universe 中有多少证券有该因子值。

这不是根因，但放大了诊断误读成本。

## 时间线

时间为远端主机日志时间 UTC。

| 时间 | 事件 |
| --- | --- |
| 2026-06-30 02:44:35 | 开始远端 13F 历史回填 PASS 1, 从 2025Q1 开始。 |
| 2026-06-30 03:44:11 | PASS 1 2025Q1 完成，处理 6938 个 filing, 失败 0, 写入/更新 2,691,834 行。 |
| 2026-06-30 05:02:54 | PASS 1 2025Q2 完成，处理 8730 个 filing, 失败 2, 写入/更新 3,509,824 行。 |
| 2026-06-30 06:20:56 | PASS 1 2025Q3 完成，处理 8878 个 filing, 失败 3, 写入/更新 3,301,713 行。 |
| 2026-06-30 07:16:19 | PASS 1 2025Q4 完成，处理 8576 个 filing, 失败 0, 写入/更新 3,308,404 行。 |
| 2026-06-30 07:16:32 | PASS 2 2025Q1 完成，没有待处理 filing。 |
| 2026-06-30 07:16:44 | PASS 2 2025Q2 完成，重试 2 个 filing, 失败 0, 写入/更新 678 行。 |
| 2026-06-30 07:16:56 | PASS 2 2025Q3 完成，重试 3 个 filing, 失败 0, 写入/更新 1053 行。 |
| 2026-06-30 07:17:08 | PASS 2 2025Q4 完成，没有待处理 filing。 |

远端回填日志：

```text
/home/wenruifeng/projects/stock/logs/manual_backfill/13f_backfill_resume_20260630_024435.log
```

## 修复动作

### 代码修复

修改文件：

- `research/factors/asof.py`
- `tests/test_factors_asof.py`
- `tests/test_institutional.py`

修复内容：

- 移除 as-of 事件表按值去重的逻辑。
- 强制 `effective_visible_date` 为 `datetime64[ns]`，避免 pandas dtype mismatch。
- 让 `merge_asof` 通过 `effective_visible_date` 和 `staleness_anchor_column` 的稳定排序选择最新 anchor。
- 增加回归测试，确保同一可见日下“最新 reporting period 赢”，即使新值小于旧值。

验证命令：

```bash
python -m pytest tests/test_factors_asof.py tests/test_institutional.py
python -m compileall research tests
```

结果：

- 本地 focused pytest: 32 passed。
- 远端 focused pytest: 32 passed。
- 本地 compileall: passed。

### 数据修复

远端执行季度回填：

```bash
cd /home/wenruifeng/projects/stock
source .venv/bin/activate
for pass in 1 2; do
  for q in 2025Q1 2025Q2 2025Q3 2025Q4; do
    python main.py update_institutional_holdings --quarter "$q"
  done
done
```

说明：

- `update_institutional_holdings` 是幂等脚本。
- PASS 2 通过 `filter_pending` 跳过已入库 accession，只重试 PASS 1 中 SEC timeout 的 filing。
- PASS 2 已成功补齐 5 个 timeout filing。

## 修复后数据覆盖

最终 `institutional_holdings` period 覆盖：

| period | filings | rows | mapped_rows | mapped_pct |
| --- | ---: | ---: | ---: | ---: |
| 2024-12-31 | 8518 | 3,254,896 | 2,889,775 | 88.78% |
| 2025-03-31 | 8450 | 3,284,909 | 2,945,009 | 89.65% |
| 2025-06-30 | 8437 | 3,252,400 | 2,935,008 | 90.24% |
| 2025-09-30 | 8378 | 3,284,259 | 2,990,018 | 91.04% |
| 2025-12-31 | 9058 | 3,420,351 | 3,138,729 | 91.77% |

这说明原先 2025Q1-Q3 的稀疏状态已修复，窗口内主要 reporting period 均恢复到 8k+ filings。

## 修复后评估结果

评估命令：

```bash
python -m research.evaluate \
  --factors institutional_breadth,ownership_concentration,delta_institutional_ownership \
  --start 2024-05-14 \
  --end 2026-06-26 \
  --no-persist
```

输出报告：

- `/home/wenruifeng/projects/stock/research/output/evaluate_institutional_breadth_2025-05-16_2026-06-26.md`
- `/home/wenruifeng/projects/stock/research/output/evaluate_ownership_concentration_2025-05-16_2026-06-26.md`
- `/home/wenruifeng/projects/stock/research/output/evaluate_delta_institutional_ownership_2025-05-16_2026-06-26.md`

核心结果：

| factor | IC h=10 | NW t | q_ls_sharpe_net | n_obs | PIT max |
| --- | ---: | ---: | ---: | ---: | ---: |
| institutional_breadth | +0.0281 | 1.28 | +1.00 | 232 | 0 |
| ownership_concentration | -0.0305 | -1.79 | -1.76 | 232 | 0 |
| delta_institutional_ownership | +0.0340 | 1.81 | -1.39 | 202 | 0 |

解读：

- `institutional_breadth` 仍有正向迹象，但显著性不足，不能再称为极强信号。
- `ownership_concentration` 是反向迹象，高集中度后续表现较弱，但高值做多低值做空的方向会亏。
- `delta_institutional_ownership` IC 为正，但多空组合表现反向，不适合直接交易化。
- 旧结果中的极端 NW t 主要来自覆盖缺口和样本不足。

## 复盘

做得好的地方：

- PIT regression 没有误报，确认问题不是 lookahead。
- `n_obs` 和 `coverage_p05` 暴露了异常，避免直接把极端 t 值当作结论。
- `update_institutional_holdings` 幂等设计有效，PASS 2 能快速补齐 timeout。
- as-of helper 的回归测试补上后，可以防止同类错误复发。

做得不够的地方：

- 13F 因子上线后，没有先建立 reporting period 完整性检查。
- 评估摘要没有直接显示 `factor_count_p05`、`factor_count_median`、`ic_valid_days` 等更直观指标。
- 人工解读时过度看重极端 t 值，没有把 `n_obs` 和季频数据稀疏性放在第一优先级。

## 后续动作

建议按优先级处理：

1. 给 `institutional_holdings` 加一个覆盖健康检查：最近 N 个 reporting period 的 distinct accession 数应接近 form index 发现数，mapped_pct 应高于稳定阈值，例如 85%。
2. 在 `research.evaluate` 摘要中增加计数型覆盖指标：`factor_count_p05`, `factor_count_median`, `factor_count_max`, `ic_valid_days`, `days_below_min_coverage`。
3. 当 `n_obs < MIN_OBS` 或 `coverage_p05` 很低时，在报告里给出更强的不可采信标记，而不是只给 `*`。
4. 为所有事件型因子保留同可见日多事件的 as-of 回归测试，避免值大小影响事件新旧选择。
5. 在新接入低频数据源时，先写 period/source completeness SQL，再跑因子评估。

## 当前结论

本次事故已修复。13F 数据覆盖已恢复，as-of 选择 bug 已修，PIT 检查仍为 0。新的 13F 因子结果不再支持“极强信号”的判断，只能认为：

- breadth 方向值得继续观察。
- concentration 的反向信息存在，但交易方向要反过来验证。
- delta ownership 暂不具备直接使用价值。

以后看到“极端 IC/t + coverage 异常 + 低 n_obs”的组合，默认先按数据完整性事故处理，不先按 alpha 发现处理。
