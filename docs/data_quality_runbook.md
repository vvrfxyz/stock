# 数据质量 Runbook

## 日常监控

```bash
# 一页摘要（连生产库）
python main.py health_report --market US

# 最近 3 天的 pipeline 状态
python main.py health_report --days 3
```

### 读懂 health_report 输出

报告按严重度分层：

| 层级 | 含义 | 退出码 | 处理方式 |
|------|------|--------|---------|
| **P0 BLOCKING** | 阻塞性问题，数据不可信 | 1 | 立即处理 |
| **P1 WARNING** | 告警，部分数据域受影响 | 0（汇总区显著告警） | 当天关注 |
| **P2 ADVISORY** | 新鲜度超期，非紧急 | 0 | 下次调度处理 |

2026-07-07 起：只有 P0 才非零退出。此前 P1-only 也 exit=1，导致
`pipeline_task_runs` 每天把 health_report 记成 FAILED、systemd OnFailure
假告警；P1 汇总仍在报告末尾显著输出，靠日志/报告跟进。

### P0 问题处理

**price_data_latest_date 不一致**：
```bash
# 找出哪些证券不一致
python scripts/check_data_integrity.py --limit 20
# 单只修复
python main.py update_massive_prices AAPL --full-refresh
```

**同 FIGI 多 security_id（身份分裂）**：
```bash
python scripts/audit_security_identity.py
python scripts/repair_identity.py --dry-run
# 确认后
python scripts/repair_identity.py --apply
```

### P1 问题处理

**pipeline 步骤失败**：
```sql
-- 查最近失败的步骤
SELECT task_name, error_sample, started_at
FROM pipeline_task_runs
WHERE status = 'FAILED'
ORDER BY started_at DESC LIMIT 10;
```

**vendor factor mismatch**：
周日 full rebuild 使用 `--fail-on-vendor-mismatch`。如果触发：
```bash
# 查看哪些证券 mismatch
python main.py update_adjustment_factors AAPL --tolerance 0.0001
# 通常是因子窗口边界的时区差异，确认后可提高容忍度
```

### P2 问题处理

**数据新鲜度超期**：通常是 API 配额耗尽或网络问题，下次调度自动重试。
```bash
# 手动刷新
python main.py update_massive_details --market US --all --force
python main.py update_massive_shares --market US --all
```

## 退出码约定

所有脚本统一：
- `0` = 完全成功或无工作
- `1` = 有错误但可恢复（部分失败）
- `2` = 阻塞性数据风险

`scheduled_update` 区分 WARNING (exit=1) 和 BLOCKING (exit=2)，整体退出码取最严重的。
例外（2026-07-07 起）：`health_report` 与 `audit_security_identity` 的 advisory
发现（P1 / 需人工甄别不阻塞）exit=0 只留日志，硬问题（P0 / 阻塞迁移的活跃行
重复）exit=1——避免 advisory 每天把调度打红。

## 调度故障排查

```bash
# systemd 日志
journalctl -u stock-daily-run.service --since today

# 应用日志
tail -100 logs/cron_daily_run.log

# pipeline_task_runs 查询
python main.py health_report --days 7
```

### Massive T+1 延迟窗口（已定性形态，2026-07-09）

update_massive_prices 对**前一交易日**的请求出现**大面积 403 + 少量成功**
（如 2026-07-08 晨跑：10,912 只处理 / 4,925 写入 / 5,872 个 403），当日步骤 FAILED、
水位缺前一交易日大半——这是 **Massive 免费档对 T+1 数据分批开放**的形态，**非 key
失效**（key 坏则全 0 成功）、非休市日边界。判定与处置：
- 次日晨跑自然补齐（实测 07-07：首日 852 行 → 次日补至 10,325）即闭环，**单日缺口
  不升级**；连续两班后仍缺同一交易日才按"连缺两日"升级排查配额/权限。
- 研究侧影响：评估窗口 `--end` 留至最近完整日即可，价格脚本本就只更新到最近完成
  交易时段（CLAUDE.md 数据完整性注意事项）。


## 2026-07-06 新增质量门与调度变化

- `check_data_integrity` 新增三探针：全史 OHLC 包含违规（基线 0，超出即阻塞）、
  近窗 vwap 越界率（>10% 阻塞；~5% 属盘前盘后口径正常）、活跃 US CS/ETF 的
  list_date NULL 计数（>50 阻塞——2026-07-06 每日同步抹除事故的回归防线）。
- `scheduled_update` 每日新增 `update_massive_news_recent`（3 天窗）与
  `health_report` 步；周六新增 `update_minute_bars_weekly`（分钟增量，~70 分钟）；
  周日新增 `update_trading_calendars`（日历保鲜）；周末因子全量改
  `--max-mismatch-rate 0.02`（vendor 口径存量 ~177 只，绝对断言会每周必红）。
- `pipeline_task_runs`：失败步的 error_sample 现含 ERROR 日志尾部（不再只有
  exit=1）；卡死 >12h 的 RUNNING 行开跑即标 ORPHANED。
- `health_report` 同时检查 PG 日线与 CH 分钟线的最近交易日：日线落后超过 1 个 session、
  周更分钟线落后超过 5 个 sessions 记 P1。健康检查固定排在当日全部采集步骤之后；CH
  查询只扫描允许下限后的分区，不要改回无 WHERE 的 `max(toDate(ts))`，51 亿行全表扫描
  会污染 4G cgroup 内存账。
- `pipeline_task_runs` 的历史失败继续展示，但只有某任务的**最新一次**仍为 FAILED 才计 P1；
  后续 SUCCESS 已恢复的旧失败不再制造持续告警。
- `update_minute_bars` 的 API 抓取保留 8 并发，但跨证券累计 50,000 行后由主线程串行
  INSERT，并关闭 ClickHouse parallel parsing；生产首跑实证这是 3.2G 服务端预算下的稳定写法。
  所有消费 `ReplacingMergeTree` 分钟事实的聚合/修复查询必须使用 `FINAL`，避免重叠窗口
  写入尚未后台合并时重复累计成交量或读取旧值。
- audit_security_identity 的"审计有发现待人工"是 advisory：2026-07-07 起 exit=0
  只留日志告警（此前 exit=1 被 pipeline_task_runs 记为 FAILED 假故障）。
