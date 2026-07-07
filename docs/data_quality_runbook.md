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
- audit_security_identity 的"审计有发现待人工"是 advisory：2026-07-07 起 exit=0
  只留日志告警（此前 exit=1 被 pipeline_task_runs 记为 FAILED 假故障）。
