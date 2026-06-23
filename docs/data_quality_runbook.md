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
| **P0 BLOCKING** | 阻塞性问题，数据不可信 | 2 | 立即处理 |
| **P1 WARNING** | 告警，部分数据域受影响 | 1 | 当天关注 |
| **P2 ADVISORY** | 新鲜度超期，非紧急 | 0 | 下次调度处理 |

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

## 调度故障排查

```bash
# systemd 日志
journalctl -u stock-daily-run.service --since today

# 应用日志
tail -100 logs/cron_daily_run.log

# pipeline_task_runs 查询
python main.py health_report --days 7
```
