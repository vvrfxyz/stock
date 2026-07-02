# Debian Stock Deployment

部署目录：`/home/wenruifeng/projects/stock`

> 同步代码、迁移、故障排查的完整操作手册见 `docs/deployment.md`。
> 该机器无法访问 github.com，必须从本地 SSH 直推同步。

已准备的基础服务：
- PostgreSQL 17.5 via Docker Compose: `stock-postgres`

## 定时策略

生产定时由 systemd timer 管理。timer 每天在 UTC+8 `10:00`
（`Asia/Shanghai`，本机 UTC 时间 `02:00`）启动一次
`python main.py scheduled_update --market US`。

`scheduled_update` 内部负责把不同频率的任务错峰（权威实现见 `main.py` 的
`build_scheduled_update_steps()`，逐条命令参数见根 `README.md` 的「常用命令」小节）：

| 频率 | 时间 | 任务 |
| --- | --- | --- |
| 每天 | 每次 timer 触发 | `sync_massive_universe`、`update_massive_prices` 增量、`update_massive_short_data` 增量、`update_massive_actions --recent-days 14`、`update_adjustment_factors --changed-since 3` 增量、当日 `update_open_close_summary --all`、`check_data_integrity --window-days 14` |
| 每周 | 周六 | `update_massive_shares --all`、最近 5 个交易日 grouped daily、`update_open_close_summary` 5 日窗口补漏 |
| 每周 | 周日 | `update_fx_rates`、`update_risk_free_rates`（DTB3）、`update_massive_actions --all --force`、`update_adjustment_factors --all --fail-on-vendor-mismatch` 全量重建、SEC identifiers/filings/fundamentals/insider 增量、CUSIP/13F 增量、`audit_security_identity` |
| 每月 | 第一个周二 | `update_massive_events --all --force` |
| 每月 | 第一个周三 | `update_massive_details --all --force` |
| 按需 | 手动执行 | `update_massive_news`、各类 `--force` / `--full-refresh` 全量重建 |

`update_massive_short_data` 的默认路径是增量更新，会从库里已有的
short interest / short volume 最大日期继续拉。`--force` 仍然保留，用于以后升级会员后一次性回补 Massive 可覆盖窗口。

## 安装或刷新 timer

```bash
cd /home/wenruifeng/projects/stock
chmod +x scripts/run_daily_cron.sh scripts/backup_postgres.sh
sudo install -m 0644 deploy/systemd/stock-daily-run.service /etc/systemd/system/
sudo install -m 0644 deploy/systemd/stock-daily-run.timer /etc/systemd/system/
sudo install -m 0644 deploy/systemd/stock-postgres-backup.service /etc/systemd/system/
sudo install -m 0644 deploy/systemd/stock-postgres-backup.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now stock-daily-run.timer stock-postgres-backup.timer
systemctl list-timers stock-daily-run.timer stock-postgres-backup.timer
```

## 备份与 dead-man's switch

`scripts/backup_postgres.sh` 默认走 `docker exec stock-postgres pg_dump --format=custom`
（生产库在容器内，宿主机不一定装匹配版本的 client），生成可恢复备份，默认写到
`$HOME/stock_backups/postgres`（不放进 git 工作树），可用环境变量覆盖：

```bash
STOCK_BACKUP_DIR=/data/stock_backups STOCK_BACKUP_RETENTION_DAYS=30 scripts/backup_postgres.sh
```

- 默认 `STOCK_BACKUP_MODE=docker`，读 `.env` 的 `POSTGRES_USER`/`POSTGRES_DB`，容器名可用
  `STOCK_PG_CONTAINER` 覆盖（默认 `stock-postgres`）。运行用户若不在 docker 组但有免密
  `sudo docker`（253 即如此），`STOCK_DOCKER_SUDO=auto`（默认）会自动回退到 `sudo -n docker`；
  可用 `STOCK_DOCKER_SUDO=1/0` 显式强制。
- 设 `STOCK_BACKUP_MODE=host` 改用宿主机 `pg_dump` + `DATABASE_URL`（需自行装 postgresql-client）。
- dump 产出空文件会直接报错退出，systemd unit 已挂 `OnFailure=stock-notify-failure@%n`，
  备份失败会走告警链而非静默。
- 如需离机副本，设置 `STOCK_BACKUP_RSYNC_TARGET=user@host:/path/`，本地备份成功后再 `rsync`。
  恢复演练示例会在脚本输出末尾打印。

`run_daily_cron.sh` 成功完成后会读取可选的 `STOCK_HEALTHCHECK_URL` 并发送 HTTP ping；
建议填 healthchecks.io 这类 dead-man's switch URL，让“没有成功心跳”也能告警。

## 手动执行一次

```bash
cd /home/wenruifeng/projects/stock
scripts/run_daily_cron.sh
```

或者直接让 systemd 执行：

```bash
sudo systemctl start stock-daily-run.service
```

## 常用检查

```bash
cd /home/wenruifeng/projects/stock
docker compose ps
systemctl status stock-daily-run.timer
systemctl list-timers --all stock-daily-run.timer
journalctl -u stock-daily-run.service -n 100 --no-pager
tail -f logs/cron_daily_run.log
```

## 部署前身份对账（建议）

`active-only` 唯一索引迁移要求库内不存在重复的活跃 symbol / (current_symbol, exchange)。
跑 `e5f6a7b8c9d0` 这步 `alembic upgrade head` 前先做只读对账，存在阻塞冲突会退出码 2：

```bash
.venv/bin/python scripts/audit_security_identity.py --limit 30
```

返回 2：有阻塞迁移的活跃行重复，必须先人工处理；返回 1：有需甄别的存量身份异常
（分裂/回收/映射歧义），不阻塞迁移但建议在提高 universe 同步频率前清理。
