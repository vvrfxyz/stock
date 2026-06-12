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

`scheduled_update` 内部负责把不同频率的任务错峰：

| 频率 | 时间 | 任务 |
| --- | --- | --- |
| 每天 | 每次 timer 触发 | `update_massive_prices` 增量、`update_massive_short_data` 增量、最近已完成交易日的 `update_open_close_summary --all` |
| 每周 | 周六 | `update_massive_shares --all` |
| 每周 | 周日 | `update_massive_actions --all --force`、SEC identifiers/filings/fundamentals/insider 增量 |
| 每月 | 第一个周二 | `update_massive_events --all --force` |
| 每月 | 第一个周三 | `update_massive_details --all --force` |
| 按需 | 手动执行 | `update_massive_news`、`update_adjustment_factors`、各类 `--force` / `--full-refresh` 全量重建 |

`update_massive_short_data` 的默认路径是增量更新，会从库里已有的
short interest / short volume 最大日期继续拉。`--force` 仍然保留，用于以后升级会员后一次性回补 Massive 可覆盖窗口。

## 安装或刷新 timer

```bash
cd /home/wenruifeng/projects/stock
chmod +x scripts/run_daily_cron.sh
sudo install -m 0644 deploy/systemd/stock-daily-run.service /etc/systemd/system/
sudo install -m 0644 deploy/systemd/stock-daily-run.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now stock-daily-run.timer
systemctl list-timers stock-daily-run.timer
```

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
