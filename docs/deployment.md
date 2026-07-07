# Deployment: 192.168.1.253 (home-debian)

生产部署的完整操作手册。日常调度策略见 `README.debian.md`。

## 基本信息

| 项 | 值 |
| --- | --- |
| 主机 | `192.168.1.253`，ssh 别名 `home-debian`（root 登录，`~/.ssh/config`） |
| 部署目录 | `/home/wenruifeng/projects/stock` |
| 运行用户 | `wenruifeng`（systemd 服务以该用户执行） |
| Python | `.venv/bin/python`（注意：venv 内未安装 pytest） |
| 数据库 | docker compose: `stock-postgres` (PG 17.5) + `stock-clickhouse`（24.12，**自 2026-07 起承载 50.6 亿行分钟线 `stock.minute_bars`，绝不可移除**；HTTP 8123 供装载与研究读取） |
| 定时 | `stock-daily-run.timer` → 每天 10:00 Asia/Shanghai 跑 `scheduled_update` |
| GitHub | `https://github.com/vvrfxyz/stock`（**远端机器无法访问 github.com**） |

远端独有、绝不能被同步覆盖或提交的文件：

- `.env`（数据库连接等）
- `activation_value.txt`（Massive API keys，一行一个）

## 同步流程（标准做法：SSH 直推）

远端连不上 GitHub，所以不要在远端 `git pull`。在本地：

```bash
# 1. 本地提交并推 GitHub
git push origin main

# 2. 把同一个 commit 直推到远端仓库的临时分支
#    （首次需要远端: git config receive.denyCurrentBranch ignore，
#      以及 root 侧: git config --global --add safe.directory /home/wenruifeng/projects/stock/.git）
git push ssh://home-debian/home/wenruifeng/projects/stock <sha>:refs/heads/_sync_main

# 3. 远端对齐工作区并清理
ssh home-debian "cd /home/wenruifeng/projects/stock \
  && git checkout main && git reset --hard <sha> && git branch -D _sync_main \
  && chown -R wenruifeng:wenruifeng ."
```

注意事项：

- root 执行 git 操作会把文件属主改成 root，最后一步 `chown` 必须执行，否则 systemd 任务（以 wenruifeng 运行）可能写日志失败。
- 同步前确认远端没有任务在跑：`pgrep -af main.py`，以及 `ls -la /tmp/stock_daily_run.lock`。
- 如果只想快速同步工作区（不走 git），可用 rsync，但必须带排除项：

```bash
rsync -av --delete --chown=wenruifeng:wenruifeng \
  --exclude='.git/' --exclude='.venv/' --exclude='logs/' \
  --exclude='__pycache__/' --exclude='*.pyc' --exclude='.pytest_cache/' \
  --exclude='.idea/' --exclude='.claude/' \
  --exclude='.env' --exclude='activation_value.txt' \
  ./ home-debian:/home/wenruifeng/projects/stock/
```

## 同步后的固定动作

```bash
ssh home-debian "cd /home/wenruifeng/projects/stock \
  && sudo -u wenruifeng .venv/bin/alembic upgrade head \
  && sudo -u wenruifeng .venv/bin/python -c 'import main; print(\"imports OK\")' \
  && sudo -u wenruifeng .venv/bin/python main.py scheduled_update --help >/dev/null && echo 'CLI OK'"
```

- schema 有变更时 `alembic upgrade head` 必须跑（迁移以 `.env` 的 `DATABASE_URL` 为准）。
- 验证约束/表结构可直接进容器：
  `docker exec stock-postgres psql -U postgres -d stock -c "\d daily_prices"`。

## 运行状态检查

```bash
ssh home-debian "systemctl list-timers stock-daily-run.timer --no-pager"
ssh home-debian "journalctl -u stock-daily-run.service -n 100 --no-pager"
ssh home-debian "tail -50 /home/wenruifeng/projects/stock/logs/cron_daily_run.log"
```

`scheduled_update` 现在会隔离单步失败、继续后续步骤，并在有失败时以非 0 退出——
`journalctl` 里 service 失败即代表当天至少一步出错，去 `logs/` 下对应脚本日志定位。

## 研究长任务标准发射器（run_research.sh，2026-07-07）

研究/回填长任务不再裸 `nohup`，一律走 `scripts/run_research.sh`：

```bash
# 253 上，wenruifeng 身份
scripts/run_research.sh ta-zoo -- .venv/bin/python -m research.evaluate --factors size --start 2016-01-04
journalctl --user -u research-ta-zoo -f          # 跟进度（Progress 行全在这）
systemctl --user show research-ta-zoo -p Result  # 死因：oom-kill / exit-code / signal
```

要点：固定 unit 名（日志不再靠 `ls -t logs/` 猜）、`MemoryHigh=5G`/`MemoryMax=7G`
护住 11G 机器（可用 `RESEARCH_MEMORY_HIGH/MAX` 环境变量覆盖）、失败经
`stock-research-notify@` 走既有 `notify_failure.sh`（logs/failures.log + 可选 webhook）。

一次性准备（每台机器一次）：

```bash
sudo loginctl enable-linger wenruifeng    # user manager 不随 ssh 会话退出，OnFailure 才发得出
mkdir -p ~/.config/systemd/user
cp deploy/systemd/user/stock-research-notify@.service ~/.config/systemd/user/
systemctl --user daemon-reload
```

## 已知环境差异

- 远端 venv 没有 pytest；测试只在本地跑（600+ 用例，2026-07 时点 634 个）。
- 远端 git 首次以 root 操作需要 `safe.directory` 配置（`/home/wenruifeng/projects/stock` 和 `.../.git` 两条）。
- docker-compose 端口默认绑定 `0.0.0.0`，`.env` 可用 `POSTGRES_BIND` 收紧为 `127.0.0.1`。

## 同步记录

| 日期 | commit | 内容 |
| --- | --- | --- |
| 2026-06-11 | `b7697f1` | 深度 review 修复（数据正确性/健壮性/清理），应用迁移 `a1b2c3d4e5f6`（删除 daily_prices 冗余唯一约束） |
| 2026-05-15 | `15fe91e` | Massive-only 管道重构（初始部署基线） |
