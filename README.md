# Stock Data Pipeline

一个用于将美股/证券基础信息、公司行动（分红/拆股）和日线行情写入 PostgreSQL 的数据管道与命令行工具。

数据源：
- Massive：US 股票详情、公司行动、日线聚合、Grouped Daily 刷新、shares/float
- 东方财富（通过 `akshare`）：手动应急价格兜底

## Quickstart

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
alembic upgrade head
python main.py daily_run --market US
```

`.env` 至少需要：
- `DATABASE_URL`：PostgreSQL 连接串
- `MASSIVE_API_KEYS`：Massive API Key（多个 key 用英文逗号分隔）

## 常用命令

```bash
# 标准每日增量流程（Universe -> 详情 -> 公司行动 -> Massive 个股/Grouped Daily -> shares -> turnover）
python main.py daily_run --market US

# 同步 Massive universe
python main.py sync_massive_universe --market US

# 单独更新详情/公司行动
python main.py update_details AAPL NVDA --workers 4
python main.py update_actions --market US --workers 4
# 默认：update_actions 完成后会自动重算 adj_factor

# 单独更新价格
python main.py update_massive_prices AAPL --full-refresh
python main.py update_grouped_daily --market US --start-date 2026-03-01 --end-date 2026-03-10
python main.py update_em_prices --market US

# 复权因子（前复权 + total return：含拆股+现金分红）
python main.py recalc_adj_factor AAPL

# 补全公司行动历史（当 Massive 免费层 2 年窗口之外存在缺口时，用 YFinance 补齐；默认仅插入缺失日期）
python main.py backfill_actions AAPL --recalc-adj-factor

# 历史股本 & 换手率重建（优先 float_shares，否则 total_shares）
python main.py update_massive_shares AAPL --full-refresh
python main.py rebuild_turnover_rate --market US

# 清理非普通股 / ETF / ADR 证券（默认 dry-run）
python main.py cleanup_us_universe
python main.py cleanup_us_universe --apply

# 迁移数据库（需要 OLD_DATABASE_URL / NEW_DATABASE_URL）
python scripts/migrate_database.py
```

## 目录结构

- `main.py`：CLI 中央控制器（集成调用 `scripts/` 的各个任务）
- `scripts/`：各类更新/维护脚本（`update_*`, `migrate_database.py`, `calibrate_price_latest_date.py`）
- `data_models/models.py`：SQLAlchemy ORM 模型（schema）
- `db_manager.py`：`DatabaseManager`（session/upsert/批量写入）
- `data_sources/`：外部数据源适配器（当前主要为 Massive）
- `alembic/`：数据库迁移
- `logs/`：运行日志（loguru 旋转写入）

## 开发提示

- 语法检查：`python -m compileall .`
- 一致性检查（只读）：`python scripts/check_data_integrity.py`
- schema 变更：更新 `data_models/models.py` 并生成/应用 Alembic revision（`alembic revision --autogenerate ...` / `alembic upgrade head`）

## 数据一致性约定（重要）

- 价格更新脚本默认只更新到最近一个**已收盘**交易日（避免时区差异与盘中数据）。
- 只写“数据源确实提供”的字段，避免把未知字段写成 `NULL` 覆盖既有值：
  - 东方财富更新不应覆盖 `daily_prices.vwap`；
  - Massive 日线更新不应覆盖 `daily_prices.turnover_rate`；
  - Grouped Daily 刷新仅更新已存在记录，并保持 `turnover_rate` 等字段原样。

- `daily_prices.adj_factor`：
  - 定义：前复权（最新交易日=1）+ total return（拆股+现金分红）。
  - 用法：`adj_close = close * adj_factor`（OHLC 同理）。

## 更多文档

- 架构与约定：`docs/architecture.md`
- 复权因子口径：`docs/adj_factor.md`
- 公司行动补全：`docs/actions_backfill.md`
- 变更记录：`CHANGELOG.md`
- 事后分析（预防性）：`docs/postmortems/2026-03-11-security-upsert-overwrite.md`
- 事后分析（预防性）：`docs/postmortems/2026-03-11-adj-factor-mismatch-incomplete-actions.md`
