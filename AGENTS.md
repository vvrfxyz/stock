# Repository Guidelines

## Agent Working Rules
0. Greenfield Architecture First
   - Treat this project as a greenfield system unless the user explicitly says to preserve legacy behavior.
   - Do not keep compatibility shims, legacy tables, or old data-source paths merely because they already exist.
   - Prefer the target architecture over incremental patching when the two conflict.
   - Use `security_id` as the canonical identity across the system; `symbol` is an attribute/history item, not a durable key.
   - Keep PostgreSQL as the metadata/event/ACID store and ClickHouse as the future matrix-compute store.
   - Design PostgreSQL fields so they map cleanly to ClickHouse types and sort keys later.
   - Price bars should represent raw market facts. Do not store adjusted prices, adjustment factors, turnover, or technical indicators on `daily_prices`.
   - Adjustment factors may be stored only in explicitly separated layers: vendor-provided factors as reference snapshots, internally computed factors as reproducible caches with a methodology version and event hash. They are never source-of-truth facts.
   - New index-membership data should be generic, such as `index_constituents_history`, and anchored by `security_id`; avoid index-specific symbol-keyed tables.

1. Think Before Coding
   - State assumptions clearly before implementing.
   - Ask when important details are unknown instead of guessing.
   - When wording is ambiguous, call out the plausible interpretations.
   - If a simpler approach exists, argue for or against it before choosing.

2. Simplicity First
   - Write the smallest amount of code that solves the problem.
   - Do not add speculative features.
   - Do not introduce an abstraction for code that is only used once.

3. Surgical Changes
   - Touch only the files and lines required for the task.
   - Do not opportunistically optimize unrelated code, comments, or formatting.
   - Do not fix code that is not broken.
   - Match the existing style of the surrounding code.

## Project Structure & Module Organization
`main.py` is the CLI controller for the daily workflow and individual update commands. Put task-specific ingestion and maintenance logic in `scripts/` (`update_*`, `migrate_database.py`, `calibrate_price_latest_date.py`). Keep database schema in `data_models/models.py`, and route shared session/upsert logic through `db_manager.py`. External API adapters belong in `data_sources/`, while small reusable helpers belong in `utils/`. Schema migrations live in `alembic/versions/`. Runtime logs are written to `logs/`.

## Build, Test, and Development Commands
Use a local virtualenv and install dependencies before running anything:

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
alembic upgrade head
python main.py daily_run --market US
```

Useful task-level commands:

```bash
python main.py update_details AAPL NVDA --workers 4
python main.py update_massive_prices AAPL --full-refresh
python main.py update_grouped_daily --start-date 2026-03-01 --end-date 2026-03-10
python scripts/migrate_database.py
```

`alembic upgrade head` applies the latest schema to `DATABASE_URL`. `migrate_database.py` requires `OLD_DATABASE_URL` and `NEW_DATABASE_URL`.

## Coding Style & Naming Conventions
Follow PEP 8 with 4-space indentation. Use `snake_case` for modules, functions, variables, and CLI flags; use `PascalCase` for SQLAlchemy models such as `Security` and `DailyPrice`. Keep new ingestion code script-oriented and reuse `DatabaseManager` instead of duplicating connection or upsert logic. Prefer `loguru` for operational logs and keep docstrings short and concrete.

## Testing Guidelines
There is no committed automated test suite yet. Before opening a PR, run a targeted command against a non-production database and verify syntax with `python -m compileall .`. For schema changes, add or update an Alembic revision and confirm `alembic upgrade head` succeeds. If you add tests, place them under `tests/` and name them `test_<feature>.py`.

## Commit & Pull Request Guidelines
Recent history uses concise Conventional Commit prefixes such as `feat:`, `fix:`, and `refactor:`. Follow `type: imperative summary`, for example `fix: handle empty Massive response`. Keep commits focused, especially when changing both migrations and ingestion scripts. PRs should include the purpose, commands run, required env or schema changes, and sample log output for data pipeline changes. Mention new Alembic revision IDs explicitly.

## Security & Configuration Tips
Keep secrets in `.env` and out of git. Common variables are `DATABASE_URL`, `OLD_DATABASE_URL`, and `NEW_DATABASE_URL`; Massive keys are read from `activation_value.txt`. Do not commit production dumps, generated logs, or API credentials.
