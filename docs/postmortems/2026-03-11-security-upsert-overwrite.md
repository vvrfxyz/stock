# Postmortem (Preventative): `upsert_security_info` overwrote maintenance fields

Date: 2026-03-11

## Summary

During a full repo review, we identified a data-integrity risk in `db_manager.py`:
`DatabaseManager.upsert_security_info()` built its `ON CONFLICT DO UPDATE` `set_` clause by iterating `stmt.excluded`,
which enumerates **all columns** of the `securities` table. This caused fields **not provided** by the details updater
to be updated to `NULL`/`DEFAULT` on every run.

## Potential impact

Depending on the current schema and workload, this could:
- Reset `securities.price_data_latest_date` to `NULL`, forcing unnecessary full refreshes or repeated backfills.
- Reset other maintenance columns (e.g. action/price timestamps), obscuring operational visibility.
- Create hard-to-debug behavior where running “details update” unexpectedly affects price pipelines.

We did not attempt to prove whether this happened historically; the change is treated as a correctness fix to prevent it.

## Root cause

- SQLAlchemy’s `insert(...).excluded` is a column collection for the table, not “only the keys you inserted”.
- The implementation updated every column except a small hard-coded denylist (`id`, `symbol`, `em_code`), so absent keys
  (e.g. `price_data_latest_date`) were overwritten by excluded values (often `NULL`).

## Resolution

- `DatabaseManager.upsert_security_info()` now:
  - updates **only** keys explicitly present in `security_data`;
  - protects maintenance fields (`price_data_latest_date`, `actions_last_updated_at`, etc.) from being updated by the
    details updater;
  - ensures `full_refresh_interval` is set for insert paths (required by NOT NULL constraint when server defaults are
    removed).

## Follow-ups

- Consider adding a lightweight “data integrity” check script (or a future test) to validate that details updates never
  modify price/action maintenance fields.
- Consider adding an operational dashboard query that flags sudden drops of `price_data_latest_date` to `NULL`.

