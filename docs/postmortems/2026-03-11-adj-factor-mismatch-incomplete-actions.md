# Postmortem (Preventative): `adj_factor` mismatch due to incomplete action history

Date: 2026-03-11

## Summary

During validation on AAPL, we observed `daily_prices.adj_factor` around the 2020-08-31 4-for-1 split did not satisfy the expected relationship until action history was completed.

The adjustment-factor algorithm itself was correct; the underlying issue was that the corporate actions dataset was incomplete (missing early dividends/splits), so the computed cumulative factor was missing historical events.

## Impact

- `adj_close = close * adj_factor` was biased for dates prior to the missing events.
- Any return series computed on `adj_close` would under/over-state total return, depending on which actions were missing.

## Root cause

- Polygon.io actions endpoint may return incomplete history depending on plan/entitlements.
- The pipeline previously assumed Polygon actions were “complete enough” to compute a total-return forward adjustment factor.
- AAPL specifically lacked older dividends and two older splits when using Polygon-only actions.

## Resolution

- Added a maintenance command `backfill_actions` to supplement missing dividend/split dates via YFinance:
  - `python main.py backfill_actions AAPL --recalc-adj-factor`
- Implemented per-symbol validation to automatically choose whether YFinance dividends need split “un-adjustment” back to raw units by comparing with existing DB dividends.
- Recomputed `adj_factor` after actions were completed.

## Follow-ups

- Consider adding an optional integrity check that flags suspiciously short action histories (e.g., dividends present only after a recent year for mature tickers).
- For any future multi-source actions ingestion, define an explicit precedence rule (do we trust Polygon or YFinance when both exist but disagree?).

