# Wave 14 preregistration: earnings gaps and volume confirmation

> Frozen date: 2026-07-12. This document is written before any Wave 14
> primary or stability result is run.
> Family: `earnings_gap`. Only the two definitions below are allowed. A
> failure does not authorize threshold searches, alternate ATR windows, or
> related candlestick variants.

## Question

Daily cross-sectional technical factors dilute sparse information events. This
study instead asks whether a gap caused by an earnings-related disclosure has
post-close continuation, and whether full reaction-day volume strengthens that
continuation. It does not claim to capture the opening jump: every position is
formed at the reaction-day close.

## Event sample and point-in-time rule

The source is `sec_filings`, restricted to original `SEC_EDGAR` `8-K` filings
whose comma-delimited `items` field contains item `2.02`. `8-K/A` records are
counted in the audit but are not mixed into the primary sample.

Each event must have non-null `security_id`, `accepted_at`, and
`period_of_report`. Before any timing filter, retain exactly one event for each
`security_id x period_of_report`: the earliest `accepted_at`, then
`accession_number` as a deterministic tie break. Later disclosures for that
reporting period are excluded rather than selected after observing outcomes.

The XNYS rows in `trading_calendars` are the only event-time calendar. A
missing calendar row is a hard error; weekday inference is not permitted.

- `accepted_at < session open`: reaction day is that XNYS session.
- `accepted_at > session close`: reaction day is the next XNYS session.
- non-session timestamp: reaction day is the next XNYS session.
- timestamps in `[session open, session close]`, including boundaries, are
  excluded from the main sample. Daily OHLCV cannot determine a clean
  post-disclosure opening gap for them.

The reaction day must have raw positive open, high, low, close, and volume;
the immediately preceding XNYS session must have a positive raw close. Events
with a split or dividend ex-date on the reaction day are excluded. Securities
with an uncovered adjustment event in the price window are excluded using the
project-standard research integrity gate.

After timing is assigned, a security can still have more than one retained
reporting period mapped to the same reaction day. Before any price, signal, or
return check, retain only the earliest `accepted_at` event for each
`security_id x reaction_day`, with `accession_number` as the deterministic tie
break. The later event is counted in the audit and never becomes a replacement
because the earlier event later fails an eligibility check.

## Signal definitions

All formation variables below are raw daily-price facts. Let `d` be the
reaction day and `C[d-1]` be the raw close on the immediately preceding XNYS
session.

1. `gap = O[d] / C[d-1] - 1`.
2. `TR[t] = max(H[t]-L[t], abs(H[t]-C[t-1]), abs(L[t]-C[t-1]))`.
3. `ATR20[d]` is the arithmetic mean of `TR[d-20] ... TR[d-1]`.
4. `atr_pct = ATR20[d] / C[d-1]`.
5. `gap_atr = gap / atr_pct`.
6. `volume_ratio = min(V[d] / median(V[d-20] ... V[d-1]), 3)`.
7. `gap_atr_volume_confirmed = gap_atr * volume_ratio`.

All 20 observations required by ATR and volume are required; no missing-day
scaling, imputation, or alternate window is allowed. Standard eligibility is
also fixed at `C[d-1] >= $3` and 63-session median prior dollar volume of at
least `$2m`. A zero or non-finite signal is not a tradable event.

## Returns and portfolios

Forward asset returns use research-layer adjusted close so splits and
dividends do not create holding-period returns. The market proxy is SPY total
return; the script asserts its adjustment-factor coverage before a run.

For each event, daily abnormal return after close is
`r[i,t] - r[SPY,t]`. The diagnostic `CAR_h` is the sum from `d+1` through
`d+h`, for `h in {1, 5, 20}`. It excludes the reaction-day open-to-close move.

For a signal `s`, every reaction-day cohort uses gross-normalized weights
`w[i] = s[i] / sum(abs(s))`. A calendar-time portfolio holds each cohort for
exactly `h` subsequent close-to-close returns, with equal capital across the
`h` active cohorts. The portfolio is entered and exited at closing prices.
Market adjustment subtracts its daily net SPY exposure times SPY return.

An event is complete only when it has all 20 post-close adjusted daily returns
and the matching SPY returns. Events without that complete path, including
events in the final 20 observed XNYS sessions, remain in the audit count but
are excluded from every reported horizon and portfolio. This fixes the event
set across the 1-, 5-, and 20-session results rather than letting shorter
horizons admit additional tail observations. `valid-event net CAR` is the
arithmetic mean of complete reaction-day cohorts' signal-weighted CAR after
one entry and one exit cost.

Report gross results and net results at 10, 25, and 40 bps per side. The
primary net cost is 25 bps; each complete cohort pays one entry and one exit.
No borrow fee is assumed, so the short-gap leg is an explicit limitation.

The primary sample is 2016-01-04 to the latest complete trading day used in
this run. The stability sample is 2007-07-02 to 2015-12-31. Event formation
dates, not subsequent outcome dates, define sample membership.

Newey-West inference is applied to the daily calendar-time market-adjusted
portfolio return, with lag
`max(h, floor(4*(n/100)^(2/9)))`, which covers overlapping cohorts.

## Hypotheses and decision rules

### H1: `gap_atr`

The signed standardized gap has positive post-close market-adjusted
continuation.

Primary discovery requires all of the following:

- primary 20-session net calendar-time mean return is positive and NW t >= 3;
- primary 1- and 5-session net means are positive;
- stability 20-session net mean has the same positive sign;
- 20-session valid-event net CAR is positive.

### H2: `gap_atr_volume_confirmed`

The volume-confirmed gap has stronger post-close continuation than `gap_atr`.

Primary discovery requires all H1 conditions for the volume-confirmed signal,
plus all of the following:

- primary net calendar-time mean is higher than `gap_atr` at 1, 5, and 20
  sessions;
- primary 20-session valid-event net CAR is at least 125% of the corresponding
  `gap_atr` net CAR;
- stability 20-session volume-confirmed net mean is positive.

The event-vs-matched-non-event gap comparison, gap-fill probability,
maximum-favorable/adverse excursion, size, liquidity, and timing splits are
diagnostics. They run only if H2 passes; they cannot replace a failed primary
criterion.

## Multiple testing and stop rule

The family contains exactly H1 and H2. The 1- and 5-session horizons,
cost-pressure table, timing split, and path diagnostics are supporting checks,
not substitutes for the 20-session main result.

- H1 and H2 both fail: close `earnings_gap`; do not test alternative ATR,
  volume, gap-size, holding-period, or reversal variants.
- H1 passes but H2 fails: retain only the bare `gap_atr` definition; no volume
  threshold or alternative cap may be searched.
- H2 passes but its conditional non-event comparison fails: record the result
  as an execution/event-reaction finding, not a deployable holding-period
  alpha.

## Literature anchors

- Ball, R. and P. Brown (1968), *An Empirical Evaluation of Accounting Income
  Numbers*, Journal of Accounting Research.
- Bernard, V. and J. Thomas (1989), *Post-Earnings-Announcement Drift*, Journal
  of Accounting Research.
- The event definition intentionally uses 8-K Item 2.02 and SEC acceptance
  timestamps, not analyst-surprise data, which this project does not have.
