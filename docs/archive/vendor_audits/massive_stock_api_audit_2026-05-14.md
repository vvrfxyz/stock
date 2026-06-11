# Massive Stocks API Key Audit Archive (2026-05-14)

Status: Archived vendor capability evidence. This file is not an ingestion contract and is intentionally kept outside the current documentation path.

This archive preserves the useful conclusions from the 2026-05-14 Massive Stocks REST endpoint audit. The original raw audit was generated from Massive's Stocks REST documentation and local `activation_value.txt` keys. It included endpoint paths, sampled parameters, response snippets, and entitlement results. The current ingestion contract lives in `docs/massive_free_tier_daily_data.md`, `docs/architecture.md`, and the SQLAlchemy/Alembic schema.

## Audit Result Summary

- Audit date: 2026-05-14.
- Local key count at audit time: 20.
- Official Stocks REST endpoints tested: 46.
- Callable with local keys: 34.
- Not authorized: 12.
- Not-authorized groups: snapshots, trades/quotes, financial statements, ratios.
- Callable groups with useful raw/reference data: tickers/reference, daily aggregates, market operations, corporate actions, short data, float, filings, news, and technical indicators.

## Current Architecture Decisions Absorbed From This Audit

- Raw daily bars are stored as market facts only.
- `daily_prices` stores raw OHLCV/VWAP/trade_count/pre_market/after_hours/OTC fields, not adjusted prices or indicators.
- Vendor adjustment factors are kept outside raw price facts.
- Technical indicators from Massive are vendor references for future calculation QA, not fact-table data.
- Exchange/static reference data belongs in `exchanges`; per-day session data belongs in `trading_calendars`.
- SEC foundation tables are `sec_filings`, `insider_transactions`, `institutional_holdings`, and `security_identifiers`.
- Financial statements and ratios were not available to the audited keys and remain outside current ingestion.

## Endpoint Capability Matrix

| Group | Endpoint | Status | Key fields observed |
| --- | --- | --- | --- |
| Tickers / Reference | `GET /v3/reference/tickers` | Callable | `ticker`, `name`, `market`, `locale`, `primary_exchange`, `type`, `active`, `currency_name`, `cik`, `composite_figi`, `share_class_figi`, `last_updated_utc` |
| Tickers / Reference | `GET /v3/reference/tickers/{ticker}` | Callable | ticker metadata plus `market_cap`, `description`, `sic_code`, `sic_description`, `ticker_root`, `homepage_url`, `total_employees`, `list_date`, `branding`, `share_class_shares_outstanding`, `weighted_shares_outstanding`, `round_lot` |
| Tickers / Reference | `GET /v3/reference/tickers/types` | Callable | `code`, `description`, `asset_class`, `locale` |
| Tickers / Reference | `GET /v1/related-companies/{ticker}` | Callable | `ticker` |
| Aggregates / OHLC | `GET /v2/aggs/ticker/{ticker}/range/{multiplier}/{timespan}/{from}/{to}` | Callable | `v`, `vw`, `o`, `c`, `h`, `l`, `t`, `n` |
| Aggregates / OHLC | `GET /v2/aggs/grouped/locale/us/market/stocks/{date}` | Callable | `T`, `v`, `vw`, `o`, `c`, `h`, `l`, `t`, `n` |
| Aggregates / OHLC | `GET /v1/open-close/{ticker}/{date}` | Callable | `from`, `symbol`, `open`, `high`, `low`, `close`, `volume`, `afterHours`, `preMarket` |
| Aggregates / OHLC | `GET /v2/aggs/ticker/{ticker}/prev` | Callable | `T`, `v`, `vw`, `o`, `c`, `h`, `l`, `t`, `n` |
| Snapshots | `GET /v2/snapshot/...` and `GET /v3/snapshot` | Not authorized | `message` |
| Trades / Quotes | `GET /v3/trades/{ticker}`, `GET /v3/quotes/{ticker}`, last trade/quote endpoints | Not authorized | `message` |
| Technical Indicators | `GET /v1/indicators/sma/{ticker}` | Callable | `underlying`, `values` |
| Technical Indicators | `GET /v1/indicators/ema/{ticker}` | Callable | `underlying`, `values` |
| Technical Indicators | `GET /v1/indicators/macd/{ticker}` | Callable | `underlying`, `values` |
| Technical Indicators | `GET /v1/indicators/rsi/{ticker}` | Callable | `underlying`, `values` |
| Market Operations | `GET /v3/reference/exchanges` | Callable | `id`, `type`, `asset_class`, `locale`, `name`, `acronym`, `mic`, `operating_mic`, `participant_id`, `url` |
| Market Operations | `GET /v1/marketstatus/upcoming` | Callable | `date`, `exchange`, `name` |
| Market Operations | `GET /v1/marketstatus/now` | Callable | `afterHours`, `earlyHours`, `nasdaq`, `nyse`, `otc`, `market`, `serverTime` |
| Market Operations | `GET /v3/reference/conditions` | Callable | `id`, `type`, `name`, `asset_class`, `sip_mapping`, `update_rules`, `data_types` |
| Corporate Actions | `GET /vX/reference/ipos` | Callable | `ticker`, `last_updated`, `announced_date`, `listing_date`, `issuer_name`, `currency_code`, `isin`, `final_issue_price`, `max_shares_offered`, `total_offer_size`, `primary_exchange`, `shares_outstanding`, `security_type`, `ipo_status` |
| Corporate Actions | `GET /v3/reference/splits` | Callable | `execution_date`, `id`, `split_from`, `split_to`, `ticker` |
| Corporate Actions | `GET /stocks/v1/splits` | Callable | `id`, `execution_date`, `split_from`, `split_to`, `ticker`, `adjustment_type`, `historical_adjustment_factor` |
| Corporate Actions | `GET /v3/reference/dividends` | Callable | `cash_amount`, `currency`, `declaration_date`, `dividend_type`, `ex_dividend_date`, `frequency`, `id`, `pay_date`, `record_date`, `ticker` |
| Corporate Actions | `GET /stocks/v1/dividends` | Callable | `id`, `ticker`, `record_date`, `pay_date`, `declaration_date`, `ex_dividend_date`, `frequency`, `cash_amount`, `currency`, `distribution_type`, `historical_adjustment_factor`, `split_adjusted_cash_amount` |
| Corporate Actions | `GET /vX/reference/tickers/{id}/events` | Callable | `name`, `composite_figi`, `cik`, `events` |
| Fundamentals | `GET /stocks/financials/v1/balance-sheets` | Not authorized | `message` |
| Fundamentals | `GET /stocks/financials/v1/cash-flow-statements` | Not authorized | `message` |
| Fundamentals | `GET /stocks/financials/v1/income-statements` | Not authorized | `message` |
| Fundamentals | `GET /stocks/financials/v1/ratios` | Not authorized | `message` |
| Fundamentals | `GET /stocks/v1/short-interest` | Callable | `settlement_date`, `ticker`, `short_interest`, `avg_daily_volume`, `days_to_cover` |
| Fundamentals | `GET /stocks/v1/short-volume` | Callable | `ticker`, `date`, `total_volume`, `short_volume`, `exempt_volume`, `non_exempt_volume`, `short_volume_ratio`, venue short-volume fields |
| Fundamentals | `GET /stocks/vX/float` | Callable | `ticker`, `free_float`, `effective_date`, `free_float_percent` |
| Filings | `GET /stocks/filings/vX/index` | Callable | `cik`, `issuer_name`, `form_type`, `filing_date`, `filing_url`, `accession_number`, `ticker` |
| Filings | `GET /stocks/filings/10-K/vX/sections` | Callable | `cik`, `ticker`, `section`, `filing_date`, `period_end`, `text`, `filing_url` |
| Filings | `GET /stocks/filings/8-K/vX/text` | Callable | `cik`, `ticker`, `accession_number`, `form_type`, `filing_date`, `items_text`, `filing_url` |
| Filings | `GET /stocks/filings/vX/13-F` | Callable | `filer_cik`, `accession_number`, `form_type`, `filing_date`, `period`, `issuer_name`, `title_of_class`, `market_value`, `shares_or_principal_amount`, `cusip`, voting fields |
| Filings | `GET /stocks/filings/vX/risk-factors` | Callable | `cik`, `ticker`, `primary_category`, `secondary_category`, `tertiary_category`, `filing_date`, `supporting_text` |
| Filings | `GET /stocks/taxonomies/vX/risk-factors` | Callable | `primary_category`, `secondary_category`, `tertiary_category`, `description`, `taxonomy` |
| Filings | `GET /stocks/filings/vX/form-3` | Callable | `tickers`, `issuer_cik`, `owner_cik`, `accession_number`, `form_type`, `filing_date`, `period_of_report`, `issuer_name`, `owner_name`, ownership-role fields |
| Filings | `GET /stocks/filings/vX/form-4` | Callable | `tickers`, `issuer_cik`, `owner_cik`, `accession_number`, `form_type`, `filing_date`, `period_of_report`, `issuer_name`, `owner_name`, transaction fields |
| News | `GET /v2/reference/news` | Callable | `id`, `publisher`, `title`, `author`, `published_utc`, `article_url`, `tickers`, `amp_url`, `image_url`, `description`, `keywords`, `insights` |

## Notes From Follow-up Probes

- AAPL Form 4 was observed through Massive back to at least 2006 in sampled queries.
- Berkshire 13-F filing headers were observed back to 1999, but sampled structured holdings fields became reliably present around 2013. Earlier filings may require SEC raw filing parsing or completeness checks.
- 13-F holdings identify securities primarily by CUSIP/issuer/class, so mapping to `security_id` requires `security_identifiers`.

## Non-contractual Items

The audit proved that some endpoints were callable, but current architecture deliberately does not ingest all of them:

- Ticker type dictionary and related-company sets are not core facts.
- IPO/listing events are not in the current ingestion scope.
- Massive SMA/EMA/MACD/RSI are reference values for future indicator QA, not fact-table rows.
- Massive snapshots/trades/quotes were not authorized for the audited keys.
- Massive financial statements and ratios were not authorized for the audited keys.
