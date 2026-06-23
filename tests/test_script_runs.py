"""七个 update_massive_* 脚本 run(args, source, db) 主流程的编排测试。

证券选择函数已由 test_select_us_securities 单独覆盖，这里统一打桩，
专注验证：source 调用 -> 行归一化 -> db 写入 -> watermark -> 退出码 的链路。
"""
from datetime import date
from types import SimpleNamespace
from unittest.mock import Mock

import pandas as pd
import pytest

import scripts.update_massive_actions as actions
import scripts.update_massive_details as details
import scripts.update_massive_events as events
import scripts.update_massive_news as news
import scripts.update_massive_prices as prices
import scripts.update_massive_shares as shares
import scripts.update_massive_short_data as short_data
import scripts.update_risk_free_rates as risk_free

END_DATE = date(2026, 6, 11)


def _security(**extra):
    defaults = dict(
        id=1,
        symbol="aapl",
        currency="USD",
        exchange="XNAS",
        price_data_latest_date=None,
        info_last_updated_at=None,
        actions_last_updated_at=None,
        list_date=None,
    )
    defaults.update(extra)
    return SimpleNamespace(**defaults)


# ---------------------------------------------------------------------------
# details
# ---------------------------------------------------------------------------

class TestDetailsRun:
    def test_happy_path_upserts_payload_with_id(self, monkeypatch):
        sec = _security()
        monkeypatch.setattr(details, "ensure_missing_symbols_exist", lambda db, src, syms: 0)
        monkeypatch.setattr(details, "get_securities_to_update", lambda db, args: [sec])
        source, db = Mock(), Mock()
        source.get_security_info.return_value = {"name": "Apple", "type": "CS"}

        args = details.create_parser().parse_args(["aapl"])
        assert details.run(args, source, db) == 0

        payload = db.upsert_security_info.call_args.args[0]
        assert payload["id"] == 1
        assert payload["name"] == "Apple"

    def test_no_pending_securities_short_circuits(self, monkeypatch):
        monkeypatch.setattr(details, "ensure_missing_symbols_exist", lambda db, src, syms: 0)
        monkeypatch.setattr(details, "get_securities_to_update", lambda db, args: [])
        source, db = Mock(), Mock()

        assert details.run(details.create_parser().parse_args([]), source, db) == 0
        db.upsert_security_info.assert_not_called()

    def test_process_error_returns_one(self, monkeypatch):
        monkeypatch.setattr(details, "ensure_missing_symbols_exist", lambda db, src, syms: 0)
        monkeypatch.setattr(details, "get_securities_to_update", lambda db, args: [_security()])
        source, db = Mock(), Mock()
        source.get_security_info.side_effect = RuntimeError("api down")

        assert details.run(details.create_parser().parse_args([]), source, db) == 1


# ---------------------------------------------------------------------------
# prices
# ---------------------------------------------------------------------------

class TestPricesRun:
    def _frame(self):
        return pd.DataFrame(
            {
                "Open": [1.0], "High": [2.0], "Low": [0.5], "Close": [1.5],
                "Volume": [100.0], "vwap": [1.2], "trade_count": [10.0], "otc": [None],
            },
            index=[date(2026, 6, 10)],
        )

    def test_happy_path_writes_rows_and_metadata(self, monkeypatch):
        sec = _security()
        monkeypatch.setattr(prices, "get_last_completed_trading_date", lambda market: END_DATE)
        monkeypatch.setattr(prices, "get_securities_to_update", lambda db, args, end: [sec])
        source, db = Mock(), Mock()
        source.get_historical_data.return_value = self._frame()
        db.get_security_price_max_date.return_value = date(2026, 6, 10)

        result = prices.run(prices.create_parser().parse_args([]), source, db)
        exit_code = result[0] if isinstance(result, tuple) else result
        assert exit_code == 0

        rows = db.upsert_daily_prices.call_args.args[0]
        assert rows[0]["security_id"] == 1
        assert rows[0]["volume"] == 100 and isinstance(rows[0]["volume"], int)
        db.update_security_price_latest_date.assert_called_once_with(1, date(2026, 6, 10), is_full_run=True)

    def test_empty_frame_syncs_metadata_from_existing_rows(self, monkeypatch):
        sec = _security()
        monkeypatch.setattr(prices, "get_last_completed_trading_date", lambda market: END_DATE)
        monkeypatch.setattr(prices, "get_securities_to_update", lambda db, args, end: [sec])
        source, db = Mock(), Mock()
        source.get_historical_data.return_value = pd.DataFrame()
        db.get_security_price_max_date.return_value = END_DATE  # 库里其实已是最新

        result = prices.run(prices.create_parser().parse_args([]), source, db)
        exit_code = result[0] if isinstance(result, tuple) else result
        assert exit_code == 0
        db.upsert_daily_prices.assert_not_called()
        # 落后的 metadata 被对齐
        db.update_security_price_latest_date.assert_called_once_with(1, END_DATE, is_full_run=False)


# ---------------------------------------------------------------------------
# actions
# ---------------------------------------------------------------------------

class TestActionsRun:
    def test_happy_path_fills_currency_and_touches_watermark(self, monkeypatch):
        sec = _security(currency=None)  # 触发 USD 兜底
        monkeypatch.setattr(actions, "get_last_completed_trading_date", lambda market: END_DATE)
        monkeypatch.setattr(actions, "get_securities_to_update", lambda db, args: [sec])
        source, db = Mock(), Mock()
        source.get_dividends_batch.return_value = [
            {
                "ticker": "aapl", "ex_dividend_date": date(2026, 5, 11),
                "cash_amount": "0.27", "currency": None, "source_event_id": "d1",
                "historical_adjustment_factor": "0.999",
            }
        ]
        source.get_splits_batch.return_value = []
        db.upsert_dividends.return_value = 1
        db.upsert_splits.return_value = 0
        db.upsert_vendor_adjustment_factors.return_value = 1

        assert actions.run(actions.create_parser().parse_args([]), source, db) == 0

        dividends = db.upsert_dividends.call_args.args[1]
        assert dividends[0]["currency"] == "USD"
        assert "ticker" not in dividends[0]
        factor_rows = db.upsert_vendor_adjustment_factors.call_args.args[0]
        assert factor_rows[0]["factor_key"] == "dividend:d1"
        db.update_security_timestamp.assert_called_once_with(1, "actions_last_updated_at")

    def test_db_error_counts_and_run_returns_one(self, monkeypatch):
        monkeypatch.setattr(actions, "get_last_completed_trading_date", lambda market: END_DATE)
        monkeypatch.setattr(actions, "get_securities_to_update", lambda db, args: [_security()])
        source, db = Mock(), Mock()
        source.get_dividends_batch.return_value = []
        source.get_splits_batch.return_value = []
        db.upsert_vendor_adjustment_factors.side_effect = RuntimeError("db down")

        assert actions.run(actions.create_parser().parse_args([]), source, db) == 1


# ---------------------------------------------------------------------------
# events
# ---------------------------------------------------------------------------

class TestEventsRun:
    def test_happy_path_normalizes_ticker_change_rows(self, monkeypatch):
        sec = _security()
        monkeypatch.setattr(events, "get_securities_to_update", lambda db, args: [sec])
        source, db = Mock(), Mock()
        source.get_ticker_events.return_value = {
            "events": [
                {"type": "ticker_change", "date": "2020-01-01", "ticker_change": {"ticker": "AAPL_OLD"}},
                {"type": "ipo", "date": "2019-01-01"},  # 非 ticker_change 被忽略
            ]
        }
        db.upsert_symbol_history.return_value = 1

        assert events.run(events.create_parser().parse_args([]), source, db) == 0

        rows = db.upsert_symbol_history.call_args.args[0]
        assert rows == [
            {
                "security_id": 1, "symbol": "aapl_old", "exchange": "XNAS",
                "source": "MASSIVE", "source_event_id": "1:aapl_old:2020-01-01",
                "event_type": "ticker_change", "start_date": "2020-01-01",
            }
        ]
        db.update_security_timestamp.assert_called_once_with(1, "events_last_updated_at")

    def test_process_error_returns_one(self, monkeypatch):
        monkeypatch.setattr(events, "get_securities_to_update", lambda db, args: [_security()])
        source, db = Mock(), Mock()
        source.get_ticker_events.side_effect = RuntimeError("api down")

        assert events.run(events.create_parser().parse_args([]), source, db) == 1


# ---------------------------------------------------------------------------
# short data
# ---------------------------------------------------------------------------
class TestShortDataRun:
    def test_happy_path_returns_zero(self, monkeypatch):
        sec = _security()
        monkeypatch.setattr(short_data, "get_last_completed_trading_date", lambda market: END_DATE)
        monkeypatch.setattr(short_data, "get_securities_to_update", lambda db, args, end: [sec])
        source, db = Mock(), Mock()
        db.get_security_short_max_dates.return_value = {1: {"interest": None, "volume": None}}
        source.get_short_interest_batch.return_value = [
            {"ticker": "aapl", "settlement_date": date(2026, 6, 1), "short_interest": 10},
        ]
        source.get_short_volume_batch.return_value = []
        db.upsert_short_interests.return_value = 1

        assert short_data.run(short_data.create_parser().parse_args([]), source, db) == 0
        written = db.upsert_short_interests.call_args.args[0]
        assert written[0]["security_id"] == 1 and written[0]["source"] == "MASSIVE"
        db.update_security_timestamps.assert_called_once_with([1], "short_data_last_updated_at")

    def test_batch_fatal_error_returns_one(self, monkeypatch):
        monkeypatch.setattr(short_data, "get_last_completed_trading_date", lambda market: END_DATE)
        monkeypatch.setattr(short_data, "get_securities_to_update", lambda db, args, end: [_security()])
        source, db = Mock(), Mock()
        db.get_security_short_max_dates.side_effect = RuntimeError("db down")

        assert short_data.run(short_data.create_parser().parse_args([]), source, db) == 1


# ---------------------------------------------------------------------------
# news
# ---------------------------------------------------------------------------

class TestNewsRun:
    def test_happy_path_maps_symbols_and_touches_watermark(self, monkeypatch):
        sec = _security()
        monkeypatch.setattr(news, "get_securities_to_update", lambda db, args: [sec])
        source, db = Mock(), Mock()
        source.get_news.return_value = [{"source_article_id": "a1", "tickers": ["aapl"]}]
        db.upsert_news_articles.return_value = (1, 1)

        assert news.run(news.create_parser().parse_args([]), source, db) == 0
        assert db.upsert_news_articles.call_args.kwargs["symbol_to_id"] == {"aapl": 1}
        db.update_security_timestamps.assert_called_once_with([1], "news_last_updated_at")

    def test_batch_failure_returns_one(self, monkeypatch):
        monkeypatch.setattr(news, "get_securities_to_update", lambda db, args: [_security()])
        source, db = Mock(), Mock()
        source.get_news.side_effect = RuntimeError("api down")

        assert news.run(news.create_parser().parse_args([]), source, db) == 1


class TestRiskFreeRatesMain:
    def test_happy_path_fetches_and_upserts(self, monkeypatch):
        rows = [{"date": date(2026, 6, 5), "series_id": "DTB3", "rate_pct": "4.28"}]
        db = Mock()
        db.upsert_risk_free_rates.return_value = 1
        monkeypatch.setattr(risk_free, "fetch_fred_series", lambda series_id, since=None: rows)
        monkeypatch.setattr(risk_free, "DatabaseManager", lambda: db)

        assert risk_free.main(["--series-id", "DTB3", "--since", "2026-06-01"]) == 0

        db.upsert_risk_free_rates.assert_called_once_with(rows)
        db.close.assert_called_once()

    def test_fetch_error_returns_one(self, monkeypatch):
        monkeypatch.setattr(risk_free, "fetch_fred_series", lambda series_id, since=None: (_ for _ in ()).throw(RuntimeError("down")))

        assert risk_free.main([]) == 1

    def test_empty_fetch_returns_one(self, monkeypatch):
        monkeypatch.setattr(risk_free, "fetch_fred_series", lambda series_id, since=None: [])

        assert risk_free.main([]) == 1


# ---------------------------------------------------------------------------
# shares
# ---------------------------------------------------------------------------

class TestSharesRun:
    def test_happy_path_attaches_floats_and_writes_both_tables(self, monkeypatch):
        sec = _security()
        monkeypatch.setattr(shares, "get_last_completed_trading_date", lambda market: END_DATE)
        monkeypatch.setattr(shares, "get_securities_to_process", lambda db, args, end: [sec])
        source, db = Mock(), Mock()
        source.get_ticker_overview.return_value = {"share_class_shares_outstanding": 1000}
        source.get_float_batch.return_value = [
            {"ticker": "aapl", "effective_date": date(2026, 6, 1), "free_float": 800, "free_float_percent": 80},
        ]

        assert shares.run(shares.create_parser().parse_args([]), source, db) == 0

        share_rows = db.upsert_historical_shares.call_args.args[0]
        assert share_rows[0]["total_shares"] == 1000
        assert share_rows[0]["float_shares"] == 800  # effective_date <= filing_date 才能附加
        float_rows = db.upsert_historical_floats.call_args.args[0]
        assert float_rows[0]["security_id"] == 1
        db.update_security_timestamps.assert_called_once_with([1], "shares_last_updated_at")

    def test_future_float_not_attached_to_past_snapshot(self, monkeypatch):
        sec = _security()
        monkeypatch.setattr(shares, "get_last_completed_trading_date", lambda market: END_DATE)
        monkeypatch.setattr(shares, "get_securities_to_process", lambda db, args, end: [sec])
        source, db = Mock(), Mock()
        source.get_ticker_overview.return_value = {"share_class_shares_outstanding": 1000}
        # float 晚于 snapshot(end_date)，属于"未来数据"，不得回写
        source.get_float_batch.return_value = [
            {"ticker": "aapl", "effective_date": date(2026, 6, 12), "free_float": 800, "free_float_percent": 80},
        ]

        assert shares.run(shares.create_parser().parse_args([]), source, db) == 0
        share_rows = db.upsert_historical_shares.call_args.args[0]
        assert share_rows[0]["float_shares"] is None

    def test_symbol_error_returns_one_for_chunk_retry(self, monkeypatch):
        monkeypatch.setattr(shares, "get_last_completed_trading_date", lambda market: END_DATE)
        monkeypatch.setattr(shares, "get_securities_to_process", lambda db, args, end: [_security()])
        source, db = Mock(), Mock()
        source.get_ticker_overview.side_effect = RuntimeError("api down")
        source.get_float_batch.return_value = []

        assert shares.run(shares.create_parser().parse_args([]), source, db) == 1
