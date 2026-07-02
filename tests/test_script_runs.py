"""七个 update_massive_* 脚本 run(args, source, db) 主流程的编排测试。

证券选择函数已由 test_select_us_securities 单独覆盖，这里统一打桩，
专注验证：source 调用 -> 行归一化 -> db 写入 -> watermark -> 退出码 的链路。
"""
from datetime import date, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock

import pandas as pd
import pytest
from sqlalchemy import MetaData, create_engine
from sqlalchemy.orm import sessionmaker

from data_models.models import Security

import scripts.update_massive_actions as actions
import scripts.update_massive_details as details
import scripts.update_massive_events as events
import scripts.update_massive_news as news
import scripts.update_massive_prices as prices
import scripts.update_massive_shares as shares
import scripts.update_massive_short_data as short_data
import scripts.update_grouped_daily as grouped_daily
import scripts.update_risk_free_rates as risk_free

END_DATE = date(2026, 6, 11)


def _exit_code(result) -> int:
    """run() 返回 int 或 (int, stats_dict)，统一取退出码。"""
    return result[0] if isinstance(result, tuple) else result


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
        result = details.run(args, source, db)
        assert _exit_code(result) == 0

        payload = db.upsert_security_info.call_args.args[0]
        assert payload["id"] == 1
        assert payload["name"] == "Apple"

    def test_no_pending_securities_short_circuits(self, monkeypatch):
        monkeypatch.setattr(details, "ensure_missing_symbols_exist", lambda db, src, syms: 0)
        monkeypatch.setattr(details, "get_securities_to_update", lambda db, args: [])
        source, db = Mock(), Mock()

        result = details.run(details.create_parser().parse_args([]), source, db)
        assert _exit_code(result) == 0
        db.upsert_security_info.assert_not_called()

    def test_process_error_returns_one(self, monkeypatch):
        monkeypatch.setattr(details, "ensure_missing_symbols_exist", lambda db, src, syms: 0)
        monkeypatch.setattr(details, "get_securities_to_update", lambda db, args: [_security()])
        source, db = Mock(), Mock()
        source.get_security_info.side_effect = RuntimeError("api down")

        result = details.run(details.create_parser().parse_args([]), source, db)
        assert _exit_code(result) == 1


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
        assert _exit_code(result) == 0

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
        assert _exit_code(result) == 0
        db.upsert_daily_prices.assert_not_called()
        # 落后的 metadata 被对齐
        db.update_security_price_latest_date.assert_called_once_with(1, END_DATE, is_full_run=False)


# ---------------------------------------------------------------------------
# grouped daily
# ---------------------------------------------------------------------------

GROUPED_AGGS = [
    {"T": "AAPL", "o": 1, "h": 2, "l": 0.5, "c": 1.5, "v": 100.0, "vw": 1.2, "n": 10},
    {"T": "MSFT", "o": 3, "h": 4, "l": 2.5, "c": 3.5, "v": 200, "vw": 3.2, "n": 20},
    {"T": "UNMAPPED", "o": 5, "h": 6, "l": 4.5, "c": 5.5, "v": 300},
]


def _grouped_db_with_existing(existing_rows):
    """远期 existing-only 路径的 db 桩：get_session 查询返回该日已存在的 (security_id,) 行。"""
    db = Mock()
    session = Mock()
    session.query.return_value.filter.return_value = existing_rows
    session_context = MagicMock()
    session_context.__enter__.return_value = session
    db.get_session.return_value = session_context
    return db


class TestGroupedDailyProcessDate:
    def test_recent_window_upserts_rows_without_existing_partition(self):
        source, db = Mock(), Mock()
        db.get_session.side_effect = AssertionError("近窗 upsert 不应查询既有行")
        db.upsert_daily_prices.return_value = 2
        source.get_grouped_daily_data.return_value = GROUPED_AGGS

        result = grouped_daily.process_date(
            date(2026, 6, 29), source, db, {"aapl": 1, "msft": 2},
            allow_insert=True,
        )

        assert result == ("2026-06-29", "SUCCESS", 2)
        rows = db.upsert_daily_prices.call_args.args[0]
        assert [row["security_id"] for row in rows] == [1, 2]
        assert rows[0]["date"] == date(2026, 6, 29)
        assert rows[0]["volume"] == 100
        db.ensure_security_price_latest_date_at_least.assert_called_once_with([1, 2], date(2026, 6, 29))

    def test_far_history_without_existing_rows_skips_insert(self):
        source = Mock()
        db = _grouped_db_with_existing([])

        result = grouped_daily.process_date(
            date(2025, 1, 6), source, db, {"aapl": 1, "msft": 2},
            allow_insert=False,
        )

        assert result == ("2025-01-06", "SKIPPED_NO_EXISTING_DATA", 0)
        source.get_grouped_daily_data.assert_not_called()
        db.upsert_daily_prices.assert_not_called()
        db.bulk_update_mappings.assert_not_called()
        db.ensure_security_price_latest_date_at_least.assert_not_called()

    def test_far_history_updates_existing_rows_only(self):
        source = Mock()
        db = _grouped_db_with_existing([(1,)])
        db.bulk_update_mappings.return_value = 1
        source.get_grouped_daily_data.return_value = GROUPED_AGGS

        result = grouped_daily.process_date(
            date(2025, 1, 6), source, db, {"aapl": 1, "msft": 2},
            allow_insert=False,
        )

        assert result == ("2025-01-06", "SUCCESS", 1)
        db.upsert_daily_prices.assert_not_called()
        rows = db.bulk_update_mappings.call_args.args[1]
        assert [row["security_id"] for row in rows] == [1]  # msft 无既有行，不得 INSERT
        db.ensure_security_price_latest_date_at_least.assert_called_once_with([1], date(2025, 1, 6))

    def test_null_watermark_security_not_stamped(self):
        source, db = Mock(), Mock()
        db.upsert_daily_prices.return_value = 2
        source.get_grouped_daily_data.return_value = GROUPED_AGGS

        result = grouped_daily.process_date(
            date(2026, 6, 29), source, db, {"aapl": 1, "msft": 2},
            allow_insert=True, skip_stamp_ids={1},
        )

        assert result == ("2026-06-29", "SUCCESS", 2)
        # NULL 水位的 1 不盖戳，保住 update_massive_prices 的自动全量回填入口
        db.ensure_security_price_latest_date_at_least.assert_called_once_with([2], date(2026, 6, 29))

    def test_all_null_watermark_skips_stamping_entirely(self):
        source, db = Mock(), Mock()
        db.upsert_daily_prices.return_value = 2
        source.get_grouped_daily_data.return_value = GROUPED_AGGS

        grouped_daily.process_date(
            date(2026, 6, 29), source, db, {"aapl": 1, "msft": 2},
            allow_insert=True, skip_stamp_ids={1, 2},
        )

        db.ensure_security_price_latest_date_at_least.assert_not_called()


class TestGroupedDailySymbolMap:
    @pytest.fixture()
    def session(self):
        engine = create_engine("sqlite:///:memory:")
        # PG 上 symbol 唯一索引是 partial（仅 active 行）；sqlite 不识别 postgresql_where
        # 会退化成全表 unique，插不进 active+inactive 同 symbol，故去掉索引建表
        table = Security.__table__.to_metadata(MetaData())
        table.indexes.clear()
        table.create(engine)
        session = sessionmaker(bind=engine)()
        yield session
        session.close()

    def _add(self, session, id_, symbol, active, latest=None):
        session.add(Security(
            id=id_, symbol=symbol, current_symbol=symbol, market="US",
            type="CS", is_active=active, full_refresh_interval=30,
            price_data_latest_date=latest,
        ))

    def test_active_symbol_wins_over_inactive(self, session):
        self._add(session, 1, "aapl", True)
        self._add(session, 2, "aapl", False)  # 回收/退市旧身份占同一 symbol
        session.commit()

        assert grouped_daily.load_symbol_to_id_map(session) == {"aapl": 1}

    def test_duplicate_active_symbol_dropped_entirely(self, session):
        self._add(session, 1, "dup", True)
        self._add(session, 2, "DUP", True)  # lowercase 后碰撞：整体剔除，不 last-wins
        self._add(session, 3, "msft", True)
        session.commit()

        assert grouped_daily.load_symbol_to_id_map(session) == {"msft": 3}

    def test_null_watermark_ids_loaded(self, session):
        self._add(session, 1, "aapl", True, latest=date(2026, 6, 29))
        self._add(session, 2, "newipo", True, latest=None)
        session.commit()

        assert grouped_daily.load_null_watermark_ids(session) == {2}


class TestGroupedDailyMain:
    LAST_COMPLETED = date(2026, 6, 30)

    def _stub_runtime(self, monkeypatch, process_stub):
        monkeypatch.setattr(grouped_daily, "setup_logging", lambda: None)
        monkeypatch.setattr(grouped_daily, "enforce_us_market", lambda market: None)
        monkeypatch.setattr(grouped_daily, "get_massive_api_keys", lambda: ["key"])
        monkeypatch.setattr(grouped_daily, "KeyRateLimiter", lambda *args, **kwargs: object())
        monkeypatch.setattr(grouped_daily, "MassiveSource", lambda rate_limiter: Mock())
        monkeypatch.setattr(grouped_daily, "get_last_completed_trading_date", lambda market: self.LAST_COMPLETED)
        # 简化为按自然日回移：近窗下限 = 2026-06-21
        monkeypatch.setattr(
            grouped_daily, "shift_trading_date",
            lambda market, session_date, sessions: session_date + timedelta(days=sessions),
        )
        monkeypatch.setattr(grouped_daily, "load_symbol_to_id_map", lambda session: {"aapl": 1})
        monkeypatch.setattr(grouped_daily, "load_null_watermark_ids", lambda session: set())

        db = Mock()
        db.get_session.return_value = MagicMock()
        monkeypatch.setattr(grouped_daily, "DatabaseManager", lambda: db)
        monkeypatch.setattr(grouped_daily, "process_date", process_stub)
        return db

    def test_main_returns_one_when_a_date_fails(self, monkeypatch):
        db = self._stub_runtime(
            monkeypatch,
            lambda target_date, source, db_manager, symbol_to_id_map, **kwargs: (target_date.isoformat(), "ERROR", 0),
        )

        result = grouped_daily.main(["--start-date", "2026-06-29", "--end-date", "2026-06-29"])

        assert result == 1
        db.close.assert_called_once()

    def test_main_clamps_end_date_and_splits_upsert_window(self, monkeypatch):
        calls: dict[date, bool] = {}

        def process_stub(target_date, source, db_manager, symbol_to_id_map, *, allow_insert, skip_stamp_ids):
            calls[target_date] = allow_insert
            return target_date.isoformat(), "SUCCESS", 1

        self._stub_runtime(monkeypatch, process_stub)

        result = grouped_daily.main(["--start-date", "2026-06-20", "--end-date", "2026-07-05"])

        assert result == 0
        # 超出最近已完成交易日（06-30）的部分被钳掉
        assert max(calls) == self.LAST_COMPLETED
        assert min(calls) == date(2026, 6, 20)
        # 近窗下限 06-21：更早的日期退回 existing-only，不允许 INSERT
        assert calls[date(2026, 6, 20)] is False
        assert calls[date(2026, 6, 21)] is True
        assert calls[self.LAST_COMPLETED] is True


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

        result = actions.run(actions.create_parser().parse_args([]), source, db)
        assert _exit_code(result) == 0

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

        result = actions.run(actions.create_parser().parse_args([]), source, db)
        assert _exit_code(result) == 1


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

        result = events.run(events.create_parser().parse_args([]), source, db)
        assert _exit_code(result) == 0

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

        result = events.run(events.create_parser().parse_args([]), source, db)
        assert _exit_code(result) == 1


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

        result = short_data.run(short_data.create_parser().parse_args([]), source, db)
        assert _exit_code(result) == 0
        written = db.upsert_short_interests.call_args.args[0]
        assert written[0]["security_id"] == 1 and written[0]["source"] == "MASSIVE"
        db.update_security_timestamps.assert_called_once_with([1], "short_data_last_updated_at")

    def test_batch_fatal_error_returns_one(self, monkeypatch):
        monkeypatch.setattr(short_data, "get_last_completed_trading_date", lambda market: END_DATE)
        monkeypatch.setattr(short_data, "get_securities_to_update", lambda db, args, end: [_security()])
        source, db = Mock(), Mock()
        db.get_security_short_max_dates.side_effect = RuntimeError("db down")

        result = short_data.run(short_data.create_parser().parse_args([]), source, db)
        assert _exit_code(result) == 1


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

        assert _exit_code(news.run(news.create_parser().parse_args([]), source, db)) == 0
        assert db.upsert_news_articles.call_args.kwargs["symbol_to_id"] == {"aapl": 1}
        db.update_security_timestamps.assert_called_once_with([1], "news_last_updated_at")

    def test_batch_failure_returns_one(self, monkeypatch):
        monkeypatch.setattr(news, "get_securities_to_update", lambda db, args: [_security()])
        source, db = Mock(), Mock()
        source.get_news.side_effect = RuntimeError("api down")

        assert _exit_code(news.run(news.create_parser().parse_args([]), source, db)) == 1


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

        assert _exit_code(shares.run(shares.create_parser().parse_args([]), source, db)) == 0

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

        assert _exit_code(shares.run(shares.create_parser().parse_args([]), source, db)) == 0
        share_rows = db.upsert_historical_shares.call_args.args[0]
        assert share_rows[0]["float_shares"] is None

    def test_symbol_error_returns_one_for_chunk_retry(self, monkeypatch):
        monkeypatch.setattr(shares, "get_last_completed_trading_date", lambda market: END_DATE)
        monkeypatch.setattr(shares, "get_securities_to_process", lambda db, args, end: [_security()])
        source, db = Mock(), Mock()
        source.get_ticker_overview.side_effect = RuntimeError("api down")
        source.get_float_batch.return_value = []

        assert _exit_code(shares.run(shares.create_parser().parse_args([]), source, db)) == 1
