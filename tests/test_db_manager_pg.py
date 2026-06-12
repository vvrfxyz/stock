"""db_manager 写入层的 PostgreSQL 集成测试。

覆盖的是单元测试无法替代的方言级语义：
- ON CONFLICT 的冲突键与 protected 字段保护；
- 合成事件被真实 vendor 事件替换时的去重 DELETE；
- 序列同步（手工导库后 INSERT 不撞主键）；
- 各表 upsert 的"只更新提供字段 / NULL 不覆盖"语义。
"""
from datetime import date, datetime, timezone
from decimal import Decimal

import pytest
from sqlalchemy import text

from data_models.models import (
    CorporateAction,
    DailyPrice,
    HistoricalShare,
    NewsArticle,
    NewsArticleInsight,
    SecFundamentalFact,
    Security,
    SecurityIdentifier,
    ShortVolume,
)

pytestmark = pytest.mark.integration


def _insert_security(pg_db, security_id=1, symbol="aapl", **extra) -> int:
    row = {
        "id": security_id,
        "symbol": symbol,
        "current_symbol": symbol,
        "market": "US",
        "type": "CS",
        "is_active": True,
        "full_refresh_interval": 30,
        **extra,
    }
    with pg_db.get_session() as session:
        session.add(Security(**row))
        session.commit()
    return security_id


def _scalar(pg_db, sql, **params):
    with pg_db.engine.connect() as conn:
        return conn.execute(text(sql), params).scalar()


# ---------------------------------------------------------------------------
# securities
# ---------------------------------------------------------------------------

class TestUpsertSecurityInfo:
    def test_insert_then_update_preserves_protected_watermarks(self, pg_db):
        pg_db.upsert_security_info({"id": 1, "symbol": "aapl", "name": "Apple Inc.", "market": "US", "type": "CS"})

        with pg_db.engine.connect() as conn:
            conn.execute(text(
                "UPDATE securities SET price_data_latest_date = '2026-06-01', "
                "actions_last_updated_at = now() WHERE id = 1"
            ))
            conn.commit()

        # 第二次 upsert 不携带 watermark 字段——它们必须原样保留
        pg_db.upsert_security_info({"id": 1, "symbol": "aapl", "name": "Apple Inc. (new)", "market": "US"})

        assert _scalar(pg_db, "SELECT name FROM securities WHERE id=1") == "Apple Inc. (new)"
        assert _scalar(pg_db, "SELECT price_data_latest_date FROM securities WHERE id=1") == date(2026, 6, 1)
        assert _scalar(pg_db, "SELECT actions_last_updated_at FROM securities WHERE id=1") is not None

    def test_unknown_fields_are_dropped_not_raised(self, pg_db):
        pg_db.upsert_security_info({"id": 1, "symbol": "aapl", "market": "US", "bogus_field": "x"})
        assert _scalar(pg_db, "SELECT count(*) FROM securities") == 1

    def test_update_does_not_null_out_omitted_columns(self, pg_db):
        pg_db.upsert_security_info({"id": 1, "symbol": "aapl", "market": "US", "description": "long text"})
        pg_db.upsert_security_info({"id": 1, "symbol": "aapl", "market": "US"})
        assert _scalar(pg_db, "SELECT description FROM securities WHERE id=1") == "long text"

    def test_missing_id_raises(self, pg_db):
        with pytest.raises(ValueError):
            pg_db.upsert_security_info({"symbol": "aapl"})


class TestUpsertSecuritiesBySymbol:
    def test_heterogeneous_key_sets_insert_in_groups(self, pg_db):
        rows = [
            {"symbol": "aapl", "market": "US", "type": "CS", "name": "Apple"},
            {"symbol": "msft", "market": "US", "type": "CS"},  # 无 name —— 不同键集
        ]
        written = pg_db.upsert_securities_by_symbol(rows)
        assert written == 2
        assert _scalar(pg_db, "SELECT count(*) FROM securities") == 2

    def test_conflict_updates_only_provided_fields(self, pg_db):
        _insert_security(pg_db, 1, "aapl", name="Apple Inc.", description="keep me")
        pg_db.upsert_securities_by_symbol([{"symbol": "aapl", "market": "US", "name": "Apple (updated)"}])

        assert _scalar(pg_db, "SELECT name FROM securities WHERE symbol='aapl'") == "Apple (updated)"
        assert _scalar(pg_db, "SELECT description FROM securities WHERE symbol='aapl'") == "keep me"

    def test_does_not_touch_info_timestamp_by_default(self, pg_db):
        _insert_security(pg_db, 1, "aapl")
        with pg_db.engine.connect() as conn:
            conn.execute(text("UPDATE securities SET info_last_updated_at = '2020-01-01T00:00:00Z' WHERE id=1"))
            conn.commit()

        pg_db.upsert_securities_by_symbol([{"symbol": "aapl", "market": "US", "name": "x"}])
        stamp = _scalar(pg_db, "SELECT info_last_updated_at FROM securities WHERE id=1")
        assert stamp.year == 2020

        pg_db.upsert_securities_by_symbol([{"symbol": "aapl", "market": "US", "name": "y"}], touch_info_timestamp=True)
        stamp = _scalar(pg_db, "SELECT info_last_updated_at FROM securities WHERE id=1")
        assert stamp.year >= 2026

    def test_sequence_synced_after_manual_id_insert(self, pg_db):
        # 手工导库（显式 id=100）后序列落后；批量 upsert 必须自动追平，不撞 pkey
        _insert_security(pg_db, 100, "aapl")
        written = pg_db.upsert_securities_by_symbol([{"symbol": "msft", "market": "US", "type": "CS"}])
        assert written == 1
        new_id = _scalar(pg_db, "SELECT id FROM securities WHERE symbol='msft'")
        assert new_id > 100


class TestSecurityTimestamps:
    def test_update_security_timestamps_batch(self, pg_db):
        _insert_security(pg_db, 1, "aapl")
        _insert_security(pg_db, 2, "msft")
        updated = pg_db.update_security_timestamps([1, 2], "shares_last_updated_at")
        assert updated == 2
        assert _scalar(pg_db, "SELECT count(*) FROM securities WHERE shares_last_updated_at IS NOT NULL") == 2

    def test_rejects_unknown_field(self, pg_db):
        with pytest.raises(ValueError):
            pg_db.update_security_timestamp(1, "created_at")

    def test_price_latest_date_monotonic_advance(self, pg_db):
        _insert_security(pg_db, 1, "aapl", price_data_latest_date=date(2026, 6, 10))
        # 不允许倒退
        moved = pg_db.ensure_security_price_latest_date_at_least([1], date(2026, 6, 1))
        assert moved == 0
        # 允许前进
        moved = pg_db.ensure_security_price_latest_date_at_least([1], date(2026, 6, 11))
        assert moved == 1
        assert _scalar(pg_db, "SELECT price_data_latest_date FROM securities WHERE id=1") == date(2026, 6, 11)

    def test_update_price_latest_date_full_run_touches_full_timestamp(self, pg_db):
        _insert_security(pg_db, 1, "aapl")
        pg_db.update_security_price_latest_date(1, date(2026, 6, 11), is_full_run=False)
        assert _scalar(pg_db, "SELECT full_data_last_updated_at FROM securities WHERE id=1") is None
        pg_db.update_security_price_latest_date(1, date(2026, 6, 11), is_full_run=True)
        assert _scalar(pg_db, "SELECT full_data_last_updated_at FROM securities WHERE id=1") is not None


# ---------------------------------------------------------------------------
# corporate actions / adjustment factors
# ---------------------------------------------------------------------------

class TestCorporateActions:
    DIV = {
        "ex_dividend_date": date(2026, 5, 11),
        "cash_amount": Decimal("0.27"),
        "currency": "USD",
        "source_event_id": "ev-div-1",
    }

    def test_dividend_insert_and_idempotent_reupsert(self, pg_db):
        _insert_security(pg_db)
        assert pg_db.upsert_dividends(1, [dict(self.DIV)]) == 1
        pg_db.upsert_dividends(1, [dict(self.DIV)])
        assert _scalar(pg_db, "SELECT count(*) FROM corporate_actions") == 1

    def test_dividend_missing_required_fields_skipped(self, pg_db):
        _insert_security(pg_db)
        inserted = pg_db.upsert_dividends(1, [{"cash_amount": Decimal("1"), "currency": "USD"}])  # 无 ex_date
        assert inserted == 0

    def test_synthetic_dividend_replaced_by_real_vendor_event(self, pg_db):
        _insert_security(pg_db)
        # 第一轮：无 vendor event id -> 合成 id
        synthetic = {k: v for k, v in self.DIV.items() if k != "source_event_id"}
        pg_db.upsert_dividends(1, [synthetic])
        synthetic_id = _scalar(pg_db, "SELECT source_event_id FROM corporate_actions")
        assert synthetic_id.startswith("massive-dividend:")

        # 第二轮：vendor 返回了真实 event id —— 合成行必须被清理，只剩真实行
        pg_db.upsert_dividends(1, [dict(self.DIV)])
        assert _scalar(pg_db, "SELECT count(*) FROM corporate_actions") == 1
        assert _scalar(pg_db, "SELECT source_event_id FROM corporate_actions") == "ev-div-1"

    def test_synthetic_split_replaced_by_real_vendor_event(self, pg_db):
        _insert_security(pg_db)
        split = {"execution_date": date(2026, 6, 1), "split_from": Decimal("1"), "split_to": Decimal("2")}
        pg_db.upsert_splits(1, [split])
        pg_db.upsert_splits(1, [{**split, "source_event_id": "ev-split-1"}])
        assert _scalar(pg_db, "SELECT count(*) FROM corporate_actions") == 1
        assert _scalar(pg_db, "SELECT source_event_id FROM corporate_actions") == "ev-split-1"

    def test_synthetic_cleanup_keeps_different_amounts(self, pg_db):
        """金额不同的合成行不是重复，不能被误删。"""
        _insert_security(pg_db)
        pg_db.upsert_dividends(1, [{"ex_dividend_date": date(2026, 5, 11), "cash_amount": Decimal("0.50"), "currency": "USD"}])
        pg_db.upsert_dividends(1, [dict(self.DIV)])  # 真实事件, 金额 0.27
        assert _scalar(pg_db, "SELECT count(*) FROM corporate_actions") == 2


class TestAdjustmentFactors:
    def test_vendor_factor_upsert_by_factor_key(self, pg_db):
        _insert_security(pg_db)
        row = {
            "security_id": 1,
            "date": date(2026, 5, 11),
            "source": "MASSIVE",
            "factor_type": "historical_adjustment",
            "factor_key": "dividend:ev-1",
            "adjustment_factor": Decimal("0.999"),
            "as_of_date": date(2026, 6, 10),
        }
        assert pg_db.upsert_vendor_adjustment_factors([row]) == 1
        # 同 factor_key 不同值 -> 更新而非新增
        pg_db.upsert_vendor_adjustment_factors([{**row, "adjustment_factor": Decimal("0.998")}])
        assert _scalar(pg_db, "SELECT count(*) FROM vendor_adjustment_factors") == 1
        assert _scalar(pg_db, "SELECT adjustment_factor FROM vendor_adjustment_factors") == Decimal("0.998000000000")

    def test_replace_computed_factors_swaps_whole_version(self, pg_db):
        _insert_security(pg_db)

        def row(key, factor):
            return {
                "security_id": 1,
                "date": date(2026, 5, 11),
                "methodology_version": "raw_actions_v1",
                "factor_type": "split",
                "factor_key": key,
                "cumulative_factor": factor,
                "event_hash": "h" * 8,
            }

        pg_db.replace_computed_adjustment_factors(1, "raw_actions_v1", [row("a", Decimal("0.5")), row("b", Decimal("0.25"))])
        assert _scalar(pg_db, "SELECT count(*) FROM computed_adjustment_factors") == 2
        # 重建为 1 行：旧版本行必须整体消失
        pg_db.replace_computed_adjustment_factors(1, "raw_actions_v1", [row("c", Decimal("0.5"))])
        assert _scalar(pg_db, "SELECT count(*) FROM computed_adjustment_factors") == 1
        assert _scalar(pg_db, "SELECT factor_key FROM computed_adjustment_factors") == "c"

    def test_replace_with_empty_rows_clears_version(self, pg_db):
        _insert_security(pg_db)
        pg_db.replace_computed_adjustment_factors(1, "raw_actions_v1", [])
        assert _scalar(pg_db, "SELECT count(*) FROM computed_adjustment_factors") == 0


# ---------------------------------------------------------------------------
# market data
# ---------------------------------------------------------------------------

class TestDailyPrices:
    def test_upsert_overwrites_ohlcv_on_conflict(self, pg_db):
        _insert_security(pg_db)
        row = {"security_id": 1, "date": date(2026, 6, 10), "open": 1, "high": 2, "low": 1, "close": 2, "volume": 100}
        pg_db.upsert_daily_prices([row])
        pg_db.upsert_daily_prices([{**row, "close": 3, "volume": 200}])
        assert _scalar(pg_db, "SELECT count(*) FROM daily_prices") == 1
        assert _scalar(pg_db, "SELECT close FROM daily_prices") == Decimal("3.000000")

    def test_partial_row_does_not_null_other_columns(self, pg_db):
        """open_close_summary 只回填盘前/盘后字段时，不能把 OHLCV 抹掉。"""
        _insert_security(pg_db)
        pg_db.upsert_daily_prices([
            {"security_id": 1, "date": date(2026, 6, 10), "open": 1, "high": 2, "low": 1, "close": 2, "volume": 100}
        ])
        pg_db.upsert_daily_prices([
            {"security_id": 1, "date": date(2026, 6, 10), "pre_market": Decimal("1.5"), "after_hours": Decimal("2.5")}
        ])
        assert _scalar(pg_db, "SELECT close FROM daily_prices") == Decimal("2.000000")
        assert _scalar(pg_db, "SELECT pre_market FROM daily_prices") == Decimal("1.500000")

    def test_get_security_price_max_date(self, pg_db):
        _insert_security(pg_db)
        assert pg_db.get_security_price_max_date(1) is None
        pg_db.upsert_daily_prices([
            {"security_id": 1, "date": date(2026, 6, 9), "close": 1},
            {"security_id": 1, "date": date(2026, 6, 10), "close": 2},
        ])
        assert pg_db.get_security_price_max_date(1) == date(2026, 6, 10)


class TestHistoricalSharesAndFloats:
    def test_upsert_shares_conflict_updates_values(self, pg_db):
        _insert_security(pg_db)
        row = {
            "security_id": 1, "filing_date": date(2026, 3, 31), "period_end_date": date(2026, 3, 31),
            "total_shares": 1000, "source": "MASSIVE",
        }
        pg_db.upsert_historical_shares([row])
        pg_db.upsert_historical_shares([{**row, "total_shares": 1100, "float_shares": 900}])
        assert _scalar(pg_db, "SELECT count(*) FROM historical_shares") == 1
        assert _scalar(pg_db, "SELECT total_shares FROM historical_shares") == 1100
        assert _scalar(pg_db, "SELECT float_shares FROM historical_shares") == 900

    def test_upsert_shares_skips_incomplete_rows(self, pg_db):
        _insert_security(pg_db)
        written = pg_db.upsert_historical_shares([
            {"security_id": 1, "filing_date": date(2026, 3, 31), "source": "MASSIVE"},  # 缺 total_shares/period_end
        ])
        assert written == 0

    def test_upsert_floats_conflict_updates(self, pg_db):
        _insert_security(pg_db)
        row = {"security_id": 1, "effective_date": date(2026, 6, 1), "free_float": 800, "source": "MASSIVE"}
        pg_db.upsert_historical_floats([row])
        pg_db.upsert_historical_floats([{**row, "free_float": 850}])
        assert _scalar(pg_db, "SELECT count(*) FROM historical_floats") == 1
        assert _scalar(pg_db, "SELECT free_float FROM historical_floats") == 850


class TestShortData:
    def test_short_interest_upsert_and_max_dates(self, pg_db):
        _insert_security(pg_db, 1, "aapl")
        _insert_security(pg_db, 2, "msft")
        pg_db.upsert_short_interests([
            {"security_id": 1, "ticker": "aapl", "settlement_date": date(2026, 6, 1), "short_interest": 10, "source": "MASSIVE"},
        ])
        pg_db.upsert_short_volumes([
            {"security_id": 1, "ticker": "aapl", "date": date(2026, 6, 5), "short_volume": 5, "source": "MASSIVE"},
        ])
        result = pg_db.get_security_short_max_dates([1, 2])
        assert result[1] == {"interest": date(2026, 6, 1), "volume": date(2026, 6, 5)}
        assert result[2] == {"interest": None, "volume": None}

    def test_short_volume_conflict_updates_dynamic_columns(self, pg_db):
        _insert_security(pg_db)
        row = {"security_id": 1, "ticker": "aapl", "date": date(2026, 6, 5), "short_volume": 5, "source": "MASSIVE"}
        pg_db.upsert_short_volumes([row])
        pg_db.upsert_short_volumes([{**row, "short_volume": 7, "total_volume": 100}])
        assert _scalar(pg_db, "SELECT short_volume FROM short_volumes") == 7
        assert _scalar(pg_db, "SELECT total_volume FROM short_volumes") == 100


# ---------------------------------------------------------------------------
# reference data
# ---------------------------------------------------------------------------

class TestSymbolHistory:
    def test_upsert_idempotent_on_natural_key(self, pg_db):
        _insert_security(pg_db)
        row = {
            "security_id": 1, "symbol": "aapl_old", "source": "MASSIVE",
            "source_event_id": "1:aapl_old:2020-01-01", "event_type": "ticker_change",
            "start_date": date(2020, 1, 1),
        }
        pg_db.upsert_symbol_history([row])
        pg_db.upsert_symbol_history([{**row, "end_date": date(2021, 1, 1)}])
        assert _scalar(pg_db, "SELECT count(*) FROM security_symbol_history") == 1
        assert _scalar(pg_db, "SELECT end_date FROM security_symbol_history") == date(2021, 1, 1)


class TestSecurityIdentifiers:
    def test_null_start_date_rows_do_not_duplicate_across_runs(self, pg_db):
        """唯一约束含 start_date(NULL)，ON CONFLICT 永不触发——必须靠应用层去重。"""
        _insert_security(pg_db)
        row = {"security_id": 1, "id_type": "CIK", "id_value": "0000320193", "source": "SEC"}
        assert pg_db.insert_missing_security_identifiers([row]) == 1
        assert pg_db.insert_missing_security_identifiers([row]) == 0
        assert _scalar(pg_db, "SELECT count(*) FROM security_identifiers") == 1


class TestSecFilings:
    def test_dedup_within_batch_and_conflict_update(self, pg_db):
        row = {
            "source": "SEC", "accession_number": "0001-26-000001", "form_type": "10-K",
            "filing_date": date(2026, 2, 1), "cik": "0000320193",
        }
        # 批内重复（双重上市共用 CIK）只写一行
        assert pg_db.upsert_sec_filings([row, dict(row)]) == 1
        # 冲突时更新元数据
        pg_db.upsert_sec_filings([{**row, "issuer_name": "Apple Inc."}])
        assert _scalar(pg_db, "SELECT count(*) FROM sec_filings") == 1
        assert _scalar(pg_db, "SELECT issuer_name FROM sec_filings") == "Apple Inc."


class TestSecFundamentalFacts:
    def test_value_immutable_on_conflict_only_labels_refresh(self, pg_db):
        _insert_security(pg_db)
        row = {
            "cik": "0000320193", "taxonomy": "us-gaap", "concept": "Revenues", "unit": "USD",
            "period_start": date(2026, 1, 1), "period_end": date(2026, 3, 31),
            "accession_number": "0001-26-000001", "filed_date": date(2026, 5, 1),
            "value": Decimal("100"), "fiscal_year": 2026, "fiscal_period": "Q1",
        }
        pg_db.upsert_sec_fundamental_facts([row])
        # 同 accession 重写：value 不可变，fiscal/frame 标签可刷新
        pg_db.upsert_sec_fundamental_facts([{**row, "value": Decimal("999"), "frame": "CY2026Q1", "security_id": 1}])
        assert _scalar(pg_db, "SELECT count(*) FROM sec_fundamental_facts") == 1
        assert _scalar(pg_db, "SELECT value FROM sec_fundamental_facts") == Decimal("100.000000")
        assert _scalar(pg_db, "SELECT frame FROM sec_fundamental_facts") == "CY2026Q1"
        assert _scalar(pg_db, "SELECT security_id FROM sec_fundamental_facts") == 1

    def test_restatement_under_new_accession_is_kept_as_new_row(self, pg_db):
        row = {
            "cik": "0000320193", "taxonomy": "us-gaap", "concept": "Revenues", "unit": "USD",
            "period_start": date(2026, 1, 1), "period_end": date(2026, 3, 31),
            "accession_number": "0001-26-000001", "filed_date": date(2026, 5, 1),
            "value": Decimal("100"),
        }
        pg_db.upsert_sec_fundamental_facts([row])
        pg_db.upsert_sec_fundamental_facts([
            {**row, "accession_number": "0001-26-000002", "filed_date": date(2026, 6, 1), "value": Decimal("105")}
        ])
        assert _scalar(pg_db, "SELECT count(*) FROM sec_fundamental_facts") == 2


class TestNewsArticles:
    def _article(self, **extra):
        return {
            "source_article_id": "art-1",
            "published_utc": datetime(2026, 6, 10, 12, 0, tzinfo=timezone.utc),
            "title": "AAPL news",
            "insights": [
                {"ticker": "AAPL", "sentiment": "positive", "sentiment_reasoning": "strong results"},
            ],
            **extra,
        }

    def test_articles_and_insights_written(self, pg_db):
        _insert_security(pg_db)
        articles, insights = pg_db.upsert_news_articles([self._article()], symbol_to_id={"aapl": 1})
        assert (articles, insights) == (1, 1)
        assert _scalar(pg_db, "SELECT security_id FROM news_article_insights") == 1
        assert _scalar(pg_db, "SELECT ticker FROM news_article_insights") == "aapl"

    def test_unresolved_ticker_does_not_null_existing_security_id(self, pg_db):
        """第二批没带 symbol 映射时，security_id=None 不能覆盖已解析值。"""
        _insert_security(pg_db)
        pg_db.upsert_news_articles([self._article()], symbol_to_id={"aapl": 1})
        pg_db.upsert_news_articles([self._article(title="updated")], symbol_to_id={})
        assert _scalar(pg_db, "SELECT security_id FROM news_article_insights") == 1
        assert _scalar(pg_db, "SELECT title FROM news_articles") == "updated"

    def test_article_without_required_fields_skipped(self, pg_db):
        articles, insights = pg_db.upsert_news_articles([{"title": "no id"}])
        assert (articles, insights) == (0, 0)
