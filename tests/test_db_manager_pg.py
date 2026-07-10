"""db_manager 写入层的 PostgreSQL 集成测试。

覆盖的是单元测试无法替代的方言级语义：
- ON CONFLICT 的冲突键与 protected 字段保护；
- 合成事件被真实 vendor 事件替换时的去重 DELETE；
- 序列同步（手工导库后 INSERT 不撞主键）；
- 各表 upsert 的"只更新提供字段 / NULL 不覆盖"语义；
- SAVEPOINT 隔离与部分唯一索引的 NULL 语义。
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
    SecurityIdentityEvent,
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

    def test_symbol_conflict_with_different_identity_is_not_merged(self, pg_db):
        _insert_security(pg_db, 1, "abcd", name="Old Co", composite_figi="BBGOLD", cik="0000000001")

        written = pg_db.upsert_securities_by_symbol(
            [{"symbol": "abcd", "market": "US", "type": "CS", "name": "New Co", "composite_figi": "BBGNEW", "cik": "0000000002"}]
        )

        assert written == 0
        assert _scalar(pg_db, "SELECT name FROM securities WHERE symbol='abcd'") == "Old Co"
        assert _scalar(pg_db, "SELECT composite_figi FROM securities WHERE symbol='abcd'") == "BBGOLD"
        # 冲突同时落一条 QUARANTINE 审计事件
        assert _scalar(pg_db, "SELECT event_type FROM security_identity_events WHERE security_id=1") == "QUARANTINE"

    def test_identity_event_failure_does_not_poison_main_upsert(self, pg_db, monkeypatch):
        """事件写入在 SAVEPOINT 内失败：只回滚事件本身，主 upsert 照常提交。"""
        _insert_security(pg_db, 1, "abcd", name="Old Co", composite_figi="BBGOLD", cik="0000000001")

        original = pg_db._sync_model_id_sequence

        def broken_for_events(conn, model):
            if model is SecurityIdentityEvent:
                # 制造真实的 PG 服务端错误——不包 SAVEPOINT 时会毒化整个共享事务
                conn.execute(text("SELECT 1/0"))
            return original(conn, model)

        monkeypatch.setattr(pg_db, "_sync_model_id_sequence", broken_for_events)

        written = pg_db.upsert_securities_by_symbol([
            {"symbol": "abcd", "market": "US", "type": "CS", "composite_figi": "BBGNEW", "cik": "0000000002"},
            {"symbol": "wxyz", "market": "US", "type": "CS", "name": "Clean Co"},
        ])

        assert written == 1
        assert _scalar(pg_db, "SELECT name FROM securities WHERE symbol='wxyz'") == "Clean Co"
        assert _scalar(pg_db, "SELECT name FROM securities WHERE symbol='abcd'") == "Old Co"
        assert _scalar(pg_db, "SELECT count(*) FROM security_identity_events") == 0

    def test_batch_duplicate_symbols_keep_first_row(self, pg_db):
        """vendor 同批返回大小写变体（如 TPC/TpC）归一化后撞同一冲突键——
        保留首条并跳过后续，而非 CardinalityViolation 打挂整批。"""
        written = pg_db.upsert_securities_by_symbol([
            {"symbol": "tpc", "market": "US", "type": "CS", "name": "Tutor Perini", "composite_figi": "BBGTPC1"},
            {"symbol": "tpc", "market": "US", "type": "CS", "name": "AT&T preferred", "composite_figi": "BBGTPC2"},
            {"symbol": "msft", "market": "US", "type": "CS", "name": "Microsoft", "composite_figi": "BBGMSFT"},
        ])

        assert written == 2
        assert _scalar(pg_db, "SELECT count(*) FROM securities WHERE symbol='tpc'") == 1
        assert _scalar(pg_db, "SELECT name FROM securities WHERE symbol='tpc'") == "Tutor Perini"
        assert _scalar(pg_db, "SELECT name FROM securities WHERE symbol='msft'") == "Microsoft"

    def test_explicit_none_is_active_normalized_to_true(self, pg_db):
        """is_active 显式 None 时 setdefault 失效——NULL 行不受 is_active IS TRUE
        部分唯一索引约束，重复运行会反复插入；必须归一化为 True。"""
        row = {"symbol": "aapl", "market": "US", "type": "CS", "is_active": None}
        pg_db.upsert_securities_by_symbol([dict(row)])
        pg_db.upsert_securities_by_symbol([dict(row)])

        assert _scalar(pg_db, "SELECT count(*) FROM securities WHERE symbol='aapl'") == 1
        assert _scalar(pg_db, "SELECT is_active FROM securities WHERE symbol='aapl'") is True

    def test_inactive_symbol_can_be_reused_for_new_identity(self, pg_db):
        _insert_security(pg_db, 1, "abcd", name="Old Co", composite_figi="BBGOLD", cik="0000000001", is_active=False)

        written = pg_db.upsert_securities_by_symbol(
            [{"symbol": "abcd", "market": "US", "type": "CS", "name": "New Co", "composite_figi": "BBGNEW", "cik": "0000000002"}]
        )

        assert written == 1
        assert _scalar(pg_db, "SELECT count(*) FROM securities WHERE symbol='abcd'") == 2
        assert _scalar(pg_db, "SELECT name FROM securities WHERE symbol='abcd' AND is_active IS TRUE") == "New Co"

    def test_sequence_synced_after_manual_id_insert(self, pg_db):
        # 手工导库（显式 id=100）后序列落后；批量 upsert 必须自动追平，不撞 pkey
        _insert_security(pg_db, 100, "aapl")
        written = pg_db.upsert_securities_by_symbol([{"symbol": "msft", "market": "US", "type": "CS"}])
        assert written == 1
        new_id = _scalar(pg_db, "SELECT id FROM securities WHERE symbol='msft'")
        assert new_id > 100


class TestInsertBackfilledSecurities:
    def test_forces_inactive_and_keeps_recycled_symbols_separate(self, pg_db):
        _insert_security(pg_db, 1, "same", is_active=True)
        inserted = pg_db.insert_backfilled_securities([
            {"symbol": "same", "market": "US", "type": "CS",
             "delist_date": date(2010, 1, 4), "is_active": True},
            {"symbol": "same", "market": "US", "type": "CS",
             "delist_date": date(2020, 1, 4)},
            {"symbol": "missing-date", "market": "US", "type": "CS"},
        ])

        assert len(inserted) == 2
        assert _scalar(pg_db, "SELECT count(*) FROM securities WHERE symbol='same'") == 3
        assert _scalar(pg_db, "SELECT count(*) FROM securities WHERE symbol='same' AND is_active") == 1

    def test_sequence_syncs_after_manual_id(self, pg_db):
        _insert_security(pg_db, 100, "active")
        inserted = pg_db.insert_backfilled_securities([
            {"symbol": "dead", "market": "US", "type": "CS", "delist_date": date(2010, 1, 4)}
        ])
        assert inserted[0][0] > 100


class TestSecurityIdentityChanges:
    def test_rename_updates_symbol_and_writes_history(self, pg_db):
        _insert_security(pg_db, 1, "fb", composite_figi="BBG000MM2P62", exchange="XNAS")

        pg_db.rename_security(1, old_symbol="fb", new_symbol="meta", exchange="XNAS")

        assert _scalar(pg_db, "SELECT symbol FROM securities WHERE id=1") == "meta"
        assert _scalar(pg_db, "SELECT current_symbol FROM securities WHERE id=1") == "meta"
        assert _scalar(pg_db, "SELECT count(*) FROM security_symbol_history WHERE security_id=1 AND symbol='fb'") == 1

    def test_rename_closes_old_interval_and_opens_new_one(self, pg_db):
        """区间语义：old_symbol 闭合（end_date=今天），new_symbol 开启（start_date=今天，end_date NULL）。"""
        _insert_security(pg_db, 1, "fb", exchange="XNAS")

        pg_db.rename_security(1, old_symbol="fb", new_symbol="meta", exchange="XNAS")

        assert _scalar(
            pg_db, "SELECT end_date FROM security_symbol_history WHERE security_id=1 AND symbol='fb'"
        ) == date.today()
        assert _scalar(
            pg_db, "SELECT start_date FROM security_symbol_history WHERE security_id=1 AND symbol='meta'"
        ) == date.today()
        assert _scalar(
            pg_db, "SELECT end_date FROM security_symbol_history WHERE security_id=1 AND symbol='meta'"
        ) is None

    def test_rename_closes_existing_open_interval_preserving_start_date(self, pg_db):
        """old_symbol 已有开区间行（update_massive_events 写入口径）：闭合它而非另插一行。"""
        _insert_security(pg_db, 1, "fb", exchange="XNAS")
        pg_db.upsert_symbol_history([{
            "security_id": 1, "symbol": "fb", "source": "MASSIVE",
            "event_type": "ticker_change", "start_date": date(2012, 5, 18),
        }])

        pg_db.rename_security(1, old_symbol="fb", new_symbol="meta", exchange="XNAS")

        assert _scalar(pg_db, "SELECT count(*) FROM security_symbol_history WHERE security_id=1 AND symbol='fb'") == 1
        assert _scalar(
            pg_db, "SELECT start_date FROM security_symbol_history WHERE security_id=1 AND symbol='fb'"
        ) == date(2012, 5, 18)
        assert _scalar(
            pg_db, "SELECT end_date FROM security_symbol_history WHERE security_id=1 AND symbol='fb'"
        ) == date.today()

    def test_rename_is_idempotent_on_history(self, pg_db):
        _insert_security(pg_db, 1, "fb", exchange="XNAS")

        pg_db.rename_security(1, old_symbol="fb", new_symbol="meta", exchange="XNAS")
        pg_db.rename_security(1, old_symbol="fb", new_symbol="meta", exchange="XNAS")

        assert _scalar(pg_db, "SELECT count(*) FROM security_symbol_history WHERE security_id=1 AND symbol='fb'") == 1
        assert _scalar(pg_db, "SELECT count(*) FROM security_symbol_history WHERE security_id=1 AND symbol='meta'") == 1
        # meta 的区间保持打开
        assert _scalar(
            pg_db, "SELECT end_date FROM security_symbol_history WHERE security_id=1 AND symbol='meta'"
        ) is None

    def test_insert_identity_events(self, pg_db):
        _insert_security(pg_db, 1, "meta")

        count = pg_db.insert_identity_events([
            {"security_id": 1, "event_type": "RENAME", "old_symbol": "fb", "new_symbol": "meta",
             "resolution_source": "AUTO", "confidence": "HIGH"},
            {"security_id": 1, "event_type": "NEW_LISTING", "new_symbol": "meta",
             "resolution_source": "AUTO", "confidence": "HIGH"},
        ])

        assert count == 2
        assert _scalar(pg_db, "SELECT count(*) FROM security_identity_events WHERE security_id=1") == 2
        assert _scalar(pg_db, "SELECT event_type FROM security_identity_events WHERE old_symbol='fb'") == "RENAME"

    def test_insert_identity_events_skips_invalid_rows(self, pg_db):
        count = pg_db.insert_identity_events([
            {"security_id": None, "event_type": "RENAME"},
            {"security_id": 1, "event_type": None},
            {},
        ])
        assert count == 0

    def test_rename_rejects_symbol_already_active_on_another_security(self, pg_db):
        _insert_security(pg_db, 1, "fb", composite_figi="BBG000MM2P62")
        _insert_security(pg_db, 2, "meta", composite_figi="BBG000OTHER")

        with pytest.raises(ValueError, match="new_symbol=meta.*占用"):
            pg_db.rename_security(1, old_symbol="fb", new_symbol="meta")

        # fb should remain unchanged
        assert _scalar(pg_db, "SELECT symbol FROM securities WHERE id=1") == "fb"

    def test_rename_allows_symbol_if_only_inactive_row_holds_it(self, pg_db):
        _insert_security(pg_db, 1, "fb", composite_figi="BBG000MM2P62")
        _insert_security(pg_db, 2, "meta", composite_figi="BBG000OTHER", is_active=False)

        pg_db.rename_security(1, old_symbol="fb", new_symbol="meta")
        assert _scalar(pg_db, "SELECT symbol FROM securities WHERE id=1") == "meta"


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

    def test_dividend_batch_duplicate_source_event_last_row_wins(self, pg_db):
        _insert_security(pg_db)
        first = dict(self.DIV)
        second = {**self.DIV, "cash_amount": Decimal("0.30")}
        pg_db.upsert_dividends(1, [first, second])
        assert _scalar(pg_db, "SELECT count(*) FROM corporate_actions") == 1
        assert _scalar(pg_db, "SELECT cash_amount FROM corporate_actions") == Decimal("0.3000000000")

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

    def test_synthetic_dividend_replaced_by_real_vendor_event_even_if_amount_revised(self, pg_db):
        """vendor 补发真实 ID 时金额可能修订；单笔 synthetic+real 仍视为同一经济事件。"""
        _insert_security(pg_db)
        pg_db.upsert_dividends(1, [{"ex_dividend_date": date(2026, 5, 11), "cash_amount": Decimal("0.50"), "currency": "USD"}])
        pg_db.upsert_dividends(1, [dict(self.DIV)])  # 真实事件, 金额修订为 0.27
        assert _scalar(pg_db, "SELECT count(*) FROM corporate_actions") == 1
        assert _scalar(pg_db, "SELECT source_event_id FROM corporate_actions") == "ev-div-1"
        assert _scalar(pg_db, "SELECT cash_amount FROM corporate_actions") == Decimal("0.2700000000")


class TestDelistingEvents:
    def _row(self, **extra):
        return {
            "security_id": 1,
            "delist_date": date(2026, 5, 29),
            "reason_code": "UNKNOWN",
            "reason_confidence": "LOW",
            "final_price": Decimal("9.98"),
            "final_price_date": date(2026, 5, 28),
            "source": "PRICE_INFERRED",
            "evidence": "terminal price stable near integer",
            **extra,
        }

    def test_insert_then_conflict_updates_payload_preserving_created_at(self, pg_db):
        _insert_security(pg_db)
        assert pg_db.upsert_delisting_events([self._row()]) == 1
        created = _scalar(pg_db, "SELECT created_at FROM delisting_events")

        # 分类器重跑证据升级：同 (security_id, delist_date) 原位覆盖
        pg_db.upsert_delisting_events([self._row(
            reason_code="ACQUISITION_CASH", reason_confidence="HIGH",
            final_price=Decimal("10.00"), source="8K",
            acquirer_name="Acme Corp", consideration_cash=Decimal("10.00"),
            delisting_return=Decimal("0.002"),
        )])

        assert _scalar(pg_db, "SELECT count(*) FROM delisting_events") == 1
        assert _scalar(pg_db, "SELECT reason_code FROM delisting_events") == "ACQUISITION_CASH"
        assert _scalar(pg_db, "SELECT final_price FROM delisting_events") == Decimal("10.000000")
        assert _scalar(pg_db, "SELECT acquirer_name FROM delisting_events") == "Acme Corp"
        assert _scalar(pg_db, "SELECT delisting_return FROM delisting_events") == Decimal("0.00200000")
        assert _scalar(pg_db, "SELECT created_at FROM delisting_events") == created

    def test_full_rebuild_clears_omitted_fields_to_null(self, pg_db):
        """幂等全量重建语义：证据撤销后重跑，未再给出的字段清成 NULL（不残留旧证据）。"""
        _insert_security(pg_db)
        pg_db.upsert_delisting_events([self._row(acquirer_name="Acme Corp")])
        pg_db.upsert_delisting_events([{
            "security_id": 1, "delist_date": date(2026, 5, 29), "reason_code": "UNKNOWN",
        }])
        assert _scalar(pg_db, "SELECT count(*) FROM delisting_events") == 1
        assert _scalar(pg_db, "SELECT reason_code FROM delisting_events") == "UNKNOWN"
        assert _scalar(pg_db, "SELECT acquirer_name FROM delisting_events") is None
        assert _scalar(pg_db, "SELECT final_price FROM delisting_events") is None

    def test_distinct_delist_dates_create_separate_rows(self, pg_db):
        """同一证券多次退市（重新上市后再退）是不同结局，各存一行。"""
        _insert_security(pg_db)
        pg_db.upsert_delisting_events([
            self._row(),
            self._row(delist_date=date(2020, 3, 2), reason_code="EXCHANGE_DROP"),
        ])
        assert _scalar(pg_db, "SELECT count(*) FROM delisting_events") == 2

    def test_missing_required_keys_and_batch_duplicates(self, pg_db):
        _insert_security(pg_db)
        written = pg_db.upsert_delisting_events([
            {"security_id": 1},                       # 缺 delist_date
            {"delist_date": date(2026, 5, 29)},       # 缺 security_id
            self._row(reason_code="VOLUNTARY"),
            self._row(reason_code="MERGER"),           # 批内同键——后行胜出
        ])
        assert written == 1
        assert _scalar(pg_db, "SELECT count(*) FROM delisting_events") == 1
        assert _scalar(pg_db, "SELECT reason_code FROM delisting_events") == "MERGER"


class TestCompanies:
    def test_upsert_by_cik_insert_then_name_refresh(self, pg_db):
        assert pg_db.upsert_companies([{"cik": "0000320193", "name": "Apple Computer"}]) == 1
        created = _scalar(pg_db, "SELECT created_at FROM companies")

        pg_db.upsert_companies([{"cik": "0000320193", "name": "Apple Inc."}])
        assert _scalar(pg_db, "SELECT count(*) FROM companies") == 1
        assert _scalar(pg_db, "SELECT name FROM companies") == "Apple Inc."
        assert _scalar(pg_db, "SELECT created_at FROM companies") == created

    def test_null_cik_rows_skipped(self, pg_db):
        """cik 唯一约束对 NULL 永不触发冲突——NULL cik 行必须拒收，否则重复运行无限插入。"""
        assert pg_db.upsert_companies([{"cik": None, "name": "ETF Trust"}, {"name": "No CIK"}]) == 0
        assert _scalar(pg_db, "SELECT count(*) FROM companies") == 0

    def test_batch_duplicate_cik_last_row_wins(self, pg_db):
        pg_db.upsert_companies([
            {"cik": "0000320193", "name": "first"},
            {"cik": "0000320193", "name": "second"},
        ])
        assert _scalar(pg_db, "SELECT count(*) FROM companies") == 1
        assert _scalar(pg_db, "SELECT name FROM companies") == "second"

    def test_conflict_without_name_does_not_null_existing(self, pg_db):
        pg_db.upsert_companies([{"cik": "0000320193", "name": "Apple Inc."}])
        pg_db.upsert_companies([{"cik": "0000320193"}])
        assert _scalar(pg_db, "SELECT name FROM companies") == "Apple Inc."

    def test_get_company_id_by_cik(self, pg_db):
        pg_db.upsert_companies([{"cik": "0000320193", "name": "Apple Inc."}])
        assert pg_db.get_company_id_by_cik("0000320193") is not None
        assert pg_db.get_company_id_by_cik("0000999999") is None
        assert pg_db.get_company_id_by_cik(None) is None

    def test_security_company_id_fk_roundtrip(self, pg_db):
        pg_db.upsert_companies([{"cik": "0000320193", "name": "Apple Inc."}])
        company_id = pg_db.get_company_id_by_cik("0000320193")
        _insert_security(pg_db, 1, "aapl", company_id=company_id)
        assert _scalar(pg_db, "SELECT company_id FROM securities WHERE id=1") == company_id


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

    def test_batch_duplicate_price_key_last_row_wins(self, pg_db):
        _insert_security(pg_db)
        row = {"security_id": 1, "date": date(2026, 6, 10), "close": 2, "volume": 100}
        pg_db.upsert_daily_prices([row, {**row, "close": 3, "volume": 200}])
        assert _scalar(pg_db, "SELECT count(*) FROM daily_prices") == 1
        assert _scalar(pg_db, "SELECT close FROM daily_prices") == Decimal("3.000000")
        assert _scalar(pg_db, "SELECT volume FROM daily_prices") == 200

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

    def test_upsert_shares_does_not_overwrite_float_with_null(self, pg_db):
        _insert_security(pg_db)
        row = {
            "security_id": 1, "filing_date": date(2026, 3, 31), "period_end_date": date(2026, 3, 31),
            "total_shares": 1000, "float_shares": 800, "free_float_percent": Decimal("80"), "source": "MASSIVE",
        }
        pg_db.upsert_historical_shares([row])
        pg_db.upsert_historical_shares([{**row, "total_shares": 1100, "float_shares": None, "free_float_percent": None}])
        assert _scalar(pg_db, "SELECT total_shares FROM historical_shares") == 1100
        assert _scalar(pg_db, "SELECT float_shares FROM historical_shares") == 800
        assert _scalar(pg_db, "SELECT free_float_percent FROM historical_shares") == Decimal("80.0000")

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

    def test_batch_duplicate_identifier_inserted_once(self, pg_db):
        _insert_security(pg_db)
        row = {"security_id": 1, "id_type": "CIK", "id_value": "0000320193", "source": "SEC"}
        assert pg_db.insert_missing_security_identifiers([row, dict(row)]) == 1
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

    def test_items_passthrough_and_conflict_refresh(self, pg_db):
        """8-K item codes 原样入库；重拉时以 vendor 当前值原位刷新（含刷成 NULL）。"""
        row = {
            "source": "SEC", "accession_number": "0001-26-000002", "form_type": "8-K",
            "filing_date": date(2026, 2, 10), "cik": "0000320193", "items": "2.01,9.01",
        }
        pg_db.upsert_sec_filings([row])
        assert _scalar(pg_db, "SELECT items FROM sec_filings") == "2.01,9.01"

        pg_db.upsert_sec_filings([{**row, "items": "2.01"}])
        assert _scalar(pg_db, "SELECT count(*) FROM sec_filings") == 1
        assert _scalar(pg_db, "SELECT items FROM sec_filings") == "2.01"

        pg_db.upsert_sec_filings([{**row, "items": None}])
        assert _scalar(pg_db, "SELECT items FROM sec_filings") is None

    def test_row_without_items_key_preserves_existing_items(self, pg_db):
        """不带 items 键的写入（非 EDGAR submissions 通道）不得抹掉已有值。"""
        row = {
            "source": "SEC", "accession_number": "0001-26-000003", "form_type": "8-K",
            "filing_date": date(2026, 2, 10), "items": "5.02",
        }
        pg_db.upsert_sec_filings([row])
        no_items = {key: value for key, value in row.items() if key != "items"}
        pg_db.upsert_sec_filings([{**no_items, "issuer_name": "Apple Inc."}])
        assert _scalar(pg_db, "SELECT items FROM sec_filings") == "5.02"
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


class TestInsiderTransactions:
    def _row(self, **extra):
        return {
            "source": "SEC_EDGAR",
            "accession_number": "0001-26-000042",
            "source_row_hash": "a" * 64,
            "form_type": "4",
            "transaction_code": "S",
            "transaction_shares": Decimal("100"),
            "transaction_price_per_share": Decimal("10.5"),
            **extra,
        }

    def test_insert_then_idempotent_reupsert(self, pg_db):
        assert pg_db.upsert_insider_transactions([self._row()]) == 1
        pg_db.upsert_insider_transactions([self._row()])
        assert _scalar(pg_db, "SELECT count(*) FROM insider_transactions") == 1

    def test_conflict_updates_provided_fields(self, pg_db):
        _insert_security(pg_db)
        pg_db.upsert_insider_transactions([self._row()])
        pg_db.upsert_insider_transactions([self._row(security_id=1, transaction_shares=Decimal("200"))])
        assert _scalar(pg_db, "SELECT count(*) FROM insider_transactions") == 1
        assert _scalar(pg_db, "SELECT security_id FROM insider_transactions") == 1
        assert _scalar(pg_db, "SELECT transaction_shares FROM insider_transactions") == Decimal("200.000000")

    def test_batch_dedup_and_missing_keys_skipped(self, pg_db):
        rows = [self._row(), self._row(), {"source": "SEC_EDGAR", "accession_number": "x"}]
        assert pg_db.upsert_insider_transactions(rows) == 1

    def test_distinct_hashes_create_separate_rows(self, pg_db):
        pg_db.upsert_insider_transactions([self._row(), self._row(source_row_hash="b" * 64)])
        assert _scalar(pg_db, "SELECT count(*) FROM insider_transactions") == 2


class TestInstitutionalHoldings:
    def _row(self, **extra):
        return {
            "source": "SEC_EDGAR",
            "accession_number": "0001-26-000099",
            "source_row_hash": "c" * 64,
            "filer_cik": "0001779506",
            "cusip": "037833100",
            "market_value": Decimal("1864"),
            "shares_or_principal_amount": Decimal("10"),
            **extra,
        }

    def test_insert_then_idempotent_reupsert(self, pg_db):
        assert pg_db.upsert_institutional_holdings([self._row()]) == 1
        pg_db.upsert_institutional_holdings([self._row()])
        assert _scalar(pg_db, "SELECT count(*) FROM institutional_holdings") == 1

    def test_security_id_not_cleared_by_unmapped_reupsert(self, pg_db):
        _insert_security(pg_db)
        pg_db.upsert_institutional_holdings([self._row(security_id=1)])
        # CUSIP 映射缺失的重灌批次 security_id=None，不得清掉已映射值
        pg_db.upsert_institutional_holdings([self._row(security_id=None, market_value=Decimal("2000"))])
        assert _scalar(pg_db, "SELECT security_id FROM institutional_holdings") == 1
        assert _scalar(pg_db, "SELECT market_value FROM institutional_holdings") == Decimal("2000.0000")

    def test_missing_filer_cik_skipped(self, pg_db):
        row = self._row()
        row.pop("filer_cik")
        assert pg_db.upsert_institutional_holdings([row]) == 0


class TestFxRates:
    def _rows(self):
        return [
            {"rate_date": date(2026, 6, 5), "base_currency": "EUR", "quote_currency": "USD",
             "source": "ECB", "rate": Decimal("1.14")},
            {"rate_date": date(2026, 6, 5), "base_currency": "EUR", "quote_currency": "CAD",
             "source": "ECB", "rate": Decimal("1.60")},
            {"rate_date": date(2026, 6, 11), "base_currency": "EUR", "quote_currency": "USD",
             "source": "ECB", "rate": Decimal("1.1537")},
            {"rate_date": date(2026, 6, 11), "base_currency": "EUR", "quote_currency": "CAD",
             "source": "ECB", "rate": Decimal("1.6127")},
        ]

    def test_upsert_idempotent_and_rate_refresh(self, pg_db):
        assert pg_db.upsert_fx_rates(self._rows()) == 4
        pg_db.upsert_fx_rates([{**self._rows()[0], "rate": Decimal("1.15")}])
        assert _scalar(pg_db, "SELECT count(*) FROM fx_rates") == 4
        assert _scalar(
            pg_db,
            "SELECT rate FROM fx_rates WHERE quote_currency='USD' AND rate_date='2026-06-05'",
        ) == Decimal("1.1500000000")

    def test_usd_converter_cross_rate_and_asof_fallback(self, pg_db):
        from utils.fx_rates import UsdFxConverter

        pg_db.upsert_fx_rates(self._rows())
        fx = UsdFxConverter(pg_db)
        # 当日有行情：CAD->USD = 1.1537/1.6127
        rate = fx.rate_to_usd("CAD", date(2026, 6, 11))
        assert rate is not None
        assert abs(rate - Decimal("1.1537") / Decimal("1.6127")) < Decimal("1e-15")
        # 周末回退到 6/5（间隔 2 天 < 7 天阈值）
        assert fx.rate_to_usd("CAD", date(2026, 6, 7)) == Decimal("1.14") / Decimal("1.60")
        # 超过 staleness 阈值
        assert fx.rate_to_usd("CAD", date(2026, 7, 1)) is None
        # EUR 与 USD 特例
        assert fx.rate_to_usd("EUR", date(2026, 6, 11)) == Decimal("1.1537")
        assert fx.rate_to_usd("USD", date(2026, 6, 11)) == Decimal("1")
        # 无该币种数据
        assert fx.rate_to_usd("XXX", date(2026, 6, 11)) is None


class TestRiskFreeRates:
    def _rows(self):
        return [
            {"date": date(2026, 6, 5), "series_id": "DTB3", "rate_pct": Decimal("4.28")},
            {"date": date(2026, 6, 8), "series_id": "DTB3", "rate_pct": Decimal("4.30")},
        ]

    def test_upsert_idempotent_and_rate_refresh(self, pg_db):
        assert pg_db.upsert_risk_free_rates(self._rows()) == 2
        pg_db.upsert_risk_free_rates([{**self._rows()[0], "rate_pct": Decimal("4.31")}])
        assert _scalar(pg_db, "SELECT count(*) FROM risk_free_rates") == 2
        assert _scalar(
            pg_db,
            "SELECT rate_pct FROM risk_free_rates WHERE series_id='DTB3' AND date='2026-06-05'",
        ) == Decimal("4.310000")

    def test_load_risk_free_daily_returns(self, pg_db):
        import pandas as pd

        from utils.risk_free_rates import load_risk_free_daily_returns

        pg_db.upsert_risk_free_rates(self._rows())
        index = pd.to_datetime(["2026-06-05", "2026-06-08"])
        returns = load_risk_free_daily_returns(pg_db.engine, index)
        assert list(returns.index) == list(pd.DatetimeIndex(index))
        assert returns.iloc[0] == pytest.approx(float(Decimal("4.28") / Decimal("100") / Decimal(360)))
        assert returns.iloc[1] == pytest.approx(float(Decimal("4.30") / Decimal("100") * Decimal(3) / Decimal(360)))
        with pytest.raises(LookupError, match="no fresh DTB3 row"):
            load_risk_free_daily_returns(pg_db.engine, pd.to_datetime(["2026-06-20"]))


class TestMapUnlinkedHoldings:
    def _holding(self, hash_ch, cusip, security_id=None):
        return {
            "source": "SEC_EDGAR", "accession_number": "0001-26-000077",
            "source_row_hash": hash_ch * 64, "filer_cik": "0001779506",
            "cusip": cusip, "security_id": security_id,
        }

    def test_backfills_null_only_and_skips_ambiguous(self, pg_db):
        _insert_security(pg_db, 1, "aapl")
        _insert_security(pg_db, 2, "msft")
        _insert_security(pg_db, 3, "goog")
        pg_db.insert_missing_security_identifiers([
            {"security_id": 1, "id_type": "CUSIP", "id_value": "037833100", "source": "SEC_FTD"},
            # 歧义 CUSIP：两个 security 共用
            {"security_id": 2, "id_type": "CUSIP", "id_value": "594918104", "source": "SEC_FTD"},
            {"security_id": 3, "id_type": "CUSIP", "id_value": "594918104", "source": "SEC_FTD"},
        ])
        pg_db.upsert_institutional_holdings([
            self._holding("a", "037833100"),              # 应回填 -> 1
            self._holding("b", "594918104"),              # 歧义，保持 NULL
            self._holding("c", "037833100", security_id=2),  # 已关联，不得覆盖
            self._holding("d", "000000000"),              # 无映射，保持 NULL
        ])

        assert pg_db.map_unlinked_holdings_to_securities() == 1
        assert _scalar(pg_db, "SELECT security_id FROM institutional_holdings WHERE source_row_hash = :h",
                       h="a" * 64) == 1
        assert _scalar(pg_db, "SELECT security_id FROM institutional_holdings WHERE source_row_hash = :h",
                       h="b" * 64) is None
        assert _scalar(pg_db, "SELECT security_id FROM institutional_holdings WHERE source_row_hash = :h",
                       h="c" * 64) == 2
        # 幂等：再跑无行可回填
        assert pg_db.map_unlinked_holdings_to_securities() == 0


# ---------------------------------------------------------------------------
# pipeline task runs
# ---------------------------------------------------------------------------

class TestPipelineTaskRuns:
    def test_start_and_finish_success(self, pg_db):
        task_id = pg_db.start_task_run("run-001", "update_massive_prices")
        assert task_id > 0
        assert _scalar(pg_db, "SELECT status FROM pipeline_task_runs WHERE id = :id", id=task_id) == "RUNNING"

        pg_db.finish_task_run(task_id, exit_code=0)
        assert _scalar(pg_db, "SELECT status FROM pipeline_task_runs WHERE id = :id", id=task_id) == "SUCCESS"
        assert _scalar(pg_db, "SELECT exit_code FROM pipeline_task_runs WHERE id = :id", id=task_id) == 0
        assert _scalar(pg_db, "SELECT ended_at IS NOT NULL FROM pipeline_task_runs WHERE id = :id", id=task_id) is True

    def test_start_and_finish_failure(self, pg_db):
        task_id = pg_db.start_task_run("run-002", "sync_massive_universe")
        pg_db.finish_task_run(task_id, exit_code=1, error_sample="exit=1")

        assert _scalar(pg_db, "SELECT status FROM pipeline_task_runs WHERE id = :id", id=task_id) == "FAILED"
        assert _scalar(pg_db, "SELECT error_sample FROM pipeline_task_runs WHERE id = :id", id=task_id) == "exit=1"

    def test_multiple_tasks_same_run(self, pg_db):
        id1 = pg_db.start_task_run("run-003", "step_a")
        id2 = pg_db.start_task_run("run-003", "step_b")
        pg_db.finish_task_run(id1, exit_code=0)
        pg_db.finish_task_run(id2, exit_code=1, error_sample="timeout")

        assert _scalar(pg_db, "SELECT count(*) FROM pipeline_task_runs WHERE run_id = 'run-003'") == 2


# ---------------------------------------------------------------------------
# 阶段 1a 收口 API（B1/B2-B4-B9/B6-B7 旁路收编，语义见各 docstring）
# ---------------------------------------------------------------------------

class TestDeactivateMissingSecurities:
    def test_only_active_us_whitelisted_types_are_deactivated(self, pg_db):
        """B1 三重过滤：非 US / 非白名单类型 / 已 inactive 的行都不触碰。"""
        _insert_security(pg_db, 1, "gone")                              # US CS 活跃、不在名单 → 摘牌
        _insert_security(pg_db, 2, "keep")                              # US CS 活跃、在名单 → 不动
        _insert_security(pg_db, 3, "cagone", market="CA")              # 非 US → 不动
        _insert_security(pg_db, 4, "fundgone", type="FUND")            # 非白名单类型 → 不动
        _insert_security(pg_db, 5, "deadgone", is_active=False)        # 已 inactive → 不动（rowcount 不含它）

        marked = pg_db.deactivate_missing_securities({"keep"})

        assert marked == 1
        assert _scalar(pg_db, "SELECT is_active FROM securities WHERE id=1") is False
        assert _scalar(pg_db, "SELECT is_active FROM securities WHERE id=2") is True
        assert _scalar(pg_db, "SELECT is_active FROM securities WHERE id=3") is True
        assert _scalar(pg_db, "SELECT is_active FROM securities WHERE id=4") is True
        assert _scalar(pg_db, "SELECT is_active FROM securities WHERE id=5") is False

    def test_market_and_type_match_case_insensitively(self, pg_db):
        _insert_security(pg_db, 1, "lowmkt", market="us", type="cs")
        assert pg_db.deactivate_missing_securities({"other"}) == 1
        assert _scalar(pg_db, "SELECT is_active FROM securities WHERE id=1") is False

    def test_etf_type_included_in_whitelist(self, pg_db):
        _insert_security(pg_db, 1, "etfgone", type="ETF")
        assert pg_db.deactivate_missing_securities({"other"}) == 1

    def test_empty_active_symbols_rejected(self, pg_db):
        """空名单等价于全量摘牌，只可能是上游拉取失败——必须拒绝。"""
        _insert_security(pg_db, 1, "aapl")
        with pytest.raises(ValueError, match="空 active_symbols"):
            pg_db.deactivate_missing_securities(set())
        assert _scalar(pg_db, "SELECT is_active FROM securities WHERE id=1") is True

    def test_idempotent_rerun_returns_zero(self, pg_db):
        _insert_security(pg_db, 1, "gone")
        assert pg_db.deactivate_missing_securities({"keep"}) == 1
        assert pg_db.deactivate_missing_securities({"keep"}) == 0


class TestEnrichSecurityIdentity:
    def test_fills_only_null_columns_never_overwrites(self, pg_db):
        """核心不变量：既有非 NULL 值绝不被覆盖，NULL 列逐列补入。"""
        _insert_security(pg_db, 1, "dead", is_active=False,
                         cik="0000000001", name="Existing Name")

        changed = pg_db.enrich_security_identity(1, {
            "cik": "0000009999",            # 已有值 → 不覆盖
            "name": "Vendor Name",          # 已有值 → 不覆盖
            "delist_date": date(2015, 6, 1),  # NULL → 补入
            "composite_figi": "BBG000NEW",    # NULL → 补入
        })

        assert changed == 1
        assert _scalar(pg_db, "SELECT cik FROM securities WHERE id=1") == "0000000001"
        assert _scalar(pg_db, "SELECT name FROM securities WHERE id=1") == "Existing Name"
        assert _scalar(pg_db, "SELECT delist_date FROM securities WHERE id=1") == date(2015, 6, 1)
        assert _scalar(pg_db, "SELECT composite_figi FROM securities WHERE id=1") == "BBG000NEW"

    def test_partially_filled_row_gets_remaining_nulls(self, pg_db):
        """语义定案（vs 旧 _apply_fills 整行 AND 守卫）：部分列已有值时不再
        整行跳过，而是补齐剩余 NULL 列——严格更精确且仍绝不覆盖。"""
        _insert_security(pg_db, 1, "dead", is_active=False, cik="0000000001")

        changed = pg_db.enrich_security_identity(1, {
            "cik": "0000009999",
            "delist_date": date(2015, 6, 1),
        })

        assert changed == 1
        assert _scalar(pg_db, "SELECT cik FROM securities WHERE id=1") == "0000000001"
        assert _scalar(pg_db, "SELECT delist_date FROM securities WHERE id=1") == date(2015, 6, 1)

    def test_all_target_columns_already_filled_returns_zero(self, pg_db):
        """全部目标列均已非 NULL → rowcount=0 无操作写
        （backfill_rename_events 的竞态跳过计数依赖此语义）。"""
        _insert_security(pg_db, 1, "aapl", composite_figi="BBG000EXIST")
        assert pg_db.enrich_security_identity(1, {"composite_figi": "BBG000OTHER"}) == 0
        assert _scalar(pg_db, "SELECT composite_figi FROM securities WHERE id=1") == "BBG000EXIST"

    def test_missing_row_returns_zero(self, pg_db):
        assert pg_db.enrich_security_identity(404, {"cik": "0000000001"}) == 0

    def test_none_values_are_skipped(self, pg_db):
        _insert_security(pg_db, 1, "aapl")
        # fills 全为 None：无操作，也不应生成空 SET 的非法 SQL
        assert pg_db.enrich_security_identity(1, {"cik": None, "name": None}) == 0
        assert _scalar(pg_db, "SELECT cik FROM securities WHERE id=1") is None

    def test_unknown_column_rejected(self, pg_db):
        _insert_security(pg_db, 1, "aapl")
        with pytest.raises(ValueError, match="白名单外"):
            pg_db.enrich_security_identity(1, {"is_active": False})
        with pytest.raises(ValueError, match="白名单外"):
            pg_db.enrich_security_identity(1, {"symbol": "hack"})

    def test_idempotent_rerun(self, pg_db):
        _insert_security(pg_db, 1, "dead", is_active=False)
        assert pg_db.enrich_security_identity(1, {"cik": "0000000001"}) == 1
        assert pg_db.enrich_security_identity(1, {"cik": "0000000001"}) == 0
        assert _scalar(pg_db, "SELECT cik FROM securities WHERE id=1") == "0000000001"


class TestRecalculatePriceLatestDates:
    def _prices(self, pg_db, security_id, *dates):
        pg_db.upsert_daily_prices([
            {"security_id": security_id, "date": d, "close": 1} for d in dates
        ])

    def test_full_table_recalculation(self, pg_db):
        """security_ids=None：全表按 MAX(daily_prices.date) 校准（B6 语义）。"""
        _insert_security(pg_db, 1, "aapl", price_data_latest_date=date(2026, 6, 1))
        _insert_security(pg_db, 2, "msft")   # 水位 NULL、有价格 → 补齐
        _insert_security(pg_db, 3, "goog")   # 无价格行 → 不触碰（不回落 NULL 也不写）
        self._prices(pg_db, 1, date(2026, 6, 10))
        self._prices(pg_db, 2, date(2026, 6, 9), date(2026, 6, 10))

        fixed = pg_db.recalculate_price_latest_dates()

        assert fixed == 2
        assert _scalar(pg_db, "SELECT price_data_latest_date FROM securities WHERE id=1") == date(2026, 6, 10)
        assert _scalar(pg_db, "SELECT price_data_latest_date FROM securities WHERE id=2") == date(2026, 6, 10)
        assert _scalar(pg_db, "SELECT price_data_latest_date FROM securities WHERE id=3") is None

    def test_scoped_recalculation_only_touches_given_ids(self, pg_db):
        """传 ids：范围重算（B7 touched_ids 语义），范围外滞后水位不动。"""
        _insert_security(pg_db, 1, "aapl", price_data_latest_date=date(2026, 1, 1))
        _insert_security(pg_db, 2, "msft", price_data_latest_date=date(2026, 1, 1))
        self._prices(pg_db, 1, date(2026, 6, 10))
        self._prices(pg_db, 2, date(2026, 6, 10))

        assert pg_db.recalculate_price_latest_dates([1]) == 1
        assert _scalar(pg_db, "SELECT price_data_latest_date FROM securities WHERE id=1") == date(2026, 6, 10)
        assert _scalar(pg_db, "SELECT price_data_latest_date FROM securities WHERE id=2") == date(2026, 1, 1)

    def test_is_distinct_from_guard_skips_up_to_date_rows(self, pg_db):
        """水位已一致 → 不产生无效写，rowcount=0。"""
        _insert_security(pg_db, 1, "aapl", price_data_latest_date=date(2026, 6, 10))
        self._prices(pg_db, 1, date(2026, 6, 10))
        assert pg_db.recalculate_price_latest_dates([1]) == 0
        assert pg_db.recalculate_price_latest_dates() == 0

    def test_stale_watermark_can_move_backwards_to_fact(self, pg_db):
        """重算是"按事实对齐"而非单向推进：虚高水位会被拉回 MAX(date)。"""
        _insert_security(pg_db, 1, "aapl", price_data_latest_date=date(2026, 12, 31))
        self._prices(pg_db, 1, date(2026, 6, 10))
        assert pg_db.recalculate_price_latest_dates([1]) == 1
        assert _scalar(pg_db, "SELECT price_data_latest_date FROM securities WHERE id=1") == date(2026, 6, 10)

    def test_empty_id_list_is_noop(self, pg_db):
        _insert_security(pg_db, 1, "aapl")
        self._prices(pg_db, 1, date(2026, 6, 10))
        assert pg_db.recalculate_price_latest_dates([]) == 0
        assert _scalar(pg_db, "SELECT price_data_latest_date FROM securities WHERE id=1") is None
