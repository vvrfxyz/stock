"""sync_massive_universe NEW 路径的 NEW_LISTING 身份事件编排测试。

回滚双保险锚点：新上市（含死票回收的新行）在 upsert 落库后必须写一条
NEW_LISTING 事件（details 含 origin=massive_universe_sync）。幂等性由
resolver 状态保证：重跑时 symbol 已解析为 ACTIVE_SYMBOL，不再进 NEW 路径，
事件不会重复。
"""
import json
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock

import pytest

import scripts.sync_massive_universe as sync_universe

NEWCO = {
    "symbol": "newco",
    "composite_figi": "BBG000NEW1",
    "cik": "0000000042",
    "name": "NewCo Inc",
    "type": "ADRC",
    "exchange": "XNAS",
}


def _result(**extra):
    defaults = dict(
        security_id=-1,
        resolution_type="NEW",
        confidence="HIGH",
        matched_field="",
        is_rename=False,
        is_recycle=False,
        recycled_from=None,
    )
    defaults.update(extra)
    return SimpleNamespace(**defaults)


class _StubResolver:
    def __init__(self, results):
        self._results = results

    def resolve_batch(self, rows, **kwargs):
        return self._results

    def _existing_symbol(self, security_id):
        return None


def _stub_runtime(monkeypatch, payloads, results, lookup_rows):
    """打桩 main() 的运行时依赖，返回 (source, db) 供断言。

    lookup_rows: NEW_LISTING 反查 (symbol, id) 的返回行——query 桩对 filter
    链深度不敏感（filter 返回自身）。
    """
    monkeypatch.setattr(sync_universe, "setup_logging", lambda: None)
    monkeypatch.setattr(sync_universe, "enforce_us_market", lambda market: None)
    monkeypatch.setattr(sync_universe, "get_massive_api_keys", lambda: ["key"])
    monkeypatch.setattr(sync_universe, "KeyRateLimiter", lambda *args, **kwargs: object())

    source = Mock()
    source.list_active_tickers.return_value = [dict(p) for p in payloads]
    source._build_reference_payload.side_effect = lambda item: item
    monkeypatch.setattr(sync_universe, "MassiveSource", lambda rate_limiter: source)

    query = MagicMock()
    query.filter.return_value = query
    query.all.return_value = lookup_rows
    session = MagicMock()
    session.query.return_value = query
    session_ctx = MagicMock()
    session_ctx.__enter__.return_value = session

    db = Mock()
    db.get_session.return_value = session_ctx
    db.upsert_securities_by_symbol.return_value = len(payloads)
    monkeypatch.setattr(sync_universe, "DatabaseManager", lambda: db)
    monkeypatch.setattr(
        sync_universe, "SecurityIdentityResolver", lambda session: _StubResolver(results)
    )
    return source, db


class TestNewListingEvent:
    def test_new_path_emits_single_new_listing_event(self, monkeypatch):
        _, db = _stub_runtime(
            monkeypatch, [NEWCO], [_result()], lookup_rows=[("newco", 42)],
        )

        assert sync_universe.main(["--skip-mark-missing-inactive"]) == 0

        db.insert_identity_events.assert_called_once()
        events = db.insert_identity_events.call_args.args[0]
        assert len(events) == 1
        event = events[0]
        assert event["event_type"] == "NEW_LISTING"
        assert event["security_id"] == 42
        assert event["new_symbol"] == "newco"
        assert event["resolution_source"] == "AUTO"
        assert event["confidence"] == "HIGH"
        details = json.loads(event["details"])
        assert details["origin"] == "massive_universe_sync"
        assert details["incoming_figi"] == "BBG000NEW1"
        assert details["incoming_cik"] == "0000000042"
        assert details["incoming_type"] == "ADRC"
        # 事件必须在新行 upsert 之后写：security_id 反查依赖行已存在
        call_names = [name for name, *_ in db.method_calls]
        assert call_names.index("upsert_securities_by_symbol") < call_names.index(
            "insert_identity_events"
        )

    def test_rerun_resolved_as_active_symbol_emits_no_event(self, monkeypatch):
        # 幂等重跑：上次 run 已插入该行，resolver 判 ACTIVE_SYMBOL 而非 NEW，
        # 不得再发 NEW_LISTING（也不发其他事件）。
        existing = _result(
            security_id=42, resolution_type="ACTIVE_SYMBOL", matched_field="symbol",
        )
        _, db = _stub_runtime(monkeypatch, [NEWCO], [existing], lookup_rows=[])

        assert sync_universe.main(["--skip-mark-missing-inactive"]) == 0
        db.insert_identity_events.assert_not_called()

    def test_dead_ticker_recycle_writes_recycle_then_new_listing(self, monkeypatch):
        # 死票回收新行：RECYCLE（旧身份）先写，新行入库后 NEW_LISTING 镜像
        # 事件带 related_security_id 指回旧身份。
        _, db = _stub_runtime(
            monkeypatch, [NEWCO], [_result(recycled_from=7)], lookup_rows=[("newco", 42)],
        )

        assert sync_universe.main(["--skip-mark-missing-inactive"]) == 0

        assert db.insert_identity_events.call_count == 2
        recycle_events = db.insert_identity_events.call_args_list[0].args[0]
        assert [e["event_type"] for e in recycle_events] == ["RECYCLE"]
        assert recycle_events[0]["security_id"] == 7

        listing_events = db.insert_identity_events.call_args_list[1].args[0]
        assert [e["event_type"] for e in listing_events] == ["NEW_LISTING"]
        event = listing_events[0]
        assert event["security_id"] == 42
        assert event["related_security_id"] == 7
        details = json.loads(event["details"])
        assert details["recycled_from"] == 7
        assert details["origin"] == "massive_universe_sync"

    def test_symbol_missing_after_upsert_skips_event(self, monkeypatch):
        # 批内变体去重等原因未实际落库：跳过事件而非错锚，run 仍成功。
        _, db = _stub_runtime(monkeypatch, [NEWCO], [_result()], lookup_rows=[])

        assert sync_universe.main(["--skip-mark-missing-inactive"]) == 0
        db.insert_identity_events.assert_not_called()


@pytest.mark.integration
class TestActiveLookupPg:
    """_lookup_active_us_ids_by_symbol 的 is_active 过滤在真实 PG 上锁定。

    mock 桩对 filter 链不敏感，测不出丢失 is_active 过滤的错锚——而该过滤
    恰是 DEAD_TICKER_RECYCLE 场景（死行与新行同 symbol）的正确性前提。
    """

    def test_recycled_symbol_resolves_to_active_row(self, pg_db):
        from sqlalchemy import text

        with pg_db.engine.connect() as conn:
            conn.execute(text(
                """
                insert into securities
                    (id, symbol, current_symbol, name, market, type,
                     is_active, full_refresh_interval)
                values
                    (7, 'foo', 'foo', 'Dead Predecessor', 'US', 'CS', false, 30),
                    (42, 'foo', 'foo', 'New Tenant', 'US', 'ADRC', true, 30)
                """
            ))
            conn.commit()

        ids = sync_universe._lookup_active_us_ids_by_symbol(pg_db, {"foo"})
        assert ids == {"foo": 42}
