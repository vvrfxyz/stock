"""scripts/cleanup_unknown_figi.py 的单元 + PG 集成测试。

单元测试打桩 db（test_script_runs 风格），锁定：选取 SQL 参数、事件 payload
形状、dry-run 不写库、apply 只对 RETURNING 命中的行写事件。
集成测试走 conftest 的一次性 PG：种 2 只证券（一只 UNKNOWN、一只真 FIGI），
验证 apply 置 NULL + 写事件 + 二次执行是 no-op（无重复事件）。
"""
import json
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock

import pytest
from sqlalchemy import text

import scripts.cleanup_unknown_figi as cleanup


def _mock_db(select_rows):
    """构造带 get_session 上下文和 engine.connect 上下文的 Mock db。"""
    db = Mock()
    session = MagicMock()
    session.execute.return_value.all.return_value = select_rows
    db.get_session.return_value.__enter__ = Mock(return_value=session)
    db.get_session.return_value.__exit__ = Mock(return_value=False)
    return db, session


def _row(sec_id, symbol, figi=cleanup.UNKNOWN_FIGI_LITERAL):
    return SimpleNamespace(id=sec_id, symbol=symbol, composite_figi=figi)


class TestBuildIdentityEvents:
    def test_payload_shape(self):
        events = cleanup.build_identity_events([{"id": 71, "symbol": "xtkg"}])
        assert len(events) == 1
        event = events[0]
        assert event["security_id"] == 71
        assert event["event_type"] == "MANUAL"
        assert event["resolution_source"] == "MANUAL"
        assert event["confidence"] == "HIGH"
        details = json.loads(event["details"])
        assert details == {
            "action": "clear_unknown_figi_literal",
            "previous_composite_figi": "UNKNOWN",
            "symbol": "xtkg",
        }

    def test_empty_input(self):
        assert cleanup.build_identity_events([]) == []


class TestRunDryRun:
    def test_dry_run_lists_but_never_writes(self):
        db, session = _mock_db([_row(71, "xtkg"), _row(206, "cjet")])

        args = cleanup.create_parser().parse_args([])
        assert cleanup.run(args, db) == 0

        # 选取 SQL 以字面量为参数
        params = session.execute.call_args.args[1]
        assert params == {"literal": "UNKNOWN"}
        db.insert_identity_events.assert_not_called()
        db.engine.connect.assert_not_called()

    def test_no_rows_short_circuits(self):
        db, _ = _mock_db([])

        args = cleanup.create_parser().parse_args(["--apply"])
        assert cleanup.run(args, db) == 0
        db.insert_identity_events.assert_not_called()
        db.engine.connect.assert_not_called()


class TestRunApply:
    def test_apply_updates_and_writes_events_for_returned_rows_only(self):
        db, _ = _mock_db([_row(71, "xtkg"), _row(206, "cjet")])
        conn = MagicMock()
        # RETURNING 只命中一行（模拟并发下另一行已被清理）
        conn.execute.return_value = [SimpleNamespace(id=71, symbol="xtkg")]
        db.engine.connect.return_value.__enter__ = Mock(return_value=conn)
        db.engine.connect.return_value.__exit__ = Mock(return_value=False)
        db.insert_identity_events.return_value = 1

        args = cleanup.create_parser().parse_args(["--apply"])
        assert cleanup.run(args, db) == 0

        update_sql = str(conn.execute.call_args.args[0])
        assert "UPDATE securities SET composite_figi = NULL" in update_sql
        assert "RETURNING id, symbol" in update_sql
        assert conn.execute.call_args.args[1] == {"literal": "UNKNOWN"}
        conn.commit.assert_called_once()

        events = db.insert_identity_events.call_args.args[0]
        assert [e["security_id"] for e in events] == [71]
        assert events[0]["event_type"] == "MANUAL"

    def test_apply_zero_updates_skips_event_insert(self):
        db, _ = _mock_db([_row(71, "xtkg")])
        conn = MagicMock()
        conn.execute.return_value = []
        db.engine.connect.return_value.__enter__ = Mock(return_value=conn)
        db.engine.connect.return_value.__exit__ = Mock(return_value=False)

        args = cleanup.create_parser().parse_args(["--apply"])
        assert cleanup.run(args, db) == 0
        db.insert_identity_events.assert_not_called()


@pytest.mark.integration
class TestCleanupUnknownFigiPG:
    def _seed(self, pg_db):
        from data_models.models import Security

        with pg_db.get_session() as session:
            unknown = Security(symbol="xtkg", current_symbol="xtkg",
                               composite_figi="UNKNOWN", is_active=False)
            real = Security(symbol="aapl", current_symbol="aapl",
                            composite_figi="BBG000B9XRY4", is_active=True)
            session.add_all([unknown, real])
            session.commit()
            return unknown.id, real.id

    def test_apply_clears_writes_event_and_second_run_is_noop(self, pg_db):
        unknown_id, real_id = self._seed(pg_db)
        parser = cleanup.create_parser()

        # dry-run 不改库不写事件
        assert cleanup.run(parser.parse_args([]), pg_db) == 0
        with pg_db.get_session() as session:
            figi = session.execute(text(
                "SELECT composite_figi FROM securities WHERE id = :id"
            ), {"id": unknown_id}).scalar_one()
            assert figi == "UNKNOWN"
            assert session.execute(text(
                "SELECT count(*) FROM security_identity_events"
            )).scalar_one() == 0

        # apply：UNKNOWN 置 NULL、真 FIGI 不动、写一条 MANUAL 事件
        assert cleanup.run(parser.parse_args(["--apply"]), pg_db) == 0
        with pg_db.get_session() as session:
            figis = dict(session.execute(text(
                "SELECT id, composite_figi FROM securities"
            )).all())
            assert figis[unknown_id] is None
            assert figis[real_id] == "BBG000B9XRY4"

            events = session.execute(text(
                "SELECT security_id, event_type, resolution_source, confidence, details "
                "FROM security_identity_events"
            )).all()
            assert len(events) == 1
            event = events[0]
            assert event.security_id == unknown_id
            assert event.event_type == "MANUAL"
            assert event.resolution_source == "MANUAL"
            assert event.confidence == "HIGH"
            assert json.loads(event.details) == {
                "action": "clear_unknown_figi_literal",
                "previous_composite_figi": "UNKNOWN",
                "symbol": "xtkg",
            }

        # 二次执行是 no-op：无新事件、FIGI 状态不变
        assert cleanup.run(parser.parse_args(["--apply"]), pg_db) == 0
        with pg_db.get_session() as session:
            assert session.execute(text(
                "SELECT count(*) FROM security_identity_events"
            )).scalar_one() == 1
            assert session.execute(text(
                "SELECT count(*) FROM securities WHERE composite_figi = 'UNKNOWN'"
            )).scalar_one() == 0
