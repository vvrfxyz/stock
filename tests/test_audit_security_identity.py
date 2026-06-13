"""scripts/audit_security_identity 的 PostgreSQL 集成测试。

只读对账脚本的每个检查都依赖 PG 方言（array_agg ORDER BY、FILTER、ANY），
单元 Mock 无法验证；这里用一次性测试库构造场景断言计数。
"""
from datetime import date

import pytest
from sqlalchemy import text

from data_models.models import Security, SecurityIdentifier, SecuritySymbolHistory
import scripts.audit_security_identity as audit

pytestmark = pytest.mark.integration


def _add_security(session, **kw):
    row = dict(
        market="US", type="CS", is_active=True, full_refresh_interval=30,
    )
    row.update(kw)
    row.setdefault("current_symbol", row.get("symbol"))
    session.add(Security(**row))


def _session(pg_db):
    return pg_db.get_session()


def test_shared_identity_column_detects_split(pg_db):
    with _session(pg_db) as s:
        _add_security(s, id=1, symbol="meta", composite_figi="BBG-META", cik="0001326801", is_active=True)
        _add_security(s, id=2, symbol="fb", composite_figi="BBG-META", cik="0001326801", is_active=False)
        _add_security(s, id=3, symbol="aapl", composite_figi="BBG-AAPL", is_active=True)
        s.commit()
        assert audit.check_shared_identity_column(s, "composite_figi", limit=10) == 1
        assert audit.check_shared_identity_column(s, "cik", limit=10) == 1


def test_recycled_symbol_only_flags_identity_mismatch(pg_db):
    with _session(pg_db) as s:
        # 同 symbol 但 FIGI 不一致 -> 回收
        _add_security(s, id=1, symbol="zzz", composite_figi="BBG-OLD", is_active=False)
        _add_security(s, id=2, symbol="zzz", composite_figi="BBG-NEW", is_active=True)
        # 同 symbol 且 FIGI 一致 -> 正常，不应报
        _add_security(s, id=3, symbol="yyy", composite_figi="BBG-SAME", is_active=False)
        _add_security(s, id=4, symbol="yyy", composite_figi="BBG-SAME", is_active=True)
        s.commit()
        rows = audit.check_recycled_symbol(s, "symbol", limit=10)
        assert rows == 1


def test_active_symbol_collisions_block_migration(pg_db):
    # 该检查针对的是 *迁移前* 的库：active-only 唯一索引尚未建立、已有重复活跃行。
    # 测试库 schema 已含索引（会直接拒绝重复插入），临时 drop 模拟迁移前状态。
    with pg_db.engine.connect() as conn:
        conn.execute(text("DROP INDEX IF EXISTS _active_symbol_uc"))
        conn.commit()
    try:
        with _session(pg_db) as s:
            _add_security(s, id=1, symbol="dup", exchange="XNAS", is_active=True)
            # 第二个活跃同 symbol：迁移前的硬冲突
            _add_security(s, id=2, symbol="dup", exchange="XNYS", is_active=True)
            s.commit()
            # symbol 重复 1 组 +（current_symbol,exchange）不冲突（exchange 不同）
            assert audit.check_active_symbol_collisions(s, limit=10) == 1
    finally:
        with pg_db.engine.connect() as conn:
            # 先清掉重复行再重建唯一索引，否则索引创建会失败；下个用例的 TRUNCATE 兜底。
            conn.execute(text("DELETE FROM securities"))
            conn.execute(text(
                "CREATE UNIQUE INDEX IF NOT EXISTS _active_symbol_uc "
                "ON securities (symbol) WHERE is_active IS TRUE"
            ))
            conn.commit()


def test_ambiguous_identifier_map(pg_db):
    with _session(pg_db) as s:
        _add_security(s, id=1, symbol="a1")
        _add_security(s, id=2, symbol="a2")
        s.add(SecurityIdentifier(security_id=1, id_type="CUSIP", id_value="037833100", source="FTD"))
        s.add(SecurityIdentifier(security_id=2, id_type="CUSIP", id_value="037833100", source="FTD"))
        s.add(SecurityIdentifier(security_id=1, id_type="CIK", id_value="0000320193", source="SEC"))
        s.commit()
        assert audit.check_ambiguous_identifier_map(s, ["CUSIP", "CIK", "FIGI"], limit=10) == 1


def test_symbol_history_reconnect(pg_db):
    with _session(pg_db) as s:
        # 旧 id=1 改名历史里有 'oldsym'，而 'oldsym' 现在是活跃 id=2 的代码
        _add_security(s, id=1, symbol="newsym", is_active=False)
        _add_security(s, id=2, symbol="oldsym", is_active=True)
        s.add(SecuritySymbolHistory(
            security_id=1, symbol="oldsym", source="MASSIVE",
            event_type="ticker_change", start_date=date(2020, 1, 1),
        ))
        s.commit()
        assert audit.check_symbol_history_reconnect(s, limit=10) == 1


def test_clean_db_reports_nothing(pg_db):
    with _session(pg_db) as s:
        _add_security(s, id=1, symbol="aapl", composite_figi="BBG-AAPL", cik="0000320193", exchange="XNAS")
        _add_security(s, id=2, symbol="msft", composite_figi="BBG-MSFT", cik="0000789019", exchange="XNAS")
        s.commit()
        assert audit.check_shared_identity_column(s, "composite_figi", limit=10) == 0
        assert audit.check_shared_identity_column(s, "cik", limit=10) == 0
        assert audit.check_recycled_symbol(s, "symbol", limit=10) == 0
        assert audit.check_recycled_symbol(s, "current_symbol", limit=10) == 0
        assert audit.check_active_symbol_collisions(s, limit=10) == 0
        assert audit.check_ambiguous_identifier_map(s, ["CUSIP", "CIK", "FIGI"], limit=10) == 0
        assert audit.check_symbol_history_reconnect(s, limit=10) == 0
