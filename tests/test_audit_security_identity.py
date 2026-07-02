"""scripts/audit_security_identity 的 PostgreSQL 集成测试。

只读对账脚本的每个检查都依赖 PG 方言（array_agg ORDER BY、FILTER、ANY），
单元 Mock 无法验证；这里用一次性测试库构造场景断言计数。
"""
from datetime import date, datetime, timezone

import pytest
from sqlalchemy import text

from data_models.models import (
    InstitutionalHolding,
    Security,
    SecurityIdentifier,
    SecurityIdentityEvent,
    SecuritySymbolHistory,
)
import scripts.audit_security_identity as audit
import scripts.repair_cusip_links as repair

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
        assert audit.check_ftd_symbol_attribution(s, limit=10) == 0


# --------------------------------------------------------------------------- #
# SEC_FTD 反向校验：源期间 symbol 归属可疑的 CUSIP 链接
# --------------------------------------------------------------------------- #

def _ts(year, month, day):
    return datetime(year, month, day, 12, 0, tzinfo=timezone.utc)


def _add_ftd_identifier(session, security_id, cusip, start_date, created_at, source="SEC_FTD"):
    session.add(SecurityIdentifier(
        security_id=security_id, id_type="CUSIP", id_value=cusip,
        source=source, confidence="ftd_symbol_match",
        start_date=start_date, created_at=created_at,
    ))


def _add_event(session, security_id, event_type, old_symbol, new_symbol, created_at):
    session.add(SecurityIdentityEvent(
        security_id=security_id, event_type=event_type,
        old_symbol=old_symbol, new_symbol=new_symbol,
        resolution_source="AUTO", created_at=created_at,
    ))


class TestFtdSymbolAttribution:
    def test_rename_hijack_flagged(self, pg_db):
        # 证券 6 月 10 日才改名获得 'hot'，5 月的 FTD 观测里的 'hot' 属于别人
        with _session(pg_db) as s:
            _add_security(s, id=1, symbol="hot")
            _add_event(s, 1, "RENAME", "cold", "hot", _ts(2026, 6, 10))
            _add_ftd_identifier(s, 1, "111111111", date(2026, 5, 1), _ts(2026, 7, 1))
            s.commit()
            rows = audit.find_suspect_ftd_links(s)
            assert len(rows) == 1
            assert rows[0].via_rename_event is True
            assert rows[0].identifier_id is not None
            assert audit.check_ftd_symbol_attribution(s, limit=10) == 1

    def test_rename_after_link_creation_not_flagged(self, pg_db):
        # 链接建立在改名之前：当时匹配是对的，之后的改名不追溯否定 CUSIP 归属
        with _session(pg_db) as s:
            _add_security(s, id=1, symbol="hot")
            _add_event(s, 1, "RENAME", "cold", "hot", _ts(2026, 6, 10))
            _add_ftd_identifier(s, 1, "111111111", date(2026, 3, 1), _ts(2026, 4, 1))
            s.commit()
            assert audit.find_suspect_ftd_links(s) == []

    def test_rename_before_observation_not_flagged(self, pg_db):
        # 改名早于观测期起点：观测到的已经是改名后的 symbol，链接正确
        with _session(pg_db) as s:
            _add_security(s, id=1, symbol="hot")
            _add_event(s, 1, "RENAME", "cold", "hot", _ts(2026, 1, 10))
            _add_ftd_identifier(s, 1, "111111111", date(2026, 5, 1), _ts(2026, 7, 1))
            s.commit()
            assert audit.find_suspect_ftd_links(s) == []

    def test_recycle_quarantine_legacy_row_flagged(self, pg_db):
        # 回收隔离期错链的存量行：start_date 为 NULL，用创建时间回推窗口
        with _session(pg_db) as s:
            _add_security(s, id=1, symbol="zzz")
            _add_event(s, 1, "QUARANTINE", "zzz", "zzz", _ts(2026, 6, 10))
            _add_ftd_identifier(s, 1, "222222222", None, _ts(2026, 7, 1))
            s.commit()
            rows = audit.find_suspect_ftd_links(s)
            assert len(rows) == 1
            assert rows[0].via_recycle_event is True

    def test_recycle_with_observation_entirely_before_event_not_flagged(self, pg_db):
        # 观测期整体早于回收事件：观测到的还是旧公司的 CUSIP，链到旧身份是对的
        with _session(pg_db) as s:
            _add_security(s, id=1, symbol="zzz")
            _add_event(s, 1, "RECYCLE", "zzz", "zzz", _ts(2026, 6, 10))
            _add_ftd_identifier(s, 1, "222222222", date(2026, 2, 1), _ts(2026, 7, 1))
            s.commit()
            assert audit.find_suspect_ftd_links(s) == []

    def test_ancient_unresolved_recycle_not_flagged(self, pg_db):
        # 陈年未决的回收事件不得把此后所有重链永久打回（防 flag-repair-relink 死循环）
        with _session(pg_db) as s:
            _add_security(s, id=1, symbol="zzz")
            _add_event(s, 1, "RECYCLE", "zzz", "zzz", _ts(2024, 1, 1))
            _add_ftd_identifier(s, 1, "222222222", date(2026, 6, 16), _ts(2026, 7, 1))
            s.commit()
            assert audit.find_suspect_ftd_links(s) == []

    def test_symbol_history_flux_flagged(self, pg_db):
        # 无身份事件（早于事件表上线）的存量改名：symbol history 显示窗口内换过代码
        with _session(pg_db) as s:
            _add_security(s, id=1, symbol="cur")
            s.add(SecuritySymbolHistory(
                security_id=1, symbol="old", source="MASSIVE",
                event_type="ticker_change", start_date=date(2026, 6, 10),
            ))
            _add_ftd_identifier(s, 1, "333333333", date(2026, 5, 1), _ts(2026, 7, 1))
            s.commit()
            rows = audit.find_suspect_ftd_links(s)
            assert len(rows) == 1
            assert rows[0].via_symbol_history is True

    def test_ipo_first_listing_history_not_flagged(self, pg_db):
        # 新上市首发行（history 只有当前 symbol 自己）不是改名信号，不得误报 IPO
        with _session(pg_db) as s:
            _add_security(s, id=1, symbol="ipo")
            s.add(SecuritySymbolHistory(
                security_id=1, symbol="ipo", source="MASSIVE",
                event_type="ticker_change", start_date=date(2026, 6, 10),
            ))
            _add_ftd_identifier(s, 1, "444444444", None, _ts(2026, 7, 1))
            s.commit()
            assert audit.find_suspect_ftd_links(s) == []

    def test_non_ftd_identifiers_ignored(self, pg_db):
        with _session(pg_db) as s:
            _add_security(s, id=1, symbol="hot")
            _add_event(s, 1, "RENAME", "cold", "hot", _ts(2026, 6, 10))
            _add_ftd_identifier(s, 1, "555555555", date(2026, 5, 1), _ts(2026, 7, 1), source="MANUAL")
            s.commit()
            assert audit.find_suspect_ftd_links(s) == []


# --------------------------------------------------------------------------- #
# repair_cusip_links：删除坏行 + holdings 置 NULL
# --------------------------------------------------------------------------- #

def _add_holding(session, hash_ch, cusip, security_id):
    session.add(InstitutionalHolding(
        source="SEC_EDGAR", accession_number="0001-26-000077",
        source_row_hash=hash_ch * 64, filer_cik="0001779506",
        cusip=cusip, security_id=security_id,
    ))


class TestRepairCusipLinks:
    def _seed_bad_link(self, s):
        _add_security(s, id=1, symbol="hot")
        _add_event(s, 1, "RENAME", "cold", "hot", _ts(2026, 6, 10))
        _add_ftd_identifier(s, 1, "111111111", date(2026, 5, 1), _ts(2026, 7, 1))
        _add_holding(s, "a", "111111111", security_id=1)   # 经错链回填，应重置
        _add_holding(s, "b", "111111111", security_id=None)  # 本就未关联，保持 NULL
        _add_holding(s, "c", "999999999", security_id=1)   # 其他 CUSIP 的关联，不得波及
        s.commit()

    def test_build_plans_reports_affected_holdings(self, pg_db):
        with _session(pg_db) as s:
            self._seed_bad_link(s)
            plans = repair.build_plans(s, limit=10)
        assert len(plans) == 1
        assert plans[0]["cusip"] == "111111111"
        assert plans[0]["security_id"] == 1
        assert plans[0]["affected_holdings"] == 1
        assert plans[0]["signals"]["rename_event"] is True

    def test_apply_plan_deletes_identifier_and_unlinks_holdings(self, pg_db):
        with _session(pg_db) as s:
            self._seed_bad_link(s)
            plans = repair.build_plans(s, limit=10)

        deleted, unlinked = repair.apply_plan(pg_db, plans[0])
        assert (deleted, unlinked) == (1, 1)

        with pg_db.engine.connect() as conn:
            assert conn.execute(text(
                "SELECT count(*) FROM security_identifiers WHERE source = 'SEC_FTD'"
            )).scalar() == 0
            by_hash = {
                r.source_row_hash[0]: r.security_id
                for r in conn.execute(text(
                    "SELECT source_row_hash, security_id FROM institutional_holdings"
                ))
            }
        assert by_hash["a"] is None
        assert by_hash["b"] is None
        assert by_hash["c"] == 1

        # 修复后反向校验清零
        with _session(pg_db) as s:
            assert audit.find_suspect_ftd_links(s) == []

    def test_limit_caps_plans(self, pg_db):
        with _session(pg_db) as s:
            _add_security(s, id=1, symbol="hot")
            _add_event(s, 1, "RENAME", "cold", "hot", _ts(2026, 6, 10))
            _add_ftd_identifier(s, 1, "111111111", date(2026, 5, 1), _ts(2026, 7, 1))
            _add_ftd_identifier(s, 1, "222222222", date(2026, 5, 1), _ts(2026, 7, 1))
            s.commit()
            assert len(repair.build_plans(s, limit=1)) == 1
            assert len(repair.build_plans(s, limit=10)) == 2
