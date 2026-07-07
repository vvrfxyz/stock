"""build_company_events 与 upsert_company_events 的测试。

分两层：
- 纯解析层单测（无 DB，无 integration 标记）：evidence 令牌与对价文档解析；
- PG 集成（pg_db fixture）：upsert 冲突/protected 语义，脚本 run() 在合成数据上
  同时缝出 Alphabet CIK_CHANGE 与 delisting MERGER 两类边，且重跑 no-op。
"""
from datetime import date

import pytest
from sqlalchemy import text

from data_models.models import Company, Security
from scripts.build_company_events import (
    AcquirerToken,
    build_merger_evidence,
    parse_acquirer_security_token,
    parse_consideration_docs,
    run,
)


# ---------------------------------------------------------------------------
# 纯解析层单测（无 DB）
# ---------------------------------------------------------------------------

class TestParseAcquirerToken:
    def test_basic_token(self):
        ev = "form25=x|acquirer_security=xom#3659|acquirer_close=59.1"
        assert parse_acquirer_security_token(ev) == AcquirerToken("xom", 3659)

    def test_symbol_with_dot(self):
        assert parse_acquirer_security_token("acquirer_security=brk.a#123") == AcquirerToken("brk.a", 123)

    def test_symbol_with_hyphen(self):
        assert parse_acquirer_security_token("x|acquirer_security=rds-a#77|y") == AcquirerToken("rds-a", 77)

    def test_no_token_returns_none(self):
        assert parse_acquirer_security_token("form25=x|consideration_cash=10") is None

    def test_none_and_empty(self):
        assert parse_acquirer_security_token(None) is None
        assert parse_acquirer_security_token("") is None

    def test_token_at_start(self):
        assert parse_acquirer_security_token("acquirer_security=aa#9771|rest") == AcquirerToken("aa", 9771)

    def test_stops_at_pipe_boundary(self):
        # symbol 段不得吞掉 '#'/'|' 后的内容
        tok = parse_acquirer_security_token("acquirer_security=it#1807|consideration_docs=a,b")
        assert tok == AcquirerToken("it", 1807)


class TestParseConsiderationDocs:
    def test_multiple_docs(self):
        ev = "acquirer_security=xom#3659|consideration_docs=0001-1,0002-2,0003-3|note=x"
        assert parse_consideration_docs(ev) == ["0001-1", "0002-2", "0003-3"]

    def test_single_doc(self):
        assert parse_consideration_docs("consideration_docs=0001-1") == ["0001-1"]

    def test_absent_returns_empty(self):
        assert parse_consideration_docs("acquirer_security=xom#3659") == []

    def test_none_and_empty(self):
        assert parse_consideration_docs(None) == []
        assert parse_consideration_docs("") == []

    def test_strips_whitespace(self):
        assert parse_consideration_docs("consideration_docs=a , b ,c") == ["a", "b", "c"]


class TestBuildMergerEvidence:
    def test_with_docs(self):
        ev = build_merger_evidence(AcquirerToken("xom", 3659), ["0001-1", "0002-2"])
        assert ev == "acquirer_security=xom#3659|consideration_docs=0001-1,0002-2"

    def test_without_docs(self):
        ev = build_merger_evidence(AcquirerToken("xom", 3659), [])
        assert ev == "acquirer_security=xom#3659"


# ---------------------------------------------------------------------------
# PG 集成
# ---------------------------------------------------------------------------

pg = pytest.mark.integration


def _insert_company(pg_db, company_id, cik, name="Co"):
    with pg_db.get_session() as session:
        session.add(Company(id=company_id, cik=cik, name=name))
        session.commit()


def _insert_security(pg_db, security_id, symbol, *, company_id=None, **extra):
    row = {
        "id": security_id,
        "symbol": symbol,
        "current_symbol": symbol,
        "market": "US",
        "type": "CS",
        "is_active": extra.pop("is_active", True),
        "full_refresh_interval": 30,
        "company_id": company_id,
        **extra,
    }
    with pg_db.get_session() as session:
        session.add(Security(**row))
        session.commit()
    return security_id


def _insert_delisting(pg_db, security_id, delist_date, reason_code, evidence):
    with pg_db.engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO delisting_events (security_id, delist_date, reason_code, evidence)
                VALUES (:sid, :dd, :rc, :ev)
                """
            ),
            {"sid": security_id, "dd": delist_date, "rc": reason_code, "ev": evidence},
        )


def _scalar(pg_db, sql, **params):
    with pg_db.engine.connect() as conn:
        return conn.execute(text(sql), params).scalar()


def _all(pg_db, sql, **params):
    with pg_db.engine.connect() as conn:
        return conn.execute(text(sql), params).fetchall()


@pg
class TestUpsertCompanyEvents:
    def test_insert_and_conflict_updates_evidence(self, pg_db):
        _insert_company(pg_db, 1, "0000000001")
        _insert_company(pg_db, 2, "0000000002")
        edge = {
            "predecessor_company_id": 1,
            "successor_company_id": 2,
            "event_date": date(2020, 1, 1),
            "event_type": "MERGER",
            "evidence": "acquirer_security=xom#3659",
            "source": "DELISTING",
        }
        assert pg_db.upsert_company_events([edge]) == 1
        assert _scalar(pg_db, "SELECT count(*) FROM company_events") == 1

        # 同键重跑，evidence 刷新，行数不变
        edge2 = dict(edge, evidence="acquirer_security=xom#3659|consideration_docs=a,b")
        pg_db.upsert_company_events([edge2])
        assert _scalar(pg_db, "SELECT count(*) FROM company_events") == 1
        assert (
            _scalar(pg_db, "SELECT evidence FROM company_events")
            == "acquirer_security=xom#3659|consideration_docs=a,b"
        )

    def test_protected_id_and_created_at(self, pg_db):
        _insert_company(pg_db, 1, "0000000001")
        _insert_company(pg_db, 2, "0000000002")
        edge = {
            "predecessor_company_id": 1,
            "successor_company_id": 2,
            "event_date": date(2020, 1, 1),
            "event_type": "MERGER",
            "evidence": "v1",
        }
        pg_db.upsert_company_events([edge])
        row1 = _all(pg_db, "SELECT id, created_at, updated_at FROM company_events")[0]
        pg_db.upsert_company_events([dict(edge, evidence="v2")])
        row2 = _all(pg_db, "SELECT id, created_at, updated_at FROM company_events")[0]
        assert row1[0] == row2[0]          # id 不变
        assert row1[1] == row2[1]          # created_at 不变
        assert row2[2] >= row1[2]          # updated_at 刷新

    def test_distinct_event_type_is_separate_edge(self, pg_db):
        _insert_company(pg_db, 1, "0000000001")
        _insert_company(pg_db, 2, "0000000002")
        base = {
            "predecessor_company_id": 1,
            "successor_company_id": 2,
            "event_date": date(2020, 1, 1),
            "evidence": "x",
        }
        pg_db.upsert_company_events([dict(base, event_type="MERGER")])
        pg_db.upsert_company_events([dict(base, event_type="CIK_CHANGE")])
        assert _scalar(pg_db, "SELECT count(*) FROM company_events") == 2

    def test_rows_missing_required_fields_skipped(self, pg_db):
        _insert_company(pg_db, 1, "0000000001")
        rows = [
            {"predecessor_company_id": 1, "successor_company_id": None,
             "event_date": date(2020, 1, 1), "event_type": "MERGER"},
            {"predecessor_company_id": 1, "successor_company_id": 1,
             "event_date": None, "event_type": "MERGER"},
        ]
        assert pg_db.upsert_company_events(rows) == 0
        assert _scalar(pg_db, "SELECT count(*) FROM company_events") == 0


class _Args:
    def __init__(self, apply):
        self.apply = apply
        self.dry_run = not apply


@pg
class TestBuildScript:
    def _seed(self, pg_db):
        # 公司实体：1=被并A, 2=收购方X, 3=Alphabet；旧 Google 实体故意不建（脚本按需建）
        _insert_company(pg_db, 1, "0000000011", name="Target A")
        _insert_company(pg_db, 2, "0000000022", name="Acquirer X")
        _insert_company(pg_db, 3, "0001652044", name="Alphabet Inc.")
        # 被并证券 100 挂公司 1；收购方证券 200（symbol xom）挂公司 2
        _insert_security(pg_db, 100, "targ", company_id=1, is_active=False)
        _insert_security(pg_db, 200, "xom", company_id=2)
        # goog/googl 挂 Alphabet 公司 3
        _insert_security(pg_db, 300, "googl", company_id=3)
        _insert_security(pg_db, 301, "goog", company_id=3)
        # delisting 并购行：evidence 带收购方令牌
        _insert_delisting(
            pg_db, 100, date(2020, 6, 15), "ACQUISITION_CASH",
            "form25=z|acquirer_security=xom#200|consideration_docs=0001-1,0002-2",
        )

    def test_apply_seeds_both_edges(self, pg_db):
        self._seed(pg_db)
        rc = run(_Args(apply=True), pg_db)
        assert rc == 0

        # 边 (b)：MERGER 1 -> 2 @ 2020-06-15
        merger = _all(
            pg_db,
            "SELECT predecessor_company_id, successor_company_id, event_date, source, evidence "
            "FROM company_events WHERE event_type='MERGER'",
        )
        assert len(merger) == 1
        assert merger[0][0] == 1 and merger[0][1] == 2
        assert merger[0][2] == date(2020, 6, 15)
        assert merger[0][3] == "DELISTING"
        assert "acquirer_security=xom#200" in merger[0][4]
        assert "consideration_docs=0001-1,0002-2" in merger[0][4]

        # 边 (a)：旧 Google 公司实体被按需创建 + CIK_CHANGE -> Alphabet(3)
        old_google_id = _scalar(pg_db, "SELECT id FROM companies WHERE cik='0001288776'")
        assert old_google_id is not None
        cik_change = _all(
            pg_db,
            "SELECT predecessor_company_id, successor_company_id, event_date, event_type "
            "FROM company_events WHERE event_type='CIK_CHANGE'",
        )
        assert len(cik_change) == 1
        assert cik_change[0][0] == old_google_id
        assert cik_change[0][1] == 3
        assert cik_change[0][2] == date(2015, 10, 2)

    def test_rerun_is_noop(self, pg_db):
        self._seed(pg_db)
        run(_Args(apply=True), pg_db)
        n1 = _scalar(pg_db, "SELECT count(*) FROM company_events")
        comp1 = _scalar(pg_db, "SELECT count(*) FROM companies")
        run(_Args(apply=True), pg_db)
        n2 = _scalar(pg_db, "SELECT count(*) FROM company_events")
        comp2 = _scalar(pg_db, "SELECT count(*) FROM companies")
        assert n1 == n2 == 2          # 1 MERGER + 1 CIK_CHANGE，重跑不增
        assert comp1 == comp2         # 旧 Google 实体只建一次

    def test_dry_run_writes_nothing(self, pg_db):
        self._seed(pg_db)
        rc = run(_Args(apply=False), pg_db)
        assert rc == 0
        assert _scalar(pg_db, "SELECT count(*) FROM company_events") == 0
        # dry-run 不创建旧 Google 实体
        assert _scalar(pg_db, "SELECT count(*) FROM companies WHERE cik='0001288776'") == 0

    def test_same_company_edge_skipped(self, pg_db):
        # 被并证券与收购方证券挂同一公司实体（多类股）→ 不建边
        _insert_company(pg_db, 1, "0000000011")
        _insert_security(pg_db, 100, "targ", company_id=1, is_active=False)
        _insert_security(pg_db, 200, "targb", company_id=1)
        _insert_delisting(
            pg_db, 100, date(2020, 6, 15), "MERGER",
            "acquirer_security=targb#200",
        )
        run(_Args(apply=True), pg_db)
        assert _scalar(pg_db, "SELECT count(*) FROM company_events WHERE event_type='MERGER'") == 0

    def test_null_successor_company_skipped(self, pg_db):
        # 收购方证券的 company_id 为 NULL → 跳过
        _insert_company(pg_db, 1, "0000000011")
        _insert_security(pg_db, 100, "targ", company_id=1, is_active=False)
        _insert_security(pg_db, 200, "acq", company_id=None)
        _insert_delisting(
            pg_db, 100, date(2020, 6, 15), "MERGER",
            "acquirer_security=acq#200",
        )
        run(_Args(apply=True), pg_db)
        assert _scalar(pg_db, "SELECT count(*) FROM company_events WHERE event_type='MERGER'") == 0
