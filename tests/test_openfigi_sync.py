"""sync_openfigi_identifiers 的单元测试与 PG 集成测试。

单元：缓存 TTL 过滤、FIGI 候选裁决、share_class 回退、--limit 截断、批次失败退出码。
集成（pg_db）：候选集 SQL、husk 优先活跃、纯退市唯一命中链接、多活跃歧义跳过、
share_class 回退、MATCHED 缓存零 API 复用、负缓存重查刷新、identifiers 首插胜出
不覆盖 SEC_FTD 已有行、map_unlinked 端到端回填与跨源歧义保护。
"""
from datetime import date, datetime, timedelta, timezone
from unittest.mock import Mock

import pytest
from sqlalchemy import text

import scripts.sync_openfigi_identifiers as sync
from scripts.sync_openfigi_identifiers import (
    build_figi_maps,
    load_candidate_cusips,
    pick_security,
    resolve_links,
    select_cusips_to_query,
)

NOW = datetime(2026, 7, 2, 12, 0, tzinfo=timezone.utc)


def _result(status="MATCHED", composite_figi=None, share_class_figi=None, **overrides):
    row = {
        "status": status,
        "composite_figi": composite_figi,
        "share_class_figi": share_class_figi,
        "ticker": None,
        "name": None,
        "security_type": None,
        "market_sector": None,
        "exch_code": None,
    }
    row.update(overrides)
    return row


class FakeOpenFigiSource:
    """按预置字典应答的 OpenFigiSource 替身；errors 里的 CUSIP 所在批次抛异常。"""

    def __init__(self, results=None, batch_size=10, errors=()):
        self.results = results or {}
        self.batch_size = batch_size
        self.errors = set(errors)
        self.calls: list[list[str]] = []

    def map_cusips(self, cusips):
        self.calls.append(list(cusips))
        if self.errors & set(cusips):
            raise RuntimeError("OpenFIGI 限流(429)重试耗尽")
        return {c: dict(self.results.get(c) or _result("NOT_FOUND")) for c in cusips}


# ---------------------------------------------------------------------------
# 单元：缓存 TTL 过滤
# ---------------------------------------------------------------------------

class TestSelectCusipsToQuery:
    def test_matched_never_requeried(self):
        cache = {"037833100": ("MATCHED", NOW - timedelta(days=3650))}
        assert select_cusips_to_query(["037833100"], cache, refresh_days=90, now=NOW) == []

    def test_uncached_always_queried(self):
        assert select_cusips_to_query(["037833100"], {}, refresh_days=90, now=NOW) == ["037833100"]

    def test_fresh_negative_cache_skipped(self):
        cache = {
            "11111111A": ("NOT_FOUND", NOW - timedelta(days=89)),
            "22222222B": ("AMBIGUOUS", NOW - timedelta(days=1)),
        }
        assert select_cusips_to_query(list(cache), cache, refresh_days=90, now=NOW) == []

    def test_stale_negative_cache_requeried(self):
        cache = {
            "11111111A": ("NOT_FOUND", NOW - timedelta(days=91)),
            "22222222B": ("AMBIGUOUS", NOW - timedelta(days=180)),
        }
        assert select_cusips_to_query(
            ["11111111A", "22222222B"], cache, refresh_days=90, now=NOW
        ) == ["11111111A", "22222222B"]

    def test_candidate_order_preserved(self):
        candidates = ["33333333C", "11111111A", "22222222B"]
        cache = {"11111111A": ("MATCHED", NOW)}
        assert select_cusips_to_query(candidates, cache, refresh_days=90, now=NOW) == [
            "33333333C", "22222222B",
        ]


# ---------------------------------------------------------------------------
# 单元：FIGI 候选裁决与回退
# ---------------------------------------------------------------------------

class TestPickSecurity:
    def test_single_active(self):
        assert pick_security([(1, True)]) == (1, "linked_active")

    def test_husk_prefers_active_over_inactive(self):
        # 身份合并后 husk（inactive）保留 FIGI 与承接方共存：必须链到活跃方
        assert pick_security([(1, False), (2, True)]) == (2, "linked_active")

    def test_multiple_active_ambiguous(self):
        assert pick_security([(1, True), (2, True)]) == (None, "multiple_active")

    def test_sole_inactive_linked(self):
        # 退市股持仓历史同样有研究价值
        assert pick_security([(3, False)]) == (3, "linked_inactive")

    def test_multiple_inactive_without_active_ambiguous(self):
        assert pick_security([(1, False), (2, False)]) == (None, "multiple_inactive")


class TestResolveLinks:
    def test_composite_hit_wins_no_fallback(self):
        by_composite, by_share_class = build_figi_maps(
            [(1, "BBG000AAAAA1", None, True), (9, None, "BBG000SCSCS1", True)]
        )
        links, outcomes = resolve_links(
            [("037833100", "BBG000AAAAA1", "BBG000SCSCS1")], by_composite, by_share_class
        )
        assert links == {"037833100": 1}
        assert outcomes["linked_active"] == 1

    def test_share_class_fallback_when_composite_misses(self):
        by_composite, by_share_class = build_figi_maps([(7, "BBG000OTHER9", "BBG000SCSCS1", True)])
        links, _ = resolve_links(
            [("037833100", "BBG000AAAAA1", "BBG000SCSCS1")], by_composite, by_share_class
        )
        assert links == {"037833100": 7}

    def test_no_hit_anywhere_unlinked(self):
        links, outcomes = resolve_links([("037833100", "BBG000AAAAA1", None)], {}, {})
        assert links == {}
        assert outcomes["no_security_match"] == 1

    def test_ambiguous_composite_skipped_not_fallback(self):
        # composite 有命中（虽歧义）就不回退 share_class——回退只针对"无命中"
        by_composite, by_share_class = build_figi_maps(
            [(1, "BBG000AAAAA1", None, True), (2, "BBG000AAAAA1", None, True),
             (3, None, "BBG000SCSCS1", True)]
        )
        links, outcomes = resolve_links(
            [("037833100", "BBG000AAAAA1", "BBG000SCSCS1")], by_composite, by_share_class
        )
        assert links == {}
        assert outcomes["multiple_active"] == 1


# ---------------------------------------------------------------------------
# 单元：run() 编排（mock 掉 SQL 加载与 db 写入）
# ---------------------------------------------------------------------------

def _mock_db():
    db = Mock()
    db.upsert_openfigi_lookups.return_value = 0
    db.insert_missing_security_identifiers.return_value = 0
    db.map_unlinked_holdings_to_securities.return_value = 0
    return db


def _patch_loaders(monkeypatch, candidates, cache=None, matched=None, securities=None):
    monkeypatch.setattr(sync, "load_candidate_cusips", lambda db: list(candidates))
    monkeypatch.setattr(sync, "load_lookup_cache", lambda db, cusips: dict(cache or {}))
    monkeypatch.setattr(sync, "load_matched_lookups", lambda db: list(matched or []))
    monkeypatch.setattr(sync, "load_securities_figi", lambda db: list(securities or []))


class TestRunOrchestration:
    def test_limit_truncates_api_queries(self, monkeypatch):
        candidates = ["11111111A", "22222222B", "33333333C"]
        _patch_loaders(monkeypatch, candidates)
        source = FakeOpenFigiSource()
        db = _mock_db()

        args = sync.create_parser().parse_args(["--limit", "2"])
        exit_code, stats = sync.run(args, source, db)

        assert exit_code == 0
        assert source.calls == [["11111111A", "22222222B"]]
        assert stats["queried"] == 2
        assert stats["candidates"] == 3

    def test_batches_follow_source_batch_size(self, monkeypatch):
        candidates = ["11111111A", "22222222B", "33333333C"]
        _patch_loaders(monkeypatch, candidates)
        source = FakeOpenFigiSource(batch_size=2)
        db = _mock_db()

        exit_code, _ = sync.run(sync.create_parser().parse_args([]), source, db)

        assert exit_code == 0
        assert source.calls == [["11111111A", "22222222B"], ["33333333C"]]
        assert db.upsert_openfigi_lookups.call_count == 2  # 逐批落缓存

    def test_all_batches_failed_exit_1_but_resolution_still_runs(self, monkeypatch):
        _patch_loaders(
            monkeypatch,
            ["11111111A", "22222222B"],
            matched=[("99999999Z", "BBG000AAAAA1", None)],  # 旧缓存仍可落链
            securities=[(5, "BBG000AAAAA1", None, True)],
        )
        source = FakeOpenFigiSource(batch_size=1, errors={"11111111A", "22222222B"})
        db = _mock_db()
        db.insert_missing_security_identifiers.return_value = 1

        exit_code, stats = sync.run(sync.create_parser().parse_args([]), source, db)

        assert exit_code == 1
        assert stats["failed_batches"] == 2
        assert stats["queried"] == 0
        assert stats["linked"] == 1  # 解析阶段不受查询失败影响
        assert db.insert_missing_security_identifiers.call_args.args[0][0]["security_id"] == 5

    def test_partial_batch_failure_keeps_results_exit_0(self, monkeypatch):
        _patch_loaders(monkeypatch, ["11111111A", "22222222B"])
        source = FakeOpenFigiSource(
            results={"22222222B": _result("MATCHED", composite_figi="BBG000AAAAA1")},
            batch_size=1,
            errors={"11111111A"},
        )
        db = _mock_db()

        exit_code, stats = sync.run(sync.create_parser().parse_args([]), source, db)

        assert exit_code == 0
        assert stats["failed_batches"] == 1
        assert stats["queried"] == 1
        assert stats["matched"] == 1
        db.upsert_openfigi_lookups.assert_called_once()  # 成功批已落缓存

    def test_nothing_to_query_exit_0(self, monkeypatch):
        _patch_loaders(monkeypatch, [], cache={})
        source = FakeOpenFigiSource()
        db = _mock_db()

        exit_code, stats = sync.run(sync.create_parser().parse_args([]), source, db)

        assert exit_code == 0
        assert source.calls == []
        assert stats == {
            "candidates": 0, "queried": 0, "matched": 0, "not_found": 0,
            "ambiguous": 0, "failed_batches": 0, "linked": 0,
            "identifiers_inserted": 0, "holdings_backfilled": 0,
        }


# ---------------------------------------------------------------------------
# PG 集成
# ---------------------------------------------------------------------------

@pytest.mark.integration
class TestOpenFigiSyncIntegration:
    @staticmethod
    def _add_security(pg_db, security_id, symbol, composite_figi=None,
                      share_class_figi=None, is_active=True):
        from data_models.models import Security

        with pg_db.get_session() as session:
            session.add(Security(
                id=security_id,
                symbol=symbol,
                current_symbol=symbol,
                market="US",
                type="CS",
                is_active=is_active,
                composite_figi=composite_figi,
                share_class_figi=share_class_figi,
            ))
            session.commit()

    @staticmethod
    def _add_holding(pg_db, cusip, accession, security_id=None):
        from data_models.models import InstitutionalHolding

        with pg_db.get_session() as session:
            session.add(InstitutionalHolding(
                source="SEC_EDGAR",
                accession_number=accession,
                source_row_hash=f"{accession}:0",
                filer_cik="0001",
                cusip=cusip,
                security_id=security_id,
            ))
            session.commit()

    @staticmethod
    def _identifier_rows(pg_db, id_value=None):
        sql = "SELECT security_id, id_type, id_value, source, confidence, start_date FROM security_identifiers"
        params = {}
        if id_value:
            sql += " WHERE id_value = :id_value"
            params["id_value"] = id_value
        with pg_db.engine.connect() as conn:
            return conn.execute(text(sql), params).all()

    @staticmethod
    def _holding_security_id(pg_db, accession):
        with pg_db.engine.connect() as conn:
            return conn.execute(
                text("SELECT security_id FROM institutional_holdings WHERE accession_number = :a"),
                {"a": accession},
            ).scalar()

    def _run(self, pg_db, source, argv=None):
        return sync.run(sync.create_parser().parse_args(argv or []), source, pg_db)

    def test_candidate_selection_sql(self, pg_db):
        self._add_security(pg_db, 1, "aapl")
        self._add_holding(pg_db, "037833100", "A1")            # 未链接 -> 候选
        self._add_holding(pg_db, " 037833100 ", "A2")          # 同 CUSIP 带空白 -> 去重
        self._add_holding(pg_db, "36467w109", "A3")            # 小写 -> upper
        self._add_holding(pg_db, "88160r101", "A4", security_id=1)  # 已链接 -> 排除
        self._add_holding(pg_db, None, "A5")                   # 空 -> 排除
        self._add_holding(pg_db, "SHORT", "A6")                # 非 9 位 -> 排除

        assert load_candidate_cusips(pg_db) == ["037833100", "36467W109"]

    def test_husk_prefers_active_end_to_end(self, pg_db):
        # 同 FIGI 一活跃一退市（合并后 husk）：必须链到活跃行，并端到端回填 holdings
        self._add_security(pg_db, 1, "new", composite_figi="BBG000AAAAA1", is_active=True)
        self._add_security(pg_db, 2, "husk", composite_figi="BBG000AAAAA1", is_active=False)
        self._add_holding(pg_db, "037833100", "A1")
        source = FakeOpenFigiSource(
            results={"037833100": _result("MATCHED", composite_figi="BBG000AAAAA1")}
        )

        exit_code, stats = self._run(pg_db, source)

        assert exit_code == 0
        assert source.calls == [["037833100"]]
        rows = self._identifier_rows(pg_db, "037833100")
        assert len(rows) == 1
        assert (rows[0].security_id, rows[0].source, rows[0].confidence, rows[0].start_date) == (
            1, "OPENFIGI", "openfigi_figi_match", None,
        )
        assert self._holding_security_id(pg_db, "A1") == 1
        assert stats["matched"] == 1
        assert stats["linked"] == 1
        assert stats["identifiers_inserted"] == 1
        assert stats["holdings_backfilled"] == 1

    def test_sole_inactive_unique_hit_linked(self, pg_db):
        self._add_security(pg_db, 3, "gone", composite_figi="BBG000BBBBB2", is_active=False)
        self._add_holding(pg_db, "12345678A", "B1")
        source = FakeOpenFigiSource(
            results={"12345678A": _result("MATCHED", composite_figi="BBG000BBBBB2")}
        )

        exit_code, stats = self._run(pg_db, source)

        assert exit_code == 0
        rows = self._identifier_rows(pg_db, "12345678A")
        assert [(r.security_id, r.source) for r in rows] == [(3, "OPENFIGI")]
        assert self._holding_security_id(pg_db, "B1") == 3
        assert stats["linked"] == 1

    def test_multiple_active_same_figi_skipped(self, pg_db):
        self._add_security(pg_db, 1, "twin1", composite_figi="BBG000CCCCC3", is_active=True)
        self._add_security(pg_db, 2, "twin2", composite_figi="BBG000CCCCC3", is_active=True)
        self._add_holding(pg_db, "12345678A", "C1")
        source = FakeOpenFigiSource(
            results={"12345678A": _result("MATCHED", composite_figi="BBG000CCCCC3")}
        )

        exit_code, stats = self._run(pg_db, source)

        assert exit_code == 0
        assert self._identifier_rows(pg_db) == []
        assert self._holding_security_id(pg_db, "C1") is None
        assert stats["linked"] == 0

    def test_share_class_figi_fallback(self, pg_db):
        # composite 无命中（库内该证券 composite 缺失），share_class 唯一命中 -> 链接
        self._add_security(pg_db, 4, "sc", composite_figi=None,
                           share_class_figi="BBG000SCSCS1", is_active=True)
        self._add_holding(pg_db, "12345678A", "D1")
        source = FakeOpenFigiSource(
            results={"12345678A": _result(
                "MATCHED", composite_figi="BBG000NOHIT9", share_class_figi="BBG000SCSCS1",
            )}
        )

        exit_code, stats = self._run(pg_db, source)

        assert exit_code == 0
        rows = self._identifier_rows(pg_db, "12345678A")
        assert [(r.security_id, r.source) for r in rows] == [(4, "OPENFIGI")]
        assert self._holding_security_id(pg_db, "D1") == 4

    def test_matched_cache_reused_without_api(self, pg_db):
        # 缓存已 MATCHED：一次 API 都不调，仅靠解析阶段落链（新上市证券命中旧缓存）
        pg_db.upsert_openfigi_lookups([
            {"cusip": "037833100", **_result("MATCHED", composite_figi="BBG000AAAAA1")}
        ])
        self._add_security(pg_db, 1, "late", composite_figi="BBG000AAAAA1", is_active=True)
        self._add_holding(pg_db, "037833100", "E1")
        source = FakeOpenFigiSource()

        exit_code, stats = self._run(pg_db, source)

        assert exit_code == 0
        assert source.calls == []  # MATCHED 永不重查
        assert stats["queried"] == 0
        assert stats["linked"] == 1
        assert self._holding_security_id(pg_db, "E1") == 1

    def test_negative_cache_ttl_and_requery_refresh(self, pg_db):
        # 过期 NOT_FOUND 重查并升级为 MATCHED；新鲜 NOT_FOUND 不重查
        pg_db.upsert_openfigi_lookups([
            {"cusip": "11111111A", **_result("NOT_FOUND")},
            {"cusip": "22222222B", **_result("NOT_FOUND")},
        ])
        with pg_db.engine.connect() as conn:
            conn.execute(text(
                "UPDATE openfigi_cusip_lookups SET queried_at = now() - interval '120 days' "
                "WHERE cusip = '11111111A'"
            ))
            conn.commit()
        self._add_security(pg_db, 1, "reborn", composite_figi="BBG000AAAAA1", is_active=True)
        self._add_holding(pg_db, "11111111A", "F1")
        self._add_holding(pg_db, "22222222B", "F2")
        source = FakeOpenFigiSource(
            results={"11111111A": _result("MATCHED", composite_figi="BBG000AAAAA1")}
        )

        exit_code, stats = self._run(pg_db, source, ["--refresh-days", "90"])

        assert exit_code == 0
        assert source.calls == [["11111111A"]]
        with pg_db.engine.connect() as conn:
            row = conn.execute(text(
                "SELECT status, composite_figi, queried_at FROM openfigi_cusip_lookups "
                "WHERE cusip = '11111111A'"
            )).one()
        assert row.status == "MATCHED"
        assert row.composite_figi == "BBG000AAAAA1"
        # queried_at 已被 ON CONFLICT 显式刷新（不再是 120 天前）
        assert row.queried_at > datetime.now(timezone.utc) - timedelta(days=1)
        assert self._holding_security_id(pg_db, "F1") == 1
        assert stats == {
            "candidates": 2, "queried": 1, "matched": 1, "not_found": 0,
            "ambiguous": 0, "failed_batches": 0, "linked": 1,
            "identifiers_inserted": 1, "holdings_backfilled": 1,
        }

    def test_upsert_full_column_overwrite_on_status_transition(self, pg_db):
        # MATCHED -> NOT_FOUND 重查必须清掉旧 figi 字段，不留脏残影
        pg_db.upsert_openfigi_lookups([
            {"cusip": "33333333C", **_result(
                "MATCHED", composite_figi="BBG000AAAAA1", share_class_figi="BBG000SCSCS1",
                ticker="AAPL", name="APPLE INC",
            )}
        ])
        written = pg_db.upsert_openfigi_lookups([{"cusip": "33333333C", "status": "NOT_FOUND"}])

        assert written == 1
        with pg_db.engine.connect() as conn:
            row = conn.execute(text(
                "SELECT status, composite_figi, share_class_figi, ticker, name "
                "FROM openfigi_cusip_lookups WHERE cusip = '33333333C'"
            )).one()
        assert row.status == "NOT_FOUND"
        assert row.composite_figi is None
        assert row.share_class_figi is None
        assert row.ticker is None
        assert row.name is None

    def test_first_insert_wins_ftd_row_untouched_and_idempotent(self, pg_db):
        from data_models.models import SecurityIdentifier

        self._add_security(pg_db, 1, "aapl", composite_figi="BBG000AAAAA1", is_active=True)
        with pg_db.get_session() as session:
            session.add(SecurityIdentifier(
                security_id=1, id_type="CUSIP", id_value="037833100",
                source="SEC_FTD", confidence="ftd_symbol_match", start_date=date(2026, 1, 1),
            ))
            session.commit()
        self._add_holding(pg_db, "037833100", "G1")
        source = FakeOpenFigiSource(
            results={"037833100": _result("MATCHED", composite_figi="BBG000AAAAA1")}
        )

        _, stats_first = self._run(pg_db, source)
        _, stats_second = self._run(pg_db, source)

        rows = sorted(self._identifier_rows(pg_db, "037833100"), key=lambda r: r.source)
        assert [(r.security_id, r.source, r.confidence, r.start_date) for r in rows] == [
            (1, "OPENFIGI", "openfigi_figi_match", None),      # OPENFIGI 补一行
            (1, "SEC_FTD", "ftd_symbol_match", date(2026, 1, 1)),  # FTD 行原样保留
        ]
        assert stats_first["identifiers_inserted"] == 1
        assert stats_second["identifiers_inserted"] == 0  # 幂等：首插胜出，重跑不加行
        assert self._holding_security_id(pg_db, "G1") == 1

    def test_cross_source_disagreement_blocks_backfill(self, pg_db):
        # FTD 与 OPENFIGI 对同一 CUSIP 指向不同 security：holdings 回填的
        # HAVING count(DISTINCT security_id)=1 守卫必须拦下，不许错链
        from data_models.models import SecurityIdentifier

        self._add_security(pg_db, 1, "ftdsec")
        self._add_security(pg_db, 2, "figisec", composite_figi="BBG000AAAAA1", is_active=True)
        with pg_db.get_session() as session:
            session.add(SecurityIdentifier(
                security_id=1, id_type="CUSIP", id_value="037833100",
                source="SEC_FTD", confidence="ftd_symbol_match",
            ))
            session.commit()
        self._add_holding(pg_db, "037833100", "H1")
        source = FakeOpenFigiSource(
            results={"037833100": _result("MATCHED", composite_figi="BBG000AAAAA1")}
        )

        _, stats = self._run(pg_db, source)

        assert stats["identifiers_inserted"] == 1  # OPENFIGI 行照写（供 audit 对账）
        assert stats["holdings_backfilled"] == 0   # 但跨源歧义拦住回填
        assert self._holding_security_id(pg_db, "H1") is None


@pytest.mark.integration
def test_upsert_clamps_overlong_diagnostic_fields(pg_db):
    """债券/期权 CUSIP 的 ticker 超 varchar(20)（如 'AAPL 3.35 02/09/27 CALL'）：
    必须截断入库而不是让整批报 StringDataRightTruncation——2026-07-02 生产首跑实故。"""
    written = pg_db.upsert_openfigi_lookups([{
        "cusip": "037833AK6",
        "status": "MATCHED",
        "composite_figi": "BBG00GBVBK51",
        "ticker": "AAPL 3.35 02/09/27 CORP BOND",
        "name": "X" * 300,
        "security_type": "Y" * 80,
        "exch_code": "Z" * 15,
    }])
    assert written == 1
    from sqlalchemy import text
    with pg_db.engine.connect() as conn:
        row = conn.execute(text(
            "SELECT ticker, length(name) AS ln, length(security_type) AS lt, exch_code "
            "FROM openfigi_cusip_lookups WHERE cusip = '037833AK6'"
        )).fetchone()
    assert row.ticker == "AAPL 3.35 02/09/27 C"  # 截到 20
    assert row.ln == 255 and row.lt == 60
    assert row.exch_code == "Z" * 10
