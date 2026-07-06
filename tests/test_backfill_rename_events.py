"""backfill_rename_events 的单元与集成测试。

单元测试用合成的 parquet 行/证券快照锁定：epoch 丢弃、解析优先级（figi->cik、
唯一命中要求、歧义桶）、confidence 分级、live 胜（尾部不符/日期冲突/既有事件）、
退市任期补链、FIGI 补链护栏、PIT 物化跨 source 去重。
集成测试（pg fixture）锁定 --apply 的写入幂等与 dry-run 零写入。
"""
import json
from datetime import date

import pandas as pd
import pytest
from sqlalchemy import text

from data_models.models import Security

import scripts.backfill_rename_events as bre
from scripts.backfill_rename_events import (
    ARCHIVE_SOURCE,
    EPOCH_SENTINEL,
    ArchiveGroup,
    EntityEvidence,
    SecurityInfo,
    build_cik_figi_map,
    build_groups,
    build_security_indexes,
    create_parser,
    drop_epoch_rows,
    load_archive_rows,
    merge_by_security,
    plan_figi_fills,
    plan_identifier_rows,
    plan_renames,
    resolve_group,
    run,
)


def _sec(security_id, symbol, *, active=True, cik=None, figi=None,
         current=None, delist=None) -> SecurityInfo:
    return SecurityInfo(
        id=security_id,
        symbol=symbol,
        current_symbol=current or symbol,
        cik=cik,
        figi=figi,
        is_active=active,
        delist_date=delist,
    )


def _row(queried, ticker, event_date, *, cik=None, figi=None) -> dict:
    return {
        "queried_ticker": queried,
        "cik": cik,
        "figi": figi,
        "event_date": event_date,
        "ticker": ticker,
    }


def _entity(sec, events, *, anchors=("figi", "cik")) -> dict:
    ent = EntityEvidence(security=sec)
    ent.anchors = set(anchors)
    ent.queried_tickers = [sec.symbol]
    ent.events = events
    return {sec.id: ent}


# --------------------------------------------------------------------------- #
# 归档解析
# --------------------------------------------------------------------------- #

PARQUET_COLUMNS = ["cik", "composite_figi", "queried_ticker", "event_date",
                   "event_type", "ticker_at_event", "event_payload"]


def _write_parquet(path, records) -> str:
    frame = pd.DataFrame(records, columns=PARQUET_COLUMNS)
    frame.to_parquet(path)
    return str(path)


def _parquet_record(queried, ticker, event_date, *, cik=None, figi=None,
                    event_type="ticker_change", ticker_at_event=...):
    payload = json.dumps({"ticker_change": {"ticker": ticker}, "type": event_type,
                          "date": event_date.isoformat() if event_date else None})
    return {
        "cik": cik,
        "composite_figi": figi,
        "queried_ticker": queried,
        "event_date": event_date,
        "event_type": event_type,
        "ticker_at_event": ticker if ticker_at_event is ... else ticker_at_event,
        "event_payload": payload,
    }


class TestLoadArchiveRows:
    def test_normalizes_and_drops_invalid(self, tmp_path):
        path = _write_parquet(tmp_path / "events.parquet", [
            _parquet_record("AA", "AA", date(2016, 10, 18), cik="0000000123", figi="bbg00b3t3hd3"),
            _parquet_record("AA", "AA", date(2015, 1, 1), event_type="split"),      # 非 ticker_change
            _parquet_record("BB", "BB", None, cik="0000000456"),                     # 无日期
            _parquet_record("CC", "CC", EPOCH_SENTINEL, cik="0000000789", figi="BBG000EPOCH1"),
            _parquet_record("DD", "DD", date(2020, 2, 2), ticker_at_event=None),     # payload 兜底
        ])
        rows, counts = load_archive_rows(path)

        assert counts["parquet_rows_total"] == 5
        assert counts["non_ticker_change_dropped"] == 1
        assert counts["invalid_dropped"] == 1
        assert len(rows) == 3  # epoch 行由 loader 保留（figi 阶段还要用），丢弃归 drop_epoch_rows

        first = rows[0]
        assert first["ticker"] == "aa"                 # 小写归一
        assert first["queried_ticker"] == "aa"
        assert first["cik"] == "123"                   # 去前导零
        assert first["figi"] == "BBG00B3T3HD3"         # 大写归一
        fallback = [r for r in rows if r["queried_ticker"] == "dd"]
        assert fallback and fallback[0]["ticker"] == "dd"

    def test_drop_epoch_rows(self):
        rows = [
            _row("aa", "aa", date(2016, 10, 18)),
            _row("bb", "bb", EPOCH_SENTINEL),
            _row("cc", "cc", EPOCH_SENTINEL),
        ]
        kept, dropped = drop_epoch_rows(rows)
        assert dropped == 2
        assert [r["queried_ticker"] for r in kept] == ["aa"]


class TestBuildGroups:
    def test_sorts_dedupes_and_collapses(self):
        rows = [
            _row("meta", "meta", date(2022, 6, 9), cik="1326801", figi="BBG000MM9KP9"),
            _row("meta", "fb", date(2012, 5, 18), cik="1326801", figi="BBG000MM9KP9"),
            _row("meta", "fb", date(2012, 5, 18), cik="1326801", figi="BBG000MM9KP9"),   # 重复行
            _row("meta", "meta", date(2023, 1, 1), cik="1326801", figi="BBG000MM9KP9"),  # 连续同 ticker
        ]
        groups = build_groups(rows)
        assert len(groups) == 1
        group = groups[0]
        assert group.cik == "1326801"
        assert group.figi == "BBG000MM9KP9"
        assert group.events == [(date(2012, 5, 18), "fb"), (date(2022, 6, 9), "meta")]
        assert group.same_date_conflict is False

    def test_flags_same_date_conflict(self):
        rows = [
            _row("xx", "aa", date(2020, 1, 1)),
            _row("xx", "bb", date(2020, 1, 1)),
        ]
        groups = build_groups(rows)
        assert groups[0].same_date_conflict is True


# --------------------------------------------------------------------------- #
# 解析优先级
# --------------------------------------------------------------------------- #

class TestResolveGroup:
    S_FIGI_CIK = _sec(1, "aaa", cik="111", figi="BBG000000001")
    S_OTHER = _sec(2, "bbb", cik="222", figi="BBG000000002")

    def _indexes(self, securities):
        return build_security_indexes(securities)

    def _group(self, *, cik=None, figi=None):
        return ArchiveGroup("q", cik, figi, [(date(2020, 1, 1), "q")])

    def test_both_anchors_agree(self):
        by_figi, by_cik = self._indexes([self.S_FIGI_CIK, self.S_OTHER])
        res = resolve_group(self._group(cik="111", figi="BBG000000001"), by_figi, by_cik)
        assert res.security.id == 1
        assert set(res.anchors) == {"figi", "cik"}

    def test_anchor_conflict_reported(self):
        by_figi, by_cik = self._indexes([self.S_FIGI_CIK, self.S_OTHER])
        res = resolve_group(self._group(cik="222", figi="BBG000000001"), by_figi, by_cik)
        assert res.security is None
        assert res.bucket == "anchor_conflict"

    def test_figi_ambiguous_reported_even_with_unique_cik(self):
        dup1 = _sec(3, "ccc", cik="333", figi="BBG000000DUP")
        dup2 = _sec(4, "ddd", cik="444", figi="BBG000000DUP")
        by_figi, by_cik = self._indexes([dup1, dup2])
        res = resolve_group(self._group(cik="333", figi="BBG000000DUP"), by_figi, by_cik)
        assert res.security is None
        assert res.bucket == "figi_ambiguous"

    def test_cik_fallback_when_figi_absent_or_unknown(self):
        by_figi, by_cik = self._indexes([self.S_FIGI_CIK])
        res = resolve_group(self._group(cik="111"), by_figi, by_cik)
        assert res.security.id == 1 and set(res.anchors) == {"cik"}
        # figi 在库内无命中时同样回退 cik
        res = resolve_group(self._group(cik="111", figi="BBG000UNKNOWN"), by_figi, by_cik)
        assert res.security.id == 1 and set(res.anchors) == {"cik"}

    def test_cik_ambiguous_reported(self):
        share_a = _sec(5, "gooa", cik="555")
        share_b = _sec(6, "goob", cik="555")
        by_figi, by_cik = self._indexes([share_a, share_b])
        res = resolve_group(self._group(cik="555"), by_figi, by_cik)
        assert res.security is None
        assert res.bucket == "cik_ambiguous"

    def test_figi_unique_with_multi_cik_containing_it_stays_single_anchor(self):
        share_a = _sec(5, "gooa", cik="555", figi="BBG000000005")
        share_b = _sec(6, "goob", cik="555", figi="BBG000000006")
        by_figi, by_cik = self._indexes([share_a, share_b])
        res = resolve_group(self._group(cik="555", figi="BBG000000005"), by_figi, by_cik)
        assert res.security.id == 5
        assert set(res.anchors) == {"figi"}   # cik 未唯一命中，不算双锚

    def test_unresolved(self):
        by_figi, by_cik = self._indexes([self.S_FIGI_CIK])
        res = resolve_group(self._group(cik="999", figi="BBG000UNKNOWN"), by_figi, by_cik)
        assert res.security is None
        assert res.bucket == "unresolved"


# --------------------------------------------------------------------------- #
# RENAME 计划
# --------------------------------------------------------------------------- #

class TestPlanRenames:
    def test_high_confidence_needs_both_anchors_and_corroboration(self):
        sec = _sec(1, "new", cik="111", figi="BBG000000001")
        entities = _entity(sec, [(date(2010, 1, 4), "old"), (date(2024, 5, 1), "new")])
        plan = plan_renames(entities, live_history={}, existing_rename_keys={})
        assert len(plan.events) == 1
        event = plan.events[0]
        assert (event["old_symbol"], event["new_symbol"]) == ("old", "new")
        assert event["confidence"] == "HIGH"       # 双锚 + new==symbol 佐证
        assert event["resolution_source"] == "AUDIT"
        details = json.loads(event["details"])
        assert details["event_date"] == "2024-05-01"
        assert details["source"] == ARCHIVE_SOURCE

    def test_single_anchor_is_medium(self):
        sec = _sec(1, "new", cik="111")
        entities = _entity(sec, [(date(2010, 1, 4), "old"), (date(2024, 5, 1), "new")],
                           anchors=("cik",))
        plan = plan_renames(entities, {}, {})
        assert plan.events[0]["confidence"] == "MEDIUM"

    def test_tail_mismatch_reports_never_writes(self):
        # vendor 杂音样本：Agilent 的链尾是 awd，但库内 symbol 是 a —— live 胜
        sec = _sec(1, "a", cik="1090872", figi="BBG000C2V3D6")
        entities = _entity(sec, [(date(2003, 9, 10), "a"), (date(2005, 11, 23), "awd")])
        plan = plan_renames(entities, {}, {})
        assert plan.events == [] and plan.tenures == []
        assert len(plan.buckets["tail_mismatch"]) == 1
        assert plan.counts["entities_tail_mismatch"] == 1

    def test_live_date_conflict_quarantines_whole_entity(self):
        sec = _sec(1, "new", cik="111", figi="BBG000000001", active=False,
                   delist=date(2025, 1, 1))
        entities = _entity(sec, [(date(2010, 1, 4), "old"), (date(2024, 5, 1), "new")])
        live_history = {1: {"new": {date(2024, 6, 15)}}}   # live 起始日与归档不符
        plan = plan_renames(entities, live_history, {})
        assert plan.events == [] and plan.tenures == []
        assert len(plan.buckets["live_date_conflict"]) == 1

    def test_live_date_match_corroborates(self):
        sec = _sec(1, "new", cik="111", figi="BBG000000001")
        entities = _entity(sec, [(date(2010, 1, 4), "old"), (date(2024, 5, 1), "new")])
        live_history = {1: {"new": {date(2024, 5, 1)}}}
        plan = plan_renames(entities, live_history, {})
        assert len(plan.events) == 1 and plan.events[0]["confidence"] == "HIGH"

    def test_existing_event_skipped_by_symbols_and_date(self):
        sec = _sec(1, "new", cik="111", figi="BBG000000001")
        events = [(date(2010, 1, 4), "old"), (date(2024, 5, 1), "new")]
        # 同日期已存在 -> 跳过
        plan = plan_renames(_entity(sec, events), {},
                            {(1, "old", "new"): {date(2024, 5, 1)}})
        assert plan.events == [] and plan.counts["events_already_exist"] == 1
        # 旧 live 事件无日期（details 无 event_date）-> 视为已覆盖，跳过
        plan = plan_renames(_entity(sec, events), {}, {(1, "old", "new"): {None}})
        assert plan.events == [] and plan.counts["events_already_exist"] == 1
        # 已有事件日期不同 -> live 胜，只报告
        plan = plan_renames(_entity(sec, events), {},
                            {(1, "old", "new"): {date(2024, 6, 1)}})
        assert plan.events == []
        assert len(plan.buckets["event_date_mismatch"]) == 1

    def test_tenures_only_for_delisted_and_live_covered_skip(self):
        active = _sec(1, "new", cik="111", figi="BBG000000001")
        dead = _sec(2, "dead", cik="222", active=False, delist=date(2020, 6, 30))
        entities = {
            **_entity(active, [(date(2010, 1, 4), "old"), (date(2024, 5, 1), "new")]),
            **_entity(dead, [(date(2005, 3, 15), "oldd"), (date(2012, 9, 10), "dead")],
                      anchors=("cik",)),
        }
        live_history = {2: {"oldd": {None}}}   # oldd 已有 live 行 -> 跳过，dead 补
        plan = plan_renames(entities, live_history, {})
        assert plan.counts["tenures_skipped_live_covered"] == 1
        assert len(plan.tenures) == 1
        tenure = plan.tenures[0]
        assert tenure["security_id"] == 2 and tenure["symbol"] == "dead"
        assert tenure["source"] == ARCHIVE_SOURCE
        assert tenure["start_date"] == date(2012, 9, 10)
        assert tenure["end_date"] == date(2020, 6, 30)   # 末段任期用 delist_date 闭合
        # active 证券不补任期
        assert all(t["security_id"] != 1 for t in plan.tenures)

    def test_intermediate_tenure_closed_by_next_event(self):
        dead = _sec(2, "dead", cik="222", active=False, delist=date(2020, 6, 30))
        entities = _entity(dead, [(date(2005, 3, 15), "oldd"), (date(2012, 9, 10), "dead")],
                           anchors=("cik",))
        plan = plan_renames(entities, {}, {})
        by_symbol = {t["symbol"]: t for t in plan.tenures}
        assert by_symbol["oldd"]["end_date"] == date(2012, 9, 10)
        assert by_symbol["dead"]["end_date"] == date(2020, 6, 30)

    def test_same_date_conflict_quarantined(self):
        sec = _sec(1, "bb", cik="111", figi="BBG000000001")
        entities = _entity(sec, [(date(2020, 1, 1), "aa"), (date(2020, 1, 1), "bb")])
        entities[1].same_date_conflict = True
        plan = plan_renames(entities, {}, {})
        assert plan.events == [] and plan.tenures == []
        assert len(plan.buckets["same_date_conflict"]) == 1


class TestMergeBySecurity:
    def test_groups_resolving_to_same_security_merge_evidence(self):
        sec = _sec(1, "new", cik="111", figi="BBG000000001")
        g_old = ArchiveGroup("old", "111", None, [(date(2010, 1, 4), "old")])
        g_new = ArchiveGroup("new", None, "BBG000000001",
                             [(date(2010, 1, 4), "old"), (date(2024, 5, 1), "new")])
        res = [
            bre.GroupResolution(g_old, security=sec, anchors=frozenset({"cik"})),
            bre.GroupResolution(g_new, security=sec, anchors=frozenset({"figi"})),
        ]
        entities = merge_by_security(res)
        ent = entities[1]
        assert ent.anchors == {"figi", "cik"}   # 两组分别以不同锚命中同一证券 -> 双锚
        assert ent.events == [(date(2010, 1, 4), "old"), (date(2024, 5, 1), "new")]
        assert sorted(ent.queried_tickers) == ["new", "old"]


# --------------------------------------------------------------------------- #
# FIGI 补链
# --------------------------------------------------------------------------- #

class TestPlanFigiFills:
    def test_unique_cik_match_fills(self):
        mapping = build_cik_figi_map([
            _row("dead", "dead", EPOCH_SENTINEL, cik="222", figi="BBG000000002"),
        ])
        dead = _sec(2, "dead", cik="222", active=False)
        plan = plan_figi_fills(mapping, [dead])
        assert plan.fills == [(2, "BBG000000002")]

    def test_only_inactive_null_figi_with_cik_are_candidates(self):
        mapping = {"111": {"BBG000000001"}, "222": {"BBG000000002"}}
        active = _sec(1, "aaa", cik="111", active=True)
        has_figi = _sec(2, "bbb", cik="222", figi="BBG000EXIST2", active=False)
        no_cik = _sec(3, "ccc", active=False)
        plan = plan_figi_fills(mapping, [active, has_figi, no_cik])
        assert plan.fills == []
        assert plan.counts["candidates"] == 0

    def test_parquet_multi_figi_reported(self):
        mapping = {"222": {"BBG000000002", "BBG000000003"}}
        plan = plan_figi_fills(mapping, [_sec(2, "dead", cik="222", active=False)])
        assert plan.fills == []
        assert len(plan.buckets["parquet_multi_figi"]) == 1

    def test_db_multi_security_cik_reported(self):
        mapping = {"555": {"BBG000000005"}}
        dead = _sec(5, "gooa", cik="555", active=False)
        peer = _sec(6, "goob", cik="555", figi="BBG000000006", active=False)
        plan = plan_figi_fills(mapping, [dead, peer])
        assert plan.fills == []
        assert len(plan.buckets["db_multi_security_cik"]) == 1

    def test_figi_already_held_reported(self):
        mapping = {"444": {"BBG000EXIST8"}}
        holder = _sec(8, "bb", cik="333", figi="BBG000EXIST8", active=False)
        dead = _sec(9, "cc", cik="444", active=False)
        plan = plan_figi_fills(mapping, [holder, dead])
        assert plan.fills == []
        assert len(plan.buckets["figi_already_held"]) == 1
        assert plan.buckets["figi_already_held"][0]["held_by"] == [8]

    def test_dup_figi_in_batch_reported(self):
        mapping = {"666": {"BBG000000DUP"}, "777": {"BBG000000DUP"}}
        a = _sec(10, "xx", cik="666", active=False)
        b = _sec(11, "yy", cik="777", active=False)
        plan = plan_figi_fills(mapping, [a, b])
        assert plan.fills == []
        assert len(plan.buckets["dup_figi_in_batch"]) == 1
        assert plan.buckets["dup_figi_in_batch"][0]["security_ids"] == [10, 11]


class TestPlanIdentifierRows:
    def test_skips_existing_any_source(self):
        secs = [
            _sec(1, "aaa", figi="BBG000000001"),
            _sec(2, "bbb", figi="BBG000000002", active=False),
            _sec(3, "ccc"),   # 无 figi
        ]
        existing = {(2, "BBG000000002")}   # figi 阶段已按 MASSIVE_ARCHIVE 物化
        rows, counts = plan_identifier_rows(secs, existing)
        assert counts["securities_with_figi"] == 2
        assert counts["already_materialized"] == 1
        assert len(rows) == 1
        row = rows[0]
        assert row["security_id"] == 1
        assert row["id_type"] == "FIGI" and row["source"] == "MASSIVE"
        assert row["start_date"] is None


# --------------------------------------------------------------------------- #
# 集成：--apply 幂等 / dry-run 零写入 / live 胜护栏
# --------------------------------------------------------------------------- #

def _insert_security(pg_db, security_id, symbol, **extra) -> None:
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


def _scalar(pg_db, sql, **params):
    with pg_db.engine.connect() as conn:
        return conn.execute(text(sql), params).scalar()


def _run_script(pg_db, tmp_path, parquet_path, *extra_args):
    args = create_parser().parse_args([
        "--parquet", parquet_path, "--report-dir", str(tmp_path / "reports"), *extra_args,
    ])
    return run(args, pg_db)


@pytest.mark.integration
class TestBackfillRenameEventsIntegration:
    def _seed(self, pg_db, tmp_path) -> str:
        # id 1: 活跃、双锚可解析、atln -> circ 改名（live history 有 circ 行佐证）
        _insert_security(pg_db, 1, "circ", cik="0000000111", composite_figi="BBGTEST00001")
        # id 2: 退市、无 FIGI、cik 唯一 -> RENAME + 任期补链 + FIGI 回填
        _insert_security(pg_db, 2, "dead", cik="0000000222", is_active=False,
                         delist_date=date(2020, 6, 30))
        # id 3: 退市、已有 FIGI -> 只做 PIT 物化（source=MASSIVE）
        _insert_security(pg_db, 3, "keep", composite_figi="BBGTEST00003", is_active=False)
        pg_db.upsert_symbol_history([{
            "security_id": 1, "symbol": "circ", "source": "MASSIVE",
            "source_event_id": "1:circ:2024-05-01", "event_type": "ticker_change",
            "start_date": date(2024, 5, 1),
        }])
        return _write_parquet(tmp_path / "events.parquet", [
            _parquet_record("CIRC", "ATLN", date(2010, 1, 4),
                            cik="0000000111", figi="BBGTEST00001"),
            _parquet_record("CIRC", "CIRC", date(2024, 5, 1),
                            cik="0000000111", figi="BBGTEST00001"),
            _parquet_record("DEAD", "OLDD", date(2005, 3, 15),
                            cik="0000000222", figi="BBGTEST00002"),
            _parquet_record("DEAD", "DEAD", date(2012, 9, 10),
                            cik="0000000222", figi="BBGTEST00002"),
            # epoch 哨兵行：renames 阶段丢弃，但 cik->figi 映射仍要吃到
            _parquet_record("DEAD", "DEAD", EPOCH_SENTINEL,
                            cik="0000000222", figi="BBGTEST00002"),
        ])

    def test_stage_all_apply_is_idempotent(self, pg_db, tmp_path):
        parquet = self._seed(pg_db, tmp_path)

        for _ in range(2):   # 第二遍必须零新增
            exit_code, _stats = _run_script(pg_db, tmp_path, parquet, "--apply")
            assert exit_code == 0

            assert _scalar(pg_db,
                           "SELECT count(*) FROM security_identity_events WHERE event_type='RENAME'") == 2
            high = _scalar(pg_db, "SELECT confidence FROM security_identity_events "
                                  "WHERE event_type='RENAME' AND security_id=1")
            medium = _scalar(pg_db, "SELECT confidence FROM security_identity_events "
                                    "WHERE event_type='RENAME' AND security_id=2")
            assert high == "HIGH"       # 双锚 + live history 佐证
            assert medium == "MEDIUM"   # figi 库内无命中，仅 cik 单锚

            # 任期补链：只补退市证券（id 2），active（id 1）不写
            assert _scalar(pg_db, "SELECT count(*) FROM security_symbol_history "
                                  "WHERE source = :src", src=ARCHIVE_SOURCE) == 2
            assert _scalar(pg_db, "SELECT count(*) FROM security_symbol_history "
                                  "WHERE source = :src AND security_id = 1", src=ARCHIVE_SOURCE) == 0
            end_dead = _scalar(pg_db, "SELECT end_date FROM security_symbol_history "
                                      "WHERE source = :src AND symbol = 'dead'", src=ARCHIVE_SOURCE)
            assert end_dead == date(2020, 6, 30)

            # FIGI 回填（epoch 行提供的 cik->figi 映射）+ fill-never-overwrite
            assert _scalar(pg_db, "SELECT composite_figi FROM securities WHERE id=2") == "BBGTEST00002"

            # PIT 物化：回填值 source=MASSIVE_ARCHIVE，既有值 source=MASSIVE
            rows = {}
            with pg_db.engine.connect() as conn:
                for row in conn.execute(text(
                        "SELECT security_id, id_value, source, start_date "
                        "FROM security_identifiers WHERE id_type='FIGI'")):
                    rows[row.security_id] = (row.id_value, row.source, row.start_date)
            assert rows[1] == ("BBGTEST00001", "MASSIVE", None)
            assert rows[2] == ("BBGTEST00002", ARCHIVE_SOURCE, None)
            assert rows[3] == ("BBGTEST00003", "MASSIVE", None)
            assert len(rows) == 3
            assert _scalar(pg_db, "SELECT count(*) FROM security_identifiers "
                                  "WHERE id_type='FIGI'") == 3   # 无跨 source 重复行

    def test_dry_run_writes_nothing(self, pg_db, tmp_path):
        parquet = self._seed(pg_db, tmp_path)
        exit_code, stats = _run_script(pg_db, tmp_path, parquet)
        assert exit_code == 0
        assert stats["renames_events_planned_total"] == 2
        assert stats["figi_fills_planned"] == 1
        assert _scalar(pg_db, "SELECT count(*) FROM security_identity_events") == 0
        assert _scalar(pg_db, "SELECT count(*) FROM security_symbol_history "
                              "WHERE source = :src", src=ARCHIVE_SOURCE) == 0
        assert _scalar(pg_db, "SELECT composite_figi FROM securities WHERE id=2") is None
        assert _scalar(pg_db, "SELECT count(*) FROM security_identifiers") == 0

    def test_live_wins_guards(self, pg_db, tmp_path):
        # live 日期冲突：整组隔离，不写事件
        _insert_security(pg_db, 7, "nn", cik="0000000555", composite_figi="BBGTEST00007")
        pg_db.upsert_symbol_history([{
            "security_id": 7, "symbol": "nn", "source": "MASSIVE",
            "source_event_id": "7:nn:2020-01-01", "event_type": "ticker_change",
            "start_date": date(2020, 1, 1),
        }])
        # fill-never-overwrite：已有 FIGI 的退市证券不是候选
        _insert_security(pg_db, 8, "bb", cik="0000000333", composite_figi="BBGEXIST0008",
                         is_active=False)
        # dup-FIGI 护栏：parquet 想给 id 9 的 FIGI 已被 id 8 持有
        _insert_security(pg_db, 9, "cc", cik="0000000444", is_active=False)
        parquet = _write_parquet(tmp_path / "events.parquet", [
            _parquet_record("NN", "MM", date(2010, 1, 1),
                            cik="0000000555", figi="BBGTEST00007"),
            _parquet_record("NN", "NN", date(2020, 2, 2),      # live 起始日是 2020-01-01
                            cik="0000000555", figi="BBGTEST00007"),
            _parquet_record("BB", "BB", EPOCH_SENTINEL,
                            cik="0000000333", figi="BBGDIFF00008"),
            _parquet_record("CC", "CC", EPOCH_SENTINEL,
                            cik="0000000444", figi="BBGEXIST0008"),
        ])
        exit_code, stats = _run_script(pg_db, tmp_path, parquet, "--apply")
        assert exit_code == 0
        assert stats["renames_entities_live_date_conflict"] == 1
        assert _scalar(pg_db, "SELECT count(*) FROM security_identity_events "
                              "WHERE event_type='RENAME'") == 0
        assert _scalar(pg_db, "SELECT composite_figi FROM securities WHERE id=8") == "BBGEXIST0008"
        assert _scalar(pg_db, "SELECT composite_figi FROM securities WHERE id=9") is None
        assert stats["figi_figi_already_held"] == 1

    def test_single_stage_runs_independently(self, pg_db, tmp_path):
        parquet = self._seed(pg_db, tmp_path)
        exit_code, _ = _run_script(pg_db, tmp_path, parquet, "--stage", "figi", "--apply")
        assert exit_code == 0
        assert _scalar(pg_db, "SELECT composite_figi FROM securities WHERE id=2") == "BBGTEST00002"
        # 只跑 figi：不写事件、不补任期；回填值已按 MASSIVE_ARCHIVE 物化
        assert _scalar(pg_db, "SELECT count(*) FROM security_identity_events") == 0
        assert _scalar(pg_db, "SELECT count(*) FROM security_symbol_history "
                              "WHERE source = :src", src=ARCHIVE_SOURCE) == 0
        assert _scalar(pg_db, "SELECT source FROM security_identifiers "
                              "WHERE id_type='FIGI' AND security_id=2") == ARCHIVE_SOURCE

        exit_code, _ = _run_script(pg_db, tmp_path, parquet, "--stage", "identifiers", "--apply")
        assert exit_code == 0
        # identifiers 阶段跨 source 去重：id 2 不再补 MASSIVE 行
        assert _scalar(pg_db, "SELECT count(*) FROM security_identifiers "
                              "WHERE id_type='FIGI' AND security_id=2") == 1
        assert _scalar(pg_db, "SELECT count(*) FROM security_identifiers "
                              "WHERE id_type='FIGI'") == 3
