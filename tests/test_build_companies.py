"""scripts/build_companies.py 的归组语义测试。

单元层：build_grouping / _seed_name / classify_groups 纯逻辑；
集成层：pg fixture 上的 apply 语义（多类股归一、ETF 不动、退市组、
幂等重跑、改挂保护、验收探针）。
"""
from __future__ import annotations

import csv
import os

import pytest
from sqlalchemy import text

import scripts.build_companies as build_companies


def _row(security_id, symbol, *, name=None, cik="0000000001", is_active=True):
    return {"id": security_id, "symbol": symbol, "name": name,
            "cik": cik, "is_active": is_active}


# ---------------------------------------------------------------------------
# 纯逻辑
# ---------------------------------------------------------------------------

class TestBuildGrouping:
    def test_groups_by_cik_and_sorts_members(self):
        groups = build_companies.build_grouping([
            _row(5, "b", name="Beta Inc.", cik="0000000002"),
            _row(3, "a2", name="Alpha Class B", cik="0000000001"),
            _row(1, "a1", name="Alpha Class A", cik="0000000001"),
        ])

        assert set(groups) == {"0000000001", "0000000002"}
        assert [m["security_id"] for m in groups["0000000001"]["members"]] == [1, 3]
        assert groups["0000000001"]["name"] == "Alpha Class A"

    def test_blank_cik_rows_dropped(self):
        assert build_companies.build_grouping([_row(1, "x", cik="  ")]) == {}

    def test_name_seed_prefers_active_common_equity_over_lower_id(self):
        # 最低 id 是活跃工具行（notes）——seed 必须跳到活跃 common-equity 行
        groups = build_companies.build_grouping([
            _row(1, "abcn", name="ABC 5.00% Senior Notes due 2026"),
            _row(2, "abc", name="ABC Inc. Common Stock"),
        ])
        assert groups["0000000001"]["name"] == "ABC Inc. Common Stock"

    def test_name_seed_falls_back_to_active_then_any(self):
        # 无活跃 common-equity：退级到最低 id 活跃行
        only_instrument_active = build_companies.build_grouping([
            _row(1, "abcp", name="ABC Preferred Stock"),
            _row(2, "abcw", name="ABC Warrants"),
        ])
        assert only_instrument_active["0000000001"]["name"] == "ABC Preferred Stock"

        # 全组退市：退级到最低 id 行（delisted-only CIK）
        delisted_only = build_companies.build_grouping([
            _row(7, "dead2", name="Dead Corp Class B", is_active=False),
            _row(4, "dead1", name="Dead Corp Class A", is_active=False),
        ])
        assert delisted_only["0000000001"]["name"] == "Dead Corp Class A"

    def test_name_seed_skips_blank_names_within_tier(self):
        groups = build_companies.build_grouping([
            _row(1, "x1", name=None),
            _row(2, "x2", name="Named Inc."),
        ])
        assert groups["0000000001"]["name"] == "Named Inc."

        unnamed = build_companies.build_grouping([_row(1, "x1", name=" ")])
        assert unnamed["0000000001"]["name"] is None


class TestClassifyGroups:
    def test_dual_class_vs_instrument_mislabel_split(self):
        groups = build_companies.build_grouping([
            # 真双类股 + 一条工具行
            _row(1, "gcla", name="Gamma Inc. Class A", cik="0000000001"),
            _row(2, "gclb", name="Gamma Inc. Class B", cik="0000000001"),
            _row(3, "gnote", name="Gamma 6.00% Notes due 2030", cik="0000000001"),
            # 单证券组不出现在任何名录
            _row(4, "solo", name="Solo Inc.", cik="0000000002"),
            # rilyg 型：普通股 + baby bond，两证券但只有一条 common —— 不是双类股
            _row(5, "rily", name="B. Riley Financial, Inc.", cik="0000000003"),
            _row(6, "rilyg", name="B. Riley Financial, Inc. 5.00% Senior Notes due 2026",
                 cik="0000000003"),
        ])

        dual_class, mislabel, name_conflicts = build_companies.classify_groups(groups)

        assert [(r["cik"], r["symbol"]) for r in dual_class] == [
            ("0000000001", "gcla"), ("0000000001", "gclb"),
        ]
        assert [(r["cik"], r["symbol"]) for r in mislabel] == [
            ("0000000001", "gnote"), ("0000000003", "rilyg"),
        ]
        # Gamma A/B 与 Riley 各自词干一致——不进名称分歧
        assert name_conflicts == []

    def test_same_cik_divergent_names_reported_not_blocked(self):
        # cik 0001273685 型改名世系：New York Mortgage Trust + Adamas Trust
        groups = build_companies.build_grouping([
            _row(1, "nymt", name="New York Mortgage Trust, Inc.", cik="0001273685"),
            _row(2, "adam", name="Adamas Trust", cik="0001273685", is_active=False),
        ])

        dual_class, _, name_conflicts = build_companies.classify_groups(groups)

        assert {r["symbol"] for r in name_conflicts} == {"nymt", "adam"}
        # 分歧只进报告，不影响双类股名录本身（这里两条都是 common -> 也是双证券组）
        assert {r["symbol"] for r in dual_class} == {"nymt", "adam"}


# ---------------------------------------------------------------------------
# 集成：pg fixture 上的 apply 语义
# ---------------------------------------------------------------------------

pytestmark_integration = pytest.mark.integration


def _insert_security(pg_db, security_id, symbol, *, name=None, sec_type="CS",
                     market="US", cik=None, company_id=None, is_active=True):
    with pg_db.engine.connect() as conn:
        conn.execute(
            text(
                """
                insert into securities
                    (id, symbol, current_symbol, name, market, type, cik, company_id,
                     is_active, full_refresh_interval)
                values
                    (:id, :symbol, :symbol, :name, :market, :type, :cik, :company_id,
                     :is_active, 30)
                """
            ),
            {"id": security_id, "symbol": symbol, "name": name, "market": market,
             "type": sec_type, "cik": cik, "company_id": company_id, "is_active": is_active},
        )
        conn.commit()


def _scalar(pg_db, sql, **params):
    with pg_db.engine.connect() as conn:
        return conn.execute(text(sql), params).scalar()


def _args(tmp_path, *extra):
    return build_companies.create_parser().parse_args(
        ["--report-dir", str(tmp_path), *extra]
    )


def _read_tsv(path):
    with open(path, newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh, delimiter="\t"))


@pytest.mark.integration
class TestBuildCompaniesApply:
    def test_multi_class_cik_maps_to_one_company(self, pg_db, tmp_path):
        _insert_security(pg_db, 775, "googl", name="Alphabet Inc. Class A", cik="0001652044")
        _insert_security(pg_db, 797, "goog", name="Alphabet Inc. Class C", cik="0001652044")
        _insert_security(pg_db, 8801, "brk.a", name="Berkshire Hathaway Inc.", cik="0001067983")
        _insert_security(pg_db, 8967, "brk.b", name="Berkshire Hathaway Inc. Class B", cik="0001067983")

        assert build_companies.run(_args(tmp_path, "--apply"), pg_db) == 0

        goog = _scalar(pg_db, "SELECT company_id FROM securities WHERE symbol='goog'")
        googl = _scalar(pg_db, "SELECT company_id FROM securities WHERE symbol='googl'")
        brka = _scalar(pg_db, "SELECT company_id FROM securities WHERE symbol='brk.a'")
        brkb = _scalar(pg_db, "SELECT company_id FROM securities WHERE symbol='brk.b'")
        assert goog is not None and goog == googl
        assert brka is not None and brka == brkb
        assert goog != brka
        assert _scalar(pg_db, "SELECT count(*) FROM companies") == 2
        # 名 seed 取最低 id 活跃 common-equity 行
        assert _scalar(pg_db, "SELECT name FROM companies WHERE cik='0001652044'") == "Alphabet Inc. Class A"
        # 双类股名录（验收交付物）含四条腿
        dual = _read_tsv(tmp_path / build_companies.DUAL_CLASS_REPORT)
        assert {r["symbol"] for r in dual} == {"goog", "googl", "brk.a", "brk.b"}

    def test_etf_and_non_us_rows_untouched(self, pg_db, tmp_path):
        _insert_security(pg_db, 1, "abc", name="ABC Inc.", cik="0000000001")
        # 同 CIK 的 ETF（发行人 CIK ≠ 基金实体）与非 US CS 都绝不归组
        _insert_security(pg_db, 2, "abcetf", name="ABC Trust ETF", sec_type="ETF", cik="0000000001")
        _insert_security(pg_db, 3, "abchk", name="ABC HK", market="HK", cik="0000000001")

        assert build_companies.run(_args(tmp_path, "--apply"), pg_db) == 0

        assert _scalar(pg_db, "SELECT company_id FROM securities WHERE id=1") is not None
        assert _scalar(pg_db, "SELECT company_id FROM securities WHERE id=2") is None
        assert _scalar(pg_db, "SELECT company_id FROM securities WHERE id=3") is None

    def test_delisted_only_cik_grouped_and_null_cik_left_alone(self, pg_db, tmp_path):
        _insert_security(pg_db, 1, "dead1", name="Dead Corp", cik="0000000009", is_active=False)
        _insert_security(pg_db, 2, "dead2", name="Dead Corp Class B", cik="0000000009", is_active=False)
        _insert_security(pg_db, 3, "nocik", name="No CIK Corp", cik=None)

        assert build_companies.run(_args(tmp_path, "--apply"), pg_db) == 0

        dead1 = _scalar(pg_db, "SELECT company_id FROM securities WHERE id=1")
        dead2 = _scalar(pg_db, "SELECT company_id FROM securities WHERE id=2")
        assert dead1 is not None and dead1 == dead2
        assert _scalar(pg_db, "SELECT name FROM companies WHERE cik='0000000009'") == "Dead Corp"
        assert _scalar(pg_db, "SELECT company_id FROM securities WHERE id=3") is None

    def test_instrument_mislabel_gets_company_id_but_reported(self, pg_db, tmp_path):
        # flag-don't-drop：notes 行照挂 company_id，但进 mislabel 报告、不进双类股名录
        _insert_security(pg_db, 1, "rily", name="B. Riley Financial, Inc.", cik="0000000003")
        _insert_security(pg_db, 2, "rilyg",
                         name="B. Riley Financial, Inc. 5.00% Senior Notes due 2026",
                         cik="0000000003")

        assert build_companies.run(_args(tmp_path, "--apply"), pg_db) == 0

        common = _scalar(pg_db, "SELECT company_id FROM securities WHERE id=1")
        note = _scalar(pg_db, "SELECT company_id FROM securities WHERE id=2")
        assert common is not None and common == note
        mislabel = _read_tsv(tmp_path / build_companies.MISLABEL_REPORT)
        assert [r["symbol"] for r in mislabel] == ["rilyg"]
        dual = _read_tsv(tmp_path / build_companies.DUAL_CLASS_REPORT)
        assert dual == []

    def test_idempotent_rerun_is_noop(self, pg_db, tmp_path):
        _insert_security(pg_db, 1, "abc", name="ABC Inc.", cik="0000000001")
        _insert_security(pg_db, 2, "abcb", name="ABC Inc. Class B", cik="0000000001")

        assert build_companies.run(_args(tmp_path, "--apply"), pg_db) == 0
        first = {
            "companies": _scalar(pg_db, "SELECT count(*) FROM companies"),
            "cid1": _scalar(pg_db, "SELECT company_id FROM securities WHERE id=1"),
            "cid2": _scalar(pg_db, "SELECT company_id FROM securities WHERE id=2"),
        }

        # 直接调 apply_grouping 拿第二次的行计数：新挂/改挂都必须为 0
        groups = build_companies.build_grouping(build_companies.fetch_cs_rows(pg_db))
        _, linked, reassigned = build_companies.apply_grouping(pg_db, groups, allow_reassign=False)
        assert linked == 0
        assert reassigned == 0
        assert _scalar(pg_db, "SELECT count(*) FROM companies") == first["companies"]
        assert _scalar(pg_db, "SELECT company_id FROM securities WHERE id=1") == first["cid1"]
        assert _scalar(pg_db, "SELECT company_id FROM securities WHERE id=2") == first["cid2"]

    def test_dry_run_writes_nothing(self, pg_db, tmp_path):
        _insert_security(pg_db, 1, "abc", name="ABC Inc.", cik="0000000001")

        assert build_companies.run(_args(tmp_path), pg_db) == 0

        assert _scalar(pg_db, "SELECT count(*) FROM companies") == 0
        assert _scalar(pg_db, "SELECT company_id FROM securities WHERE id=1") is None
        # dry-run 也要产出报告与 summary
        assert os.path.exists(tmp_path / build_companies.SUMMARY_REPORT)
        assert os.path.exists(tmp_path / build_companies.DUAL_CLASS_REPORT)

    def test_existing_different_company_id_never_silently_reassigned(self, pg_db, tmp_path):
        # 先造一个"挂错"的现状：证券 cik=...01 却挂在 cik=...02 的公司上
        pg_db.upsert_companies([{"cik": "0000000002", "name": "Other Corp"}])
        other_id = pg_db.get_company_id_by_cik("0000000002")
        _insert_security(pg_db, 1, "abc", name="ABC Inc.", cik="0000000001",
                         company_id=other_id)

        assert build_companies.run(_args(tmp_path, "--apply"), pg_db) == 0

        # 默认绝不静默改挂：company_id 保持不变，冲突进报告
        assert _scalar(pg_db, "SELECT company_id FROM securities WHERE id=1") == other_id
        conflicts = _read_tsv(tmp_path / build_companies.REASSIGN_CONFLICT_REPORT)
        assert [r["symbol"] for r in conflicts] == ["abc"]
        assert conflicts[0]["old_company_cik"] == "0000000002"

        # 人工确认后 --allow-reassign 才改挂到按 CIK 计算的公司
        assert build_companies.run(_args(tmp_path, "--apply", "--allow-reassign"), pg_db) == 0
        correct_id = pg_db.get_company_id_by_cik("0000000001")
        assert _scalar(pg_db, "SELECT company_id FROM securities WHERE id=1") == correct_id

    def test_acceptance_probe_fails_when_pair_splits(self, pg_db, tmp_path):
        # goog/googl 落在不同 CIK（数据错乱场景）——探针必须把退出码打成 1
        _insert_security(pg_db, 1, "googl", name="Alphabet Inc. Class A", cik="0001652044")
        _insert_security(pg_db, 2, "goog", name="Alphabet Inc. Class C", cik="0009999999")

        assert build_companies.run(_args(tmp_path, "--apply"), pg_db) == 1
