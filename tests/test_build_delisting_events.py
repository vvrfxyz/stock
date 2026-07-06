"""build_delisting_events 的单元 + PostgreSQL 集成测试。

单元层锁定纯函数：final_price 窗口选择、失败证据桶、12d2-2 规则段解析、
终价形态推断、reason 决策表（含 full-rebuild 全列 payload）。
集成层锁定端到端语义：dry-run 不落库、--apply 幂等重建、MANUAL 行保护、
delist_date 修订后的残行清理。文档抓取一律 mock，测试不触网。
"""
from datetime import date, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import text

from data_models.models import DelistingEvent
from scripts.build_delisting_events import (
    BUCKET_COHORT_2025_08,
    BUCKET_NO_PRICE_HISTORY,
    BUCKET_NO_RELIABLE_BAR,
    BUCKET_TRUNCATED,
    DelistedSecurity,
    Evidence,
    Filing,
    MergeEvent,
    classify,
    classify_price_failure,
    create_parser,
    fetch_form25_rules,
    infer_price_pattern,
    needs_price_pattern,
    parse_form25_rule,
    run,
    select_final_bar,
)


def _security(security_id=1, symbol="dead", type_="CS", cik=None, delist=date(2025, 6, 30)):
    return DelistedSecurity(id=security_id, symbol=symbol, type=type_, cik=cik, delist_date=delist)


def _filing(accession="0001-25-000001", form="25-NSE", filed=date(2025, 6, 25), doc_url=None):
    return Filing(accession, form, filed, doc_url)


# ---------------------------------------------------------------------------
# final_price 窗口选择
# ---------------------------------------------------------------------------

class TestSelectFinalBar:
    DELIST = date(2025, 6, 30)

    def test_picks_last_positive_close_within_window(self):
        bars = [
            (date(2025, 6, 26), Decimal("10.10")),
            (date(2025, 6, 27), Decimal("10.05")),
            (date(2025, 7, 2), Decimal("9.98")),  # OTC 尾巴，窗口内最后一根
        ]
        assert select_final_bar(bars, self.DELIST) == (Decimal("9.98"), date(2025, 7, 2))

    def test_ignores_zero_and_null_close(self):
        bars = [
            (date(2025, 6, 27), Decimal("10.05")),
            (date(2025, 6, 30), Decimal("0")),
            (date(2025, 7, 1), None),
        ]
        assert select_final_bar(bars, self.DELIST) == (Decimal("10.05"), date(2025, 6, 27))

    def test_never_uses_stale_bar_outside_window(self):
        # 最后 bar 停在窗口前 —— 绝不回退用陈旧价
        bars = [(date(2025, 6, 1), Decimal("8.00"))]
        assert select_final_bar(bars, self.DELIST) is None

    def test_bar_after_window_is_excluded(self):
        bars = [(date(2025, 7, 6), Decimal("5.00"))]  # delist+6 > +5
        assert select_final_bar(bars, self.DELIST) is None

    def test_window_boundaries_inclusive(self):
        bars = [(date(2025, 6, 25), Decimal("1.00")), (date(2025, 7, 5), Decimal("2.00"))]
        assert select_final_bar(bars, self.DELIST) == (Decimal("2.00"), date(2025, 7, 5))

    def test_unordered_input(self):
        bars = [
            (date(2025, 7, 1), Decimal("9.90")),
            (date(2025, 6, 26), Decimal("10.10")),
        ]
        assert select_final_bar(bars, self.DELIST) == (Decimal("9.90"), date(2025, 7, 1))

    def test_empty(self):
        assert select_final_bar([], self.DELIST) is None


class TestClassifyPriceFailure:
    def test_no_price_history(self):
        assert classify_price_failure(False, None, date(2025, 9, 1)) == BUCKET_NO_PRICE_HISTORY

    def test_cohort_2025_08_01(self):
        assert classify_price_failure(
            True, date(2025, 8, 1), date(2025, 9, 15)
        ) == BUCKET_COHORT_2025_08

    def test_max_date_2025_08_01_but_delist_within_grace_is_not_cohort(self):
        # delist_date <= 2025-08-06：窗口本身就够得到 08-01，不是休眠伪影
        assert classify_price_failure(
            True, date(2025, 8, 1), date(2025, 8, 6)
        ) != BUCKET_COHORT_2025_08

    def test_truncated_early_stop(self):
        assert classify_price_failure(
            True, date(2024, 11, 3), date(2025, 6, 30)
        ) == BUCKET_TRUNCATED

    def test_no_reliable_bar_in_window(self):
        # 有 bar 覆盖到窗口，但全是零价/错位 —— 单列证据桶
        assert classify_price_failure(
            True, date(2025, 6, 30), date(2025, 6, 30)
        ) == BUCKET_NO_RELIABLE_BAR


# ---------------------------------------------------------------------------
# Form 25 规则段解析
# ---------------------------------------------------------------------------

class TestParseForm25Rule:
    def test_single_citation(self):
        assert parse_form25_rule("pursuant to 17 CFR 240.12d2-2(b) the exchange...") == "b"

    def test_tag_style(self):
        assert parse_form25_rule("<rule12d2-2c>X</rule12d2-2c>") == "c"

    def test_template_listing_all_three_is_indeterminate(self):
        text_ = "12d2-2(a) [ ]  12d2-2(b) [ ]  12d2-2(c) [X]"
        # 三段都出现（表单模板选项），无法判定 —— 宁缺毋滥
        assert parse_form25_rule(text_) is None

    def test_no_citation(self):
        assert parse_form25_rule("nothing relevant here") is None

    def test_case_insensitive_and_spacing(self):
        assert parse_form25_rule("Rule 12D2-2 ( A ) applies") == "a"


# ---------------------------------------------------------------------------
# 终价形态推断（LOW 层）
# ---------------------------------------------------------------------------

def _bars(closes, volumes=None, start=date(2025, 3, 1)):
    volumes = volumes or [10_000] * len(closes)
    return [
        (start + timedelta(days=i), Decimal(str(c)), v)
        for i, (c, v) in enumerate(zip(closes, volumes))
    ]


class TestInferPricePattern:
    def test_distress_decline_to_pennies(self):
        closes = [2.0] * 20 + [1.5, 1.0, 0.8, 0.6, 0.5, 0.4, 0.35, 0.3, 0.28, 0.25]
        result = infer_price_pattern(_bars(closes), Decimal("0.25"))
        assert result is not None
        assert result[0] == "EXCHANGE_DROP"
        assert "suspected EXCHANGE_DROP/BANKRUPTCY" in result[1]

    def test_stable_round_price_with_shrinking_volume_is_cash_acquisition(self):
        closes = [24.0, 25.1, 24.8, 25.3, 24.5] * 4 + [26.50] * 10
        volumes = [100_000] * 20 + [10_000] * 10
        result = infer_price_pattern(_bars(closes, volumes), Decimal("26.50"))
        assert result is not None
        assert result[0] == "ACQUISITION_CASH"
        assert "suspected cash acquisition" in result[1]

    def test_stable_but_volume_not_shrinking_is_none(self):
        closes = [24.0] * 20 + [26.50] * 10
        volumes = [10_000] * 30
        assert infer_price_pattern(_bars(closes, volumes), Decimal("26.50")) is None

    def test_stable_but_off_grid_price_is_none(self):
        closes = [24.0] * 20 + [26.37] * 10
        volumes = [100_000] * 20 + [10_000] * 10
        assert infer_price_pattern(_bars(closes, volumes), Decimal("26.37")) is None

    def test_penny_but_always_was_penny_is_none(self):
        # 一直 0.30 上下横盘：不是"持续阴跌"
        closes = [0.30] * 30
        assert infer_price_pattern(_bars(closes), Decimal("0.30")) is None

    def test_insufficient_bars_is_none(self):
        assert infer_price_pattern(_bars([0.5] * 5), Decimal("0.25")) is None

    def test_null_volume_blocks_cash_inference(self):
        closes = [24.0] * 20 + [26.50] * 10
        volumes = [None] * 30
        assert infer_price_pattern(_bars(closes, volumes), Decimal("26.50")) is None


# ---------------------------------------------------------------------------
# reason 决策表
# ---------------------------------------------------------------------------

class TestClassifyDecisionTable:
    def _classify(self, security=None, evidence=None, final_price=Decimal("10.00"),
                  final_price_date=date(2025, 6, 27), price_bucket=None, price_pattern=None):
        return classify(
            security or _security(),
            evidence or Evidence(),
            final_price=final_price,
            final_price_date=final_price_date,
            price_bucket=price_bucket,
            price_pattern=price_pattern,
        )

    def test_payload_covers_all_columns_full_rebuild(self):
        # full-rebuild upsert 语义：漏列 = 冲突时清 NULL，payload 必须全列显式
        expected = {
            c.name for c in DelistingEvent.__table__.columns
            if c.name not in {"id", "created_at", "updated_at"}
        }
        assert set(self._classify().keys()) == expected

    def test_8k_alone_is_merger_high_source_8k(self):
        row = self._classify(evidence=Evidence(eightk_201=[_filing(form="8-K")]))
        assert (row["reason_code"], row["reason_confidence"], row["source"]) == ("MERGER", "HIGH", "8K")
        assert "8k_item201=0001-25-000001" in row["evidence"]

    def test_form25_plus_8k_is_merger_high_source_form25(self):
        row = self._classify(evidence=Evidence(
            form25=[_filing()], eightk_201=[_filing(accession="0002-25-000002", form="8-K")],
        ))
        assert (row["reason_code"], row["reason_confidence"], row["source"]) == ("MERGER", "HIGH", "FORM25")
        assert "form25=" in row["evidence"] and "8k_item201=" in row["evidence"]

    @pytest.mark.parametrize("rule,expected", [
        ("a", "MERGER"), ("b", "EXCHANGE_DROP"), ("c", "VOLUNTARY"),
    ])
    def test_form25_rule_citation_maps_reason(self, rule, expected):
        row = self._classify(evidence=Evidence(form25=[_filing()], form25_rule=rule))
        assert (row["reason_code"], row["reason_confidence"], row["source"]) == (expected, "HIGH", "FORM25")
        assert f"form25_rule=12d2-2({rule})" in row["evidence"]

    def test_form25_without_rule_falls_to_next_tier_keeping_accession(self):
        # Form 25 单独在场且解析不出规则段：不允许拍脑袋定 VOLUNTARY，降层
        row = self._classify(evidence=Evidence(form25=[_filing()]))
        assert row["reason_code"] == "UNKNOWN"
        assert row["reason_confidence"] is None
        assert row["source"] == "FORM25"  # 证据在，定性不了
        assert "form25=0001-25-000001" in row["evidence"]

    def test_identity_merge_is_merger_medium(self):
        row = self._classify(evidence=Evidence(
            merge_events=[MergeEvent(event_id=7, keep_security_id=99, keep_symbol="keep")],
        ))
        assert (row["reason_code"], row["reason_confidence"], row["source"]) == ("MERGER", "MEDIUM", "TICKER_EVENT")
        assert "identity_merge=event#7->keep keep#99" in row["evidence"]

    def test_high_tier_beats_merge_event(self):
        row = self._classify(evidence=Evidence(
            eightk_201=[_filing(form="8-K")],
            merge_events=[MergeEvent(1, 2, "keep")],
        ))
        assert (row["reason_confidence"], row["source"]) == ("HIGH", "8K")

    def test_etf_is_fund_closure_medium_with_nav_note(self):
        row = self._classify(security=_security(type_="ETF"))
        assert (row["reason_code"], row["reason_confidence"], row["source"]) == ("FUND_CLOSURE", "MEDIUM", "TICKER_EVENT")
        assert "delisting_return~0 is CORRECT" in row["evidence"]
        assert row["delisting_return"] is None  # 只记 evidence，绝不写经验值

    def test_price_pattern_is_low_source_price_inferred_return_null(self):
        row = self._classify(price_pattern=("ACQUISITION_CASH", "stable near grid"))
        assert (row["reason_code"], row["reason_confidence"], row["source"]) == ("ACQUISITION_CASH", "LOW", "PRICE_INFERRED")
        assert row["delisting_return"] is None
        assert "price_pattern=stable near grid" in row["evidence"]

    def test_no_evidence_is_unknown(self):
        row = self._classify()
        assert row["reason_code"] == "UNKNOWN"
        assert row["reason_confidence"] is None
        assert row["source"] is None
        assert row["evidence"] is None

    def test_price_failure_bucket_recorded_in_evidence(self):
        row = self._classify(final_price=None, final_price_date=None,
                             price_bucket=BUCKET_COHORT_2025_08)
        assert row["final_price"] is None
        assert row["final_price_date"] is None
        assert f"final_price_bucket={BUCKET_COHORT_2025_08}" in row["evidence"]

    def test_consideration_fields_null_without_8k_doc_extraction(self):
        row = self._classify(evidence=Evidence(eightk_201=[_filing(form="8-K")]))
        assert row["acquirer_name"] is None
        assert row["consideration_cash"] is None
        assert row["consideration_stock_ratio"] is None
        assert row["delisting_return"] is None


class TestNeedsPricePattern:
    def test_plain_cs_needs_pattern(self):
        assert needs_price_pattern(_security(), Evidence()) is True

    def test_8k_short_circuits(self):
        assert needs_price_pattern(_security(), Evidence(eightk_201=[_filing()])) is False

    def test_form25_without_rule_still_needs_pattern(self):
        assert needs_price_pattern(_security(), Evidence(form25=[_filing()])) is True

    def test_form25_with_rule_short_circuits(self):
        assert needs_price_pattern(_security(), Evidence(form25=[_filing()], form25_rule="b")) is False

    def test_merge_event_short_circuits(self):
        assert needs_price_pattern(_security(), Evidence(merge_events=[MergeEvent(1, 2, "k")])) is False

    def test_etf_short_circuits(self):
        assert needs_price_pattern(_security(type_="ETF"), Evidence()) is False


# ---------------------------------------------------------------------------
# --fetch-form25-docs 阶段（mock 抓取，不触网）
# ---------------------------------------------------------------------------

class TestFetchForm25Rules:
    def test_parses_rule_into_evidence(self):
        security = _security(cik="0000000123")
        evidence = Evidence(form25=[_filing(doc_url="https://sec.gov/doc25.htm")])
        stats = fetch_form25_rules(
            [security], {security.id: evidence},
            fetch_text=lambda url: "removal pursuant to Rule 12d2-2(b).",
        )
        assert evidence.form25_rule == "b"
        assert stats == {"candidates": 1, "fetched": 1, "parsed": 1, "failed": 0, "no_doc_url": 0}

    def test_skips_security_with_8k_evidence(self):
        security = _security()
        evidence = Evidence(form25=[_filing(doc_url="https://x")], eightk_201=[_filing(form="8-K")])
        calls = []
        stats = fetch_form25_rules([security], {security.id: evidence},
                                   fetch_text=lambda url: calls.append(url) or "")
        assert calls == []
        assert stats["candidates"] == 0

    def test_missing_doc_url_counted(self):
        security = _security()
        evidence = Evidence(form25=[_filing(doc_url=None)])
        stats = fetch_form25_rules([security], {security.id: evidence}, fetch_text=lambda url: "")
        assert stats["no_doc_url"] == 1
        assert evidence.form25_rule is None

    def test_offline_aborts_gracefully_after_consecutive_failures(self):
        securities = [_security(security_id=i) for i in range(1, 10)]
        evidences = {
            s.id: Evidence(form25=[_filing(doc_url=f"https://x/{s.id}")]) for s in securities
        }

        def _fail(url):
            raise ConnectionError("offline")

        stats = fetch_form25_rules(securities, evidences, fetch_text=_fail)
        assert stats["failed"] == 5  # FORM25_DOC_FAILURE_ABORT 后停止
        assert stats["parsed"] == 0
        assert all(e.form25_rule is None for e in evidences.values())

    def test_indeterminate_document_leaves_rule_none(self):
        security = _security()
        evidence = Evidence(form25=[_filing(doc_url="https://x")])
        template = "12d2-2(a) [ ] 12d2-2(b) [ ] 12d2-2(c) [X]"
        stats = fetch_form25_rules([security], {security.id: evidence},
                                   fetch_text=lambda url: template)
        assert evidence.form25_rule is None
        assert stats["fetched"] == 1 and stats["parsed"] == 0


# ---------------------------------------------------------------------------
# PostgreSQL 集成：端到端 dry-run / --apply / 幂等 / MANUAL 保护 / 残行清理
# ---------------------------------------------------------------------------

def _args(*argv):
    return create_parser().parse_args(list(argv))


@pytest.mark.integration
class TestBuildDelistingEventsPg:
    DELIST = date(2025, 6, 30)

    def _seed(self, pg_db):
        from data_models.models import (
            DailyPrice, SecFiling, Security, SecurityIdentityEvent,
        )

        with pg_db.get_session() as session:
            def sec(sid, symbol, **extra):
                defaults = dict(
                    id=sid, symbol=symbol, current_symbol=symbol, market="US",
                    type="CS", is_active=False, delist_date=self.DELIST,
                    full_refresh_interval=30,
                )
                defaults.update(extra)
                session.add(Security(**defaults))

            sec(1, "acqd", cik="0000000123")           # 8-K + Form25 -> MERGER HIGH
            sec(2, "husk")                              # identity MERGE -> MERGER MEDIUM
            sec(3, "detf", type="ETF")                  # -> FUND_CLOSURE MEDIUM
            sec(4, "none")                              # 无证据 -> UNKNOWN
            sec(5, "nopx")                              # 无任何日线 -> NO_PRICE_HISTORY
            sec(6, "keep", is_active=True, delist_date=None)   # active：排除
            sec(7, "limbo", is_active=False, delist_date=None)  # 无 delist_date：跳过但计数

            for sid in (1, 2, 3, 4):
                session.add(DailyPrice(
                    security_id=sid, date=self.DELIST - timedelta(days=3),
                    close=Decimal("10.00"), volume=1000,
                ))
            # sec1 有更晚的 OTC 尾巴 bar（yfinance 指纹：vwap/trade_count 双 NULL）
            session.add(DailyPrice(
                security_id=1, date=self.DELIST + timedelta(days=2),
                close=Decimal("9.50"), volume=10,
            ))

            # 证据 join 必须走 CIK 列：故意用不同补零格式（'123' vs '0000000123'）
            session.add(SecFiling(
                source="SEC_EDGAR", cik="123", form_type="8-K",
                accession_number="0001-25-000201", filing_date=self.DELIST + timedelta(days=5),
                items="2.01,9.01",
            ))
            session.add(SecFiling(
                source="SEC_EDGAR", cik="123", form_type="25-NSE",
                accession_number="0001-25-000025", filing_date=self.DELIST - timedelta(days=10),
            ))
            # items 含 '12.01' 但无 '2.01' 的干扰 8-K：不得误中
            session.add(SecFiling(
                source="SEC_EDGAR", cik="123", form_type="8-K",
                accession_number="0001-25-000202", filing_date=self.DELIST,
                items="12.01",
            ))
            session.add(SecurityIdentityEvent(
                security_id=6, event_type="MERGE", related_security_id=2,
                old_symbol="husk", new_symbol="keep", resolution_source="AUDIT",
                confidence="HIGH",
                details='{"keep_id": 6, "keep_symbol": "keep", "merge_ids": [2]}',
            ))
            session.commit()

    def _rows(self, pg_db):
        with pg_db.engine.connect() as conn:
            return {
                r.security_id: r
                for r in conn.execute(text(
                    "SELECT * FROM delisting_events ORDER BY security_id"
                ))
            }

    def test_dry_run_writes_nothing(self, pg_db):
        self._seed(pg_db)
        assert run(_args(), pg_db) == 0
        assert self._rows(pg_db) == {}

    def test_apply_end_to_end_then_idempotent_rerun(self, pg_db):
        self._seed(pg_db)
        assert run(_args("--apply"), pg_db) == 0

        rows = self._rows(pg_db)
        assert set(rows) == {1, 2, 3, 4, 5}  # active/无 delist_date 不建行

        assert rows[1].reason_code == "MERGER"
        assert rows[1].reason_confidence == "HIGH"
        assert rows[1].source == "FORM25"
        assert "0001-25-000201" in rows[1].evidence
        assert "0001-25-000202" not in rows[1].evidence  # item 12.01 不得误中 2.01
        assert rows[1].final_price == Decimal("9.500000")  # OTC 尾巴是最后一根
        assert rows[1].final_price_date == self.DELIST + timedelta(days=2)

        assert rows[2].reason_code == "MERGER"
        assert rows[2].reason_confidence == "MEDIUM"
        assert rows[2].source == "TICKER_EVENT"
        assert "identity_merge=" in rows[2].evidence

        assert rows[3].reason_code == "FUND_CLOSURE"
        assert rows[3].source == "TICKER_EVENT"

        assert rows[4].reason_code == "UNKNOWN"
        assert rows[4].reason_confidence is None
        assert rows[4].source is None

        assert rows[5].reason_code == "UNKNOWN"
        assert rows[5].final_price is None
        assert "final_price_bucket=NO_PRICE_HISTORY" in rows[5].evidence

        # 全表 delisting_return 恒 NULL（本迭代无对价/破产硬证据）
        assert all(r.delisting_return is None for r in rows.values())

        created_before = {sid: r.created_at for sid, r in rows.items()}
        assert run(_args("--apply"), pg_db) == 0
        rows_after = self._rows(pg_db)
        assert set(rows_after) == {1, 2, 3, 4, 5}
        assert {sid: r.created_at for sid, r in rows_after.items()} == created_before

    def test_limit_restricts_population(self, pg_db):
        self._seed(pg_db)
        assert run(_args("--apply", "--limit", "2"), pg_db) == 0
        assert set(self._rows(pg_db)) == {1, 2}

    def test_manual_rows_never_overwritten(self, pg_db):
        self._seed(pg_db)
        pg_db.upsert_delisting_events([{
            "security_id": 4, "delist_date": self.DELIST,
            "reason_code": "BANKRUPTCY", "reason_confidence": "HIGH",
            "delisting_return": Decimal("-1.0"),
            "source": "MANUAL", "evidence": "court docket #42 (human adjudicated)",
        }])
        assert run(_args("--apply"), pg_db) == 0
        rows = self._rows(pg_db)
        assert rows[4].reason_code == "BANKRUPTCY"
        assert rows[4].source == "MANUAL"
        assert rows[4].delisting_return == Decimal("-1.00000000")

    def test_stale_row_removed_after_delist_date_revision(self, pg_db):
        self._seed(pg_db)
        assert run(_args("--apply"), pg_db) == 0

        with pg_db.engine.connect() as conn:
            conn.execute(text(
                "UPDATE securities SET delist_date = :d WHERE id = 4"
            ), {"d": self.DELIST + timedelta(days=30)})
            conn.commit()

        assert run(_args("--apply"), pg_db) == 0
        with pg_db.engine.connect() as conn:
            dates = conn.execute(text(
                "SELECT delist_date FROM delisting_events WHERE security_id = 4"
            )).scalars().all()
        assert dates == [self.DELIST + timedelta(days=30)]  # 旧行清理，无残留

    def test_cohort_truncation_bucket_and_upgrade_after_price_repair(self, pg_db):
        """417 只 2025-08-01 截断队列：先记证据桶，价格修复后幂等重跑升级为真终价。"""
        from data_models.models import DailyPrice, Security

        delist = date(2025, 9, 20)
        with pg_db.get_session() as session:
            session.add(Security(
                id=10, symbol="trnc", current_symbol="trnc", market="US", type="CS",
                is_active=False, delist_date=delist, full_refresh_interval=30,
            ))
            session.add(DailyPrice(
                security_id=10, date=date(2025, 8, 1), close=Decimal("4.20"), volume=500,
            ))
            session.commit()

        assert run(_args("--apply"), pg_db) == 0
        row = self._rows(pg_db)[10]
        assert row.final_price is None
        assert "final_price_bucket=PRICE_TRUNCATED_2025-08-01_COHORT" in row.evidence

        # Massive 重拉修复补齐了窗口内的真实 bar
        with pg_db.get_session() as session:
            session.add(DailyPrice(
                security_id=10, date=delist - timedelta(days=1),
                close=Decimal("3.85"), volume=800,
            ))
            session.commit()

        assert run(_args("--apply"), pg_db) == 0
        row = self._rows(pg_db)[10]
        assert row.final_price == Decimal("3.850000")
        assert row.final_price_date == delist - timedelta(days=1)
        assert "final_price_bucket" not in (row.evidence or "")


# ---------------------------------------------------------------------------
# health_report 探针：退市 >90 天仍无结局归因（P1 warning）
# ---------------------------------------------------------------------------

@pytest.mark.integration
class TestDelistingOutcomesProbePg:
    def _seed(self, pg_db):
        from data_models.models import Security

        with pg_db.get_session() as session:
            session.add(Security(
                id=1, symbol="olda", current_symbol="olda", market="US", type="CS",
                is_active=False, delist_date=date(2024, 6, 28), full_refresh_interval=30,
            ))
            # 退市不足 90 天：不计入探针（归因管道有正常时滞）
            session.add(Security(
                id=2, symbol="newb", current_symbol="newb", market="US", type="CS",
                is_active=False, delist_date=date.today() - timedelta(days=10),
                full_refresh_interval=30,
            ))
            session.commit()

    def test_probe_counts_missing_then_unknown_then_clears(self, pg_db):
        from scripts.health_report import report_delisting_outcomes

        self._seed(pg_db)
        with pg_db.get_session() as session:
            # 无 delisting_events 行 -> P1
            assert report_delisting_outcomes(session) == 1

        assert run(_args("--apply"), pg_db) == 0
        with pg_db.get_session() as session:
            # 有行但 reason 仍 UNKNOWN -> 仍是 P1
            assert report_delisting_outcomes(session) == 1

        with pg_db.engine.connect() as conn:
            conn.execute(text(
                "UPDATE delisting_events SET reason_code = 'MERGER' WHERE security_id = 1"
            ))
            conn.commit()
        with pg_db.get_session() as session:
            assert report_delisting_outcomes(session) == 0
