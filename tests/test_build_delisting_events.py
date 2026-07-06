"""build_delisting_events 的单元 + PostgreSQL 集成测试。

单元层锁定纯函数：final_price 窗口选择、失败证据桶、12d2-2 规则段解析、
终价形态推断、reason 决策表（含 full-rebuild 全列 payload、破产三证决策表、
股票腿/混合对价 return 数学与闸门）、8-K 对价抽取（现金正则金样本 / 众数判定 /
sanity 闸门 / 收购方保守抽取 / 换股比 / 文档优先级与漏斗 / DEFM14A 章节切片 /
election 结构）、收购方名字归一与解析优先链。
集成层锁定端到端语义：dry-run 不落库、--apply 幂等重建、MANUAL 行保护、
delist_date 修订后的残行清理、--fetch-8k-docs 对价写入与 ACQUISITION_CASH
升级、换股腿估值、破产三证 -1、降级重建保险丝（--allow-degraded-rebuild）。
文档抓取一律 mock，测试不触网。
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
    DEFM14A_SECTION_WINDOW,
    EIGHTK_DOC_FAILURE_ABORT,
    AcquirerNameIndex,
    ConsiderationExtraction,
    DelistedSecurity,
    Evidence,
    Filing,
    MergeEvent,
    SecurityRef,
    _acquirer_is_self,
    cash_within_sanity_gate,
    classify,
    classify_price_failure,
    create_parser,
    extract_acquirer_names,
    extract_cash_amounts,
    extract_consideration,
    extract_stock_ratios,
    fetch_form25_rules,
    fetch_merger_considerations,
    form25_class_matches_security,
    infer_price_pattern,
    load_form25_filings,
    needs_price_pattern,
    normalize_acquirer_name,
    normalize_form25_doc_url,
    parse_form25_document,
    parse_form25_rule,
    pick_clear_mode,
    pick_merger_doc_candidates,
    resolve_acquirer_security,
    run,
    select_final_bar,
    slice_defm14a_consideration_section,
    strip_html,
)


def _security(security_id=1, symbol="dead", type_="CS", cik=None, delist=date(2025, 6, 30),
              name=None):
    return DelistedSecurity(id=security_id, symbol=symbol, type=type_, cik=cik,
                            delist_date=delist, name=name)


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

# 25-NSE 原始 XML（真实形状：Great Ajax 的 notes 类 Form 25）
FORM25_NSE_NOTES_XML = (
    '<?xml version="1.0" encoding="UTF-8"?>'
    '<edgarSubmission xmlns="http://www.sec.gov/edgar/form25">'
    "<schemaVersion>X0101</schemaVersion>"
    "<documentType>25-NSE</documentType>"
    "<issuer><cik>0001614806</cik><issuerName>GREAT AJAX CORP</issuerName></issuer>"
    "<descriptionClassSecurity>7.25% Convertible Senior Notes due 2024</descriptionClassSecurity>"
    "<ruleProvision>17 CFR 240.12d2-2(a)(2)</ruleProvision>"
    "</edgarSubmission>"
)
FORM25_NSE_CS_XML = (
    '<?xml version="1.0" encoding="UTF-8"?>'
    '<edgarSubmission xmlns="http://www.sec.gov/edgar/form25">'
    "<schemaVersion>X0101</schemaVersion>"
    "<documentType>25-NSE</documentType>"
    "<issuer><cik>0000000123</cik><issuerName>ACQUIRED HOLDINGS INC</issuerName></issuer>"
    "<descriptionClassSecurity>Common Stock, par value $0.01 per share</descriptionClassSecurity>"
    "<ruleProvision>17 CFR 240.12d2-2(a)(3)</ruleProvision>"
    "</edgarSubmission>"
)

# form '25' HTML 模板（镜像 Triumph Financial d29541d25.htm：全部条款列成选项，
# checkbox 用 &#9744;/&#9746; 实体，选中的是 (c)）
TRIUMPH_FORM25_HTML = (
    "<html><body>"
    "<p>Triumph Financial, Inc. (Exact name of Issuer as specified in its charter)</p>"
    "<p>001-36722 (Commission File Number)</p>"
    "<p>Common Stock, par value $0.01 per share</p>"
    "<p>(Description of class of securities)</p>"
    "<p>Please place an X in the box to designate the rule provision relied upon "
    "to strike the class of securities from listing and registration:</p>"
    "<p>&#9744; 17 CFR 240.12d2-2(a)(1). The entire class of the security has been redeemed.</p>"
    "<p>&#9744; 17 CFR 240.12d2-2(a)(2). The entire class of the security has matured or been retired.</p>"
    "<p>&#9744; 17 CFR 240.12d2-2(a)(3). The entire class of the security has been exchanged "
    "for another security.</p>"
    "<p>&#9744; 17 CFR 240.12d2-2(a)(4). The instrument representing the security has come "
    "to evidence another security.</p>"
    "<p>&#9744; 17 CFR 240.12d2-2(b). The Exchange has filed to strike the class from "
    "listing and registration.</p>"
    "<p>&#9746; Pursuant to 17 CFR 240.12d2-2(c), the Issuer has complied with the rules of "
    "the Exchange governing an issuer&#8217;s voluntary withdrawal of the class from listing "
    "and registration.</p>"
    "</body></html>"
)


class TestNormalizeForm25DocUrl:
    def test_strips_xsl_viewer_segment(self):
        url = ("https://www.sec.gov/Archives/edgar/data/1614806/"
               "000087666124000304/xslF25X02/primary_doc.xml")
        assert normalize_form25_doc_url(url) == (
            "https://www.sec.gov/Archives/edgar/data/1614806/"
            "000087666124000304/primary_doc.xml"
        )

    def test_plain_html_url_unchanged(self):
        url = "https://www.sec.gov/Archives/edgar/data/1539638/000119312523279813/d29541d25.htm"
        assert normalize_form25_doc_url(url) == url


class TestParseForm25Xml:
    def test_rule_provision_and_class_extracted(self):
        parsed = parse_form25_document(FORM25_NSE_NOTES_XML)
        assert parsed.provision == "a2"
        assert parsed.class_description == "7.25% Convertible Senior Notes due 2024"
        assert parsed.branch == "xml"

    def test_cs_class_xml(self):
        parsed = parse_form25_document(FORM25_NSE_CS_XML)
        assert parsed.provision == "a3"
        assert parsed.class_description == "Common Stock, par value $0.01 per share"

    def test_voluntary_c_provision(self):
        xml = FORM25_NSE_CS_XML.replace("17 CFR 240.12d2-2(a)(3)", "17 CFR 240.12d2-2(c)")
        assert parse_form25_document(xml).provision == "c"

    def test_absent_rule_provision_graceful(self):
        xml = FORM25_NSE_CS_XML.replace(
            "<ruleProvision>17 CFR 240.12d2-2(a)(3)</ruleProvision>", "")
        parsed = parse_form25_document(xml)
        assert parsed.provision is None
        assert parsed.branch == "xml"
        assert parsed.class_description == "Common Stock, par value $0.01 per share"

    def test_repeated_identical_provisions_accepted(self):
        xml = FORM25_NSE_CS_XML.replace(
            "</edgarSubmission>",
            "<ruleProvision>17 CFR 240.12d2-2(a)(3)</ruleProvision></edgarSubmission>")
        assert parse_form25_document(xml).provision == "a3"

    def test_conflicting_provisions_indeterminate(self):
        xml = FORM25_NSE_CS_XML.replace(
            "</edgarSubmission>",
            "<ruleProvision>17 CFR 240.12d2-2(c)</ruleProvision></edgarSubmission>")
        parsed = parse_form25_document(xml)
        assert parsed.provision is None
        assert "multiple_families_checked=a3,c" in parsed.note


class TestParseForm25HtmlCheckbox:
    def test_triumph_template_entity_checkboxes_select_c(self):
        # 实体形式 &#9746;/&#9744; 经 strip_html unescape 成 ☒/☐ 后可判定
        parsed = parse_form25_document(TRIUMPH_FORM25_HTML)
        assert parsed.provision == "c"
        assert parsed.branch == "html"
        assert parsed.class_description == "Common Stock, par value $0.01 per share"

    def test_raw_unicode_checkboxes(self):
        text_ = ("☐ 17 CFR 240.12d2-2(a)(1). The entire class has been redeemed. "
                 "☒ Pursuant to 17 CFR 240.12d2-2(c), the Issuer has complied with the rules.")
        assert parse_form25_document(text_).provision == "c"

    def test_ascii_checkbox_variant(self):
        text_ = ("[ ] 17 CFR 240.12d2-2(a)(1). The entire class has been redeemed. "
                 "[X] Pursuant to 17 CFR 240.12d2-2(c), the Issuer has complied with the rules.")
        assert parse_form25_document(text_).provision == "c"

    def test_checked_subprovision_extracted(self):
        text_ = ("☒ 17 CFR 240.12d2-2(a)(1). The entire class of the security has been redeemed. "
                 "☐ Pursuant to 17 CFR 240.12d2-2(c), the Issuer has complied with the rules.")
        assert parse_form25_document(text_).provision == "a1"

    def test_multiple_checked_families_indeterminate(self):
        text_ = ("☒ 17 CFR 240.12d2-2(a)(3). The entire class has been exchanged for another "
                 "security. ☒ Pursuant to 17 CFR 240.12d2-2(c), the Issuer has complied.")
        parsed = parse_form25_document(text_)
        assert parsed.provision is None
        assert "multiple_families_checked=a3,c" in parsed.note

    def test_checked_same_family_collapses_to_bare_a(self):
        text_ = ("☒ 17 CFR 240.12d2-2(a)(3). Exchanged for another security. "
                 "☒ 17 CFR 240.12d2-2(a)(4). Instrument evidences another security.")
        parsed = parse_form25_document(text_)
        assert parsed.provision == "a"
        assert "multiple_checked_same_family=a3,a4" in parsed.note

    def test_all_unchecked_is_indeterminate(self):
        text_ = ("☐ 17 CFR 240.12d2-2(b). The Exchange has filed to strike the class. "
                 "☐ Pursuant to 17 CFR 240.12d2-2(c), the Issuer has complied.")
        parsed = parse_form25_document(text_)
        assert parsed.provision is None
        assert parsed.note == "no_checked_provision"

    def test_no_marker_legacy_multi_provision_is_indeterminate(self):
        text_ = ("securities may be stricken under 17 CFR 240.12d2-2(a)(1), "
                 "17 CFR 240.12d2-2(b) or 17 CFR 240.12d2-2(c)")
        assert parse_form25_document(text_).provision is None

    def test_procedural_c_subreference_collapses_to_family(self):
        # (c) 的程序性子引用 "(c)(2)" 不产生新键——坍缩到族字母
        text_ = "removal pursuant to Rule 12d2-2(c)(2) notice requirements"
        assert parse_form25_document(text_).provision == "c"


class TestForm25ClassGuard:
    def test_notes_class_rejected_for_cs(self):
        assert form25_class_matches_security(
            "7.25% Convertible Senior Notes due 2024", "CS") is False

    def test_preferred_rejected_for_cs(self):
        assert form25_class_matches_security(
            "6.00% Series A Cumulative Preferred Stock", "CS") is False

    def test_warrants_rejected_for_cs(self):
        assert form25_class_matches_security(
            "Warrants to purchase Common Stock", "CS") is False

    def test_common_stock_accepted(self):
        assert form25_class_matches_security(
            "Common Stock, par value $0.01 per share", "CS") is True

    def test_class_a_accepted(self):
        assert form25_class_matches_security("Class A Common Stock", "CS") is True

    def test_ordinary_shares_accepted(self):
        assert form25_class_matches_security("Ordinary Shares", "CS") is True

    def test_poison_pill_parenthetical_rider_not_a_rejection(self):
        assert form25_class_matches_security(
            "Common Stock, $0.01 par value (and associated Preferred Stock Purchase Rights)",
            "CS") is True

    def test_missing_description_accepted(self):
        assert form25_class_matches_security(None, "CS") is True

    def test_etf_matching_is_loose(self):
        assert form25_class_matches_security("7.00% Fund Preferred Units", "ETF") is True

    # --- 证券自身即该类工具时的例外路径（ADR/MLP）---

    def test_ads_doc_accepted_when_security_is_adr(self):
        assert form25_class_matches_security(
            "American Depositary Shares, each representing four Ordinary Shares",
            "CS", "Diageo plc American Depositary Shares") is True

    def test_ads_doc_still_rejected_for_plain_cs(self):
        assert form25_class_matches_security(
            "American Depositary Shares, each representing four Ordinary Shares",
            "CS", "Acme Corporation Common Stock") is False

    def test_common_units_accepted_for_mlp(self):
        assert form25_class_matches_security(
            "Common Units Representing Limited Partner Interests",
            "CS", "Enterprise Products Partners L.P. Common Units") is True

    def test_preferred_depositary_doc_rejected_even_for_adr(self):
        # 文档比证券名多出 preferred 标记：不是它自己的类，仍拒
        assert form25_class_matches_security(
            "Depositary Shares Each Representing a 1/40th Interest in a Share of "
            "7.125% Series C Preferred Stock",
            "CS", "XYZ Bancorp American Depositary Shares") is False

    def test_notes_doc_rejected_when_name_lacks_marker(self):
        assert form25_class_matches_security(
            "7.25% Convertible Senior Notes due 2024",
            "CS", "XYZ Corp Class A Common Stock") is False

    def test_coupon_only_doc_class_still_rejected_for_plain_cs(self):
        # 类描述只命中票息 %（无词标记）：空标记集不得触发例外放行
        assert form25_class_matches_security(
            "7.25% Securities due 2031", "CS", "Plain Co Common Stock") is False


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
                  final_price_date=date(2025, 6, 27), price_bucket=None, price_pattern=None,
                  acquirer_close=None, acquirer_security=None):
        return classify(
            security or _security(),
            evidence or Evidence(),
            final_price=final_price,
            final_price_date=final_price_date,
            price_bucket=price_bucket,
            price_pattern=price_pattern,
            acquirer_close=acquirer_close,
            acquirer_security=acquirer_security,
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

    @pytest.mark.parametrize("rule,expected", [
        ("a1", "LIQUIDATION"), ("a2", "LIQUIDATION"), ("a3", "MERGER"), ("a4", "MERGER"),
    ])
    def test_form25_subprovision_maps_reason(self, rule, expected):
        row = self._classify(evidence=Evidence(form25=[_filing()], form25_rule=rule))
        assert (row["reason_code"], row["reason_confidence"], row["source"]) == (expected, "HIGH", "FORM25")
        assert f"form25_rule=12d2-2({rule[0]})({rule[1]})" in row["evidence"]

    @pytest.mark.parametrize("rule", ["a1", "a2"])
    def test_liquidation_rules_note_redemption_provision(self, rule):
        # (a)(1)/(a)(2) 全类赎回/退休——CS 类多为 SPAC 赎回清算，evidence 留痕
        row = self._classify(evidence=Evidence(form25=[_filing()], form25_rule=rule))
        assert row["reason_code"] == "LIQUIDATION"
        assert "redemption_provision" in row["evidence"]

    def test_bare_a_maps_merger_with_ambiguity_note(self):
        row = self._classify(evidence=Evidence(form25=[_filing()], form25_rule="a"))
        assert row["reason_code"] == "MERGER"
        assert "form25_bare_a=" in row["evidence"]

    def test_8k_merger_outranks_form25_voluntary_c(self):
        # 并购常伴自愿撤牌流程件：8-K item 2.01 在场时保持 MERGER，evidence 双证据并记
        row = self._classify(evidence=Evidence(
            form25=[_filing()], form25_rule="c",
            eightk_201=[_filing(accession="0002-25-000002", form="8-K")],
        ))
        assert (row["reason_code"], row["reason_confidence"], row["source"]) == ("MERGER", "HIGH", "FORM25")
        assert "form25_rule=12d2-2(c)" in row["evidence"]
        assert "8k_item201=" in row["evidence"]

    @pytest.mark.parametrize("rule", ["a1", "c"])
    def test_etf_form25_a1_or_c_upgrades_fund_closure_to_high(self, rule):
        row = self._classify(security=_security(type_="ETF"),
                             evidence=Evidence(form25=[_filing()], form25_rule=rule))
        assert (row["reason_code"], row["reason_confidence"], row["source"]) == ("FUND_CLOSURE", "HIGH", "FORM25")
        assert "etf_form25_upgrade=" in row["evidence"]

    def test_etf_form25_b_keeps_exchange_drop(self):
        row = self._classify(security=_security(type_="ETF"),
                             evidence=Evidence(form25=[_filing()], form25_rule="b"))
        assert (row["reason_code"], row["reason_confidence"]) == ("EXCHANGE_DROP", "HIGH")
        assert "etf_form25_upgrade=" not in row["evidence"]

    def test_form25_rule_accession_and_class_recorded_in_evidence(self):
        row = self._classify(evidence=Evidence(
            form25=[_filing()], form25_rule="a3",
            form25_rule_accession="0001-25-000025",
            form25_class="Common Stock, par value $0.01 per share",
        ))
        assert "form25_rule_accession=0001-25-000025" in row["evidence"]
        assert "form25_class=Common Stock, par value $0.01 per share" in row["evidence"]

    def test_wrong_class_only_docs_fall_through_with_evidence(self):
        # 类守卫：只有 notes/preferred 类 Form 25 可解析 → 不定性、降层如旧，留痕
        row = self._classify(evidence=Evidence(
            form25=[_filing()],
            form25_skipped_classes=["0001-25-000025:7.25% Convertible Senior Notes due 2024"],
        ))
        assert row["reason_code"] == "UNKNOWN"
        assert row["reason_confidence"] is None
        assert row["source"] == "FORM25"
        assert "form25_wrong_class_skipped=0001-25-000025:" in row["evidence"]

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

    def test_8k_item301_alone_is_exchange_drop_medium(self):
        # 退市/不达标通知（item 3.01）本身即 EXCHANGE_DROP 的 MEDIUM 证据
        row = self._classify(evidence=Evidence(
            eightk_301=[_filing(accession="0003-25-000003", form="8-K")],
        ))
        assert (row["reason_code"], row["reason_confidence"], row["source"]) == ("EXCHANGE_DROP", "MEDIUM", "8K")
        assert "8k_item301=0003-25-000003" in row["evidence"]
        assert row["delisting_return"] is None

    def test_8k_item201_beats_item301(self):
        row = self._classify(evidence=Evidence(
            eightk_201=[_filing(form="8-K")],
            eightk_301=[_filing(accession="0003-25-000003", form="8-K")],
        ))
        assert (row["reason_code"], row["reason_confidence"]) == ("MERGER", "HIGH")

    def test_merge_event_beats_item301(self):
        row = self._classify(evidence=Evidence(
            merge_events=[MergeEvent(1, 2, "keep")],
            eightk_301=[_filing(form="8-K")],
        ))
        assert (row["reason_code"], row["source"]) == ("MERGER", "TICKER_EVENT")

    def test_etf_fund_closure_beats_item301(self):
        row = self._classify(security=_security(type_="ETF"), evidence=Evidence(
            eightk_301=[_filing(form="8-K")],
        ))
        assert row["reason_code"] == "FUND_CLOSURE"

    def test_item301_beats_price_pattern(self):
        row = self._classify(
            evidence=Evidence(eightk_301=[_filing(form="8-K")]),
            price_pattern=("EXCHANGE_DROP", "sub-dollar sustained decline"),
        )
        assert (row["reason_confidence"], row["source"]) == ("MEDIUM", "8K")

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

    # --- --fetch-8k-docs 对价抽取后的升级/写数分支 ---

    def test_cash_only_extraction_upgrades_to_acquisition_cash_and_computes_return(self):
        row = self._classify(
            evidence=Evidence(
                eightk_201=[_filing(form="8-K")],
                consideration=ConsiderationExtraction(
                    cash=Decimal("10.05"), acquirer="Acme Holdings, Inc.",
                    accessions=["0001-25-000201"],
                ),
            ),
            final_price=Decimal("10.00"),
        )
        assert (row["reason_code"], row["reason_confidence"]) == ("ACQUISITION_CASH", "HIGH")
        assert row["consideration_cash"] == Decimal("10.05")
        assert row["acquirer_name"] == "Acme Holdings, Inc."
        # (10.05 - 10.00) / 10.00 = 0.005，量化到 8 位小数
        assert row["delisting_return"] == Decimal("0.00500000")
        assert "consideration_cash=10.05" in row["evidence"]
        assert "consideration_docs=0001-25-000201" in row["evidence"]
        assert "acquirer=Acme Holdings, Inc." in row["evidence"]

    def test_stock_only_extraction_is_acquisition_stock_no_return(self):
        # acquirer_close 未注入（默认 None）：换股腿估不出 → return 仍 NULL。
        # 收购方 close 在场时的估值路径见 test_stock_only_return_with_acquirer_close。
        row = self._classify(evidence=Evidence(
            eightk_201=[_filing(form="8-K")],
            consideration=ConsiderationExtraction(stock_ratio=Decimal("0.7136")),
        ))
        assert row["reason_code"] == "ACQUISITION_STOCK"
        assert row["consideration_stock_ratio"] == Decimal("0.7136")
        assert row["delisting_return"] is None  # 无 acquirer_close，估不出
        assert "consideration_stock_ratio=0.7136" in row["evidence"]

    def test_mixed_consideration_stays_merger_with_both_fields_no_return(self):
        row = self._classify(evidence=Evidence(
            eightk_201=[_filing(form="8-K")],
            consideration=ConsiderationExtraction(
                cash=Decimal("2.89"), stock_ratio=Decimal("0.1867"),
            ),
        ))
        assert row["reason_code"] == "MERGER"
        assert row["consideration_cash"] == Decimal("2.89")
        assert row["consideration_stock_ratio"] == Decimal("0.1867")
        assert row["delisting_return"] is None

    # --- S5: form25 并购规则通道的对价升级（与 8-K 2.01 分支同款）---

    @pytest.mark.parametrize("rule", ["a", "a3", "a4"])
    def test_form25_merger_rule_cash_only_upgrades_to_acquisition_cash(self, rule):
        # form25 归并购族 + 现金独占 → ACQUISITION_CASH，写实测 return
        row = self._classify(
            evidence=Evidence(
                form25=[_filing()], form25_rule=rule,
                consideration=ConsiderationExtraction(cash=Decimal("10.05")),
            ),
            final_price=Decimal("10.00"),
        )
        assert row["reason_code"] == "ACQUISITION_CASH"
        assert (row["reason_confidence"], row["source"]) == ("HIGH", "FORM25")
        assert row["consideration_cash"] == Decimal("10.05")
        assert row["delisting_return"] == Decimal("0.00500000")

    def test_form25_merger_rule_stock_only_upgrades_to_acquisition_stock(self):
        row = self._classify(
            evidence=Evidence(
                form25=[_filing()], form25_rule="a3",
                consideration=ConsiderationExtraction(stock_ratio=Decimal("0.5")),
            ),
            final_price=Decimal("10.00"),
        )
        assert row["reason_code"] == "ACQUISITION_STOCK"
        assert (row["reason_confidence"], row["source"]) == ("HIGH", "FORM25")

    def test_form25_merger_rule_mixed_stays_merger(self):
        row = self._classify(
            evidence=Evidence(
                form25=[_filing()], form25_rule="a3",
                consideration=ConsiderationExtraction(
                    cash=Decimal("2.50"), stock_ratio=Decimal("0.5")),
            ),
            final_price=Decimal("10.00"),
        )
        assert row["reason_code"] == "MERGER"
        assert row["consideration_cash"] == Decimal("2.50")
        assert row["consideration_stock_ratio"] == Decimal("0.5")

    def test_form25_liquidation_rule_not_upgraded_by_consideration(self):
        # (a)(1) 是 LIQUIDATION（非 MERGER 族）——即便抽出现金也绝不升级/不算 return
        row = self._classify(
            evidence=Evidence(
                form25=[_filing()], form25_rule="a1",
                consideration=ConsiderationExtraction(cash=Decimal("10.05")),
            ),
            final_price=Decimal("10.00"),
        )
        assert row["reason_code"] == "LIQUIDATION"
        assert row["consideration_cash"] == Decimal("10.05")   # 证据仍记
        assert row["delisting_return"] is None

    def test_bankruptcy_subbranch_keeps_priority_over_upgrade(self):
        # 破产三证子分支优先：form25_rule='b' 不属并购族，走 BANKRUPTCY 不被对价升级
        row = self._classify(
            evidence=Evidence(
                form25=[_filing()], form25_rule="b",
                eightk_103=[_filing(accession="0001-25-000103", form="8-K")],
                consideration=ConsiderationExtraction(cash=Decimal("0.05")),
            ),
            final_price=Decimal("0.05"),
        )
        assert row["reason_code"] == "BANKRUPTCY"
        assert row["delisting_return"] == Decimal("-1")

    def test_etf_fund_closure_subbranch_keeps_priority_over_upgrade(self):
        # ETF (a1) FUND_CLOSURE 子分支优先：不因对价升级成 ACQUISITION_CASH
        row = self._classify(
            security=_security(type_="ETF"),
            evidence=Evidence(
                form25=[_filing()], form25_rule="a1",
                consideration=ConsiderationExtraction(cash=Decimal("10.05")),
            ),
            final_price=Decimal("10.00"),
        )
        assert row["reason_code"] == "FUND_CLOSURE"
        assert row["delisting_return"] is None

    def test_cash_extraction_without_final_price_writes_consideration_but_no_return(self):
        row = self._classify(
            evidence=Evidence(
                eightk_201=[_filing(form="8-K")],
                consideration=ConsiderationExtraction(cash=Decimal("10.00")),
            ),
            final_price=None, final_price_date=None,
            price_bucket=BUCKET_NO_PRICE_HISTORY,
        )
        assert row["reason_code"] == "ACQUISITION_CASH"
        assert row["consideration_cash"] == Decimal("10.00")
        assert row["delisting_return"] is None

    def test_empty_extraction_keeps_merger_with_note_in_evidence(self):
        row = self._classify(evidence=Evidence(
            eightk_201=[_filing(form="8-K")],
            consideration=ConsiderationExtraction(
                accessions=["0001-25-000201"],
                note="ambiguous_cash_candidates=10.00,12.00",
            ),
        ))
        assert row["reason_code"] == "MERGER"
        assert row["delisting_return"] is None
        assert "consideration_note=ambiguous_cash_candidates=10.00,12.00" in row["evidence"]

    # --- 破产三证决策表（form25_rule='b' + final_price<$0.1 + 8-K item 1.03）---

    @pytest.mark.parametrize("proof_b,proof_price,proof_103", [
        (b, p, e)
        for b in (False, True) for p in (False, True) for e in (False, True)
    ])
    def test_bankruptcy_three_proof_decision_table(self, proof_b, proof_price, proof_103):
        # 三证齐（b + <$0.1 + 8-K 1.03）才 BANKRUPTCY/-1；缺任一维持 rule 本身的定性。
        # proof_b=False 用 (c)（VOLUNTARY）代表"非 b 规则"，验证只有 (b) 走破产路径。
        rule = "b" if proof_b else "c"
        final_price = Decimal("0.05") if proof_price else Decimal("2.00")
        eightk_103 = [_filing(accession="0001-25-000103", form="8-K")] if proof_103 else []
        row = self._classify(
            evidence=Evidence(form25=[_filing()], form25_rule=rule, eightk_103=eightk_103),
            final_price=final_price,
            final_price_date=date(2025, 6, 27),
        )
        if proof_b and proof_price and proof_103:
            assert row["reason_code"] == "BANKRUPTCY"
            assert row["reason_confidence"] == "HIGH"
            assert row["source"] == "FORM25"
            assert row["delisting_return"] == Decimal("-1")
            assert "8k_item103=0001-25-000103" in row["evidence"]
            assert "bankruptcy_three_proof=" in row["evidence"]
        else:
            assert row["reason_code"] == ("EXCHANGE_DROP" if proof_b else "VOLUNTARY")
            assert row["delisting_return"] is None
            assert "bankruptcy_three_proof=" not in (row["evidence"] or "")

    def test_8k_item201_beats_bankruptcy_three_proof(self):
        # 破产重组后被收购（363 出售等）：8-K 2.01 在前，MERGER 压过破产路径
        row = self._classify(
            evidence=Evidence(
                form25=[_filing()], form25_rule="b",
                eightk_201=[_filing(accession="0002-25-000002", form="8-K")],
                eightk_103=[_filing(accession="0001-25-000103", form="8-K")],
            ),
            final_price=Decimal("0.05"),
        )
        assert row["reason_code"] == "MERGER"
        assert row["reason_confidence"] == "HIGH"
        assert row["delisting_return"] is None

    def test_bankruptcy_needs_form25_rule_b_not_bare_b_without_form25(self):
        # form25_rule='b' 但 evidence.form25 为空：HIGH-2 分支入口需要 form25 在场，
        # 破产路径不触发（8-K 1.03 单独不足以定破产）
        row = self._classify(
            evidence=Evidence(form25_rule="b", eightk_103=[_filing(form="8-K")]),
            final_price=Decimal("0.05"),
        )
        assert row["reason_code"] != "BANKRUPTCY"
        assert row["delisting_return"] is None

    def test_etf_form25_b_never_enters_bankruptcy(self):
        # F10：ETF（1940 法基金）即便 form25_rule='b' + 近零终价 + 8-K 1.03 也不进
        # 破产分支——停在 EXCHANGE_DROP，return NULL
        row = self._classify(
            security=_security(type_="ETF"),
            evidence=Evidence(
                form25=[_filing()], form25_rule="b",
                eightk_103=[_filing(accession="0001-25-000103", form="8-K")],
            ),
            final_price=Decimal("0.05"),
        )
        assert row["reason_code"] == "EXCHANGE_DROP"
        assert row["delisting_return"] is None
        assert "bankruptcy_three_proof=" not in (row["evidence"] or "")

    def test_consideration_return_gated_on_non_merger_reason(self):
        # F4：form25_rule='b' 定成 EXCHANGE_DROP，但该证券又走了对价抽取路抽出现金
        # （身份 MERGE 事件让它进 defm14a_only 通道）——对价不代表这只证券的退出对价，
        # 证据记录但绝不落成 return
        row = self._classify(
            evidence=Evidence(
                form25=[_filing()], form25_rule="b",
                consideration=ConsiderationExtraction(cash=Decimal("12.00")),
            ),
            final_price=Decimal("10.00"),
        )
        assert row["reason_code"] == "EXCHANGE_DROP"
        assert row["consideration_cash"] == Decimal("12.00")   # 证据仍记录
        assert "consideration_cash=12.00" in row["evidence"]
        assert row["delisting_return"] is None                 # 但不算 return

    # --- 换股腿/混合对价 return 数学与 sanity 闸门（acquirer_close 注入）---

    def test_stock_only_return_with_acquirer_close(self):
        # implied = ratio × acquirer_close = 0.5 × 22.00 = 11.00；final=10.00
        # return = (11.00 - 10.00) / 10.00 = 0.10
        row = self._classify(
            evidence=Evidence(
                eightk_201=[_filing(form="8-K")],
                consideration=ConsiderationExtraction(stock_ratio=Decimal("0.5")),
            ),
            final_price=Decimal("10.00"),
            acquirer_close=Decimal("22.00"),
            acquirer_security="acq#42",
        )
        assert row["reason_code"] == "ACQUISITION_STOCK"
        assert row["delisting_return"] == Decimal("0.10000000")
        assert "acquirer_security=acq#42" in row["evidence"]
        assert "acquirer_close=22.00" in row["evidence"]

    def test_mixed_return_with_acquirer_close(self):
        # implied = cash + ratio × close = 2.50 + 0.5 × 16.00 = 10.50；final=10.00
        # return = (10.50 - 10.00) / 10.00 = 0.05；混合仍 MERGER，两字段都写
        row = self._classify(
            evidence=Evidence(
                eightk_201=[_filing(form="8-K")],
                consideration=ConsiderationExtraction(
                    cash=Decimal("2.50"), stock_ratio=Decimal("0.5"),
                ),
            ),
            final_price=Decimal("10.00"),
            acquirer_close=Decimal("16.00"),
        )
        assert row["reason_code"] == "MERGER"
        assert row["consideration_cash"] == Decimal("2.50")
        assert row["consideration_stock_ratio"] == Decimal("0.5")
        assert row["delisting_return"] == Decimal("0.05000000")

    def test_stock_return_gate_floor_inclusive(self):
        # implied = 0.6 × 10.00 = 6.00 恰在股票腿紧闸门 0.6x 下界（含边界）→ 采信
        row = self._classify(
            evidence=Evidence(
                eightk_201=[_filing(form="8-K")],
                consideration=ConsiderationExtraction(stock_ratio=Decimal("0.6")),
            ),
            final_price=Decimal("10.00"),
            acquirer_close=Decimal("10.00"),
        )
        assert row["delisting_return"] == Decimal("-0.40000000")

    def test_stock_return_below_floor_gated_out(self):
        # implied = 0.599 × 10.00 = 5.99 < 0.6x 下界 → 不写数值，evidence 留痕
        # （生产实测：混合 deal 漏抽现金腿时 implied 落在 ~0.3x，全被此闸拦下）
        row = self._classify(
            evidence=Evidence(
                eightk_201=[_filing(form="8-K")],
                consideration=ConsiderationExtraction(stock_ratio=Decimal("0.599")),
            ),
            final_price=Decimal("10.00"),
            acquirer_close=Decimal("10.00"),
        )
        assert row["reason_code"] == "ACQUISITION_STOCK"
        assert row["delisting_return"] is None
        assert "stock_gated_out=" in row["evidence"]
        assert "vs final_price=10.00" in row["evidence"]

    def test_stock_return_above_ceiling_gated_out(self):
        # implied = 6 × 10.00 = 60.00 > 1.5x 上界 → gated out
        row = self._classify(
            evidence=Evidence(
                eightk_201=[_filing(form="8-K")],
                consideration=ConsiderationExtraction(stock_ratio=Decimal("6")),
            ),
            final_price=Decimal("10.00"),
            acquirer_close=Decimal("10.00"),
        )
        assert row["delisting_return"] is None
        assert "stock_gated_out=" in row["evidence"]

    def test_election_structure_skips_stock_return(self):
        # election（股东可选现金或股票）：换股腿无唯一 return，即使 close 在场也不写
        row = self._classify(
            evidence=Evidence(
                eightk_201=[_filing(form="8-K")],
                consideration=ConsiderationExtraction(
                    stock_ratio=Decimal("0.5"), election=True,
                ),
            ),
            final_price=Decimal("10.00"),
            acquirer_close=Decimal("22.00"),
        )
        assert row["reason_code"] == "ACQUISITION_STOCK"
        assert row["delisting_return"] is None

    def test_election_does_not_block_cash_exclusive_return(self):
        # 现金独占腿是文档实据：election 标记不影响现金 return
        row = self._classify(
            evidence=Evidence(
                eightk_201=[_filing(form="8-K")],
                consideration=ConsiderationExtraction(
                    cash=Decimal("10.05"), election=True,
                ),
            ),
            final_price=Decimal("10.00"),
        )
        assert row["reason_code"] == "ACQUISITION_CASH"
        assert row["delisting_return"] == Decimal("0.00500000")

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
        assert evidence.form25_rule_accession == "0001-25-000001"
        assert stats == {"candidates": 1, "fetched": 1, "parsed": 1, "parsed_xml": 0,
                         "parsed_html": 1, "wrong_class": 0, "indeterminate": 0,
                         "failed": 0, "no_doc_url": 0,
                         "fetch_unavailable": 0, "offline_abort": 0}

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
        assert stats["indeterminate"] == 1


class TestFetchForm25RulesTwoBranch:
    XSL_URL = ("https://www.sec.gov/Archives/edgar/data/1614806/"
               "000087666124000304/xslF25X02/primary_doc.xml")
    RAW_URL = ("https://www.sec.gov/Archives/edgar/data/1614806/"
               "000087666124000304/primary_doc.xml")

    def test_xsl_viewer_url_stripped_and_xml_parsed(self):
        security = _security(cik="0000000123")
        evidence = Evidence(form25=[_filing(doc_url=self.XSL_URL)])
        fetched_urls = []

        def fake(url):
            fetched_urls.append(url)
            return FORM25_NSE_CS_XML

        stats = fetch_form25_rules([security], {security.id: evidence}, fetch_text=fake)
        assert fetched_urls == [self.RAW_URL]  # 剥掉 /xslF25X02/ 抓原始 XML
        assert evidence.form25_rule == "a3"
        assert evidence.form25_rule_accession == "0001-25-000001"
        assert evidence.form25_class == "Common Stock, par value $0.01 per share"
        assert stats["parsed"] == 1
        assert stats["parsed_xml"] == 1 and stats["parsed_html"] == 0
        assert stats["wrong_class"] == 0

    def test_notes_class_doc_does_not_classify_cs_security(self):
        # 类守卫：仅有 notes 类 Form 25 可解析 → 不定性、留痕、按现状降层
        security = _security()
        evidence = Evidence(form25=[_filing(accession="AJX-NOTES", doc_url="https://x/notes.xml")])
        stats = fetch_form25_rules([security], {security.id: evidence},
                                   fetch_text=lambda url: FORM25_NSE_NOTES_XML)
        assert evidence.form25_rule is None
        assert stats["parsed"] == 0 and stats["wrong_class"] == 1
        assert evidence.form25_skipped_classes == [
            "AJX-NOTES:7.25% Convertible Senior Notes due 2024"
        ]

    def test_prefers_cs_class_doc_among_multiple(self):
        # 一司多类各报一份 Form 25：最近的 notes 类被守卫拒绝，下一份 CS 类采信
        delist = date(2025, 6, 30)
        notes = _filing(accession="F-NOTES", filed=delist - timedelta(days=1),
                        doc_url="https://x/notes")
        cs = _filing(accession="F-CS", filed=delist - timedelta(days=5),
                     doc_url="https://x/cs")
        security = _security(delist=delist)
        evidence = Evidence(form25=[notes, cs])
        docs = {"https://x/notes": FORM25_NSE_NOTES_XML, "https://x/cs": FORM25_NSE_CS_XML}
        stats = fetch_form25_rules([security], {security.id: evidence},
                                   fetch_text=docs.__getitem__)
        assert evidence.form25_rule == "a3"
        assert evidence.form25_rule_accession == "F-CS"
        assert stats["fetched"] == 2 and stats["parsed"] == 1 and stats["wrong_class"] == 1
        assert evidence.form25_skipped_classes == [
            "F-NOTES:7.25% Convertible Senior Notes due 2024"
        ]

    def test_html_template_checkbox_doc_parses_via_html_branch(self):
        security = _security()
        evidence = Evidence(form25=[_filing(doc_url="https://x/d29541d25.htm")])
        stats = fetch_form25_rules([security], {security.id: evidence},
                                   fetch_text=lambda url: TRIUMPH_FORM25_HTML)
        assert evidence.form25_rule == "c"
        assert evidence.form25_class == "Common Stock, par value $0.01 per share"
        assert stats["parsed_html"] == 1 and stats["parsed_xml"] == 0

    def test_etf_class_matching_is_loose(self):
        security = _security(type_="ETF")
        evidence = Evidence(form25=[_filing(doc_url="https://x/etf.xml")])
        stats = fetch_form25_rules([security], {security.id: evidence},
                                   fetch_text=lambda url: FORM25_NSE_NOTES_XML)
        assert evidence.form25_rule == "a2"
        assert stats["wrong_class"] == 0


# ---------------------------------------------------------------------------
# 8-K 对价抽取：现金正则金样本（realistic 8-K 句式）
# ---------------------------------------------------------------------------

class TestStripHtml:
    def test_strips_tags_and_entities(self):
        doc = ("<html><body><p>the right to receive&nbsp;$26.50 in <b>cash</b>,\n"
               "without interest</p><script>var x=1;</script></body></html>")
        assert strip_html(doc) == "the right to receive $26.50 in cash, without interest"

    def test_unescapes_curly_quotes(self):
        assert strip_html("Falcon Corp. (&#8220;Parent&#8221;)") == "Falcon Corp. (“Parent”)"


class TestExtractCashAmounts:
    def test_right_to_receive_in_cash_without_interest(self):
        text_ = ("each Share was converted into the right to receive $26.50 in cash, "
                 "without interest and less applicable withholding taxes")
        assert extract_cash_amounts(text_) == [Decimal("26.50")]

    def test_per_share_in_cash_with_thousands_comma(self):
        text_ = "holders became entitled to receive $1,264.00 per share in cash"
        assert extract_cash_amounts(text_) == [Decimal("1264.00")]

    def test_cash_in_an_amount_equal_to_per_share(self):
        text_ = "cash in an amount equal to $8.25 per share, without interest"
        assert extract_cash_amounts(text_) == [Decimal("8.25")]

    def test_amount_in_cash_equal_to_per_share(self):
        text_ = "the right to receive an amount in cash equal to $12.00 per share"
        assert extract_cash_amounts(text_) == [Decimal("12.00")]

    def test_without_interest_between_amount_and_in_cash(self):
        text_ = "the right to receive $12.00, without interest, in cash"
        assert extract_cash_amounts(text_) == [Decimal("12.00")]

    def test_aggregate_purchase_price_not_matched_as_per_share(self):
        text_ = "for an aggregate purchase price of $1,200,000,000 in cash"
        assert extract_cash_amounts(text_) == []

    def test_aggregate_guard_blocks_right_to_receive_without_per_share(self):
        # "right to receive ... in cash" 自身不含 per share 时，回看窗口出现
        # aggregate 即判为总价语境——绝不当每股对价
        text_ = "in the aggregate, holders will have the right to receive $50,000,000 in cash"
        assert extract_cash_amounts(text_) == []

    def test_aggregate_before_a_true_per_share_amount_is_kept(self):
        # 匹配段自含 per share 时不受 aggregate 回看误伤
        text_ = "for an aggregate consideration of $150,000,000, or $26.50 per share in cash"
        assert extract_cash_amounts(text_) == [Decimal("26.50")]

    def test_same_occurrence_matched_by_two_patterns_counts_once(self):
        # "right to receive $X per share in cash" 同时命中 P1/P2——按金额位置去重
        text_ = "the right to receive $26.50 per share in cash"
        assert extract_cash_amounts(text_) == [Decimal("26.50")]

    def test_repeated_occurrences_all_collected(self):
        text_ = ("converted into the right to receive $26.50 in cash. As a result of "
                 "the Merger, each Share represents the right to receive $26.50 in cash.")
        assert extract_cash_amounts(text_) == [Decimal("26.50"), Decimal("26.50")]

    def test_unrelated_dollar_amounts_ignored(self):
        text_ = "the Company repaid $45,000,000 of outstanding debt at closing"
        assert extract_cash_amounts(text_) == []


class TestNewCashPatterns:
    """二期新增的五组每股现金对价触发短语——各一条金样本 + 负样本。"""

    def test_tender_net_to_seller(self):
        text_ = ("offered to purchase all outstanding shares at $16.50 per share, "
                 "net to the seller in cash, without interest")
        assert extract_cash_amounts(text_) == [Decimal("16.50")]

    def test_net_to_holder_variant(self):
        text_ = "$18.00 per share, net to the holder in cash"
        assert extract_cash_amounts(text_) == [Decimal("18.00")]

    def test_merger_consideration_of_per_share_in_cash(self):
        text_ = "the merger consideration of $12.50 per share in cash, without interest"
        assert extract_cash_amounts(text_) == [Decimal("12.50")]

    def test_per_share_consideration_of_in_cash(self):
        text_ = "each holder received a per share consideration of $9.00 in cash"
        assert extract_cash_amounts(text_) == [Decimal("9.00")]

    def test_amount_equal_to_with_delayed_per_share_lookahead(self):
        # "per share" 出现在金额之后 60 字符内（不在同一从句）——lookahead 采信
        text_ = ("the right to receive an amount in cash equal to $12.00, "
                 "without interest, per share")
        assert extract_cash_amounts(text_) == [Decimal("12.00")]

    def test_purchase_price_of_per_share(self):
        text_ = "the purchase price of $45.00 per share was paid at the effective time"
        assert extract_cash_amounts(text_) == [Decimal("45.00")]

    def test_tender_offer_to_purchase_all_at_per_share(self):
        text_ = ("commenced a tender offer to purchase all outstanding shares of common "
                 "stock at a price of $18.00 per share")
        assert extract_cash_amounts(text_) == [Decimal("18.00")]

    # --- 负样本 ---

    def test_cash_dividend_declaration_not_matched(self):
        # 分红声明不是并购对价：无 "in cash" 并购触发短语
        text_ = "the Board declared a quarterly cash dividend of $0.25 per share payable July 1"
        assert extract_cash_amounts(text_) == []

    def test_purchase_price_aggregate_without_per_share_not_matched(self):
        # "purchase price of $X" 无 per share（总价语境）——新 purchase-price 正则不误中
        text_ = "the aggregate purchase price of $1,200,000,000 was funded through new debt"
        assert extract_cash_amounts(text_) == []

    def test_tender_offer_aggregate_dollar_without_per_share_not_matched(self):
        text_ = ("a tender offer to purchase all outstanding shares for an aggregate of "
                 "$500,000,000 in cash")
        assert extract_cash_amounts(text_) == []

    def test_integer_authorized_share_count_not_matched(self):
        # 整数股数（授权/持仓语境）无美元每股对价
        text_ = "the tender offer to purchase all 5,000,000 shares held by insiders"
        assert extract_cash_amounts(text_) == []

    def test_preferred_liquidation_preference_not_matched(self):
        # F6：优先股清算优先权 "equal to $25.00 plus accrued and unpaid dividends
        # per share" 曾被 60 字符 lookahead 误当每股现金对价——accrued/unpaid/dividend
        # 命中即拒绝
        text_ = ("holders shall have the right to receive an amount equal to $25.00 "
                 "plus accrued and unpaid dividends per share")
        assert extract_cash_amounts(text_) == []

    def test_amount_equal_to_per_share_across_sentence_not_matched(self):
        # F6：句号后的 "per share" 属另一句——clause-anchor [^$.;] 不跨句
        text_ = ("the right to receive an amount equal to $12.00 in additional value. "
                 "Amounts are quoted per share elsewhere")
        assert extract_cash_amounts(text_) == []

    def test_total_merger_consideration_aggregate_not_matched(self):
        # F12："total merger consideration of $500,000,000 in cash" 是 deal 总额——
        # 加宽的总价引导词（total/entire/combined/overall）拦下
        text_ = "the total merger consideration of $500,000,000 in cash was paid to holders"
        assert extract_cash_amounts(text_) == []

    def test_huge_no_per_share_amount_dropped_even_without_guard_word(self):
        # F12：无 aggregate/total 等引导词，但无 per share 的 5 位数金额兜底弃
        text_ = "the merger consideration of $250,000,000 in cash funded the transaction"
        assert extract_cash_amounts(text_) == []


class TestPickClearMode:
    def test_empty_is_none(self):
        assert pick_clear_mode([]) is None

    def test_single_value_accepted(self):
        assert pick_clear_mode([Decimal("26.50")]) == Decimal("26.50")

    def test_clear_mode_across_docs(self):
        amounts = [Decimal("26.50"), Decimal("26.50"), Decimal("27.00")]
        assert pick_clear_mode(amounts) == Decimal("26.50")

    def test_tie_is_indeterminate(self):
        assert pick_clear_mode([Decimal("26.50"), Decimal("27.00")]) is None


class TestCashSanityGate:
    FINAL = Decimal("10.00")

    def test_near_final_price_passes(self):
        assert cash_within_sanity_gate(Decimal("10.05"), self.FINAL) is True

    def test_boundaries_inclusive(self):
        assert cash_within_sanity_gate(Decimal("2.00"), self.FINAL) is True    # 0.2x
        assert cash_within_sanity_gate(Decimal("50.00"), self.FINAL) is True   # 5x

    def test_below_floor_rejected(self):
        assert cash_within_sanity_gate(Decimal("1.99"), self.FINAL) is False

    def test_above_ceiling_rejected(self):
        assert cash_within_sanity_gate(Decimal("500.00"), self.FINAL) is False


class TestExtractAcquirerNames:
    def test_merger_with_company_keeps_comma_suffix(self):
        text_ = "entered into an Agreement and Plan of Merger with Acme Holdings, Inc., a Delaware corporation"
        assert extract_acquirer_names(text_) == ["Acme Holdings, Inc."]

    def test_acquired_by(self):
        text_ = "the Company was acquired by Global Payments Inc. pursuant to the Merger Agreement"
        assert extract_acquirer_names(text_) == ["Global Payments Inc."]

    def test_wholly_owned_subsidiary_of(self):
        text_ = "the Company became a wholly owned subsidiary of Blackstone Inc."
        assert extract_acquirer_names(text_) == ["Blackstone Inc."]

    def test_parent_defined_term_resolution(self):
        text_ = "Falcon Bidco Corp. (“Parent”) caused Merger Sub to merge"
        assert extract_acquirer_names(text_) == ["Falcon Bidco Corp."]

    def test_corp_suffix_ends_name_before_next_sentence(self):
        # 后缀 token 是名字的自然右边界——不误吞下一句的句首大写词
        text_ = "the Company was acquired by Global Payments Inc. The transaction closed"
        assert extract_acquirer_names(text_) == ["Global Payments Inc."]

    def test_merger_sub_placeholder_rejected(self):
        assert extract_acquirer_names("the merger with Merger Sub was completed") == []

    def test_lowercase_generic_reference_not_captured(self):
        assert extract_acquirer_names("the merger with the surviving corporation") == []

    def test_parent_placeholder_alone_rejected(self):
        assert extract_acquirer_names("became a wholly owned subsidiary of Parent") == []


class TestExtractStockRatios:
    def test_clean_ratio_match(self):
        text_ = "0.7136 shares of Acquirer Inc. common stock for each share of Company common stock"
        assert extract_stock_ratios(text_) == [(Decimal("0.7136"), "Acquirer Inc.")]

    def test_integer_share_counts_not_matched(self):
        # 整数 "100 shares of" 是持仓/授权语境，换股比要求小数形态
        text_ = "purchased 100 shares of common stock for each share plan participant"
        assert extract_stock_ratios(text_) == []

    def test_mixed_sentence_yields_cash_and_ratio(self):
        text_ = ("the right to receive $3.50 per share in cash and 0.7136 shares of "
                 "Acquirer Inc. common stock for each share of Company Common Stock")
        assert extract_cash_amounts(text_) == [Decimal("3.50")]
        assert extract_stock_ratios(text_) == [(Decimal("0.7136"), "Acquirer Inc.")]

    # --- S1: 换股句自带收购方（名字在前 / "common stock of" 语序 / 占位词剔除）---

    def test_ratio_sentence_acquirer_name_first(self):
        # 金样本：并购换股句里收购方名字在 "shares of" 之后、"common stock" 之前
        text_ = ("each share was converted into 0.3163 shares of Apollo Endosurgery, Inc. "
                 "common stock for each share held")
        assert extract_stock_ratios(text_) == [(Decimal("0.3163"), "Apollo Endosurgery, Inc.")]

    def test_ratio_sentence_common_stock_of_ordering(self):
        # "common stock of <Name>" 语序：名字在 "of" 之后
        text_ = "0.5000 shares of common stock of Acme Corp. for each share"
        assert extract_stock_ratios(text_) == [(Decimal("0.5000"), "Acme Corp.")]

    def test_ratio_sentence_capital_stock_of_ordering(self):
        text_ = "0.2500 shares of capital stock of Beta Holdings, Inc. for each share"
        assert extract_stock_ratios(text_) == [(Decimal("0.2500"), "Beta Holdings, Inc.")]

    def test_ratio_sentence_parent_placeholder_rejected(self):
        # 占位词 Parent 被 blocklist 拒绝——ratio 仍抽出，acquirer 为 None
        text_ = "0.4000 shares of Parent common stock for each share"
        assert extract_stock_ratios(text_) == [(Decimal("0.4000"), None)]


# ---------------------------------------------------------------------------
# 收购方名字归一与解析优先链（换股腿估值）
# ---------------------------------------------------------------------------

def _ref(sid, symbol="acq", is_active=True, type_="CS"):
    return SecurityRef(id=sid, symbol=symbol, is_active=is_active, type=type_)


class TestNormalizeAcquirerName:
    def test_strips_single_suffix(self):
        assert normalize_acquirer_name("Acme Inc.") == "acme"

    def test_strips_multiple_trailing_suffixes(self):
        assert normalize_acquirer_name("Acme Holdings, Inc.") == "acme"

    def test_strips_corp_keeping_core(self):
        assert normalize_acquirer_name("Falcon Bidco Corp.") == "falcon bidco"

    def test_multiword_core_preserved(self):
        assert normalize_acquirer_name("Global Payments Inc.") == "global payments"

    def test_punctuation_glued(self):
        assert normalize_acquirer_name("O'Reilly Automotive, Inc.") == "oreilly automotive"

    def test_dotted_suffix_collapsed(self):
        assert normalize_acquirer_name("Foo N.V.") == "foo"

    def test_keeps_at_least_one_token(self):
        # 全是后缀词也保留最后一个 token（不返回空键）
        assert normalize_acquirer_name("Holdings") == "holdings"


class TestResolveAcquirerSecurity:
    def _name_index(self, name_to_refs):
        index = AcquirerNameIndex()
        for name, refs in name_to_refs.items():
            key = normalize_acquirer_name(name)
            for ref in refs:
                index.by_id[ref.id] = ref
                index.sec_by_name.setdefault(key, []).append(ref)
        return index

    def test_merge_keep_side_ignored_without_name_match(self):
        # F1：身份 MERGE 的 keep 侧不是收购方（repair_identity 合并的是同一实体的
        # 重复身份，keep 侧就是被并方自身），只走名字通道——无名字匹配即 unresolved
        keep = _ref(99, symbol="keep")
        index = AcquirerNameIndex(by_id={99: keep})
        ref, note = resolve_acquirer_security("Whatever Corp.", index)
        assert ref is None
        assert note == "acquirer_unresolved"

    def test_name_channel_wins_over_merge_keep(self):
        # F1：即便有唯一 keep 侧，名字通道仍是唯一解析来源——名字匹配到 other 就是
        # other（不是 keep）；keep 侧完全不参与
        keep = _ref(99, symbol="keep")
        other = _ref(5, symbol="oth")
        index = self._name_index({"Falcon Corp.": [other]})
        index.by_id[99] = keep
        ref, note = resolve_acquirer_security("Falcon Corp.", index)
        assert ref is other and note is None

    def test_name_exact_unique_match(self):
        index = self._name_index({"Falcon Bidco Corp.": [_ref(5, symbol="fbco")]})
        ref, note = resolve_acquirer_security("Falcon Bidco Corp.", index)
        assert ref.id == 5 and note is None

    def test_suffix_stripping_enables_match(self):
        # 文档写 "Acme Inc."，库里名字是 "Acme Corporation"——归一后同键
        index = self._name_index({"Acme Corporation": [_ref(7)]})
        ref, note = resolve_acquirer_security("Acme Inc.", index)
        assert ref.id == 7 and note is None

    def test_ambiguous_name_returns_none(self):
        index = self._name_index({"Acme Inc.": [_ref(1, symbol="a1"), _ref(2, symbol="a2")]})
        ref, note = resolve_acquirer_security("Acme Inc.", index)
        assert ref is None
        assert note == "acquirer_ambiguous=2"

    def test_no_match_is_unresolved(self):
        index = self._name_index({"Other Co": [_ref(3)]})
        ref, note = resolve_acquirer_security("Nonexistent Inc.", index)
        assert ref is None and note == "acquirer_unresolved"

    def test_missing_acquirer_name_is_unresolved(self):
        ref, note = resolve_acquirer_security(None, AcquirerNameIndex())
        assert ref is None and note == "acquirer_unresolved"

    def test_company_name_channel_match(self):
        member = _ref(11, symbol="sub")
        index = AcquirerNameIndex(by_id={11: member})
        index.company_members_by_name[normalize_acquirer_name("Falcon Group")] = [[member]]
        ref, note = resolve_acquirer_security("Falcon Group", index)
        assert ref.id == 11 and note is None

    def test_company_channel_narrows_to_unique_active_cs(self):
        # 公司多成员：优先唯一活跃 CS（退市壳 + 多类股并存的常见形态）
        active_cs = _ref(11, symbol="a", is_active=True, type_="CS")
        dead = _ref(12, symbol="b", is_active=False, type_="CS")
        etf = _ref(13, symbol="c", is_active=True, type_="ETF")
        index = AcquirerNameIndex(by_id={11: active_cs, 12: dead, 13: etf})
        index.company_members_by_name[normalize_acquirer_name("Falcon Group")] = [
            [active_cs, dead, etf]
        ]
        ref, note = resolve_acquirer_security("Falcon Group", index)
        assert ref.id == 11 and note is None


class TestAcquirerIsSelf:
    """S2：自指守卫——完成 8-K 常写 "merger with <本壳自身>"，用壳自身价格估股票腿
    会算出伪 return（lptn/Lpath 事故）。id 相同或名字归一相同即判自指。"""

    def test_resolved_ref_same_id_is_self(self):
        husk = _security(security_id=286871, name="Lpath, Inc.")
        ref = _ref(286871, symbol="lptn")
        assert _acquirer_is_self("Lpath", husk, ref) is True

    def test_candidate_name_equals_husk_name_is_self(self):
        # 名字侧：候选名归一后等于壳自身名字归一（ref 尚未解析，传 None）
        husk = _security(security_id=1, name="Lpath, Inc.")
        assert _acquirer_is_self("Lpath Incorporated", husk, None) is True

    def test_different_acquirer_not_self(self):
        husk = _security(security_id=1, name="Lpath, Inc.")
        other = _ref(5, symbol="apen")
        assert _acquirer_is_self("Apollo Endosurgery, Inc.", husk, other) is False

    def test_empty_husk_name_no_false_self_on_name_channel(self):
        # 壳无名字：名字侧不得因空键碰撞误判自指（id 侧仍可判）
        husk = _security(security_id=1, name=None)
        assert _acquirer_is_self("Apollo Endosurgery, Inc.", husk, None) is False

    def test_none_candidate_and_ref_not_self(self):
        husk = _security(security_id=1, name="Lpath, Inc.")
        assert _acquirer_is_self(None, husk, None) is False


# ---------------------------------------------------------------------------
# DEFM14A "Merger Consideration" 章节切片
# ---------------------------------------------------------------------------

class TestSliceDefm14aSection:
    def test_uses_first_heading_window_that_yields_candidates(self):
        # F7：目录里的 "Merger Consideration" 引用窗口抽不到金额（被无关文本推开），
        # 正文章节窗口才抽得出——取第一个能出候选的窗口，而非最后一次（最后一次常
        # 落在 Annex A 协议法条里）
        toc = "Merger Consideration " + ("as referenced in the summary of terms " * 8)
        gap = "X" * DEFM14A_SECTION_WINDOW           # 把正文推到目录窗口之外
        body = ("The Merger Consideration Each share was converted into the right "
                "to receive $30.00 per share in cash. ")
        annex = "Merger Consideration under the agreement means $99.99 per share in cash"
        section = slice_defm14a_consideration_section(toc + gap + body + annex)
        assert section is not None
        # 旧"最后一次标题"会切到 Annex A（起头 "Merger Consideration under..."）；
        # 新"第一个能出候选的窗口"命中正文章节
        assert section.startswith("The Merger Consideration")
        assert "$30.00" in section

    def test_heading_absent_returns_none(self):
        assert slice_defm14a_consideration_section("no relevant heading in this doc") is None

    def test_headings_without_any_candidate_return_none(self):
        # F7：标题在，但所有窗口都抽不到现金/换股比 → None（调用方退回全文）
        text_ = "The Merger Consideration is discussed elsewhere. No amounts appear here at all."
        assert slice_defm14a_consideration_section(text_) is None

    def test_defm14a_extraction_confined_to_section(self):
        # extract_consideration 对 DEFM14A 只在章节窗口内抽——避开摘要里的无关金额
        noise = "In the risk factors we mention $999.00 per share in cash hypothetically. "
        section = ("The Merger Consideration Each share converted into the right to "
                   "receive $30.00 in cash, without interest. ")
        filing = Filing("DEF-1", "DEFM14A", date(2025, 6, 1), "https://x/def")
        got = extract_consideration([(filing, noise + section)], Decimal("29.00"))
        assert got.cash == Decimal("30.00")

    def test_defm14a_election_detection_scoped_to_section(self):
        # F8：委托书里的代理电子送达样板句 "elect to receive future proxy materials
        # electronically" 在章节窗口之外——不触发 election 结构，正当股票腿不被抑制
        boiler = ("Stockholders may elect to receive future proxy materials electronically. " * 300)
        section = ("The Merger Consideration Each share was converted into 0.5000 shares "
                   "of Eagle Corp. common stock for each share held. ")
        filing = Filing("DEF-2", "DEFM14A", date(2025, 6, 1), "https://x/def")
        got = extract_consideration([(filing, boiler + section)], None)
        assert got.stock_ratio == Decimal("0.5000")
        assert got.election is False


# ---------------------------------------------------------------------------
# 跨文档汇总（extract_consideration）与文档优先级
# ---------------------------------------------------------------------------

def _doc(accession, text_):
    return (_filing(accession=accession, form="8-K"), text_)


class TestExtractConsideration:
    def test_mode_across_docs_with_gate_pass(self):
        docs = [
            _doc("A1", "the right to receive $26.50 in cash. Also the right to receive $26.50 in cash."),
            _doc("A2", "stockholders received $27.00 per share in cash"),
        ]
        got = extract_consideration(docs, Decimal("26.40"))
        assert got.cash == Decimal("26.50")   # 众数 2:1
        assert got.accessions == ["A1", "A2"]
        assert got.note is None

    def test_tie_is_ambiguous_and_recorded(self):
        docs = [
            _doc("A1", "the right to receive $26.50 in cash"),
            _doc("A2", "stockholders received $27.00 per share in cash"),
        ]
        got = extract_consideration(docs, Decimal("26.40"))
        assert got.cash is None
        assert "ambiguous_cash_candidates=26.50,27.00" in got.note

    def test_sanity_gate_rejection_leaves_cash_null_with_note(self):
        docs = [_doc("A1", "the right to receive $500.00 per share in cash")]
        got = extract_consideration(docs, Decimal("10.00"))
        assert got.cash is None
        assert "cash_gated_out=500.00 vs final_price=10.00" in got.note

    def test_no_final_price_skips_gate_but_keeps_cash(self):
        docs = [_doc("A1", "the right to receive $26.50 in cash")]
        got = extract_consideration(docs, None)
        assert got.cash == Decimal("26.50")

    def test_conflicting_acquirers_resolve_to_null(self):
        docs = [
            _doc("A1", "was acquired by Global Payments Inc. pursuant to the merger"),
            _doc("A2", "the merger with Acme Holdings, Inc. was completed"),
        ]
        got = extract_consideration(docs, None)
        assert got.acquirer is None
        assert "ambiguous_acquirers=" in got.note

    def test_same_acquirer_from_two_patterns_is_accepted(self):
        docs = [_doc("A1", ("the merger with Falcon Bidco Corp. was completed. "
                            "Falcon Bidco Corp. (“Parent”) paid the consideration"))]
        got = extract_consideration(docs, None)
        assert got.acquirer == "Falcon Bidco Corp."

    def test_conflicting_stock_ratios_resolve_to_null(self):
        docs = [
            _doc("A1", "0.7136 shares of Acquirer Inc. common stock for each share"),
            _doc("A2", "0.5000 shares of Acquirer Inc. common stock for each share"),
        ]
        got = extract_consideration(docs, None)
        assert got.stock_ratio is None
        assert "ambiguous_stock_ratios=" in got.note

    def test_election_phrase_at_the_election_of_holders_detected(self):
        # F2：广化后覆盖 "at the election of the holder(s)"
        docs = [_doc("A1", ("at the election of the holders, each share converted into "
                            "0.5000 shares of Acquirer Inc. common stock for each share"))]
        got = extract_consideration(docs, None)
        assert got.election is True

    def test_election_phrase_elected_to_receive_detected(self):
        # F2：广化后覆盖 "elected to receive"
        docs = [_doc("A1", ("stockholders who elected to receive stock got 0.5000 shares "
                            "of Acquirer Inc. common stock for each share"))]
        got = extract_consideration(docs, None)
        assert got.election is True

    def test_mixed_deal_cash_leg_not_gated(self):
        # F3：混合对价（同时抽到换股比）的小额现金腿不过 sanity 闸门——$2.50 相对
        # 终价 17.00 本会被 [0.2x,5x] 下界（0.2×17=3.4 > 2.50）null 掉，但混合腿放行
        docs = [_doc("A1", ("each share converted into the right to receive $2.50 in cash "
                            "and 0.5000 shares of Acquirer Inc. common stock for each share"))]
        got = extract_consideration(docs, Decimal("17.00"))
        assert got.cash == Decimal("2.50")
        assert got.stock_ratio == Decimal("0.5000")
        assert "cash_gated_out" not in (got.note or "")

    def test_cash_only_still_gated(self):
        # F3 反向锁定：现金独占（无换股比）时 sanity 闸门照常生效
        docs = [_doc("A1", "the right to receive $2.50 per share in cash")]
        got = extract_consideration(docs, Decimal("17.00"))
        assert got.cash is None
        assert "cash_gated_out" in got.note

    # --- S1: 换股句自带收购方跨文档唯一性 ---

    def test_stock_acquirer_unique_across_docs_set(self):
        docs = [
            _doc("A1", "0.5000 shares of Eagle Corp. common stock for each share"),
            _doc("A2", "each holder got 0.5000 shares of Eagle Corp. common stock for each share"),
        ]
        got = extract_consideration(docs, None)
        assert got.stock_acquirer == "Eagle Corp."
        assert "ambiguous_stock_acquirers" not in (got.note or "")

    def test_stock_acquirer_ambiguous_across_docs_noted(self):
        docs = [
            _doc("A1", "0.5000 shares of Eagle Corp. common stock for each share"),
            _doc("A2", "0.5000 shares of Falcon Holdings, Inc. common stock for each share"),
        ]
        got = extract_consideration(docs, None)
        assert got.stock_acquirer is None
        assert "ambiguous_stock_acquirers=" in got.note
        assert "Eagle Corp." in got.note and "Falcon Holdings, Inc." in got.note


class TestPickMergerDocCandidates:
    DELIST = date(2025, 6, 30)

    def test_prefers_201_then_defm14a_then_301_capped_at_three(self):
        # 有 2.01：顺序 [2.01, DEFM14A, 1.01, 3.01]——完成 8-K 常不复述对价，DEFM14A
        # 排在退市通知 3.01 之前
        evidence = Evidence(
            eightk_201=[
                _filing(accession="A201-far", filed=self.DELIST + timedelta(days=20), doc_url="https://x/201far"),
                _filing(accession="A201-near", filed=self.DELIST + timedelta(days=2), doc_url="https://x/201near"),
            ],
            eightk_301=[_filing(accession="A301", filed=self.DELIST, doc_url="https://x/301")],
            defm14a=[_filing(accession="ADEF", form="DEFM14A",
                             filed=self.DELIST - timedelta(days=60), doc_url="https://x/def")],
        )
        picked = pick_merger_doc_candidates(evidence, self.DELIST)
        assert [f.accession_number for f in picked] == ["A201-near", "A201-far", "ADEF"]

    def test_eightk_101_ordered_after_defm14a_before_301(self):
        # S4：有 2.01 时 1.01（签约公告，常含对价）排在 DEFM14A 之后、3.01 之前
        evidence = Evidence(
            eightk_201=[_filing(accession="A201", filed=self.DELIST, doc_url="https://x/201")],
            eightk_101=[_filing(accession="A101", filed=self.DELIST - timedelta(days=90),
                                doc_url="https://x/101")],
            eightk_301=[_filing(accession="A301", filed=self.DELIST, doc_url="https://x/301")],
        )
        picked = pick_merger_doc_candidates(evidence, self.DELIST)
        assert [f.accession_number for f in picked] == ["A201", "A101", "A301"]

    def test_defm14a_only_channel_orders_defm14a_101_301(self):
        # S4：无 2.01（defm14a_only 路）顺序 [DEFM14A, 1.01, 3.01]
        evidence = Evidence(
            eightk_301=[_filing(accession="A301", filed=self.DELIST, doc_url="https://x/301")],
            eightk_101=[_filing(accession="A101", filed=self.DELIST - timedelta(days=90),
                                doc_url="https://x/101")],
            defm14a=[_filing(accession="ADEF", form="DEFM14A",
                             filed=self.DELIST - timedelta(days=60), doc_url="https://x/def")],
        )
        picked = pick_merger_doc_candidates(evidence, self.DELIST)
        assert [f.accession_number for f in picked] == ["ADEF", "A101", "A301"]

    def test_cap_three_drops_301_when_defm14a_and_101_present(self):
        # S4：配额 3 满，3.01 被挤出（有 2.01 + DEFM14A + 1.01 已占满）
        evidence = Evidence(
            eightk_201=[_filing(accession="A201", filed=self.DELIST, doc_url="https://x/201")],
            defm14a=[_filing(accession="ADEF", form="DEFM14A",
                             filed=self.DELIST - timedelta(days=60), doc_url="https://x/def")],
            eightk_101=[_filing(accession="A101", filed=self.DELIST - timedelta(days=90),
                                doc_url="https://x/101")],
            eightk_301=[_filing(accession="A301", filed=self.DELIST, doc_url="https://x/301")],
        )
        picked = pick_merger_doc_candidates(evidence, self.DELIST)
        assert [f.accession_number for f in picked] == ["A201", "ADEF", "A101"]
        assert "A301" not in [f.accession_number for f in picked]

    def test_defm14a_fills_when_fewer_8ks(self):
        evidence = Evidence(
            eightk_201=[_filing(accession="A201", doc_url="https://x/201")],
            defm14a=[_filing(accession="ADEF", form="DEFM14A",
                             filed=self.DELIST - timedelta(days=60), doc_url="https://x/def")],
        )
        assert [f.accession_number for f in pick_merger_doc_candidates(evidence, self.DELIST)] == ["A201", "ADEF"]

    def test_same_accession_in_201_and_301_deduped(self):
        # 一份 8-K 常同时带 items 2.01 与 3.01
        shared = _filing(accession="A-BOTH", doc_url="https://x/both")
        evidence = Evidence(eightk_201=[shared], eightk_301=[shared])
        assert [f.accession_number for f in pick_merger_doc_candidates(evidence, self.DELIST)] == ["A-BOTH"]

    def test_docs_without_url_excluded(self):
        evidence = Evidence(eightk_201=[_filing(doc_url=None)])
        assert pick_merger_doc_candidates(evidence, self.DELIST) == []

    def test_defm14a_first_when_no_8k_201(self):
        # F11：无 8-K 2.01（defm14a_only 路）时 DEFM14A 排在 item 3.01 之前——否则
        # >=3 份 3.01 会占满 3 份配额，唯一可能含对价的 DEFM14A 永远抓不到
        evidence = Evidence(
            eightk_301=[
                _filing(accession=f"A301-{i}", filed=self.DELIST - timedelta(days=i),
                        doc_url=f"https://x/301-{i}")
                for i in range(3)
            ],
            defm14a=[_filing(accession="ADEF", form="DEFM14A",
                             filed=self.DELIST - timedelta(days=60), doc_url="https://x/def")],
        )
        picked = pick_merger_doc_candidates(evidence, self.DELIST)
        assert picked[0].accession_number == "ADEF"
        assert "ADEF" in [f.accession_number for f in picked]


# ---------------------------------------------------------------------------
# --fetch-8k-docs 阶段（mock 抓取，不触网）
# ---------------------------------------------------------------------------

class TestFetchMergerConsiderations:
    def _candidate(self, security_id=1, delist=date(2025, 6, 30), doc_url="https://x/8k.htm"):
        security = _security(security_id=security_id, cik="0000000123", delist=delist)
        evidence = Evidence(eightk_201=[_filing(form="8-K", doc_url=doc_url)])
        return security, evidence

    def test_funnel_counts_and_extraction(self):
        security, evidence = self._candidate()
        fetched_urls = []

        def fake(url):
            fetched_urls.append(url)
            return "<p>the right to receive $26.50 in cash, without interest</p>"

        stats = fetch_merger_considerations(
            [security], {security.id: evidence}, {security.id: Decimal("26.40")},
            fetch_text=fake,
        )
        assert fetched_urls == ["https://x/8k.htm"]
        assert stats["candidates"] == 1
        assert stats["docs_fetched"] == 1
        assert stats["cash_extracted"] == 1
        assert stats["cash_gated_out"] == 0
        assert evidence.consideration.cash == Decimal("26.50")

    def test_non_merger_security_is_not_a_candidate(self):
        security = _security()
        evidence = Evidence(form25=[_filing(doc_url="https://x/f25")])  # 无 8-K 2.01
        calls = []
        stats = fetch_merger_considerations(
            [security], {security.id: evidence}, {security.id: Decimal("10.00")},
            fetch_text=lambda url: calls.append(url) or "",
        )
        assert calls == []
        assert stats["candidates"] == 0
        assert evidence.consideration is None

    def test_gated_out_counted_in_funnel(self):
        security, evidence = self._candidate()
        stats = fetch_merger_considerations(
            [security], {security.id: evidence}, {security.id: Decimal("10.00")},
            fetch_text=lambda url: "the right to receive $500.00 per share in cash",
        )
        assert stats["cash_extracted"] == 0
        assert stats["cash_gated_out"] == 1
        assert evidence.consideration.cash is None
        assert "cash_gated_out" in evidence.consideration.note

    def test_missing_doc_url_counted(self):
        security, evidence = self._candidate(doc_url=None)
        stats = fetch_merger_considerations(
            [security], {security.id: evidence}, {security.id: Decimal("10.00")},
            fetch_text=lambda url: "",
        )
        assert stats["no_doc_url"] == 1
        assert evidence.consideration.note == "no_primary_document_url"

    def test_offline_aborts_gracefully_after_consecutive_failures(self):
        pairs = [self._candidate(security_id=i, doc_url=f"https://x/{i}") for i in range(1, 10)]
        securities = [s for s, _ in pairs]
        evidences = {s.id: e for s, e in pairs}

        def _fail(url):
            raise ConnectionError("offline")

        stats = fetch_merger_considerations(
            securities, evidences, {s.id: Decimal("10.00") for s in securities},
            fetch_text=_fail,
        )
        assert stats["docs_failed"] == EIGHTK_DOC_FAILURE_ABORT  # 保险丝后停止
        assert stats["candidates"] == 9  # 候选仍全量计数，漏斗可对账
        assert stats["cash_extracted"] == 0
        assert all(e.consideration is None for e in evidences.values())


# ---------------------------------------------------------------------------
# PostgreSQL 集成：端到端 dry-run / --apply / 幂等 / MANUAL 保护 / 残行清理
# ---------------------------------------------------------------------------

def _args(*argv):
    return create_parser().parse_args(list(argv))


def test_parser_fetch_8k_docs_flag_defaults_off_and_composes():
    assert _args().fetch_8k_docs is False
    args = _args("--fetch-8k-docs", "--fetch-form25-docs", "--apply", "--limit", "5")
    assert args.fetch_8k_docs is True
    assert args.fetch_form25_docs is True
    assert args.apply is True and args.limit == 5


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
# PostgreSQL 集成：--fetch-8k-docs 对价写入 / ACQUISITION_CASH 升级 / 幂等
# ---------------------------------------------------------------------------

# 现金并购：单一收购方（两种触发短语归一到同一名字）、每股 $26.50、终价 25.00
CASH_8K_HTML = (
    "<html><body><p>On June 27, 2025, the Company completed the previously announced "
    "merger with Falcon Bidco Corp. Each share of common stock issued and outstanding "
    "was converted into the right to receive $26.50 in cash, without interest. "
    "Falcon Bidco Corp. (&#8220;Parent&#8221;) paid the aggregate consideration from "
    "cash on hand.</p></body></html>"
)
# 闸门拒绝：抽出的 $500.00 相对终价 10.00 出界 [0.2x, 5x]
GATED_8K_HTML = (
    "<html><body><p>each share was converted into the right to receive "
    "$500.00 per share in cash</p></body></html>"
)
# 换股对价：只填 ratio，不算 return
STOCK_8K_HTML = (
    "<html><body><p>each share of Company common stock was converted into "
    "0.7136 shares of Eagle Acquisition Corp. common stock for each share held. "
    "The Company became a wholly owned subsidiary of Eagle Acquisition Corp.</p></body></html>"
)


@pytest.mark.integration
class TestFetch8kConsiderationPg:
    DELIST = date(2025, 6, 30)
    DOCS = {
        "https://sec.test/cash8k.htm": CASH_8K_HTML,
        "https://sec.test/gate8k.htm": GATED_8K_HTML,
        "https://sec.test/stock8k.htm": STOCK_8K_HTML,
    }

    def _seed(self, pg_db):
        from data_models.models import DailyPrice, SecFiling, Security

        with pg_db.get_session() as session:
            specs = [
                (21, "cash", "111", Decimal("25.00"), "https://sec.test/cash8k.htm"),
                (22, "gate", "222", Decimal("10.00"), "https://sec.test/gate8k.htm"),
                (23, "stok", "333", Decimal("30.00"), "https://sec.test/stock8k.htm"),
            ]
            for sid, symbol, cik, close, doc_url in specs:
                session.add(Security(
                    id=sid, symbol=symbol, current_symbol=symbol, market="US",
                    type="CS", is_active=False, delist_date=self.DELIST,
                    cik=cik, full_refresh_interval=30,
                ))
                session.add(DailyPrice(
                    security_id=sid, date=self.DELIST - timedelta(days=3),
                    close=close, volume=1000,
                ))
                session.add(SecFiling(
                    source="SEC_EDGAR", cik=cik, form_type="8-K",
                    accession_number=f"0001-25-9{sid:04d}",
                    filing_date=self.DELIST + timedelta(days=3),
                    items="2.01,9.01", primary_document_url=doc_url,
                ))
            session.commit()

    def _patch_fetcher(self, monkeypatch):
        import scripts.build_delisting_events as bde
        monkeypatch.setattr(bde, "_edgar_fetch_text", lambda: self.DOCS.__getitem__)

    def _rows(self, pg_db):
        with pg_db.engine.connect() as conn:
            return {
                r.security_id: r
                for r in conn.execute(text(
                    "SELECT * FROM delisting_events ORDER BY security_id"
                ))
            }

    def test_apply_writes_consideration_only_for_gated_hits(self, pg_db, monkeypatch):
        self._seed(pg_db)
        self._patch_fetcher(monkeypatch)
        # 与 --fetch-form25-docs 组合可用（本 seed 无 Form25 候选，阶段空跑）
        assert run(_args("--apply", "--fetch-form25-docs", "--fetch-8k-docs"), pg_db) == 0
        rows = self._rows(pg_db)

        # 现金独占：升级 ACQUISITION_CASH，写对价与实测 return
        assert rows[21].reason_code == "ACQUISITION_CASH"
        assert rows[21].reason_confidence == "HIGH"
        assert rows[21].source == "8K"
        assert rows[21].consideration_cash == Decimal("26.50")
        assert rows[21].acquirer_name == "Falcon Bidco Corp."
        # (26.50 - 25.00) / 25.00 = 0.06
        assert rows[21].delisting_return == Decimal("0.06")
        assert "consideration_docs=0001-25-90021" in rows[21].evidence
        assert "consideration_cash=26.50" in rows[21].evidence

        # 闸门拒绝：保持 MERGER，不写数值，evidence 留痕
        assert rows[22].reason_code == "MERGER"
        assert rows[22].consideration_cash is None
        assert rows[22].delisting_return is None
        assert "cash_gated_out=500.00 vs final_price=10.00" in rows[22].evidence

        # 换股独占：ACQUISITION_STOCK；收购方 'Eagle Acquisition Corp.' 库里无此证券，
        # 解析走 acquirer_unresolved，无 acquirer_close → 本行仍不算 return
        assert rows[23].reason_code == "ACQUISITION_STOCK"
        assert rows[23].consideration_stock_ratio == Decimal("0.7136")
        assert rows[23].acquirer_name == "Eagle Acquisition Corp."
        assert rows[23].consideration_cash is None
        assert rows[23].delisting_return is None
        assert "acquirer_unresolved" in rows[23].evidence

    def test_idempotent_rerun_with_8k_docs(self, pg_db, monkeypatch):
        # 两个文档旗标一起跑（保险丝要求）——重跑位级幂等
        self._seed(pg_db)
        self._patch_fetcher(monkeypatch)
        assert run(_args("--apply", "--fetch-8k-docs", "--fetch-form25-docs"), pg_db) == 0
        before = {
            sid: (r.reason_code, r.consideration_cash, r.delisting_return,
                  r.evidence, r.created_at)
            for sid, r in self._rows(pg_db).items()
        }
        assert run(_args("--apply", "--fetch-8k-docs", "--fetch-form25-docs"), pg_db) == 0
        after = {
            sid: (r.reason_code, r.consideration_cash, r.delisting_return,
                  r.evidence, r.created_at)
            for sid, r in self._rows(pg_db).items()
        }
        assert after == before

    def test_rerun_without_8k_docs_reverts_to_pure_classifier_output(self, pg_db, monkeypatch):
        """full-rebuild 语义：显式降级重建（--allow-degraded-rebuild）不带
        --fetch-8k-docs 重跑，对价字段随之清 NULL——对价不是缓存而是每次运行的
        抽取产物，evidence 反映当次运行的证据面。（不带旗标时保险丝拒绝，见
        TestDegradedRebuildFusePg。）"""
        self._seed(pg_db)
        self._patch_fetcher(monkeypatch)
        assert run(_args("--apply", "--fetch-8k-docs"), pg_db) == 0
        assert self._rows(pg_db)[21].consideration_cash == Decimal("26.50")

        assert run(_args("--apply", "--allow-degraded-rebuild"), pg_db) == 0
        row = self._rows(pg_db)[21]
        assert row.reason_code == "MERGER"
        assert row.consideration_cash is None
        assert row.delisting_return is None

    def test_limit_composes_with_8k_docs(self, pg_db, monkeypatch):
        self._seed(pg_db)
        self._patch_fetcher(monkeypatch)
        assert run(_args("--apply", "--fetch-8k-docs", "--limit", "1"), pg_db) == 0
        rows = self._rows(pg_db)
        assert set(rows) == {21}
        assert rows[21].reason_code == "ACQUISITION_CASH"


# ---------------------------------------------------------------------------
# PostgreSQL 集成：降级重建保险丝（--allow-degraded-rebuild）
# ---------------------------------------------------------------------------

@pytest.mark.integration
class TestDegradedRebuildFusePg:
    """库里已有非 MANUAL 对价/return 数据时，不带 --fetch-8k-docs 的 --apply 是降级
    重建（full-rebuild 会清 NULL）——默认拒绝，须显式 --allow-degraded-rebuild
    或带 --fetch-8k-docs 才放行。"""
    DELIST = date(2025, 6, 30)

    def _seed(self, pg_db):
        from data_models.models import DailyPrice, Security

        with pg_db.get_session() as session:
            session.add(Security(
                id=31, symbol="cons", current_symbol="cons", market="US", type="CS",
                is_active=False, delist_date=self.DELIST, cik="0000000900",
                full_refresh_interval=30,
            ))
            session.add(DailyPrice(
                security_id=31, date=self.DELIST - timedelta(days=2),
                close=Decimal("10.00"), volume=1000,
            ))
            session.commit()
        # 存量非 MANUAL 行携对价/return——保险丝的保护对象
        pg_db.upsert_delisting_events([{
            "security_id": 31, "delist_date": self.DELIST,
            "reason_code": "ACQUISITION_CASH", "reason_confidence": "HIGH",
            "consideration_cash": Decimal("11.00"), "delisting_return": Decimal("0.1"),
            "source": "8K", "evidence": "consideration_cash=11.00",
        }])

    def _rows(self, pg_db):
        with pg_db.engine.connect() as conn:
            return {
                r.security_id: r
                for r in conn.execute(text(
                    "SELECT * FROM delisting_events ORDER BY security_id"
                ))
            }

    def test_apply_without_8k_docs_refuses_and_preserves(self, pg_db):
        self._seed(pg_db)
        assert run(_args("--apply"), pg_db) == 1     # 保险丝拒绝
        row = self._rows(pg_db)[31]
        assert row.consideration_cash == Decimal("11.000000")   # 未被清 NULL
        assert row.delisting_return == Decimal("0.10000000")

    def test_allow_degraded_rebuild_proceeds_and_clears(self, pg_db):
        self._seed(pg_db)
        assert run(_args("--apply", "--allow-degraded-rebuild"), pg_db) == 0
        row = self._rows(pg_db)[31]
        # 显式放行：全量重建，无抽取产物 → 对价/return 清 NULL，回退纯分类器输出
        assert row.consideration_cash is None
        assert row.delisting_return is None
        assert row.reason_code == "UNKNOWN"     # seed 无任何证据 filing

    def test_8k_only_without_form25_still_refuses(self, pg_db):
        # F5(a)：只带 --fetch-8k-docs（缺 --fetch-form25-docs）仍是降级重建——会丢
        # form25_rule → BANKRUPTCY -1 与 form25 规则段 HIGH 定性，保险丝照样拒绝
        self._seed(pg_db)
        assert run(_args("--apply", "--fetch-8k-docs"), pg_db) == 1
        row = self._rows(pg_db)[31]
        assert row.consideration_cash == Decimal("11.000000")   # 未被清 NULL
        assert row.delisting_return == Decimal("0.10000000")

    def test_both_doc_flags_bypass_fuse(self, pg_db, monkeypatch):
        # F5(a)：同时带两个文档旗标才是全证据重建——保险丝(a)放行
        self._seed(pg_db)
        import scripts.build_delisting_events as bde
        # 无候选（seed 无 8-K 2.01 / MERGE / form25 并购规则），抓取阶段空跑；getter
        # 可用（非 None）故 part(b) 不判终端降级
        monkeypatch.setattr(bde, "_edgar_fetch_text", lambda: (lambda url: ""))
        assert run(_args("--apply", "--fetch-8k-docs", "--fetch-form25-docs"), pg_db) == 0

    def test_both_flags_but_fetch_unavailable_aborts(self, pg_db, monkeypatch):
        # F5(b)：两个旗标都带、保险丝(a)放行，但 SEC getter 不可用（UA 缺失/离线）——
        # 抽取实质没发生，全量重建照样清空存量；part(b) 侦测 fetch_unavailable → 拒绝
        self._seed(pg_db)
        import scripts.build_delisting_events as bde
        monkeypatch.setattr(bde, "_edgar_fetch_text", lambda: None)   # getter 不可用
        assert run(_args("--apply", "--fetch-8k-docs", "--fetch-form25-docs"), pg_db) == 1
        row = self._rows(pg_db)[31]
        assert row.consideration_cash == Decimal("11.000000")   # 未被清 NULL
        assert row.delisting_return == Decimal("0.10000000")


# ---------------------------------------------------------------------------
# PostgreSQL 集成：换股腿估值（收购方解析 + load_acquirer_closes unnest SQL）
# ---------------------------------------------------------------------------

STOCK_LEG_8K_HTML = (
    "<html><body><p>On June 27, 2025, the Company completed its merger. Each share of "
    "Company common stock was converted into 0.5000 shares of Eagle Corp. common stock "
    "for each share held. The Company became a wholly owned subsidiary of Eagle "
    "Corp.</p></body></html>"
)


@pytest.mark.integration
class TestStockLegValuationPg:
    """换股独占对价：抽出 ratio → 解析收购方证券（名字精确匹配）→ load_acquirer_closes
    取 final_price_date 当日收盘 → implied = ratio×close 过闸门 → 写实测 return。
    这条路径覆盖 load_acquirer_closes 的 unnest join SQL（PG 实跑）。"""
    DELIST = date(2025, 6, 30)

    def _seed(self, pg_db):
        from data_models.models import DailyPrice, SecFiling, Security

        with pg_db.get_session() as session:
            # 退市标的：final_price 10.00 @ DELIST-2
            session.add(Security(
                id=41, symbol="tgt", current_symbol="tgt", market="US", type="CS",
                name="Target Co Common Stock", is_active=False, delist_date=self.DELIST,
                cik="0000000411", full_refresh_interval=30,
            ))
            session.add(DailyPrice(
                security_id=41, date=self.DELIST - timedelta(days=2),
                close=Decimal("10.00"), volume=1000,
            ))
            session.add(SecFiling(
                source="SEC_EDGAR", cik="0000000411", form_type="8-K",
                accession_number="0001-25-90041",
                filing_date=self.DELIST + timedelta(days=3),
                items="2.01,9.01", primary_document_url="https://sec.test/stockleg.htm",
            ))
            # 收购方：活跃 CS，名字精确匹配 "Eagle Corp."，final_price_date 当日收盘 22.00
            session.add(Security(
                id=42, symbol="eagl", current_symbol="eagl", market="US", type="CS",
                name="Eagle Corp.", is_active=True, full_refresh_interval=30,
            ))
            session.add(DailyPrice(
                security_id=42, date=self.DELIST - timedelta(days=2),
                close=Decimal("22.00"), volume=5000,
            ))
            session.commit()

    def test_stock_leg_return_from_resolved_acquirer_close(self, pg_db, monkeypatch):
        self._seed(pg_db)
        import scripts.build_delisting_events as bde
        monkeypatch.setattr(
            bde, "_edgar_fetch_text",
            lambda: {"https://sec.test/stockleg.htm": STOCK_LEG_8K_HTML}.__getitem__,
        )
        assert run(_args("--apply", "--fetch-8k-docs"), pg_db) == 0

        with pg_db.engine.connect() as conn:
            row = conn.execute(text(
                "SELECT * FROM delisting_events WHERE security_id = 41"
            )).one()
        assert row.reason_code == "ACQUISITION_STOCK"
        assert row.consideration_stock_ratio == Decimal("0.5000000000")
        assert row.acquirer_name == "Eagle Corp."
        # implied = 0.5 × 22.00 = 11.00；return = (11.00 - 10.00) / 10.00 = 0.10
        assert row.delisting_return == Decimal("0.10000000")
        assert "acquirer_security=eagl#42" in row.evidence

    def test_acquirer_close_lookback_when_exact_day_missing(self, pg_db, monkeypatch):
        # 收购方 final_price_date 当日无 bar，回看 3 天内取最近 close（unnest join 的
        # BETWEEN fpd-3 AND fpd 分支，多命中取 max(date)）
        self._seed(pg_db)
        with pg_db.engine.connect() as conn:
            conn.execute(text(
                "DELETE FROM daily_prices WHERE security_id = 42"
            ))
            conn.execute(text(
                "INSERT INTO daily_prices (security_id, date, close, volume) "
                "VALUES (42, :d, 22.00, 5000)"
            ), {"d": self.DELIST - timedelta(days=4)})   # 窗口内较旧的 bar，应被忽略
            conn.execute(text(
                "INSERT INTO daily_prices (security_id, date, close, volume) "
                "VALUES (42, :d, 20.00, 5000)"
            ), {"d": self.DELIST - timedelta(days=3)})   # 窗口内最近的 bar，应被采用
            conn.commit()

        import scripts.build_delisting_events as bde
        monkeypatch.setattr(
            bde, "_edgar_fetch_text",
            lambda: {"https://sec.test/stockleg.htm": STOCK_LEG_8K_HTML}.__getitem__,
        )
        assert run(_args("--apply", "--fetch-8k-docs"), pg_db) == 0
        with pg_db.engine.connect() as conn:
            row = conn.execute(text(
                "SELECT * FROM delisting_events WHERE security_id = 41"
            )).one()
        # final_price_date = DELIST-2；当日无 bar，回看命中最近的 DELIST-3 close 20.00
        # implied = 0.5 × 20.00 = 10.00；return = (10.00 - 10.00) / 10.00 = 0.00
        assert row.delisting_return == Decimal("0.00000000")


# ---------------------------------------------------------------------------
# PostgreSQL 集成：S1 换股句收购方解锁 + S2 自指守卫
# ---------------------------------------------------------------------------

# S1：收购方名字只出现在换股句里（无 "acquired by"/"subsidiary of" 措辞）——
# 通用 acquirer 抽不出，stock_acquirer 兜住，换股腿仍解析出 return
RATIO_ONLY_8K_HTML = (
    "<html><body><p>On June 27, 2025, the transactions contemplated by the merger "
    "agreement were consummated. Each share of Company common stock was converted "
    "into 0.5000 shares of Eagle Corp. common stock for each share held.</p></body></html>"
)
# S2：完成 8-K 说 "merger with <本壳自身>"——收购方被抽成壳自己的名字
SELF_MERGER_8K_HTML = (
    "<html><body><p>On June 27, 2025, the Company completed its merger with Target Co. "
    "Each share was converted into 0.5000 shares of Target Co. common stock for each "
    "share held.</p></body></html>"
)


@pytest.mark.integration
class TestStockAcquirerUnlockAndSelfGuardPg:
    DELIST = date(2025, 6, 30)

    def _seed(self, pg_db, doc_url, target_name="Target Co Common Stock"):
        from data_models.models import DailyPrice, SecFiling, Security

        with pg_db.get_session() as session:
            session.add(Security(
                id=61, symbol="tgt", current_symbol="tgt", market="US", type="CS",
                name=target_name, is_active=False, delist_date=self.DELIST,
                cik="0000000611", full_refresh_interval=30,
            ))
            session.add(DailyPrice(
                security_id=61, date=self.DELIST - timedelta(days=2),
                close=Decimal("10.00"), volume=1000,
            ))
            session.add(SecFiling(
                source="SEC_EDGAR", cik="0000000611", form_type="8-K",
                accession_number="0001-25-90061",
                filing_date=self.DELIST + timedelta(days=3),
                items="2.01,9.01", primary_document_url=doc_url,
            ))
            # 收购方：活跃 CS，名字 "Eagle Corp."，final_price_date 当日收盘 22.00
            session.add(Security(
                id=62, symbol="eagl", current_symbol="eagl", market="US", type="CS",
                name="Eagle Corp.", is_active=True, full_refresh_interval=30,
            ))
            session.add(DailyPrice(
                security_id=62, date=self.DELIST - timedelta(days=2),
                close=Decimal("22.00"), volume=5000,
            ))
            session.commit()

    def test_ratio_sentence_acquirer_unlocks_stock_leg(self, pg_db, monkeypatch):
        # S1：通用 acquirer 措辞缺席，靠换股句里的 "Eagle Corp." 解析出收购方 → 写 return
        self._seed(pg_db, "https://sec.test/ratioonly.htm")
        import scripts.build_delisting_events as bde
        monkeypatch.setattr(
            bde, "_edgar_fetch_text",
            lambda: {"https://sec.test/ratioonly.htm": RATIO_ONLY_8K_HTML}.__getitem__,
        )
        assert run(_args("--apply", "--fetch-8k-docs"), pg_db) == 0
        with pg_db.engine.connect() as conn:
            row = conn.execute(text(
                "SELECT * FROM delisting_events WHERE security_id = 61"
            )).one()
        assert row.reason_code == "ACQUISITION_STOCK"
        assert row.acquirer_name == "Eagle Corp."   # 来自换股句（通用 acquirer 缺席）
        assert row.delisting_return == Decimal("0.10000000")
        assert "stock_acquirer=Eagle Corp." in row.evidence
        assert "acquirer_security=eagl#62" in row.evidence

    def test_self_reference_acquirer_yields_no_return(self, pg_db, monkeypatch):
        # S2：8-K 说 "merger with Target Co"（壳自身名）——收购方解析被自指守卫拦下，
        # 不写伪 return，evidence 记 acquirer_self
        self._seed(pg_db, "https://sec.test/selfmerge.htm", target_name="Target Co")
        import scripts.build_delisting_events as bde
        monkeypatch.setattr(
            bde, "_edgar_fetch_text",
            lambda: {"https://sec.test/selfmerge.htm": SELF_MERGER_8K_HTML}.__getitem__,
        )
        assert run(_args("--apply", "--fetch-8k-docs"), pg_db) == 0
        with pg_db.engine.connect() as conn:
            row = conn.execute(text(
                "SELECT * FROM delisting_events WHERE security_id = 61"
            )).one()
        assert row.reason_code == "ACQUISITION_STOCK"
        assert row.delisting_return is None            # 自指 → 绝不写伪 return
        assert "acquirer_self" in row.evidence
        assert "acquirer_security=" not in row.evidence


# ---------------------------------------------------------------------------
# PostgreSQL 集成：证据 loader 排序稳定（F9 accession tiebreak）
# ---------------------------------------------------------------------------

@pytest.mark.integration
class TestLoaderOrderingStable:
    """F9：同 filing_date 的多份 filing 若无 accession tiebreak，跨重跑顺序不定
    → evidence 字符串抖动。三份 Form 25 同日、accession 乱序入库，loader 须按
    accession 升序稳定返回。"""
    def test_form25_filings_ordered_by_accession_within_same_date(self, pg_db):
        from data_models.models import SecFiling, Security

        with pg_db.get_session() as session:
            session.add(Security(
                id=51, symbol="ord", current_symbol="ord", market="US", type="CS",
                is_active=False, delist_date=date(2025, 6, 30), cik="0000000511",
                full_refresh_interval=30,
            ))
            for acc in ("0009-25-000009", "0001-25-000001", "0005-25-000005"):
                session.add(SecFiling(
                    source="SEC_EDGAR", cik="0000000511", form_type="25-NSE",
                    accession_number=acc, filing_date=date(2025, 6, 25),
                    primary_document_url=f"https://x/{acc}",
                ))
            session.commit()

        with pg_db.get_session() as session:
            got = load_form25_filings(session, [51])
        accns = [f.accession_number for f in got[51]]
        assert accns == sorted(accns)


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


class TestReturnConfidenceGate:
    """return 只认 HIGH 置信的并购族——MEDIUM identity-merge 是同实体延续。"""

    def test_medium_identity_merge_never_gets_consideration_return(self):
        security = DelistedSecurity(id=1, symbol="verb", type="CS", cik="1",
                                    delist_date=date(2025, 12, 1), name="Verb Tech")
        evidence = Evidence(
            merge_events=[MergeEvent(event_id=1, keep_security_id=2, keep_symbol="tonx")],
            consideration=ConsiderationExtraction(cash=Decimal("9.51")),
        )
        row = classify(security, evidence, final_price=Decimal("14.50"),
                       final_price_date=date(2025, 12, 1),
                       price_bucket=None, price_pattern=None)
        assert row["reason_code"] == "MERGER"
        assert row["reason_confidence"] == "MEDIUM"
        # DEFM14A 抽出的现金不是这只壳的退出对价——绝不落 return
        assert row["delisting_return"] is None
        # 对价证据照记（人工复核线索）
        assert "consideration_cash=9.51" in row["evidence"]
