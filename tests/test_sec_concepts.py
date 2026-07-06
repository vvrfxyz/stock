"""锁定 SEC 摄取白名单的 ADR/IFRS 扩容口径（ADR 方案 Phase 0）。

- utils/sec_concepts.py 必须含 ifrs-full 概念块（TSM 等 20-F 申报方的 companyfacts
  挂在 ifrs-full taxonomy 下，缺块 = 334 个概念被静默丢弃）；
- update_sec_filings.DEFAULT_FORMS 必须含 6-K/A、40-F/A（外国私募发行人修订件）。
"""
import scripts.update_sec_filings as sec_filings
from utils.sec_concepts import CURATED_CONCEPTS


class TestCuratedConceptsIfrsBlock:
    def test_ifrs_full_block_exists_and_nonempty(self):
        assert "ifrs-full" in CURATED_CONCEPTS
        assert len(CURATED_CONCEPTS["ifrs-full"]) > 0

    def test_ifrs_full_covers_core_statement_anchors(self):
        """三大报表各锁若干经济锚点，防止块被误删/误改成空壳。"""
        assert {
            # 利润表
            "Revenue",
            "ProfitLoss",
            "ProfitLossAttributableToOwnersOfParent",
            "BasicEarningsLossPerShare",
            # 资产负债表
            "Assets",
            "Equity",
            "EquityAttributableToOwnersOfParent",
            "CashAndCashEquivalents",
            # 现金流量表
            "CashFlowsFromUsedInOperatingActivities",
            # 股数（PIT 股本链路）
            "NumberOfSharesOutstanding",
        } <= CURATED_CONCEPTS["ifrs-full"]

    def test_existing_taxonomies_untouched(self):
        """ifrs-full 是新增块，us-gaap/dei 既有口径不得被挤掉。"""
        assert "Revenues" in CURATED_CONCEPTS["us-gaap"]
        assert "NetIncomeLoss" in CURATED_CONCEPTS["us-gaap"]
        assert "EntityCommonStockSharesOutstanding" in CURATED_CONCEPTS["dei"]

    def test_concept_names_are_clean_strings(self):
        for taxonomy, concepts in CURATED_CONCEPTS.items():
            for concept in concepts:
                assert isinstance(concept, str) and concept == concept.strip() and concept, (
                    taxonomy,
                    concept,
                )


class TestDefaultFormsAdrAmendments:
    def test_default_forms_include_foreign_issuer_amendments(self):
        assert {"6-K/A", "40-F/A"} <= sec_filings.DEFAULT_FORMS

    def test_default_forms_keep_base_foreign_issuer_forms(self):
        assert {"20-F", "20-F/A", "40-F", "6-K"} <= sec_filings.DEFAULT_FORMS
