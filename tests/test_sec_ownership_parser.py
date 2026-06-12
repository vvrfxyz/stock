"""Form 3/4/5 ownershipDocument 解析与 URL 推导的单元测试。"""
from datetime import date
from decimal import Decimal

import pytest

from data_sources.sec_edgar_source import (
    parse_ownership_document,
    raw_ownership_xml_url,
)

FORM4_XML = """<?xml version="1.0"?>
<ownershipDocument>
    <schemaVersion>X0609</schemaVersion>
    <documentType>4</documentType>
    <periodOfReport>2026-05-27</periodOfReport>
    <issuer>
        <issuerCik>0000320193</issuerCik>
        <issuerName>Apple Inc.</issuerName>
        <issuerTradingSymbol>AAPL</issuerTradingSymbol>
    </issuer>
    <reportingOwner>
        <reportingOwnerId>
            <rptOwnerCik>0001214128</rptOwnerCik>
            <rptOwnerName>LEVINSON ARTHUR D</rptOwnerName>
        </reportingOwnerId>
        <reportingOwnerRelationship>
            <isDirector>true</isDirector>
            <isOfficer>0</isOfficer>
            <officerTitle></officerTitle>
        </reportingOwnerRelationship>
    </reportingOwner>
    <aff10b5One>false</aff10b5One>
    <nonDerivativeTable>
        <nonDerivativeTransaction>
            <securityTitle><value>Common Stock</value></securityTitle>
            <transactionDate><value>2026-05-27</value></transactionDate>
            <transactionCoding>
                <transactionFormType>4</transactionFormType>
                <transactionCode>S</transactionCode>
                <equitySwapInvolved>0</equitySwapInvolved>
            </transactionCoding>
            <transactionAmounts>
                <transactionShares><value>50000</value></transactionShares>
                <transactionPricePerShare>
                    <value>311.02</value>
                    <footnoteId id="F1"/>
                </transactionPricePerShare>
                <transactionAcquiredDisposedCode><value>D</value></transactionAcquiredDisposedCode>
            </transactionAmounts>
            <postTransactionAmounts>
                <sharesOwnedFollowingTransaction><value>3764576</value></sharesOwnedFollowingTransaction>
            </postTransactionAmounts>
            <ownershipNature>
                <directOrIndirectOwnership><value>D</value></directOrIndirectOwnership>
            </ownershipNature>
        </nonDerivativeTransaction>
        <nonDerivativeHolding>
            <securityTitle><value>Common Stock</value></securityTitle>
            <postTransactionAmounts>
                <sharesOwnedFollowingTransaction><value>56000</value></sharesOwnedFollowingTransaction>
            </postTransactionAmounts>
            <ownershipNature>
                <directOrIndirectOwnership><value>I</value></directOrIndirectOwnership>
                <natureOfOwnership><value>By Spouse</value></natureOfOwnership>
            </ownershipNature>
        </nonDerivativeHolding>
    </nonDerivativeTable>
    <derivativeTable>
        <derivativeTransaction>
            <securityTitle><value>Restricted Stock Unit</value></securityTitle>
            <conversionOrExercisePrice><footnoteId id="F1"/></conversionOrExercisePrice>
            <transactionDate><value>2026-05-27</value></transactionDate>
            <transactionCoding>
                <transactionFormType>4</transactionFormType>
                <transactionCode>M</transactionCode>
                <equitySwapInvolved>0</equitySwapInvolved>
            </transactionCoding>
            <transactionAmounts>
                <transactionShares><value>540</value></transactionShares>
                <transactionPricePerShare><footnoteId id="F1"/></transactionPricePerShare>
                <transactionAcquiredDisposedCode><value>D</value></transactionAcquiredDisposedCode>
            </transactionAmounts>
            <exerciseDate><footnoteId id="F1"/></exerciseDate>
            <expirationDate><footnoteId id="F1"/></expirationDate>
            <underlyingSecurity>
                <underlyingSecurityTitle><value>Common Stock</value></underlyingSecurityTitle>
                <underlyingSecurityShares><value>540</value></underlyingSecurityShares>
            </underlyingSecurity>
            <postTransactionAmounts>
                <sharesOwnedFollowingTransaction><value>540</value></sharesOwnedFollowingTransaction>
            </postTransactionAmounts>
            <ownershipNature>
                <directOrIndirectOwnership><value>D</value></directOrIndirectOwnership>
            </ownershipNature>
        </derivativeTransaction>
    </derivativeTable>
    <footnotes>
        <footnote id="F1">Weighted average sale price.</footnote>
    </footnotes>
    <remarks>Some remark</remarks>
</ownershipDocument>
"""

FORM3_NO_HOLDINGS_XML = """<?xml version="1.0"?>
<ownershipDocument>
    <documentType>3</documentType>
    <periodOfReport>2026-01-05</periodOfReport>
    <noSecuritiesOwned>1</noSecuritiesOwned>
    <issuer>
        <issuerCik>123456</issuerCik>
        <issuerName>Foo Corp</issuerName>
        <issuerTradingSymbol>FOO</issuerTradingSymbol>
    </issuer>
    <reportingOwner>
        <reportingOwnerId>
            <rptOwnerCik>0000009999</rptOwnerCik>
            <rptOwnerName>DOE JANE</rptOwnerName>
        </reportingOwnerId>
        <reportingOwnerRelationship><isOfficer>1</isOfficer><officerTitle>CFO</officerTitle></reportingOwnerRelationship>
    </reportingOwner>
</ownershipDocument>
"""


class TestParseOwnershipDocument:
    def test_form4_rows_cover_transactions_holdings_and_derivatives(self):
        rows = parse_ownership_document(FORM4_XML, "0001140361-26-023363")

        assert len(rows) == 3
        sale, holding, rsu = rows

        assert sale["security_type"] == "NON_DERIVATIVE"
        assert sale["record_type"] == "TRANSACTION"
        assert sale["transaction_code"] == "S"
        assert sale["transaction_date"] == date(2026, 5, 27)
        assert sale["transaction_shares"] == Decimal("50000")
        assert sale["transaction_price_per_share"] == Decimal("311.02")
        assert sale["transaction_value"] == Decimal("50000") * Decimal("311.02")
        assert sale["transaction_acquired_disposed"] == "D"
        assert sale["shares_owned_following_transaction"] == Decimal("3764576")
        assert sale["direct_or_indirect"] == "D"
        assert sale["footnotes"] == "Weighted average sale price."
        assert sale["equity_swap_involved"] is False

        assert holding["record_type"] == "HOLDING"
        assert holding["transaction_code"] is None
        assert holding["direct_or_indirect"] == "I"
        assert holding["shares_owned_following_transaction"] == Decimal("56000")

        assert rsu["security_type"] == "DERIVATIVE"
        assert rsu["transaction_code"] == "M"
        assert rsu["underlying_security_title"] == "Common Stock"
        assert rsu["underlying_security_shares"] == Decimal("540")
        assert rsu["transaction_price_per_share"] is None
        assert rsu["transaction_value"] is None

    def test_document_level_fields_propagate(self):
        rows = parse_ownership_document(FORM4_XML, "acc-1")
        for row in rows:
            assert row["source"] == "SEC_EDGAR"
            assert row["accession_number"] == "acc-1"
            assert row["form_type"] == "4"
            assert row["period_of_report"] == date(2026, 5, 27)
            assert row["issuer_cik"] == "0000320193"
            assert row["issuer_trading_symbol"] == "aapl"
            assert row["owner_cik"] == "0001214128"
            assert row["owner_name"] == "LEVINSON ARTHUR D"
            assert row["is_director"] is True
            assert row["is_officer"] is False
            assert row["officer_title"] is None
            assert row["aff_10b5_one"] is False
            assert row["remarks"] == "Some remark"

    def test_row_hashes_are_stable_and_unique(self):
        rows_a = parse_ownership_document(FORM4_XML, "acc-1")
        rows_b = parse_ownership_document(FORM4_XML, "acc-1")
        hashes_a = [r["source_row_hash"] for r in rows_a]
        hashes_b = [r["source_row_hash"] for r in rows_b]
        assert hashes_a == hashes_b
        assert len(set(hashes_a)) == len(hashes_a)

    def test_form3_with_no_holdings_yields_no_rows(self):
        rows = parse_ownership_document(FORM3_NO_HOLDINGS_XML, "acc-3")
        assert rows == []

    def test_multiple_owners_duplicate_rows_with_distinct_hashes(self):
        xml = FORM4_XML.replace(
            "</reportingOwner>",
            """</reportingOwner>
            <reportingOwner>
                <reportingOwnerId>
                    <rptOwnerCik>0000008888</rptOwnerCik>
                    <rptOwnerName>TRUST FUND LLC</rptOwnerName>
                </reportingOwnerId>
                <reportingOwnerRelationship><isTenPercentOwner>1</isTenPercentOwner></reportingOwnerRelationship>
            </reportingOwner>""",
            1,
        )
        rows = parse_ownership_document(xml, "acc-1")
        assert len(rows) == 6
        assert len({r["source_row_hash"] for r in rows}) == 6
        owner_ciks = {r["owner_cik"] for r in rows}
        assert owner_ciks == {"0001214128", "0000008888"}


class TestRawOwnershipXmlUrl:
    def test_strips_xsl_prefix(self):
        url = "https://www.sec.gov/Archives/edgar/data/320193/000114036126023363/xslF345X06/form4.xml"
        assert raw_ownership_xml_url(url) == (
            "https://www.sec.gov/Archives/edgar/data/320193/000114036126023363/form4.xml"
        )

    def test_plain_xml_url_unchanged(self):
        url = "https://www.sec.gov/Archives/edgar/data/1/000000000100000001/form4.xml"
        assert raw_ownership_xml_url(url) == url

    @pytest.mark.parametrize("bad", [None, "", "https://x/doc.html", "https://x/xslF345X06/doc.htm"])
    def test_non_xml_returns_none(self, bad):
        assert raw_ownership_xml_url(bad) is None
