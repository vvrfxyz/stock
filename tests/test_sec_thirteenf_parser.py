"""EDGAR form index 与 13F-HR 全文提交解析的单元测试。"""
from datetime import date
from decimal import Decimal

from data_sources.sec_edgar_source import parse_form_index, parse_thirteenf_submission

FORM_INDEX_TEXT = """Description:           Daily Index of EDGAR Dissemination Feed by Form Type
Last Data Received:    Jun 10, 2026
Comments:              webmaster@sec.gov
Anonymous FTP:         ftp://ftp.sec.gov/edgar/



Form Type   Company Name                                                  CIK
      Date Filed  File Name
---------------------------------------------------------------------------------------------------------------------------------------------
1-A POS          Neptune REM, LLC                                              1992001     20260610    edgar/data/1992001/0001104659-26-072427.txt
13F-HR           Brandywine Financial Group                                    1779506     20260610    edgar/data/1779506/0001779506-26-000002.txt
13F-HR/A         Curry Webb Wealth Management LLC                              2060114     20260610    edgar/data/2060114/0002060114-26-000003.txt
4                Doe John                                                      1234567     20260610    edgar/data/1234567/0001234567-26-000001.txt
"""

THIRTEENF_SUBMISSION = """<SEC-DOCUMENT>0001779506-26-000002.txt : 20260610
<SEC-HEADER>0001779506-26-000002.hdr.sgml : 20260610
ACCESSION NUMBER:\t\t0001779506-26-000002
CONFORMED SUBMISSION TYPE:\t13F-HR
</SEC-HEADER>
<DOCUMENT>
<TYPE>13F-HR
<SEQUENCE>1
<FILENAME>primary_doc.xml
<TEXT>
<XML>
<?xml version="1.0" encoding="UTF-8"?>
<edgarSubmission xmlns="http://www.sec.gov/edgar/thirteenffiler" xmlns:com="http://www.sec.gov/edgar/common">
  <headerData>
    <submissionType>13F-HR</submissionType>
    <filerInfo>
      <filer>
        <credentials><cik>0001779506</cik><ccc>XXXX</ccc></credentials>
      </filer>
      <periodOfReport>12-31-2025</periodOfReport>
    </filerInfo>
  </headerData>
  <formData>
    <coverPage>
      <reportCalendarOrQuarter>12-31-2025</reportCalendarOrQuarter>
      <filingManager><name>Brandywine Financial Group</name></filingManager>
      <reportType>13F HOLDINGS REPORT</reportType>
      <form13FFileNumber>028-27015</form13FFileNumber>
    </coverPage>
  </formData>
</edgarSubmission>
</XML>
</TEXT>
</DOCUMENT>
<DOCUMENT>
<TYPE>INFORMATION TABLE
<SEQUENCE>2
<FILENAME>20251231_BFG_13f.xml
<TEXT>
<XML>
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<ns1:informationTable xmlns:ns1="http://www.sec.gov/edgar/document/thirteenf/informationtable">
\t<ns1:infoTable>
\t\t<ns1:nameOfIssuer>APPLE INC</ns1:nameOfIssuer>
\t\t<ns1:titleOfClass>COM</ns1:titleOfClass>
\t\t<ns1:cusip>037833100</ns1:cusip>
\t\t<ns1:value>1864</ns1:value>
\t\t<ns1:shrsOrPrnAmt>
\t\t\t<ns1:sshPrnamt>10</ns1:sshPrnamt>
\t\t\t<ns1:sshPrnamtType>SH</ns1:sshPrnamtType>
\t\t</ns1:shrsOrPrnAmt>
\t\t<ns1:investmentDiscretion>SOLE</ns1:investmentDiscretion>
\t\t<ns1:votingAuthority>
\t\t\t<ns1:Sole>0</ns1:Sole>
\t\t\t<ns1:Shared>0</ns1:Shared>
\t\t\t<ns1:None>10</ns1:None>
\t\t</ns1:votingAuthority>
\t</ns1:infoTable>
\t<ns1:infoTable>
\t\t<ns1:nameOfIssuer>SPY PUTS</ns1:nameOfIssuer>
\t\t<ns1:titleOfClass>ETF</ns1:titleOfClass>
\t\t<ns1:cusip>78462F103</ns1:cusip>
\t\t<ns1:value>500</ns1:value>
\t\t<ns1:shrsOrPrnAmt>
\t\t\t<ns1:sshPrnamt>100</ns1:sshPrnamt>
\t\t\t<ns1:sshPrnamtType>SH</ns1:sshPrnamtType>
\t\t</ns1:shrsOrPrnAmt>
\t\t<ns1:putCall>Put</ns1:putCall>
\t\t<ns1:investmentDiscretion>DFND</ns1:investmentDiscretion>
\t\t<ns1:votingAuthority>
\t\t\t<ns1:Sole>100</ns1:Sole>
\t\t\t<ns1:Shared>0</ns1:Shared>
\t\t\t<ns1:None>0</ns1:None>
\t\t</ns1:votingAuthority>
\t</ns1:infoTable>
</ns1:informationTable>
</XML>
</TEXT>
</DOCUMENT>
</SEC-DOCUMENT>
"""


class TestParseFormIndex:
    def test_filters_forms_and_parses_fields(self):
        rows = parse_form_index(FORM_INDEX_TEXT, {"13F-HR", "13F-HR/A"})
        assert len(rows) == 2
        hr, hra = rows
        assert hr["form_type"] == "13F-HR"
        assert hr["filer_cik"] == "0001779506"
        assert hr["filing_date"] == date(2026, 6, 10)
        assert hr["file_path"] == "edgar/data/1779506/0001779506-26-000002.txt"
        assert hr["accession_number"] == "0001779506-26-000002"
        assert hra["form_type"] == "13F-HR/A"

    def test_form_type_with_spaces_not_confused_with_company(self):
        rows = parse_form_index(FORM_INDEX_TEXT, {"1-A POS"})
        assert len(rows) == 1
        assert rows[0]["filer_cik"] == "0001992001"

    def test_header_lines_skipped(self):
        rows = parse_form_index(FORM_INDEX_TEXT, {"10-K"})
        assert rows == []


class TestParseThirteenfSubmission:
    def test_holdings_rows_with_filer_metadata(self):
        rows = parse_thirteenf_submission(THIRTEENF_SUBMISSION, "0001779506-26-000002")
        assert len(rows) == 2
        aapl, spy = rows

        assert aapl["source"] == "SEC_EDGAR"
        assert aapl["accession_number"] == "0001779506-26-000002"
        assert aapl["filer_cik"] == "0001779506"
        assert aapl["filer_name"] == "Brandywine Financial Group"
        assert aapl["form_type"] == "13F-HR"
        assert aapl["period"] == date(2025, 12, 31)
        assert aapl["issuer_name"] == "APPLE INC"
        assert aapl["title_of_class"] == "COM"
        assert aapl["cusip"] == "037833100"
        assert aapl["market_value"] == Decimal("1864")
        assert aapl["shares_or_principal_amount"] == Decimal("10")
        assert aapl["shares_or_principal_type"] == "SH"
        assert aapl["put_call"] is None
        assert aapl["investment_discretion"] == "SOLE"
        assert aapl["voting_authority_none"] == Decimal("10")
        assert aapl["file_number"] == "028-27015"

        assert spy["put_call"] == "Put"
        assert spy["voting_authority_sole"] == Decimal("100")

    def test_row_hashes_stable_and_distinct(self):
        rows_a = parse_thirteenf_submission(THIRTEENF_SUBMISSION, "acc")
        rows_b = parse_thirteenf_submission(THIRTEENF_SUBMISSION, "acc")
        assert [r["source_row_hash"] for r in rows_a] == [r["source_row_hash"] for r in rows_b]
        assert rows_a[0]["source_row_hash"] != rows_a[1]["source_row_hash"]

    def test_submission_without_information_table_returns_empty(self):
        head, _, _ = THIRTEENF_SUBMISSION.partition("<DOCUMENT>\n<TYPE>INFORMATION TABLE")
        assert parse_thirteenf_submission(head, "acc") == []
