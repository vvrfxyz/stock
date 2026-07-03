"""EDGAR form index 与 13F-HR 全文提交解析的单元测试。"""
from datetime import date
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from data_sources.sec_edgar_source import parse_form_index, parse_thirteenf_submission
from scripts.update_institutional_holdings import load_cusip_map, process_filing

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

    def test_quarterly_index_iso_dates_parsed(self):
        # quarterly full-index 的日期是 YYYY-MM-DD，daily 是 YYYYMMDD，两种都要认
        text = (
            "13F-HR           1 NORTH WEALTH SERVICES LLC                                   "
            "1641761     2026-02-13  edgar/data/1641761/0001641761-26-000001.txt\n"
        )
        rows = parse_form_index(text, {"13F-HR"})
        assert len(rows) == 1
        assert rows[0]["filing_date"] == date(2026, 2, 13)
        assert rows[0]["accession_number"] == "0001641761-26-000001"


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


# primary_doc XML 损坏（标签不闭合 -> ParseError）但 information table 完好
BROKEN_PRIMARY_DOC = THIRTEENF_SUBMISSION.replace("</edgarSubmission>", "</edgarSubmissionX>")

# SGML 头含 CONFORMED PERIOD OF REPORT / CENTRAL INDEX KEY 的损坏 primary_doc 变体
BROKEN_PRIMARY_WITH_HEADER = BROKEN_PRIMARY_DOC.replace(
    "CONFORMED SUBMISSION TYPE:\t13F-HR\n",
    "CONFORMED SUBMISSION TYPE:\t13F-HR\n"
    "CONFORMED PERIOD OF REPORT:\t20251231\n"
    "CENTRAL INDEX KEY:\t\t\t0001779506\n",
)


class TestSgmlHeaderFallback:
    def test_period_and_cik_backfilled_from_sgml_header(self):
        rows = parse_thirteenf_submission(BROKEN_PRIMARY_WITH_HEADER, "0001779506-26-000002")
        assert len(rows) == 2
        for row in rows:
            assert row["period"] == date(2025, 12, 31)
            assert row["form_type"] == "13F-HR"
            assert row["filer_cik"] == "0001779506"

    def test_period_stays_none_when_header_also_missing(self):
        rows = parse_thirteenf_submission(BROKEN_PRIMARY_DOC, "0001779506-26-000002")
        assert len(rows) == 2
        assert all(row["period"] is None for row in rows)
        # form_type 仍能从 CONFORMED SUBMISSION TYPE 回填
        assert all(row["form_type"] == "13F-HR" for row in rows)

    def test_intact_primary_doc_takes_precedence_over_header(self):
        with_header = THIRTEENF_SUBMISSION.replace(
            "CONFORMED SUBMISSION TYPE:\t13F-HR\n",
            "CONFORMED SUBMISSION TYPE:\t13F-HR\nCONFORMED PERIOD OF REPORT:\t20240630\n",
        )
        rows = parse_thirteenf_submission(with_header, "acc")
        assert all(row["period"] == date(2025, 12, 31) for row in rows)


def _ref() -> dict:
    return {
        "accession_number": "0001779506-26-000002",
        "filer_cik": "0001779506",
        "form_type": "13F-HR",
        "filing_date": date(2026, 6, 10),
        "file_path": "edgar/data/1779506/0001779506-26-000002.txt",
    }


class TestProcessFiling:
    def test_period_null_rejected_and_not_written(self):
        source = MagicMock()
        source.fetch_full_submission.return_value = BROKEN_PRIMARY_DOC
        db = MagicMock()
        with pytest.raises(ValueError, match="period"):
            process_filing(_ref(), source, db, {})
        db.upsert_institutional_holdings.assert_not_called()

    def test_gone_filing_404_skipped_not_failed(self):
        # form.idx 里的"幽灵" filing（EDGAR 已删除）：永久 404 应跳过而非抛错，
        # 否则历史回填中每个幽灵把所在季度永远标 FAILED。
        import requests

        source = MagicMock()
        response = MagicMock()
        response.status_code = 404
        source.fetch_full_submission.side_effect = requests.HTTPError(response=response)
        db = MagicMock()
        status, written = process_filing(_ref(), source, db, {})
        assert status == "SKIPPED_GONE"
        assert written == 0
        db.upsert_institutional_holdings.assert_not_called()

    def test_non_404_http_error_still_raises(self):
        import requests

        source = MagicMock()
        response = MagicMock()
        response.status_code = 503
        source.fetch_full_submission.side_effect = requests.HTTPError(response=response)
        db = MagicMock()
        with pytest.raises(requests.HTTPError):
            process_filing(_ref(), source, db, {})

    def test_period_backfilled_rows_written_with_cusip_map(self):
        source = MagicMock()
        source.fetch_full_submission.return_value = BROKEN_PRIMARY_WITH_HEADER
        db = MagicMock()
        db.upsert_institutional_holdings.return_value = 2
        status, written = process_filing(_ref(), source, db, {"037833100": 42})
        assert status == "SUCCESS"
        assert written == 2
        rows = db.upsert_institutional_holdings.call_args[0][0]
        assert all(row["period"] == date(2025, 12, 31) for row in rows)
        by_cusip = {row["cusip"]: row for row in rows}
        assert by_cusip["037833100"]["security_id"] == 42
        assert "security_id" not in by_cusip["78462F103"]


class TestLoadCusipMap:
    def test_ambiguous_cusip_excluded(self):
        session = MagicMock()
        session.query.return_value.filter.return_value.all.return_value = [
            ("037833100", 1),
            ("78462f103", 2),
            ("78462F103", 3),  # 大小写归一后同 CUSIP -> 一对多，整体剔除
            ("594918104", 7),
            ("594918104", 7),  # 重复行但同一 security -> 保留
        ]
        db = MagicMock()
        db.get_session.return_value.__enter__.return_value = session
        mapping = load_cusip_map(db)
        assert mapping == {"037833100": 1, "594918104": 7}
