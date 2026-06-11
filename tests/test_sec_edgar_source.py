import unittest
from datetime import date

from data_sources.sec_edgar_source import SecEdgarSource, cik_to_10digit, normalize_cik


class FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


class FakeSession:
    """带 headers 属性的最小 session 替身；按 URL 返回预置 payload。"""

    def __init__(self, payload_by_url_part: dict):
        self.payload_by_url_part = payload_by_url_part
        self.headers = {}
        self.calls = []

    def mount(self, *args, **kwargs):
        return None

    def get(self, url, timeout=None):
        self.calls.append(url)
        for part, payload in self.payload_by_url_part.items():
            if part in url:
                return FakeResponse(payload)
        raise AssertionError(f"unexpected url: {url}")


def _source(payload_by_url_part):
    return SecEdgarSource(session=FakeSession(payload_by_url_part), user_agent="test test@example.com")


class CikNormalizationTests(unittest.TestCase):
    def test_normalize_strips_leading_zeros(self):
        self.assertEqual(normalize_cik("0000320193"), "320193")
        self.assertEqual(normalize_cik(320193), "320193")
        self.assertEqual(normalize_cik("0"), "0")
        self.assertIsNone(normalize_cik(None))
        self.assertIsNone(normalize_cik("not-a-cik"))

    def test_cik_to_10digit_pads(self):
        self.assertEqual(cik_to_10digit("320193"), "0000320193")
        self.assertEqual(cik_to_10digit(1045810), "0001045810")


class TickerMapTests(unittest.TestCase):
    def test_ticker_map_lowercases_and_converts_class_share_dash(self):
        payload = {
            "0": {"cik_str": 1067983, "ticker": "BRK-B", "title": "BERKSHIRE HATHAWAY INC"},
            "1": {"cik_str": 1045810, "ticker": "NVDA", "title": "NVIDIA CORP"},
        }
        rows = _source({"company_tickers.json": payload}).fetch_ticker_cik_map()

        by_ticker = {row["ticker"]: row for row in rows}
        # SEC 的 BRK-B 必须映射成库内的 brk.b，否则 21 个多类股全部匹配不上
        self.assertIn("brk.b", by_ticker)
        self.assertEqual(by_ticker["brk.b"]["cik"], "1067983")
        self.assertIn("nvda", by_ticker)


class FetchFilingsTests(unittest.TestCase):
    SUBMISSIONS = {
        "name": "Apple Inc.",
        "filings": {
            "recent": {
                "accessionNumber": ["0000320193-25-000079", "0001140361-26-023363"],
                "form": ["10-K", "4"],
                "filingDate": ["2025-10-31", "2026-05-29"],
                "reportDate": ["2025-09-27", "2026-05-28"],
                "acceptanceDateTime": ["2025-10-31T18:01:14.000Z", "2026-05-29T18:31:09.000Z"],
                "primaryDocument": ["aapl-20250927.htm", "xslF345X06/form4.xml"],
            },
            "files": [],
        },
    }

    def test_parses_rows_with_urls_and_dates(self):
        rows = _source({"CIK0000320193.json": self.SUBMISSIONS}).fetch_filings("320193")

        self.assertEqual(len(rows), 2)
        ten_k = next(r for r in rows if r["form_type"] == "10-K")
        self.assertEqual(ten_k["cik"], "0000320193")
        self.assertEqual(ten_k["filing_date"], date(2025, 10, 31))
        self.assertEqual(ten_k["period_of_report"], date(2025, 9, 27))
        self.assertEqual(ten_k["issuer_name"], "Apple Inc.")
        self.assertEqual(
            ten_k["filing_url"],
            "https://www.sec.gov/Archives/edgar/data/320193/000032019325000079/0000320193-25-000079-index.htm",
        )
        self.assertTrue(ten_k["primary_document_url"].endswith("/aapl-20250927.htm"))
        self.assertIsNotNone(ten_k["accepted_at"])

    def test_form_filter_and_since(self):
        source_forms = _source({"CIK0000320193.json": self.SUBMISSIONS})
        only_10k = source_forms.fetch_filings("320193", forms={"10-K"})
        self.assertEqual([r["form_type"] for r in only_10k], ["10-K"])

        source_since = _source({"CIK0000320193.json": self.SUBMISSIONS})
        recent_only = source_since.fetch_filings("320193", since=date(2026, 1, 1))
        self.assertEqual([r["form_type"] for r in recent_only], ["4"])


if __name__ == "__main__":
    unittest.main()
