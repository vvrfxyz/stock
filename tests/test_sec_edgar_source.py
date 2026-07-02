import unittest
from datetime import date
from unittest import mock

import requests

from data_sources.sec_edgar_source import (
    _FORM_INDEX_MAX_ATTEMPTS,
    SecEdgarSource,
    cik_to_10digit,
    normalize_cik,
    parse_company_facts,
)


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


class FakeHttpResponse:
    """带状态码/响应体的替身；raise_for_status 行为与 requests 对齐。"""

    def __init__(self, status_code=200, text=""):
        self.status_code = status_code
        self.text = text

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"status {self.status_code}", response=self)


class SequenceSession:
    """按调用顺序吐出预置响应；用于限流重试路径测试。"""

    def __init__(self, responses):
        self.responses = list(responses)
        self.headers = {}
        self.calls = []

    def mount(self, *args, **kwargs):
        return None

    def get(self, url, timeout=None):
        self.calls.append(url)
        return self.responses.pop(0)


# SEC 限流封禁页的两种签名文案（真实页面两句都有，这里各取一种覆盖 any() 分支）
RATE_LIMIT_BODY_THRESHOLD = "<html><p>Your request rate has exceeded the SEC's Request Rate Threshold.</p></html>"
RATE_LIMIT_BODY_AUTOMATED = "<html><h1>Your Request Originates from an Undeclared Automated Tool</h1></html>"


class DailyFormIndexTests(unittest.TestCase):
    DAY = date(2026, 6, 10)

    def _fetch(self, responses):
        session = SequenceSession(responses)
        source = SecEdgarSource(session=session, user_agent="test test@example.com")
        with mock.patch("data_sources.sec_edgar_source.time.sleep"):
            return source.fetch_daily_form_index(self.DAY), session

    def test_404_means_not_published_returns_none(self):
        result, session = self._fetch([FakeHttpResponse(404, "Not Found")])
        self.assertIsNone(result)
        self.assertEqual(len(session.calls), 1)

    def test_403_rate_limited_retries_then_succeeds(self):
        result, session = self._fetch(
            [
                FakeHttpResponse(403, RATE_LIMIT_BODY_THRESHOLD),
                FakeHttpResponse(403, RATE_LIMIT_BODY_AUTOMATED),
                FakeHttpResponse(200, "form index text"),
            ]
        )
        self.assertEqual(result, "form index text")
        self.assertEqual(len(session.calls), 3)

    def test_403_rate_limited_exhausted_raises_not_none(self):
        session = SequenceSession(
            [FakeHttpResponse(403, RATE_LIMIT_BODY_THRESHOLD)] * (_FORM_INDEX_MAX_ATTEMPTS + 2)
        )
        source = SecEdgarSource(session=session, user_agent="test test@example.com")
        with mock.patch("data_sources.sec_edgar_source.time.sleep"):
            with self.assertRaises(requests.HTTPError):
                source.fetch_daily_form_index(self.DAY)
        # 重试耗尽后必须抛出（本次运行失败），绝不能静默当"非工作日"
        self.assertEqual(len(session.calls), _FORM_INDEX_MAX_ATTEMPTS)

    def test_403_without_rate_limit_signature_raises_immediately(self):
        session = SequenceSession([FakeHttpResponse(403, "<html>Forbidden</html>")])
        source = SecEdgarSource(session=session, user_agent="test test@example.com")
        with self.assertRaises(requests.HTTPError):
            source.fetch_daily_form_index(self.DAY)
        self.assertEqual(len(session.calls), 1)

    def test_5xx_not_swallowed(self):
        session = SequenceSession([FakeHttpResponse(500, "Server Error")])
        source = SecEdgarSource(session=session, user_agent="test test@example.com")
        with self.assertRaises(requests.HTTPError):
            source.fetch_daily_form_index(self.DAY)


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


class ParseCompanyFactsTests(unittest.TestCase):
    PAYLOAD = {
        "facts": {
            "us-gaap": {
                "Revenues": {
                    "units": {
                        "USD": [
                            # Q2 10-Q 同 accession 同 end 给 3 个月 + 6 个月两条（start 不同）
                            {"start": "2025-12-28", "end": "2026-03-28", "val": 111184000000,
                             "accn": "0000320193-26-000013", "fy": 2026, "fp": "Q2", "form": "10-Q",
                             "filed": "2026-05-01", "frame": "CY2026Q1"},
                            {"start": "2025-09-28", "end": "2026-03-28", "val": 213500000000,
                             "accn": "0000320193-26-000013", "fy": 2026, "fp": "Q2", "form": "10-Q",
                             "filed": "2026-05-01"},
                        ]
                    }
                },
                "Assets": {
                    "units": {
                        "USD": [
                            # instant 型：无 start
                            {"end": "2026-03-28", "val": 371082000000, "accn": "0000320193-26-000013",
                             "fy": 2026, "fp": "Q2", "form": "10-Q", "filed": "2026-05-01",
                             "frame": "CY2026Q1I"},
                        ]
                    }
                },
                "NotInWhitelist": {
                    "units": {"USD": [{"end": "2026-03-28", "val": 1, "accn": "x", "filed": "2026-05-01"}]}
                },
            },
        }
    }
    CONCEPTS = {"us-gaap": {"Revenues", "Assets"}}

    def test_duration_pairs_with_same_end_both_kept(self):
        rows = parse_company_facts(self.PAYLOAD, "0000320193", concepts=self.CONCEPTS)
        revenues = [r for r in rows if r["concept"] == "Revenues"]
        # 3 个月与 6 个月两条都保留——period_start 必须参与唯一键，否则丢一条
        self.assertEqual(len(revenues), 2)
        starts = {r["period_start"] for r in revenues}
        self.assertEqual(starts, {date(2025, 12, 28), date(2025, 9, 28)})
        for r in revenues:
            self.assertFalse(r["is_instant"])

    def test_instant_fact_uses_end_as_start(self):
        rows = parse_company_facts(self.PAYLOAD, "0000320193", concepts=self.CONCEPTS)
        assets = next(r for r in rows if r["concept"] == "Assets")
        self.assertTrue(assets["is_instant"])
        self.assertEqual(assets["period_start"], assets["period_end"])
        self.assertEqual(assets["filed_date"], date(2026, 5, 1))

    def test_whitelist_and_filed_since_filtering(self):
        rows = parse_company_facts(self.PAYLOAD, "0000320193", concepts=self.CONCEPTS)
        self.assertFalse(any(r["concept"] == "NotInWhitelist" for r in rows))

        none_recent = parse_company_facts(
            self.PAYLOAD, "0000320193", concepts=self.CONCEPTS, filed_since=date(2026, 6, 1)
        )
        self.assertEqual(none_recent, [])


if __name__ == "__main__":
    unittest.main()
