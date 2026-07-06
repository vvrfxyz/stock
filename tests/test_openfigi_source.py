"""OpenFigiSource 单元测试：mock requests session、monkeypatch time.sleep。"""
from __future__ import annotations

import traceback

import pytest
from requests.exceptions import ConnectionError as RequestsConnectionError

from data_sources import openfigi_source
from data_sources.openfigi_source import OPENFIGI_MAPPING_URL, OpenFigiSource


class FakeResponse:
    def __init__(self, payload, status_code=200, headers=None, text=None):
        self.payload = payload
        self.status_code = status_code
        self.headers = headers or {}
        self.text = text if text is not None else str(payload)

    def raise_for_status(self):
        if self.status_code >= 400:
            from requests import HTTPError

            raise HTTPError(response=self)

    def json(self):
        return self.payload


class FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def post(self, url, json=None, headers=None, timeout=None):
        self.calls.append({"url": url, "json": json, "headers": headers, "timeout": timeout})
        result = self.responses.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


@pytest.fixture
def sleep_calls(monkeypatch):
    """屏蔽真实 sleep（节流/退避都走它），并记录调用参数。"""
    calls: list[float] = []
    monkeypatch.setattr(openfigi_source.time, "sleep", lambda seconds: calls.append(seconds))
    return calls


def _candidate(figi, **overrides):
    row = {
        "compositeFIGI": figi,
        "shareClassFIGI": f"{figi}-SC",
        "ticker": "AAPL",
        "name": "APPLE INC",
        "securityType": "Common Stock",
        "marketSector": "Equity",
        "exchCode": "US",
    }
    row.update(overrides)
    return row


def test_multi_exchange_candidates_merge_to_single_matched(sleep_calls):
    session = FakeSession(
        [
            FakeResponse(
                [
                    {
                        "data": [
                            _candidate("BBG000B9XRY4", exchCode="US"),
                            _candidate("BBG000B9XRY4", exchCode="UN"),
                            _candidate("BBG000B9XRY4", exchCode="UW"),
                        ]
                    }
                ]
            )
        ]
    )
    source = OpenFigiSource(api_key="", session=session)

    results = source.map_cusips(["037833100"])

    assert len(session.calls) == 1
    row = results["037833100"]
    assert row["status"] == "MATCHED"
    assert row["composite_figi"] == "BBG000B9XRY4"
    assert row["share_class_figi"] == "BBG000B9XRY4-SC"
    assert row["ticker"] == "AAPL"
    assert row["name"] == "APPLE INC"
    assert row["security_type"] == "Common Stock"
    assert row["market_sector"] == "Equity"
    assert row["exch_code"] == "US"  # 第一条候选的字段


def test_distinct_composite_figis_yield_ambiguous(sleep_calls):
    # _candidate 默认 exchCode='US'：两个不同的 US composite -> 消歧不了，保持 AMBIGUOUS
    session = FakeSession(
        [
            FakeResponse(
                [
                    {
                        "data": [
                            _candidate("BBG000AAAAA1", name="FIRST CO"),
                            _candidate("BBG000BBBBB2", name="SECOND CO"),
                        ]
                    }
                ]
            )
        ]
    )
    source = OpenFigiSource(api_key="", session=session)

    row = source.map_cusips(["12345678X"])["12345678X"]

    assert row["status"] == "AMBIGUOUS"
    assert row["composite_figi"] is None
    assert row["share_class_figi"] is None
    assert row["ticker"] is None
    assert row["security_type"] is None
    assert row["market_sector"] is None
    assert row["exch_code"] is None
    assert row["name"] == "FIRST CO"  # 保留第一条 name 供诊断


def test_unique_us_composite_disambiguates_multi_listing_to_matched(sleep_calls):
    # ADR 跨市场多上市（BABA 型）：他国 composite + 唯一 US composite（含其
    # 下属交易所行同 FIGI）-> 以 US composite MATCHED，字段取 US 行
    session = FakeSession(
        [
            FakeResponse(
                [
                    {
                        "data": [
                            _candidate("BBG00KVTBY91", exchCode="HK", ticker="9988",
                                       name="ALIBABA GROUP HOLDING LTD"),
                            _candidate("BBG006G2JVL2", exchCode="US", ticker="BABA",
                                       name="ALIBABA GRP-ADR"),
                            _candidate("BBG006G2JVL2", exchCode="UN", ticker="BABA",
                                       name="ALIBABA GRP-ADR"),
                        ]
                    }
                ]
            )
        ]
    )
    source = OpenFigiSource(api_key="", session=session)

    row = source.map_cusips(["01609W102"])["01609W102"]

    assert row["status"] == "MATCHED"
    assert row["composite_figi"] == "BBG006G2JVL2"
    assert row["share_class_figi"] == "BBG006G2JVL2-SC"
    assert row["ticker"] == "BABA"
    assert row["name"] == "ALIBABA GRP-ADR"
    assert row["exch_code"] == "US"


def test_multi_composite_zero_us_stays_ambiguous(sleep_calls):
    session = FakeSession(
        [
            FakeResponse(
                [
                    {
                        "data": [
                            _candidate("BBG000AAAAA1", exchCode="LN", name="FIRST CO"),
                            _candidate("BBG000BBBBB2", exchCode="GR", name="SECOND CO"),
                        ]
                    }
                ]
            )
        ]
    )
    source = OpenFigiSource(api_key="", session=session)

    row = source.map_cusips(["12345678X"])["12345678X"]

    assert row["status"] == "AMBIGUOUS"
    assert row["composite_figi"] is None
    assert row["name"] == "FIRST CO"


def test_multi_composite_multiple_us_composites_stay_ambiguous(sleep_calls):
    session = FakeSession(
        [
            FakeResponse(
                [
                    {
                        "data": [
                            _candidate("BBG000AAAAA1", exchCode="US", name="US ONE"),
                            _candidate("BBG000BBBBB2", exchCode="US", name="US TWO"),
                            _candidate("BBG000CCCCC3", exchCode="LN", name="FOREIGN"),
                        ]
                    }
                ]
            )
        ]
    )
    source = OpenFigiSource(api_key="", session=session)

    row = source.map_cusips(["12345678X"])["12345678X"]

    assert row["status"] == "AMBIGUOUS"
    assert row["composite_figi"] is None
    assert row["name"] == "US ONE"


def test_us_row_missing_composite_figi_is_not_an_anchor(sleep_calls):
    # exchCode='US' 但 compositeFIGI 缺失的行不构成消歧锚点
    session = FakeSession(
        [
            FakeResponse(
                [
                    {
                        "data": [
                            _candidate(None, exchCode="US", name="NO FIGI US"),
                            _candidate("BBG000AAAAA1", exchCode="LN"),
                            _candidate("BBG000BBBBB2", exchCode="GR"),
                        ]
                    }
                ]
            )
        ]
    )
    source = OpenFigiSource(api_key="", session=session)

    row = source.map_cusips(["12345678X"])["12345678X"]

    assert row["status"] == "AMBIGUOUS"
    assert row["composite_figi"] is None
    assert row["name"] == "NO FIGI US"


def test_warning_and_empty_data_are_not_found(sleep_calls):
    session = FakeSession(
        [
            FakeResponse(
                [
                    {"warning": "No identifier found."},
                    {"data": []},
                    {"error": "invalid request"},
                ]
            )
        ]
    )
    source = OpenFigiSource(api_key="", session=session)

    results = source.map_cusips(["11111111A", "22222222B", "33333333C"])

    for cusip in ("11111111A", "22222222B", "33333333C"):
        assert results[cusip]["status"] == "NOT_FOUND"
        assert results[cusip]["composite_figi"] is None
        assert results[cusip]["name"] is None


def test_anonymous_mode_splits_eleven_cusips_into_two_requests(sleep_calls):
    cusips = [f"00000000{index}" for index in range(9)] + ["11111111A", "22222222B"]
    assert len(cusips) == 11
    session = FakeSession(
        [
            FakeResponse([{"data": [_candidate("BBG00000000%d" % i)]} for i in range(10)]),
            FakeResponse([{"data": [_candidate("BBG000000010")]}]),
        ]
    )
    source = OpenFigiSource(api_key="", session=session)
    assert source.batch_size == 10  # 匿名档

    results = source.map_cusips(cusips)

    assert len(session.calls) == 2
    assert len(session.calls[0]["json"]) == 10
    assert len(session.calls[1]["json"]) == 1
    assert session.calls[0]["json"][0] == {"idType": "ID_CUSIP", "idValue": "000000000"}
    assert session.calls[0]["headers"] is None  # 匿名不带 key 头
    assert all(row["status"] == "MATCHED" for row in results.values())
    assert len(results) == 11


def test_keyed_mode_uses_large_batch_and_api_key_header(sleep_calls):
    session = FakeSession(
        [FakeResponse([{"data": [_candidate("BBG000B9XRY4")]}, {"data": [_candidate("BBG000BVPV84")]}])]
    )
    source = OpenFigiSource(api_key="unit-test-key", session=session)
    assert source.batch_size == 100  # keyed 档

    source.map_cusips(["037833100", "02079K305"])

    assert len(session.calls) == 1
    assert session.calls[0]["url"] == OPENFIGI_MAPPING_URL
    assert session.calls[0]["headers"] == {"X-OPENFIGI-APIKEY": "unit-test-key"}


def test_429_sleeps_retry_after_then_retries_same_batch(sleep_calls):
    session = FakeSession(
        [
            FakeResponse({}, status_code=429, headers={"Retry-After": "7"}),
            FakeResponse([{"data": [_candidate("BBG000B9XRY4")]}]),
        ]
    )
    source = OpenFigiSource(api_key="", session=session)

    results = source.map_cusips(["037833100"])

    assert len(session.calls) == 2
    assert session.calls[0]["json"] == session.calls[1]["json"]  # 重试同批
    assert 7.0 in sleep_calls  # 尊重 Retry-After
    assert results["037833100"]["status"] == "MATCHED"


def test_429_exhaustion_raises_runtime_error(sleep_calls):
    responses = [FakeResponse({}, status_code=429) for _ in range(4)]  # 首发 + 3 次重试
    session = FakeSession(responses)
    source = OpenFigiSource(api_key="", session=session, max_retries=3)

    with pytest.raises(RuntimeError, match="429"):
        source.map_cusips(["037833100"])

    assert len(session.calls) == 4


def test_invalid_length_cusips_skip_request(sleep_calls):
    session = FakeSession([])
    source = OpenFigiSource(api_key="", session=session)

    results = source.map_cusips(["ABC", "  0378331000 ", ""])

    assert len(session.calls) == 0  # 一个请求都不发
    assert results["ABC"]["status"] == "NOT_FOUND"
    assert results["0378331000"]["status"] == "NOT_FOUND"
    assert "" not in results  # 空串不产生缓存行


def test_input_cleaning_uppercases_and_dedupes(sleep_calls):
    session = FakeSession([FakeResponse([{"data": [_candidate("BBG000B9XRY4")]}])])
    source = OpenFigiSource(api_key="", session=session)

    results = source.map_cusips([" 037833abc ", "037833ABC"])

    assert len(session.calls) == 1
    assert session.calls[0]["json"] == [{"idType": "ID_CUSIP", "idValue": "037833ABC"}]
    assert list(results) == ["037833ABC"]


def test_retry_exhaustion_masks_key_and_severs_exception_chain(sleep_calls):
    secret = "plain-secret-key"
    session = FakeSession(
        [
            RequestsConnectionError(
                f"HTTPSConnectionPool: mapping refused; headers=X-OPENFIGI-APIKEY: {secret}; apiKey={secret}"
            )
        ]
    )
    source = OpenFigiSource(api_key=secret, session=session, max_retries=0)

    with pytest.raises(RuntimeError) as exc_info:
        source.map_cusips(["037833100"])

    exc = exc_info.value
    assert secret not in str(exc)
    assert "***" in str(exc)
    assert exc.__cause__ is None
    assert exc.__suppress_context__ is True
    rendered = "".join(traceback.format_exception(exc))
    assert secret not in rendered


def test_http_error_message_is_masked(sleep_calls):
    secret = "leaky-secret-key"
    session = FakeSession(
        [FakeResponse({}, status_code=401, text=f"unauthorized key apiKey={secret}")]
    )
    source = OpenFigiSource(api_key=secret, session=session)

    with pytest.raises(RuntimeError) as exc_info:
        source.map_cusips(["037833100"])

    assert secret not in str(exc_info.value)
    assert exc_info.value.__cause__ is None


def test_api_key_falls_back_to_environment(monkeypatch, sleep_calls):
    monkeypatch.setenv("OPENFIGI_API_KEY", "env-key")
    session = FakeSession([FakeResponse([{"data": [_candidate("BBG000B9XRY4")]}])])

    source = OpenFigiSource(session=session)
    source.map_cusips(["037833100"])

    assert source.batch_size == 100
    assert session.calls[0]["headers"] == {"X-OPENFIGI-APIKEY": "env-key"}


def test_anonymous_when_env_unset(monkeypatch):
    monkeypatch.delenv("OPENFIGI_API_KEY", raising=False)

    source = OpenFigiSource(session=FakeSession([]))

    assert source.batch_size == 10
