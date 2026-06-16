"""ECB 汇率解析、USD 交叉换算与非 USD 分红因子折算的单元测试。"""
from datetime import date
from decimal import Decimal
from types import SimpleNamespace

import pytest

from data_sources.ecb_fx_source import parse_ecb_fx_csv
from data_sources.fred_source import parse_fred_observations
from scripts.update_adjustment_factors import (
    compute_adjustment_factor_rows,
    evaluate_vendor_comparison,
)

ECB_CSV = """Date,USD,JPY,CAD,ILS,
2026-06-11,1.1537,185.21,1.6127,3.4137,
2026-06-10,1.1500,184.90,1.6100,N/A,
2026-06-05,1.1400,184.00,1.6000,3.4000,
"""


class TestParseEcbCsv:
    def test_rows_parsed_and_na_skipped(self):
        rows = parse_ecb_fx_csv(ECB_CSV)
        by_key = {(r["rate_date"], r["quote_currency"]): r for r in rows}
        assert by_key[(date(2026, 6, 11), "USD")]["rate"] == Decimal("1.1537")
        assert by_key[(date(2026, 6, 11), "CAD")]["base_currency"] == "EUR"
        assert by_key[(date(2026, 6, 11), "CAD")]["source"] == "ECB"
        assert (date(2026, 6, 10), "ILS") not in by_key  # N/A 跳过
        assert all(r["base_currency"] == "EUR" for r in rows)

    def test_since_filter(self):
        rows = parse_ecb_fx_csv(ECB_CSV, since=date(2026, 6, 10))
        assert {r["rate_date"] for r in rows} == {date(2026, 6, 10), date(2026, 6, 11)}


FRED_PAYLOAD = {
    "observations": [
        {"date": "2026-06-05", "value": "4.28"},
        {"date": "2026-06-06", "value": "."},
        {"date": "2026-06-08", "value": "4.30"},
    ]
}


class TestParseFredObservations:
    def test_rows_parsed_and_missing_skipped(self):
        rows = parse_fred_observations(FRED_PAYLOAD)

        assert rows == [
            {"date": date(2026, 6, 5), "series_id": "DTB3", "rate_pct": Decimal("4.28")},
            {"date": date(2026, 6, 8), "series_id": "DTB3", "rate_pct": Decimal("4.30")},
        ]

    def test_missing_observations_key_raises(self):
        with pytest.raises(ValueError, match="missing 'observations'"):
            parse_fred_observations({})

    def test_bad_date_and_bad_value_raise(self):
        with pytest.raises(ValueError, match="invalid observation date"):
            parse_fred_observations({"observations": [{"date": "not-a-date", "value": "4.28"}]})
        with pytest.raises(ValueError, match="invalid rate"):
            parse_fred_observations({"observations": [{"date": "2026-06-05", "value": "bad"}]})

    def test_empty_observations_raises(self):
        with pytest.raises(ValueError, match="contained no DTB3 rows"):
            parse_fred_observations({"observations": []})


class _StubFx:
    """rate_to_usd 的测试替身。"""

    def __init__(self, rates):
        self._rates = rates

    def rate_to_usd(self, currency, on_date):
        return self._rates.get(currency.upper())


def _dividend(currency, amount, event_id="div-cad"):
    return SimpleNamespace(
        action_type="DIVIDEND",
        ex_date=date(2026, 2, 10),
        cash_amount=Decimal(amount),
        currency=currency,
        split_from=None,
        split_to=None,
        source="MASSIVE",
        source_event_id=event_id,
    )


PRICE_DATES = [date(2026, 2, 9)]
CLOSE_BY_DATE = {date(2026, 2, 9): Decimal("100")}


class TestNonUsdDividendConversion:
    def test_converted_with_fx_rate(self):
        # 1 CAD = 0.73 USD：2 CAD 分红折 1.46 USD -> factor (100-1.46)/100
        rows, stats = compute_adjustment_factor_rows(
            1, [_dividend("CAD", "2")], PRICE_DATES, CLOSE_BY_DATE,
            methodology_version="raw_actions_v1", as_of_date=date(2026, 5, 13),
            fx_converter=_StubFx({"CAD": Decimal("0.73")}),
        )
        assert stats["FX_CONVERTED_DIVIDEND"] == 1
        assert len(rows) == 1
        assert rows[0]["single_event_factor"] == Decimal("0.985400000000")

    def test_skipped_without_converter_or_rate(self):
        for converter in (None, _StubFx({})):
            rows, stats = compute_adjustment_factor_rows(
                1, [_dividend("CAD", "2")], PRICE_DATES, CLOSE_BY_DATE,
                methodology_version="raw_actions_v1", as_of_date=date(2026, 5, 13),
                fx_converter=converter,
            )
            assert rows == []
            assert stats["SKIP_NON_USD_DIVIDEND"] == 1

    def test_usd_dividend_hash_unchanged_by_fx_support(self):
        """USD 事件的 event_hash 不能因为引入 FX 字段而改变（避免全量 cache 抖动）。"""
        kwargs = dict(
            methodology_version="raw_actions_v1", as_of_date=date(2026, 5, 13),
        )
        rows_plain, _ = compute_adjustment_factor_rows(
            1, [_dividend("USD", "2")], PRICE_DATES, CLOSE_BY_DATE, **kwargs
        )
        rows_with_fx, _ = compute_adjustment_factor_rows(
            1, [_dividend("USD", "2")], PRICE_DATES, CLOSE_BY_DATE,
            fx_converter=_StubFx({"CAD": Decimal("0.73")}), **kwargs
        )
        assert rows_plain[0]["event_hash"] == rows_with_fx[0]["event_hash"]

    def test_fx_rate_change_changes_event_hash(self):
        base = dict(methodology_version="raw_actions_v1", as_of_date=date(2026, 5, 13))
        rows_a, _ = compute_adjustment_factor_rows(
            1, [_dividend("CAD", "2")], PRICE_DATES, CLOSE_BY_DATE,
            fx_converter=_StubFx({"CAD": Decimal("0.73")}), **base
        )
        rows_b, _ = compute_adjustment_factor_rows(
            1, [_dividend("CAD", "2")], PRICE_DATES, CLOSE_BY_DATE,
            fx_converter=_StubFx({"CAD": Decimal("0.74")}), **base
        )
        assert rows_a[0]["event_hash"] != rows_b[0]["event_hash"]


class TestVendorComparisonExcludesNonUsd:
    TOL = Decimal("0.00001")
    AS_OF = date(2026, 5, 13)

    def test_fx_converted_dividend_not_in_comparison_chain(self):
        """vendor 链不含非 USD 分红；对账链若计入，其后历史分红必然失败。"""
        rows = [
            {  # FX 折算事件：vendor 无行
                "date": date(2026, 3, 1), "factor_key": "dividend:cad", "action_type": "DIVIDEND",
                "cumulative_factor": None, "single_event_factor": Decimal("0.985400000000"),
                "currency": "CAD", "adjustment_factor": None, "vendor_as_of": None,
            },
            {  # 更早的 USD 分红：vendor 链只含它自己
                "date": date(2026, 1, 1), "factor_key": "dividend:usd", "action_type": "DIVIDEND",
                "cumulative_factor": None, "single_event_factor": Decimal("0.990000000000"),
                "currency": "USD", "adjustment_factor": Decimal("0.990000000000"),
                "vendor_as_of": self.AS_OF,
            },
        ]
        result = evaluate_vendor_comparison(rows, self.TOL, as_of_date=self.AS_OF)
        assert result["non_usd_dividends"] == 1
        assert result["matched"] == 1
        assert result["failed"] == 0
