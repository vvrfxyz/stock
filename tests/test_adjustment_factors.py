import unittest
from datetime import date
from decimal import Decimal
from types import SimpleNamespace

from scripts.update_adjustment_factors import compute_adjustment_factor_rows, evaluate_vendor_comparison
from scripts.update_massive_actions import _build_vendor_factor_rows


class AdjustmentFactorTests(unittest.TestCase):
    def test_vendor_factor_rows_use_event_identity_as_factor_key(self):
        security = SimpleNamespace(id=1)

        rows = _build_vendor_factor_rows(
            security,
            [
                {
                    "source_event_id": "div-1",
                    "ex_dividend_date": date(2026, 5, 11),
                    "cash_amount": Decimal("0.27"),
                    "historical_adjustment_factor": "0.99908",
                }
            ],
            [
                {
                    "source_event_id": "split-1",
                    "execution_date": date(2026, 6, 1),
                    "split_from": Decimal("1"),
                    "split_to": Decimal("2"),
                    "historical_adjustment_factor": "0.5",
                }
            ],
            date(2026, 5, 13),
        )

        self.assertEqual(rows[0]["factor_key"], "dividend:div-1")
        self.assertEqual(rows[0]["adjustment_factor"], Decimal("0.999080000000"))
        self.assertEqual(rows[1]["factor_key"], "split:split-1")

    def test_computed_factor_accumulates_events_from_event_date_to_present(self):
        actions = [
            SimpleNamespace(
                action_type="DIVIDEND",
                ex_date=date(2026, 1, 10),
                cash_amount=Decimal("1"),
                split_from=None,
                split_to=None,
                source="MASSIVE",
                source_event_id="div-old",
            ),
            SimpleNamespace(
                action_type="DIVIDEND",
                ex_date=date(2026, 2, 10),
                cash_amount=Decimal("2"),
                split_from=None,
                split_to=None,
                source="MASSIVE",
                source_event_id="div-new",
            ),
        ]
        price_dates = [date(2026, 1, 9), date(2026, 2, 9)]
        close_by_date = {
            date(2026, 1, 9): Decimal("100"),
            date(2026, 2, 9): Decimal("200"),
        }

        rows, stats = compute_adjustment_factor_rows(
            1,
            actions,
            price_dates,
            close_by_date,
            methodology_version="raw_actions_v1",
            as_of_date=date(2026, 5, 13),
        )
        by_key = {row["factor_key"]: row for row in rows}

        self.assertEqual(stats, {})
        self.assertEqual(by_key["dividend:div-new"]["single_event_factor"], Decimal("0.990000000000"))
        self.assertEqual(by_key["dividend:div-new"]["cumulative_factor"], Decimal("0.990000000000"))
        self.assertEqual(by_key["dividend:div-old"]["single_event_factor"], Decimal("0.990000000000"))
        self.assertEqual(by_key["dividend:div-old"]["cumulative_factor"], Decimal("0.980100000000"))

    def test_computed_factor_handles_forward_split(self):
        actions = [
            SimpleNamespace(
                action_type="SPLIT",
                ex_date=date(2026, 3, 1),
                cash_amount=None,
                split_from=Decimal("1"),
                split_to=Decimal("4"),
                source="MASSIVE",
                source_event_id="split-4-for-1",
            ),
        ]

        rows, _ = compute_adjustment_factor_rows(
            1,
            actions,
            [],
            {},
            methodology_version="raw_actions_v1",
            as_of_date=date(2026, 5, 13),
        )

        self.assertEqual(rows[0]["single_event_factor"], Decimal("0.250000000000"))
        self.assertEqual(rows[0]["cumulative_factor"], Decimal("0.250000000000"))

    def test_computed_factor_deduplicates_synthetic_and_real_action_ids(self):
        actions = [
            SimpleNamespace(
                action_type="DIVIDEND",
                ex_date=date(2026, 1, 10),
                cash_amount=Decimal("1"),
                currency="USD",
                split_from=None,
                split_to=None,
                source="MASSIVE",
                source_event_id="massive-dividend:1:2026-01-10:1.0000000000",
            ),
            SimpleNamespace(
                action_type="DIVIDEND",
                ex_date=date(2026, 1, 10),
                cash_amount=Decimal("1"),
                currency="USD",
                split_from=None,
                split_to=None,
                source="MASSIVE",
                source_event_id="vendor-event-id",
            ),
        ]

        rows, stats = compute_adjustment_factor_rows(
            1,
            actions,
            [date(2026, 1, 9)],
            {date(2026, 1, 9): Decimal("100")},
            methodology_version="raw_actions_v1",
            as_of_date=date(2026, 5, 13),
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["factor_key"], "dividend:vendor-event-id")
        self.assertEqual(stats["DEDUPLICATED_ECONOMIC_EVENTS"], 1)

    def test_same_day_same_amount_real_events_are_both_kept(self):
        # Ford 2025-02-18 形态：常规 + 特别分红同日同为 0.15，两个不同真实 vendor ID，
        # 是真正的两笔事件——经济去重不得移除，累计因子须计两次。
        def _dividend(event_id):
            return SimpleNamespace(
                action_type="DIVIDEND",
                ex_date=date(2025, 2, 18),
                cash_amount=Decimal("0.15"),
                currency="USD",
                split_from=None,
                split_to=None,
                source="MASSIVE",
                source_event_id=event_id,
            )

        rows, stats = compute_adjustment_factor_rows(
            1,
            [_dividend("Eb44real1"), _dividend("Ed5ereal2")],
            [date(2025, 2, 17)],
            {date(2025, 2, 17): Decimal("9.48")},
            methodology_version="raw_actions_v1",
            as_of_date=date(2026, 5, 13),
        )

        self.assertEqual(len(rows), 2)
        self.assertEqual(stats["DEDUPLICATED_ECONOMIC_EVENTS"], 0)
        single = (Decimal("9.48") - Decimal("0.15")) / Decimal("9.48")
        for row in rows:
            self.assertEqual(
                row["cumulative_factor"].quantize(Decimal("0.000000000001")),
                (single * single).quantize(Decimal("0.000000000001")),
            )


def _cmp_row(d, action_type, single, vendor=None, vendor_as_of=None):
    return {
        "date": d,
        "factor_key": f"{action_type.lower()}:{d}",
        "action_type": action_type,
        "cumulative_factor": None,
        "single_event_factor": single,
        "adjustment_factor": vendor,
        "vendor_as_of": vendor_as_of,
    }


class EvaluateVendorComparisonTests(unittest.TestCase):
    """Massive historical_adjustment_factor 是同类型事件链（分红/拆股各自连乘、不跨类型），
    且不含 as_of 之后的未来事件。evaluate_vendor_comparison 须按该口径重算后再对账。"""

    TOL = Decimal("0.00001")
    AS_OF = date(2026, 6, 10)

    def test_dividend_chain_excludes_split_factor(self):
        # bbsi 形态：拆股夹在分红中间，vendor 分红链不含 0.25 拆股因子。
        rows = [
            _cmp_row(date(2026, 1, 10), "DIVIDEND", Decimal("0.99"), vendor=Decimal("0.99"), vendor_as_of=self.AS_OF),
            _cmp_row(date(2025, 6, 1), "SPLIT", Decimal("0.25"), vendor=Decimal("0.25"), vendor_as_of=self.AS_OF),
            _cmp_row(date(2025, 1, 10), "DIVIDEND", Decimal("0.98"), vendor=Decimal("0.9702"), vendor_as_of=self.AS_OF),
        ]
        result = evaluate_vendor_comparison(rows, self.TOL, as_of_date=self.AS_OF)
        self.assertEqual(result["matched"], 3)
        self.assertEqual(result["failed"], 0)

    def test_future_event_excluded_from_chain(self):
        # xxii 形态：未来 20:1 拆股 vendor 尚无参考行，不应进入对账链，
        # 否则历史事件全部差 20 倍。
        rows = [
            _cmp_row(date(2026, 6, 12), "SPLIT", Decimal("20")),  # 未来事件，vendor 无行
            _cmp_row(date(2026, 1, 26), "SPLIT", Decimal("15"), vendor=Decimal("15"), vendor_as_of=self.AS_OF),
        ]
        result = evaluate_vendor_comparison(rows, self.TOL, as_of_date=self.AS_OF)
        self.assertEqual(result["future_events"], 1)
        self.assertEqual(result["matched"], 1)
        self.assertEqual(result["failed"], 0)

    def test_event_after_as_of_with_vendor_row_stays_in_chain(self):
        # shph 形态：ex 日为今日（> 上一完成交易日 as_of）但 vendor 已出参考行，
        # vendor 链已包含该事件（2025 行 = 10×25 = 250），剔除它会差 10 倍。
        rows = [
            _cmp_row(date(2026, 6, 11), "SPLIT", Decimal("10"), vendor=Decimal("10"), vendor_as_of=date(2026, 6, 11)),
            _cmp_row(date(2025, 6, 16), "SPLIT", Decimal("25"), vendor=Decimal("250"), vendor_as_of=date(2026, 6, 11)),
        ]
        result = evaluate_vendor_comparison(rows, self.TOL, as_of_date=self.AS_OF)
        self.assertEqual(result["future_events"], 0)
        self.assertEqual(result["matched"], 2)
        self.assertEqual(result["failed"], 0)

    def test_stale_vendor_rows_counted_not_failed(self):
        # 滑出 730 天窗口的 vendor 行冻结在旧 as_of，链缺新事件——计 stale 而非 fail。
        rows = [
            _cmp_row(date(2026, 1, 10), "DIVIDEND", Decimal("0.99"), vendor=Decimal("0.99"), vendor_as_of=self.AS_OF),
            _cmp_row(date(2024, 5, 16), "DIVIDEND", Decimal("0.98"), vendor=Decimal("0.98"), vendor_as_of=date(2026, 5, 15)),
        ]
        result = evaluate_vendor_comparison(rows, self.TOL, as_of_date=self.AS_OF)
        self.assertEqual(result["stale_vendor"], 1)
        self.assertEqual(result["matched"], 1)
        self.assertEqual(result["failed"], 0)

    def test_relative_tolerance_for_large_split_chains(self):
        # cmct 形态：链值 24506.200069 vs 24506.200068625，绝对差 3.75e-7 但相对差极小。
        rows = [
            _cmp_row(date(2024, 9, 25), "SPLIT", Decimal("24506.200068625"), vendor=Decimal("24506.200069000000"), vendor_as_of=self.AS_OF),
        ]
        result = evaluate_vendor_comparison(rows, self.TOL, as_of_date=self.AS_OF)
        self.assertEqual(result["failed"], 0)

    def test_same_day_multi_event_group_compares_day_product(self):
        # mdrr 形态：同日 1:5 正股 + 10:1 缩股。vendor 给日内后缀积 {2, 10}，
        # 我们的日因子=2；该组取最接近的 vendor 行（2）对账，不应误报 fail。
        rows = [
            _cmp_row(date(2024, 7, 3), "SPLIT", Decimal("0.2"), vendor=Decimal("2"), vendor_as_of=self.AS_OF),
            _cmp_row(date(2024, 7, 3), "SPLIT", Decimal("10"), vendor=Decimal("10"), vendor_as_of=self.AS_OF),
        ]
        rows[1]["factor_key"] = "split:other-event"
        result = evaluate_vendor_comparison(rows, self.TOL, as_of_date=self.AS_OF)
        self.assertEqual(result["matched"], 1)
        self.assertEqual(result["failed"], 0)

    def test_vendor_unadjusted_placeholder_skipped_from_chain(self):
        # ibot 形态：vendor 对资本利得类分派给精确 1.0 占位（不调整），
        # 该事件须整体跳出对账链，否则上游历史事件全部差一个我方因子。
        rows = [
            _cmp_row(date(2025, 12, 22), "DIVIDEND", Decimal("0.995664"), vendor=Decimal("1"), vendor_as_of=self.AS_OF),
            _cmp_row(date(2025, 1, 10), "DIVIDEND", Decimal("0.99"), vendor=Decimal("0.99"), vendor_as_of=self.AS_OF),
        ]
        result = evaluate_vendor_comparison(rows, self.TOL, as_of_date=self.AS_OF)
        self.assertEqual(result["vendor_unadjusted"], 1)
        self.assertEqual(result["matched"], 1)
        self.assertEqual(result["failed"], 0)

    def test_genuine_mismatch_still_fails(self):
        rows = [
            _cmp_row(date(2026, 1, 10), "DIVIDEND", Decimal("0.99"), vendor=Decimal("0.95"), vendor_as_of=self.AS_OF),
        ]
        result = evaluate_vendor_comparison(rows, self.TOL, as_of_date=self.AS_OF)
        self.assertEqual(result["failed"], 1)


if __name__ == "__main__":
    unittest.main()
