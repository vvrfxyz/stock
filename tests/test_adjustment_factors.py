import unittest
from datetime import date
from decimal import Decimal
from types import SimpleNamespace

from scripts.update_adjustment_factors import compute_adjustment_factor_rows
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


if __name__ == "__main__":
    unittest.main()
