import unittest

from scripts.backfill_turnover_rate import _pick_denominator


class TurnoverRateLogicTests(unittest.TestCase):
    def test_float_shares_take_priority(self):
        self.assertEqual(_pick_denominator(1000, 600), 600)

    def test_total_shares_used_when_float_missing(self):
        self.assertEqual(_pick_denominator(1000, None), 1000)

    def test_none_when_no_valid_shares(self):
        self.assertIsNone(_pick_denominator(None, None))
        self.assertIsNone(_pick_denominator(0, 0))


if __name__ == "__main__":
    unittest.main()
