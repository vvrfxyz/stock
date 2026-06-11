import unittest
from datetime import date
from types import SimpleNamespace
from unittest.mock import Mock

from scripts.update_massive_prices import _finalize_price_metadata_after_successful_write


class MassiveDailyPriceMetadataTests(unittest.TestCase):
    def test_full_refresh_updates_timestamp_even_when_latest_date_is_unchanged(self):
        security = SimpleNamespace(id=1, symbol="aapl", price_data_latest_date=date(2026, 3, 13))
        db_manager = Mock()

        _finalize_price_metadata_after_successful_write(
            security,
            db_manager,
            date(2026, 3, 13),
            is_full_run=True,
        )

        db_manager.update_security_price_latest_date.assert_called_once_with(
            1,
            date(2026, 3, 13),
            is_full_run=True,
        )

    def test_incremental_write_does_not_touch_metadata_when_latest_date_is_unchanged(self):
        security = SimpleNamespace(id=1, symbol="aapl", price_data_latest_date=date(2026, 3, 13))
        db_manager = Mock()

        _finalize_price_metadata_after_successful_write(
            security,
            db_manager,
            date(2026, 3, 13),
            is_full_run=False,
        )

        db_manager.update_security_price_latest_date.assert_not_called()


if __name__ == "__main__":
    unittest.main()
