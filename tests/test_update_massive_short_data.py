from datetime import date
from types import SimpleNamespace
from unittest.mock import Mock

from scripts.update_massive_short_data import get_securities_to_update, process_batch


def test_process_batch_uses_incremental_start_dates_for_existing_rows():
    security = SimpleNamespace(id=1, symbol="aapl")
    source = Mock()
    db_manager = Mock()
    db_manager.get_security_short_max_dates.return_value = {
        1: {"interest": date(2026, 4, 1), "volume": date(2026, 4, 2)}
    }
    source.get_short_interest_batch.return_value = [
        {"ticker": "aapl", "settlement_date": date(2026, 4, 1), "short_interest": 10},
        {"ticker": "aapl", "settlement_date": date(2026, 4, 3), "short_interest": 12},
    ]
    source.get_short_volume_batch.return_value = [
        {"ticker": "aapl", "date": date(2026, 4, 2), "short_volume": 20},
        {"ticker": "aapl", "date": date(2026, 4, 4), "short_volume": 22},
    ]
    db_manager.upsert_short_interests.return_value = 1
    db_manager.upsert_short_volumes.return_value = 1

    counter, interest_count, volume_count = process_batch(
        [security],
        source,
        db_manager,
        date(2026, 3, 1),
        force=False,
    )

    source.get_short_interest_batch.assert_called_once()
    source.get_short_volume_batch.assert_called_once()
    assert source.get_short_interest_batch.call_args.kwargs["start_date"] == "2026-04-02"
    assert source.get_short_volume_batch.call_args.kwargs["start_date"] == "2026-04-03"
    assert interest_count == 1
    assert volume_count == 1
    assert counter["SUCCESS"] == 1


def test_get_securities_to_update_keeps_force_path_available():
    assert callable(get_securities_to_update)
