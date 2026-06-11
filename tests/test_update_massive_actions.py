from datetime import date, datetime, timezone
from types import SimpleNamespace

from scripts.update_massive_actions import _get_batch_start_date


HISTORY_FLOOR = date(2024, 6, 11)


def _security(last_updated_at):
    return SimpleNamespace(actions_last_updated_at=last_updated_at)


def test_force_always_starts_from_history_floor():
    securities = [_security(datetime(2026, 3, 1, tzinfo=timezone.utc))]

    assert _get_batch_start_date(securities, HISTORY_FLOOR, force=True) == HISTORY_FLOOR.isoformat()


def test_batch_with_never_updated_security_starts_from_history_floor():
    securities = [
        _security(None),
        _security(datetime(2026, 3, 1, tzinfo=timezone.utc)),
    ]

    assert _get_batch_start_date(securities, HISTORY_FLOOR, force=False) == HISTORY_FLOOR.isoformat()


def test_batch_of_recently_updated_securities_uses_oldest_timestamp_with_buffer():
    securities = [
        _security(datetime(2026, 3, 10, tzinfo=timezone.utc)),
        _security(datetime(2026, 3, 1, tzinfo=timezone.utc)),
    ]

    assert _get_batch_start_date(securities, HISTORY_FLOOR, force=False) == "2026-02-22"


def test_batch_start_never_precedes_history_floor():
    securities = [_security(datetime(2024, 1, 1, tzinfo=timezone.utc))]

    assert _get_batch_start_date(securities, HISTORY_FLOOR, force=False) == HISTORY_FLOOR.isoformat()
