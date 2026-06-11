from sqlalchemy.dialects import postgresql

from data_models.models import CorporateAction, HistoricalShare
from db_manager import _build_upsert_statement, _group_rows_by_key_set, _normalize_batch_rows


def test_corporate_action_upsert_updates_nullable_vendor_fields_on_conflict():
    stmt = _build_upsert_statement(
        CorporateAction,
        [
            {
                "security_id": 1,
                "action_type": "DIVIDEND",
                "ex_date": "2026-05-11",
                "declaration_date": "2026-04-30",
                "record_date": "2026-05-11",
                "pay_date": "2026-05-14",
                "cash_amount": "0.27",
                "currency": "USD",
                "frequency": 4,
                "distribution_type": "recurring",
                "source": "MASSIVE",
                "source_event_id": "E9fd05fa01d55de07885332c97a263e93ac4bf03c742faef9b4f18544b34e928f",
            }
        ],
        ["security_id", "action_type", "source", "source_event_id"],
        update_on_conflict=True,
    )

    compiled = str(stmt.compile(dialect=postgresql.dialect()))

    assert "ON CONFLICT (security_id, action_type, source, source_event_id) DO UPDATE" in compiled
    assert "declaration_date = excluded.declaration_date" in compiled
    assert "record_date = excluded.record_date" in compiled
    assert "pay_date = excluded.pay_date" in compiled
    assert "frequency = excluded.frequency" in compiled
    assert "distribution_type = excluded.distribution_type" in compiled
    assert "updated_at = now()" in compiled


def test_normalize_batch_rows_fills_missing_optional_keys_for_multi_insert():
    rows = _normalize_batch_rows(
        HistoricalShare,
        [
            {
                "security_id": 1,
                "filing_date": "2026-05-14",
                "period_end_date": "2026-05-14",
                "total_shares": 100,
                "float_shares": 80,
                "free_float_percent": "80.0",
                "source": "MASSIVE",
            },
            {
                "security_id": 2,
                "filing_date": "2026-05-14",
                "period_end_date": "2026-05-14",
                "total_shares": 200,
                "source": "MASSIVE",
            },
        ],
    )

    assert rows[1]["float_shares"] is None
    assert rows[1]["free_float_percent"] is None
    compiled = str(
        _build_upsert_statement(
            HistoricalShare,
            rows,
            ["security_id", "filing_date", "source"],
            update_on_conflict=True,
        ).compile(dialect=postgresql.dialect())
    )
    assert "float_shares" in compiled


def test_group_rows_by_key_set_splits_heterogeneous_rows_for_multi_insert():
    rows = [
        {"symbol": "aapl", "name": "Apple", "cik": "0000320193"},
        {"symbol": "msft", "name": "Microsoft", "cik": "0000789019"},
        {"symbol": "newco", "name": "New Co"},
    ]

    groups = _group_rows_by_key_set(rows)

    assert sorted(len(group) for group in groups) == [1, 2]
    for group in groups:
        key_sets = {frozenset(row.keys()) for row in group}
        assert len(key_sets) == 1
