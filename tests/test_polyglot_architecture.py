import inspect
from pathlib import Path

from sqlalchemy import BigInteger, Index, Numeric, String, UniqueConstraint

from data_models.models import (
    ComputedAdjustmentFactor,
    CorporateAction,
    DailyPrice,
    Exchange,
    HistoricalShare,
    InstitutionalHolding,
    InsiderTransaction,
    SecFiling,
    Security,
    SecurityIdentifier,
    SecuritySymbolHistory,
    TradingCalendar,
    VendorAdjustmentFactor,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
ALEMBIC_REVISION = PROJECT_ROOT / "alembic" / "versions" / "9a1b2c3d4e5f_add_polyglot_control_tables.py"
ADJUSTMENT_FACTOR_REVISION = PROJECT_ROOT / "alembic" / "versions" / "e1f2a3b4c5d6_add_adjustment_factor_reference_cache.py"
EXCHANGE_SEC_REVISION = PROJECT_ROOT / "alembic" / "versions" / "f2a3b4c5d6e7_add_exchange_calendar_and_sec_foundation.py"


def _unique_constraints(model):
    return {
        tuple(constraint.columns.keys())
        for constraint in model.__table__.constraints
        if isinstance(constraint, UniqueConstraint)
    }


def _unique_indexes(model):
    return {
        tuple(index.columns.keys())
        for index in model.__table__.indexes
        if isinstance(index, Index) and index.unique
    }

def test_security_model_exposes_postgres_control_center_columns():
    columns = Security.__table__.columns

    assert isinstance(columns["id"].type, BigInteger)
    assert isinstance(columns["current_symbol"].type, String)
    assert columns["current_symbol"].type.length == 30
    assert not columns["current_symbol"].nullable
    assert "em_code" not in columns
    assert isinstance(columns["sector"].type, String)
    assert columns["sector"].type.length == 100
    assert isinstance(columns["composite_figi"].type, String)
    assert columns["composite_figi"].type.length == 30
    assert not columns["created_at"].nullable
    assert not columns["updated_at"].nullable
    assert ("symbol",) in _unique_indexes(Security)
    assert ("current_symbol", "exchange") in _unique_indexes(Security)


def test_existing_security_references_are_int64_compatible():
    for model in [DailyPrice, HistoricalShare, CorporateAction]:
        assert isinstance(model.__table__.columns["security_id"].type, BigInteger)


def test_symbol_history_model_matches_polyglot_plan():
    columns = SecuritySymbolHistory.__table__.columns

    assert isinstance(columns["id"].type, BigInteger)
    assert isinstance(columns["security_id"].type, BigInteger)
    assert not columns["security_id"].nullable
    assert columns["symbol"].type.length == 30
    assert not columns["symbol"].nullable
    assert columns["source"].type.length == 30
    assert not columns["source"].nullable
    assert "start_date" in columns
    assert "end_date" in columns
    assert not columns["created_at"].nullable


def test_corporate_action_model_matches_polyglot_plan():
    columns = CorporateAction.__table__.columns

    assert isinstance(columns["id"].type, BigInteger)
    assert isinstance(columns["security_id"].type, BigInteger)
    assert columns["action_type"].type.length == 20
    assert not columns["action_type"].nullable
    assert isinstance(columns["cash_amount"].type, Numeric)
    assert columns["cash_amount"].type.precision == 20
    assert columns["cash_amount"].type.scale == 10
    assert isinstance(columns["split_from"].type, Numeric)
    assert columns["split_to"].type.scale == 10
    assert columns["source"].type.length == 30
    assert columns["source_event_id"].type.length == 128
    assert not columns["source_event_id"].nullable
    assert not columns["ex_date"].nullable
    assert not columns["created_at"].nullable
    assert not columns["updated_at"].nullable
    assert ("security_id", "action_type", "source", "source_event_id") in _unique_constraints(CorporateAction)


def test_daily_price_model_stores_only_raw_bar_fields():
    columns = DailyPrice.__table__.columns

    assert set(columns.keys()) == {
        "security_id",
        "date",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "otc",
        "vwap",
        "trade_count",
        "pre_market",
        "after_hours",
    }


def test_adjustment_factor_models_are_reference_and_cache_not_price_truth():
    vendor_columns = VendorAdjustmentFactor.__table__.columns
    computed_columns = ComputedAdjustmentFactor.__table__.columns

    assert isinstance(vendor_columns["security_id"].type, BigInteger)
    assert isinstance(vendor_columns["adjustment_factor"].type, Numeric)
    assert vendor_columns["adjustment_factor"].type.precision == 24
    assert vendor_columns["adjustment_factor"].type.scale == 12
    assert ("security_id", "source", "factor_key") in _unique_constraints(VendorAdjustmentFactor)

    assert isinstance(computed_columns["security_id"].type, BigInteger)
    assert isinstance(computed_columns["cumulative_factor"].type, Numeric)
    assert computed_columns["cumulative_factor"].type.scale == 12
    assert isinstance(computed_columns["methodology_version"].type, String)
    assert "event_hash" in computed_columns
    assert ("security_id", "methodology_version", "factor_key") in _unique_constraints(ComputedAdjustmentFactor)


def test_historical_share_model_is_point_in_time_safe():
    columns = HistoricalShare.__table__.columns

    assert isinstance(columns["security_id"].type, BigInteger)
    assert not columns["security_id"].nullable
    assert not columns["filing_date"].nullable
    assert not columns["period_end_date"].nullable
    assert not columns["total_shares"].nullable
    assert columns["source"].type.length == 30
    assert not columns["source"].nullable
    assert ("security_id", "filing_date", "source") in _unique_constraints(HistoricalShare)


def test_exchange_calendar_models_are_mic_anchored():
    exchange_columns = Exchange.__table__.columns
    calendar_columns = TradingCalendar.__table__.columns

    assert isinstance(exchange_columns["mic"].type, String)
    assert exchange_columns["mic"].primary_key
    assert exchange_columns["mic"].type.length == 20
    assert isinstance(calendar_columns["exchange_mic"].type, String)
    assert calendar_columns["exchange_mic"].primary_key
    assert calendar_columns["trade_date"].primary_key
    assert "market" not in calendar_columns
    assert not calendar_columns["is_open"].nullable
    assert not calendar_columns["is_half_day"].nullable
    assert "open_at" in calendar_columns
    assert "close_at" in calendar_columns
    assert "holiday_name" in calendar_columns


def test_sec_foundation_models_are_point_in_time_and_identifier_ready():
    identifier_columns = SecurityIdentifier.__table__.columns
    filing_columns = SecFiling.__table__.columns
    insider_columns = InsiderTransaction.__table__.columns
    holding_columns = InstitutionalHolding.__table__.columns

    assert isinstance(identifier_columns["security_id"].type, BigInteger)
    assert identifier_columns["id_type"].type.length == 30
    assert identifier_columns["id_value"].type.length == 80
    assert ("security_id", "id_type", "id_value", "source", "start_date") in _unique_constraints(SecurityIdentifier)

    assert isinstance(filing_columns["security_id"].type, BigInteger)
    assert filing_columns["accession_number"].type.length == 32
    assert not filing_columns["accession_number"].nullable
    assert not filing_columns["filing_date"].nullable
    assert not filing_columns["available_at"].nullable
    assert ("source", "accession_number") in _unique_constraints(SecFiling)

    assert insider_columns["source_row_hash"].type.length == 64
    assert "transaction_code" in insider_columns
    assert "aff_10b5_one" in insider_columns
    assert ("source", "accession_number", "source_row_hash") in _unique_constraints(InsiderTransaction)

    assert holding_columns["cusip"].type.length == 20
    assert "period" in holding_columns
    assert "shares_or_principal_amount" in holding_columns
    assert ("source", "accession_number", "source_row_hash") in _unique_constraints(InstitutionalHolding)


def test_alembic_revision_materializes_polyglot_plan():
    migration = ALEMBIC_REVISION.read_text(encoding="utf-8")

    assert "down_revision" in migration
    assert "6e9f8c1d2a7b" in migration
    assert "op.alter_column('securities', 'id', type_=sa.BigInteger()" in migration
    assert "op.alter_column('daily_prices', 'security_id', type_=sa.BigInteger()" in migration
    assert "op.add_column(\n        'securities'," in migration
    assert "current_symbol" in migration
    assert "_current_symbol_exchange_uc" in migration
    assert "op.create_table(\n        'security_symbol_history'," in migration
    assert "op.create_table(\n        'corporate_actions'," in migration
    assert "_corporate_action_source_event_uc" in migration


def test_adjustment_factor_revision_adds_separate_reference_and_cache_tables():
    migration = ADJUSTMENT_FACTOR_REVISION.read_text(encoding="utf-8")

    assert "vendor_adjustment_factors" in migration
    assert "computed_adjustment_factors" in migration
    assert "_vendor_adjustment_factor_key_uc" in migration
    assert "_computed_adjustment_factor_key_uc" in migration
    assert "methodology_version" in migration


def test_exchange_sec_revision_adds_reference_and_sec_foundation_tables():
    migration = EXCHANGE_SEC_REVISION.read_text(encoding="utf-8")

    assert "exchanges" in migration
    assert "trading_calendars_pkey" in migration
    assert "['exchange_mic', 'trade_date']" in migration
    assert "security_identifiers" in migration
    assert "sec_filings" in migration
    assert "_sec_filing_source_accession_uc" in migration
    assert "insider_transactions" in migration
    assert "_insider_transaction_row_uc" in migration
    assert "institutional_holdings" in migration
    assert "_institutional_holding_row_uc" in migration


def test_database_manager_sets_current_symbol_for_insert_paths():
    import db_manager

    source = inspect.getsource(db_manager.DatabaseManager.upsert_security_info)
    batch_source = inspect.getsource(db_manager.DatabaseManager.upsert_securities_by_symbol)

    assert "security_data.setdefault('current_symbol', security_data.get('symbol'))" in source
    assert 'cleaned.setdefault("current_symbol", cleaned.get("symbol"))' in batch_source
