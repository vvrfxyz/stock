"""Add exchange calendar and SEC foundation tables

Revision ID: f2a3b4c5d6e7
Revises: e1f2a3b4c5d6
Create Date: 2026-05-14 16:10:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: str = 'f2a3b4c5d6e7'
down_revision: Union[str, Sequence[str], None] = 'e1f2a3b4c5d6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _table_exists(table_name: str) -> bool:
    conn = op.get_bind()
    return bool(
        conn.execute(
            sa.text(
                """
                SELECT 1
                FROM information_schema.tables
                WHERE table_name = :table_name
                """
            ),
            {"table_name": table_name},
        ).first()
    )


def _column_exists(table_name: str, column_name: str) -> bool:
    conn = op.get_bind()
    return bool(
        conn.execute(
            sa.text(
                """
                SELECT 1
                FROM information_schema.columns
                WHERE table_name = :table_name
                  AND column_name = :column_name
                """
            ),
            {"table_name": table_name, "column_name": column_name},
        ).first()
    )


def _constraint_exists(constraint_name: str) -> bool:
    conn = op.get_bind()
    return bool(
        conn.execute(
            sa.text("SELECT 1 FROM pg_constraint WHERE conname = :constraint_name"),
            {"constraint_name": constraint_name},
        ).first()
    )


def _create_index_if_missing(index_name: str, table_name: str, columns: list[str]) -> None:
    conn = op.get_bind()
    exists = conn.execute(
        sa.text("SELECT 1 FROM pg_indexes WHERE tablename = :table_name AND indexname = :index_name"),
        {"table_name": table_name, "index_name": index_name},
    ).first()
    if not exists:
        op.create_index(index_name, table_name, columns, unique=False)


def _add_column_if_missing(table_name: str, column: sa.Column) -> None:
    if not _column_exists(table_name, column.name):
        op.add_column(table_name, column)


def _drop_constraint_if_exists(table_name: str, constraint_name: str, constraint_type: str) -> None:
    if _constraint_exists(constraint_name):
        op.drop_constraint(constraint_name, table_name, type_=constraint_type)


def _create_exchanges() -> None:
    if not _table_exists('exchanges'):
        op.create_table(
            'exchanges',
            sa.Column('mic', sa.String(length=20), nullable=False),
            sa.Column('operating_mic', sa.String(length=20), nullable=True),
            sa.Column('acronym', sa.String(length=30), nullable=True),
            sa.Column('name', sa.String(length=255), nullable=False),
            sa.Column('exchange_type', sa.String(length=50), nullable=True),
            sa.Column('asset_class', sa.String(length=50), nullable=True),
            sa.Column('locale', sa.String(length=20), nullable=True),
            sa.Column('market', sa.String(length=50), nullable=True),
            sa.Column('participant_id', sa.String(length=30), nullable=True),
            sa.Column('url', sa.Text(), nullable=True),
            sa.Column('source', sa.String(length=30), server_default='manual', nullable=False),
            sa.Column('source_id', sa.String(length=128), nullable=True),
            sa.Column('source_updated_at', sa.TIMESTAMP(timezone=True), nullable=True),
            sa.Column('created_at', sa.TIMESTAMP(timezone=True), server_default=sa.text('now()'), nullable=False),
            sa.Column('updated_at', sa.TIMESTAMP(timezone=True), server_default=sa.text('now()'), nullable=False),
            sa.PrimaryKeyConstraint('mic'),
        )
    _create_index_if_missing('ix_exchanges_operating_mic', 'exchanges', ['operating_mic'])
    _create_index_if_missing('ix_exchanges_asset_class', 'exchanges', ['asset_class'])
    _create_index_if_missing('ix_exchanges_locale', 'exchanges', ['locale'])
    _create_index_if_missing('ix_exchanges_market', 'exchanges', ['market'])
    _create_index_if_missing('ix_exchanges_source', 'exchanges', ['source'])


def _migrate_trading_calendars() -> None:
    if not _table_exists('trading_calendars'):
        op.create_table(
            'trading_calendars',
            sa.Column('exchange_mic', sa.String(length=20), nullable=False),
            sa.Column('trade_date', sa.Date(), nullable=False),
            sa.Column('market', sa.String(length=50), nullable=False),
            sa.Column('is_open', sa.Boolean(), server_default=sa.text('true'), nullable=False),
            sa.Column('is_half_day', sa.Boolean(), server_default=sa.text('false'), nullable=False),
            sa.Column('open_at', sa.TIMESTAMP(timezone=True), nullable=True),
            sa.Column('close_at', sa.TIMESTAMP(timezone=True), nullable=True),
            sa.Column('timezone', sa.String(length=50), nullable=True),
            sa.Column('holiday_name', sa.String(length=255), nullable=True),
            sa.Column('source', sa.String(length=30), server_default='manual', nullable=False),
            sa.Column('source_updated_at', sa.TIMESTAMP(timezone=True), nullable=True),
            sa.Column('created_at', sa.TIMESTAMP(timezone=True), server_default=sa.text('now()'), nullable=False),
            sa.Column('updated_at', sa.TIMESTAMP(timezone=True), server_default=sa.text('now()'), nullable=False),
            sa.ForeignKeyConstraint(['exchange_mic'], ['exchanges.mic']),
            sa.PrimaryKeyConstraint('exchange_mic', 'trade_date'),
        )
    else:
        _drop_constraint_if_exists('trading_calendars', 'trading_calendars_pkey', 'primary')
        _drop_constraint_if_exists('trading_calendars', '_market_trade_date_uc', 'unique')

        _add_column_if_missing('trading_calendars', sa.Column('exchange_mic', sa.String(length=20), nullable=True))
        _add_column_if_missing('trading_calendars', sa.Column('is_open', sa.Boolean(), server_default=sa.text('true'), nullable=False))
        _add_column_if_missing('trading_calendars', sa.Column('open_at', sa.TIMESTAMP(timezone=True), nullable=True))
        _add_column_if_missing('trading_calendars', sa.Column('close_at', sa.TIMESTAMP(timezone=True), nullable=True))
        _add_column_if_missing('trading_calendars', sa.Column('timezone', sa.String(length=50), nullable=True))
        _add_column_if_missing('trading_calendars', sa.Column('holiday_name', sa.String(length=255), nullable=True))
        _add_column_if_missing('trading_calendars', sa.Column('source', sa.String(length=30), server_default='manual', nullable=False))
        _add_column_if_missing('trading_calendars', sa.Column('source_updated_at', sa.TIMESTAMP(timezone=True), nullable=True))
        _add_column_if_missing('trading_calendars', sa.Column('created_at', sa.TIMESTAMP(timezone=True), server_default=sa.text('now()'), nullable=False))
        _add_column_if_missing('trading_calendars', sa.Column('updated_at', sa.TIMESTAMP(timezone=True), server_default=sa.text('now()'), nullable=False))

        op.execute(
            """
            INSERT INTO exchanges (mic, name, market, locale, asset_class, source)
            SELECT DISTINCT
                CASE UPPER(market)
                    WHEN 'US' THEN 'XNYS'
                    WHEN 'HK' THEN 'XHKG'
                    WHEN 'CNA' THEN 'XSHG'
                    ELSE UPPER(market)
                END AS mic,
                CASE UPPER(market)
                    WHEN 'US' THEN 'New York Stock Exchange'
                    WHEN 'HK' THEN 'Hong Kong Stock Exchange'
                    WHEN 'CNA' THEN 'Shanghai Stock Exchange'
                    ELSE UPPER(market)
                END AS name,
                UPPER(market) AS market,
                CASE UPPER(market)
                    WHEN 'US' THEN 'us'
                    WHEN 'HK' THEN 'hk'
                    WHEN 'CNA' THEN 'cn'
                    ELSE LOWER(market)
                END AS locale,
                'stocks' AS asset_class,
                'migration' AS source
            FROM trading_calendars
            WHERE market IS NOT NULL
            ON CONFLICT (mic) DO NOTHING
            """
        )
        op.execute(
            """
            UPDATE trading_calendars
            SET exchange_mic = CASE UPPER(market)
                WHEN 'US' THEN 'XNYS'
                WHEN 'HK' THEN 'XHKG'
                WHEN 'CNA' THEN 'XSHG'
                ELSE UPPER(market)
            END
            WHERE exchange_mic IS NULL
            """
        )
        op.alter_column('trading_calendars', 'exchange_mic', nullable=False)
        if not _constraint_exists('trading_calendars_exchange_mic_fkey'):
            op.create_foreign_key(
                'trading_calendars_exchange_mic_fkey',
                'trading_calendars',
                'exchanges',
                ['exchange_mic'],
                ['mic'],
            )
        if not _constraint_exists('trading_calendars_pkey'):
            op.create_primary_key('trading_calendars_pkey', 'trading_calendars', ['exchange_mic', 'trade_date'])

    _create_index_if_missing('ix_trading_calendars_exchange_mic', 'trading_calendars', ['exchange_mic'])
    _create_index_if_missing('ix_trading_calendars_market', 'trading_calendars', ['market'])
    _create_index_if_missing('ix_trading_calendars_trade_date', 'trading_calendars', ['trade_date'])
    _create_index_if_missing('ix_trading_calendars_source', 'trading_calendars', ['source'])


def upgrade() -> None:
    _create_exchanges()
    _migrate_trading_calendars()

    if not _table_exists('security_identifiers'):
        op.create_table(
            'security_identifiers',
            sa.Column('id', sa.BigInteger(), nullable=False),
            sa.Column('security_id', sa.BigInteger(), nullable=False),
            sa.Column('id_type', sa.String(length=30), nullable=False),
            sa.Column('id_value', sa.String(length=80), nullable=False),
            sa.Column('start_date', sa.Date(), nullable=True),
            sa.Column('end_date', sa.Date(), nullable=True),
            sa.Column('source', sa.String(length=30), nullable=False),
            sa.Column('confidence', sa.String(length=20), nullable=True),
            sa.Column('created_at', sa.TIMESTAMP(timezone=True), server_default=sa.text('now()'), nullable=False),
            sa.Column('updated_at', sa.TIMESTAMP(timezone=True), server_default=sa.text('now()'), nullable=False),
            sa.ForeignKeyConstraint(['security_id'], ['securities.id']),
            sa.PrimaryKeyConstraint('id'),
            sa.UniqueConstraint(
                'security_id',
                'id_type',
                'id_value',
                'source',
                'start_date',
                name='_security_identifier_source_uc',
            ),
        )
    _create_index_if_missing('ix_security_identifiers_security_id', 'security_identifiers', ['security_id'])
    _create_index_if_missing('ix_security_identifiers_id_type', 'security_identifiers', ['id_type'])
    _create_index_if_missing('ix_security_identifiers_id_value', 'security_identifiers', ['id_value'])
    _create_index_if_missing('ix_security_identifiers_start_date', 'security_identifiers', ['start_date'])
    _create_index_if_missing('ix_security_identifiers_end_date', 'security_identifiers', ['end_date'])
    _create_index_if_missing('ix_security_identifiers_source', 'security_identifiers', ['source'])

    if not _table_exists('sec_filings'):
        op.create_table(
            'sec_filings',
            sa.Column('id', sa.BigInteger(), nullable=False),
            sa.Column('security_id', sa.BigInteger(), nullable=True),
            sa.Column('source', sa.String(length=30), nullable=False),
            sa.Column('cik', sa.String(length=20), nullable=True),
            sa.Column('ticker', sa.String(length=30), nullable=True),
            sa.Column('issuer_name', sa.String(length=255), nullable=True),
            sa.Column('form_type', sa.String(length=30), nullable=False),
            sa.Column('accession_number', sa.String(length=32), nullable=False),
            sa.Column('filing_date', sa.Date(), nullable=False),
            sa.Column('accepted_at', sa.TIMESTAMP(timezone=True), nullable=True),
            sa.Column('period_of_report', sa.Date(), nullable=True),
            sa.Column('filing_url', sa.Text(), nullable=True),
            sa.Column('primary_document_url', sa.Text(), nullable=True),
            sa.Column('available_at', sa.TIMESTAMP(timezone=True), server_default=sa.text('now()'), nullable=False),
            sa.Column('created_at', sa.TIMESTAMP(timezone=True), server_default=sa.text('now()'), nullable=False),
            sa.Column('updated_at', sa.TIMESTAMP(timezone=True), server_default=sa.text('now()'), nullable=False),
            sa.ForeignKeyConstraint(['security_id'], ['securities.id']),
            sa.PrimaryKeyConstraint('id'),
            sa.UniqueConstraint('source', 'accession_number', name='_sec_filing_source_accession_uc'),
        )
    _create_index_if_missing('ix_sec_filings_security_id', 'sec_filings', ['security_id'])
    _create_index_if_missing('ix_sec_filings_source', 'sec_filings', ['source'])
    _create_index_if_missing('ix_sec_filings_cik', 'sec_filings', ['cik'])
    _create_index_if_missing('ix_sec_filings_ticker', 'sec_filings', ['ticker'])
    _create_index_if_missing('ix_sec_filings_form_type', 'sec_filings', ['form_type'])
    _create_index_if_missing('ix_sec_filings_accession_number', 'sec_filings', ['accession_number'])
    _create_index_if_missing('ix_sec_filings_filing_date', 'sec_filings', ['filing_date'])
    _create_index_if_missing('ix_sec_filings_accepted_at', 'sec_filings', ['accepted_at'])
    _create_index_if_missing('ix_sec_filings_period_of_report', 'sec_filings', ['period_of_report'])
    _create_index_if_missing('ix_sec_filings_available_at', 'sec_filings', ['available_at'])

    if not _table_exists('insider_transactions'):
        op.create_table(
            'insider_transactions',
            sa.Column('id', sa.BigInteger(), nullable=False),
            sa.Column('filing_id', sa.BigInteger(), nullable=True),
            sa.Column('security_id', sa.BigInteger(), nullable=True),
            sa.Column('source', sa.String(length=30), nullable=False),
            sa.Column('accession_number', sa.String(length=32), nullable=False),
            sa.Column('source_row_hash', sa.String(length=64), nullable=False),
            sa.Column('form_type', sa.String(length=10), nullable=True),
            sa.Column('filing_date', sa.Date(), nullable=True),
            sa.Column('period_of_report', sa.Date(), nullable=True),
            sa.Column('issuer_cik', sa.String(length=20), nullable=True),
            sa.Column('issuer_trading_symbol', sa.String(length=30), nullable=True),
            sa.Column('issuer_name', sa.String(length=255), nullable=True),
            sa.Column('owner_cik', sa.String(length=20), nullable=True),
            sa.Column('owner_name', sa.String(length=255), nullable=True),
            sa.Column('is_director', sa.Boolean(), nullable=True),
            sa.Column('is_officer', sa.Boolean(), nullable=True),
            sa.Column('is_ten_percent_owner', sa.Boolean(), nullable=True),
            sa.Column('is_other', sa.Boolean(), nullable=True),
            sa.Column('officer_title', sa.String(length=255), nullable=True),
            sa.Column('security_type', sa.String(length=50), nullable=True),
            sa.Column('record_type', sa.String(length=50), nullable=True),
            sa.Column('security_title', sa.String(length=255), nullable=True),
            sa.Column('transaction_timeliness', sa.String(length=10), nullable=True),
            sa.Column('aff_10b5_one', sa.Boolean(), nullable=True),
            sa.Column('transaction_date', sa.Date(), nullable=True),
            sa.Column('deemed_execution_date', sa.Date(), nullable=True),
            sa.Column('transaction_code', sa.String(length=10), nullable=True),
            sa.Column('equity_swap_involved', sa.Boolean(), nullable=True),
            sa.Column('transaction_shares', sa.Numeric(24, 6), nullable=True),
            sa.Column('transaction_price_per_share', sa.Numeric(24, 6), nullable=True),
            sa.Column('transaction_acquired_disposed', sa.String(length=5), nullable=True),
            sa.Column('shares_owned_following_transaction', sa.Numeric(24, 6), nullable=True),
            sa.Column('transaction_value', sa.Numeric(28, 6), nullable=True),
            sa.Column('exercise_date', sa.Date(), nullable=True),
            sa.Column('expiration_date', sa.Date(), nullable=True),
            sa.Column('underlying_security_title', sa.String(length=255), nullable=True),
            sa.Column('underlying_security_shares', sa.Numeric(24, 6), nullable=True),
            sa.Column('direct_or_indirect', sa.String(length=5), nullable=True),
            sa.Column('footnotes', sa.Text(), nullable=True),
            sa.Column('remarks', sa.Text(), nullable=True),
            sa.Column('created_at', sa.TIMESTAMP(timezone=True), server_default=sa.text('now()'), nullable=False),
            sa.Column('updated_at', sa.TIMESTAMP(timezone=True), server_default=sa.text('now()'), nullable=False),
            sa.ForeignKeyConstraint(['filing_id'], ['sec_filings.id']),
            sa.ForeignKeyConstraint(['security_id'], ['securities.id']),
            sa.PrimaryKeyConstraint('id'),
            sa.UniqueConstraint('source', 'accession_number', 'source_row_hash', name='_insider_transaction_row_uc'),
        )
    _create_index_if_missing('ix_insider_transactions_filing_id', 'insider_transactions', ['filing_id'])
    _create_index_if_missing('ix_insider_transactions_security_id', 'insider_transactions', ['security_id'])
    _create_index_if_missing('ix_insider_transactions_source', 'insider_transactions', ['source'])
    _create_index_if_missing('ix_insider_transactions_accession_number', 'insider_transactions', ['accession_number'])
    _create_index_if_missing('ix_insider_transactions_form_type', 'insider_transactions', ['form_type'])
    _create_index_if_missing('ix_insider_transactions_filing_date', 'insider_transactions', ['filing_date'])
    _create_index_if_missing('ix_insider_transactions_issuer_cik', 'insider_transactions', ['issuer_cik'])
    _create_index_if_missing('ix_insider_transactions_issuer_trading_symbol', 'insider_transactions', ['issuer_trading_symbol'])
    _create_index_if_missing('ix_insider_transactions_owner_cik', 'insider_transactions', ['owner_cik'])
    _create_index_if_missing('ix_insider_transactions_owner_name', 'insider_transactions', ['owner_name'])
    _create_index_if_missing('ix_insider_transactions_security_type', 'insider_transactions', ['security_type'])
    _create_index_if_missing('ix_insider_transactions_record_type', 'insider_transactions', ['record_type'])
    _create_index_if_missing('ix_insider_transactions_transaction_date', 'insider_transactions', ['transaction_date'])
    _create_index_if_missing('ix_insider_transactions_transaction_code', 'insider_transactions', ['transaction_code'])
    _create_index_if_missing('ix_insider_transactions_transaction_acquired_disposed', 'insider_transactions', ['transaction_acquired_disposed'])

    if not _table_exists('institutional_holdings'):
        op.create_table(
            'institutional_holdings',
            sa.Column('id', sa.BigInteger(), nullable=False),
            sa.Column('filing_id', sa.BigInteger(), nullable=True),
            sa.Column('security_id', sa.BigInteger(), nullable=True),
            sa.Column('source', sa.String(length=30), nullable=False),
            sa.Column('accession_number', sa.String(length=32), nullable=False),
            sa.Column('source_row_hash', sa.String(length=64), nullable=False),
            sa.Column('filer_cik', sa.String(length=20), nullable=False),
            sa.Column('form_type', sa.String(length=20), nullable=True),
            sa.Column('filing_date', sa.Date(), nullable=True),
            sa.Column('period', sa.Date(), nullable=True),
            sa.Column('issuer_name', sa.String(length=255), nullable=True),
            sa.Column('title_of_class', sa.String(length=100), nullable=True),
            sa.Column('cusip', sa.String(length=20), nullable=True),
            sa.Column('market_value', sa.Numeric(24, 4), nullable=True),
            sa.Column('shares_or_principal_amount', sa.Numeric(24, 4), nullable=True),
            sa.Column('shares_or_principal_type', sa.String(length=10), nullable=True),
            sa.Column('put_call', sa.String(length=10), nullable=True),
            sa.Column('investment_discretion', sa.String(length=20), nullable=True),
            sa.Column('other_managers', postgresql.ARRAY(sa.String(length=255)), nullable=True),
            sa.Column('voting_authority_sole', sa.Numeric(24, 4), nullable=True),
            sa.Column('voting_authority_shared', sa.Numeric(24, 4), nullable=True),
            sa.Column('voting_authority_none', sa.Numeric(24, 4), nullable=True),
            sa.Column('file_number', sa.String(length=50), nullable=True),
            sa.Column('film_number', sa.String(length=50), nullable=True),
            sa.Column('filing_url', sa.Text(), nullable=True),
            sa.Column('created_at', sa.TIMESTAMP(timezone=True), server_default=sa.text('now()'), nullable=False),
            sa.Column('updated_at', sa.TIMESTAMP(timezone=True), server_default=sa.text('now()'), nullable=False),
            sa.ForeignKeyConstraint(['filing_id'], ['sec_filings.id']),
            sa.ForeignKeyConstraint(['security_id'], ['securities.id']),
            sa.PrimaryKeyConstraint('id'),
            sa.UniqueConstraint('source', 'accession_number', 'source_row_hash', name='_institutional_holding_row_uc'),
        )
    _create_index_if_missing('ix_institutional_holdings_filing_id', 'institutional_holdings', ['filing_id'])
    _create_index_if_missing('ix_institutional_holdings_security_id', 'institutional_holdings', ['security_id'])
    _create_index_if_missing('ix_institutional_holdings_source', 'institutional_holdings', ['source'])
    _create_index_if_missing('ix_institutional_holdings_accession_number', 'institutional_holdings', ['accession_number'])
    _create_index_if_missing('ix_institutional_holdings_filer_cik', 'institutional_holdings', ['filer_cik'])
    _create_index_if_missing('ix_institutional_holdings_form_type', 'institutional_holdings', ['form_type'])
    _create_index_if_missing('ix_institutional_holdings_filing_date', 'institutional_holdings', ['filing_date'])
    _create_index_if_missing('ix_institutional_holdings_period', 'institutional_holdings', ['period'])
    _create_index_if_missing('ix_institutional_holdings_issuer_name', 'institutional_holdings', ['issuer_name'])
    _create_index_if_missing('ix_institutional_holdings_cusip', 'institutional_holdings', ['cusip'])
    _create_index_if_missing('ix_institutional_holdings_put_call', 'institutional_holdings', ['put_call'])


def downgrade() -> None:
    raise NotImplementedError("Exchange calendar and SEC foundation migration is intentionally not downgradable.")
