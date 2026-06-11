"""Apply raw truth schema

Revision ID: c8d9e10f1122
Revises: b7c8d9e10f11
Create Date: 2026-05-14 09:50:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'c8d9e10f1122'
down_revision: Union[str, Sequence[str], None] = 'b7c8d9e10f11'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.drop_index(op.f('ix_daily_technical_indicators_date'), table_name='daily_technical_indicators')
    op.drop_table('daily_technical_indicators')

    op.drop_column('daily_prices', 'turnover_rate')
    op.drop_column('daily_prices', 'turnover')
    op.drop_column('daily_prices', 'split_adj_factor')
    op.drop_column('daily_prices', 'adj_factor')

    op.drop_index(op.f('ix_corporate_actions_execution_date'), table_name='corporate_actions')
    op.drop_index(op.f('ix_corporate_actions_available_at'), table_name='corporate_actions')
    op.execute(
        """
        UPDATE corporate_actions
        SET ex_date = execution_date
        WHERE action_type = 'SPLIT'
          AND ex_date IS NULL
          AND execution_date IS NOT NULL
        """
    )
    op.execute("DELETE FROM corporate_actions WHERE ex_date IS NULL")
    op.alter_column('corporate_actions', 'ex_date', existing_type=sa.Date(), nullable=False)
    op.alter_column('corporate_actions', 'source_event_id', existing_type=sa.String(length=128), nullable=False)
    op.add_column('corporate_actions', sa.Column('created_at', sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text('now()')))
    op.add_column('corporate_actions', sa.Column('updated_at', sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text('now()')))
    op.drop_column('corporate_actions', 'execution_date')
    op.drop_column('corporate_actions', 'available_at')

    op.drop_index(op.f('ix_historical_shares_change_date'), table_name='historical_shares')
    op.drop_constraint('_security_change_date_uc', 'historical_shares', type_='unique')
    op.drop_column('historical_shares', 'free_float_percent')
    op.add_column('historical_shares', sa.Column('filing_date', sa.Date(), nullable=True))
    op.add_column('historical_shares', sa.Column('period_end_date', sa.Date(), nullable=True))
    op.add_column('historical_shares', sa.Column('source', sa.String(length=30), nullable=True))
    op.add_column('historical_shares', sa.Column('created_at', sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text('now()')))
    op.alter_column('historical_shares', 'id', type_=sa.BigInteger(), existing_type=sa.Integer())
    op.execute(
        """
        UPDATE historical_shares
        SET filing_date = change_date,
            period_end_date = change_date,
            source = 'LEGACY'
        """
    )
    op.alter_column('historical_shares', 'filing_date', existing_type=sa.Date(), nullable=False)
    op.alter_column('historical_shares', 'period_end_date', existing_type=sa.Date(), nullable=False)
    op.alter_column('historical_shares', 'source', existing_type=sa.String(length=30), nullable=False)
    op.alter_column('historical_shares', 'total_shares', existing_type=sa.BigInteger(), nullable=False)
    op.drop_column('historical_shares', 'change_date')
    op.create_foreign_key(
        'historical_shares_security_id_fkey',
        'historical_shares',
        'securities',
        ['security_id'],
        ['id'],
    )
    op.create_unique_constraint(
        '_historical_shares_filing_source_uc',
        'historical_shares',
        ['security_id', 'filing_date', 'source'],
    )
    op.create_index(op.f('ix_historical_shares_security_id'), 'historical_shares', ['security_id'], unique=False)
    op.create_index(op.f('ix_historical_shares_filing_date'), 'historical_shares', ['filing_date'], unique=False)
    op.create_index(op.f('ix_historical_shares_period_end_date'), 'historical_shares', ['period_end_date'], unique=False)
    op.create_index(op.f('ix_historical_shares_source'), 'historical_shares', ['source'], unique=False)

    op.drop_constraint('_market_trade_date_uc', 'trading_calendars', type_='unique')
    op.drop_constraint('trading_calendars_pkey', 'trading_calendars', type_='primary')
    op.add_column('trading_calendars', sa.Column('is_half_day', sa.Boolean(), nullable=False, server_default=sa.text('false')))
    op.drop_column('trading_calendars', 'id')
    op.create_primary_key('trading_calendars_pkey', 'trading_calendars', ['market', 'trade_date'])


def downgrade() -> None:
    raise NotImplementedError("Raw truth schema migration is intentionally not downgradable.")
