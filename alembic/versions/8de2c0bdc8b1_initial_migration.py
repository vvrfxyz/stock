"""Initial migration

Revision ID: 8de2c0bdc8b1
Revises: 
Create Date: 2025-06-24 11:55:25.018509

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '8de2c0bdc8b1'
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('corporate_actions',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('security_id', sa.Integer(), nullable=False),
    sa.Column('event_date', sa.Date(), nullable=False),
    sa.Column('event_type', postgresql.ENUM('DIVIDEND', 'SPLIT', 'BONUS', name='action_type'), nullable=False),
    sa.Column('value', sa.Numeric(precision=20, scale=10), nullable=False),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('security_id', 'event_date', 'event_type', name='_security_date_type_uc')
    )
    op.create_index(op.f('ix_corporate_actions_event_date'), 'corporate_actions', ['event_date'], unique=False)
    op.create_table('daily_prices',
    sa.Column('security_id', sa.Integer(), nullable=False),
    sa.Column('date', sa.Date(), nullable=False),
    sa.Column('open', sa.Numeric(precision=19, scale=6), nullable=True),
    sa.Column('high', sa.Numeric(precision=19, scale=6), nullable=True),
    sa.Column('low', sa.Numeric(precision=19, scale=6), nullable=True),
    sa.Column('close', sa.Numeric(precision=19, scale=6), nullable=True),
    sa.Column('volume', sa.BigInteger(), nullable=True),
    sa.Column('adj_close', sa.Numeric(precision=19, scale=6), nullable=True),
    sa.Column('turnover_rate', sa.Numeric(precision=10, scale=6), nullable=True),
    sa.Column('adj_factor', sa.Numeric(precision=20, scale=12), server_default='1.0', nullable=False),
    sa.PrimaryKeyConstraint('security_id', 'date')
    )
    op.create_index(op.f('ix_daily_prices_date'), 'daily_prices', ['date'], unique=False)
    op.create_table('historical_shares',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('security_id', sa.Integer(), nullable=False),
    sa.Column('change_date', sa.Date(), nullable=False),
    sa.Column('total_shares', sa.BigInteger(), nullable=True),
    sa.Column('float_shares', sa.BigInteger(), nullable=True),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('security_id', 'change_date', name='_security_change_date_uc')
    )
    op.create_index(op.f('ix_historical_shares_change_date'), 'historical_shares', ['change_date'], unique=False)
    op.create_table('securities',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('symbol', sa.String(length=20), nullable=False),
    sa.Column('name', sa.String(length=255), nullable=True),
    sa.Column('market', postgresql.ENUM('CNA', 'HK', 'US', 'CRYPTO', 'FOREX', 'INDEX', name='market_type'), nullable=False),
    sa.Column('type', postgresql.ENUM('STOCK', 'ETF', 'INDEX', 'CRYPTO', 'FOREX', name='asset_type'), nullable=False),
    sa.Column('exchange', sa.String(length=50), nullable=True),
    sa.Column('currency', sa.String(length=10), nullable=True),
    sa.Column('sector', sa.String(length=100), nullable=True),
    sa.Column('industry', sa.String(length=100), nullable=True),
    sa.Column('is_active', sa.Boolean(), nullable=True),
    sa.Column('list_date', sa.Date(), nullable=True),
    sa.Column('delist_date', sa.Date(), nullable=True),
    sa.Column('last_updated', sa.TIMESTAMP(), server_default=sa.text('now()'), nullable=True),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_securities_symbol'), 'securities', ['symbol'], unique=True)
    op.create_table('special_adjustments',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('security_id', sa.Integer(), nullable=False),
    sa.Column('event_date', sa.Date(), nullable=False),
    sa.Column('adjustment_factor', sa.Numeric(precision=20, scale=10), nullable=False),
    sa.Column('description', sa.Text(), nullable=True),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('security_id', 'event_date', name='_security_date_uc')
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    """Downgrade schema."""
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('special_adjustments')
    op.drop_index(op.f('ix_securities_symbol'), table_name='securities')
    op.drop_table('securities')
    op.drop_index(op.f('ix_historical_shares_change_date'), table_name='historical_shares')
    op.drop_table('historical_shares')
    op.drop_index(op.f('ix_daily_prices_date'), table_name='daily_prices')
    op.drop_table('daily_prices')
    op.drop_index(op.f('ix_corporate_actions_event_date'), table_name='corporate_actions')
    op.drop_table('corporate_actions')
    # ### end Alembic commands ###
