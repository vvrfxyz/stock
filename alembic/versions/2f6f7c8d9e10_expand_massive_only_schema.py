"""Expand Massive-only schema

Revision ID: 2f6f7c8d9e10
Revises: d60eeb05622e
Create Date: 2026-03-14 20:45:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '2f6f7c8d9e10'
down_revision: Union[str, Sequence[str], None] = 'd60eeb05622e'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        'daily_prices',
        sa.Column('trade_count', sa.BigInteger(), nullable=True, comment='成交笔数'),
    )
    op.add_column(
        'daily_prices',
        sa.Column('pre_market', sa.Numeric(precision=19, scale=6), nullable=True, comment='盘前价格'),
    )
    op.add_column(
        'daily_prices',
        sa.Column('after_hours', sa.Numeric(precision=19, scale=6), nullable=True, comment='盘后价格'),
    )
    op.add_column(
        'daily_prices',
        sa.Column(
            'split_adj_factor',
            sa.Numeric(precision=20, scale=6),
            nullable=True,
            server_default=sa.text('1.0'),
            comment='仅拆股前复权因子 (最新交易日=1)',
        ),
    )

    op.add_column(
        'stock_dividends',
        sa.Column('source_event_id', sa.String(length=128), nullable=True, comment='Massive 事件唯一标识'),
    )
    op.add_column(
        'stock_dividends',
        sa.Column('distribution_type', sa.String(length=32), nullable=True, comment='分红分布类型 (recurring, special 等)'),
    )
    op.add_column(
        'stock_dividends',
        sa.Column('historical_adjustment_factor', sa.Numeric(precision=20, scale=10), nullable=True, comment='Massive 返回的历史调整因子'),
    )
    op.add_column(
        'stock_dividends',
        sa.Column('split_adjusted_cash_amount', sa.Numeric(precision=20, scale=10), nullable=True, comment='按后续拆股调整后的每股分红金额'),
    )

    op.add_column(
        'stock_splits',
        sa.Column('source_event_id', sa.String(length=128), nullable=True, comment='Massive 事件唯一标识'),
    )
    op.add_column(
        'stock_splits',
        sa.Column('adjustment_type', sa.String(length=32), nullable=True, comment='调整类型 (forward_split, reverse_split 等)'),
    )
    op.add_column(
        'stock_splits',
        sa.Column('historical_adjustment_factor', sa.Numeric(precision=20, scale=10), nullable=True, comment='Massive 返回的历史调整因子'),
    )

    op.add_column(
        'historical_shares',
        sa.Column('free_float_percent', sa.Numeric(precision=10, scale=6), nullable=True),
    )

    op.create_table(
        'daily_technical_indicators',
        sa.Column('security_id', sa.Integer(), nullable=False),
        sa.Column('date', sa.Date(), nullable=False),
        sa.Column('sma_20', sa.Numeric(precision=19, scale=6), nullable=True),
        sa.Column('sma_50', sa.Numeric(precision=19, scale=6), nullable=True),
        sa.Column('sma_200', sa.Numeric(precision=19, scale=6), nullable=True),
        sa.Column('ema_12', sa.Numeric(precision=19, scale=6), nullable=True),
        sa.Column('ema_26', sa.Numeric(precision=19, scale=6), nullable=True),
        sa.Column('ema_50', sa.Numeric(precision=19, scale=6), nullable=True),
        sa.Column('ema_200', sa.Numeric(precision=19, scale=6), nullable=True),
        sa.Column('macd_line', sa.Numeric(precision=19, scale=6), nullable=True),
        sa.Column('macd_signal', sa.Numeric(precision=19, scale=6), nullable=True),
        sa.Column('macd_hist', sa.Numeric(precision=19, scale=6), nullable=True),
        sa.Column('rsi_14', sa.Numeric(precision=19, scale=6), nullable=True),
        sa.Column('atr_14', sa.Numeric(precision=19, scale=6), nullable=True),
        sa.Column('bb_middle_20', sa.Numeric(precision=19, scale=6), nullable=True),
        sa.Column('bb_upper_20', sa.Numeric(precision=19, scale=6), nullable=True),
        sa.Column('bb_lower_20', sa.Numeric(precision=19, scale=6), nullable=True),
        sa.Column('obv', sa.Numeric(precision=25, scale=4), nullable=True),
        sa.ForeignKeyConstraint(['security_id'], ['securities.id']),
        sa.PrimaryKeyConstraint('security_id', 'date'),
        sa.UniqueConstraint('security_id', 'date', name='_security_indicator_date_uc'),
    )
    op.create_index(op.f('ix_daily_technical_indicators_date'), 'daily_technical_indicators', ['date'], unique=False)


def downgrade() -> None:
    op.drop_index(op.f('ix_daily_technical_indicators_date'), table_name='daily_technical_indicators')
    op.drop_table('daily_technical_indicators')

    op.drop_column('historical_shares', 'free_float_percent')

    op.drop_column('stock_splits', 'historical_adjustment_factor')
    op.drop_column('stock_splits', 'adjustment_type')
    op.drop_column('stock_splits', 'source_event_id')

    op.drop_column('stock_dividends', 'split_adjusted_cash_amount')
    op.drop_column('stock_dividends', 'historical_adjustment_factor')
    op.drop_column('stock_dividends', 'distribution_type')
    op.drop_column('stock_dividends', 'source_event_id')

    op.drop_column('daily_prices', 'split_adj_factor')
    op.drop_column('daily_prices', 'after_hours')
    op.drop_column('daily_prices', 'pre_market')
    op.drop_column('daily_prices', 'trade_count')
