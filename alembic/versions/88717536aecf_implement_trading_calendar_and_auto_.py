"""Implement trading calendar and auto full refresh logic

Revision ID: 88717536aecf
Revises: 3bf1dd222fb8
Create Date: 2025-06-25 15:01:48.223131

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '88717536aecf'
down_revision: Union[str, Sequence[str], None] = '3bf1dd222fb8'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('trading_calendars',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('market', postgresql.ENUM('CNA', 'HK', 'US', 'CRYPTO', 'FOREX', 'INDEX', name='market_type', create_type=False), nullable=False),
    sa.Column('trade_date', sa.Date(), nullable=False),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('market', 'trade_date', name='_market_trade_date_uc')
    )
    op.create_index(op.f('ix_trading_calendars_market'), 'trading_calendars', ['market'], unique=False)
    op.create_index(op.f('ix_trading_calendars_trade_date'), 'trading_calendars', ['trade_date'], unique=False)
    op.add_column('securities', sa.Column('last_updated_at', sa.TIMESTAMP(timezone=True), server_default=sa.text('now()'), nullable=True, comment='记录行任意更新的时间'))
    op.add_column('securities', sa.Column('info_last_updated_at', sa.TIMESTAMP(timezone=True), server_default=sa.text('now()'), nullable=True, comment='基本信息（info）上次成功更新的时间'))
    op.add_column('securities', sa.Column('full_data_last_updated_at', sa.TIMESTAMP(timezone=True), nullable=True, comment='上一次全量历史数据更新的成功时间'))
    op.add_column('securities', sa.Column('full_refresh_interval', sa.Integer(), server_default='30', nullable=False, comment='自动全量刷新的随机周期（天）'))
    op.create_index(op.f('ix_securities_is_active'), 'securities', ['is_active'], unique=False)
    op.drop_column('securities', 'last_updated')
    op.drop_column('securities', 'info_last_updated')
    # ### end Alembic commands ###


def downgrade() -> None:
    """Downgrade schema."""
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('securities', sa.Column('info_last_updated', postgresql.TIMESTAMP(), server_default=sa.text('now()'), autoincrement=False, nullable=True))
    op.add_column('securities', sa.Column('last_updated', postgresql.TIMESTAMP(), server_default=sa.text('now()'), autoincrement=False, nullable=True))
    op.drop_index(op.f('ix_securities_is_active'), table_name='securities')
    op.drop_column('securities', 'full_refresh_interval')
    op.drop_column('securities', 'full_data_last_updated_at')
    op.drop_column('securities', 'info_last_updated_at')
    op.drop_column('securities', 'last_updated_at')
    op.drop_index(op.f('ix_trading_calendars_trade_date'), table_name='trading_calendars')
    op.drop_index(op.f('ix_trading_calendars_market'), table_name='trading_calendars')
    op.drop_table('trading_calendars')
    # ### end Alembic commands ###
