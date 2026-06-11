"""Add sp500 constituent history

Revision ID: 6e9f8c1d2a7b
Revises: 2f6f7c8d9e10
Create Date: 2026-03-19 12:30:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '6e9f8c1d2a7b'
down_revision: Union[str, Sequence[str], None] = '2f6f7c8d9e10'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'sp500_constituent_history',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('symbol', sa.String(length=30), nullable=False, comment="标准化的小写证券代码 (例如 'aapl', 'brk.b')"),
        sa.Column('security_name', sa.String(length=255), nullable=True, comment='成分股名称'),
        sa.Column('sector', sa.String(length=255), nullable=True, comment='GICS 一级行业'),
        sa.Column('sub_sector', sa.String(length=255), nullable=True, comment='GICS 子行业'),
        sa.Column('head_quarters', sa.String(length=255), nullable=True, comment='公司总部所在地'),
        sa.Column('start_date', sa.Date(), nullable=False, comment='纳入指数的生效日期'),
        sa.Column('end_date', sa.Date(), nullable=True, comment='移出指数后的最后一个有效日期 (含)'),
        sa.Column('is_current', sa.Boolean(), nullable=False, server_default=sa.text('false'), comment='最近一次同步时是否仍为现任成分股'),
        sa.Column('source', sa.String(length=32), nullable=False, server_default=sa.text("'fmp'"), comment='来源标签 (如 fmp)'),
        sa.Column('last_synced_at', sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text('now()'), comment='最近一次从外部源成功同步该区间的时间'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('symbol', 'start_date', 'source', name='_sp500_hist_symbol_start_source_uc'),
    )
    op.create_index(op.f('ix_sp500_constituent_history_symbol'), 'sp500_constituent_history', ['symbol'], unique=False)
    op.create_index(op.f('ix_sp500_constituent_history_start_date'), 'sp500_constituent_history', ['start_date'], unique=False)
    op.create_index(op.f('ix_sp500_constituent_history_end_date'), 'sp500_constituent_history', ['end_date'], unique=False)
    op.create_index(op.f('ix_sp500_constituent_history_source'), 'sp500_constituent_history', ['source'], unique=False)


def downgrade() -> None:
    op.drop_index(op.f('ix_sp500_constituent_history_source'), table_name='sp500_constituent_history')
    op.drop_index(op.f('ix_sp500_constituent_history_end_date'), table_name='sp500_constituent_history')
    op.drop_index(op.f('ix_sp500_constituent_history_start_date'), table_name='sp500_constituent_history')
    op.drop_index(op.f('ix_sp500_constituent_history_symbol'), table_name='sp500_constituent_history')
    op.drop_table('sp500_constituent_history')
