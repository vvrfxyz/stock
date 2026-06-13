"""Use partial unique indexes for active security symbols

Revision ID: e5f6a7b8c9d0
Revises: d4e5f6a7b8c9
Create Date: 2026-06-13
"""
from alembic import op
import sqlalchemy as sa


revision = 'e5f6a7b8c9d0'
down_revision = 'd4e5f6a7b8c9'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_constraint('_current_symbol_exchange_uc', 'securities', type_='unique')
    op.drop_constraint('_symbol_market_type_uc', 'securities', type_='unique')
    op.create_index(
        '_active_symbol_uc',
        'securities',
        ['symbol'],
        unique=True,
        postgresql_where=sa.text('is_active IS TRUE'),
    )
    op.create_index(
        '_active_current_symbol_exchange_uc',
        'securities',
        ['current_symbol', 'exchange'],
        unique=True,
        postgresql_where=sa.text('is_active IS TRUE'),
    )


def downgrade() -> None:
    op.drop_index('_active_current_symbol_exchange_uc', table_name='securities')
    op.drop_index('_active_symbol_uc', table_name='securities')
    op.create_unique_constraint('_symbol_market_type_uc', 'securities', ['symbol'])
    op.create_unique_constraint('_current_symbol_exchange_uc', 'securities', ['current_symbol', 'exchange'])
