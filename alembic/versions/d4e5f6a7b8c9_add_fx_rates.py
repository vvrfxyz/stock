"""Add fx_rates (ECB daily reference rates for non-USD dividend factor conversion)

Revision ID: d4e5f6a7b8c9
Revises: c3d4e5f6a7b8
Create Date: 2026-06-12 00:00:00.000000

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = 'd4e5f6a7b8c9'
down_revision: Union[str, Sequence[str], None] = 'c3d4e5f6a7b8'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'fx_rates',
        sa.Column('rate_date', sa.Date(), nullable=False),
        sa.Column('base_currency', sa.String(length=10), nullable=False),
        sa.Column('quote_currency', sa.String(length=10), nullable=False),
        sa.Column('source', sa.String(length=30), nullable=False),
        sa.Column('rate', sa.Numeric(precision=20, scale=10), nullable=False),
        sa.Column('created_at', sa.TIMESTAMP(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('updated_at', sa.TIMESTAMP(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.PrimaryKeyConstraint('rate_date', 'base_currency', 'quote_currency', 'source'),
    )


def downgrade() -> None:
    op.drop_table('fx_rates')
