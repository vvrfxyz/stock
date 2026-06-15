"""Add risk_free_rates for FRED DTB3 reference data

Revision ID: a6b7c8d9e0f1
Revises: e5f6a7b8c9d0
Create Date: 2026-06-15
"""
from alembic import op
import sqlalchemy as sa


revision = 'a6b7c8d9e0f1'
down_revision = 'e5f6a7b8c9d0'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'risk_free_rates',
        sa.Column('date', sa.Date(), nullable=False),
        sa.Column('series_id', sa.String(length=30), nullable=False),
        sa.Column('rate_pct', sa.Numeric(precision=12, scale=6), nullable=False),
        sa.Column('fetched_at', sa.TIMESTAMP(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.PrimaryKeyConstraint('date', 'series_id'),
    )


def downgrade() -> None:
    op.drop_table('risk_free_rates')
