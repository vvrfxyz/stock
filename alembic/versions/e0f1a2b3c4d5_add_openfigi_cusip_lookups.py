"""Add openfigi_cusip_lookups (OpenFIGI CUSIP->FIGI query cache with negative caching)

Revision ID: e0f1a2b3c4d5
Revises: c8d9e0f1a2b3
Create Date: 2026-07-02
"""
from alembic import op
import sqlalchemy as sa


revision = 'e0f1a2b3c4d5'
down_revision = 'c8d9e0f1a2b3'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'openfigi_cusip_lookups',
        sa.Column('cusip', sa.String(9), primary_key=True),
        sa.Column('status', sa.String(20), nullable=False,
                  comment='MATCHED / NOT_FOUND / AMBIGUOUS'),
        sa.Column('composite_figi', sa.String(20), nullable=True, index=True),
        sa.Column('share_class_figi', sa.String(20), nullable=True),
        sa.Column('ticker', sa.String(20), nullable=True),
        sa.Column('name', sa.String(255), nullable=True),
        sa.Column('security_type', sa.String(60), nullable=True),
        sa.Column('market_sector', sa.String(30), nullable=True),
        sa.Column('exch_code', sa.String(10), nullable=True),
        sa.Column('queried_at', sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False,
                  comment='最近一次实际查询 OpenFIGI 的时间；重查须显式刷新'),
    )


def downgrade() -> None:
    op.drop_table('openfigi_cusip_lookups')
