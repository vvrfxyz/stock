"""Add company_events (company lineage / merger edge table)

Revision ID: b3c4d5e6f7a8
Revises: a2b3c4d5e6f7
Create Date: 2026-07-07
"""
from alembic import op
import sqlalchemy as sa


revision = 'b3c4d5e6f7a8'
down_revision = 'a2b3c4d5e6f7'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'company_events',
        sa.Column('id', sa.BigInteger(), primary_key=True),
        sa.Column('predecessor_company_id', sa.BigInteger(), sa.ForeignKey('companies.id'),
                  nullable=False, index=True, comment='前身公司实体（companies.id）'),
        sa.Column('successor_company_id', sa.BigInteger(), sa.ForeignKey('companies.id'),
                  nullable=False, index=True, comment='继承公司实体（companies.id）'),
        sa.Column('event_date', sa.Date(), nullable=False, index=True),
        sa.Column('event_type', sa.String(20), nullable=False, index=True,
                  comment='MERGER / CIK_CHANGE / SPINOFF / RENAME'),
        sa.Column('evidence', sa.Text(), nullable=True, comment='accession number / 令牌 / 推断依据'),
        sa.Column('source', sa.String(30), nullable=True, comment='DELISTING / MANUAL'),
        sa.Column('created_at', sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column('updated_at', sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint('predecessor_company_id', 'successor_company_id', 'event_date', 'event_type',
                            name='_company_event_edge_uc'),
    )


def downgrade() -> None:
    op.drop_table('company_events')
