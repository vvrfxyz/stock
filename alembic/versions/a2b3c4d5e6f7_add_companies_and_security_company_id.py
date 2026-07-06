"""Add companies (PERMCO-equivalent entity) and securities.company_id

Revision ID: a2b3c4d5e6f7
Revises: f1a2b3c4d5e6
Create Date: 2026-07-06
"""
from alembic import op
import sqlalchemy as sa


revision = 'a2b3c4d5e6f7'
down_revision = 'f1a2b3c4d5e6'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'companies',
        sa.Column('id', sa.BigInteger(), primary_key=True),
        sa.Column('cik', sa.String(20), nullable=True, unique=True,
                  comment='SEC CIK 主锚；无 SEC 申报实体为 NULL'),
        sa.Column('name', sa.String(255), nullable=True, comment='公司名称'),
        sa.Column('created_at', sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column('updated_at', sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
    )

    op.add_column(
        'securities',
        sa.Column('company_id', sa.BigInteger(), sa.ForeignKey('companies.id'), nullable=True,
                  comment='所属公司实体（companies.id，PERMCO 等价物）；按 CIK 归组，未归组为 NULL'),
    )
    op.create_index('ix_securities_company_id', 'securities', ['company_id'])


def downgrade() -> None:
    op.drop_index('ix_securities_company_id', table_name='securities')
    op.drop_column('securities', 'company_id')
    op.drop_table('companies')
