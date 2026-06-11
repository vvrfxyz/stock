"""Add sec_fundamental_facts (curated XBRL facts, point-in-time by filed_date)

Revision ID: b2c3d4e5f6a7
Revises: a1b2c3d4e5f6
Create Date: 2026-06-11 00:00:00.000000

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = 'b2c3d4e5f6a7'
down_revision: Union[str, Sequence[str], None] = 'a1b2c3d4e5f6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'sec_fundamental_facts',
        sa.Column('id', sa.BigInteger(), nullable=False),
        sa.Column('security_id', sa.BigInteger(), nullable=True),
        sa.Column('cik', sa.String(length=20), nullable=False),
        sa.Column('taxonomy', sa.String(length=20), nullable=False),
        sa.Column('concept', sa.String(length=120), nullable=False),
        sa.Column('unit', sa.String(length=40), nullable=False),
        sa.Column('period_start', sa.Date(), nullable=False),
        sa.Column('period_end', sa.Date(), nullable=False),
        sa.Column('is_instant', sa.Boolean(), nullable=False),
        sa.Column('value', sa.Numeric(precision=28, scale=6), nullable=False),
        sa.Column('fiscal_year', sa.Integer(), nullable=True),
        sa.Column('fiscal_period', sa.String(length=10), nullable=True),
        sa.Column('form_type', sa.String(length=30), nullable=True),
        sa.Column('accession_number', sa.String(length=32), nullable=False),
        sa.Column('filed_date', sa.Date(), nullable=False),
        sa.Column('frame', sa.String(length=30), nullable=True),
        sa.Column('created_at', sa.TIMESTAMP(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('updated_at', sa.TIMESTAMP(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.ForeignKeyConstraint(['security_id'], ['securities.id']),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint(
            'cik', 'taxonomy', 'concept', 'unit', 'period_start', 'period_end', 'accession_number',
            name='_sec_fundamental_fact_uc',
        ),
    )
    op.create_index('ix_sec_fundamental_facts_security_id', 'sec_fundamental_facts', ['security_id'])
    op.create_index('ix_sec_fundamental_facts_cik', 'sec_fundamental_facts', ['cik'])
    op.create_index('ix_sec_fundamental_facts_concept', 'sec_fundamental_facts', ['concept'])
    op.create_index('ix_sec_fundamental_facts_period_end', 'sec_fundamental_facts', ['period_end'])
    op.create_index('ix_sec_fundamental_facts_form_type', 'sec_fundamental_facts', ['form_type'])
    op.create_index('ix_sec_fundamental_facts_filed_date', 'sec_fundamental_facts', ['filed_date'])
    # 读取层主路径：某证券某概念按 point-in-time 取最新申报
    op.create_index(
        'ix_sec_fund_facts_lookup',
        'sec_fundamental_facts',
        ['security_id', 'concept', 'filed_date', 'period_end'],
    )


def downgrade() -> None:
    op.drop_index('ix_sec_fund_facts_lookup', table_name='sec_fundamental_facts')
    op.drop_index('ix_sec_fundamental_facts_filed_date', table_name='sec_fundamental_facts')
    op.drop_index('ix_sec_fundamental_facts_form_type', table_name='sec_fundamental_facts')
    op.drop_index('ix_sec_fundamental_facts_period_end', table_name='sec_fundamental_facts')
    op.drop_index('ix_sec_fundamental_facts_concept', table_name='sec_fundamental_facts')
    op.drop_index('ix_sec_fundamental_facts_cik', table_name='sec_fundamental_facts')
    op.drop_index('ix_sec_fundamental_facts_security_id', table_name='sec_fundamental_facts')
    op.drop_table('sec_fundamental_facts')
