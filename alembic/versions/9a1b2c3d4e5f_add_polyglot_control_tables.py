"""Add polyglot control tables

Revision ID: 9a1b2c3d4e5f
Revises: 6e9f8c1d2a7b
Create Date: 2026-05-12 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '9a1b2c3d4e5f'
down_revision: Union[str, Sequence[str], None] = '6e9f8c1d2a7b'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.drop_constraint('daily_prices_security_id_fkey', 'daily_prices', type_='foreignkey')
    op.drop_constraint('stock_dividends_security_id_fkey', 'stock_dividends', type_='foreignkey')
    op.drop_constraint('stock_splits_security_id_fkey', 'stock_splits', type_='foreignkey')
    op.drop_constraint('daily_technical_indicators_security_id_fkey', 'daily_technical_indicators', type_='foreignkey')

    op.alter_column('securities', 'id', type_=sa.BigInteger(), existing_type=sa.Integer())
    op.alter_column('daily_prices', 'security_id', type_=sa.BigInteger(), existing_type=sa.Integer())
    op.alter_column('stock_dividends', 'security_id', type_=sa.BigInteger(), existing_type=sa.Integer())
    op.alter_column('stock_splits', 'security_id', type_=sa.BigInteger(), existing_type=sa.Integer())
    op.alter_column('historical_shares', 'security_id', type_=sa.BigInteger(), existing_type=sa.Integer())
    op.alter_column('daily_technical_indicators', 'security_id', type_=sa.BigInteger(), existing_type=sa.Integer())

    op.create_foreign_key('daily_prices_security_id_fkey', 'daily_prices', 'securities', ['security_id'], ['id'])
    op.create_foreign_key('stock_dividends_security_id_fkey', 'stock_dividends', 'securities', ['security_id'], ['id'])
    op.create_foreign_key('stock_splits_security_id_fkey', 'stock_splits', 'securities', ['security_id'], ['id'])
    op.create_foreign_key('daily_technical_indicators_security_id_fkey', 'daily_technical_indicators', 'securities', ['security_id'], ['id'])

    op.add_column(
        'securities',
        sa.Column('current_symbol', sa.String(length=30), nullable=True, comment='当前最新证券代码'),
    )
    op.execute("UPDATE securities SET current_symbol = symbol WHERE current_symbol IS NULL")
    op.alter_column('securities', 'current_symbol', nullable=False)
    op.create_index(op.f('ix_securities_current_symbol'), 'securities', ['current_symbol'], unique=False)
    op.create_unique_constraint('_current_symbol_exchange_uc', 'securities', ['current_symbol', 'exchange'])

    op.add_column('securities', sa.Column('created_at', sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text('now()')))
    op.add_column('securities', sa.Column('updated_at', sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text('now()')))
    op.alter_column('securities', 'composite_figi', type_=sa.String(length=30), existing_nullable=True)
    op.add_column('securities', sa.Column('sector', sa.String(length=100), nullable=True, comment='行业板块'))

    op.create_table(
        'security_symbol_history',
        sa.Column('id', sa.BigInteger(), nullable=False),
        sa.Column('security_id', sa.BigInteger(), nullable=False),
        sa.Column('symbol', sa.String(length=30), nullable=False),
        sa.Column('exchange', sa.String(length=30), nullable=True),
        sa.Column('source', sa.String(length=30), nullable=False),
        sa.Column('start_date', sa.Date(), nullable=True),
        sa.Column('end_date', sa.Date(), nullable=True),
        sa.Column('created_at', sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text('now()')),
        sa.ForeignKeyConstraint(['security_id'], ['securities.id']),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index(op.f('ix_security_symbol_history_security_id'), 'security_symbol_history', ['security_id'], unique=False)
    op.create_index(op.f('ix_security_symbol_history_symbol'), 'security_symbol_history', ['symbol'], unique=False)
    op.create_index(op.f('ix_security_symbol_history_source'), 'security_symbol_history', ['source'], unique=False)
    op.create_index(op.f('ix_security_symbol_history_start_date'), 'security_symbol_history', ['start_date'], unique=False)
    op.create_index(op.f('ix_security_symbol_history_end_date'), 'security_symbol_history', ['end_date'], unique=False)

    op.create_table(
        'corporate_actions',
        sa.Column('id', sa.BigInteger(), nullable=False),
        sa.Column('security_id', sa.BigInteger(), nullable=False),
        sa.Column('action_type', sa.String(length=20), nullable=False, comment="'DIVIDEND' 或 'SPLIT'"),
        sa.Column('ex_date', sa.Date(), nullable=True),
        sa.Column('execution_date', sa.Date(), nullable=True),
        sa.Column('pay_date', sa.Date(), nullable=True),
        sa.Column('cash_amount', sa.Numeric(precision=20, scale=10), nullable=True),
        sa.Column('currency', sa.String(length=10), nullable=True),
        sa.Column('split_from', sa.Numeric(precision=20, scale=10), nullable=True),
        sa.Column('split_to', sa.Numeric(precision=20, scale=10), nullable=True),
        sa.Column('source', sa.String(length=30), nullable=False),
        sa.Column('source_event_id', sa.String(length=128), nullable=True),
        sa.Column('available_at', sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text('now()')),
        sa.ForeignKeyConstraint(['security_id'], ['securities.id']),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('security_id', 'action_type', 'source', 'source_event_id', name='_corporate_action_source_event_uc'),
    )
    op.create_index(op.f('ix_corporate_actions_security_id'), 'corporate_actions', ['security_id'], unique=False)
    op.create_index(op.f('ix_corporate_actions_action_type'), 'corporate_actions', ['action_type'], unique=False)
    op.create_index(op.f('ix_corporate_actions_ex_date'), 'corporate_actions', ['ex_date'], unique=False)
    op.create_index(op.f('ix_corporate_actions_execution_date'), 'corporate_actions', ['execution_date'], unique=False)
    op.create_index(op.f('ix_corporate_actions_source'), 'corporate_actions', ['source'], unique=False)
    op.create_index(op.f('ix_corporate_actions_available_at'), 'corporate_actions', ['available_at'], unique=False)

    op.create_table(
        'financial_reports',
        sa.Column('id', sa.BigInteger(), nullable=False),
        sa.Column('security_id', sa.BigInteger(), nullable=False),
        sa.Column('source', sa.String(length=30), nullable=False),
        sa.Column('form_type', sa.String(length=20), nullable=True),
        sa.Column('fiscal_period_end', sa.Date(), nullable=False),
        sa.Column('filing_date', sa.Date(), nullable=False),
        sa.Column('accepted_at', sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column('available_at', sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text('now()')),
        sa.ForeignKeyConstraint(['security_id'], ['securities.id']),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('security_id', 'source', 'fiscal_period_end', name='_financial_report_period_uc'),
    )
    op.create_index(op.f('ix_financial_reports_security_id'), 'financial_reports', ['security_id'], unique=False)
    op.create_index(op.f('ix_financial_reports_source'), 'financial_reports', ['source'], unique=False)
    op.create_index(op.f('ix_financial_reports_fiscal_period_end'), 'financial_reports', ['fiscal_period_end'], unique=False)
    op.create_index(op.f('ix_financial_reports_filing_date'), 'financial_reports', ['filing_date'], unique=False)
    op.create_index(op.f('ix_financial_reports_available_at'), 'financial_reports', ['available_at'], unique=False)


def downgrade() -> None:
    op.drop_index(op.f('ix_financial_reports_available_at'), table_name='financial_reports')
    op.drop_index(op.f('ix_financial_reports_filing_date'), table_name='financial_reports')
    op.drop_index(op.f('ix_financial_reports_fiscal_period_end'), table_name='financial_reports')
    op.drop_index(op.f('ix_financial_reports_source'), table_name='financial_reports')
    op.drop_index(op.f('ix_financial_reports_security_id'), table_name='financial_reports')
    op.drop_table('financial_reports')

    op.drop_index(op.f('ix_corporate_actions_available_at'), table_name='corporate_actions')
    op.drop_index(op.f('ix_corporate_actions_source'), table_name='corporate_actions')
    op.drop_index(op.f('ix_corporate_actions_execution_date'), table_name='corporate_actions')
    op.drop_index(op.f('ix_corporate_actions_ex_date'), table_name='corporate_actions')
    op.drop_index(op.f('ix_corporate_actions_action_type'), table_name='corporate_actions')
    op.drop_index(op.f('ix_corporate_actions_security_id'), table_name='corporate_actions')
    op.drop_table('corporate_actions')

    op.drop_index(op.f('ix_security_symbol_history_end_date'), table_name='security_symbol_history')
    op.drop_index(op.f('ix_security_symbol_history_start_date'), table_name='security_symbol_history')
    op.drop_index(op.f('ix_security_symbol_history_source'), table_name='security_symbol_history')
    op.drop_index(op.f('ix_security_symbol_history_symbol'), table_name='security_symbol_history')
    op.drop_index(op.f('ix_security_symbol_history_security_id'), table_name='security_symbol_history')
    op.drop_table('security_symbol_history')

    op.drop_column('securities', 'sector')
    op.alter_column('securities', 'composite_figi', type_=sa.String(length=20), existing_nullable=True)
    op.drop_column('securities', 'updated_at')
    op.drop_column('securities', 'created_at')
    op.drop_constraint('_current_symbol_exchange_uc', 'securities', type_='unique')
    op.drop_index(op.f('ix_securities_current_symbol'), table_name='securities')
    op.drop_column('securities', 'current_symbol')

    op.drop_constraint('daily_prices_security_id_fkey', 'daily_prices', type_='foreignkey')
    op.drop_constraint('stock_dividends_security_id_fkey', 'stock_dividends', type_='foreignkey')
    op.drop_constraint('stock_splits_security_id_fkey', 'stock_splits', type_='foreignkey')
    op.drop_constraint('daily_technical_indicators_security_id_fkey', 'daily_technical_indicators', type_='foreignkey')

    op.alter_column('daily_technical_indicators', 'security_id', type_=sa.Integer(), existing_type=sa.BigInteger())
    op.alter_column('historical_shares', 'security_id', type_=sa.Integer(), existing_type=sa.BigInteger())
    op.alter_column('stock_splits', 'security_id', type_=sa.Integer(), existing_type=sa.BigInteger())
    op.alter_column('stock_dividends', 'security_id', type_=sa.Integer(), existing_type=sa.BigInteger())
    op.alter_column('daily_prices', 'security_id', type_=sa.Integer(), existing_type=sa.BigInteger())
    op.alter_column('securities', 'id', type_=sa.Integer(), existing_type=sa.BigInteger())

    op.create_foreign_key('daily_technical_indicators_security_id_fkey', 'daily_technical_indicators', 'securities', ['security_id'], ['id'])
    op.create_foreign_key('stock_splits_security_id_fkey', 'stock_splits', 'securities', ['security_id'], ['id'])
    op.create_foreign_key('stock_dividends_security_id_fkey', 'stock_dividends', 'securities', ['security_id'], ['id'])
    op.create_foreign_key('daily_prices_security_id_fkey', 'daily_prices', 'securities', ['security_id'], ['id'])
