"""Add delisting_events (delisting outcome facts) and sec_filings.items (8-K item codes)

Revision ID: f1a2b3c4d5e6
Revises: e0f1a2b3c4d5
Create Date: 2026-07-06
"""
from alembic import op
import sqlalchemy as sa


revision = 'f1a2b3c4d5e6'
down_revision = 'e0f1a2b3c4d5'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'delisting_events',
        sa.Column('id', sa.BigInteger(), primary_key=True),
        sa.Column('security_id', sa.BigInteger(), sa.ForeignKey('securities.id'), nullable=False, index=True),
        sa.Column('delist_date', sa.Date(), nullable=False, index=True),
        sa.Column('reason_code', sa.String(20), nullable=True,
                  comment='MERGER / ACQUISITION_CASH / ACQUISITION_STOCK / BANKRUPTCY / '
                          'LIQUIDATION / EXCHANGE_DROP / VOLUNTARY / FUND_CLOSURE / UNKNOWN'),
        sa.Column('reason_confidence', sa.String(10), nullable=True,
                  comment='HIGH / MEDIUM / LOW（由分类证据强度决定）'),
        sa.Column('acquirer_name', sa.String(255), nullable=True, comment='并购类：收购方名称'),
        sa.Column('consideration_cash', sa.Numeric(20, 6), nullable=True, comment='每股现金对价'),
        sa.Column('consideration_stock_ratio', sa.Numeric(20, 10), nullable=True,
                  comment='换股比（对价含股票时）'),
        sa.Column('final_price', sa.Numeric(19, 6), nullable=True,
                  comment='退市前最后可靠成交价（含 OTC 尾巴）'),
        sa.Column('final_price_date', sa.Date(), nullable=True),
        sa.Column('delisting_return', sa.Numeric(12, 8), nullable=True,
                  comment='实测退市收益 = (实际所得-final_price)/final_price；无实据时 NULL'),
        sa.Column('source', sa.String(30), nullable=True,
                  comment='FORM25 / 8K / TICKER_EVENT / PRICE_INFERRED / MANUAL'),
        sa.Column('evidence', sa.Text(), nullable=True, comment='accession number / 事件 id / 推断依据'),
        sa.Column('created_at', sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column('updated_at', sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint('security_id', 'delist_date', name='_delisting_event_security_date_uc'),
    )

    op.add_column(
        'sec_filings',
        sa.Column('items', sa.String(255), nullable=True,
                  comment='8-K item codes as reported, comma-separated e.g. 2.01,9.01'),
    )


def downgrade() -> None:
    op.drop_column('sec_filings', 'items')
    op.drop_table('delisting_events')
