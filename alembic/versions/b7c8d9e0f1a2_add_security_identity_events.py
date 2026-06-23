"""Add security_identity_events for identity lifecycle tracking

Revision ID: b7c8d9e0f1a2
Revises: a6b7c8d9e0f1
Create Date: 2026-06-23
"""
from alembic import op
import sqlalchemy as sa


revision = 'b7c8d9e0f1a2'
down_revision = 'a6b7c8d9e0f1'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'security_identity_events',
        sa.Column('id', sa.BigInteger(), primary_key=True),
        sa.Column('security_id', sa.BigInteger(), sa.ForeignKey('securities.id'), nullable=False, index=True),
        sa.Column('event_type', sa.String(30), nullable=False, index=True,
                  comment='RENAME / RECYCLE / MERGE / SPLIT_IDENTITY / QUARANTINE / NEW_LISTING / MANUAL'),
        sa.Column('old_symbol', sa.String(30), nullable=True),
        sa.Column('new_symbol', sa.String(30), nullable=True),
        sa.Column('related_security_id', sa.BigInteger(), nullable=True, index=True,
                  comment='合并/拆分时涉及的另一个 security_id'),
        sa.Column('resolution_source', sa.String(30), nullable=False, server_default='AUTO',
                  comment='AUTO / MANUAL / AUDIT'),
        sa.Column('confidence', sa.String(20), nullable=True, comment='HIGH / MEDIUM / LOW'),
        sa.Column('details', sa.Text(), nullable=True, comment='JSON: FIGI/CIK 匹配细节等'),
        sa.Column('created_at', sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
    )
    op.create_index('ix_identity_event_type_created', 'security_identity_events', ['event_type', 'created_at'])


def downgrade() -> None:
    op.drop_index('ix_identity_event_type_created', table_name='security_identity_events')
    op.drop_table('security_identity_events')
