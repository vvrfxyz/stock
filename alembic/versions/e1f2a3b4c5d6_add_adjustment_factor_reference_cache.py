"""Add adjustment factor reference/cache tables

Revision ID: e1f2a3b4c5d6
Revises: d9e0f1a2b3c4
Create Date: 2026-05-14 14:30:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'e1f2a3b4c5d6'
down_revision: Union[str, Sequence[str], None] = 'd9e0f1a2b3c4'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _table_exists(table_name: str) -> bool:
    conn = op.get_bind()
    return bool(
        conn.execute(
            sa.text(
                """
                SELECT 1
                FROM information_schema.tables
                WHERE table_name = :table_name
                """
            ),
            {"table_name": table_name},
        ).first()
    )


def _create_index_if_missing(index_name: str, table_name: str, columns: list[str]) -> None:
    conn = op.get_bind()
    exists = conn.execute(
        sa.text("SELECT 1 FROM pg_indexes WHERE tablename = :table_name AND indexname = :index_name"),
        {"table_name": table_name, "index_name": index_name},
    ).first()
    if not exists:
        op.create_index(index_name, table_name, columns, unique=False)


def upgrade() -> None:
    if not _table_exists('vendor_adjustment_factors'):
        op.create_table(
            'vendor_adjustment_factors',
            sa.Column('id', sa.BigInteger(), nullable=False),
            sa.Column('security_id', sa.BigInteger(), nullable=False),
            sa.Column('date', sa.Date(), nullable=False),
            sa.Column('source', sa.String(length=30), nullable=False),
            sa.Column('factor_type', sa.String(length=30), nullable=False),
            sa.Column('factor_key', sa.String(length=160), nullable=False),
            sa.Column('source_event_id', sa.String(length=128), nullable=True),
            sa.Column('adjustment_factor', sa.Numeric(24, 12), nullable=False),
            sa.Column('raw_close', sa.Numeric(19, 6), nullable=True),
            sa.Column('adjusted_close', sa.Numeric(19, 6), nullable=True),
            sa.Column('as_of_date', sa.Date(), nullable=True),
            sa.Column('created_at', sa.TIMESTAMP(timezone=True), server_default=sa.text('now()'), nullable=False),
            sa.Column('updated_at', sa.TIMESTAMP(timezone=True), server_default=sa.text('now()'), nullable=False),
            sa.ForeignKeyConstraint(['security_id'], ['securities.id']),
            sa.PrimaryKeyConstraint('id'),
            sa.UniqueConstraint('security_id', 'source', 'factor_key', name='_vendor_adjustment_factor_key_uc'),
        )
    _create_index_if_missing('ix_vendor_adjustment_factors_security_id', 'vendor_adjustment_factors', ['security_id'])
    _create_index_if_missing('ix_vendor_adjustment_factors_date', 'vendor_adjustment_factors', ['date'])
    _create_index_if_missing('ix_vendor_adjustment_factors_source', 'vendor_adjustment_factors', ['source'])
    _create_index_if_missing('ix_vendor_adjustment_factors_factor_type', 'vendor_adjustment_factors', ['factor_type'])
    _create_index_if_missing('ix_vendor_adjustment_factors_source_event_id', 'vendor_adjustment_factors', ['source_event_id'])
    _create_index_if_missing('ix_vendor_adjustment_factors_as_of_date', 'vendor_adjustment_factors', ['as_of_date'])

    if not _table_exists('computed_adjustment_factors'):
        op.create_table(
            'computed_adjustment_factors',
            sa.Column('id', sa.BigInteger(), nullable=False),
            sa.Column('security_id', sa.BigInteger(), nullable=False),
            sa.Column('date', sa.Date(), nullable=False),
            sa.Column('methodology_version', sa.String(length=50), nullable=False),
            sa.Column('factor_type', sa.String(length=30), nullable=False),
            sa.Column('factor_key', sa.String(length=160), nullable=False),
            sa.Column('source_event_id', sa.String(length=128), nullable=True),
            sa.Column('action_type', sa.String(length=20), nullable=True),
            sa.Column('single_event_factor', sa.Numeric(24, 12), nullable=True),
            sa.Column('cumulative_factor', sa.Numeric(24, 12), nullable=False),
            sa.Column('previous_close', sa.Numeric(19, 6), nullable=True),
            sa.Column('event_hash', sa.String(length=64), nullable=False),
            sa.Column('as_of_date', sa.Date(), nullable=True),
            sa.Column('built_at', sa.TIMESTAMP(timezone=True), server_default=sa.text('now()'), nullable=False),
            sa.ForeignKeyConstraint(['security_id'], ['securities.id']),
            sa.PrimaryKeyConstraint('id'),
            sa.UniqueConstraint('security_id', 'methodology_version', 'factor_key', name='_computed_adjustment_factor_key_uc'),
        )
    _create_index_if_missing('ix_computed_adjustment_factors_security_id', 'computed_adjustment_factors', ['security_id'])
    _create_index_if_missing('ix_computed_adjustment_factors_date', 'computed_adjustment_factors', ['date'])
    _create_index_if_missing('ix_computed_adjustment_factors_methodology_version', 'computed_adjustment_factors', ['methodology_version'])
    _create_index_if_missing('ix_computed_adjustment_factors_factor_type', 'computed_adjustment_factors', ['factor_type'])
    _create_index_if_missing('ix_computed_adjustment_factors_source_event_id', 'computed_adjustment_factors', ['source_event_id'])
    _create_index_if_missing('ix_computed_adjustment_factors_action_type', 'computed_adjustment_factors', ['action_type'])
    _create_index_if_missing('ix_computed_adjustment_factors_as_of_date', 'computed_adjustment_factors', ['as_of_date'])


def downgrade() -> None:
    raise NotImplementedError("Adjustment factor reference/cache migration is intentionally not downgradable.")
