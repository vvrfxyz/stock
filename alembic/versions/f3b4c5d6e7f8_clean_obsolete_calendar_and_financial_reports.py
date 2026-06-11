"""Clean obsolete schema

Revision ID: f3b4c5d6e7f8
Revises: f2a3b4c5d6e7
Create Date: 2026-05-14 17:05:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'f3b4c5d6e7f8'
down_revision: Union[str, Sequence[str], None] = 'f2a3b4c5d6e7'
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


def _column_exists(table_name: str, column_name: str) -> bool:
    conn = op.get_bind()
    return bool(
        conn.execute(
            sa.text(
                """
                SELECT 1
                FROM information_schema.columns
                WHERE table_name = :table_name
                  AND column_name = :column_name
                """
            ),
            {"table_name": table_name, "column_name": column_name},
        ).first()
    )


def _index_exists(index_name: str) -> bool:
    conn = op.get_bind()
    return bool(
        conn.execute(
            sa.text("SELECT 1 FROM pg_indexes WHERE indexname = :index_name"),
            {"index_name": index_name},
        ).first()
    )


def upgrade() -> None:
    if _table_exists('financial_reports'):
        op.drop_table('financial_reports')

    if _column_exists('securities', 'em_code'):
        if _index_exists('ix_securities_em_code'):
            op.drop_index('ix_securities_em_code', table_name='securities')
        op.drop_column('securities', 'em_code')

    if _column_exists('trading_calendars', 'market'):
        if _index_exists('ix_trading_calendars_market'):
            op.drop_index('ix_trading_calendars_market', table_name='trading_calendars')
        op.drop_column('trading_calendars', 'market')


def downgrade() -> None:
    raise NotImplementedError("Obsolete schema cleanup is intentionally not downgradable.")
