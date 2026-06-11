"""Merge dividends and splits into corporate_actions

Revision ID: a4b7c9d1e2f3
Revises: 9a1b2c3d4e5f
Create Date: 2026-05-13 11:45:00.000000

"""
from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = 'a4b7c9d1e2f3'
down_revision: Union[str, Sequence[str], None] = '9a1b2c3d4e5f'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        """
        INSERT INTO corporate_actions (
            security_id,
            action_type,
            ex_date,
            pay_date,
            cash_amount,
            currency,
            source,
            source_event_id,
            available_at
        )
        SELECT
            security_id,
            'DIVIDEND',
            ex_dividend_date,
            pay_date,
            cash_amount,
            currency,
            'MASSIVE',
            COALESCE(
                source_event_id,
                'massive-dividend:'
                || security_id::text
                || ':'
                || ex_dividend_date::text
                || ':'
                || to_char(cash_amount, 'FM999999999999999990.0000000000')
            ),
            now()
        FROM stock_dividends
        WHERE ex_dividend_date IS NOT NULL
          AND cash_amount IS NOT NULL
          AND currency IS NOT NULL
        ON CONFLICT ON CONSTRAINT _corporate_action_source_event_uc DO NOTHING
        """
    )
    op.execute(
        """
        INSERT INTO corporate_actions (
            security_id,
            action_type,
            execution_date,
            split_from,
            split_to,
            source,
            source_event_id,
            available_at
        )
        SELECT
            security_id,
            'SPLIT',
            execution_date,
            split_from,
            split_to,
            'MASSIVE',
            COALESCE(
                source_event_id,
                'massive-split:'
                || security_id::text
                || ':'
                || execution_date::text
                || ':'
                || to_char(split_from, 'FM999999999999999990.0000000000')
                || ':'
                || to_char(split_to, 'FM999999999999999990.0000000000')
            ),
            now()
        FROM stock_splits
        WHERE execution_date IS NOT NULL
          AND split_from IS NOT NULL
          AND split_to IS NOT NULL
        ON CONFLICT ON CONSTRAINT _corporate_action_source_event_uc DO NOTHING
        """
    )

    op.drop_table('stock_splits')
    op.drop_table('stock_dividends')


def downgrade() -> None:
    raise NotImplementedError(
        "Downgrade is intentionally unsupported after merging stock_dividends "
        "and stock_splits into corporate_actions."
    )
