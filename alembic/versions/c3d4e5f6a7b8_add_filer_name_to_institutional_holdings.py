"""Add filer_name to institutional_holdings (raw manager name from 13F primary_doc)

Revision ID: c3d4e5f6a7b8
Revises: b2c3d4e5f6a7
Create Date: 2026-06-12 00:00:00.000000

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = 'c3d4e5f6a7b8'
down_revision: Union[str, Sequence[str], None] = 'b2c3d4e5f6a7'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        'institutional_holdings',
        sa.Column('filer_name', sa.String(length=255), nullable=True),
    )
    op.create_index(
        op.f('ix_institutional_holdings_filer_name'),
        'institutional_holdings',
        ['filer_name'],
    )


def downgrade() -> None:
    op.drop_index(op.f('ix_institutional_holdings_filer_name'), table_name='institutional_holdings')
    op.drop_column('institutional_holdings', 'filer_name')
