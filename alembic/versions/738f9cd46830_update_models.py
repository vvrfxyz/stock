"""update models

Revision ID: 738f9cd46830
Revises: c56d778a3d71
Create Date: 2025-06-24 19:09:15.217246

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '738f9cd46830'
down_revision: Union[str, Sequence[str], None] = 'c56d778a3d71'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column('daily_prices', 'adj_factor',
               existing_type=sa.NUMERIC(precision=20, scale=12),
               type_=sa.Numeric(precision=20, scale=6),
               existing_nullable=False,
               existing_server_default=sa.text('1.0'))
    op.alter_column('daily_prices', 'event_factor',
               existing_type=sa.NUMERIC(precision=20, scale=12),
               type_=sa.Numeric(precision=20, scale=6),
               existing_nullable=False)
    # ### end Alembic commands ###


def downgrade() -> None:
    """Downgrade schema."""
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column('daily_prices', 'event_factor',
               existing_type=sa.Numeric(precision=20, scale=6),
               type_=sa.NUMERIC(precision=20, scale=12),
               existing_nullable=False)
    op.alter_column('daily_prices', 'adj_factor',
               existing_type=sa.Numeric(precision=20, scale=6),
               type_=sa.NUMERIC(precision=20, scale=12),
               existing_nullable=False,
               existing_server_default=sa.text('1.0'))
    # ### end Alembic commands ###
