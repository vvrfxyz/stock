"""Drop redundant daily_prices unique constraint

复合主键 (security_id, date) 已经保证唯一性并提供索引，
_security_id_date_uc 是完全冗余的第二个唯一索引：
浪费约一半的索引存储并拖慢每次价格写入。

Revision ID: a1b2c3d4e5f6
Revises: f3b4c5d6e7f8
Create Date: 2026-06-11 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = 'a1b2c3d4e5f6'
down_revision: Union[str, Sequence[str], None] = 'f3b4c5d6e7f8'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("ALTER TABLE daily_prices DROP CONSTRAINT IF EXISTS _security_id_date_uc")


def downgrade() -> None:
    op.create_unique_constraint('_security_id_date_uc', 'daily_prices', ['security_id', 'date'])
