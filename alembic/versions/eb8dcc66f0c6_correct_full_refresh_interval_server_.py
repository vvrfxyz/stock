"""Correct full_refresh_interval server_default and randomize existing values

Revision ID: eb8dcc66f0c6
Revises: 88717536aecf
Create Date: 2025-06-25 15:14:21.953684

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from loguru import logger

# revision identifiers, used by Alembic.
revision: str = 'eb8dcc66f0c6'
down_revision: Union[str, Sequence[str], None] = '88717536aecf'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """
    Applies the correction:
    1. Removes the incorrect server-side default value.
    2. Updates existing rows with a random interval.
    """
    logger.info("Correcting 'full_refresh_interval' column...")

    # --- Step 1: Remove the server_default from the column ---
    logger.info("Removing server_default from securities.full_refresh_interval...")
    op.alter_column(
        'securities',
        'full_refresh_interval',
        server_default=None
    )
    logger.info("Server_default removed.")
    # --- Step 2: Randomize the values for existing rows ---
    # This part is crucial for distributing the load.
    # We need to execute a raw SQL statement for database-native randomness.

    # For PostgreSQL:
    logger.info("Randomizing existing values for securities.full_refresh_interval (PostgreSQL)...")
    op.execute("""
        UPDATE securities
        SET full_refresh_interval = floor(random() * (40 - 25 + 1) + 25)::integer;
    """)
    # For MySQL:
    # op.execute("""
    #     UPDATE securities
    #     SET full_refresh_interval = floor(rand() * (40 - 25 + 1) + 25);
    # """)
    # For SQLite (less common for production, but for completeness):
    # This is more complex as SQLite's random() returns a large integer.
    # op.execute("""
    #     UPDATE securities
    #     SET full_refresh_interval = (abs(random()) % (40 - 25 + 1)) + 25;
    # """)

    logger.info("Correction applied successfully.")


def downgrade() -> None:
    """
    Reverts the changes, restoring the incorrect state.
    """
    logger.info("Reverting correction for 'full_refresh_interval' column...")
    # --- Step 1: Restore the server_default ---
    logger.info("Restoring server_default='30' to securities.full_refresh_interval...")
    op.alter_column(
        'securities',
        'full_refresh_interval',
        server_default=sa.text('30')
    )

    # --- Step 2 (Optional but good practice): Reset values to 30 ---
    logger.info("Resetting all existing values to 30...")
    op.execute("UPDATE securities SET full_refresh_interval = 30;")
    logger.info("Downgrade complete. The column is back to its incorrect state.")