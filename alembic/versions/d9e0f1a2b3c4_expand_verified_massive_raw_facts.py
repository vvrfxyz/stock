"""Expand verified Massive raw facts

Revision ID: d9e0f1a2b3c4
Revises: c8d9e10f1122
Create Date: 2026-05-14 13:20:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: str = 'd9e0f1a2b3c4'
down_revision: Union[str, Sequence[str], None] = 'c8d9e10f1122'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _add_column_if_missing(table_name: str, column: sa.Column) -> None:
    conn = op.get_bind()
    exists = conn.execute(
        sa.text(
            """
            SELECT 1
            FROM information_schema.columns
            WHERE table_name = :table_name
              AND column_name = :column_name
            """
        ),
        {"table_name": table_name, "column_name": column.name},
    ).first()
    if not exists:
        op.add_column(table_name, column)


def _create_index_if_missing(index_name: str, table_name: str, columns: list[str]) -> None:
    conn = op.get_bind()
    exists = conn.execute(
        sa.text("SELECT 1 FROM pg_indexes WHERE tablename = :table_name AND indexname = :index_name"),
        {"table_name": table_name, "index_name": index_name},
    ).first()
    if not exists:
        op.create_index(index_name, table_name, columns, unique=False)


def _create_unique_constraint_if_missing(constraint_name: str, table_name: str, columns: list[str]) -> None:
    conn = op.get_bind()
    exists = conn.execute(
        sa.text("SELECT 1 FROM pg_constraint WHERE conname = :constraint_name"),
        {"constraint_name": constraint_name},
    ).first()
    if not exists:
        op.create_unique_constraint(constraint_name, table_name, columns)


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


def upgrade() -> None:
    _add_column_if_missing('securities', sa.Column('currency_symbol', sa.String(length=10), nullable=True))
    _add_column_if_missing('securities', sa.Column('base_currency_name', sa.String(length=50), nullable=True))
    _add_column_if_missing('securities', sa.Column('base_currency_symbol', sa.String(length=10), nullable=True))
    _add_column_if_missing('securities', sa.Column('vendor_market', sa.String(length=30), nullable=True))
    _add_column_if_missing('securities', sa.Column('locale', sa.String(length=10), nullable=True))
    _add_column_if_missing('securities', sa.Column('ticker_root', sa.String(length=30), nullable=True))
    _add_column_if_missing('securities', sa.Column('ticker_suffix', sa.String(length=30), nullable=True))
    _add_column_if_missing('securities', sa.Column('round_lot', sa.Integer(), nullable=True))
    _add_column_if_missing('securities', sa.Column('share_class_shares_outstanding', sa.BigInteger(), nullable=True))
    _add_column_if_missing('securities', sa.Column('weighted_shares_outstanding', sa.BigInteger(), nullable=True))
    _add_column_if_missing('securities', sa.Column('vendor_last_updated_at', sa.TIMESTAMP(timezone=True), nullable=True))
    _add_column_if_missing('securities', sa.Column('events_last_updated_at', sa.TIMESTAMP(timezone=True), nullable=True))
    _add_column_if_missing('securities', sa.Column('shares_last_updated_at', sa.TIMESTAMP(timezone=True), nullable=True))
    _add_column_if_missing('securities', sa.Column('short_data_last_updated_at', sa.TIMESTAMP(timezone=True), nullable=True))
    _add_column_if_missing('securities', sa.Column('news_last_updated_at', sa.TIMESTAMP(timezone=True), nullable=True))
    _create_index_if_missing('ix_securities_vendor_market', 'securities', ['vendor_market'])
    _create_index_if_missing('ix_securities_locale', 'securities', ['locale'])
    _create_index_if_missing('ix_securities_ticker_root', 'securities', ['ticker_root'])

    _add_column_if_missing('security_symbol_history', sa.Column('source_event_id', sa.String(length=128), nullable=True))
    _add_column_if_missing('security_symbol_history', sa.Column('event_type', sa.String(length=30), nullable=True))
    _create_unique_constraint_if_missing(
        '_security_symbol_history_source_uc',
        'security_symbol_history',
        ['security_id', 'symbol', 'source', 'start_date'],
    )

    _add_column_if_missing('corporate_actions', sa.Column('declaration_date', sa.Date(), nullable=True))
    _add_column_if_missing('corporate_actions', sa.Column('record_date', sa.Date(), nullable=True))
    _add_column_if_missing('corporate_actions', sa.Column('frequency', sa.Integer(), nullable=True))
    _add_column_if_missing('corporate_actions', sa.Column('distribution_type', sa.String(length=30), nullable=True))
    _add_column_if_missing('corporate_actions', sa.Column('adjustment_type', sa.String(length=30), nullable=True))

    _add_column_if_missing('daily_prices', sa.Column('otc', sa.Boolean(), nullable=True))
    _add_column_if_missing('historical_shares', sa.Column('free_float_percent', sa.Numeric(10, 4), nullable=True))

    if not _table_exists('historical_floats'):
        op.create_table(
            'historical_floats',
            sa.Column('id', sa.BigInteger(), nullable=False),
            sa.Column('security_id', sa.BigInteger(), nullable=False),
            sa.Column('effective_date', sa.Date(), nullable=False),
            sa.Column('free_float', sa.BigInteger(), nullable=False),
            sa.Column('free_float_percent', sa.Numeric(10, 4), nullable=True),
            sa.Column('source', sa.String(length=30), nullable=False),
            sa.Column('created_at', sa.TIMESTAMP(timezone=True), server_default=sa.text('now()'), nullable=False),
            sa.ForeignKeyConstraint(['security_id'], ['securities.id']),
            sa.PrimaryKeyConstraint('id'),
            sa.UniqueConstraint('security_id', 'effective_date', 'source', name='_historical_floats_effective_source_uc'),
        )
    _create_index_if_missing('ix_historical_floats_security_id', 'historical_floats', ['security_id'])
    _create_index_if_missing('ix_historical_floats_effective_date', 'historical_floats', ['effective_date'])
    _create_index_if_missing('ix_historical_floats_source', 'historical_floats', ['source'])

    if not _table_exists('short_interests'):
        op.create_table(
            'short_interests',
            sa.Column('id', sa.BigInteger(), nullable=False),
            sa.Column('security_id', sa.BigInteger(), nullable=False),
            sa.Column('ticker', sa.String(length=30), nullable=False),
            sa.Column('settlement_date', sa.Date(), nullable=False),
            sa.Column('short_interest', sa.BigInteger(), nullable=False),
            sa.Column('avg_daily_volume', sa.BigInteger(), nullable=True),
            sa.Column('days_to_cover', sa.Numeric(20, 6), nullable=True),
            sa.Column('source', sa.String(length=30), nullable=False),
            sa.Column('created_at', sa.TIMESTAMP(timezone=True), server_default=sa.text('now()'), nullable=False),
            sa.ForeignKeyConstraint(['security_id'], ['securities.id']),
            sa.PrimaryKeyConstraint('id'),
            sa.UniqueConstraint('security_id', 'settlement_date', 'source', name='_short_interest_security_date_source_uc'),
        )
    _create_index_if_missing('ix_short_interests_security_id', 'short_interests', ['security_id'])
    _create_index_if_missing('ix_short_interests_ticker', 'short_interests', ['ticker'])
    _create_index_if_missing('ix_short_interests_settlement_date', 'short_interests', ['settlement_date'])
    _create_index_if_missing('ix_short_interests_source', 'short_interests', ['source'])

    if not _table_exists('short_volumes'):
        op.create_table(
            'short_volumes',
            sa.Column('id', sa.BigInteger(), nullable=False),
            sa.Column('security_id', sa.BigInteger(), nullable=False),
            sa.Column('ticker', sa.String(length=30), nullable=False),
            sa.Column('date', sa.Date(), nullable=False),
            sa.Column('short_volume', sa.BigInteger(), nullable=False),
            sa.Column('total_volume', sa.BigInteger(), nullable=True),
            sa.Column('short_volume_ratio', sa.Numeric(20, 6), nullable=True),
            sa.Column('exempt_volume', sa.BigInteger(), nullable=True),
            sa.Column('non_exempt_volume', sa.BigInteger(), nullable=True),
            sa.Column('adf_short_volume', sa.BigInteger(), nullable=True),
            sa.Column('adf_short_volume_exempt', sa.BigInteger(), nullable=True),
            sa.Column('nasdaq_carteret_short_volume', sa.BigInteger(), nullable=True),
            sa.Column('nasdaq_carteret_short_volume_exempt', sa.BigInteger(), nullable=True),
            sa.Column('nasdaq_chicago_short_volume', sa.BigInteger(), nullable=True),
            sa.Column('nasdaq_chicago_short_volume_exempt', sa.BigInteger(), nullable=True),
            sa.Column('nyse_short_volume', sa.BigInteger(), nullable=True),
            sa.Column('nyse_short_volume_exempt', sa.BigInteger(), nullable=True),
            sa.Column('source', sa.String(length=30), nullable=False),
            sa.Column('created_at', sa.TIMESTAMP(timezone=True), server_default=sa.text('now()'), nullable=False),
            sa.ForeignKeyConstraint(['security_id'], ['securities.id']),
            sa.PrimaryKeyConstraint('id'),
            sa.UniqueConstraint('security_id', 'date', 'source', name='_short_volume_security_date_source_uc'),
        )
    _create_index_if_missing('ix_short_volumes_security_id', 'short_volumes', ['security_id'])
    _create_index_if_missing('ix_short_volumes_ticker', 'short_volumes', ['ticker'])
    _create_index_if_missing('ix_short_volumes_date', 'short_volumes', ['date'])
    _create_index_if_missing('ix_short_volumes_source', 'short_volumes', ['source'])

    if not _table_exists('news_articles'):
        op.create_table(
            'news_articles',
            sa.Column('id', sa.BigInteger(), nullable=False),
            sa.Column('source', sa.String(length=30), nullable=False),
            sa.Column('source_article_id', sa.String(length=128), nullable=False),
            sa.Column('published_utc', sa.TIMESTAMP(timezone=True), nullable=False),
            sa.Column('title', sa.Text(), nullable=True),
            sa.Column('author', sa.String(length=255), nullable=True),
            sa.Column('description', sa.Text(), nullable=True),
            sa.Column('article_url', sa.Text(), nullable=True),
            sa.Column('amp_url', sa.Text(), nullable=True),
            sa.Column('image_url', sa.Text(), nullable=True),
            sa.Column('publisher_name', sa.String(length=255), nullable=True),
            sa.Column('publisher_homepage_url', sa.Text(), nullable=True),
            sa.Column('publisher_logo_url', sa.Text(), nullable=True),
            sa.Column('publisher_favicon_url', sa.Text(), nullable=True),
            sa.Column('tickers', postgresql.ARRAY(sa.String(length=30)), nullable=True),
            sa.Column('keywords', postgresql.ARRAY(sa.String(length=100)), nullable=True),
            sa.Column('created_at', sa.TIMESTAMP(timezone=True), server_default=sa.text('now()'), nullable=False),
            sa.Column('updated_at', sa.TIMESTAMP(timezone=True), server_default=sa.text('now()'), nullable=False),
            sa.PrimaryKeyConstraint('id'),
            sa.UniqueConstraint('source_article_id', name='uq_news_articles_source_article_id'),
        )
    _create_index_if_missing('ix_news_articles_source', 'news_articles', ['source'])
    _create_index_if_missing('ix_news_articles_source_article_id', 'news_articles', ['source_article_id'])
    _create_index_if_missing('ix_news_articles_published_utc', 'news_articles', ['published_utc'])
    _create_index_if_missing('ix_news_articles_publisher_name', 'news_articles', ['publisher_name'])

    if not _table_exists('news_article_insights'):
        op.create_table(
            'news_article_insights',
            sa.Column('id', sa.BigInteger(), nullable=False),
            sa.Column('source_article_id', sa.String(length=128), nullable=False),
            sa.Column('security_id', sa.BigInteger(), nullable=True),
            sa.Column('ticker', sa.String(length=30), nullable=False),
            sa.Column('sentiment', sa.String(length=30), nullable=True),
            sa.Column('sentiment_reasoning', sa.Text(), nullable=True),
            sa.Column('created_at', sa.TIMESTAMP(timezone=True), server_default=sa.text('now()'), nullable=False),
            sa.ForeignKeyConstraint(['security_id'], ['securities.id']),
            sa.PrimaryKeyConstraint('id'),
            sa.UniqueConstraint('source_article_id', 'ticker', name='_news_article_insight_article_ticker_uc'),
        )
    _create_index_if_missing('ix_news_article_insights_source_article_id', 'news_article_insights', ['source_article_id'])
    _create_index_if_missing('ix_news_article_insights_security_id', 'news_article_insights', ['security_id'])
    _create_index_if_missing('ix_news_article_insights_ticker', 'news_article_insights', ['ticker'])
    _create_index_if_missing('ix_news_article_insights_sentiment', 'news_article_insights', ['sentiment'])


def downgrade() -> None:
    raise NotImplementedError("Verified Massive raw facts migration is intentionally not downgradable.")
