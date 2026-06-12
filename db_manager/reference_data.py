"""身份映射、symbol 历史、SEC filing/XBRL 事实与新闻的写入。"""
from sqlalchemy import func
from sqlalchemy.dialects.postgresql import insert as pg_insert

from data_models.models import (
    NewsArticle,
    NewsArticleInsight,
    SecFiling,
    SecFundamentalFact,
    SecurityIdentifier,
    SecuritySymbolHistory,
)

from .helpers import ACTION_SOURCE_MASSIVE, _clean_for_model, _group_rows_by_key_set


class ReferenceDataMixin:
    def upsert_symbol_history(self, rows_data: list[dict]) -> int:
        rows = [_clean_for_model(SecuritySymbolHistory, row) for row in rows_data]
        rows = [row for row in rows if row.get('security_id') and row.get('symbol') and row.get('source')]
        if not rows:
            return 0

        stmt = pg_insert(SecuritySymbolHistory).values(rows)
        update_columns = {
            'exchange': stmt.excluded.exchange,
            'source_event_id': stmt.excluded.source_event_id,
            'event_type': stmt.excluded.event_type,
            'end_date': stmt.excluded.end_date,
        }
        stmt = stmt.on_conflict_do_update(
            index_elements=['security_id', 'symbol', 'source', 'start_date'],
            set_=update_columns,
        )
        with self.engine.connect() as conn:
            self._lock_model_sequence_sync(conn, SecuritySymbolHistory)
            self._sync_model_id_sequence(conn, SecuritySymbolHistory)
            result = conn.execute(stmt)
            conn.commit()
            return result.rowcount

    def insert_missing_security_identifiers(self, rows_data: list[dict]) -> int:
        """只插入库内不存在的 (security_id, id_type, id_value, source) 身份行。

        不能走 ON CONFLICT：唯一约束含 start_date，而身份快照行的 start_date 为 NULL，
        PG 默认 NULLS DISTINCT 导致冲突永不触发、每次运行都会重复插入。
        """
        rows = [_clean_for_model(SecurityIdentifier, row) for row in rows_data]
        rows = [
            row for row in rows
            if row.get("security_id") and row.get("id_type") and row.get("id_value") and row.get("source")
        ]
        if not rows:
            return 0

        with self.engine.connect() as conn:
            existing = {
                (r.security_id, r.id_type, r.id_value, r.source)
                for r in conn.execute(
                    SecurityIdentifier.__table__.select().with_only_columns(
                        SecurityIdentifier.security_id,
                        SecurityIdentifier.id_type,
                        SecurityIdentifier.id_value,
                        SecurityIdentifier.source,
                    ).where(SecurityIdentifier.id_type.in_({row["id_type"] for row in rows}))
                )
            }
            fresh = [
                row for row in rows
                if (row["security_id"], row["id_type"], row["id_value"], row["source"]) not in existing
            ]
            if not fresh:
                return 0
            self._lock_model_sequence_sync(conn, SecurityIdentifier)
            self._sync_model_id_sequence(conn, SecurityIdentifier)
            for group in _group_rows_by_key_set(fresh):
                conn.execute(pg_insert(SecurityIdentifier).values(group))
            conn.commit()
            return len(fresh)

    def upsert_sec_filings(self, rows_data: list[dict]) -> int:
        rows = [_clean_for_model(SecFiling, row) for row in rows_data]
        rows = [
            row for row in rows
            if row.get("source") and row.get("accession_number") and row.get("form_type") and row.get("filing_date")
        ]
        if not rows:
            return 0

        # 同一 accession 在一批内可能出现两次（如双重上市类多 security 共用 CIK），保留首条。
        deduped: dict[tuple, dict] = {}
        for row in rows:
            deduped.setdefault((row["source"], row["accession_number"]), row)
        rows = list(deduped.values())

        written = 0
        with self.engine.connect() as conn:
            self._lock_model_sequence_sync(conn, SecFiling)
            self._sync_model_id_sequence(conn, SecFiling)
            for group in _group_rows_by_key_set(rows):
                stmt = pg_insert(SecFiling).values(group)
                update_keys = set(group[0].keys())
                update_columns = {
                    key: getattr(stmt.excluded, key)
                    for key in update_keys
                    if key not in {"id", "source", "accession_number", "created_at", "available_at"}
                }
                update_columns["updated_at"] = func.now()
                stmt = stmt.on_conflict_do_update(
                    index_elements=["source", "accession_number"],
                    set_=update_columns,
                )
                result = conn.execute(stmt)
                written += result.rowcount
            conn.commit()
        return written

    def upsert_sec_fundamental_facts(self, rows_data: list[dict]) -> int:
        """写 curated XBRL 事实。值本身不可变（同一 accession 的申报值不会改），
        冲突时只刷新 security_id 关联与 frame/fiscal 标签。批内按唯一键去重。"""
        rows = [_clean_for_model(SecFundamentalFact, row) for row in rows_data]
        rows = [
            row for row in rows
            if row.get("cik") and row.get("taxonomy") and row.get("concept") and row.get("unit")
            and row.get("period_start") and row.get("period_end")
            and row.get("accession_number") and row.get("filed_date") and row.get("value") is not None
        ]
        if not rows:
            return 0

        deduped: dict[tuple, dict] = {}
        for row in rows:
            key = (
                row["cik"], row["taxonomy"], row["concept"], row["unit"],
                row["period_start"], row["period_end"], row["accession_number"],
            )
            deduped[key] = row
        rows = list(deduped.values())

        written = 0
        with self.engine.connect() as conn:
            self._lock_model_sequence_sync(conn, SecFundamentalFact)
            self._sync_model_id_sequence(conn, SecFundamentalFact)
            for group in _group_rows_by_key_set(rows):
                for start in range(0, len(group), 5000):
                    chunk = group[start:start + 5000]
                    stmt = pg_insert(SecFundamentalFact).values(chunk)
                    stmt = stmt.on_conflict_do_update(
                        index_elements=[
                            'cik', 'taxonomy', 'concept', 'unit',
                            'period_start', 'period_end', 'accession_number',
                        ],
                        set_={
                            'security_id': stmt.excluded.security_id,
                            'fiscal_year': stmt.excluded.fiscal_year,
                            'fiscal_period': stmt.excluded.fiscal_period,
                            'frame': stmt.excluded.frame,
                            'updated_at': func.now(),
                        },
                    )
                    result = conn.execute(stmt)
                    written += result.rowcount
            conn.commit()
        return written

    def upsert_news_articles(self, articles: list[dict], symbol_to_id: dict[str, int] | None = None) -> tuple[int, int]:
        if not articles:
            return 0, 0

        article_rows = []
        insight_rows = []
        symbol_to_id = symbol_to_id or {}
        for article in articles:
            article_row = _clean_for_model(NewsArticle, article)
            article_row.setdefault('source', ACTION_SOURCE_MASSIVE)
            if not article_row.get('source_article_id') or not article_row.get('published_utc'):
                continue
            article_rows.append(article_row)
            source_article_id = article_row['source_article_id']
            for insight in article.get('insights') or []:
                ticker = (insight.get('ticker') or '').lower()
                if not ticker:
                    continue
                insight_rows.append(
                    {
                        'source_article_id': source_article_id,
                        'security_id': symbol_to_id.get(ticker),
                        'ticker': ticker,
                        'sentiment': insight.get('sentiment'),
                        'sentiment_reasoning': insight.get('sentiment_reasoning'),
                    }
                )
        insight_rows = list(
            {
                (row['source_article_id'], row['ticker']): row
                for row in insight_rows
            }.values()
        )

        if not article_rows:
            return 0, 0

        article_stmt = pg_insert(NewsArticle).values(article_rows)
        update_keys = set().union(*(row.keys() for row in article_rows))
        article_update_columns = {
            key: getattr(article_stmt.excluded, key)
            for key in update_keys
            if key not in {'id', 'source_article_id', 'created_at'}
        }
        article_stmt = article_stmt.on_conflict_do_update(
            index_elements=['source_article_id'],
            set_=article_update_columns,
        )

        article_count = 0
        insight_count = 0
        with self.engine.connect() as conn:
            self._lock_model_sequence_sync(conn, NewsArticle)
            self._sync_model_id_sequence(conn, NewsArticle)
            article_result = conn.execute(article_stmt)
            article_count = article_result.rowcount or 0

            if insight_rows:
                self._lock_model_sequence_sync(conn, NewsArticleInsight)
                self._sync_model_id_sequence(conn, NewsArticleInsight)
                insight_stmt = pg_insert(NewsArticleInsight).values(insight_rows)
                insight_stmt = insight_stmt.on_conflict_do_update(
                    index_elements=['source_article_id', 'ticker'],
                    set_={
                        # ticker 不在本批 symbol 映射内时 security_id 为 None；
                        # 不能用 None 覆盖此前已解析出的 security_id。
                        'security_id': func.coalesce(
                            insight_stmt.excluded.security_id,
                            NewsArticleInsight.security_id,
                        ),
                        'sentiment': insight_stmt.excluded.sentiment,
                        'sentiment_reasoning': insight_stmt.excluded.sentiment_reasoning,
                    },
                )
                insight_result = conn.execute(insight_stmt)
                insight_count = insight_result.rowcount or 0

            conn.commit()
        return article_count, insight_count
