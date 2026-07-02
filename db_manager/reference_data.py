"""身份映射、symbol 历史、SEC filing/XBRL 事实与新闻的写入。"""
from sqlalchemy import func, text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from data_models.models import (
    FxRate,
    InsiderTransaction,
    InstitutionalHolding,
    NewsArticle,
    NewsArticleInsight,
    OpenFigiCusipLookup,
    RiskFreeRate,
    SecFiling,
    SecFundamentalFact,
    SecurityIdentifier,
    SecuritySymbolHistory,
)

from .helpers import ACTION_SOURCE_MASSIVE, _clean_for_model, _dedupe_rows_by_key, _group_rows_by_key_set


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

        行可携带 start_date 作为 PIT 边界（如 SEC_FTD 行的观测期起始日）；存在性
        判定刻意不含 start_date——同一身份映射只保留首次插入的行（含其 start_date），
        后续运行即便算出不同的观测期也不新增、不改写（只插不改）。

        不能走 ON CONFLICT：唯一约束含 start_date，快照行的 start_date 为 NULL 时
        PG 默认 NULLS DISTINCT 导致冲突永不触发；非 NULL 时 5 元组冲突键也比这里的
        4 元组语义键更宽，都会造成重复插入。
        """
        rows = [_clean_for_model(SecurityIdentifier, row) for row in rows_data]
        rows = [
            row for row in rows
            if row.get("security_id") and row.get("id_type") and row.get("id_value") and row.get("source")
        ]
        if not rows:
            return 0

        rows = _dedupe_rows_by_key(rows, ["security_id", "id_type", "id_value", "source"])

        with self.engine.connect() as conn:
            # 唯一约束含 start_date(NULL) 无法保护“当前快照”身份行；用事务级 advisory
            # lock 串行化 check-then-insert，避免并发回填重复写入同一身份映射。
            conn.execute(text("SELECT pg_advisory_xact_lock(hashtext(:lock_key))"), {"lock_key": "security-identifiers:insert-missing"})
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

    def upsert_insider_transactions(self, rows_data: list[dict]) -> int:
        """写 Form 3/4/5 明细行。同一 accession 重新解析时整批 hash 一致，
        冲突更新除身份键外的全部提供字段（XML 不可变，但解析器口径可能升级）。"""
        rows = [_clean_for_model(InsiderTransaction, row) for row in rows_data]
        rows = [
            row for row in rows
            if row.get("source") and row.get("accession_number") and row.get("source_row_hash")
        ]
        if not rows:
            return 0

        deduped: dict[tuple, dict] = {}
        for row in rows:
            deduped.setdefault((row["source"], row["accession_number"], row["source_row_hash"]), row)
        rows = list(deduped.values())

        written = 0
        with self.engine.connect() as conn:
            self._lock_model_sequence_sync(conn, InsiderTransaction)
            self._sync_model_id_sequence(conn, InsiderTransaction)
            for group in _group_rows_by_key_set(rows):
                stmt = pg_insert(InsiderTransaction).values(group)
                update_keys = set(group[0].keys())
                update_columns = {
                    key: getattr(stmt.excluded, key)
                    for key in update_keys
                    if key not in {"id", "source", "accession_number", "source_row_hash", "created_at"}
                }
                update_columns["updated_at"] = func.now()
                stmt = stmt.on_conflict_do_update(
                    index_elements=["source", "accession_number", "source_row_hash"],
                    set_=update_columns,
                )
                result = conn.execute(stmt)
                written += result.rowcount
            conn.commit()
        return written

    def upsert_institutional_holdings(self, rows_data: list[dict]) -> int:
        """写 13-F 持仓明细行。security_id 允许为空（CUSIP 映射后补）；
        冲突更新时 security_id 用 COALESCE 保护，不让未映射批次清掉已映射值。"""
        rows = [_clean_for_model(InstitutionalHolding, row) for row in rows_data]
        rows = [
            row for row in rows
            if row.get("source") and row.get("accession_number")
            and row.get("source_row_hash") and row.get("filer_cik")
        ]
        if not rows:
            return 0

        deduped: dict[tuple, dict] = {}
        for row in rows:
            deduped.setdefault((row["source"], row["accession_number"], row["source_row_hash"]), row)
        rows = list(deduped.values())

        written = 0
        with self.engine.connect() as conn:
            self._lock_model_sequence_sync(conn, InstitutionalHolding)
            self._sync_model_id_sequence(conn, InstitutionalHolding)
            for group in _group_rows_by_key_set(rows):
                for start in range(0, len(group), 2000):
                    chunk = group[start:start + 2000]
                    stmt = pg_insert(InstitutionalHolding).values(chunk)
                    update_keys = set(chunk[0].keys())
                    update_columns = {
                        key: getattr(stmt.excluded, key)
                        for key in update_keys
                        if key not in {
                            "id", "source", "accession_number", "source_row_hash",
                            "created_at", "security_id",
                        }
                    }
                    if "security_id" in update_keys:
                        update_columns["security_id"] = func.coalesce(
                            stmt.excluded.security_id,
                            InstitutionalHolding.security_id,
                        )
                    update_columns["updated_at"] = func.now()
                    stmt = stmt.on_conflict_do_update(
                        index_elements=["source", "accession_number", "source_row_hash"],
                        set_=update_columns,
                    )
                    result = conn.execute(stmt)
                    written += result.rowcount
            conn.commit()
        return written

    def map_unlinked_holdings_to_securities(self) -> int:
        """用 security_identifiers 的 CUSIP 映射回填 institutional_holdings.security_id。

        只回填 NULL 行；一个 CUSIP 对应多个 security 时视为歧义跳过（HAVING 保护），
        不覆盖任何已有关联。返回回填行数。"""
        from sqlalchemy import text

        sql = text(
            """
            UPDATE institutional_holdings h
            SET security_id = m.security_id, updated_at = now()
            FROM (
                SELECT id_value, min(security_id) AS security_id
                FROM security_identifiers
                WHERE id_type = 'CUSIP'
                GROUP BY id_value
                HAVING count(DISTINCT security_id) = 1
            ) m
            WHERE h.security_id IS NULL
              AND upper(h.cusip) = m.id_value
            """
        )
        with self.engine.connect() as conn:
            result = conn.execute(sql)
            conn.commit()
            return result.rowcount

    def upsert_openfigi_lookups(self, rows: list[dict]) -> int:
        """写 OpenFIGI CUSIP 查询缓存（含 NOT_FOUND/AMBIGUOUS 负缓存）。

        每行是该 CUSIP 最近一次查询的完整快照：冲突时全列覆盖并显式刷新
        queried_at=now()（server_default 只管首插，重查负缓存必须推进 TTL 时钟）。
        缺失字段按 None 归一化后参与覆盖——状态迁移（如 MATCHED -> NOT_FOUND）
        不得残留旧 figi 字段。"""
        data_columns = (
            "status", "composite_figi", "share_class_figi", "ticker",
            "name", "security_type", "market_sector", "exch_code",
        )
        cleaned = [_clean_for_model(OpenFigiCusipLookup, row) for row in rows]
        cleaned = [
            row for row in cleaned
            # cusip 是 String(9) 主键：超长值让整条语句报错，防御性剔除
            if row.get("cusip") and len(row["cusip"]) <= 9 and row.get("status")
        ]
        if not cleaned:
            return 0

        cleaned = _dedupe_rows_by_key(cleaned, ["cusip"])
        # 全列归一化（缺失补 None），键集恒定，无需 _group_rows_by_key_set
        cleaned = [
            {"cusip": row["cusip"], **{column: row.get(column) for column in data_columns}}
            for row in cleaned
        ]

        written = 0
        with self.engine.connect() as conn:
            for start in range(0, len(cleaned), 5000):
                chunk = cleaned[start:start + 5000]
                stmt = pg_insert(OpenFigiCusipLookup).values(chunk)
                update_columns = {
                    column: getattr(stmt.excluded, column) for column in data_columns
                }
                update_columns["queried_at"] = func.now()
                stmt = stmt.on_conflict_do_update(
                    index_elements=["cusip"],
                    set_=update_columns,
                )
                result = conn.execute(stmt)
                written += result.rowcount
            conn.commit()
        return written

    def upsert_fx_rates(self, rows_data: list[dict]) -> int:
        """写 ECB 参考汇率。复合主键无序列；冲突时刷新 rate（来源重发布修正）。"""
        rows = [_clean_for_model(FxRate, row) for row in rows_data]
        rows = [
            row for row in rows
            if row.get("rate_date") and row.get("base_currency")
            and row.get("quote_currency") and row.get("source") and row.get("rate") is not None
        ]
        if not rows:
            return 0

        written = 0
        with self.engine.connect() as conn:
            for start in range(0, len(rows), 5000):
                chunk = rows[start:start + 5000]
                stmt = pg_insert(FxRate).values(chunk)
                stmt = stmt.on_conflict_do_update(
                    index_elements=["rate_date", "base_currency", "quote_currency", "source"],
                    set_={"rate": stmt.excluded.rate, "updated_at": func.now()},
                )
                result = conn.execute(stmt)
                written += result.rowcount
            conn.commit()
        return written

    def upsert_risk_free_rates(self, rows_data: list[dict]) -> int:
        """写 FRED risk-free reference rates。冲突时刷新 rate_pct 与 fetched_at。"""
        rows = [_clean_for_model(RiskFreeRate, row) for row in rows_data]
        rows = [
            row for row in rows
            if row.get("date") and row.get("series_id") and row.get("rate_pct") is not None
        ]
        if not rows:
            return 0

        written = 0
        with self.engine.connect() as conn:
            for start in range(0, len(rows), 5000):
                chunk = rows[start:start + 5000]
                stmt = pg_insert(RiskFreeRate).values(chunk)
                stmt = stmt.on_conflict_do_update(
                    index_elements=["date", "series_id"],
                    set_={"rate_pct": stmt.excluded.rate_pct, "fetched_at": func.now()},
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
