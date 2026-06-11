import os
import random
from contextlib import contextmanager
from datetime import date
from decimal import Decimal, InvalidOperation

from dotenv import load_dotenv
from loguru import logger
from sqlalchemy import create_engine, func, text, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session, sessionmaker

from data_models.models import (
    ComputedAdjustmentFactor,
    DailyPrice,
    HistoricalShare,
    HistoricalFloat,
    CorporateAction,
    NewsArticle,
    NewsArticleInsight,
    SecFiling,
    SecurityIdentifier,
    SecuritySymbolHistory,
    ShortInterest,
    ShortVolume,
    Security,
    VendorAdjustmentFactor,
)

load_dotenv()

ACTION_SOURCE_MASSIVE = "MASSIVE"


def _format_action_decimal(value) -> str:
    try:
        return f"{Decimal(str(value)).quantize(Decimal('1.0000000000')):f}"
    except (InvalidOperation, TypeError, ValueError):
        return str(value)


def _clean_for_model(model, row: dict) -> dict:
    valid_columns = set(model.__table__.columns.keys())
    return {key: value for key, value in row.items() if key in valid_columns}


def _normalize_batch_rows(model, rows: list[dict]) -> list[dict]:
    cleaned_rows = [_clean_for_model(model, row) for row in rows]
    if not cleaned_rows:
        return []

    all_keys = set().union(*(row.keys() for row in cleaned_rows))
    return [{key: row.get(key) for key in all_keys} for row in cleaned_rows]


def _group_rows_by_key_set(rows: list[dict]) -> list[list[dict]]:
    """
    多行 VALUES 插入要求所有 dict 键集一致，否则 SQLAlchemy 直接 CompileError。
    与 _normalize_batch_rows 的 None 填充不同，按键集分组能保留
    “冲突时只更新该行明确提供的字段”的语义，不会把缺失字段覆盖成 NULL。
    """
    groups: dict[frozenset, list[dict]] = {}
    for row in rows:
        groups.setdefault(frozenset(row.keys()), []).append(row)
    return list(groups.values())


def _build_upsert_statement(
    model,
    data_list: list[dict],
    index_elements: list[str],
    *,
    update_on_conflict: bool = False,
    protected_columns: set[str] | None = None,
):
    stmt = pg_insert(model).values(data_list)
    if not update_on_conflict:
        return stmt.on_conflict_do_nothing(index_elements=index_elements)

    protected = set(index_elements) | {"id", "created_at"} | (protected_columns or set())
    update_keys = set().union(*(row.keys() for row in data_list))
    update_columns = {
        key: getattr(stmt.excluded, key)
        for key in update_keys
        if key not in protected
    }
    if "updated_at" in model.__table__.columns:
        update_columns["updated_at"] = func.now()
    if not update_columns:
        return stmt.on_conflict_do_nothing(index_elements=index_elements)
    return stmt.on_conflict_do_update(index_elements=index_elements, set_=update_columns)


class DatabaseManager:
    def __init__(self, db_url: str = None):
        if db_url is None:
            db_url = os.getenv("DATABASE_URL")
        if not db_url:
            raise ValueError("数据库URL未找到。请在 .env 文件中设置 DATABASE_URL 或在初始化时提供。")
        self.engine = create_engine(db_url, pool_pre_ping=True)
        self._session_factory = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
        logger.info("数据库引擎创建成功。")

    def close(self):
        if self.engine:
            self.engine.dispose()
            logger.success("数据库引擎已成功关闭。")

    @contextmanager
    def get_session(self) -> Session:
        session = self._session_factory()
        try:
            yield session
        except Exception as e:
            logger.opt(exception=e).error(f"Session 上下文管理器捕获到异常，将执行回滚。原因: {e}")
            session.rollback()
            raise
        finally:
            session.close()

    def _sync_model_id_sequence(self, conn, model) -> None:
        """
        确保 PostgreSQL 自增序列不落后于现有主键数据。
        这在手工迁移/导库后很常见，否则后续 INSERT 会命中 duplicate key on *_pkey。
        """
        table = getattr(model, "__table__", None)
        if table is None or "id" not in table.columns:
            return

        table_name = table.name
        stmt = text(
            f"""
            WITH seq_name AS (
                SELECT pg_get_serial_sequence('{table_name}', 'id') AS name
            ),
            table_max AS (
                SELECT COALESCE(MAX(id), 0) AS max_id
                FROM {table_name}
            ),
            seq_state AS (
                SELECT COALESCE(last_value, 0) AS last_value
                FROM seq_name, pg_sequences
                WHERE schemaname || '.' || sequencename = seq_name.name
            )
            SELECT setval(
                (SELECT name FROM seq_name),
                GREATEST(
                    (SELECT max_id FROM table_max),
                    COALESCE((SELECT last_value FROM seq_state), 0),
                    1
                ),
                true
            )
            """
        )
        conn.execute(stmt)

    def _lock_model_sequence_sync(self, conn, model) -> None:
        table = getattr(model, "__table__", None)
        if table is None or "id" not in table.columns:
            return
        lock_stmt = text("SELECT pg_advisory_xact_lock(hashtext(:lock_key))")
        conn.execute(lock_stmt, {"lock_key": f"seq-sync:{table.name}"})

    def upsert_security_info(self, security_data: dict) -> None:
        """
        智能地更新或插入 Security 信息 (UPSERT)。
        - 如果记录不存在，则插入新记录。
        - 如果记录已存在，则根据传入的 `security_data` 字典更新字段。
        - **关键**: 字典中未包含的维护字段将保持不变，从而保护现有数据。
        - 更新操作通过主键 `id` 进行定位，确保精确性。
        """
        if 'id' not in security_data:
            raise ValueError("更新数据必须包含 'id' 字段以定位记录。")

        valid_columns = set(Security.__table__.columns.keys())
        unknown_keys = set(security_data.keys()) - valid_columns
        if unknown_keys:
            logger.warning(f"upsert_security_info 收到未知字段，将被忽略: {sorted(unknown_keys)}")
            for key in unknown_keys:
                security_data.pop(key, None)

        # Insert path must satisfy NOT NULL constraints.
        # Keep it stable across updates by excluding from ON CONFLICT updates.
        security_data.setdefault('full_refresh_interval', random.randint(25, 40))
        security_data.setdefault('current_symbol', security_data.get('symbol'))

        # 使用 SQLAlchemy 2.0 风格的 insert 语句
        stmt = pg_insert(Security).values(security_data)

        # 定义冲突时的更新策略：
        # 仅更新 security_data 中明确提供的字段，避免将未提供字段覆盖为 NULL/DEFAULT。
        protected_fields = {
            'id',
            'symbol',
            'price_data_latest_date',
            'full_data_last_updated_at',
            'actions_last_updated_at',
            'events_last_updated_at',
            'shares_last_updated_at',
            'short_data_last_updated_at',
            'news_last_updated_at',
            'full_refresh_interval',
        }
        update_columns = {
            key: getattr(stmt.excluded, key)
            for key in security_data.keys()
            if key not in protected_fields
        }

        # 2. 无论如何都要更新时间戳
        update_columns['info_last_updated_at'] = func.now()

        # 3. 构建完整的 on_conflict_do_update 语句
        #    当 'id' 冲突时，执行更新操作
        final_stmt = stmt.on_conflict_do_update(
            index_elements=['id'],  # 使用主键 'id' 进行冲突检测
            set_=update_columns
        )

        # 4. 执行
        with self.engine.connect() as conn:
            self._lock_model_sequence_sync(conn, Security)
            self._sync_model_id_sequence(conn, Security)
            conn.execute(final_stmt)
            conn.commit()

        logger.success(
            f"✅ 成功更新 Security (ID: {security_data['id']}, Symbol: {security_data.get('symbol', 'N/A')})"
        )

    def upsert_securities_by_symbol(self, securities_data: list[dict], touch_info_timestamp: bool = False) -> int:
        """
        基于 symbol 的批量 UPSERT，适合全市场 reference/universe 同步。
        默认不更新 info_last_updated_at，避免把“基础引用数据刷新”误判成“详情刷新”。
        """
        if not securities_data:
            return 0

        valid_columns = set(Security.__table__.columns.keys())
        cleaned_rows: list[dict] = []
        for row in securities_data:
            cleaned = {key: value for key, value in row.items() if key in valid_columns}
            if "symbol" not in cleaned:
                continue
            cleaned.setdefault("full_refresh_interval", random.randint(25, 40))
            cleaned.setdefault("current_symbol", cleaned.get("symbol"))
            cleaned_rows.append(cleaned)

        if not cleaned_rows:
            return 0

        protected_fields = {
            'id',
            'symbol',
            'price_data_latest_date',
            'full_data_last_updated_at',
            'actions_last_updated_at',
            'events_last_updated_at',
            'shares_last_updated_at',
            'short_data_last_updated_at',
            'news_last_updated_at',
            'full_refresh_interval',
            'info_last_updated_at',
        }

        total_rowcount = 0
        with self.engine.connect() as conn:
            self._lock_model_sequence_sync(conn, Security)
            self._sync_model_id_sequence(conn, Security)
            # 详情 payload 会剔除 None 字段，键集可能互不相同；多行 VALUES 必须按键集分组，
            # 否则 SQLAlchemy 抛 CompileError。分组同时保留“只更新提供字段”的语义。
            for group in _group_rows_by_key_set(cleaned_rows):
                stmt = pg_insert(Security).values(group)
                update_keys = set(group[0].keys())
                update_columns = {
                    key: getattr(stmt.excluded, key)
                    for key in update_keys
                    if key not in protected_fields
                }
                if touch_info_timestamp:
                    update_columns['info_last_updated_at'] = func.now()

                if not update_columns:
                    final_stmt = stmt.on_conflict_do_nothing(index_elements=['symbol'])
                else:
                    final_stmt = stmt.on_conflict_do_update(
                        index_elements=['symbol'],
                        set_=update_columns,
                    )
                result = conn.execute(final_stmt)
                total_rowcount += result.rowcount or 0
            conn.commit()
        return total_rowcount

    def _batch_upsert(
        self,
        model,
        data_list: list[dict],
        index_elements: list[str],
        *,
        update_on_conflict: bool = False,
        protected_columns: set[str] | None = None,
    ):
        """通用批量插入/忽略冲突的方法"""
        if not data_list:
            return 0

        stmt = _build_upsert_statement(
            model,
            data_list,
            index_elements,
            update_on_conflict=update_on_conflict,
            protected_columns=protected_columns,
        )

        with self.engine.connect() as conn:
            self._lock_model_sequence_sync(conn, model)
            self._sync_model_id_sequence(conn, model)
            result = conn.execute(stmt)
            conn.commit()
            return result.rowcount

    def upsert_dividends(self, security_id: int, dividends_data: list[dict]) -> int:
        """批量插入分红公司行动，如果已存在则忽略。"""
        if not dividends_data:
            return 0

        rows = []
        for item in dividends_data:
            ex_date = item.get('ex_dividend_date') or item.get('ex_date')
            cash_amount = item.get('cash_amount')
            currency = item.get('currency')
            if not ex_date or cash_amount is None or not currency:
                continue

            source = item.get('source') or ACTION_SOURCE_MASSIVE
            source_event_id = item.get('source_event_id')
            if not source_event_id:
                source_event_id = (
                    f"{source.lower()}-dividend:"
                    f"{security_id}:{ex_date}:{_format_action_decimal(cash_amount)}"
                )

            rows.append(
                {
                    'security_id': security_id,
                    'action_type': 'DIVIDEND',
                    'ex_date': ex_date,
                    'declaration_date': item.get('declaration_date'),
                    'record_date': item.get('record_date'),
                    'pay_date': item.get('pay_date'),
                    'cash_amount': cash_amount,
                    'currency': currency,
                    'frequency': item.get('frequency'),
                    'distribution_type': item.get('distribution_type'),
                    'source': source,
                    'source_event_id': source_event_id,
                }
            )

        rows_affected = self._batch_upsert(
            CorporateAction,
            rows,
            ['security_id', 'action_type', 'source', 'source_event_id'],
            update_on_conflict=True,
        )
        deleted_duplicates = self.cleanup_synthetic_corporate_action_duplicates(
            security_id,
            "DIVIDEND",
            source=ACTION_SOURCE_MASSIVE,
        )
        logger.debug(f"为 Security ID {security_id} 同步 {len(dividends_data)} 条分红记录。")
        return rows_affected + deleted_duplicates

    def upsert_vendor_adjustment_factors(self, rows_data: list[dict]) -> int:
        rows = [_clean_for_model(VendorAdjustmentFactor, row) for row in rows_data]
        rows = [
            row
            for row in rows
            if row.get('security_id')
            and row.get('date')
            and row.get('source')
            and row.get('factor_key')
            and row.get('factor_type')
            and row.get('adjustment_factor') is not None
        ]
        if not rows:
            return 0

        stmt = pg_insert(VendorAdjustmentFactor).values(rows)
        update_keys = set().union(*(row.keys() for row in rows))
        update_columns = {
            key: getattr(stmt.excluded, key)
            for key in update_keys
            if key not in {'id', 'security_id', 'source', 'factor_key', 'created_at'}
        }
        update_columns['updated_at'] = func.now()
        stmt = stmt.on_conflict_do_update(
            index_elements=['security_id', 'source', 'factor_key'],
            set_=update_columns,
        )
        with self.engine.connect() as conn:
            self._lock_model_sequence_sync(conn, VendorAdjustmentFactor)
            self._sync_model_id_sequence(conn, VendorAdjustmentFactor)
            result = conn.execute(stmt)
            conn.commit()
            return result.rowcount

    def replace_computed_adjustment_factors(
        self,
        security_id: int,
        methodology_version: str,
        rows_data: list[dict],
    ) -> int:
        rows = [_clean_for_model(ComputedAdjustmentFactor, row) for row in rows_data]
        rows = [
            row
            for row in rows
            if row.get('security_id')
            and row.get('date')
            and row.get('methodology_version')
            and row.get('factor_key')
            and row.get('factor_type')
            and row.get('cumulative_factor') is not None
            and row.get('event_hash')
        ]

        with self.engine.connect() as conn:
            conn.execute(
                text(
                    """
                    DELETE FROM computed_adjustment_factors
                    WHERE security_id = :security_id
                      AND methodology_version = :methodology_version
                    """
                ),
                {
                    "security_id": security_id,
                    "methodology_version": methodology_version,
                },
            )
            if not rows:
                conn.commit()
                return 0

            self._lock_model_sequence_sync(conn, ComputedAdjustmentFactor)
            self._sync_model_id_sequence(conn, ComputedAdjustmentFactor)
            stmt = pg_insert(ComputedAdjustmentFactor).values(rows)
            result = conn.execute(stmt)
            conn.commit()
            return result.rowcount

    def upsert_splits(self, security_id: int, splits_data: list[dict]) -> int:
        """批量插入拆股公司行动，如果已存在则忽略。"""
        if not splits_data:
            return 0

        rows = []
        for item in splits_data:
            execution_date = item.get('execution_date')
            split_from = item.get('split_from')
            split_to = item.get('split_to')
            if not execution_date or split_from is None or split_to is None:
                continue

            source = item.get('source') or ACTION_SOURCE_MASSIVE
            source_event_id = item.get('source_event_id')
            if not source_event_id:
                source_event_id = (
                    f"{source.lower()}-split:"
                    f"{security_id}:{execution_date}:"
                    f"{_format_action_decimal(split_from)}:{_format_action_decimal(split_to)}"
                )

            rows.append(
                {
                    'security_id': security_id,
                    'action_type': 'SPLIT',
                    'ex_date': execution_date,
                    'split_from': split_from,
                    'split_to': split_to,
                    'adjustment_type': item.get('adjustment_type'),
                    'source': source,
                    'source_event_id': source_event_id,
                }
            )

        rows_affected = self._batch_upsert(
            CorporateAction,
            rows,
            ['security_id', 'action_type', 'source', 'source_event_id'],
            update_on_conflict=True,
        )
        deleted_duplicates = self.cleanup_synthetic_corporate_action_duplicates(
            security_id,
            "SPLIT",
            source=ACTION_SOURCE_MASSIVE,
        )
        logger.debug(f"为 Security ID {security_id} 同步 {len(splits_data)} 条拆股记录。")
        return rows_affected + deleted_duplicates

    def cleanup_synthetic_corporate_action_duplicates(
        self,
        security_id: int,
        action_type: str,
        *,
        source: str = ACTION_SOURCE_MASSIVE,
    ) -> int:
        action_type = (action_type or "").upper()
        if action_type not in {"DIVIDEND", "SPLIT"}:
            return 0

        synthetic_prefix = f"{source.lower()}-{'dividend' if action_type == 'DIVIDEND' else 'split'}:%"
        if action_type == "DIVIDEND":
            matching_predicate = """
                synthetic.cash_amount IS NOT DISTINCT FROM real.cash_amount
                AND synthetic.currency IS NOT DISTINCT FROM real.currency
            """
        else:
            matching_predicate = """
                synthetic.split_from IS NOT DISTINCT FROM real.split_from
                AND synthetic.split_to IS NOT DISTINCT FROM real.split_to
            """

        stmt = text(
            f"""
            DELETE FROM corporate_actions AS synthetic
            USING corporate_actions AS real
            WHERE synthetic.security_id = :security_id
              AND real.security_id = synthetic.security_id
              AND synthetic.id <> real.id
              AND synthetic.action_type = :action_type
              AND real.action_type = synthetic.action_type
              AND synthetic.ex_date = real.ex_date
              AND upper(synthetic.source) = upper(:source)
              AND upper(real.source) = upper(synthetic.source)
              AND synthetic.source_event_id LIKE :synthetic_prefix
              AND real.source_event_id NOT LIKE :synthetic_prefix
              AND {matching_predicate}
            """
        )
        with self.engine.connect() as conn:
            result = conn.execute(
                stmt,
                {
                    "security_id": security_id,
                    "action_type": action_type,
                    "source": source,
                    "synthetic_prefix": synthetic_prefix,
                },
            )
            conn.commit()
            return result.rowcount or 0

    def update_security_timestamp(self, security_id: int, field_name: str) -> None:
        """更新 Security 表中指定的 TIMESTAMP 字段为当前时间。"""
        self.update_security_timestamps([security_id], field_name)

    def update_security_timestamps(self, security_ids: list[int], field_name: str) -> int:
        """批量更新 Security 表中指定的 TIMESTAMP 字段为当前时间（单条 UPDATE，避免逐行往返）。"""
        allowed_fields = [
            'info_last_updated_at',
            'full_data_last_updated_at',
            'actions_last_updated_at',
            'events_last_updated_at',
            'shares_last_updated_at',
            'short_data_last_updated_at',
            'news_last_updated_at',
        ]
        if field_name not in allowed_fields:
            raise ValueError(f"无效的时间戳字段名: {field_name}")
        if not security_ids:
            return 0
        stmt = (
            update(Security)
            .where(Security.id.in_(security_ids))
            .values({field_name: func.now()})
        )
        with self.engine.connect() as conn:
            result = conn.execute(stmt)
            conn.commit()
            return result.rowcount or 0

    def bulk_update_mappings(self, model, mappings: list[dict]) -> int:
        """
        高效批量更新（executemany UPDATE），适合已知主键且只更新部分列的场景。
        :param model: SQLAlchemy ORM 模型类。
        :param mappings: 每条更新的字典，必须包含主键字段。
        :return: 尝试更新的记录数（不保证每条都实际命中行）。
        """
        if not mappings:
            return 0
        with self.get_session() as session:
            session.bulk_update_mappings(model, mappings)
            session.commit()
        return len(mappings)

    def upsert_daily_prices(self, price_data: list[dict]) -> int:
        """
        批量插入或更新日线价格数据 (基于UPSERT)。
        此方法适用于 Massive aggregates / grouped daily 等批量价格写入。
        """
        if not price_data:
            return 0

        stmt = pg_insert(DailyPrice).values(price_data)
        # 动态构建更新集
        update_keys = set().union(*(row.keys() for row in price_data))
        update_columns = {}
        if 'open' in update_keys: update_columns['open'] = stmt.excluded.open
        if 'high' in update_keys: update_columns['high'] = stmt.excluded.high
        if 'low' in update_keys: update_columns['low'] = stmt.excluded.low
        if 'close' in update_keys: update_columns['close'] = stmt.excluded.close
        if 'volume' in update_keys: update_columns['volume'] = stmt.excluded.volume
        if 'vwap' in update_keys: update_columns['vwap'] = stmt.excluded.vwap
        if 'trade_count' in update_keys: update_columns['trade_count'] = stmt.excluded.trade_count
        if 'otc' in update_keys: update_columns['otc'] = stmt.excluded.otc
        if 'pre_market' in update_keys: update_columns['pre_market'] = stmt.excluded.pre_market
        if 'after_hours' in update_keys: update_columns['after_hours'] = stmt.excluded.after_hours
        if not update_columns:
            stmt = stmt.on_conflict_do_nothing(index_elements=['security_id', 'date'])
        else:
            stmt = stmt.on_conflict_do_update(
                index_elements=['security_id', 'date'],
                set_=update_columns
            )
        with self.engine.connect() as conn:
            result = conn.execute(stmt)
            conn.commit()
            return result.rowcount

    def upsert_historical_shares(self, shares_data: list[dict]) -> int:
        """
        批量插入或更新历史股本数据 (UPSERT)。
        冲突键: (security_id, filing_date, source)
        """
        if not shares_data:
            return 0

        rows = _normalize_batch_rows(HistoricalShare, shares_data)
        rows = [
            row
            for row in rows
            if row.get('security_id')
            and row.get('filing_date')
            and row.get('period_end_date')
            and row.get('total_shares') is not None
            and row.get('source')
        ]
        if not rows:
            return 0

        stmt = pg_insert(HistoricalShare).values(rows)
        update_keys = set().union(*(row.keys() for row in rows))
        update_columns = {}
        if 'total_shares' in update_keys: update_columns['total_shares'] = stmt.excluded.total_shares
        if 'float_shares' in update_keys: update_columns['float_shares'] = stmt.excluded.float_shares
        if 'free_float_percent' in update_keys: update_columns['free_float_percent'] = stmt.excluded.free_float_percent
        if 'period_end_date' in update_keys: update_columns['period_end_date'] = stmt.excluded.period_end_date

        if not update_columns:
            stmt = stmt.on_conflict_do_nothing(index_elements=['security_id', 'filing_date', 'source'])
        else:
            stmt = stmt.on_conflict_do_update(
                index_elements=['security_id', 'filing_date', 'source'],
                set_=update_columns
            )

        with self.engine.connect() as conn:
            self._lock_model_sequence_sync(conn, HistoricalShare)
            self._sync_model_id_sequence(conn, HistoricalShare)
            result = conn.execute(stmt)
            conn.commit()
            return result.rowcount

    def upsert_historical_floats(self, float_data: list[dict]) -> int:
        if not float_data:
            return 0

        rows = [_clean_for_model(HistoricalFloat, row) for row in float_data]
        rows = [row for row in rows if row.get('security_id') and row.get('effective_date') and row.get('free_float') is not None]
        if not rows:
            return 0

        stmt = pg_insert(HistoricalFloat).values(rows)
        update_columns = {
            'free_float': stmt.excluded.free_float,
            'free_float_percent': stmt.excluded.free_float_percent,
        }
        stmt = stmt.on_conflict_do_update(
            index_elements=['security_id', 'effective_date', 'source'],
            set_=update_columns,
        )
        with self.engine.connect() as conn:
            self._lock_model_sequence_sync(conn, HistoricalFloat)
            self._sync_model_id_sequence(conn, HistoricalFloat)
            result = conn.execute(stmt)
            conn.commit()
            return result.rowcount

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

    def upsert_short_interests(self, rows_data: list[dict]) -> int:
        rows = [_clean_for_model(ShortInterest, row) for row in rows_data]
        rows = [row for row in rows if row.get('security_id') and row.get('settlement_date') and row.get('short_interest') is not None]
        if not rows:
            return 0

        stmt = pg_insert(ShortInterest).values(rows)
        update_columns = {
            'ticker': stmt.excluded.ticker,
            'short_interest': stmt.excluded.short_interest,
            'avg_daily_volume': stmt.excluded.avg_daily_volume,
            'days_to_cover': stmt.excluded.days_to_cover,
        }
        stmt = stmt.on_conflict_do_update(
            index_elements=['security_id', 'settlement_date', 'source'],
            set_=update_columns,
        )
        with self.engine.connect() as conn:
            self._lock_model_sequence_sync(conn, ShortInterest)
            self._sync_model_id_sequence(conn, ShortInterest)
            result = conn.execute(stmt)
            conn.commit()
            return result.rowcount

    def upsert_short_volumes(self, rows_data: list[dict]) -> int:
        rows = [_clean_for_model(ShortVolume, row) for row in rows_data]
        rows = [row for row in rows if row.get('security_id') and row.get('date') and row.get('short_volume') is not None]
        if not rows:
            return 0

        stmt = pg_insert(ShortVolume).values(rows)
        update_keys = set().union(*(row.keys() for row in rows))
        update_columns = {
            key: getattr(stmt.excluded, key)
            for key in update_keys
            if key not in {'id', 'security_id', 'date', 'source', 'created_at'}
        }
        stmt = stmt.on_conflict_do_update(
            index_elements=['security_id', 'date', 'source'],
            set_=update_columns,
        )
        with self.engine.connect() as conn:
            self._lock_model_sequence_sync(conn, ShortVolume)
            self._sync_model_id_sequence(conn, ShortVolume)
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

    def update_security_price_latest_date(self, security_id: int, latest_date: date, is_full_run: bool):
        """
        更新 Security 表中的价格数据最新日期和全量更新时间戳。
        """
        values_to_update = {
            'price_data_latest_date': latest_date
        }
        if is_full_run:
            values_to_update['full_data_last_updated_at'] = func.now()
        stmt = (
            update(Security)
            .where(Security.id == security_id)
            .values(values_to_update)
        )
        with self.engine.connect() as conn:
            conn.execute(stmt)
            conn.commit()

    def get_security_price_max_date(self, security_id: int) -> date | None:
        """返回某个 security 在 daily_prices 中实际存在的最大交易日。"""
        with self.get_session() as session:
            return (
                session.query(func.max(DailyPrice.date))
                .filter(DailyPrice.security_id == security_id)
                .scalar()
            )

    def get_security_short_max_dates(self, security_ids: list[int]) -> dict[int, dict[str, date | None]]:
        """返回每个 security 在 short interest / short volume 中实际存在的最大日期。"""
        if not security_ids:
            return {}

        result = {security_id: {"interest": None, "volume": None} for security_id in security_ids}
        with self.get_session() as session:
            interest_rows = (
                session.query(ShortInterest.security_id, func.max(ShortInterest.settlement_date))
                .filter(ShortInterest.security_id.in_(security_ids))
                .group_by(ShortInterest.security_id)
                .all()
            )
            volume_rows = (
                session.query(ShortVolume.security_id, func.max(ShortVolume.date))
                .filter(ShortVolume.security_id.in_(security_ids))
                .group_by(ShortVolume.security_id)
                .all()
            )

        for security_id, max_date in interest_rows:
            result[security_id]["interest"] = max_date
        for security_id, max_date in volume_rows:
            result[security_id]["volume"] = max_date
        return result

    def ensure_security_price_latest_date_at_least(self, security_ids: list[int], latest_date: date) -> int:
        """
        将 Security.price_data_latest_date 至少推进到指定日期。
        适用于“覆盖更新已有价格行”后同步 metadata，避免 latest_date 落后于实际数据。
        """
        if not security_ids:
            return 0

        stmt = (
            update(Security)
            .where(Security.id.in_(security_ids))
            .where(
                (Security.price_data_latest_date.is_(None))
                | (Security.price_data_latest_date < latest_date)
            )
            .values(price_data_latest_date=latest_date)
        )
        with self.engine.connect() as conn:
            result = conn.execute(stmt)
            conn.commit()
            return result.rowcount or 0
