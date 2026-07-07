"""securities 表的 upsert 与各类 watermark 时间戳维护。"""
import json
import random
from datetime import date, datetime, timezone

from loguru import logger
from sqlalchemy import func, or_, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert

from data_models.models import Company, CompanyEvent, DailyPrice, Security, SecurityIdentityEvent, SecuritySymbolHistory
from utils.massive_config import ALLOWED_US_SECURITY_TYPES

from .helpers import _clean_for_model, _dedupe_rows_by_key, _group_rows_by_key_set

# enrich_security_identity 可补空的身份/生命周期列白名单（阶段 1a 收编
# B2/B3/B4/B9 各脚本散落的 NULL-only 补空直写；name/exchange 在拆表终态
# 分属详情/身份两桶，阶段 3 再分流）。
ENRICHABLE_SECURITY_COLUMNS = frozenset({
    "delist_date",
    "list_date",
    "cik",
    "composite_figi",
    "share_class_figi",
    "name",
    "exchange",
    "company_id",
})


def _norm_identifier(value) -> str | None:
    if value is None:
        return None
    text = str(value).strip().upper()
    return text or None


def _is_identity_conflict(existing: dict, incoming: dict) -> tuple[bool, str]:
    for field in ("composite_figi", "cik"):
        old_value = _norm_identifier(existing.get(field))
        new_value = _norm_identifier(incoming.get(field))
        if old_value and new_value and old_value != new_value:
            return True, field
    return False, ""


class SecuritiesMixin:
    def upsert_companies(self, rows_data: list[dict]) -> int:
        """写公司实体（PERMCO 等价物）。冲突键 ['cik']。

        cik 为 NULL 的行直接跳过：companies.cik 唯一约束在 PG 默认
        NULLS DISTINCT 语义下对 NULL 永不触发冲突，重复运行会无限插入——
        无 CIK 实体（ETF 发行主体等）不该经此通道建行。
        冲突时刷新 name（公司改名以 vendor/SEC 最新为准）；id/created_at/cik 受保护。"""
        rows = [_clean_for_model(Company, row) for row in rows_data]
        rows = [row for row in rows if row.get('cik')]
        if not rows:
            return 0
        rows = _dedupe_rows_by_key(rows, ['cik'])
        written = 0
        # 按键集分组：冲突时只更新该行明确提供的字段（缺 name 不覆盖成 NULL）。
        for group in _group_rows_by_key_set(rows):
            written += self._batch_upsert(
                Company,
                group,
                ['cik'],
                update_on_conflict=True,
            )
        return written

    def upsert_company_events(self, rows_data: list[dict]) -> int:
        """写公司世系/并购边。冲突键
        (predecessor_company_id, successor_company_id, event_date, event_type)。

        幂等全量重建语义：冲突时以本批为准原位覆盖 evidence/source（重跑用最新
        证据刷新），id/created_at 受保护。缺任一必填字段（两端 company_id/日期/
        类型）的行直接跳过——NOT NULL 约束不该靠数据库报错兜底。"""
        rows = [_clean_for_model(CompanyEvent, row) for row in rows_data]
        rows = [
            row for row in rows
            if row.get('predecessor_company_id')
            and row.get('successor_company_id')
            and row.get('event_date')
            and row.get('event_type')
        ]
        if not rows:
            return 0
        index_elements = [
            'predecessor_company_id', 'successor_company_id', 'event_date', 'event_type',
        ]
        written = 0
        # 按键集分组：冲突时只更新该行明确提供的字段（缺 evidence 不覆盖成 NULL）。
        for group in _group_rows_by_key_set(rows):
            written += self._batch_upsert(
                CompanyEvent,
                group,
                index_elements,
                update_on_conflict=True,
            )
        return written

    def get_company_id_by_cik(self, cik: str) -> int | None:
        """按 CIK 查公司实体 id；无则 None。"""
        if not cik:
            return None
        with self.engine.connect() as conn:
            return conn.execute(
                select(Company.id).where(Company.cik == cik)
            ).scalar_one_or_none()

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
        默认不更新 info_last_updated_at，避免把"基础引用数据刷新"误判成"详情刷新"。
        """
        if not securities_data:
            return 0

        valid_columns = set(Security.__table__.columns.keys())
        cleaned_rows: list[dict] = []
        seen_symbols: set[str] = set()
        for row in securities_data:
            cleaned = {key: value for key, value in row.items() if key in valid_columns}
            if "symbol" not in cleaned:
                continue
            # 批内按冲突键 symbol 去重：vendor 可能同批返回大小写变体（如 TPC/TpC），
            # 归一化后撞同一冲突键会让整批 ON CONFLICT 抛 CardinalityViolation。
            # 两条往往是不同身份，不能 last-wins——保留首条并告警，留待人工拆分。
            if cleaned["symbol"] in seen_symbols:
                logger.warning(
                    "批内 symbol={} 重复（大小写变体/vendor 重复行，可能是不同身份），保留首条并跳过后续行。",
                    cleaned["symbol"],
                )
                continue
            seen_symbols.add(cleaned["symbol"])
            cleaned.setdefault("full_refresh_interval", random.randint(25, 40))
            cleaned.setdefault("current_symbol", cleaned.get("symbol"))
            # setdefault 对显式 None 失效：is_active=NULL 会绕过 is_active IS TRUE
            # 的部分唯一索引反复插入重复行，这里统一归一化为 True。
            if cleaned.get("is_active") is None:
                cleaned["is_active"] = True
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
            symbols = sorted({row["symbol"] for row in cleaned_rows})
            existing_rows = conn.execute(
                select(Security.id, Security.symbol, Security.composite_figi, Security.cik)
                .where(Security.symbol.in_(symbols), Security.is_active.is_(True))
            ).mappings().all()
            existing_by_symbol = {row["symbol"]: dict(row) for row in existing_rows}
            safe_rows: list[dict] = []
            skipped_conflicts = 0
            identity_events: list[dict] = []
            for row in cleaned_rows:
                existing = existing_by_symbol.get(row["symbol"])
                if existing:
                    conflict, field = _is_identity_conflict(existing, row)
                    if conflict:
                        skipped_conflicts += 1
                        identity_events.append({
                            "security_id": existing.get("id") or 0,
                            "event_type": "QUARANTINE",
                            "old_symbol": row["symbol"],
                            "new_symbol": row["symbol"],
                            "resolution_source": "AUTO",
                            "confidence": "LOW",
                            "details": json.dumps({
                                "conflict_field": field,
                                "existing": {k: existing.get(k) for k in ("composite_figi", "cik")},
                                "incoming": {k: row.get(k) for k in ("composite_figi", "cik", "name", "market", "type", "exchange")},
                            }, ensure_ascii=False),
                        })
                        logger.error(
                            "跳过 symbol={} 的 universe upsert: 现有 {}={}, 新值 {}={}, 疑似 ticker 回收; "
                            "已写入 identity_events, 请人工拆分身份。",
                            row["symbol"],
                            field,
                            existing.get(field),
                            field,
                            row.get(field),
                        )
                        continue
                safe_rows.append(row)
            if skipped_conflicts:
                logger.warning(
                    "跳过 {} 条疑似 ticker 回收的 securities upsert（已记录 identity event）。",
                    skipped_conflicts,
                )
            # 写冲突事件到 security_identity_events。包进 SAVEPOINT：事件插入失败
            # 只回滚嵌套事务本身，不毒化共享连接上的主 upsert 事务（同 repair_identity 的做法）。
            valid_events = [e for e in identity_events if e.get("security_id")]
            if valid_events:
                try:
                    with conn.begin_nested():
                        self._lock_model_sequence_sync(conn, SecurityIdentityEvent)
                        self._sync_model_id_sequence(conn, SecurityIdentityEvent)
                        for grp in _group_rows_by_key_set(
                            [_clean_for_model(SecurityIdentityEvent, e) for e in valid_events]
                        ):
                            conn.execute(pg_insert(SecurityIdentityEvent).values(grp))
                except Exception as exc:
                    logger.opt(exception=exc).warning("写入身份冲突 identity event 失败: {}", exc)
            if not safe_rows:
                conn.commit()
                return 0

            self._lock_model_sequence_sync(conn, Security)
            self._sync_model_id_sequence(conn, Security)
            # 详情 payload 会剔除 None 字段，键集可能互不相同；多行 VALUES 必须按键集分组，
            # 否则 SQLAlchemy 抛 CompileError。分组同时保留"只更新提供字段"的语义。
            for group in _group_rows_by_key_set(safe_rows):
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
                    final_stmt = stmt.on_conflict_do_nothing(
                        index_elements=['symbol'],
                        index_where=Security.is_active.is_(True),
                    )
                else:
                    final_stmt = stmt.on_conflict_do_update(
                        index_elements=['symbol'],
                        index_where=Security.is_active.is_(True),
                        set_=update_columns,
                    )
                result = conn.execute(final_stmt)
                total_rowcount += result.rowcount or 0
            conn.commit()
        return total_rowcount

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

    def ensure_security_price_latest_date_at_least(self, security_ids: list[int], latest_date: date) -> int:
        """
        将 Security.price_data_latest_date 至少推进到指定日期。
        适用于"覆盖更新已有价格行"后同步 metadata，避免 latest_date 落后于实际数据。
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

    def recalculate_price_latest_dates(self, security_ids: list[int] | None = None) -> int:
        """按 daily_prices 事实重算 price_data_latest_date 水位（收编 B6/B7 直写）。

        - security_ids=None：全表校准（calibrate_price_latest_date 语义）；
          传 ids：范围自愈（import_day_aggs touched_ids 语义）；空列表直接返回 0。
        - join 语义：只更新在 daily_prices 里有行的证券——无价格行的证券
          不触碰、绝不回落 NULL（那是 repair_identity husk 合并的专属语义，
          阶段 2 收口时另走通道）。
        - 守卫统一为 IS DISTINCT FROM：水位已一致的行不产生无效写
          （与 B6 的 `!= OR IS NULL` 在 max_date 非 NULL 前提下逐行等价）。

        返回实际更新的行数。
        """
        latest = select(
            DailyPrice.security_id,
            func.max(DailyPrice.date).label("max_date"),
        ).group_by(DailyPrice.security_id)
        if security_ids is not None:
            if not security_ids:
                return 0
            latest = latest.where(DailyPrice.security_id.in_(security_ids))
        subquery = latest.subquery("latest_dates")

        stmt = (
            update(Security)
            .values(price_data_latest_date=subquery.c.max_date)
            .where(Security.id == subquery.c.security_id)
            .where(Security.price_data_latest_date.is_distinct_from(subquery.c.max_date))
        )
        with self.engine.connect() as conn:
            result = conn.execute(stmt)
            conn.commit()
            return result.rowcount or 0

    def deactivate_missing_securities(self, active_symbols: set[str]) -> int:
        """把不在 vendor 活跃名单中的 US 白名单类型活跃证券标记为 inactive（收编 B1 直写）。

        三重过滤内聚在此：upper(market)='US' + upper(type) IN
        ALLOWED_US_SECURITY_TYPES + is_active=True，且 symbol 不在 active_symbols
        中才摘牌。--limit / --skip-mark-missing-inactive 之类的门禁留在脚本层。

        active_symbols 为空集抛 ValueError：空名单等价于全量摘牌，只可能是
        上游拉取失败，绝不能落库（sync_massive_universe 在名单为空时早退，
        正常路径到不了这里）。返回实际摘牌行数。
        """
        if not active_symbols:
            raise ValueError("deactivate_missing_securities 拒绝空 active_symbols：等价于全量摘牌。")
        stmt = (
            update(Security)
            .where(func.upper(Security.market) == "US")
            .where(func.upper(Security.type).in_(ALLOWED_US_SECURITY_TYPES))
            .where(Security.is_active == True)  # noqa: E712
            .where(~Security.symbol.in_(active_symbols))
            .values(is_active=False)
        )
        with self.engine.connect() as conn:
            result = conn.execute(stmt)
            conn.commit()
            return result.rowcount or 0

    def enrich_security_identity(self, security_id: int, fills: dict) -> int:
        """NULL-only 逐列补空身份/生命周期字段（收编 B2/B3/B4/B9 各脚本的补空直写）。

        对 fills 中每个非 None 值执行 SET col = COALESCE(col, :val)，
        WHERE 带 OR(col IS NULL) 守卫——既有值绝不覆盖。列名限
        ENRICHABLE_SECURITY_COLUMNS 白名单，未知列抛 ValueError。

        返回 rowcount：1 = 至少一列实际补入；0 = 行不存在、fills 全为
        None、或所有目标列均已非 NULL（无操作——backfill_rename_events
        的竞态计数正依赖这一语义）。

        语义定案（vs sync_delisted_universe 旧 _apply_fills 的整行 AND 守卫）：
        旧守卫要求全部 fill 列都为 NULL 才整行补；本 API 是逐列 COALESCE——
        并发下部分列已被他人补入的行不再整行跳过，而是补齐剩余 NULL 列。
        这严格更精确，且任何情况下都不会覆盖既有值。
        """
        unknown = set(fills) - ENRICHABLE_SECURITY_COLUMNS
        if unknown:
            raise ValueError(
                f"enrich_security_identity 收到白名单外的列: {sorted(unknown)}"
            )
        columns = Security.__table__.c
        values = {
            col: func.coalesce(columns[col], value)
            for col, value in fills.items()
            if value is not None
        }
        if not values:
            return 0
        stmt = (
            update(Security)
            .where(Security.id == security_id)
            .where(or_(*[columns[col].is_(None) for col in values]))
            .values(values)
        )
        with self.engine.connect() as conn:
            result = conn.execute(stmt)
            conn.commit()
            return result.rowcount or 0

    # ------------------------------------------------------------------ #
    # 身份变更
    # ------------------------------------------------------------------ #
    def rename_security(
        self,
        security_id: int,
        old_symbol: str,
        new_symbol: str,
        *,
        exchange: str | None = None,
        source: str = "MASSIVE",
    ) -> None:
        """改名：更新 symbol/current_symbol 并维护 symbol history 区间。

        区间语义：每行表示 (symbol, start_date=生效日, end_date=失效日/NULL 表示仍在用)。
        改名时关闭 old_symbol 的开区间行（end_date=今天；old_symbol 无任何既有行时
        补插一条 end_date=今天 的闭合行，start_date 未知留 NULL），并为 new_symbol
        插入 start_date=今天 的开区间行——与 update_massive_events 写入的
        (新 ticker, start_date=生效日) 口径一致。

        如果 new_symbol 已被另一个活跃行占用，抛出 ValueError 而非违反唯一索引。
        """
        today = date.today()
        with self.engine.connect() as conn:
            # 防御：new_symbol 不得已被其他活跃行占用
            conflict = conn.execute(
                select(Security.id)
                .where(Security.symbol == new_symbol, Security.is_active.is_(True),
                       Security.id != security_id)
            ).scalar()
            if conflict is not None:
                raise ValueError(
                    f"rename_security 失败: new_symbol={new_symbol} 已被 security_id={conflict} 占用"
                )
            conn.execute(
                update(Security)
                .where(Security.id == security_id)
                .values(symbol=new_symbol, current_symbol=new_symbol)
            )
            self._lock_model_sequence_sync(conn, SecuritySymbolHistory)
            self._sync_model_id_sequence(conn, SecuritySymbolHistory)
            if old_symbol:
                closed = conn.execute(
                    update(SecuritySymbolHistory)
                    .where(
                        SecuritySymbolHistory.security_id == security_id,
                        SecuritySymbolHistory.symbol == old_symbol,
                        SecuritySymbolHistory.end_date.is_(None),
                    )
                    .values(end_date=today)
                ).rowcount or 0
                if not closed:
                    has_any = conn.execute(
                        select(SecuritySymbolHistory.id)
                        .where(
                            SecuritySymbolHistory.security_id == security_id,
                            SecuritySymbolHistory.symbol == old_symbol,
                        )
                        .limit(1)
                    ).scalar()
                    if has_any is None:
                        closing_row = _clean_for_model(SecuritySymbolHistory, {
                            "security_id": security_id,
                            "symbol": old_symbol,
                            "exchange": exchange,
                            "source": source,
                            "event_type": "ticker_change",
                            "end_date": today,
                        })
                        conn.execute(pg_insert(SecuritySymbolHistory).values(closing_row))
            new_row = _clean_for_model(SecuritySymbolHistory, {
                "security_id": security_id,
                "symbol": new_symbol,
                "exchange": exchange,
                "source": source,
                "event_type": "ticker_change",
                "start_date": today,
            })
            stmt = pg_insert(SecuritySymbolHistory).values(new_row)
            stmt = stmt.on_conflict_do_update(
                index_elements=["security_id", "symbol", "source", "start_date"],
                set_={
                    "exchange": stmt.excluded.exchange,
                    "event_type": stmt.excluded.event_type,
                    # 同日重复 rename：new_symbol 重新生效，区间重新打开
                    "end_date": None,
                },
            )
            conn.execute(stmt)
            conn.commit()
        logger.info(
            "证券 id={} 改名: {} -> {}",
            security_id, old_symbol, new_symbol,
        )

    def insert_backfilled_securities(self, rows_data: list[dict]) -> list[tuple[int, str, date]]:
        """纯插入退市补录证券行（sync_delisted_universe 专用），返回 [(id, symbol, delist_date)]。

        与 upsert_securities_by_symbol 隔离：那条路径以 symbol 为冲突键，会把
        同 symbol 的退市补录行错误合并到现任持有者上。这里只插不改，且强制
        is_active=False——symbol 的部分唯一索引只约束活跃行，退市行同 symbol
        多条合法（车牌历任持有者各占一行）。返回值带 delist_date 是因为
        symbol 在死票之间也可能重复，(symbol, delist_date) 才是本批唯一键。
        """
        rows = [_clean_for_model(Security, row) for row in rows_data]
        rows = [row for row in rows if row.get("symbol") and row.get("delist_date")]
        if not rows:
            return []
        for row in rows:
            row["is_active"] = False
            row.setdefault("current_symbol", row["symbol"])
        inserted: list[tuple[int, str, date]] = []
        with self.engine.connect() as conn:
            self._lock_model_sequence_sync(conn, Security)
            self._sync_model_id_sequence(conn, Security)
            for group in _group_rows_by_key_set(rows):
                result = conn.execute(
                    pg_insert(Security).values(group)
                    .returning(Security.id, Security.symbol, Security.delist_date)
                )
                inserted.extend((r.id, r.symbol, r.delist_date) for r in result)
            conn.commit()
        return inserted

    def insert_identity_events(self, events: list[dict]) -> int:
        """批量写入身份变更事件（纯追加，不做 upsert）。"""
        rows = [_clean_for_model(SecurityIdentityEvent, e) for e in events]
        rows = [r for r in rows if r.get("security_id") and r.get("event_type")]
        if not rows:
            return 0
        total = 0
        with self.engine.connect() as conn:
            self._lock_model_sequence_sync(conn, SecurityIdentityEvent)
            self._sync_model_id_sequence(conn, SecurityIdentityEvent)
            for group in _group_rows_by_key_set(rows):
                result = conn.execute(pg_insert(SecurityIdentityEvent).values(group))
                total += result.rowcount
            conn.commit()
        return total
