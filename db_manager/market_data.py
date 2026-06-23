"""日线价格、历史股本/流通盘、空头数据等市场事实表的写入与查询。"""
from datetime import date

from sqlalchemy import func
from sqlalchemy.dialects.postgresql import insert as pg_insert

from data_models.models import (
    DailyPrice,
    HistoricalFloat,
    HistoricalShare,
    ShortInterest,
    ShortVolume,
)

from .helpers import _clean_for_model, _dedupe_rows_by_key, _group_rows_by_key_set, _normalize_batch_rows


class MarketDataMixin:
    def upsert_daily_prices(self, price_data: list[dict]) -> int:
        """
        批量插入或更新日线价格数据 (基于UPSERT)。
        此方法适用于 Massive aggregates / grouped daily 等批量价格写入。
        按 key set 分组执行，避免混合键集批次把缺失字段覆盖成 NULL。
        """
        if not price_data:
            return 0

        price_data = _dedupe_rows_by_key(price_data, ['security_id', 'date'])
        total_rowcount = 0
        for group in _group_rows_by_key_set(price_data):
            stmt = pg_insert(DailyPrice).values(group)
            # 动态构建更新集——只覆盖本组明确提供的字段
            update_keys = set(group[0].keys())
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
                total_rowcount += result.rowcount
        return total_rowcount

    def get_security_price_max_date(self, security_id: int) -> date | None:
        """返回某个 security 在 daily_prices 中实际存在的最大交易日。"""
        with self.get_session() as session:
            return (
                session.query(func.max(DailyPrice.date))
                .filter(DailyPrice.security_id == security_id)
                .scalar()
            )

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

        rows = _dedupe_rows_by_key(rows, ['security_id', 'filing_date', 'source'])

        stmt = pg_insert(HistoricalShare).values(rows)
        update_keys = set().union(*(row.keys() for row in rows))
        update_columns = {}
        if 'total_shares' in update_keys: update_columns['total_shares'] = stmt.excluded.total_shares
        if 'float_shares' in update_keys:
            update_columns['float_shares'] = func.coalesce(stmt.excluded.float_shares, HistoricalShare.float_shares)
        if 'free_float_percent' in update_keys:
            update_columns['free_float_percent'] = func.coalesce(
                stmt.excluded.free_float_percent,
                HistoricalShare.free_float_percent,
            )
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

        rows = _dedupe_rows_by_key(rows, ['security_id', 'effective_date', 'source'])

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

    def upsert_short_interests(self, rows_data: list[dict]) -> int:
        rows = [_clean_for_model(ShortInterest, row) for row in rows_data]
        rows = [row for row in rows if row.get('security_id') and row.get('settlement_date') and row.get('short_interest') is not None]
        if not rows:
            return 0

        rows = _dedupe_rows_by_key(rows, ['security_id', 'settlement_date', 'source'])

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

        rows = _dedupe_rows_by_key(rows, ['security_id', 'date', 'source'])

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
