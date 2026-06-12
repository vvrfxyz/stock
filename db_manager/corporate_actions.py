"""公司行动（分红/拆股）与复权因子 reference/cache 的写入。"""
from loguru import logger
from sqlalchemy import func, text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from data_models.models import (
    ComputedAdjustmentFactor,
    CorporateAction,
    VendorAdjustmentFactor,
)

from .helpers import ACTION_SOURCE_MASSIVE, _clean_for_model, _format_action_decimal


class CorporateActionsMixin:
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
