"""securities 表的 upsert 与各类 watermark 时间戳维护。"""
import json
import os
import random
from datetime import date, datetime, timezone

from loguru import logger
from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert

from data_models.models import Security

from .helpers import _group_rows_by_key_set


_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
# 身份冲突被跳过的行写到这里做持久 quarantine：只进 log 会随轮转丢失，
# 而被跳过的往往是"正在交易但库里没有"的新公司，必须可事后人工对账重建。
IDENTITY_CONFLICT_QUARANTINE = os.path.join(
    _PROJECT_ROOT, "logs", "security_identity_conflicts.jsonl"
)


def _quarantine_identity_conflict(existing: dict, incoming: dict, field: str) -> None:
    """把疑似 ticker 回收、被 upsert 跳过的行追加到持久 quarantine 文件。"""
    try:
        os.makedirs(os.path.dirname(IDENTITY_CONFLICT_QUARANTINE), exist_ok=True)
        record = {
            "detected_at": datetime.now(timezone.utc).isoformat(),
            "symbol": incoming.get("symbol"),
            "conflict_field": field,
            "existing": {
                "composite_figi": existing.get("composite_figi"),
                "cik": existing.get("cik"),
            },
            "incoming": {
                key: incoming.get(key)
                for key in ("composite_figi", "cik", "name", "market", "type", "exchange")
            },
        }
        with open(IDENTITY_CONFLICT_QUARANTINE, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")
    except Exception as exc:  # quarantine 失败不能阻断主写入
        logger.opt(exception=exc).error("写入身份冲突 quarantine 文件失败: {}", exc)


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
            cleaned.setdefault("is_active", True)
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
                select(Security.symbol, Security.composite_figi, Security.cik)
                .where(Security.symbol.in_(symbols), Security.is_active.is_(True))
            ).mappings().all()
            existing_by_symbol = {row["symbol"]: dict(row) for row in existing_rows}
            safe_rows: list[dict] = []
            skipped_conflicts = 0
            for row in cleaned_rows:
                existing = existing_by_symbol.get(row["symbol"])
                if existing:
                    conflict, field = _is_identity_conflict(existing, row)
                    if conflict:
                        skipped_conflicts += 1
                        _quarantine_identity_conflict(existing, row, field)
                        logger.error(
                            "跳过 symbol={} 的 universe upsert：现有 {}={}，新值 {}={}，疑似 ticker 回收；"
                            "已写入 quarantine {}，请人工拆分身份。",
                            row["symbol"],
                            field,
                            existing.get(field),
                            field,
                            row.get(field),
                            IDENTITY_CONFLICT_QUARANTINE,
                        )
                        continue
                safe_rows.append(row)
            if skipped_conflicts:
                logger.warning(
                    "跳过 {} 条疑似 ticker 回收的 securities upsert（已 quarantine 至 {}）。",
                    skipped_conflicts,
                    IDENTITY_CONFLICT_QUARANTINE,
                )
            if not safe_rows:
                return 0

            self._lock_model_sequence_sync(conn, Security)
            self._sync_model_id_sequence(conn, Security)
            # 详情 payload 会剔除 None 字段，键集可能互不相同；多行 VALUES 必须按键集分组，
            # 否则 SQLAlchemy 抛 CompileError。分组同时保留“只更新提供字段”的语义。
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
