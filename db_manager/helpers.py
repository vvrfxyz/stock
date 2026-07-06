"""db_manager 各模块共享的行清洗与 upsert 语句构造工具。"""
from decimal import Decimal, InvalidOperation

from sqlalchemy import func
from sqlalchemy.dialects.postgresql import insert as pg_insert

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


def _dedupe_rows_by_key(rows: list[dict], key_columns: list[str]) -> list[dict]:
    """批内按冲突键去重，避免 PostgreSQL ON CONFLICT 同语句 CardinalityViolation。

    后出现的行胜出，符合“同一批里更靠后的 vendor 修订覆盖前值”的直觉。
    调用方应先过滤掉缺少冲突键的行。
    """
    deduped: dict[tuple, dict] = {}
    for row in rows:
        deduped[tuple(row.get(column) for column in key_columns)] = row
    return list(deduped.values())


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
        # excluded[key] 索引访问：列名撞 ColumnCollection 字典方法（items/keys/values）
        # 时 getattr 拿到的是 bound method 而非列，psycopg2 报 can't adapt type 'method'
        key: stmt.excluded[key]
        for key in update_keys
        if key not in protected
    }
    if "updated_at" in model.__table__.columns:
        update_columns["updated_at"] = func.now()
    if not update_columns:
        return stmt.on_conflict_do_nothing(index_elements=index_elements)
    return stmt.on_conflict_do_update(index_elements=index_elements, set_=update_columns)
