"""repair_identity 存量修复工具的单元测试。

不依赖 PostgreSQL——验证 plan 生成和 SQL 构建逻辑。
"""
from scripts.repair_identity import (
    TABLE_CONFLICT_KEYS,
    TABLES_WITH_SECURITY_ID,
    generate_merge_sql,
)


def test_generate_merge_sql_produces_update_per_table():
    plan = {
        "type": "MERGE",
        "figi": "BBG000MM2P62",
        "keep_id": 1,
        "keep_symbol": "meta",
        "merge_ids": [2],
        "merge_symbols": ["fb"],
    }
    stmts = generate_merge_sql(plan)
    # 每个表一条 UPDATE + 1 条 inactive
    assert len(stmts) == len(TABLES_WITH_SECURITY_ID) + 1
    assert any("UPDATE daily_prices SET security_id = 1" in s for s in stmts)
    assert any("UPDATE securities SET is_active = false WHERE id = 2" in s for s in stmts)


def test_generate_merge_sql_handles_multiple_merge_ids():
    plan = {
        "type": "MERGE",
        "figi": "BBG000TEST",
        "keep_id": 10,
        "keep_symbol": "keep",
        "merge_ids": [20, 30],
        "merge_symbols": ["old1", "old2"],
    }
    stmts = generate_merge_sql(plan)
    # 2 个 merge id × (tables + 1 inactive) 条
    expected = 2 * (len(TABLES_WITH_SECURITY_ID) + 1)
    assert len(stmts) == expected
    assert sum("SET is_active = false" in s for s in stmts) == 2
    assert any("WHERE security_id = 20" in s for s in stmts)
    assert any("WHERE security_id = 30" in s for s in stmts)


def test_tables_list_covers_all_key_domains():
    """确保 TABLES_WITH_SECURITY_ID 覆盖了所有关键数据域。"""
    required = {
        "daily_prices", "corporate_actions", "computed_adjustment_factors",
        "short_volumes", "short_interests", "historical_shares",
        "sec_fundamental_facts", "insider_transactions", "institutional_holdings",
        "sec_filings", "news_article_insights",
    }
    assert required.issubset(set(TABLES_WITH_SECURITY_ID))
    # news_articles 没有 security_id 列（按 tickers 关联），出现在清单里说明回归
    assert "news_articles" not in TABLES_WITH_SECURITY_ID


def test_tables_list_matches_models_schema():
    """表清单/冲突键必须与 models.py 的真实列与唯一约束一致，防漂移。"""
    from data_models import models
    from sqlalchemy import UniqueConstraint

    by_name = {m.__tablename__: m.__table__ for m in models.Base.__subclasses__()}
    for table in TABLES_WITH_SECURITY_ID:
        assert table in by_name, f"{table} 不在 models.py 中"
        assert "security_id" in by_name[table].columns, f"{table} 没有 security_id 列"
    for table, cols in TABLE_CONFLICT_KEYS.items():
        tbl = by_name[table]
        ucs = [
            set(c.name for c in cons.columns)
            for cons in tbl.constraints
            if isinstance(cons, UniqueConstraint)
        ]
        # 主键含 security_id 的（daily_prices）也构成冲突键
        pk = set(c.name for c in tbl.primary_key.columns)
        if "security_id" in pk and len(pk) > 1:
            ucs.append(pk)
        expect = set(cols) | {"security_id"}
        assert expect in ucs, f"{table} 冲突键 {cols} 与唯一约束 {ucs} 不一致"


def test_conflict_guard_uses_null_safe_match():
    """守卫必须用 IS NOT DISTINCT FROM——security_identifiers.start_date 等键列可为 NULL。"""
    plan = {
        "type": "MERGE", "figi": "X", "keep_id": 1, "keep_symbol": "a",
        "merge_ids": [2], "merge_symbols": ["b"],
    }
    stmts = generate_merge_sql(plan)
    ident = next(s for s in stmts if s.startswith("UPDATE security_identifiers"))
    assert "IS NOT DISTINCT FROM" in ident
    assert "t2.start_date" in ident
    # 无唯一约束的表不加守卫，整表直迁
    facts = next(s for s in stmts if s.startswith("UPDATE insider_transactions"))
    assert "NOT EXISTS" not in facts


# --------------------------------------------------------------------------- #
# PG 集成：apply_merge 的守卫迁移 + 幂等语义
# --------------------------------------------------------------------------- #

import pytest
from datetime import date
from sqlalchemy import text

from data_models.models import DailyPrice, Security
from scripts.repair_identity import apply_merge


@pytest.mark.integration
def test_apply_merge_migrates_nonconflicting_rows_and_is_idempotent(pg_db):
    """键冲突行留守 husk、非冲突行迁移；重复执行不翻倍、不重复写 MERGE 事件。

    锁定 2026-07-02 生产事故：apply 曾用无守卫整表 UPDATE，一行冲突导致
    husk 全部历史（bk→bny 10502 行日线）静默滞留。"""
    with pg_db.get_session() as s:
        s.add(Security(id=1, symbol="bny", current_symbol="bny", market="US", type="CS",
                       is_active=True, full_refresh_interval=30, composite_figi="BBG-X"))
        s.add(Security(id=2, symbol="bk", current_symbol="bk", market="US", type="CS",
                       is_active=False, full_refresh_interval=30, composite_figi="BBG-X"))
        # keep 已有 6-02；husk 有 6-01（可迁）+ 6-02（冲突，应留守）
        s.add(DailyPrice(security_id=1, date=date(2026, 6, 2), close=10))
        s.add(DailyPrice(security_id=2, date=date(2026, 6, 1), close=9))
        s.add(DailyPrice(security_id=2, date=date(2026, 6, 2), close=9.5))
        s.commit()

    plan = {"type": "MERGE", "figi": "BBG-X", "keep_id": 1, "keep_symbol": "bny",
            "merge_ids": [2], "merge_symbols": ["bk"]}
    assert apply_merge(pg_db, plan) == 1  # 仅 6-01 迁移

    with pg_db.engine.connect() as conn:
        keep_dates = [r.date for r in conn.execute(text(
            "SELECT date FROM daily_prices WHERE security_id = 1 ORDER BY date"))]
        husk_dates = [r.date for r in conn.execute(text(
            "SELECT date FROM daily_prices WHERE security_id = 2"))]
        events = conn.execute(text(
            "SELECT count(*) FROM security_identity_events WHERE event_type = 'MERGE'"
        )).scalar()
    assert keep_dates == [date(2026, 6, 1), date(2026, 6, 2)]
    assert husk_dates == [date(2026, 6, 2)]  # 冲突行留守
    assert events == 1

    # 重跑：0 行迁移、事件不重复
    assert apply_merge(pg_db, plan) == 0
    with pg_db.engine.connect() as conn:
        assert conn.execute(text(
            "SELECT count(*) FROM security_identity_events WHERE event_type = 'MERGE'"
        )).scalar() == 1
