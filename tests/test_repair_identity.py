"""repair_identity 存量修复工具的单元测试。

不依赖 PostgreSQL——验证 plan 生成和 SQL 构建逻辑。
"""
from scripts.repair_identity import generate_merge_sql, TABLES_WITH_SECURITY_ID


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
    }
    assert required.issubset(set(TABLES_WITH_SECURITY_ID))
