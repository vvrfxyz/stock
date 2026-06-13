"""scripts/update_massive_details.ensure_missing_symbols_exist 的 PG 集成测试。

锁定：只把"活跃行"算作已存在；某 symbol 仅以退市(inactive)行存在时，复用该代码
重新上市的新证券仍应被当作 missing 并插入（与 active-only 部分唯一索引一致）。
"""
from unittest.mock import Mock

import pytest

from data_models.models import Security
import scripts.update_massive_details as details

pytestmark = pytest.mark.integration


def _add_security(session, sid, symbol, is_active):
    session.add(Security(
        id=sid, symbol=symbol, current_symbol=symbol, market="US", type="CS",
        is_active=is_active, full_refresh_interval=30,
    ))


def test_delisted_only_symbol_is_treated_as_missing(pg_db):
    with pg_db.get_session() as s:
        _add_security(s, 1, "old", is_active=False)   # 仅退市行存在
        _add_security(s, 2, "live", is_active=True)   # 活跃行存在
        s.commit()

    source = Mock()
    source.get_security_info.return_value = {
        "symbol": "old", "name": "Reborn Co", "type": "CS", "market": "US",
        "composite_figi": "BBG-REBORN",
    }

    inserted = details.ensure_missing_symbols_exist(pg_db, source, ["old", "live"])

    # 'live' 是活跃已存在 -> 不查；'old' 只剩退市行 -> 视为 missing 并插入新活跃行
    assert inserted == 1
    source.get_security_info.assert_called_once_with("old")
    with pg_db.get_session() as s:
        active_old = s.query(Security).filter(
            Security.symbol == "old", Security.is_active.is_(True)
        ).count()
        assert active_old == 1
