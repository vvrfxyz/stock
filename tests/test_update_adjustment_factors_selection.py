"""scripts/update_adjustment_factors.get_securities_to_update 增量选取的 PG 集成测试。

锁定 --changed-since 按 corporate_actions.updated_at 判定（真正的事件变化），
而非 securities.actions_last_updated_at 水位线（每日全量刷新会让增量失效）；
并锁定 ex_date 刚生效的事件确定性触发重算（预告分红因子的自愈不依赖
upsert 无条件刷新 updated_at 的副作用）。
"""
from argparse import Namespace
from datetime import date, datetime, timedelta, timezone

import pytest
from sqlalchemy import text

from data_models.models import CorporateAction, Security
import scripts.update_adjustment_factors as uaf

pytestmark = pytest.mark.integration


def _args(**kw):
    base = dict(market="US", symbols=[], limit=0, source="MASSIVE", changed_since=0)
    base.update(kw)
    return Namespace(**base)


def _add_security(session, sid, symbol, **kw):
    session.add(Security(
        id=sid, symbol=symbol, current_symbol=symbol, market="US", type="CS",
        is_active=kw.get("is_active", True), full_refresh_interval=30,
        actions_last_updated_at=kw.get("actions_last_updated_at"),
    ))


def _add_action(session, sid, ex_date, updated_at):
    session.add(CorporateAction(
        security_id=sid, action_type="DIVIDEND", ex_date=ex_date,
        cash_amount=1, currency="USD", source="MASSIVE",
        source_event_id=f"ev-{sid}-{ex_date}",
    ))
    session.flush()
    # server_default 会把 updated_at 设成 now()，显式回写到目标时间
    session.execute(
        text("UPDATE corporate_actions SET updated_at = :ts WHERE security_id = :sid"),
        {"ts": updated_at, "sid": sid},
    )


def test_changed_since_uses_event_updated_at_not_watermark(pg_db):
    now = datetime.now(timezone.utc)
    old = now - timedelta(days=30)
    with pg_db.get_session() as s:
        # A: 水位线很新，但事件很旧 -> 不应被选（证明不是看水位线）
        _add_security(s, 1, "aaa", actions_last_updated_at=now)
        _add_action(s, 1, date(2025, 1, 2), updated_at=old)
        # B: 水位线很旧，但事件最近修订过 -> 应被选
        _add_security(s, 2, "bbb", actions_last_updated_at=old)
        _add_action(s, 2, date(2025, 1, 3), updated_at=now)
        # C: 无任何事件 -> 不应被选
        _add_security(s, 3, "ccc", actions_last_updated_at=now)
        s.commit()

    selected = {sec.symbol for sec in uaf.get_securities_to_update(pg_db, _args(changed_since=3))}
    assert selected == {"bbb"}


def test_changed_since_selects_recently_effective_ex_date(pg_db):
    """ex_date 刚过而 updated_at 陈旧的证券必须被确定性选中：预告分红按公告价折的
    因子须在 ex_date 生效后用真实前收盘重算，不能依赖 upsert 刷新 updated_at 的副作用。"""
    now = datetime.now(timezone.utc)
    old = now - timedelta(days=30)
    today = date.today()
    with pg_db.get_session() as s:
        # D: 事件陈旧但 ex_date 昨天刚生效 -> 必须被选
        _add_security(s, 1, "ddd")
        _add_action(s, 1, today - timedelta(days=1), updated_at=old)
        # E: 事件陈旧且 ex_date 仍在未来 -> 生效前不触发重算
        _add_security(s, 2, "eee")
        _add_action(s, 2, today + timedelta(days=10), updated_at=old)
        # F: 事件陈旧且 ex_date 早已滑出窗口 -> 不应被选
        _add_security(s, 3, "fff")
        _add_action(s, 3, today - timedelta(days=20), updated_at=old)
        s.commit()

    selected = {sec.symbol for sec in uaf.get_securities_to_update(pg_db, _args(changed_since=3))}
    assert selected == {"ddd"}


def test_no_changed_since_selects_all_reserved_types(pg_db):
    with pg_db.get_session() as s:
        _add_security(s, 1, "aaa")
        _add_security(s, 2, "bbb", is_active=False)  # 含 inactive（退市股因子缺口）
        s.commit()
    selected = {sec.symbol for sec in uaf.get_securities_to_update(pg_db, _args())}
    assert selected == {"aaa", "bbb"}
