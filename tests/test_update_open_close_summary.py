from types import SimpleNamespace
from datetime import date
from unittest.mock import Mock

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from data_models.models import Base, Security
from scripts.update_open_close_summary import get_security_scope, process_security_date


class _TestDatabaseManager:
    def __init__(self):
        self.engine = create_engine("sqlite:///:memory:")
        Security.__table__.create(self.engine)
        self.session_factory = sessionmaker(bind=self.engine)

    def get_session(self):
        return self.session_factory()


def test_get_security_scope_applies_order_before_limit():
    db_manager = _TestDatabaseManager()
    session = db_manager.session_factory()
    session.add_all(
        [
            Security(id=1, symbol="b", current_symbol="b", market="US", type="CS", is_active=True),
            Security(id=2, symbol="a", current_symbol="a", market="US", type="CS", is_active=True),
        ]
    )
    session.commit()
    session.close()

    scope = get_security_scope(
        db_manager,
        SimpleNamespace(symbols=[], market="US", all=False, limit=1),
    )

    assert scope == {2: "a"}


def test_process_security_date_only_includes_non_none_fields():
    """vendor 返回 preMarket=None 时，payload 不应包含 pre_market 键，
    避免 upsert 把已有的非空值覆盖成 NULL。"""
    source = Mock()
    source.get_open_close_data.return_value = {
        "preMarket": None,
        "afterHours": 165.50,
        "otc": None,
    }
    symbol, status, row = process_security_date(1, "aapl", date(2026, 6, 20), source)
    assert status == "SUCCESS"
    assert "pre_market" not in row
    assert "otc" not in row
    assert row["after_hours"] == 165.50
    assert row["security_id"] == 1
    assert row["date"] == date(2026, 6, 20)


def test_process_security_date_includes_both_when_present():
    source = Mock()
    source.get_open_close_data.return_value = {
        "preMarket": 160.00,
        "afterHours": 165.50,
    }
    symbol, status, row = process_security_date(1, "aapl", date(2026, 6, 20), source)
    assert status == "SUCCESS"
    assert row["pre_market"] == 160.00
    assert row["after_hours"] == 165.50


def test_process_security_date_skips_when_both_none():
    source = Mock()
    source.get_open_close_data.return_value = {
        "preMarket": None,
        "afterHours": None,
    }
    symbol, status, row = process_security_date(1, "aapl", date(2026, 6, 20), source)
    assert status == "SKIP_NO_SESSION_DATA"
    assert row is None
