from types import SimpleNamespace

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from data_models.models import Base, Security
from scripts.update_open_close_summary import get_security_scope


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
