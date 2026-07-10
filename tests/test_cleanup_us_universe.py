from datetime import date

import pytest
from sqlalchemy import text

from data_models.models import (
    DailyPrice,
    NewsArticleInsight,
    SecFiling,
    Security,
    SecurityIdentifier,
    SecurityIdentityEvent,
)
from scripts.cleanup_us_universe import collect_related_counts, run_apply

pytestmark = pytest.mark.integration


def test_cleanup_deletes_owned_rows_and_unlinks_source_facts(pg_db):
    with pg_db.get_session() as session:
        session.add_all([
            Security(id=1, symbol="bad", current_symbol="bad", market="US", type="WARRANT", is_active=False),
            Security(id=2, symbol="keep", current_symbol="keep", market="US", type="CS", is_active=True),
        ])
        session.flush()
        session.add_all([
            DailyPrice(security_id=1, date=date(2026, 1, 2), close=1),
            SecurityIdentifier(security_id=1, id_type="CIK", id_value="1", source="SEC"),
            SecurityIdentityEvent(security_id=1, event_type="MANUAL", resolution_source="MANUAL"),
            SecurityIdentityEvent(security_id=2, related_security_id=1, event_type="MERGE", resolution_source="MANUAL"),
            SecFiling(security_id=1, source="SEC", form_type="10-K", accession_number="a",
                      filing_date=date(2025, 1, 1)),
            NewsArticleInsight(source_article_id="n1", security_id=1, ticker="BAD"),
        ])
        session.commit()

    counts = collect_related_counts(pg_db, [1])
    assert counts["daily_prices"] == 1
    assert counts["sec_filings"] == 1

    deleted = run_apply(pg_db, [1])
    assert deleted["securities"] == 1
    with pg_db.engine.connect() as conn:
        assert conn.execute(text("SELECT count(*) FROM securities WHERE id=1")).scalar() == 0
        assert conn.execute(text("SELECT count(*) FROM daily_prices WHERE security_id=1")).scalar() == 0
        assert conn.execute(text("SELECT security_id FROM sec_filings WHERE accession_number='a'")).scalar() is None
        assert conn.execute(text("SELECT security_id FROM news_article_insights WHERE source_article_id='n1'")).scalar() is None
        assert conn.execute(text("SELECT related_security_id FROM security_identity_events WHERE security_id=2")).scalar() is None
