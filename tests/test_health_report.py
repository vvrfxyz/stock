from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from loguru import logger

from scripts.health_report import (
    report_institutional_holdings_completeness,
    report_market_data_freshness,
    report_pipeline_runs,
)

# 固定"今天"，避免用例随日历漂移：所有 2026-03-31 及更早的 period 申报截止均已过
TODAY = date(2026, 7, 2)


class _Result:
    def __init__(self, rows):
        self._rows = rows

    def all(self):
        return self._rows

    def scalar(self):
        return self._rows


class _Session:
    """假 Session：按 execute 调用顺序依次弹出预置结果集。"""

    def __init__(self, *results):
        self._results = list(results)

    def execute(self, _sql, _params=None):
        return _Result(self._results.pop(0))


def _capture_logs():
    messages: list[str] = []
    handler_id = logger.add(messages.append, format="{message}", level="INFO")
    return messages, handler_id


def _configure_clickhouse(monkeypatch):
    monkeypatch.setenv("CLICKHOUSE_URL", "http://clickhouse")
    monkeypatch.setenv("CLICKHOUSE_USER", "default")
    monkeypatch.setenv("CLICKHOUSE_PASSWORD", "test")


# ---------------------------------------------------------------------------
# 13F 覆盖：阈值分支（假 Session）
# ---------------------------------------------------------------------------

def test_institutional_holdings_completeness_ok():
    rows = [
        (date(2025, 12, 31), 9000, 3_400_000, 3_100_000, 91.0),
        (date(2025, 9, 30), 8400, 3_200_000, 2_900_000, 90.0),
        (date(2025, 6, 30), 8300, 3_100_000, 2_800_000, 89.0),
        (date(2025, 3, 31), 8200, 3_000_000, 2_700_000, 88.0),
    ]

    assert report_institutional_holdings_completeness(_Session(rows), today=TODAY) == 0


def test_institutional_holdings_completeness_warns_on_sparse_period():
    rows = [
        (date(2025, 12, 31), 9000, 3_400_000, 3_100_000, 91.0),
        (date(2025, 9, 30), 177, 150_000, 140_000, 90.0),
    ]

    assert report_institutional_holdings_completeness(_Session(rows), today=TODAY) == 1


def test_institutional_holdings_completeness_warns_on_low_mapping():
    rows = [
        (date(2025, 12, 31), 9000, 3_400_000, 2_000_000, 58.8),
    ]

    assert report_institutional_holdings_completeness(_Session(rows), today=TODAY) == 1


def test_in_flight_quarter_is_info_only():
    # 2026-06-30 申报截止（+60 天）未过：filings 远低于阈值也只展示不告警
    rows = [
        (date(2026, 6, 30), 12, 5_000, 4_500, 90.0),
        (date(2026, 3, 31), 9000, 3_400_000, 3_100_000, 91.0),
    ]

    assert report_institutional_holdings_completeness(_Session(rows), today=TODAY) == 0


def test_malformed_period_is_ignored():
    # EDGAR 畸形 period（非标准季末）不参与阈值判定，filings=1 不产生 P1
    rows = [
        (date(2026, 3, 31), 9000, 3_400_000, 3_100_000, 91.0),
        (date(2025, 11, 15), 1, 3, 3, 100.0),
    ]

    assert report_institutional_holdings_completeness(_Session(rows), today=TODAY) == 0


# ---------------------------------------------------------------------------
# Pipeline runs：RUNNING 僵尸（假 Session）
# ---------------------------------------------------------------------------

def test_pipeline_runs_counts_stuck_running_as_p1():
    grouped = [
        ("update_massive_prices", "SUCCESS", 7, None, datetime(2026, 7, 1, 10, 30)),
        ("update_massive_news", "RUNNING", 1, None, None),
    ]
    stuck = [
        ("update_massive_news", "run-20260701", datetime(2026, 7, 1, 9, 0)),
    ]
    failures = []

    assert report_pipeline_runs(_Session(grouped, stuck, failures), days=7) == 1


def test_pipeline_runs_all_green_without_zombies():
    grouped = [
        ("update_massive_prices", "SUCCESS", 7, None, datetime(2026, 7, 1, 10, 30)),
    ]

    assert report_pipeline_runs(_Session(grouped, [], []), days=7) == 0


def test_pipeline_runs_recovered_failure_is_history_not_p1():
    grouped = [
        ("update_massive_prices", "FAILED", 1, None, datetime(2026, 7, 1, 10, 30)),
        ("update_massive_prices", "SUCCESS", 1, None, datetime(2026, 7, 2, 10, 30)),
    ]

    assert report_pipeline_runs(_Session(grouped, [], []), days=7) == 0


def test_pipeline_runs_latest_failure_is_p1():
    grouped = [
        ("update_massive_prices", "FAILED", 1, None, datetime(2026, 7, 2, 10, 30)),
    ]
    latest_failures = [
        ("update_massive_prices", "api down", datetime(2026, 7, 2, 9, 0)),
    ]

    assert report_pipeline_runs(_Session(grouped, [], latest_failures), days=7) == 1


def test_market_data_freshness_allows_one_session_lag(monkeypatch):
    import scripts.health_report as health

    seen = {}
    _configure_clickhouse(monkeypatch)

    class Response:
        status_code = 200
        text = "2026-07-08\n"

    monkeypatch.setattr(health, "get_last_completed_trading_date", lambda market: date(2026, 7, 9))
    monkeypatch.setattr(
        health,
        "shift_trading_date",
        lambda market, day, sessions: date(2026, 7, 8) if sessions == -1 else date(2026, 7, 2),
    )
    def post(*args, **kwargs):
        seen["sql"] = kwargs["data"].decode()
        return Response()

    monkeypatch.setattr("requests.post", post)

    assert report_market_data_freshness(_Session(date(2026, 7, 8)), "US") == 0
    assert "WHERE ts >= toDateTime('2026-07-02 00:00:00'" in seen["sql"]


def test_market_data_freshness_warns_each_stale_store(monkeypatch):
    import scripts.health_report as health

    _configure_clickhouse(monkeypatch)

    class Response:
        status_code = 200
        text = "2026-07-01\n"

    monkeypatch.setattr(health, "get_last_completed_trading_date", lambda market: date(2026, 7, 9))
    monkeypatch.setattr(
        health,
        "shift_trading_date",
        lambda market, day, sessions: date(2026, 7, 8) if sessions == -1 else date(2026, 7, 2),
    )
    monkeypatch.setattr("requests.post", lambda *a, **kw: Response())

    assert report_market_data_freshness(_Session(date(2026, 7, 7)), "US") == 2


def test_market_data_freshness_counts_clickhouse_failure(monkeypatch):
    import scripts.health_report as health

    _configure_clickhouse(monkeypatch)

    class Response:
        status_code = 503
        text = "unavailable"

    monkeypatch.setattr(health, "get_last_completed_trading_date", lambda market: date(2026, 7, 9))
    monkeypatch.setattr(
        health,
        "shift_trading_date",
        lambda market, day, sessions: date(2026, 7, 8) if sessions == -1 else date(2026, 7, 2),
    )
    monkeypatch.setattr("requests.post", lambda *a, **kw: Response())

    assert report_market_data_freshness(_Session(date(2026, 7, 8)), "US") == 1


def test_market_data_freshness_counts_missing_clickhouse_credentials(monkeypatch):
    import scripts.health_report as health

    monkeypatch.setenv("CLICKHOUSE_URL", "http://clickhouse")
    for name in (
        "RESEARCH_CLICKHOUSE_USER",
        "RESEARCH_CLICKHOUSE_PASSWORD",
        "CLICKHOUSE_USER",
        "CLICKHOUSE_PASSWORD",
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(health, "get_last_completed_trading_date", lambda market: date(2026, 7, 9))
    monkeypatch.setattr(
        health,
        "shift_trading_date",
        lambda market, day, sessions: date(2026, 7, 8) if sessions == -1 else date(2026, 7, 2),
    )

    assert report_market_data_freshness(_Session(date(2026, 7, 8)), "US") == 1


def test_market_data_freshness_counts_invalid_clickhouse_date(monkeypatch):
    import scripts.health_report as health

    _configure_clickhouse(monkeypatch)
    monkeypatch.setattr(health, "get_last_completed_trading_date", lambda market: date(2026, 7, 9))
    monkeypatch.setattr(
        health,
        "shift_trading_date",
        lambda market, day, sessions: date(2026, 7, 8) if sessions == -1 else date(2026, 7, 2),
    )
    monkeypatch.setattr(
        "requests.post",
        lambda *args, **kwargs: SimpleNamespace(status_code=200, text="not-a-date\n"),
    )

    assert report_market_data_freshness(_Session(date(2026, 7, 8)), "US") == 1


# ---------------------------------------------------------------------------
# PostgreSQL 集成：锁定真实 SQL 可执行且计数正确（防列名漂移静默失效）
# ---------------------------------------------------------------------------

@pytest.mark.integration
class TestHealthReportPg:
    def _insert_holdings(self, session):
        from data_models.models import InstitutionalHolding, Security

        session.add(Security(
            id=1, symbol="aapl", current_symbol="aapl",
            market="US", type="CS", is_active=True,
        ))

        def holding(accession, row_hash, period, security_id=None):
            return InstitutionalHolding(
                source="SEC_13F",
                accession_number=accession,
                source_row_hash=row_hash,
                filer_cik="0000102909",
                period=period,
                security_id=security_id,
            )

        # 标准季末且截止已过：2 filings / 4 rows / 3 mapped -> 75.00%，双双低于阈值
        session.add_all([
            holding("A-1", "h1", date(2025, 12, 31), security_id=1),
            holding("A-1", "h2", date(2025, 12, 31), security_id=1),
            holding("A-2", "h3", date(2025, 12, 31), security_id=1),
            holding("A-2", "h4", date(2025, 12, 31)),
            # 在途季度：只展示，不计阈值
            holding("B-1", "h5", date(2026, 6, 30), security_id=1),
            # 畸形 period：SQL 层直接排除，不占 LIMIT 4 名额
            holding("C-1", "h6", date(2025, 11, 15), security_id=1),
            # NULL period：排除
            holding("D-1", "h7", None),
        ])
        session.commit()

    def test_institutional_holdings_sql_executes_and_counts(self, pg_db):
        with pg_db.get_session() as session:
            self._insert_holdings(session)

            messages, handler_id = _capture_logs()
            try:
                result = report_institutional_holdings_completeness(session, today=TODAY)
            finally:
                logger.remove(handler_id)

        joined = "\n".join(messages)
        assert result == 1
        assert "2025-11-15" not in joined
        assert "mapped_pct=75.00%" in joined
        assert "在途季度" in joined

    def test_pipeline_runs_sql_counts_stuck_running(self, pg_db):
        from data_models.models import PipelineTaskRun

        now = datetime.now(timezone.utc)
        with pg_db.get_session() as session:
            session.add_all([
                PipelineTaskRun(
                    run_id="run-a", task_name="update_massive_prices",
                    started_at=now - timedelta(hours=3),
                    ended_at=now - timedelta(hours=2),
                    exit_code=0, status="SUCCESS",
                ),
                # 刚起跑的 RUNNING：正常
                PipelineTaskRun(
                    run_id="run-b", task_name="update_massive_news",
                    started_at=now - timedelta(hours=2), status="RUNNING",
                ),
                # 停留 RUNNING 超过 12 小时：进程被 kill，计 P1
                PipelineTaskRun(
                    run_id="run-c", task_name="update_massive_shares",
                    started_at=now - timedelta(hours=26), status="RUNNING",
                ),
            ])
            session.commit()

            assert report_pipeline_runs(session, days=7) == 1
