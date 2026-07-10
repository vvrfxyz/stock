from datetime import date
from types import SimpleNamespace

import pytest

import main as main_module
from main import ScheduledStep, build_scheduled_update_steps
from utils.massive_task import TaskResult


class _FakeTrackingDb:
    """记录 start/finish_task_run 调用的假 DatabaseManager。"""

    def __init__(self):
        self._next_id = 0
        self.task_names = {}
        self.finished = []
        self.closed = False

    def start_task_run(self, run_id, task_name):
        self._next_id += 1
        self.task_names[self._next_id] = task_name
        return self._next_id

    def finish_task_run(self, task_run_id, *, exit_code, error_sample=None, stats=None):
        self.finished.append(
            (self.task_names[task_run_id], exit_code, error_sample, stats)
        )

    def close(self):
        self.closed = True


def _step_names(run_date: date) -> list[str]:
    return [step.name for step in build_scheduled_update_steps(run_date, market="US")]


def test_scheduled_update_runs_daily_tasks_every_day():
    steps = build_scheduled_update_steps(date(2026, 5, 15), market="US")
    names = [step.name for step in steps]

    assert "update_massive_prices" in names
    assert "update_massive_short_data" in names
    assert "update_open_close_summary" in names
    assert "check_data_integrity" in names
    assert names.index("check_data_integrity") > names.index("update_open_close_summary")
    assert "update_massive_actions_recent" in names
    assert names[-1] == "health_report"
    recent_step = next(step for step in steps if step.name == "update_massive_actions_recent")
    assert "--recent-days" in recent_step.args
    assert "--force" not in recent_step.args
    short_step = next(step for step in steps if step.name == "update_massive_short_data")
    assert "--force" not in short_step.args


def test_scheduled_update_staggers_weekly_tasks():
    saturday_names = _step_names(date(2026, 5, 16))
    sunday_names = _step_names(date(2026, 5, 17))

    assert "update_massive_shares" in saturday_names
    assert "update_grouped_daily_recent" in saturday_names
    assert "update_grouped_daily_recent" not in sunday_names
    assert "update_massive_actions" not in saturday_names
    assert "update_massive_actions" in sunday_names
    assert "update_massive_shares" not in sunday_names
    assert saturday_names[-1] == "health_report"
    assert sunday_names[-1] == "health_report"
    assert saturday_names.index("update_minute_bars_weekly") < saturday_names.index("health_report")
    assert "sync_sec_identifiers" in sunday_names
    assert "update_sec_filings_recent" in sunday_names
    assert "update_insider_transactions_recent" in sunday_names
    assert "update_institutional_holdings_recent" in sunday_names
    assert "audit_security_identity" in sunday_names
    assert "update_fx_rates" in sunday_names
    assert "update_risk_free_rates" in sunday_names
    assert "sync_cusip_identifiers" in sunday_names
    assert "sync_openfigi_identifiers" in sunday_names
    assert "sync_openfigi_identifiers" not in saturday_names
    # CUSIP 映射先于 13F 写入，新持仓行才能在写入时拿到 security_id；
    # OpenFIGI 兜底跟在 FTD 桥之后（只查 FTD 仍未覆盖的 CUSIP）、13F 写入之前
    assert sunday_names.index("sync_cusip_identifiers") < sunday_names.index(
        "sync_openfigi_identifiers"
    )
    assert sunday_names.index("sync_openfigi_identifiers") < sunday_names.index(
        "update_institutional_holdings_recent"
    )
    # 兜底步骤不带 --limit：靠缓存表（MATCHED 永不重查）保证周度成本递减
    openfigi_step = next(
        step for step in build_scheduled_update_steps(date(2026, 5, 17), market="US")
        if step.name == "sync_openfigi_identifiers"
    )
    assert "--limit" not in openfigi_step.args
    assert "sync_sec_identifiers" not in saturday_names
    assert "update_insider_transactions_recent" not in saturday_names
    sec_step = next(
        step for step in build_scheduled_update_steps(date(2026, 5, 17), market="US")
        if step.name == "update_sec_filings_recent"
    )
    assert "--since" in sec_step.args
    assert "--all" in sec_step.args
    # insiders 解析依赖 filings 索引先落库，必须排在其后
    sunday_steps = _step_names(date(2026, 5, 17))
    assert sunday_steps.index("update_sec_filings_recent") < sunday_steps.index(
        "update_insider_transactions_recent"
    )


def test_scheduled_update_staggers_monthly_tasks():
    first_tuesday_names = _step_names(date(2026, 6, 2))
    first_wednesday_names = _step_names(date(2026, 6, 3))
    second_tuesday_names = _step_names(date(2026, 6, 9))

    assert "update_massive_events" in first_tuesday_names
    assert "update_massive_details" not in first_tuesday_names
    assert "update_massive_details" in first_wednesday_names
    assert "update_massive_events" not in first_wednesday_names
    assert "update_massive_events" not in second_tuesday_names


def test_scheduled_update_continues_after_step_failure_and_exits_nonzero(monkeypatch):
    monkeypatch.setattr(main_module, "DatabaseManager", _FakeTrackingDb)
    executed = []

    def failing_step(argv=None):
        executed.append("fail")
        raise SystemExit(1)

    def ok_step(argv=None):
        executed.append("ok")

    steps = [
        ScheduledStep("step_fail", failing_step, []),
        ScheduledStep("step_ok", ok_step, []),
    ]
    monkeypatch.setattr(main_module, "build_scheduled_update_steps", lambda run_date, market: steps)

    with pytest.raises(SystemExit) as exc_info:
        main_module.run_scheduled_update(SimpleNamespace(run_date="2026-06-10", market="US"))

    assert executed == ["fail", "ok"]
    assert exc_info.value.code == 1


def test_scheduled_update_exits_zero_when_all_steps_succeed(monkeypatch):
    monkeypatch.setattr(main_module, "DatabaseManager", _FakeTrackingDb)
    executed = []

    steps = [
        ScheduledStep("step_a", lambda argv=None: executed.append("a"), []),
        ScheduledStep("step_b", lambda argv=None: executed.append("b"), []),
    ]
    monkeypatch.setattr(main_module, "build_scheduled_update_steps", lambda run_date, market: steps)

    main_module.run_scheduled_update(SimpleNamespace(run_date="2026-06-10", market="US"))

    assert executed == ["a", "b"]


def test_scheduled_update_passes_stats_to_finish_task_run(monkeypatch):
    fake_db = _FakeTrackingDb()
    monkeypatch.setattr(main_module, "DatabaseManager", lambda: fake_db)

    ok_stats = {"processed": 10, "written": 5, "failed": 0}
    fail_stats = {"processed": 3, "written": 0, "failed": 3}
    steps = [
        ScheduledStep("step_ok", lambda argv=None: TaskResult(0, ok_stats), []),
        ScheduledStep("step_fail", lambda argv=None: TaskResult(1, fail_stats), []),
        ScheduledStep("step_plain", lambda argv=None: 0, []),
    ]
    monkeypatch.setattr(main_module, "build_scheduled_update_steps", lambda run_date, market: steps)

    with pytest.raises(SystemExit) as exc_info:
        main_module.run_scheduled_update(SimpleNamespace(run_date="2026-06-10", market="US"))

    assert exc_info.value.code == 1
    by_name = {name: (exit_code, error, stats) for name, exit_code, error, stats in fake_db.finished}
    assert by_name["step_ok"] == (0, None, ok_stats)
    assert by_name["step_fail"][0] == 1
    assert by_name["step_fail"][2] == fail_stats
    assert by_name["step_plain"] == (0, None, None)
    assert fake_db.closed
