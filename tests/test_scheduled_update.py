from datetime import date
from types import SimpleNamespace

import pytest

import main as main_module
from main import ScheduledStep, build_scheduled_update_steps


def _step_names(run_date: date) -> list[str]:
    return [step.name for step in build_scheduled_update_steps(run_date, market="US")]


def test_scheduled_update_runs_daily_tasks_every_day():
    steps = build_scheduled_update_steps(date(2026, 5, 15), market="US")
    names = [step.name for step in steps]

    assert "update_massive_prices" in names
    assert "update_massive_short_data" in names
    assert "update_open_close_summary" in names
    assert "update_massive_actions_recent" in names
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
    assert "sync_sec_identifiers" in sunday_names
    assert "update_sec_filings_recent" in sunday_names
    assert "update_insider_transactions_recent" in sunday_names
    assert "update_institutional_holdings_recent" in sunday_names
    assert "update_fx_rates" in sunday_names
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
    executed = []

    steps = [
        ScheduledStep("step_a", lambda argv=None: executed.append("a"), []),
        ScheduledStep("step_b", lambda argv=None: executed.append("b"), []),
    ]
    monkeypatch.setattr(main_module, "build_scheduled_update_steps", lambda run_date, market: steps)

    main_module.run_scheduled_update(SimpleNamespace(run_date="2026-06-10", market="US"))

    assert executed == ["a", "b"]
