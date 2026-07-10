from types import SimpleNamespace

import pytest

import main as main_module
from main import execute_script
from utils.massive_task import TaskResult


def test_execute_script_propagates_nonzero_system_exit():
    def failing_main(argv=None):
        raise SystemExit(7)

    with pytest.raises(SystemExit) as exc_info:
        execute_script(failing_main, [])

    assert exc_info.value.code == 7


def test_execute_script_turns_nonzero_return_code_into_system_exit():
    def failing_main(argv=None):
        return 5

    with pytest.raises(SystemExit) as exc_info:
        execute_script(failing_main, [])

    assert exc_info.value.code == 5


def test_execute_script_passes_argv_list_to_script_main():
    seen = {}

    def recording_main(argv=None):
        seen["argv"] = argv
        return 0

    execute_script(recording_main, ["--market", "US", "aapl"])

    assert seen["argv"] == ["--market", "US", "aapl"]


def test_execute_script_returns_stats_from_task_result():
    stats = {"processed": 10, "written": 5, "failed": 0}

    def main_with_stats(argv=None):
        return TaskResult(0, stats)

    assert execute_script(main_with_stats, []) == stats


def test_execute_script_unpacks_tuple_result():
    stats = {"written": 2}

    def main_with_tuple(argv=None):
        return 0, stats

    assert execute_script(main_with_tuple, []) == stats


def test_execute_script_returns_none_when_no_stats():
    assert execute_script(lambda argv=None: 0, []) is None
    assert execute_script(lambda argv=None: None, []) is None


def test_execute_script_attaches_stats_to_system_exit_on_failure():
    stats = {"processed": 3, "failed": 3}

    def failing_main(argv=None):
        return TaskResult(1, stats)

    with pytest.raises(SystemExit) as exc_info:
        execute_script(failing_main, [])

    assert exc_info.value.code == 1
    assert getattr(exc_info.value.code, "stats", None) == stats


def test_execute_script_tuple_nonzero_exit_carries_stats():
    stats = {"processed": 4, "failed": 2}

    def failing_main(argv=None):
        return 1, stats

    with pytest.raises(SystemExit) as exc_info:
        execute_script(failing_main, [])

    assert exc_info.value.code == 1
    assert getattr(exc_info.value.code, "stats", None) == stats


def test_run_update_rejects_unsupported_market():
    with pytest.raises(SystemExit) as exc_info:
        main_module.run_update(SimpleNamespace(market="HK", symbols=[]))

    assert exc_info.value.code == 2


def test_run_rebuild_massive_dataset_rejects_unsupported_market():
    with pytest.raises(SystemExit) as exc_info:
        main_module.run_rebuild_massive_dataset(SimpleNamespace(market="HK"))

    assert exc_info.value.code == 2


def test_update_adjustment_factors_cli_forwards_reconciliation_flags(monkeypatch):
    captured = {}
    monkeypatch.setattr(
        main_module, "execute_script",
        lambda main_func, args_list: captured.update(argv=args_list),
    )

    args = main_module.build_parser().parse_args([
        "update_adjustment_factors", "aapl",
        "--changed-since", "3",
        "--fail-on-vendor-mismatch",
        "--max-mismatch-rate", "0.05",
    ])
    args.func(args)

    argv = captured["argv"]
    assert argv[argv.index("--changed-since") + 1] == "3"
    assert "--fail-on-vendor-mismatch" in argv
    assert argv[argv.index("--max-mismatch-rate") + 1] == "0.05"

    # 转发出的 argv 必须能被子脚本自己的 parser 原样接收
    from scripts.update_adjustment_factors import create_parser as adjustment_parser

    forwarded = adjustment_parser().parse_args(argv)
    assert forwarded.changed_since == 3
    assert forwarded.fail_on_vendor_mismatch is True
    assert forwarded.max_mismatch_rate == 0.05
    assert forwarded.symbols == ["aapl"]


def test_update_adjustment_factors_cli_defaults_omit_new_flags(monkeypatch):
    captured = {}
    monkeypatch.setattr(
        main_module, "execute_script",
        lambda main_func, args_list: captured.update(argv=args_list),
    )

    args = main_module.build_parser().parse_args(["update_adjustment_factors", "aapl"])
    args.func(args)

    argv = captured["argv"]
    assert "--changed-since" not in argv
    assert "--fail-on-vendor-mismatch" not in argv
    assert "--max-mismatch-rate" not in argv


def test_update_minute_bars_cli_forwards_args(monkeypatch):
    captured = {}
    monkeypatch.setattr(
        main_module, "execute_script",
        lambda main_func, args_list: captured.update(main_func=main_func, argv=args_list),
    )

    args = main_module.build_parser().parse_args([
        "update_minute_bars", "aapl", "--start", "2026-07-02",
        "--lookback-days", "8", "--workers", "4",
    ])
    args.func(args)

    from scripts.update_minute_bars import create_parser as minute_parser

    forwarded = minute_parser().parse_args(captured["argv"])
    assert captured["main_func"] is main_module.update_minute_bars_main
    assert forwarded.symbols == ["aapl"]
    assert forwarded.start == "2026-07-02"
    assert forwarded.lookback_days == 8
    assert forwarded.workers == 4


def test_sync_openfigi_identifiers_cli_forwards_args(monkeypatch):
    captured = {}
    monkeypatch.setattr(
        main_module, "execute_script",
        lambda main_func, args_list: captured.update(argv=args_list),
    )

    args = main_module.build_parser().parse_args([
        "sync_openfigi_identifiers", "--limit", "100", "--refresh-days", "30",
    ])
    args.func(args)

    argv = captured["argv"]
    assert argv[argv.index("--limit") + 1] == "100"
    assert argv[argv.index("--refresh-days") + 1] == "30"

    # 转发出的 argv 必须能被子脚本自己的 parser 原样接收
    from scripts.sync_openfigi_identifiers import create_parser as openfigi_parser

    forwarded = openfigi_parser().parse_args(argv)
    assert forwarded.limit == 100
    assert forwarded.refresh_days == 30
