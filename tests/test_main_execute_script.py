import pytest

from main import execute_script


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
