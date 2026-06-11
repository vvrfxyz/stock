import pytest

from main import execute_script


def test_execute_script_propagates_nonzero_system_exit():
    def failing_main():
        raise SystemExit(7)

    with pytest.raises(SystemExit) as exc_info:
        execute_script(failing_main, [])

    assert exc_info.value.code == 7


def test_execute_script_turns_nonzero_return_code_into_system_exit():
    def failing_main():
        return 5

    with pytest.raises(SystemExit) as exc_info:
        execute_script(failing_main, [])

    assert exc_info.value.code == 5
