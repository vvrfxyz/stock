"""utils.script_logging 的主日志/子脚本 sink 生命周期测试。

模块有进程级状态，用 importlib.reload + 临时 PROJECT_ROOT 隔离每个用例。
"""
import importlib
import os

import pytest
from loguru import logger


@pytest.fixture()
def fresh_logging(tmp_path, monkeypatch):
    import utils.script_logging as script_logging

    module = importlib.reload(script_logging)
    monkeypatch.setattr(module, "PROJECT_ROOT", str(tmp_path))
    yield module, tmp_path
    logger.remove()
    importlib.reload(script_logging)


def _log_files(tmp_path):
    log_dir = tmp_path / "logs"
    return sorted(p.name for p in log_dir.glob("*.log")) if log_dir.exists() else []


def _write_and_flush(message):
    logger.info(message)
    logger.complete()


def test_primary_sink_persists_across_subscript_calls(fresh_logging):
    module, tmp_path = fresh_logging
    module.setup_logging("main_controller")     # 主日志
    module.setup_logging("update_prices")       # 子脚本

    _write_and_flush("hello from sub")

    files = _log_files(tmp_path)
    assert any(f.startswith("main_controller_") for f in files)
    assert any(f.startswith("update_prices_") for f in files)
    # 主日志文件必须也收到子脚本期间的消息
    primary = next(p for p in (tmp_path / "logs").glob("main_controller_*.log"))
    assert "hello from sub" in primary.read_text()


def test_subscript_sinks_are_swapped_not_accumulated(fresh_logging):
    module, tmp_path = fresh_logging
    module.setup_logging("main_controller")
    module.setup_logging("step_one")
    module.setup_logging("step_two")

    _write_and_flush("during step two")

    step_one = next((tmp_path / "logs").glob("step_one_*.log"))
    step_two = next((tmp_path / "logs").glob("step_two_*.log"))
    # step_one 的 sink 已被摘除，不应再收到消息
    assert "during step two" not in step_one.read_text()
    assert "during step two" in step_two.read_text()


def test_repeat_setup_same_name_does_not_duplicate_sinks(fresh_logging):
    module, tmp_path = fresh_logging
    module.setup_logging("main_controller")
    module.setup_logging("main_controller")
    module.setup_logging("step")
    module.setup_logging("step")

    assert len([f for f in _log_files(tmp_path) if f.startswith("main_controller_")]) == 1
    assert len([f for f in _log_files(tmp_path) if f.startswith("step_")]) == 1


def test_standalone_script_is_its_own_primary(fresh_logging):
    module, tmp_path = fresh_logging
    module.setup_logging("update_prices")  # 独立运行：自己就是主日志

    _write_and_flush("standalone")

    files = _log_files(tmp_path)
    assert len(files) == 1 and files[0].startswith("update_prices_")


def test_traceback_does_not_annotate_local_variable_values(fresh_logging):
    module, tmp_path = fresh_logging
    module.setup_logging("main_controller")

    try:
        api_key = "plain-secret-value"
        api_key + 1  # TypeError；diagnose=True 时 loguru 会把 api_key 的值标注进 traceback
    except TypeError as e:
        logger.opt(exception=e).error("boom")
    logger.complete()

    primary = next((tmp_path / "logs").glob("main_controller_*.log"))
    content = primary.read_text()
    assert "Traceback" in content                 # backtrace 保留
    assert "plain-secret-value" not in content    # diagnose 关闭：变量值不落日志


def test_all_sinks_have_diagnose_disabled(fresh_logging):
    module, _ = fresh_logging
    module.setup_logging("main_controller")   # stderr + 主文件 sink
    module.setup_logging("sub_step")          # 子脚本文件 sink

    handlers = list(logger._core.handlers.values())
    assert len(handlers) == 3
    assert all(handler._exception_formatter._diagnose is False for handler in handlers)
