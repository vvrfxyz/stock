"""统一的脚本日志配置，替代各脚本内重复的 setup_logging 实现。

进程内首次 setup_logging 的调用方成为"主日志"（控制器或独立运行的脚本），
其 stderr/文件 sink 全程保留；后续以其它名字调用的视为子脚本，子脚本之间
互相替换各自的文件 sink。这样 main.py 顺序调度多个脚本时：
- 控制器日志文件覆盖完整运行时间线（含子脚本输出）；
- 每个子脚本仍各有独立日志文件；
- 不需要调用方在每步之后手工恢复 sink。
"""
import os
import sys

from loguru import logger

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
LOG_FORMAT = (
    "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
    "<level>{level: <8}</level> | "
    "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"
)

_console_ready = False
_primary_log_name: str | None = None
_script_sink: tuple[str, int] | None = None


def _add_file_sink(log_name: str) -> int:
    log_dir = os.path.join(PROJECT_ROOT, "logs")
    os.makedirs(log_dir, exist_ok=True)
    return logger.add(
        os.path.join(log_dir, f"{log_name}_{{time}}.log"),
        rotation="10 MB",
        retention="10 days",
        level="DEBUG",
    )


def setup_logging(log_name: str) -> None:
    """stderr 输出 INFO 及以上；logs/<log_name>_{time}.log 记录 DEBUG 及以上。"""
    global _console_ready, _primary_log_name, _script_sink

    if not _console_ready:
        logger.remove()
        logger.add(sys.stderr, level="INFO", format=LOG_FORMAT)
        _console_ready = True

    if _primary_log_name is None:
        _primary_log_name = log_name
        _add_file_sink(log_name)
        return
    if log_name == _primary_log_name:
        return
    if _script_sink is not None:
        if _script_sink[0] == log_name:
            return
        logger.remove(_script_sink[1])
    _script_sink = (log_name, _add_file_sink(log_name))
