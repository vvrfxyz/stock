"""统一的脚本日志配置，替代各脚本内重复的 setup_logging 实现。"""
import os
import sys

from loguru import logger

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
LOG_FORMAT = (
    "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
    "<level>{level: <8}</level> | "
    "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"
)


def setup_logging(log_name: str) -> None:
    """stderr 输出 INFO 及以上；logs/<log_name>_{time}.log 记录 DEBUG 及以上。"""
    logger.remove()
    logger.add(sys.stderr, level="INFO", format=LOG_FORMAT)
    log_dir = os.path.join(PROJECT_ROOT, "logs")
    os.makedirs(log_dir, exist_ok=True)
    logger.add(
        os.path.join(log_dir, f"{log_name}_{{time}}.log"),
        rotation="10 MB",
        retention="10 days",
        level="DEBUG",
    )
