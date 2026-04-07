"""结构化日志配置。"""
from __future__ import annotations

import logging
import sys

_LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"

_initialized = False


def setup_logging(verbose: bool = False) -> None:
    """配置 stdlib logging。verbose=True 时级别设为 DEBUG，否则 INFO。"""
    global _initialized  # noqa: PLW0603
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format=_LOG_FORMAT,
        stream=sys.stderr,
        force=True,
    )
    _initialized = True


def get_logger(name: str) -> logging.Logger:
    """获取指定名称的 logger，若尚未初始化则先调用 setup_logging。"""
    if not _initialized:
        setup_logging()
    return logging.getLogger(name)
