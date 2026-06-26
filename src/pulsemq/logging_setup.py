# src/pulsemq/logging_setup.py
"""loguru 结构化日志初始化 + 生命周期事件规范。"""
from __future__ import annotations

import sys

from loguru import logger

_CONFIGURED = False


def setup_logging(level: str = "INFO", json: bool = False) -> None:
    global _CONFIGURED
    logger.remove()
    fmt = (
        "{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {module}:{line} | {message}"
        if not json else "{message}"
    )
    serialize = json
    logger.add(sys.stderr, level=level, format=fmt, serialize=serialize, enqueue=False)
    _CONFIGURED = True


def log_event(level: str, event_type: str, **fields) -> None:
    """结构化输出一条生命周期事件。event_type ∈ AUTH/CLIENT/..."""
    parts = [f"[{event_type}]"] + [f"{k}={v}" for k, v in fields.items()]
    logger.log(level, " ".join(parts))


__all__ = ["setup_logging", "logger", "log_event"]
