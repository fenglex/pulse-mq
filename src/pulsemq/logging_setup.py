# src/pulsemq/logging_setup.py
"""loguru 结构化日志初始化 + 生命周期事件规范。"""
from __future__ import annotations

import sys
from pathlib import Path

from loguru import logger

_CONFIGURED = False


def setup_logging(
    level: str = "INFO",
    json: bool = False,
    log_dir: str = "data/logs",
    rotation: str = "1 day",
    retention: str = "30 days",
) -> None:
    """初始化 loguru 日志系统。

    Args:
        level: 日志级别。
        json: JSON 结构格式（默认文本）。
        log_dir: 日志文件目录（默认 ``logs/``）。
        rotation: 日志滚动周期（默认每日）。
        retention: 日志保留时长（默认 30 天）。
    """
    global _CONFIGURED
    logger.remove()
    fmt = (
        "{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {module}:{line} | {message}"
        if not json else "{message}"
    )
    serialize = json
    # stderr sink（交互/容器可见）
    logger.add(sys.stderr, level=level, format=fmt, serialize=serialize, enqueue=False)
    # 文件 sink（自动创建目录 + 每日滚动 + 定期清理）
    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)
    logger.add(
        str(log_path / "pulsemq_{time:YYYY-MM-DD}.log"),
        level=level,
        format=fmt,
        rotation=rotation,
        retention=retention,
        encoding="utf-8",
        enqueue=False,
    )
    _CONFIGURED = True


def log_event(level: str, event_type: str, **fields) -> None:
    """结构化输出一条生命周期事件。event_type ∈ AUTH/CLIENT/..."""
    parts = [f"[{event_type}]"] + [f"{k}={v}" for k, v in fields.items()]
    logger.log(level, " ".join(parts))


__all__ = ["setup_logging", "logger", "log_event"]
