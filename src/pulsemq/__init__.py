"""PulseMQ v2 — Client/Server 模型消息系统（重构中）。"""

import sys as _sys

if _sys.platform == "win32":  # pragma: no cover - 平台相关
    import asyncio as _asyncio
    if hasattr(_asyncio, "WindowsSelectorEventLoopPolicy"):
        _asyncio.set_event_loop_policy(_asyncio.WindowsSelectorEventLoopPolicy())

from pulsemq._version import __version__

__all__ = ["__version__"]
