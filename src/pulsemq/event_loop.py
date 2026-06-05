"""事件循环选择：Linux/macOS 下使用 uvloop，Windows 自动回退 asyncio。"""

from __future__ import annotations


def install_event_loop(use_uvloop: bool = True) -> str:
    """安装高性能事件循环。

    Returns:
        "uvloop" 或 "asyncio"，表示实际使用的事件循环。
    """
    if not use_uvloop:
        return "asyncio"

    try:
        import uvloop
        uvloop.install()
        return "uvloop"
    except ImportError:
        return "asyncio"
