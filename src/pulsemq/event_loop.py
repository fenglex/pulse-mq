"""事件循环选择：自动检测平台，优先使用 uvloop。

- Linux/macOS + uvloop 已安装 → uvloop（性能提升 2-4x）
- Linux/macOS + uvloop 未安装 → asyncio（打印警告）
- Windows → asyncio + WindowsSelectorEventLoopPolicy（pyzmq 必需）
"""

from __future__ import annotations

import asyncio
import logging
import sys

logger = logging.getLogger(__name__)


def install_event_loop(use_uvloop: bool = True) -> str:
    """安装高性能事件循环。

    必须在 asyncio.new_event_loop() 或 asyncio.run() 之前调用。

    Returns:
        "uvloop" 或 "asyncio"，表示实际使用的事件循环。
    """
    # Windows 强制使用 SelectorEventLoop（pyzmq 不支持 ProactorEventLoop）
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        logger.info("Windows 平台: 使用 asyncio SelectorEventLoop")
        return "asyncio"

    # 非 Windows 平台：尝试 uvloop
    if not use_uvloop:
        logger.info("uvloop 已禁用 (use_uvloop=False), 使用 asyncio")
        return "asyncio"

    try:
        import uvloop
        uvloop.install()
        logger.info(
            "uvloop %s 已激活 (高性能事件循环)",
            uvloop.__version__,
        )
        return "uvloop"
    except ImportError:
        logger.warning(
            "uvloop 未安装，使用标准 asyncio。"
            "安装 uvloop 可获得 2-4x 性能提升: pip install uvloop"
        )
        return "asyncio"


def get_event_loop_info() -> dict:
    """返回当前事件循环信息（用于监控和诊断）。"""
    info = {"platform": sys.platform}
    try:
        loop = asyncio.get_event_loop()
        info["loop_type"] = type(loop).__name__
    except RuntimeError:
        info["loop_type"] = "none"
    try:
        import uvloop
        info["uvloop_available"] = True
        info["uvloop_version"] = uvloop.__version__
    except ImportError:
        info["uvloop_available"] = False
    return info
