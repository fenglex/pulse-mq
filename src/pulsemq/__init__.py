"""PulseMQ v2 — 纯 pub → sub 架构，无 broker。"""

# ---------------------------------------------------------------------------
# Windows 事件循环策略修正（必须在 import pulsemq.publisher/subscriber 之前）
# ---------------------------------------------------------------------------
# pyzmq 的 asyncio 集成不支持 Windows 默认的 ProactorEventLoop，
# 若不切换会抛 RuntimeError 或 SUB 端静默收不到消息。
# 在包导入时设置策略，保证用户零配置可用。
# 必须在任何 asyncio 事件循环被创建之前执行。
import sys as _sys

if _sys.platform == "win32":  # pragma: no cover - 平台相关
    import asyncio as _asyncio

    if hasattr(_asyncio, "WindowsSelectorEventLoopPolicy"):
        _asyncio.set_event_loop_policy(_asyncio.WindowsSelectorEventLoopPolicy())

from pulsemq.publisher import PulsePublisher, PublisherSender
from pulsemq.producers.types import PubData
from pulsemq.subscriber import PulseSubscriber
from pulsemq.protocol.frames import PulseMessage
from pulsemq.config import PublisherConfig, load_config

__all__ = [
    "PulsePublisher",
    "PulseSubscriber",
    "PulseMessage",
    "PublisherConfig",
    "PublisherSender",
    "PubData",
    "load_config",
]
