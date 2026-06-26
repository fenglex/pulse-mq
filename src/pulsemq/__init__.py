"""PulseMQ v2 — Client/Server 模型消息系统。"""
import sys

if sys.platform == "win32":  # pragma: no cover - 平台相关
    import asyncio
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

from pulsemq._version import __version__
from pulsemq.client import Client, ConsumerClient, ProducerClient
from pulsemq.producers.types import PubData
from pulsemq.protocol.frames import PulseMessage
from pulsemq.server import Server

__all__ = [
    "Client", "ProducerClient", "ConsumerClient", "Server",
    "PulseMessage", "PubData", "__version__",
]
