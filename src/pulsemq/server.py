"""Broker 启动器：组装各层并启动消息主循环。"""

from __future__ import annotations

import asyncio
import logging
import signal
import sys

from pulsemq.config import BrokerConfig, load_config
from pulsemq.engine.handlers import MessageHandlers
from pulsemq.engine.router import MessageRouter
from pulsemq.event_loop import install_event_loop
from pulsemq.transport.zmq_transport import ZmqTransport

logger = logging.getLogger(__name__)


class PulseServer:
    """PulseMQ Broker 服务器。"""

    def __init__(self, config: BrokerConfig | None = None):
        self._config = config or load_config()
        self._router = MessageRouter()
        self._transport = ZmqTransport(self._config)
        self._handlers = MessageHandlers(
            router=self._router,
            send_fn=self._transport.send,
            broadcast_fn=self._transport.broadcast,
            default_ser=self._config.default_serializer,
            default_comp=self._config.default_compressor,
        )
        self._running = False

    async def start(self) -> None:
        """启动 Broker。"""
        # 设置事件循环
        loop_type = install_event_loop(self._config.use_uvloop)
        logger.info("事件循环: %s", loop_type)

        # 启动 ZMQ transport
        await self._transport.start()
        logger.info(
            "PulseMQ Broker 启动: ROUTER=%s, XPUB=%s",
            self._config.bind, self._config.xpub_bind,
        )

        self._running = True

        # 进入消息主循环
        await self._message_loop()

    async def stop(self) -> None:
        """停止 Broker。"""
        self._running = False
        await self._transport.stop()
        logger.info("PulseMQ Broker 已停止")

    async def _message_loop(self) -> None:
        """Phase 1 简单消息循环：逐条处理。"""
        while self._running:
            try:
                frames = await self._transport.recv()
                await self._handlers.dispatch(frames)
            except zmq.ZMQError:
                if self._running:
                    logger.exception("ZMQ 错误")
                break
            except Exception:
                logger.exception("消息处理异常")
                continue


def main() -> None:
    """CLI 入口: pulse-mq 命令。"""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    config = load_config()
    server = PulseServer(config)

    loop = asyncio.new_event_loop()

    def _shutdown():
        loop.call_soon_threadsafe(lambda: asyncio.ensure_future(server.stop()))

    # 信号处理
    if sys.platform != "win32":
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, _shutdown)

    try:
        loop.run_until_complete(server.start())
    except KeyboardInterrupt:
        pass
    finally:
        loop.run_until_complete(server.stop())
        loop.close()


if __name__ == "__main__":
    main()
