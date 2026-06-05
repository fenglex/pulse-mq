"""Broker 启动器：组装各层并启动消息主循环。"""

from __future__ import annotations

import asyncio
import logging
import signal
import sys

from pulsemq.auth.memory_store import AuthMemoryStore
from pulsemq.auth.permission import PermissionService
from pulsemq.auth.zap_handler import PulseMQZAPHandler
from pulsemq.config import BrokerConfig, load_config
from pulsemq.engine.engine import Engine
from pulsemq.engine.handlers import MessageHandlers
from pulsemq.engine.pipeline import (
    AuthInterceptor,
    InterceptorChain,
    MonitorInterceptor,
    PermissionInterceptor,
)
from pulsemq.engine.router import MessageRouter
from pulsemq.event_loop import install_event_loop
from pulsemq.storage.database import init_db, parse_db_url
from pulsemq.storage.sqlite_perm import SqlitePermGroupRepo
from pulsemq.storage.sqlite_user import SqliteUserRepo
from pulsemq.transport.zmq_transport import ZmqTransport

logger = logging.getLogger(__name__)


class PulseServer:
    """PulseMQ Broker 服务器。"""

    def __init__(self, config: BrokerConfig | None = None):
        self._config = config or load_config()
        self._router = MessageRouter()
        self._transport = ZmqTransport(self._config)
        self._monitor = MonitorInterceptor()

        # 初始化存储
        db_path = parse_db_url(self._config.db_url)
        self._db_conn = init_db(db_path)
        self._user_repo = SqliteUserRepo(self._db_conn)
        self._perm_repo = SqlitePermGroupRepo(self._db_conn)

        # 初始化认证
        self._auth_store = AuthMemoryStore()
        self._perm_service = PermissionService(self._perm_repo)

        # 初始化 ZAP Handler
        self._zap_handler = PulseMQZAPHandler(
            auth_store=self._auth_store,
            user_lookup_fn=self._user_repo.get_by_api_key,
        )

        # 初始化拦截器链
        pipeline = InterceptorChain([
            AuthInterceptor(self._auth_store),
            PermissionInterceptor(self._perm_service),
            self._monitor,
        ])

        # 初始化处理器
        self._handlers = MessageHandlers(
            router=self._router,
            send_fn=self._transport.send,
            broadcast_fn=self._transport.broadcast,
            pipeline=pipeline,
            default_ser=self._config.default_serializer,
            default_comp=self._config.default_compressor,
        )

        # 初始化引擎
        self._engine = Engine(
            transport=self._transport,
            handlers=self._handlers,
            config=self._config,
        )

        self._running = False

    async def start(self) -> None:
        """启动 Broker。"""
        loop_type = install_event_loop(self._config.use_uvloop)
        logger.info("事件循环: %s", loop_type)

        await self._transport.start()
        logger.info(
            "PulseMQ Broker 启动: ROUTER=%s, XPUB=%s",
            self._config.bind, self._config.xpub_bind,
        )

        self._running = True
        await self._engine.run()

    async def stop(self) -> None:
        """停止 Broker。"""
        self._running = False
        await self._engine.stop()
        await self._transport.stop()
        if self._db_conn:
            self._db_conn.close()
        logger.info("PulseMQ Broker 已停止")

    @property
    def engine(self) -> Engine:
        return self._engine

    @property
    def router(self) -> MessageRouter:
        return self._router

    @property
    def monitor(self) -> MonitorInterceptor:
        return self._monitor


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
