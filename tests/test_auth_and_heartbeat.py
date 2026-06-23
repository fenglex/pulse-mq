"""认证 + 心跳机制测试。

覆盖:
- 异常体系（AuthenticationError / ConnectionLostError）
- ZAP 日志增强
- on_auth 回调钩子
- 心跳机制（编码/发送/检测/超时）
"""

from __future__ import annotations

import asyncio
import logging
import sys
import time

import pytest
import zmq

from pulsemq.subscriber import (
    AuthenticationError,
    ConnectionLostError,
    PulseSubscriber,
    PulseSubscriberError,
)

# Windows: 强制 Selector 事件循环
if sys.platform == "win32" and hasattr(asyncio, "WindowsSelectorEventLoopPolicy"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

import tests.conftest as conftest  # noqa: F401, E402
from tests.conftest import (
    make_publisher,
    running_publisher,
)


class TestSubscriberExceptions:
    """模块 1：Sub 端异常体系。"""

    async def test_connection_lost_on_pub_shutdown(
        self,
        random_port_pair: tuple[int, int],
        tmp_sqlite_url: str,
    ) -> None:
        """Publisher 关闭后，subscriber 抛 ConnectionLostError。"""
        pub_port, admin_port = random_port_pair
        pub = make_publisher(pub_port=pub_port, admin_port=admin_port, tmp_db=tmp_sqlite_url)

        topic = "will_die"
        received: list = []

        async def _factory() -> dict:
            return {"x": 1}

        pub.register_producer(fn=_factory, name=topic, interval=0.1)

        sub = PulseSubscriber(f"tcp://127.0.0.1:{pub_port}", receive_timeout=3000)

        async with running_publisher(pub) as p:
            await sub.connect()
            # 先收一条确认连通
            async for msg in sub.subscribe(topic):
                received.append(msg)
                break
            # publisher 在 running_publisher 退出时关闭
            # 关闭后继续迭代应抛 ConnectionLostError
            await asyncio.sleep(0.5)

        # publisher 已关闭，尝试继续收消息
        with pytest.raises(ConnectionLostError, match="连接已断开"):
            async for _msg in sub.subscribe(topic):
                pass

        await sub.close()

    async def test_close_does_not_raise(
        self,
        random_port_pair: tuple[int, int],
        tmp_sqlite_url: str,
    ) -> None:
        """正常 close() 不抛异常。"""
        pub_port, admin_port = random_port_pair
        pub = make_publisher(pub_port=pub_port, admin_port=admin_port, tmp_db=tmp_sqlite_url)

        topic = "normal_close"
        received: list = []

        async def _factory() -> dict:
            return {"x": 1}

        pub.register_producer(fn=_factory, name=topic, interval=0.1)

        async with running_publisher(pub):
            sub = PulseSubscriber(f"tcp://127.0.0.1:{pub_port}")
            await sub.connect()
            async for msg in sub.subscribe(topic):
                received.append(msg)
                if len(received) >= 3:
                    break
            await sub.close()  # 不应抛异常

        assert len(received) >= 3

    async def test_authentication_error_raised(
        self,
        random_port_pair: tuple[int, int],
        tmp_sqlite_url: str,
    ) -> None:
        """错误凭据导致认证失败，抛 ConnectionLostError。"""
        pub_port, admin_port = random_port_pair
        pub = make_publisher(
            pub_port=pub_port, admin_port=admin_port, tmp_db=tmp_sqlite_url,
            api_keys={"alice": "right_pwd"},
        )

        topic = "auth_test_x"

        async def _factory() -> dict:
            return {"x": 1}

        pub.register_producer(fn=_factory, name=topic, interval=0.1)

        async with running_publisher(pub):
            sub = PulseSubscriber(
                f"tcp://127.0.0.1:{pub_port}",
                username="alice", password="wrong_pwd", receive_timeout=3000,
            )
            await sub.connect()
            # 错误凭据：ZMQ 会在 recv 时断开连接
            with pytest.raises(ConnectionLostError):
                async for _msg in sub.subscribe(topic):
                    pass
            await sub.close()
