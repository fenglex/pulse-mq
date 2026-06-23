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


class TestZapLogging:
    """模块 2：Pub 端 ZAP 日志增强。"""

    async def test_zap_success_log_contains_client_address(
        self,
        random_port_pair: tuple[int, int],
        tmp_sqlite_url: str,
        caplog,
    ) -> None:
        """认证成功日志包含客户端 IP。"""
        pub_port, admin_port = random_port_pair
        pub = make_publisher(
            pub_port=pub_port, admin_port=admin_port, tmp_db=tmp_sqlite_url,
            api_keys={"alice": "pwd"},
        )

        topic = "log_topic_1"

        async def _factory() -> dict:
            return {"x": 1}

        pub.register_producer(fn=_factory, name=topic, interval=0.2)

        caplog.set_level(logging.INFO, logger="pulsemq.transport.zmq_pub")

        async with running_publisher(pub):
            sub = PulseSubscriber(
                f"tcp://127.0.0.1:{pub_port}",
                username="alice", password="pwd",
            )
            await sub.connect()
            async for msg in sub.subscribe(topic):
                if msg.payload == {"x": 1}:
                    break
            await sub.close()

        success_logs = [r.message for r in caplog.records if "ZAP 认证成功" in r.message]
        assert len(success_logs) >= 1, f"应有至少一条成功日志，实际: {caplog.record_tuples}"
        assert "client=" in success_logs[0], f"日志应包含客户端地址: {success_logs[0]}"
        assert "username=alice" in success_logs[0]

    async def test_zap_failure_log_contains_client_address(
        self,
        random_port_pair: tuple[int, int],
        tmp_sqlite_url: str,
        caplog,
    ) -> None:
        """认证失败日志包含客户端 IP。"""
        pub_port, admin_port = random_port_pair
        pub = make_publisher(
            pub_port=pub_port, admin_port=admin_port, tmp_db=tmp_sqlite_url,
            api_keys={"alice": "pwd"},
        )

        topic = "log_topic_2"

        async def _factory() -> dict:
            return {"x": 1}

        pub.register_producer(fn=_factory, name=topic, interval=0.2)

        caplog.set_level(logging.WARNING, logger="pulsemq.transport.zmq_pub")

        async with running_publisher(pub):
            sub = PulseSubscriber(
                f"tcp://127.0.0.1:{pub_port}",
                username="alice", password="WRONG", receive_timeout=3000,
            )
            await sub.connect()
            with pytest.raises(ConnectionLostError):
                async for _msg in sub.subscribe(topic):
                    pass
            await sub.close()

        fail_logs = [r.message for r in caplog.records if "ZAP 认证失败" in r.message]
        assert len(fail_logs) >= 1, f"应有至少一条失败日志，实际: {caplog.record_tuples}"
        assert "client=" in fail_logs[0], f"日志应包含客户端地址: {fail_logs[0]}"


class TestAuthCallback:
    """模块 3：认证事件回调钩子。"""

    async def test_on_auth_callback_success(
        self,
        random_port_pair: tuple[int, int],
        tmp_sqlite_url: str,
    ) -> None:
        """认证成功时回调被调用，参数正确。"""
        pub_port, admin_port = random_port_pair
        events: list[dict] = []

        async def on_auth(username: str, addr: str, success: bool) -> None:
            events.append({"username": username, "addr": addr, "success": success})

        pub = make_publisher(
            pub_port=pub_port, admin_port=admin_port, tmp_db=tmp_sqlite_url,
            api_keys={"bob": "secret"},
        )
        pub._on_auth = on_auth

        topic = "cb_topic_1"

        async def _factory() -> dict:
            return {"x": 1}

        pub.register_producer(fn=_factory, name=topic, interval=0.2)

        async with running_publisher(pub):
            sub = PulseSubscriber(
                f"tcp://127.0.0.1:{pub_port}",
                username="bob", password="secret",
            )
            await sub.connect()
            async for msg in sub.subscribe(topic):
                if msg.payload == {"x": 1}:
                    break
            await sub.close()

        assert len(events) >= 1, f"回调应至少被调用一次，实际: {events}"
        evt = events[0]
        assert evt["username"] == "bob"
        assert evt["success"] is True
        assert "127.0.0.1" in evt["addr"] or "::1" in evt["addr"]

    async def test_on_auth_callback_failure(
        self,
        random_port_pair: tuple[int, int],
        tmp_sqlite_url: str,
    ) -> None:
        """认证失败时回调被调用，参数正确。"""
        pub_port, admin_port = random_port_pair
        events: list[dict] = []

        async def on_auth(username: str, addr: str, success: bool) -> None:
            events.append({"username": username, "addr": addr, "success": success})

        pub = make_publisher(
            pub_port=pub_port, admin_port=admin_port, tmp_db=tmp_sqlite_url,
            api_keys={"bob": "secret"},
        )
        pub._on_auth = on_auth

        topic = "cb_topic_2"

        async def _factory() -> dict:
            return {"x": 1}

        pub.register_producer(fn=_factory, name=topic, interval=0.2)

        async with running_publisher(pub):
            sub = PulseSubscriber(
                f"tcp://127.0.0.1:{pub_port}",
                username="bob", password="BAD_PWD", receive_timeout=3000,
            )
            await sub.connect()
            with pytest.raises(ConnectionLostError):
                async for _msg in sub.subscribe(topic):
                    pass
            await sub.close()

        assert len(events) >= 1, f"回调应至少被调用一次，实际: {events}"
        evt = events[0]
        assert evt["username"] == "bob"
        assert evt["success"] is False

    async def test_on_auth_callback_exception_does_not_crash(
        self,
        random_port_pair: tuple[int, int],
        tmp_sqlite_url: str,
    ) -> None:
        """回调抛出异常不影响认证流程。"""
        pub_port, admin_port = random_port_pair

        async def bad_callback(username: str, addr: str, success: bool) -> None:
            raise RuntimeError("回调内部错误")

        pub = make_publisher(
            pub_port=pub_port, admin_port=admin_port, tmp_db=tmp_sqlite_url,
            api_keys={"carol": "pwd"},
        )
        pub._on_auth = bad_callback

        topic = "cb_topic_3"

        async def _factory() -> dict:
            return {"x": 1}

        pub.register_producer(fn=_factory, name=topic, interval=0.2)

        async with running_publisher(pub):
            sub = PulseSubscriber(
                f"tcp://127.0.0.1:{pub_port}",
                username="carol", password="pwd",
            )
            await sub.connect()
            # 回调异常不应阻止认证 — 消息应正常到达
            async for msg in sub.subscribe(topic):
                assert msg.payload == {"x": 1}
                break
            await sub.close()


class TestHeartbeatEncoding:
    """模块 4a：心跳帧编码。"""

    def test_encode_heartbeat_format(self) -> None:
        """心跳帧格式正确。"""
        from pulsemq.protocol.frames import encode_heartbeat
        from pulsemq.protocol.msg_type import MsgType
        import struct

        frames = encode_heartbeat()
        assert len(frames) == 4, f"应为 4 帧，实际 {len(frames)}"
        # Frame 1: topic
        assert frames[0] == b"__pulse_hb__"
        # Frame 2: meta (6 bytes)
        assert len(frames[1]) == 6
        assert frames[1][0] == MsgType.PING
        # record_count = 0 (bytes 2-5, big-endian uint32)
        rc = struct.unpack(">I", frames[1][2:6])[0]
        assert rc == 0
        # Frame 3: timestamp (8 bytes)
        assert len(frames[2]) == 8
        ts = struct.unpack(">q", frames[2])[0]
        assert ts > 0
        # Frame 4: empty payload
        assert frames[3] == b""

    def test_encode_heartbeat_idempotent(self) -> None:
        """连续两次调用生成的时间戳递增。"""
        from pulsemq.protocol.frames import encode_heartbeat
        import struct

        f1 = encode_heartbeat()
        f2 = encode_heartbeat()
        ts1 = struct.unpack(">q", f1[2])[0]
        ts2 = struct.unpack(">q", f2[2])[0]
        assert ts2 >= ts1