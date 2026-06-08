"""集成测试：Publisher + Subscriber 端到端。"""

from __future__ import annotations

import asyncio
import os
import random
import tempfile

import pytest

from pulsemq.config import PublisherConfig
from pulsemq.publisher import PulsePublisher
from pulsemq.subscriber import PulseSubscriber


def _rand_port() -> int:
    return random.randint(25000, 35000)


@pytest.fixture
def ports():
    p = _rand_port()
    a = _rand_port()
    while a == p:
        a = _rand_port()
    return p, a


@pytest.fixture
def tmp_db():
    fd, path = tempfile.mkstemp(suffix=".sqlite")
    os.close(fd)
    yield f"sqlite://{path}"
    try:
        os.unlink(path)
    except Exception:
        pass


async def _run_pub(pub: PulsePublisher, duration: float = 3.0) -> None:
    """运行 publisher 指定秒数后自动停止。"""
    task = asyncio.create_task(pub._run())
    await asyncio.sleep(duration)
    pub._running = False
    await asyncio.sleep(0.5)
    task.cancel()
    try:
        await task
    except (asyncio.CancelledError, Exception):
        pass


class TestPublisherSubscriber:
    async def test_basic_pubsub(self, ports, tmp_db) -> None:
        """基础 pub/sub：单 producer，单 subscriber。"""
        port, aport = ports
        pub = PulsePublisher(
            config=PublisherConfig(
                bind=f"tcp://127.0.0.1:{port}",
                admin_bind=f"127.0.0.1:{aport}",
                stats_db=tmp_db,
            )
        )

        received = []

        @pub.producer(name="test_topic", interval=0.3)
        async def producer():
            return {"msg": "hello"}

        pub_task = asyncio.create_task(pub._run())
        await asyncio.sleep(0.5)

        sub = PulseSubscriber(f"tcp://127.0.0.1:{port}")
        await sub.connect()

        async for msg in sub.subscribe("test_topic"):
            received.append(msg)
            if len(received) >= 3:
                break

        assert len(received) >= 3
        assert all(m.topic == "test_topic" for m in received)
        assert all(m.payload["msg"] == "hello" for m in received)

        pub._running = False
        await asyncio.sleep(0.3)
        pub_task.cancel()
        try:
            await pub_task
        except (asyncio.CancelledError, Exception):
            pass
        await sub.close()

    async def test_batch_data(self, ports, tmp_db) -> None:
        """批量数据：list[str]。"""
        port, aport = ports
        pub = PulsePublisher(
            config=PublisherConfig(
                bind=f"tcp://127.0.0.1:{port}",
                admin_bind=f"127.0.0.1:{aport}",
                stats_db=tmp_db,
            )
        )

        @pub.producer(name="batch_topic", interval=0.3)
        async def producer():
            return ["a", "b", "c"]

        pub_task = asyncio.create_task(pub._run())
        await asyncio.sleep(0.5)

        sub = PulseSubscriber(f"tcp://127.0.0.1:{port}")
        await sub.connect()

        async for msg in sub.subscribe("batch_topic"):
            assert msg.record_count == 3
            assert msg.payload == ["a", "b", "c"]
            break

        pub._running = False
        await asyncio.sleep(0.3)
        pub_task.cancel()
        try:
            await pub_task
        except (asyncio.CancelledError, Exception):
            pass
        await sub.close()

    async def test_compression_roundtrip(self, ports, tmp_db) -> None:
        """压缩格式传输正确性。"""
        port, aport = ports
        pub = PulsePublisher(
            config=PublisherConfig(
                bind=f"tcp://127.0.0.1:{port}",
                admin_bind=f"127.0.0.1:{aport}",
                stats_db=tmp_db,
            )
        )

        big_data = {"payload": "x" * 2000}

        @pub.producer(name="comp_topic", interval=0.3, compression="zstd")
        async def producer():
            return big_data

        pub_task = asyncio.create_task(pub._run())
        await asyncio.sleep(0.5)

        sub = PulseSubscriber(f"tcp://127.0.0.1:{port}")
        await sub.connect()

        async for msg in sub.subscribe("comp_topic"):
            assert msg.compression == "zstd"
            assert msg.payload == big_data
            break

        pub._running = False
        await asyncio.sleep(0.3)
        pub_task.cancel()
        try:
            await pub_task
        except (asyncio.CancelledError, Exception):
            pass
        await sub.close()

    async def test_multiple_subscribers(self, ports, tmp_db) -> None:
        """多个 subscriber 同时接收。"""
        port, aport = ports
        pub = PulsePublisher(
            config=PublisherConfig(
                bind=f"tcp://127.0.0.1:{port}",
                admin_bind=f"127.0.0.1:{aport}",
                stats_db=tmp_db,
            )
        )

        @pub.producer(name="multi_topic", interval=0.3)
        async def producer():
            return {"seq": time.time_ns()}

        pub_task = asyncio.create_task(pub._run())
        await asyncio.sleep(0.5)

        sub1 = PulseSubscriber(f"tcp://127.0.0.1:{port}")
        sub2 = PulseSubscriber(f"tcp://127.0.0.1:{port}")
        await sub1.connect()
        await sub2.connect()

        results1 = []
        results2 = []

        async def collect(sub, results):
            async for msg in sub.subscribe("multi_topic"):
                results.append(msg)
                if len(results) >= 3:
                    break

        await asyncio.gather(
            collect(sub1, results1),
            collect(sub2, results2),
        )

        assert len(results1) >= 3
        assert len(results2) >= 3

        pub._running = False
        await asyncio.sleep(0.3)
        pub_task.cancel()
        try:
            await pub_task
        except (asyncio.CancelledError, Exception):
            pass
        await sub1.close()
        await sub2.close()

    async def test_plain_auth(self, ports, tmp_db) -> None:
        """PLAIN 认证：合法用户能连接。"""
        port, aport = ports
        pub = PulsePublisher(
            config=PublisherConfig(
                bind=f"tcp://127.0.0.1:{port}",
                admin_bind=f"127.0.0.1:{aport}",
                stats_db=tmp_db,
            ),
            api_keys={"testuser": "testpass"},
        )

        @pub.producer(name="auth_topic", interval=0.3)
        async def producer():
            return {"ok": True}

        pub_task = asyncio.create_task(pub._run())
        await asyncio.sleep(0.5)

        sub = PulseSubscriber(
            f"tcp://127.0.0.1:{port}",
            username="testuser",
            password="testpass",
        )
        await sub.connect()

        async for msg in sub.subscribe("auth_topic"):
            assert msg.payload["ok"] is True
            break

        pub._running = False
        await asyncio.sleep(0.3)
        pub_task.cancel()
        try:
            await pub_task
        except (asyncio.CancelledError, Exception):
            pass
        await sub.close()


# 需要 import time
import time
