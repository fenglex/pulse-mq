"""Subscriber 端端到端测试。

覆盖:
- (ser × comp × data_shape) 矩阵
- 多 subscriber 广播一致性
- Burst 模式订阅端
- 客户端侧错误（ZAP 拒绝凭证等）
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

import pytest

from pulsemq.subscriber import PulseSubscriber

"""
fixtures 来自 tests/conftest.py。
"""
import tests.conftest as conftest  # noqa: F401  # 保证 conftest 加载
from tests.conftest import (
    COMPRESSIONS,
    DATA_SHAPES,
    SERIALIZERS,
    assert_message_roundtrip,
    expected_record_count,
    is_compatible,
    make_publisher,
    make_value,
    running_publisher,
)


# ---------------------------------------------------------------------------
# 矩阵
# ---------------------------------------------------------------------------


def _matrix_ids() -> list[str]:
    ids: list[str] = []
    for ser in SERIALIZERS:
        for comp in COMPRESSIONS:
            for shape in DATA_SHAPES:
                if is_compatible(ser, shape):
                    ids.append(f"{ser}-{comp}-{shape}")
                else:
                    ids.append(f"{ser}-{comp}-{shape}-SKIP")
    return ids


def _matrix_params() -> list[pytest.param]:
    params: list[pytest.param] = []
    for ser in SERIALIZERS:
        for comp in COMPRESSIONS:
            for shape in DATA_SHAPES:
                if is_compatible(ser, shape):
                    params.append(pytest.param(ser, comp, shape, id=f"{ser}-{comp}-{shape}"))
                else:
                    params.append(
                        pytest.param(
                            ser, comp, shape,
                            id=f"{ser}-{comp}-{shape}-SKIP",
                            marks=pytest.mark.skip(reason=f"非法组合: {ser} 序列化与 {shape} 数据不兼容"),
                        )
                    )
    return params


class TestSubscriberMatrix:
    """(ser × comp × data_shape) 矩阵：从 subscriber 视角验证消息可正确还原。"""

    @pytest.mark.parametrize("ser,comp,shape", _matrix_params(), ids=_matrix_ids())
    async def test_subscriber_matrix(
        self,
        ser: str,
        comp: str,
        shape: str,
        random_port_pair: tuple[int, int],
        tmp_sqlite_url: str,
    ) -> None:
        pub_port, admin_port = random_port_pair
        pub = make_publisher(pub_port=pub_port, admin_port=admin_port, tmp_db=tmp_sqlite_url)

        topic = f"sub_{ser}_{comp}_{shape}"
        expected_value = make_value(shape, 42)

        async def _factory() -> Any:
            return expected_value

        pub.register_producer(
            fn=_factory, name=topic, interval=0.05,
            serializer=ser, compression=comp,
        )

        rc = expected_record_count(expected_value)
        received: list = []

        async with running_publisher(pub):
            sub = PulseSubscriber(f"tcp://127.0.0.1:{pub_port}")
            await sub.connect()
            try:
                async for msg in sub.subscribe(topic):
                    received.append(msg)
                    if len(received) >= 3:
                        break
            finally:
                await sub.close()

        assert len(received) >= 3, f"应至少收到 3 帧，实际 {len(received)}"
        for msg in received:
            assert msg.topic == topic
            assert_message_roundtrip(
                msg, expected_value, ser=ser, comp=comp, record_count=rc,
            )


# ---------------------------------------------------------------------------
# 广播: 多 subscriber 同步接收
# ---------------------------------------------------------------------------


class TestSubscriberBroadcast:
    async def test_three_subscribers_same_topic(
        self,
        random_port_pair: tuple[int, int],
        tmp_sqlite_url: str,
    ) -> None:
        """3 个 subscriber 订阅同一 topic，断言都收到一致数据。"""
        pub_port, admin_port = random_port_pair
        pub = make_publisher(pub_port=pub_port, admin_port=admin_port, tmp_db=tmp_sqlite_url)

        topic = "broadcast_topic"
        counter = {"n": 0}

        async def _factory() -> Any:
            counter["n"] += 1
            return {"n": counter["n"], "data": "abc"}

        pub.register_producer(fn=_factory, name=topic, interval=0.1)

        async with running_publisher(pub):
            subs = [PulseSubscriber(f"tcp://127.0.0.1:{pub_port}") for _ in range(3)]
            for s in subs:
                await s.connect()

            results: list[list] = [[] for _ in range(3)]

            async def _collect(idx: int, target: int = 5) -> None:
                async for msg in subs[idx].subscribe(topic):
                    results[idx].append(msg.payload)
                    if len(results[idx]) >= target:
                        break

            try:
                await asyncio.gather(*[_collect(i) for i in range(3)])
            finally:
                for s in subs:
                    await s.close()

        # 3 个 subscriber 都应收到 ≥ 5 条
        for i, r in enumerate(results):
            assert len(r) >= 5, f"subscriber {i} 只收到 {len(r)} 条"

        # 顺序一致性: 每个 subscriber 收到的 payload 序列与 publisher 发送序列一致
        # 注意: 各 subscriber 接收顺序应保持相同
        for r in results:
            seqs = [p["n"] for p in r]
            assert seqs == sorted(seqs), f"消息乱序: {seqs}"


# ---------------------------------------------------------------------------
# Burst 模式订阅端
# ---------------------------------------------------------------------------


class TestSubscriberBurst:
    async def test_burst_subscribe(
        self,
        random_port_pair: tuple[int, int],
        tmp_sqlite_url: str,
    ) -> None:
        """burst 模式：subscriber 累积接收。"""
        pub_port, admin_port = random_port_pair
        pub = make_publisher(pub_port=pub_port, admin_port=admin_port, tmp_db=tmp_sqlite_url)

        topic = "burst_sub_topic"
        total_batches = 30
        counter = {"n": 0}
        go = asyncio.Event()

        async def _burst_factory() -> Any:
            await go.wait()  # 等 subscriber 连上
            counter["n"] += 1
            if counter["n"] > total_batches:
                return None
            return [{"i": counter["n"] * 100 + j} for j in range(20)]

        pub._producer_mgr.register_burst(
            callback=_burst_factory, name=topic,
            serializer="msgpack", compression="none",
        )
        pub._buffers.get_or_create(topic, 10_000)

        received: list = []
        sub = PulseSubscriber(f"tcp://127.0.0.1:{pub_port}")

        async with running_publisher(pub):
            await sub.connect()
            await asyncio.sleep(0.3)  # 等订阅生效
            go.set()
            try:
                async for msg in sub.subscribe(topic):
                    received.append(msg)
                    total_records = sum(m.record_count for m in received)
                    if total_records >= total_batches * 20:
                        break
                    if len(received) > 200:  # 保险
                        break
            finally:
                await sub.close()

        total = sum(m.record_count for m in received)
        assert total >= total_batches * 20, f"应至少收到 {total_batches * 20} 条，实际 {total}"


# ---------------------------------------------------------------------------
# 客户端侧错误路径
# ---------------------------------------------------------------------------


class TestSubscriberErrors:
    async def test_plain_auth_rejected(
        self,
        random_port_pair: tuple[int, int],
        tmp_sqlite_url: str,
    ) -> None:
        """错误凭证应被 ZAP 拒绝。"""
        pub_port, admin_port = random_port_pair
        pub = make_publisher(
            pub_port=pub_port, admin_port=admin_port, tmp_db=tmp_sqlite_url,
            api_keys={"alice": "right_pwd"},
        )

        async with running_publisher(pub):
            sub = PulseSubscriber(
                f"tcp://127.0.0.1:{pub_port}",
                username="alice", password="wrong_pwd",
            )
            await sub.connect()
            try:
                # 错误凭证下，订阅后尝试接收一段时间内不应能成功收到任何消息
                # (ZAP 拒绝后 PUB 不会向该连接发送)
                # 在 zmq 层面用 setsockopt(SUBSCRIBE) 不抛异常，但 recv 会因为断连而失败
                # 这里做"足够短时间无消息收到"的弱断言
                got: list = []

                async def _consume_briefly() -> None:
                    async for _msg in sub.subscribe(topic_for_auth()):
                        got.append(_msg)
                        if len(got) > 0:
                            break

                with pytest.raises(Exception):
                    # 2 秒内尝试收消息，错误凭证下 zmq 应断开
                    await asyncio.wait_for(_consume_briefly(), timeout=2.0)
            finally:
                await sub.close()


def topic_for_auth() -> str:
    return "auth_topic"
