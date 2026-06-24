"""Subscriber 端端到端测试。

覆盖:
- (ser × comp × data_shape) 矩阵
- 多 subscriber 广播一致性
- Burst 模式订阅端
- 客户端侧错误（ZAP 拒绝凭证等）
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from typing import Any

import pandas as pd
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
            return pd.DataFrame({"i": [counter["n"] * 100 + j for j in range(20)]})

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
        capsys,
    ) -> None:
        """错误凭证：subscribe() 打提示后静默结束迭代（不卡死、不抛异常）。

        pyzmq 的 SUB 在 PLAIN 认证被拒绝时 recv 不抛错、无限重连，
        会让用户代码卡死。PulseSubscriber 通过 monitor 检测到
        EVENT_HANDSHAKE_FAILED_AUTH 后，自行输出 [SUB 认证失败] 到 stderr
        并结束迭代，``async for`` 自然退出，用户无需 try/except。
        """
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
                received: list = []

                async def _consume() -> None:
                    async for _msg in sub.subscribe(topic_for_auth()):
                        received.append(_msg)

                # 错误凭证下，subscribe() 应在握手完成后很快静默结束，
                # 而非无限阻塞。加 timeout 防止回归（卡死时测试会超时失败）。
                await asyncio.wait_for(_consume(), timeout=5.0)
            finally:
                await sub.close()

        # 不应收到任何消息
        assert received == [], f"错误凭证不应收到消息，实际 {len(received)} 条"
        # 应有认证失败的提示（输出到 stderr，不依赖 logging 配置）
        captured = capsys.readouterr()
        combined = captured.err + captured.out
        assert "认证失败" in combined, (
            f"错误凭证应输出 [SUB 认证失败] 到 stderr，实际捕获: {combined!r}"
        )


def topic_for_auth() -> str:
    return "auth_topic"


# ---------------------------------------------------------------------------
# 认证可见性：sub 端上线（成功/失败）都应有提示，失败要自动停止
# ---------------------------------------------------------------------------


class TestAuthVisibility:
    """sub 端认证可见性。

    需求：sub 连接 publisher 时，认证成功/失败都应在 sub 端有明确提示，
    认证失败要自动停止订阅（不让用户代码卡在无限重连上）。

    背景：此前 sub 端 monitor 只监听 EVENT_HANDSHAKE_FAILED_AUTH，
    导致：
    - 认证成功时 sub 端无任何感知（无上线日志）；
    - 非 PLAIN 机制失败（HANDSHAKE_FAILED_PROTOCOL 等）无提示也不停止。
    现在扩展监听到 HANDSHAKE_SUCCEEDED + 所有 FAILED 事件。
    """

    async def test_auth_success_logs_online(
        self,
        random_port_pair: tuple[int, int],
        tmp_sqlite_url: str,
        capsys,
    ) -> None:
        """正确凭证：sub 端应有上线提示（print 到 stderr，不依赖 logging 配置）。"""
        pub_port, admin_port = random_port_pair
        pub = make_publisher(
            pub_port=pub_port, admin_port=admin_port, tmp_db=tmp_sqlite_url,
            api_keys={"alice": "right_pwd"},
        )

        async with running_publisher(pub):
            sub = PulseSubscriber(
                f"tcp://127.0.0.1:{pub_port}",
                username="alice", password="right_pwd",
            )
            await sub.connect()
            try:
                # 触发 subscribe() 启动 monitor 并完成握手；
                # 正确凭证下会收到 HANDSHAKE_SUCCEEDED。
                async def _probe():
                    async for _msg in sub.subscribe(topic_for_auth()):
                        break

                try:
                    await asyncio.wait_for(_probe(), timeout=5.0)
                except asyncio.TimeoutError:
                    pass  # 超时无所谓，只关心握手结果提示
            finally:
                await sub.close()

        # 核心断言：sub 端应有认证成功的上线提示（输出到 stderr）
        captured = capsys.readouterr()
        combined = captured.err + captured.out
        assert "上线" in combined or "认证成功" in combined, (
            f"认证成功时应输出上线提示到 stderr，实际捕获: {combined!r}"
        )


# ---------------------------------------------------------------------------
# 心跳帧过滤：订阅端不应把 PING 心跳帧交付给用户迭代器
# ---------------------------------------------------------------------------


class TestHeartbeatFiltered:
    """回归：publisher 每隔 heartbeat_interval 发 PING 心跳帧（topic=__pulse_hb__）。

    订阅端必须过滤掉心跳帧，不能交付给用户的 async for 循环。
    历史 bug：subscribe() 无条件 decode+yield 所有 4 帧，心跳帧导致：
      1) decode 读 6 字节 meta（B1 修复前）报 truncated；
      2) 即使尺寸修对，空 payload 经 msgpack 反序列化也会崩；
      3) 语义上心跳是协议控制帧，不该混入业务消息流。
    """

    async def test_heartbeat_not_delivered(
        self,
        random_port_pair: tuple[int, int],
        tmp_sqlite_url: str,
    ) -> None:
        from pulsemq.config import PublisherConfig
        from pulsemq.publisher import PulsePublisher

        pub_port, admin_port = random_port_pair
        # heartbeat_interval=0.3s，确保 1.5s 收集窗口内会发出多次心跳
        pub = PulsePublisher(
            config=PublisherConfig(
                bind=f"tcp://127.0.0.1:{pub_port}",
                admin_bind=f"127.0.0.1:{admin_port}",
                stats_db=tmp_sqlite_url,
                heartbeat_interval=0.3,
            ),
        )
        # 一个业务 producer，保证订阅器有真实消息可收（不至于只收心跳）
        async def _probe() -> Any:
            return {"v": 1}

        pub.register_producer(fn=_probe, name="hb_probe", interval=0.1)

        received: list = []
        async with running_publisher(pub):
            sub = PulseSubscriber(f"tcp://127.0.0.1:{pub_port}")
            await sub.connect()
            try:
                # subscribe("") = 订阅所有 topic（含 __pulse_hb__）
                async for msg in sub.subscribe(""):
                    received.append(msg)
                    # 收到若干业务消息或超时即停
                    if sum(1 for m in received if m.topic == "hb_probe") >= 3:
                        break
            finally:
                await sub.close()

        # 核心断言：交付给用户的消息里绝不能有心跳帧
        hb = [m for m in received if m.topic == "__pulse_hb__"]
        assert hb == [], f"心跳帧不应交付给用户迭代器，实际收到 {len(hb)} 条"
        # 至少应收到业务消息，证明迭代器本身工作正常
        biz = [m for m in received if m.topic == "hb_probe"]
        assert biz, "应至少收到一条业务消息（证明迭代器正常，而非静默丢弃全部）"


# ---------------------------------------------------------------------------
# 断线检测：pub 停止后 sub 应自动结束迭代（不卡死）
# ---------------------------------------------------------------------------


class TestDisconnectDetection:
    """pub 端停止 / 网络断开后，sub 的 async for 应自动结束。

    背景：此前 sub 的 monitor 只监听握手事件，不监听 EVENT_DISCONNECTED，
    导致 pub 退出后 sub 的 recv_multipart 无限等待，用户代码卡死。
    现在监听 EVENT_DISCONNECTED，断线即结束迭代。
    """

    async def test_sub_exits_when_pub_stops(
        self,
        random_port_pair: tuple[int, int],
        tmp_sqlite_url: str,
    ) -> None:
        pub_port, admin_port = random_port_pair
        pub = make_publisher(
            pub_port=pub_port, admin_port=admin_port, tmp_db=tmp_sqlite_url,
        )

        async def _factory() -> Any:
            return {"v": 1}

        pub.register_producer(fn=_factory, name="dc_probe", interval=0.05)

        # 手动管理 pub 生命周期（不用 running_publisher，需精确控制停止时机）
        pub._running = True
        pub._start_time = time.time()
        from pulsemq.transport.zmq_pub import ZmqPubTransport
        from pulsemq.stats.storage import StatsStorage
        from pulsemq.admin.server import AdminServer
        pub._transport = ZmqPubTransport(
            bind=pub._config.bind, api_keys={}, on_auth=pub._on_auth,
        )
        await pub._transport.start()
        pub._storage = StatsStorage(pub._config.stats_db)
        pub._storage.connect()
        for nm, spec in pub._producer_mgr.specs.items():
            pub._buffers.get_or_create(nm, spec.cache_size)
        pub._admin = AdminServer(
            bind=pub._config.admin_bind, traffic_stats=pub._traffic,
            topic_buffers=pub._buffers, stats_storage=pub._storage,
            snapshot_fn=pub._system_snapshot, start_time=pub._start_time,
        )
        await pub._admin.start()
        import asyncio as _aio
        roll_task = _aio.create_task(pub._minute_roll_loop())
        await pub._producer_mgr.start_all(pub._on_produce, pub._make_sender)

        await asyncio.sleep(0.8)  # 让 pub 跑起来

        sub = PulseSubscriber(f"tcp://127.0.0.1:{pub_port}")
        await sub.connect()
        consume_done = asyncio.Event()

        async def _consume():
            async for _msg in sub.subscribe("dc_probe"):
                pass
            consume_done.set()

        c = asyncio.create_task(_consume())
        await asyncio.sleep(0.5)

        # === 停止 pub（模拟 publisher 进程退出）===
        pub._running = False
        await asyncio.sleep(0.3)
        await pub._shutdown(roll_task)

        # 等待 sub 检测到断线并结束迭代
        try:
            await asyncio.wait_for(consume_done.wait(), timeout=5.0)
            ended = True
        except asyncio.TimeoutError:
            ended = False

        c.cancel()
        with contextlib.suppress(Exception):
            await c
        with contextlib.suppress(Exception):
            await sub.close()

        assert ended, (
            "pub 停止后 sub 的 async for 应自动结束，但 5s 后仍在运行（卡死）"
        )


# ---------------------------------------------------------------------------
# 认证可见性（不依赖 logging 配置，直接输出到 stderr）
# ---------------------------------------------------------------------------


class TestConnectionNotice:
    """连接/认证关键事件应直接输出到 stderr（print），不依赖用户配置 logging。

    背景：此前用 logging 打日志，用户没配 basicConfig() 时 info 级（认证成功）
    被 Python 默认 lastResort 吞掉，导致完全看不到。改用 print(file=stderr)
    保证关键事件始终可见。
    """

    async def test_auth_success_notice_to_stderr(
        self,
        random_port_pair: tuple[int, int],
        tmp_sqlite_url: str,
        capsys,
    ) -> None:
        pub_port, admin_port = random_port_pair
        pub = make_publisher(
            pub_port=pub_port, admin_port=admin_port, tmp_db=tmp_sqlite_url,
            api_keys={"alice": "right_pwd"},
        )

        async with running_publisher(pub):
            sub = PulseSubscriber(
                f"tcp://127.0.0.1:{pub_port}",
                username="alice", password="right_pwd",
            )
            await sub.connect()
            try:
                async def _probe():
                    async for _msg in sub.subscribe(topic_for_auth()):
                        break
                try:
                    await asyncio.wait_for(_probe(), timeout=5.0)
                except asyncio.TimeoutError:
                    pass
            finally:
                await sub.close()

        captured = capsys.readouterr()
        combined = captured.err + captured.out
        # 关键：认证成功应有可见提示（不依赖 logging 配置）
        assert "上线" in combined or "auth=OK" in combined or "认证成功" in combined, (
            f"认证成功应输出到 stderr 可见，实际捕获: {combined!r}"
        )

    async def test_pub_logs_sub_online_to_stderr(
        self,
        random_port_pair: tuple[int, int],
        tmp_sqlite_url: str,
        capsys,
    ) -> None:
        """sub 上线时，pub 端（ZAP handler）应有可见提示输出到 stderr。

        背景：pub 端 ZAP handler 此前用 logging.info 打 [SUB 上线] auth=OK，
        用户没配 basicConfig() 时被吞掉，完全看不到。改用 print 到 stderr。
        本测试断言 pub 端专属格式「auth=OK」（区别于 sub 端的「认证成功」）。
        """
        pub_port, admin_port = random_port_pair
        pub = make_publisher(
            pub_port=pub_port, admin_port=admin_port, tmp_db=tmp_sqlite_url,
            api_keys={"alice": "right_pwd"},
        )

        async with running_publisher(pub):
            sub = PulseSubscriber(
                f"tcp://127.0.0.1:{pub_port}",
                username="alice", password="right_pwd",
            )
            await sub.connect()
            try:
                async def _probe():
                    async for _msg in sub.subscribe(topic_for_auth()):
                        break
                try:
                    await asyncio.wait_for(_probe(), timeout=5.0)
                except asyncio.TimeoutError:
                    pass
            finally:
                await sub.close()

        captured = capsys.readouterr()
        combined = captured.err + captured.out
        # pub 端 ZAP 输出格式：[SUB 上线] user=alice addr=... auth=OK
        assert "auth=OK" in combined, (
            f"sub 上线时 pub 端应输出 [SUB 上线] auth=OK 到 stderr，"
            f"实际捕获: {combined!r}"
        )
