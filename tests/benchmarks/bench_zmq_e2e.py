"""真实 ZMQ 端到端基准测试。

启动完整 Broker（ZMQ socket），使用 DEALER/SUB 客户端连接，
测量从 PUB 到 SUB 收到消息的完整延迟和吞吐。

测试维度:
- 单 producer → 单 consumer
- 单 producer → 多 consumer
- 各压缩格式端到端吞吐和延迟对比
- PING/PONG RTT
"""

import asyncio
import random
import time

import pytest
import zmq
import zmq.asyncio

from pulsemq.auth.memory_store import AuthMemoryStore
from pulsemq.auth.permission import PermissionService
from pulsemq.config import BrokerConfig
from pulsemq.engine.engine import Engine
from pulsemq.engine.handlers import MessageHandlers
from pulsemq.engine.pipeline import (
    AuthInterceptor,
    InterceptorChain,
    MonitorInterceptor,
    PermissionInterceptor,
)
from pulsemq.engine.router import MessageRouter
from pulsemq.models import AuthUser
from pulsemq.monitoring.realtime import RealtimeMetrics
from pulsemq.protocol.frames import FrameCodec
from pulsemq.protocol.msg_type import MsgType
from pulsemq.transport.zmq_transport import ZmqTransport
from tests.benchmarks.data_generators import get_preset_single
from tests.benchmarks.conftest import BenchResult


def _random_port() -> int:
    return random.randint(20000, 30000)


async def _start_broker(port_router: int, port_xpub: int):
    """启动一个完整 Broker 实例。"""
    config = BrokerConfig(
        bind=f"tcp://*:{port_router}",
        xpub_bind=f"tcp://*:{port_xpub}",
        auth_enabled=False,
        zmq_rcvhwm=50000,
        zmq_sndhwm=50000,
    )

    router = MessageRouter()
    auth_store = AuthMemoryStore()
    realtime = RealtimeMetrics()

    admin = AuthUser(user_id=1, role="admin", groups=[], api_key="bench", namespace="")
    auth_store.register(b"pub_id", admin)
    auth_store.register(b"sub_id", admin)
    router.register_connection(b"pub_id", admin)
    router.register_connection(b"sub_id", admin)

    pipeline = InterceptorChain([
        MonitorInterceptor(realtime_metrics=realtime),
        AuthInterceptor(auth_store),
        PermissionInterceptor(PermissionService(perm_repo=None)),
    ])

    transport = ZmqTransport(config)
    handlers = MessageHandlers(
        router=router,
        send_fn=transport.send,
        broadcast_fn=transport.broadcast,
        pipeline=pipeline,
    )
    engine = Engine(transport=transport, handlers=handlers, config=config)

    await transport.start()
    engine_task = asyncio.create_task(engine.run())
    await asyncio.sleep(0.1)

    return transport, engine, engine_task


async def _cleanup(engine_task, transport, ctx, *sockets):
    """安全清理所有资源（先关 socket，再 term context）。"""
    # 1. 关闭客户端 sockets
    for sock in sockets:
        if sock is not None:
            try:
                sock.close(linger=0)
            except Exception:
                pass
    # 2. 销毁 client context（不等 linger）
    if ctx is not None:
        try:
            ctx.destroy(linger=0)
        except Exception:
            pass
    # 3. 停止 engine
    engine_task.cancel()
    try:
        await asyncio.wait_for(engine_task, timeout=2.0)
    except (asyncio.CancelledError, asyncio.TimeoutError):
        pass
    # 4. 停止 transport
    try:
        await transport.stop()
    except Exception:
        pass


# ---- 单 Producer → 单 Consumer ----

class TestSinglePubSub:
    """单 producer → 单 consumer 端到端。"""

    async def test_e2e_throughput_msgpack_none(self):
        """msgpack + none 端到端吞吐。"""
        port = _random_port()
        transport, engine, engine_task = await _start_broker(port, port + 1)

        ctx = zmq.asyncio.Context()
        dealer = None
        sub = None
        try:
            dealer = ctx.socket(zmq.DEALER)
            dealer.setsockopt(zmq.IDENTITY, b"pub_id")
            dealer.connect(f"tcp://127.0.0.1:{port}")

            sub = ctx.socket(zmq.SUB)
            sub.connect(f"tcp://127.0.0.1:{port + 1}")
            sub.setsockopt(zmq.SUBSCRIBE, b"bench.e2e")

            sub_req = FrameCodec.encode(MsgType.SUB, "bench.e2e", 0, b"")
            await dealer.send_multipart(sub_req)
            await asyncio.sleep(0.05)

            data = get_preset_single()
            n = 5_000

            send_start = time.perf_counter()
            for _ in range(n):
                payload = FrameCodec.encode_payload(data, "msgpack", "none")
                frames = FrameCodec.encode(
                    MsgType.PUB, "bench.e2e", 1, payload, "msgpack", "none"
                )
                await dealer.send_multipart(frames)
            send_elapsed = time.perf_counter() - send_start

            recv_start = time.perf_counter()
            received = 0
            while received < n:
                try:
                    await asyncio.wait_for(sub.recv_multipart(), timeout=2.0)
                    received += 1
                except asyncio.TimeoutError:
                    break
            recv_elapsed = time.perf_counter() - recv_start

            print(f"\n  ZMQ E2E (msgpack+none, {n} msgs):")
            print(f"    send: {n / send_elapsed:,.0f} msg/s ({send_elapsed:.3f}s)")
            print(f"    recv: {received / recv_elapsed:,.0f} msg/s ({recv_elapsed:.3f}s)")
            print(f"    success: {received}/{n} ({received / n * 100:.1f}%)")

            assert received >= n * 0.95

        finally:
            await _cleanup(engine_task, transport, ctx, dealer, sub)

    @pytest.mark.parametrize("comp", ["snappy", "lz4", "zstd"])
    async def test_e2e_throughput_with_compression(self, comp):
        """各压缩格式端到端吞吐对比。"""
        port = _random_port()
        transport, engine, engine_task = await _start_broker(port, port + 1)

        ctx = zmq.asyncio.Context()
        dealer = None
        sub = None
        try:
            dealer = ctx.socket(zmq.DEALER)
            dealer.setsockopt(zmq.IDENTITY, b"pub_id")
            dealer.connect(f"tcp://127.0.0.1:{port}")

            sub = ctx.socket(zmq.SUB)
            sub.connect(f"tcp://127.0.0.1:{port + 1}")
            sub.setsockopt(zmq.SUBSCRIBE, f"bench.{comp}".encode())

            sub_req = FrameCodec.encode(MsgType.SUB, f"bench.{comp}", 0, b"")
            await dealer.send_multipart(sub_req)
            await asyncio.sleep(0.05)

            data = get_preset_single()
            n = 5_000

            send_start = time.perf_counter()
            for _ in range(n):
                payload = FrameCodec.encode_payload(data, "msgpack", comp)
                frames = FrameCodec.encode(
                    MsgType.PUB, f"bench.{comp}", 1, payload, "msgpack", comp
                )
                await dealer.send_multipart(frames)
            send_elapsed = time.perf_counter() - send_start

            recv_start = time.perf_counter()
            received = 0
            while received < n:
                try:
                    await asyncio.wait_for(sub.recv_multipart(), timeout=2.0)
                    received += 1
                except asyncio.TimeoutError:
                    break
            recv_elapsed = time.perf_counter() - recv_start

            print(f"\n  ZMQ E2E (msgpack+{comp}, {n} msgs):")
            print(f"    send: {n / send_elapsed:,.0f} msg/s")
            print(f"    recv: {received / recv_elapsed:,.0f} msg/s")
            print(f"    success: {received}/{n} ({received / n * 100:.1f}%)")

            assert received >= n * 0.90

        finally:
            await _cleanup(engine_task, transport, ctx, dealer, sub)


# ---- 多 Consumer ----

class TestMultiConsumer:
    """单 producer → 多 consumer 端到端。"""

    async def test_e2e_fanout_3_consumers(self):
        """扇出测试：1 PUB -> 3 SUB。"""
        port = _random_port()
        transport, engine, engine_task = await _start_broker(port, port + 1)

        ctx = zmq.asyncio.Context()
        dealer = None
        subs = []
        try:
            dealer = ctx.socket(zmq.DEALER)
            dealer.setsockopt(zmq.IDENTITY, b"pub_id")
            dealer.connect(f"tcp://127.0.0.1:{port}")

            for i in range(3):
                s = ctx.socket(zmq.SUB)
                s.connect(f"tcp://127.0.0.1:{port + 1}")
                s.setsockopt(zmq.SUBSCRIBE, b"bench.fanout")
                subs.append(s)

            sub_req = FrameCodec.encode(MsgType.SUB, "bench.fanout", 0, b"")
            await dealer.send_multipart(sub_req)
            await asyncio.sleep(0.05)

            data = get_preset_single()
            n = 3_000

            send_start = time.perf_counter()
            for _ in range(n):
                payload = FrameCodec.encode_payload(data, "msgpack", "none")
                frames = FrameCodec.encode(
                    MsgType.PUB, "bench.fanout", 1, payload, "msgpack", "none"
                )
                await dealer.send_multipart(frames)
            send_elapsed = time.perf_counter() - send_start

            results = []
            recv_start = time.perf_counter()
            for i, s in enumerate(subs):
                received = 0
                while received < n:
                    try:
                        await asyncio.wait_for(s.recv_multipart(), timeout=3.0)
                        received += 1
                    except asyncio.TimeoutError:
                        break
                results.append(received)
            recv_elapsed = time.perf_counter() - recv_start

            print(f"\n  ZMQ E2E fanout (1 PUB -> 3 SUB, {n} msgs):")
            print(f"    send: {n / send_elapsed:,.0f} msg/s")
            for i, r in enumerate(results):
                print(f"    Consumer {i}: {r}/{n} ({r / n * 100:.1f}%)")
            total_received = sum(results)
            print(f"    total: {total_received} ({total_received / (n * 3) * 100:.1f}%)")

        finally:
            await _cleanup(engine_task, transport, ctx, dealer, *subs)


# ---- 延迟测量 ----

class TestLatency:
    """单条消息端到端延迟。"""

    async def test_single_msg_latency(self):
        """PUB -> BROADCAST 端到端延迟分布。"""
        port = _random_port()
        transport, engine, engine_task = await _start_broker(port, port + 1)

        ctx = zmq.asyncio.Context()
        dealer = None
        sub = None
        try:
            dealer = ctx.socket(zmq.DEALER)
            dealer.setsockopt(zmq.IDENTITY, b"pub_id")
            dealer.connect(f"tcp://127.0.0.1:{port}")

            sub = ctx.socket(zmq.SUB)
            sub.connect(f"tcp://127.0.0.1:{port + 1}")
            sub.setsockopt(zmq.SUBSCRIBE, b"bench.lat")

            sub_req = FrameCodec.encode(MsgType.SUB, "bench.lat", 0, b"")
            await dealer.send_multipart(sub_req)
            await asyncio.sleep(0.1)

            data = get_preset_single()
            n = 500
            latencies: list[float] = []

            for _ in range(n):
                payload = FrameCodec.encode_payload(data, "msgpack", "none")
                frames = FrameCodec.encode(
                    MsgType.PUB, "bench.lat", 1, payload, "msgpack", "none"
                )
                send_ts = time.perf_counter()
                await dealer.send_multipart(frames)

                try:
                    await asyncio.wait_for(sub.recv_multipart(), timeout=1.0)
                    recv_ts = time.perf_counter()
                    latencies.append((recv_ts - send_ts) * 1_000_000)  # us
                except asyncio.TimeoutError:
                    pass

            if latencies:
                latencies.sort()
                p50 = latencies[len(latencies) // 2]
                p99 = latencies[int(len(latencies) * 0.99)]
                avg = sum(latencies) / len(latencies)
                print(f"\n  ZMQ E2E latency ({len(latencies)} samples, us):")
                print(f"    avg: {avg:.0f} | P50: {p50:.0f} | P99: {p99:.0f}")
                print(f"    min: {min(latencies):.0f} | max: {max(latencies):.0f}")

        finally:
            await _cleanup(engine_task, transport, ctx, dealer, sub)

    @pytest.mark.parametrize("comp", ["none", "snappy", "lz4", "zstd"])
    async def test_latency_by_compression(self, comp):
        """各压缩格式端到端延迟对比。"""
        port = _random_port()
        transport, engine, engine_task = await _start_broker(port, port + 1)

        ctx = zmq.asyncio.Context()
        dealer = None
        sub = None
        try:
            dealer = ctx.socket(zmq.DEALER)
            dealer.setsockopt(zmq.IDENTITY, b"pub_id")
            dealer.connect(f"tcp://127.0.0.1:{port}")

            sub = ctx.socket(zmq.SUB)
            sub.connect(f"tcp://127.0.0.1:{port + 1}")
            sub.setsockopt(zmq.SUBSCRIBE, f"bench.lat.{comp}".encode())

            sub_req = FrameCodec.encode(MsgType.SUB, f"bench.lat.{comp}", 0, b"")
            await dealer.send_multipart(sub_req)
            await asyncio.sleep(0.1)

            data = get_preset_single()
            n = 200
            latencies: list[float] = []

            for _ in range(n):
                payload = FrameCodec.encode_payload(data, "msgpack", comp)
                frames = FrameCodec.encode(
                    MsgType.PUB, f"bench.lat.{comp}", 1, payload, "msgpack", comp
                )
                send_ts = time.perf_counter()
                await dealer.send_multipart(frames)
                try:
                    await asyncio.wait_for(sub.recv_multipart(), timeout=1.0)
                    recv_ts = time.perf_counter()
                    latencies.append((recv_ts - send_ts) * 1_000_000)
                except asyncio.TimeoutError:
                    pass

            if latencies:
                latencies.sort()
                p50 = latencies[len(latencies) // 2]
                p99 = latencies[int(len(latencies) * 0.99)]
                avg = sum(latencies) / len(latencies)
                print(f"\n  E2E latency msgpack+{comp} ({len(latencies)} samples, us):")
                print(f"    avg: {avg:.0f} | P50: {p50:.0f} | P99: {p99:.0f}")

        finally:
            await _cleanup(engine_task, transport, ctx, dealer, sub)


# ---- PING/PONG RTT ----

class TestPingPongE2E:

    async def test_ping_pong_rtt(self):
        """PING -> PONG 往返延迟。"""
        port = _random_port()
        transport, engine, engine_task = await _start_broker(port, port + 1)

        ctx = zmq.asyncio.Context()
        dealer = None
        try:
            dealer = ctx.socket(zmq.DEALER)
            dealer.setsockopt(zmq.IDENTITY, b"pub_id")
            dealer.connect(f"tcp://127.0.0.1:{port}")
            await asyncio.sleep(0.1)

            n = 500
            rtts: list[float] = []

            for _ in range(n):
                payload = FrameCodec.encode_payload(
                    {"client_ts": time.time()}, "msgpack", "none"
                )
                frames = FrameCodec.encode(MsgType.PING, "", 0, payload)
                send_ts = time.perf_counter()
                await dealer.send_multipart(frames)

                try:
                    await asyncio.wait_for(dealer.recv_multipart(), timeout=1.0)
                    recv_ts = time.perf_counter()
                    rtts.append((recv_ts - send_ts) * 1_000_000)
                except asyncio.TimeoutError:
                    pass

            if rtts:
                rtts.sort()
                p50 = rtts[len(rtts) // 2]
                p99 = rtts[int(len(rtts) * 0.99)]
                avg = sum(rtts) / len(rtts)
                print(f"\n  PING/PONG RTT ({len(rtts)} samples, us):")
                print(f"    avg: {avg:.0f} | P50: {p50:.0f} | P99: {p99:.0f}")

        finally:
            await _cleanup(engine_task, transport, ctx, dealer)
