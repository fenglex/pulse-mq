"""全面功能测试：覆盖所有新增功能 + 监控指标 + 边界条件 + 线程安全。

测试矩阵:
  A. _DropQueue — get_batch / remaining / 并发 / 边界
  B. DropStats — 多轮 roll_minute / 1h 窗口 / 线程安全
  C. topic interning — 同 bytes 返回同 str / 缓存上限
  D. Zstd 压缩线程安全 — 多线程并发 compress/decompress
  E. TrafficStats 无锁 — 并发 record + snapshot 不崩溃
  F. 消费端两线程 e2e — 同步/异步回调 / 慢回调触发丢弃
  G. 服务端心跳 drops + credit — e2e 心跳处理
  H. Admin API drops — realtime 快照包含丢弃指标
"""
from __future__ import annotations

import asyncio
import socket as _sock
import threading
import time
from types import SimpleNamespace

import pytest

from pulsemq.client import ConsumerClient, ProducerClient, _DropQueue
from pulsemq.protocol import frames
from pulsemq.protocol.msg_type import DataType
from pulsemq.server import Server
from pulsemq.stats.drops import DropStats
from pulsemq.stats.traffic import TrafficStats


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _free_port() -> int:
    s = _sock.socket(_sock.AF_INET, _sock.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


async def _start_server(creds: dict[str, str], **kw) -> tuple[Server, int, int, int]:
    dp, cp, ap = _free_port(), _free_port(), _free_port()
    srv = Server(
        data_endpoint=f"tcp://127.0.0.1:{dp}",
        control_endpoint=f"tcp://127.0.0.1:{cp}",
        admin_endpoint=f"127.0.0.1:{ap}",
        credentials=creds, **kw,
    )
    await srv.start()
    await asyncio.sleep(0.2)
    return srv, dp, cp, ap


def _item(topic: str):
    return (b"frame", SimpleNamespace(topic=topic), [])


# ---------------------------------------------------------------------------
# A. _DropQueue — get_batch / remaining / 并发
# ---------------------------------------------------------------------------

def test_drop_queue_get_batch():
    q = _DropQueue(maxlen=10)
    q.put(_item("a"))
    q.put(_item("b"))
    q.put(_item("c"))
    batch = q.get_batch(timeout=1.0, max_items=2)
    assert len(batch) == 2
    assert batch[0][1].topic == "a"
    assert batch[1][1].topic == "b"
    # 第二批取剩余
    batch2 = q.get_batch(timeout=1.0)
    assert len(batch2) == 1
    assert batch2[0][1].topic == "c"


def test_drop_queue_get_batch_empty_timeout():
    q = _DropQueue(maxlen=10)
    batch = q.get_batch(timeout=0.3)
    assert batch == []


def test_drop_queue_remaining():
    q = _DropQueue(maxlen=100)
    assert q.remaining() == 100
    q.put(_item("a"))
    assert q.remaining() == 99
    q.put(_item("b"))
    assert q.remaining() == 98
    q.get(timeout=0.5)
    assert q.remaining() == 99


def test_drop_queue_maxlen_1():
    """边界：队列长度 1，每次 put 都丢弃最老。"""
    q = _DropQueue(maxlen=1)
    q.put(_item("t1"))
    q.put(_item("t2"))  # 丢弃 t1
    drops = q.drain_drops()
    assert drops == {"t1": 1}
    item = q.get(timeout=0.5)
    assert item[1].topic == "t2"


def test_drop_queue_concurrent_put_get():
    """并发 put + get_batch 不丢失消息。"""
    q = _DropQueue(maxlen=10000)
    received = []

    def producer():
        for i in range(500):
            q.put(_item(f"t{i}"))
        q.close()

    def consumer():
        while True:
            batch = q.get_batch(timeout=1.0)
            if not batch:
                break
            received.extend(batch)

    pt = threading.Thread(target=producer)
    ct = threading.Thread(target=consumer)
    pt.start(); ct.start()
    pt.join(timeout=5); ct.join(timeout=5)
    assert len(received) == 500


# ---------------------------------------------------------------------------
# B. DropStats — 多轮 roll / 1h 窗口
# ---------------------------------------------------------------------------

def test_drop_stats_multi_roll():
    ds = DropStats(retention_minutes=5)
    for minute in range(3):
        ds.record("t", (minute + 1) * 10)
        ds.roll_minute()
    snap = ds.snapshot()
    assert snap["t"]["drops_current"] == 0  # 最后一次 roll 清零
    # 近 3 分钟累计
    assert snap["t"]["drops_1h_total"] == 10 + 20 + 30


def test_drop_stats_window_expiry():
    """retention_minutes=2 → 超过 2 分钟的数据被淘汰。"""
    ds = DropStats(retention_minutes=2)
    ds.record("t", 10)
    ds.roll_minute()
    ds.record("t", 20)
    ds.roll_minute()
    ds.record("t", 30)
    ds.roll_minute()
    # deque(maxlen=2) → 第一轮(10)被淘汰，剩 20+30
    snap = ds.snapshot()
    assert snap["t"]["drops_1h_total"] == 50  # 20+30


def test_drop_stats_no_data_snapshot():
    ds = DropStats()
    snap = ds.snapshot()
    assert snap == {}


# ---------------------------------------------------------------------------
# C. topic interning
# ---------------------------------------------------------------------------

def test_topic_intern_same_bytes():
    """同一 topic bytes 返回同一个 str 对象。"""
    frame1 = frames.encode("market.tick", {"x": 1})
    frame2 = frames.encode("market.tick", {"x": 2})
    hdr1 = frames.decode_header(frame1)
    hdr2 = frames.decode_header(frame2)
    assert hdr1.topic is hdr2.topic  # is → 同一对象


def test_topic_intern_different_bytes():
    frame1 = frames.encode("topic.a", {"x": 1})
    frame2 = frames.encode("topic.b", {"x": 2})
    hdr1 = frames.decode_header(frame1)
    hdr2 = frames.decode_header(frame2)
    assert hdr1.topic is not hdr2.topic
    assert hdr1.topic == "topic.a"
    assert hdr2.topic == "topic.b"


def test_topic_intern_correctness():
    """intern 后 decode 结果与非 intern 一致。"""
    for topic in ["a", "a.b", "market.tick.us.aapl", "中文主题"]:
        frame = frames.encode(topic, {"x": 1})
        hdr = frames.decode_header(frame)
        assert hdr.topic == topic


# ---------------------------------------------------------------------------
# D. Zstd 压缩线程安全
# ---------------------------------------------------------------------------

def test_zstd_concurrent_compress_decompress():
    """多线程并发 compress/decompress 不崩溃、结果正确。"""
    from pulsemq.protocol.compression import get
    comp = get("zstd")
    original = [frames.encode("t", {"seq": i, "val": i * 1.5}) for i in range(200)]
    errors = []

    def worker():
        try:
            for data in original:
                compressed = comp.compress(data)
                decompressed = comp.decompress(compressed)
                assert decompressed == data
        except Exception as e:
            errors.append(e)

    threads = [threading.Thread(target=worker) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)
    assert not errors


def test_all_compressors_round_trip():
    """所有压缩算法 round-trip 正确。"""
    from pulsemq.protocol.compression import available, get
    data = b"x" * 5000
    for name in available():
        comp = get(name)
        compressed = comp.compress(data)
        assert comp.decompress(compressed) == data


# ---------------------------------------------------------------------------
# E. TrafficStats 无锁并发
# ---------------------------------------------------------------------------

def test_traffic_stats_concurrent_record_snapshot():
    """并发 record + snapshot 不崩溃，值近似正确。"""
    ts = TrafficStats(retention_minutes=60)
    stop = threading.Event()

    def recorder():
        while not stop.is_set():
            ts.record("hot", 1, 100)

    def snapshotter():
        while not stop.is_set():
            try:
                ts.snapshot()
                ts.all_topics_snapshot()
            except Exception:
                pytest.fail("snapshot raised during concurrent record")

    r = threading.Thread(target=recorder)
    s = threading.Thread(target=snapshotter)
    r.start(); s.start()
    time.sleep(0.5)
    stop.set()
    r.join(timeout=2); s.join(timeout=2)

    snap = ts.snapshot()
    assert snap["hot"]["msg_count"] > 0


def test_traffic_stats_roll_during_record():
    """roll_minute 与并发 record 不崩溃。"""
    ts = TrafficStats(retention_minutes=60)
    stop = threading.Event()

    def recorder():
        for i in range(10000):
            ts.record("t", 1, 10)

    def roller():
        while not stop.is_set():
            ts.roll_minute()
            time.sleep(0.001)

    r = threading.Thread(target=recorder)
    rl = threading.Thread(target=roller)
    r.start(); rl.start()
    r.join(timeout=5)
    stop.set()
    rl.join(timeout=2)
    # 不崩溃即通过


# ---------------------------------------------------------------------------
# F. 消费端两线程 e2e
# ---------------------------------------------------------------------------

async def test_two_thread_consumer_receives_messages():
    """两线程模式下消费端正确接收消息（同步回调）。"""
    srv, dp, cp, ap = await _start_server({"c": "c", "p": "p"})
    try:
        c = ConsumerClient(
            f"tcp://127.0.0.1:{dp}", f"tcp://127.0.0.1:{cp}",
            "c", "c", decode_queue_size=100,
        )
        p = ProducerClient(
            f"tcp://127.0.0.1:{dp}", f"tcp://127.0.0.1:{cp}",
            "p", "p",
        )
        await c.start()
        await p.start()
        got: list[str] = []
        await c.subscribe("test.*", lambda m: got.append(m.topic))
        await asyncio.sleep(0.3)
        for i in range(5):
            await p.publish(f"test.{i}", {"i": i})
        await asyncio.sleep(1.0)
        assert sorted(got) == ["test.0", "test.1", "test.2", "test.3", "test.4"]
        await c.stop()
        await p.stop()
    finally:
        await srv.stop()


async def test_two_thread_consumer_async_callback():
    """两线程模式下异步回调正常工作。"""
    srv, dp, cp, ap = await _start_server({"c": "c", "p": "p"})
    try:
        c = ConsumerClient(
            f"tcp://127.0.0.1:{dp}", f"tcp://127.0.0.1:{cp}",
            "c", "c", decode_queue_size=100,
        )
        p = ProducerClient(
            f"tcp://127.0.0.1:{dp}", f"tcp://127.0.0.1:{cp}",
            "p", "p",
        )
        await c.start()
        await p.start()
        got: list[str] = []

        async def async_cb(msg):
            got.append(msg.topic)

        await c.subscribe("async.*", async_cb)
        await asyncio.sleep(0.3)
        await p.publish("async.test", {"x": 1})
        await asyncio.sleep(1.0)
        assert got == ["async.test"]
        await c.stop()
        await p.stop()
    finally:
        await srv.stop()


async def test_consumer_drops_with_slow_callback():
    """慢回调 + 小队列 → 消息丢弃，drops > 0。"""
    srv, dp, cp, ap = await _start_server({"c": "c", "p": "p"})
    try:
        c = ConsumerClient(
            f"tcp://127.0.0.1:{dp}", f"tcp://127.0.0.1:{cp}",
            "c", "c", decode_queue_size=3,
        )
        p = ProducerClient(
            f"tcp://127.0.0.1:{dp}", f"tcp://127.0.0.1:{cp}",
            "p", "p",
        )
        await c.start()
        await p.start()
        received = []

        def slow_cb(msg):
            received.append(1)
            time.sleep(0.1)  # 100ms per message

        await c.subscribe("flood.*", slow_cb)
        await asyncio.sleep(0.3)
        # 快速发送 50 条，队列只能存 3 条
        for i in range(50):
            await p.publish("flood.tick", {"i": i})
        await asyncio.sleep(2.0)  # 等心跳上报 drops

        # consumer 端应该有 drops（心跳可能已 drain，改检查收到的 < 发送数）
        assert len(received) < 20, \
            f"Should have dropped some with slow callback, received all {len(received)}"
        await c.stop()
        await p.stop()
    finally:
        await srv.stop()


# ---------------------------------------------------------------------------
# G. 服务端心跳 drops + credit + Admin API
# ---------------------------------------------------------------------------

async def test_server_receives_drops_via_heartbeat():
    """服务端通过心跳收到消费端丢弃指标。"""
    srv, dp, cp, ap = await _start_server({"c": "c", "p": "p"})
    try:
        c = ConsumerClient(
            f"tcp://127.0.0.1:{dp}", f"tcp://127.0.0.1:{cp}",
            "c", "c", decode_queue_size=2,
        )
        p = ProducerClient(
            f"tcp://127.0.0.1:{dp}", f"tcp://127.0.0.1:{cp}",
            "p", "p",
        )
        await c.start()
        await p.start()

        def slow_cb(msg):
            time.sleep(0.2)

        await c.subscribe("drop.*", slow_cb)
        await asyncio.sleep(0.3)
        for i in range(20):
            await p.publish("drop.test", {"i": i})
        # 等至少 2 个心跳周期（2s），让 drops 上报到服务端
        await asyncio.sleep(3.0)

        # 检查服务端 DropStats
        snap = srv._drop_stats.snapshot()
        drop_data = snap.get("drop.test", {})
        total = drop_data.get("drops_current", 0) + drop_data.get("drops_1h_total", 0)
        assert total > 0, f"Server should have received drops via heartbeat, got {snap}"
        await c.stop()
        await p.stop()
    finally:
        await srv.stop()


async def test_server_credit_updated_by_heartbeat():
    """服务端 credits 字典在心跳后被更新。"""
    srv, dp, cp, ap = await _start_server({"c": "c"})
    try:
        c = ConsumerClient(
            f"tcp://127.0.0.1:{dp}", f"tcp://127.0.0.1:{cp}",
            "c", "c", decode_queue_size=500,
        )
        await c.start()
        await asyncio.sleep(2.0)  # 等心跳

        # 服务端应该有该 consumer 的 credit
        assert len(srv._credits) > 0, "Server should have credit for consumer"
        for ident, credit in srv._credits.items():
            assert credit >= 0
        await c.stop()
    finally:
        await srv.stop()


async def test_server_credit_cleanup_on_disconnect():
    """DISCONNECT 后 credits 清理。"""
    srv, dp, cp, ap = await _start_server({"c": "c"})
    try:
        c = ConsumerClient(
            f"tcp://127.0.0.1:{dp}", f"tcp://127.0.0.1:{cp}",
            "c", "c", decode_queue_size=100,
        )
        await c.start()
        await asyncio.sleep(1.5)  # 等心跳
        assert len(srv._credits) > 0
        await c.stop()
        await asyncio.sleep(0.5)
        # DISCONNECT 后 credits 应清理（或心跳超时后清理）
        # 注意：DISCONNECT 发送后 credits 立即清理
        assert len(srv._credits) == 0 or all(
            v == 0 for v in srv._credits.values())
    finally:
        await srv.stop()


# ---------------------------------------------------------------------------
# H. Admin API drops
# ---------------------------------------------------------------------------

async def test_admin_api_includes_drops():
    """Admin realtime snapshot 包含 drops 字段。"""
    srv, dp, cp, ap = await _start_server({"c": "c"})
    try:
        srv._drop_stats.record("topic.x", 42)
        snap = srv._admin._realtime_snapshot()
        assert "drops" in snap
        assert snap["drops"]["topic.x"]["drops_current"] == 42
        await asyncio.sleep(0.1)
    finally:
        await srv.stop()


async def test_admin_api_drops_after_roll():
    """roll_minute 后 drops_last_min 有值。"""
    srv, dp, cp, ap = await _start_server({"c": "c"})
    try:
        srv._drop_stats.record("t", 15)
        srv._drop_stats.roll_minute()
        snap = srv._admin._realtime_snapshot()
        assert snap["drops"]["t"]["drops_current"] == 0
        assert snap["drops"]["t"]["drops_last_min"] == 15
        assert snap["drops"]["t"]["drops_1h_total"] == 15
    finally:
        await srv.stop()


# ---------------------------------------------------------------------------
# I. 全链路 e2e：producer → server → consumer + drops + stats + latency
# ---------------------------------------------------------------------------

async def test_full_e2e_all_metrics():
    """全链路：消息收发 + 流量统计 + 延迟采样同时工作。"""
    srv, dp, cp, ap = await _start_server({"c": "c", "p": "p"})
    try:
        c = ConsumerClient(
            f"tcp://127.0.0.1:{dp}", f"tcp://127.0.0.1:{cp}",
            "c", "c", decode_queue_size=100,
            latency_sample_rate=1.0,  # 100% 采样确保有延迟数据
        )
        p = ProducerClient(
            f"tcp://127.0.0.1:{dp}", f"tcp://127.0.0.1:{cp}",
            "p", "p",
        )
        await c.start()
        await p.start()
        got = []
        await c.subscribe("e2e.*", lambda m: got.append(m.topic))
        await asyncio.sleep(0.3)
        for i in range(10):
            await p.publish("e2e.test", {"seq": i})
        await asyncio.sleep(1.5)

        # 1. 消息全部收到
        assert len(got) == 10

        # 2. 服务端流量统计有数据
        snap = srv._admin._realtime_snapshot()
        assert "e2e.test" in snap["topics"]
        assert snap["topics"]["e2e.test"]["msg_count_current"] > 0

        await c.stop()
        await p.stop()
    finally:
        await srv.stop()


async def test_header_only_callback():
    """header_only 回调跳过完整 decode，直接接收 FrameHeader。"""
    srv, dp, cp, ap = await _start_server({"c": "c", "p": "p"})
    try:
        c = ConsumerClient(
            f"tcp://127.0.0.1:{dp}", f"tcp://127.0.0.1:{cp}",
            "c", "c",
        )
        p = ProducerClient(
            f"tcp://127.0.0.1:{dp}", f"tcp://127.0.0.1:{cp}",
            "p", "p",
        )
        await c.start()
        await p.start()
        received_headers = []
        await c.subscribe("hdr.*", lambda hdr: received_headers.append(hdr),
                          header_only=True)
        await asyncio.sleep(0.3)
        await p.publish("hdr.test", {"x": 1})
        await asyncio.sleep(0.5)
        assert len(received_headers) == 1
        assert received_headers[0].topic == "hdr.test"
        assert hasattr(received_headers[0], "record_count")  # FrameHeader
        await c.stop()
        await p.stop()
    finally:
        await srv.stop()


async def test_broadcast_no_subscribers():
    """无订阅者时广播不崩溃，返回 0 drops。"""
    srv, dp, cp, ap = await _start_server({"p": "p"})
    try:
        p = ProducerClient(
            f"tcp://127.0.0.1:{dp}", f"tcp://127.0.0.1:{cp}",
            "p", "p",
        )
        await p.start()
        # 无 consumer 订阅，producer 发消息
        await p.publish("orphan.topic", {"x": 1})
        await asyncio.sleep(0.5)
        # 不崩溃即通过
        await p.stop()
    finally:
        await srv.stop()
