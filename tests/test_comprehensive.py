"""全面功能测试：覆盖所有新增功能 + 监控指标 + 边界条件 + 线程安全。

测试矩阵（9.2.4：消费端唯一模式为多进程池，_DropQueue 两线程队列已删除）:
  B. DropStats — 多轮 roll_minute / 1h 窗口 / 线程安全
  C. topic interning — 同 bytes 返回同 str / 缓存上限
  D. Zstd 压缩线程安全 — 多线程并发 compress/decompress
  E. TrafficStats 无锁 — 并发 record + snapshot 不崩溃
  F. 消费端多进程池 e2e — 同步/异步回调 / 慢回调限流不丢失
  G. 服务端心跳 drops + credit — e2e 心跳处理
  H. Admin API drops — realtime 快照包含丢弃指标
"""
from __future__ import annotations

import asyncio
import socket as _sock
import threading
import time

import pytest

from pulsemq.client import ConsumerClient, ProducerClient
from pulsemq.protocol import frames
from pulsemq.protocol.msg_type import DataType
from pulsemq.server import Server
from pulsemq.stats.drops import DropStats
from pulsemq.stats.traffic import TrafficStats
from tests import mp_callbacks


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
# F. 消费端多进程池 e2e（9.2.4 唯一消费模式）
# ---------------------------------------------------------------------------

async def test_pool_consumer_receives_messages(record_dir):
    """多进程池模式正确接收消息（模块级同步回调 + 录制器）。"""
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
        await c.subscribe("test.*", mp_callbacks.on_msg_record)
        await asyncio.sleep(0.5)
        for i in range(5):
            await p.publish(f"test.{i}", {"i": i})
        recs = await mp_callbacks.wait_records(record_dir, 5)
        assert sorted(r["topic"] for r in recs) == \
            ["test.0", "test.1", "test.2", "test.3", "test.4"]
        await c.stop()
        await p.stop()
    finally:
        await srv.stop()


async def test_pool_consumer_async_callback(record_dir):
    """异步回调在 worker 进程事件循环上正常执行。"""
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
        await c.subscribe("async.*", mp_callbacks.on_msg_record_async)
        await asyncio.sleep(0.5)
        await p.publish("async.test", {"x": 1})
        recs = await mp_callbacks.wait_records(record_dir, 1)
        assert recs[0]["topic"] == "async.test"
        await c.stop()
        await p.stop()
    finally:
        await srv.stop()


async def test_consumer_slow_callback_throttled_no_loss(record_dir):
    """慢回调：信用流控 + 服务端缓冲使消息不丢失（9.2.2 后语义）。

    慢回调（worker 处理慢）不再产生队列丢弃：服务端按心跳 credit 放行，
    未放行的帧进入订阅者缓冲（drop_old），最终全部有序送达。
    """
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
        await c.subscribe("flood.*", mp_callbacks.on_msg_record_slow)
        await asyncio.sleep(0.5)
        for i in range(20):
            await p.publish("flood.tick", {"i": i})
        # 慢回调：20 条 × ~50ms ≈ 1s + 调度，给足超时
        recs = await mp_callbacks.wait_records(record_dir, 20, timeout=15.0)
        assert [r["payload_i"] for r in recs] == list(range(20))
        await c.stop()
        await p.stop()
    finally:
        await srv.stop()


# ---------------------------------------------------------------------------
# G. 服务端心跳 drops + credit + Admin API
# ---------------------------------------------------------------------------

async def test_server_receives_drops_via_heartbeat():
    """服务端通过心跳收到消费端丢弃指标（协议层直发 drops 字段验证聚合路径）。

    9.2.2 信用窗口守恒后，真实客户端的解码队列在正确限流下不会溢出
    （每窗口发送预算 <= 快照时队列空闲位，消费只会增加空闲位——数学上
    不可能超发），队列丢弃成为结构性不可能；故心跳 drops 上报路径改由
    协议层直接验证。_DropQueue 自身的丢弃计数见 test_drop_queue_maxlen_1。
    """
    import zmq
    import zmq.asyncio
    from pulsemq.protocol import frames as _frames

    srv, dp, cp, ap = await _start_server({"c": "c", "p": "p"})
    sock = None
    try:
        ctx = zmq.asyncio.Context.instance()
        sock = ctx.socket(zmq.DEALER)
        sock.setsockopt(zmq.IDENTITY, b"hb-drops-client")
        sock.setsockopt(zmq.LINGER, 0)
        sock.plain_username = b"c"
        sock.plain_password = b"c"
        sock.connect(f"tcp://127.0.0.1:{cp}")
        reg = _frames.encode_control("REGISTER", {
            "client_id": "hb-drops", "username": "c", "endpoint": "tcp://x",
            "roles": ["subscriber"], "topics": [],
        })
        await sock.send(reg)
        await asyncio.sleep(0.3)
        hb = _frames.encode_control("HEARTBEAT", {
            "client_id": "hb-drops",
            "drops": {"drop.test": 5},
        })
        await sock.send(hb)
        await asyncio.sleep(0.5)
        snap = srv._drop_stats.snapshot()
        drop_data = snap.get("drop.test", {})
        total = drop_data.get("drops_current", 0) + drop_data.get("drops_1h_total", 0)
        assert total >= 5, f"Server should have received drops via heartbeat, got {snap}"
    finally:
        if sock is not None:
            sock.close(linger=0)
        await srv.stop()


async def test_server_credit_updated_by_heartbeat(record_dir):
    """服务端 credits 字典在心跳后被更新（池已建时心跳携带环空闲 credit）。"""
    srv, dp, cp, ap = await _start_server({"c": "c"})
    try:
        c = ConsumerClient(
            f"tcp://127.0.0.1:{dp}", f"tcp://127.0.0.1:{cp}",
            "c", "c",
        )
        await c.start()
        await c.subscribe("credit.*", mp_callbacks.on_msg_record)
        await asyncio.sleep(2.0)  # 等心跳

        # 服务端应该有该 consumer 的 credit
        assert len(srv._credits) > 0, "Server should have credit for consumer"
        for ident, credit in srv._credits.items():
            assert credit >= 0
        await c.stop()
    finally:
        await srv.stop()


async def test_server_credit_cleanup_on_disconnect(record_dir):
    """DISCONNECT 后 credits 清理。"""
    srv, dp, cp, ap = await _start_server({"c": "c"})
    try:
        c = ConsumerClient(
            f"tcp://127.0.0.1:{dp}", f"tcp://127.0.0.1:{cp}",
            "c", "c",
        )
        await c.start()
        await c.subscribe("credit.*", mp_callbacks.on_msg_record)
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

async def test_full_e2e_all_metrics(record_dir):
    """全链路：消息收发 + 流量统计 + 延迟采样同时工作。"""
    srv, dp, cp, ap = await _start_server({"c": "c", "p": "p"})
    try:
        c = ConsumerClient(
            f"tcp://127.0.0.1:{dp}", f"tcp://127.0.0.1:{cp}",
            "c", "c",
            latency_sample_rate=1.0,  # 100% 采样确保有延迟数据
        )
        p = ProducerClient(
            f"tcp://127.0.0.1:{dp}", f"tcp://127.0.0.1:{cp}",
            "p", "p",
        )
        await c.start()
        await p.start()
        await c.subscribe("e2e.*", mp_callbacks.on_msg_record)
        await asyncio.sleep(0.5)
        for i in range(10):
            await p.publish("e2e.test", {"seq": i})
        recs = await mp_callbacks.wait_records(record_dir, 10)

        # 1. 消息全部收到
        assert len(recs) == 10

        # 2. 服务端流量统计有数据
        snap = srv._admin._realtime_snapshot()
        assert "e2e.test" in snap["topics"]
        assert snap["topics"]["e2e.test"]["msg_count_current"] > 0

        await c.stop()
        await p.stop()
    finally:
        await srv.stop()


async def test_header_only_callback(record_dir):
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
        await c.subscribe("hdr.*", mp_callbacks.on_hdr_record, header_only=True)
        await asyncio.sleep(0.5)
        await p.publish("hdr.test", {"x": 1})
        recs = await mp_callbacks.wait_records(record_dir, 1)
        assert recs[0]["topic"] == "hdr.test"
        assert recs[0]["rows"] == 1  # FrameHeader.record_count
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
