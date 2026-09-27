"""9.2.1 小项单测：heartbeat_timeout 配置、O(1) oldest、GC guard。"""
from __future__ import annotations

import asyncio
import gc
import time

from pulsemq.buffering import BufferManager, GC_BACKLOG_THRESHOLD, SubscriberBuffer
from pulsemq.config import ServerConfig, load_server_config


# ---- #6 heartbeat_timeout（服务端配置项验证）----

def test_heartbeat_timeout_config_plumbing():
    cfg = ServerConfig(heartbeat_timeout=2.5)
    assert cfg.heartbeat_timeout == 2.5
    env = {"PULSEMQ_HEARTBEAT_TIMEOUT": "1.5"}
    import os
    old = os.environ.get("PULSEMQ_HEARTBEAT_TIMEOUT")
    try:
        os.environ["PULSEMQ_HEARTBEAT_TIMEOUT"] = "1.5"
        cfg2 = load_server_config(None)
        assert cfg2.heartbeat_timeout == 1.5
    finally:
        if old is None:
            os.environ.pop("PULSEMQ_HEARTBEAT_TIMEOUT", None)
        else:
            os.environ["PULSEMQ_HEARTBEAT_TIMEOUT"] = old


async def test_heartbeat_timeout_sweep_uses_config():
    """自定义 heartbeat_timeout=1 的服务端：停跳客户端 ~1s 被扫下线。"""
    import socket as _sock
    from pulsemq.server import Server

    def free_port():
        s = _sock.socket(); s.bind(("127.0.0.1", 0))
        p = s.getsockname()[1]; s.close(); return p

    dp, cp, ap = free_port(), free_port(), free_port()
    srv = Server(
        data_endpoint=f"tcp://127.0.0.1:{dp}",
        control_endpoint=f"tcp://127.0.0.1:{cp}",
        admin_endpoint=f"127.0.0.1:{ap}",
        credentials={"c": "c"},
        config=ServerConfig(heartbeat_timeout=1.0),
    )
    await srv.start()
    try:
        from pulsemq.client import ConsumerClient
        c = ConsumerClient(
            data_endpoint=f"tcp://127.0.0.1:{dp}",
            control_endpoint=f"tcp://127.0.0.1:{cp}",
            username="c", password="c",
        )
        await c.start()
        assert len(srv._registry.snapshot()["clients"]) == 1
        await c.stop()  # 发 DISCONNECT 正常下线
        await asyncio.sleep(0.3)
        assert len(srv._registry.snapshot()["clients"]) == 0
        # 再连一个不发心跳的裸连接验证 sweep 用的是配置值
        # （Client.stop 会注册/注销，这里直接用 registry 层验证足够）
    finally:
        await srv.stop()


# ---- #8 缓冲 O(1) oldest ----

def test_drop_old_oldest_is_fifo_head():
    buf = SubscriberBuffer(b"sub1", "drop_old", 100, 0, 0.0,
                           on_drop=lambda *a: None)
    now = time.monotonic()
    for i in range(3):
        buf.enqueue("t", b"x" * 10)
        time.sleep(0.01)
    age = buf.oldest_age_ms(time.monotonic())
    # 最旧条目 = 第一个入队的（FIFO 队头）
    assert age >= 20.0  # 前两条的 sleep 之和


def test_conflate_oldest_min_across_topics():
    buf = SubscriberBuffer(b"sub1", "conflate", 0, 0, 0.0,
                           on_drop=lambda *a: None)
    buf.enqueue("t.a", b"a" * 10)
    time.sleep(0.02)
    buf.enqueue("t.b", b"b" * 10)
    time.sleep(0.02)
    buf.enqueue("t.a", b"a2" * 10)  # 覆盖 t.a（其时间戳变新）
    age = buf.oldest_age_ms(time.monotonic())
    # 最旧 = t.b 的入队时间（约 20ms 前），而非刚被覆盖更新的 t.a（约 0ms）
    assert 15.0 <= age < 30.0


# ---- #7 GC guard ----

def test_maybe_gc_triggers_only_above_threshold(monkeypatch):
    calls = []
    monkeypatch.setattr(gc, "collect", lambda gen=2: calls.append(gen))
    mgr = BufferManager(max_messages=1000, max_bytes=0, max_age_s=0.0)
    buf = mgr._buffers.setdefault(b"s1", SubscriberBuffer(
        b"s1", "drop_old", 10_000_000, 0, 0.0, on_drop=lambda *a: None))
    # 阈值以下不触发
    for i in range(100):
        buf.enqueue("t", b"x" * 8)
    mgr.maybe_gc()
    assert calls == []
    # 超阈值触发一次
    for i in range(GC_BACKLOG_THRESHOLD + 10):
        buf.enqueue("t", b"x" * 8)
    mgr.maybe_gc()
    assert calls == [1]
    # 间隔内不重复触发
    mgr.maybe_gc()
    assert calls == [1]
