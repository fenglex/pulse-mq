"""v3 协议 e2e：确认发布（4B ack_token）、per-topic seq 缺口对账、默认缓冲。

对账恒等式（drop_old 缓冲，无 age 限制，多进程池消费）：
    sent == received + evicted
    client 缺口 missing == evicted（被淘汰的帧造成 seq 跳跃）
"""
from __future__ import annotations

import asyncio
import socket as _sock

import time

import pytest

from pulsemq.client import ConsumerClient, ProducerClient
from pulsemq.server import Server
from tests import mp_callbacks


def _free_port() -> int:
    s = _sock.socket(_sock.AF_INET, _sock.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


async def _start_server(creds: dict[str, str]) -> tuple[Server, int, int]:
    dp, cp, ap = _free_port(), _free_port(), _free_port()
    srv = Server(
        data_endpoint=f"tcp://127.0.0.1:{dp}",
        control_endpoint=f"tcp://127.0.0.1:{cp}",
        admin_endpoint=f"127.0.0.1:{ap}",
        credentials=creds,
    )
    await srv.start()
    await asyncio.sleep(0.2)
    return srv, dp, cp


def _client(cls, dp: int, cp: int, user: str, **kw):
    return cls(
        data_endpoint=f"tcp://127.0.0.1:{dp}",
        control_endpoint=f"tcp://127.0.0.1:{cp}",
        username=user, password=user, **kw,
    )


async def test_confirm_publish_returns_server_seq():
    """confirm=True 逐条发布：ack 返回服务端 per-topic 递增 seq。"""
    srv, dp, cp = await _start_server({"p": "p"})
    try:
        p = _client(ProducerClient, dp, cp, "p")
        await p.start()
        s1 = await p.publish("mkt.tick", {"i": 1}, confirm=True, ack_timeout=5.0)
        s2 = await p.publish("mkt.tick", {"i": 2}, confirm=True, ack_timeout=5.0)
        s3 = await p.publish("mkt.other", {"i": 3}, confirm=True, ack_timeout=5.0)
        assert s1 == 1 and s2 == 2  # 同 topic 单调
        assert s3 == 1              # 不同 topic 独立计数
        # 普通发布不受影响（无返回值变化，但同样推进 seq）
        assert await p.publish("mkt.tick", {"i": 4}) is None
        s4 = await p.publish("mkt.tick", {"i": 5}, confirm=True, ack_timeout=5.0)
        assert s4 == 4              # 普通发布同样推进 seq
        await p.stop()
    finally:
        await srv.stop()


async def test_consumer_receives_seq_and_gap_accounting(record_dir):
    """v3 消费者收到的帧带服务端 seq；窗口内淘汰在客户端表现为缺口且可精确对账。

    锚点帧设计（确定性）：
      1. 先单独发 1 帧 → 消费者观测 seq=1（窗口起点）
      2. 洪泛 100 帧（缓冲 max_messages=4）→ 绝大多数被 drop_old 淘汰
      3. 等缓冲排空 → 再发 1 帧（消费者观测到本 topic 的最大 seq，窗口终点）

    恒等式：sent(102) = received + evicted，且 missing = evicted = sent - received。
    （缺口检测语义 = 观测窗口内的缺失，与 Iggy/Kafka offset 语义一致；
    首个观测帧之前的淘汰不可见，故测试必须用锚点帧覆盖整个窗口。）

    9.2.4：回调经模块级录制器写文件（worker 进程内执行，跨进程可验证）。
    """
    srv, dp, cp = await _start_server({"p": "p", "c": "c"})
    try:
        c = _client(ConsumerClient, dp, cp, "c",
                    buffer_policy="drop_old", buffer_cfg={"max_messages": 4})
        await c.start()
        p = _client(ProducerClient, dp, cp, "p")
        await p.start()
        await c.subscribe("bench.m1", mp_callbacks.on_msg_record)
        await asyncio.sleep(0.5)

        # 锚点起点：单发 1 帧，确保被观测
        await p.publish("bench.m1", {"i": 0})
        anchor = await mp_callbacks.wait_records(
            record_dir, 1, pred=lambda r: r["topic"] == "bench.m1")
        assert anchor[0]["seq"] == 1

        # 洪泛 100 帧：极小缓冲 → drop_old 大量淘汰
        for i in range(1, 101):
            await p.publish("bench.m1", {"i": i})
        # 等缓冲排空（无睡眠回调，drain 极快）
        await asyncio.sleep(1.0)

        # 锚点终点：排空后单发 1 帧（seq=102，必为最大观测 seq）
        await p.publish("bench.m1", {"i": 101})
        await asyncio.sleep(1.0)

        n_sent = 102
        recs = [r for r in mp_callbacks.read_records(record_dir)
                if r["topic"] == "bench.m1"]
        n_recv = len(recs)
        assert n_recv > 2
        assert all(isinstance(r["seq"], int) for r in recs)
        missing = c.gap_stats().get("bench.m1", {}).get("missing", 0)
        assert missing == n_sent - n_recv, (
            f"missing={missing} received={n_recv} sent={n_sent}")
        assert missing > 50  # 极小缓冲下确实发生了大量淘汰
        # 服务端聚合（心跳 gaps 上报）与客户端累计一致（9.2.8 起 GapStats.snapshot()）
        assert srv._gap_stats.snapshot().get("bench.m1") == missing
        await p.stop()
        await c.stop()
    finally:
        await srv.stop()


async def test_consumer_default_buffer_negotiated():
    """ConsumerClient 默认 drop_old + max_age 30s：REGISTER 后服务端生效。"""
    srv, dp, cp = await _start_server({"c": "c"})
    try:
        c = _client(ConsumerClient, dp, cp, "c")
        await c.start()
        await asyncio.sleep(0.2)
        snap = srv._buffers.snapshot()
        subs = snap["subscribers"]
        assert len(subs) == 1
        entry = next(iter(subs.values()))
        assert entry["policy"] == "drop_old"
        assert entry["max_age_s"] == 30.0
        # 9.2.8 默认上限：50 万帧 / 64MB
        assert entry["max_messages"] == 500_000
        assert entry["max_bytes"] == 64 * 1024 * 1024
        # 显式 opt-out：恢复直发行为（不建缓冲）
        await c.stop()
        c2 = _client(ConsumerClient, dp, cp, "c", buffer_policy=None,
                     buffer_cfg=None)
        await c2.start()
        await asyncio.sleep(0.2)
        snap2 = srv._buffers.snapshot()
        assert snap2["subscribers"] == {}
        await c2.stop()
    finally:
        await srv.stop()
