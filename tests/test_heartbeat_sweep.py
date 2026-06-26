"""Finding 3：心跳超时 → routing 清理的集成测试。

方案（文档化）：用极短的 heartbeat_timeout + 1s sweep 间隔。先验证订阅收到消息，
再停止消费者整体（close socket），让服务端心跳扫描（非立即 disconnect 检测）
清理 routing。白盒断言 server._routing.snapshot() 不再含该 identity。
"""
from __future__ import annotations

import asyncio
import socket as _sock

from pulsemq.client import ConsumerClient, ProducerClient
from pulsemq.config import ServerConfig
from pulsemq.server import Server


def _free_port() -> int:
    s = _sock.socket(_sock.AF_INET, _sock.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


async def test_heartbeat_timeout_clears_routing():
    dp, cp, ap = _free_port(), _free_port(), _free_port()
    cfg = ServerConfig(
        data_endpoint=f"tcp://127.0.0.1:{dp}",
        control_endpoint=f"tcp://127.0.0.1:{cp}",
        admin_endpoint=f"127.0.0.1:{ap}",
        heartbeat_timeout=1.0,
    )
    srv = Server(
        data_endpoint=f"tcp://127.0.0.1:{dp}",
        control_endpoint=f"tcp://127.0.0.1:{cp}",
        admin_endpoint=f"127.0.0.1:{ap}",
        credentials={"c": "c", "p": "p"},
        config=cfg,
    )
    await srv.start()
    try:
        await asyncio.sleep(0.2)
        c = ConsumerClient(
            data_endpoint=f"tcp://127.0.0.1:{dp}",
            control_endpoint=f"tcp://127.0.0.1:{cp}",
            username="c",
            password="c",
        )
        p = ProducerClient(
            data_endpoint=f"tcp://127.0.0.1:{dp}",
            control_endpoint=f"tcp://127.0.0.1:{cp}",
            username="p",
            password="p",
        )
        await c.start()
        await p.start()
        got: list[str] = []
        await c.subscribe("topic.x", lambda m: got.append(m.topic))
        await asyncio.sleep(0.4)
        # 1) 正常路径：发布被转发
        await p.publish("topic.x", {"i": 1})
        await asyncio.sleep(0.5)
        assert got == ["topic.x"]
        # routing 表此刻应有一条订阅
        assert srv._routing.snapshot(), "routing 应非空"

        # 2) 完全停掉 consumer（关 socket）；不发送 DISCONNECT（stop 会发，
        #    这里手动 cancel 后用 transport.close 模拟"非干净断开"）。
        #    为保证不走 DISCONNECT 清理路径，直接关闭其 transport。
        for t in (c._recv_task, c._hb_task):
            if t:
                t.cancel()
        for t in (c._recv_task, c._hb_task):
            if t:
                try:
                    await t
                except (asyncio.CancelledError, Exception):
                    pass
        c._recv_task = None
        c._hb_task = None
        await c._transport.close()
        c._stop.set()

        # 3) 等 heartbeat_timeout(1s) + sweep(1s) + 余量
        await asyncio.sleep(1.0 + 1.0 + 1.0)

        # 4) routing 应已被清空（心跳扫描驱逐）
        assert srv._routing.snapshot() == {}, "routing 应已被心跳扫描清理"

        # 5) 再次发布不应被转发给已死 consumer（无异常即可）
        await p.publish("topic.x", {"i": 2})
        await asyncio.sleep(0.4)
        assert got == ["topic.x"]  # 没有第二条
        await p.stop()
    finally:
        await srv.stop()
