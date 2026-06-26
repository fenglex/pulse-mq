"""Server 组装测试。

Task 11 只解锁 start/stop 冒烟；routing e2e 需要 Client（Task 12），届时追加。
"""
from __future__ import annotations

import asyncio
import socket as _sock


def _free_port() -> int:
    s = _sock.socket(_sock.AF_INET, _sock.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


async def test_server_start_stop():
    dp, cp, ap = _free_port(), _free_port(), _free_port()
    srv = Server(
        data_endpoint=f"tcp://127.0.0.1:{dp}",
        control_endpoint=f"tcp://127.0.0.1:{cp}",
        admin_endpoint=f"127.0.0.1:{ap}",
        credentials={"alice": "secret"},
    )
    task = asyncio.create_task(srv.start())
    await asyncio.sleep(0.5)
    assert srv._running is True
    await srv.stop()
    await task
    assert srv._running is False


# 懒导入：让 collection 不依赖 pulsemq.server 存在之外的模块。
# Task 11 实现后 server 可导入；测试体内部如需 Client（Task 12）再延迟导入。
from pulsemq.server import Server  # noqa: E402
