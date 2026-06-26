"""ZAP handler 健壮性测试。

覆盖 B4 回归：AsyncZAPHandler._loop() 的 3 处 send_multipart 响应未做异常保护，
一次 send 异常会让 ZAP task 静默死亡，后续所有 SUB 的 PLAIN 认证永久挂死。

本测试用桩 socket 注入「请求正常 + 响应失败」序列，直接驱动 _loop 的一次迭代，
断言 _loop 在 send 失败后仍存活（能处理后续请求），而非整体退出。
注意：被测对象是 _loop 的错误处理逻辑，桩 socket 仅用于注入确定性的输入/失败。
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
import zmq

from pulsemq.transport.zmq_pub import AsyncZAPHandler


class _StubZapSocket:
    """最小化桩 socket：按预设序列返回 recv 帧 / 模拟 send 失败。

    - recv_multipart: 依次产出 self._recv_queue 中的帧组
    - send_multipart: 若 self._fail_send 为 True，抛 zmq.ZMQError
    """

    def __init__(self) -> None:
        self._recv_queue: list[list[bytes]] = []
        self._fail_send = False
        self.send_calls = 0

    def queue_recv(self, frames: list[bytes]) -> None:
        self._recv_queue.append(frames)

    def fail_next_send(self) -> None:
        self._fail_send = True

    async def recv_multipart(self) -> list[bytes]:
        if not self._recv_queue:
            # 无更多请求：挂起，模拟真实 REP 等待（测试通过 cancel 退出）
            await asyncio.sleep(3600)
        return self._recv_queue.pop(0)

    async def send_multipart(self, frames: list[bytes]) -> None:
        self.send_calls += 1
        if self._fail_send:
            self._fail_send = False
            raise zmq.ZMQError(zmq.EAGAIN, "模拟 send 失败")


def _make_plain_request(*, username: str = "alice", password: str = "pw") -> list[bytes]:
    """构造合法 PLAIN ZAP 请求帧（version, request_id, domain, address, identity, mechanism, username, password）。"""
    return [
        b"1.0", b"req-1", b"", b"1.2.3.4:5555", b"identity",
        b"PLAIN", username.encode(), password.encode(),
    ]


@pytest.mark.asyncio
async def test_loop_survives_send_error() -> None:
    """send_multipart 抛异常时 _loop 必须存活，继续处理后续请求。"""
    ctx = zmq.asyncio.Context()
    handler = AsyncZAPHandler(api_keys={"alice": "pw"}, ctx=ctx, connected_users=set(), last_seen={})
    stub = _StubZapSocket()
    handler._zap = stub  # type: ignore[assignment]  # 注入桩 socket 替代真实 REP

    # 第一轮：合法请求，但响应 send 失败
    stub.queue_recv(_make_plain_request())
    stub.fail_next_send()
    # 第二轮：另一个合法请求，响应应正常（证明 loop 存活）
    stub.queue_recv(_make_plain_request())

    task = asyncio.create_task(handler._loop())
    # 给两轮请求足够时间完成（_loop 是 while True，跑完两轮后会挂在第三次 recv）
    await asyncio.sleep(0.3)

    task.cancel()
    try:
        await task
    except (asyncio.CancelledError, Exception):
        pass
    ctx.term()

    # 核心：第二轮 send 也执行了 → loop 在第一次 send 失败后没有死亡
    assert stub.send_calls >= 2, (
        f"ZAP loop 应在 send 失败后存活并处理后续请求，"
        f"实际只 send 了 {stub.send_calls} 次（loop 已死亡）"
    )
