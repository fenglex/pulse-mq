"""控制面阻塞（head-of-line blocking）复现与修复验证。

Bug（9.2.0 及以前）：``_dispatch_control`` 的回复走 ``await transport.send``，
无 DONTWAIT/无超时。一个只发不收的僵尸 peer 把服务端控制 ROUTER 的
per-peer SNDHWM（默认 10000）打满后，``await send`` 永久挂起 —— 单协程
控制循环被卡死，所有新连接的 REGISTER 无响应（9.2.0 A/B 测试期间线上
实锤：控制面假死但 admin/sweep 线程存活、无异常栈）。

修复：控制面回复改为 DONTWAIT 直发（Transport.send_nowait），队列满
（zmq.Again）或对端不可达（EHOSTUNREACH）立即丢弃并告警，控制循环永不
阻塞。

复现方式说明：Windows loopback 的 TCP 收发缓冲达 MB 级，真实洪泛无法在
测试时间内打满 SNDHWM，故核心用例在传输层精确模拟 muted-pipe 语义
（zombie ident 的 send 永不就绪 / send_nowait 立即 Again，其他 ident 正常
—— 与 libzmq per-pipe 行为一致）；另保留真实洪泛用例作集成冒烟。
"""
from __future__ import annotations

import asyncio
import socket as _sock

import pytest
import zmq
import zmq.asyncio

from pulsemq.client import ProducerClient
from pulsemq.control import ControlCmd
from pulsemq.errors import ClientStartupError
from pulsemq.protocol import frames
from pulsemq.server import Server

ZOMBIE_IDENT = b"zombie-hb-flood"


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


def _simulate_muted_peer(transport) -> None:
    """把 ZOMBIE_IDENT 模拟成 SNDHWM 打满的僵死 peer（libzmq per-pipe 语义）。

    - send（旧代码路径）：zombie → 永不就绪的 future（阻塞 await）；
    - send_nowait（修复路径）：zombie → 立即 zmq.Again；
    - 其他 ident 完全正常。
    """
    real_send = transport.send
    real_nowait = getattr(transport, "send_nowait", None)

    async def fake_send(identity, frame_bytes, *, role="server_ingress"):
        if identity == ZOMBIE_IDENT and role == "control":
            await asyncio.Event().wait()  # muted：永久挂起（旧 bug）
        return await real_send(identity, frame_bytes, role=role)

    transport.send = fake_send
    if real_nowait is not None:
        async def fake_nowait(identity, frame_bytes, *, role="server_ingress"):
            if identity == ZOMBIE_IDENT and role == "control":
                raise zmq.Again()  # muted：DONTWAIT 立即 EAGAIN
            return await real_nowait(identity, frame_bytes, role=role)
        transport.send_nowait = fake_nowait


async def _zombie_register_and_flood(cp: int, count: int = 20000,
                                     warmup_s: float = 2.0):
    """真实僵尸 DEALER：注册后连发 HEARTBEAT 且从不 recv（集成冒烟用）。"""
    ctx = zmq.asyncio.Context.instance()
    sock = ctx.socket(zmq.DEALER)
    sock.setsockopt(zmq.IDENTITY, ZOMBIE_IDENT)
    sock.setsockopt(zmq.LINGER, 0)
    sock.setsockopt(zmq.RCVHWM, 2)
    sock.plain_username = b"z"
    sock.plain_password = b"z"
    sock.connect(f"tcp://127.0.0.1:{cp}")
    reg = frames.encode_control(ControlCmd.REGISTER, {
        "client_id": "zombie-1", "username": "z", "endpoint": "tcp://x",
        "roles": ["publisher"], "topics": [],
    })
    await sock.send(reg)
    await asyncio.sleep(0.3)
    hb = frames.encode_control(ControlCmd.HEARTBEAT, {"client_id": "zombie-1"})

    async def _flood() -> None:
        for _ in range(count):
            try:
                await sock.send(hb)
            except Exception:
                return

    flood_task = asyncio.create_task(_flood())
    await asyncio.sleep(warmup_s)
    return sock, flood_task


async def test_muted_peer_does_not_block_dispatch_of_other_clients(monkeypatch_style=None):
    """僵尸 peer 的 send 挂起时，其他客户端的 REGISTER 必须照常处理。

    未修复代码：_dispatch_control 内 await send(zombie) 永久挂起 →
    新客户端 REGISTER 超时（红）。修复后：DONTWAIT → Again → 丢弃 →
    控制循环继续（绿）。
    """
    srv, dp, cp = await _start_server({"z": "z", "p": "p"})
    zombie = None
    flood_task = None
    try:
        # 先在传输层模拟 zombie 的 per-pipe 已打满，再让 zombie 注册：
        # 其 REGISTER 回复本身就会命中 muted 分支（确定性，不依赖时序）
        _simulate_muted_peer(srv._transport)
        zombie, flood_task = await _zombie_register_and_flood(cp, count=100,
                                                              warmup_s=0.3)
        p = ProducerClient(
            data_endpoint=f"tcp://127.0.0.1:{dp}",
            control_endpoint=f"tcp://127.0.0.1:{cp}",
            username="p", password="p",
            register_reply_timeout=4.0,
        )
        await asyncio.wait_for(p.start(), timeout=10.0)
        await p.publish("wedge.check", {"ok": 1})
        await p.stop()
    except (ClientStartupError, asyncio.TimeoutError) as e:
        pytest.fail(f"控制面被僵尸 peer 卡死（head-of-line blocking 未修复）: {e!r}")
    finally:
        if flood_task is not None:
            flood_task.cancel()
        if zombie is not None:
            zombie.close(linger=0)
        await srv.stop()


async def test_zombie_flood_integration_smoke():
    """集成冒烟：真实洪灌 2 万条心跳后，新客户端仍可完成注册并收发。

    Windows loopback 缓冲大、可能到不了 SNDHWM（此时仅验证无回归）；
    Linux 上缓冲小、必然触发 muted 场景（验证真实修复）。
    """
    srv, dp, cp = await _start_server({"z": "z", "p": "p"})
    zombie = None
    flood_task = None
    try:
        zombie, flood_task = await _zombie_register_and_flood(cp)
        p = ProducerClient(
            data_endpoint=f"tcp://127.0.0.1:{dp}",
            control_endpoint=f"tcp://127.0.0.1:{cp}",
            username="p", password="p",
            register_reply_timeout=5.0,
        )
        await asyncio.wait_for(p.start(), timeout=10.0)
        await p.publish("wedge.check", {"ok": 1})
        await p.stop()
    except (ClientStartupError, asyncio.TimeoutError) as e:
        pytest.fail(f"控制面被僵尸洪泛卡死: {e!r}")
    finally:
        if flood_task is not None:
            flood_task.cancel()
        if zombie is not None:
            zombie.close(linger=0)
        await srv.stop()
