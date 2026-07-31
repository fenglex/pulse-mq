# tests/test_client_lifecycle.py
"""Client 生命周期测试：publish/subscribe roundtrip、auth 失败、server 宕机。

Task 12 的 monitor-based startup 设计：
- handshake_ok → 认证通过，继续 REGISTER；
- auth_failed → AuthenticationError (exit 3)；
- 超时/其他 → ClientStartupError (exit 4)。
"""
from __future__ import annotations

import asyncio
import socket as _sock

import pytest

from pulsemq.client import ConsumerClient, ProducerClient
from pulsemq.errors import AuthenticationError, ClientStartupError
from pulsemq.server import Server


def _free_port() -> int:
    s = _sock.socket(_sock.AF_INET, _sock.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


async def _start_server(creds: dict[str, str]) -> tuple[Server, int, int]:
    """启动 Server 并返回 (srv, data_port, control_port)。"""
    dp, cp, ap = _free_port(), _free_port(), _free_port()
    srv = Server(
        data_endpoint=f"tcp://127.0.0.1:{dp}",
        control_endpoint=f"tcp://127.0.0.1:{cp}",
        admin_endpoint=f"127.0.0.1:{ap}",
        credentials=creds,
    )
    await srv.start()
    # 给 ZAP/ROUTER bind 一点时间稳定下来。
    await asyncio.sleep(0.2)
    return srv, dp, cp


async def test_publish_subscribe_roundtrip():
    srv, dp, cp = await _start_server({"alice": "s", "bob": "p"})
    try:
        c = ConsumerClient(
            data_endpoint=f"tcp://127.0.0.1:{dp}",
            control_endpoint=f"tcp://127.0.0.1:{cp}",
            username="alice",
            password="s",
        )
        p = ProducerClient(
            data_endpoint=f"tcp://127.0.0.1:{dp}",
            control_endpoint=f"tcp://127.0.0.1:{cp}",
            username="bob",
            password="p",
        )
        await c.start()
        await p.start()
        got: list = []
        await c.subscribe("market.stock.*", lambda m: got.append(m))
        # 让 SUBSCRIBE 控制帧被服务端处理并写入路由表。
        await asyncio.sleep(0.3)
        await p.publish("market.stock.600000", {"price": 12.3})
        # 等数据帧被转发并由 recv_loop 投递到回调。
        await asyncio.sleep(0.5)
        assert len(got) == 1, f"expected 1 msg, got {len(got)}"
        assert got[0].payload == {"price": 12.3}
        assert got[0].topic == "market.stock.600000"
        await c.stop()
        await p.stop()
    finally:
        await srv.stop()


async def test_auth_failure_exits():
    # 服务器在线，但密码错误 → AuthenticationError。
    srv, dp, cp = await _start_server({"alice": "s"})
    try:
        c = ConsumerClient(
            data_endpoint=f"tcp://127.0.0.1:{dp}",
            control_endpoint=f"tcp://127.0.0.1:{cp}",
            username="alice",
            password="WRONG",
        )
        with pytest.raises(AuthenticationError) as ei:
            await c.start()
        assert ei.value.reason == "invalid_password"
    finally:
        await srv.stop()


async def test_connect_failure_when_server_down():
    # 没有服务器在监听 → 握手不会发生 → 超时 → ClientStartupError。
    dp = _free_port()
    cp = _free_port()
    c = ConsumerClient(
        data_endpoint=f"tcp://127.0.0.1:{dp}",
        control_endpoint=f"tcp://127.0.0.1:{cp}",
        username="ghost",
        password="b",
    )
    with pytest.raises(ClientStartupError) as ei:
        await c.start()
    assert ei.value.reason == "CONNECT_FAILED"


async def test_producer_rejects_subscribe():
    """ProducerClient.subscribe 抛 NotImplementedError。"""
    p = ProducerClient(
        data_endpoint="tcp://127.0.0.1:1",
        control_endpoint="tcp://127.0.0.1:2",
        username="x",
        password="y",
    )
    with pytest.raises(NotImplementedError):
        await p.subscribe("t", lambda m: None)


async def test_consumer_rejects_publish():
    """ConsumerClient.publish 抛 NotImplementedError。"""
    c = ConsumerClient(
        data_endpoint="tcp://127.0.0.1:1",
        control_endpoint="tcp://127.0.0.1:2",
        username="x",
        password="y",
    )
    with pytest.raises(NotImplementedError):
        await c.publish("t", {"a": 1})


async def test_consumer_run_forever_replaces_sleep():
    """ConsumerClient.run_forever 替代 asyncio.sleep，收到 stop 后退出。"""
    cons = ConsumerClient("tcp://localhost:5555", "tcp://localhost:5556",
                          username="u", password="p")
    # 用 monkeypatch 替换 start 为 no-op，避免真实连接
    async def fake_start():
        cons._connected = True
    cons.start = fake_start  # type: ignore
    # 0.1s 后触发 stop
    async def stopper():
        await asyncio.sleep(0.1)
        cons._stop.set()
    asyncio.create_task(stopper())
    await cons.run_forever()  # 应在 stopper 触发后正常返回


async def test_run_forever_reraises_reconnect_fatal():
    """重连致命错误应在 run_forever 主上下文重新抛出。"""
    cons = ConsumerClient("tcp://localhost:5555", "tcp://localhost:5556",
                          username="u", password="p")
    async def fake_start():
        cons._connected = True
    cons.start = fake_start  # type: ignore
    cons._reconnect_fatal = AuthenticationError("认证失败", reason="invalid_password")
    async def stopper():
        await asyncio.sleep(0.05)
        cons._stop.set()
    asyncio.create_task(stopper())
    with pytest.raises(AuthenticationError):
        await cons.run_forever()


async def test_subscribe_before_start_is_cached():
    """start 前调用 subscribe 应缓存，不抛异常（A3）。"""
    cons = ConsumerClient("tcp://localhost:5555", "tcp://localhost:5556",
                          username="u", password="p")
    # 未 start，transport 未就绪
    await cons.subscribe("market.*", lambda m: None)
    assert "market.*" in cons._subscriptions


async def test_install_signal_handlers_no_raise():
    """_install_signal_handlers 在 Windows 静默跳过，不抛异常（A4）。"""
    cons = ConsumerClient("tcp://localhost:5555", "tcp://localhost:5556",
                          username="u", password="p")
    cons._install_signal_handlers()  # Windows 下应静默跳过


async def test_publish_rejects_non_whitelist_type():
    """ProducerClient.publish 直调非白名单类型 → encode 抛 TypeError 冒到调用者。

    publish 内部第一步即 encode，异常在 send 之前抛出，不会产生半发送；
    校验失败后连接仍可用，后续发 dict 应正常。
    """
    srv, dp, cp = await _start_server({"p": "p"})
    try:
        prod = ProducerClient(
            data_endpoint=f"tcp://127.0.0.1:{dp}",
            control_endpoint=f"tcp://127.0.0.1:{cp}",
            username="p",
            password="p",
        )
        await prod.start()
        with pytest.raises(TypeError):
            await prod.publish("t", [1, 2, 3])
        # 校验失败后连接仍可用：发 dict 应正常返回
        await prod.publish("t", {"ok": True})
        await prod.stop()
    finally:
        await srv.stop()
