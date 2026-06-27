# tests/test_transport_router.py
import asyncio
import pytest
import zmq
from pulsemq.transport.router import Transport, PlainAuthDict, AsyncZAPHandler
from pulsemq.protocol import frames


@pytest.fixture()
def ctx():
    c = zmq.asyncio.Context.instance()
    yield c


def test_plain_auth_dict_verify():
    auth = PlainAuthDict({"alice": "secret"})
    ok, reason = auth.verify("alice", "secret")
    assert ok is True and reason is None
    ok, reason = auth.verify("alice", "wrong")
    assert ok is False and reason == "invalid_password"
    ok, reason = auth.verify("bob", "x")
    assert ok is False and reason == "user_not_found"


async def test_router_dealer_roundtrip(ctx, monkeypatch):
    # 选随机端口
    import socket as _sock
    def _free_port():
        s = _sock.socket(_sock.AF_INET, _sock.SOCK_STREAM)
        s.bind(("127.0.0.1", 0))
        p = s.getsockname()[1]
        s.close()
        return p
    dp, cp = _free_port(), _free_port()
    data_ep = f"tcp://127.0.0.1:{dp}"
    ctrl_ep = f"tcp://127.0.0.1:{cp}"

    server = Transport(ctx=ctx)
    auth = PlainAuthDict({"alice": "secret"})
    await server.bind(data_ep, "server_ingress", auth=auth)
    await server.bind(ctrl_ep, "control", auth=auth)

    client = Transport(ctx=ctx)
    await client.connect(data_ep, "consumer", credentials=("alice", "secret"))
    await asyncio.sleep(0.2)  # 等握手 + ZAP

    frame = frames.encode("t", {"x": 1})
    await client.send(b"", frame)  # DEALER 无 identity，首帧空
    ident, recv = await asyncio.wait_for(server.recv(), timeout=2.0)
    assert recv == frame
    await client.close()
    await server.close()


async def test_bind_forwards_on_auth(ctx):
    from pulsemq.transport.router import Transport, PlainAuthDict
    import socket as _sock
    def _fp():
        s = _sock.socket(); s.bind(("127.0.0.1", 0)); p = s.getsockname()[1]; s.close(); return p
    dp = _fp()
    seen = []
    async def on_auth(username, address, ok, reason=None):
        seen.append((username, ok))
    server = Transport(ctx=ctx)
    await server.bind(f"tcp://127.0.0.1:{dp}", "server_ingress",
                      auth=PlainAuthDict({"alice": "pw"}), on_auth=on_auth)
    # 触发一次 ZAP：client 连 + 错密码
    client = Transport(ctx=ctx)
    await client.connect(f"tcp://127.0.0.1:{dp}", "consumer",
                         credentials=("alice", "WRONG"))
    await asyncio.sleep(0.3)
    assert any(u == "alice" for u, _ in seen)  # on_auth 被调用
    await client.close(); await server.close()
