import time
from pulsemq.control import (
    ControlCmd, ControlMessage, ClientInfo, OnlineRegistry,
    RegisterResult,
)


def test_control_message_roundtrip():
    m = ControlMessage(cmd=ControlCmd.SUBSCRIBE, payload={"client_id": "c1", "topic": "a.*"})
    assert m.cmd == "SUBSCRIBE"
    assert m.payload["topic"] == "a.*"


def test_register_ok_and_already_online():
    reg = OnlineRegistry(heartbeat_timeout=6.0)
    info = ClientInfo(client_id="c1", username="alice", endpoint="1.2.3.4:1",
                      roles=["consumer"], topics=[], connected_at=time.time())
    assert reg.register(info) == RegisterResult.OK
    assert reg.register(info) == RegisterResult.ALREADY_ONLINE


def test_heartbeat_updates_last_seen():
    reg = OnlineRegistry(heartbeat_timeout=6.0)
    info = ClientInfo(client_id="c1", username="alice", endpoint="x",
                      roles=["consumer"], topics=[], connected_at=time.time())
    reg.register(info)
    before = reg._by_client["c1"].last_seen
    time.sleep(0.01)
    reg.heartbeat("c1")
    assert reg._by_client["c1"].last_seen > before


def test_sweep_timeout_returns_offline():
    reg = OnlineRegistry(heartbeat_timeout=0.0)  # 立即超时
    info = ClientInfo(client_id="c1", username="alice", endpoint="x",
                      roles=["consumer"], topics=["a.*"], connected_at=time.time())
    reg.register(info)
    swept = reg.sweep_timeout()
    assert len(swept) == 1
    assert swept[0].client_id == "c1"
    assert reg.snapshot()["clients"] == []


def test_unregister():
    reg = OnlineRegistry(heartbeat_timeout=6.0)
    info = ClientInfo(client_id="c1", username="alice", endpoint="x",
                      roles=["consumer"], topics=[], connected_at=time.time())
    reg.register(info)
    reg.unregister("c1")
    assert reg.snapshot()["clients"] == []
