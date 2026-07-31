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


def test_subscribe_updates_registry_topics():
    """SUBSCRIBE 必须回写 registry 的 topics，使在线 client 快照/订阅计数反映实时订阅。

    回归 Bug：原实现 topics 仅在 REGISTER 写一次，SUBSCRIBE 不回写 →
    ``total_subscriptions`` 恒为 0，在线 client 的 topics 列表永远空。
    """
    reg = OnlineRegistry(heartbeat_timeout=6.0)
    info = ClientInfo(client_id="c1", username="alice", endpoint="x",
                      roles=["consumer"], topics=[], connected_at=time.time())
    reg.register(info)
    # 注册时无订阅
    assert reg.snapshot()["clients"][0]["topics"] == []
    reg.subscribe("c1", "market.*")
    reg.subscribe("c1", "sports.news")
    snap = reg.snapshot()["clients"][0]["topics"]
    assert set(snap) == {"market.*", "sports.news"}


def test_subscribe_is_idempotent():
    """重复订阅同一 pattern 不应重复计入 topics。"""
    reg = OnlineRegistry(heartbeat_timeout=6.0)
    info = ClientInfo(client_id="c1", username="alice", endpoint="x",
                      roles=["consumer"], topics=[], connected_at=time.time())
    reg.register(info)
    reg.subscribe("c1", "market.*")
    reg.subscribe("c1", "market.*")
    assert reg.snapshot()["clients"][0]["topics"] == ["market.*"]


def test_unsubscribe_updates_registry_topics():
    """UNSUBSCRIBE 必须从 registry topics 移除对应 pattern。"""
    reg = OnlineRegistry(heartbeat_timeout=6.0)
    info = ClientInfo(client_id="c1", username="alice", endpoint="x",
                      roles=["consumer"], topics=["market.*", "news"],
                      connected_at=time.time())
    reg.register(info)
    reg.unsubscribe("c1", "market.*")
    assert reg.snapshot()["clients"][0]["topics"] == ["news"]


async def test_latency_report_recorded():
    """LATENCY_REPORT 命令记入 _lat_e2e registry。"""
    from pulsemq.server import Server
    srv = Server(credentials={"p": "p"}, admin_token="T", latency_sample_rate=1.0)
    msg = ControlMessage(
        cmd=ControlCmd.LATENCY_REPORT,
        payload={"topic": "t.x", "latency_ns": 1_000_000},
    )
    await srv._dispatch_control(b"id1", msg)
    snap = srv._lat_e2e.snapshot()
    assert "t.x" in snap
    assert snap["t.x"]["count"] == 1
