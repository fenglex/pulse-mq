# tests/test_connections_stats.py
import time
from pulsemq.stats.connections import ConnectionStats, ClientSnapshot, LifecycleEvent


def _reg_snap_factory(clients):
    def _fn():
        return {"clients": [
            {"client_id": c["client_id"], "username": c["username"], "endpoint": "x",
             "roles": c["roles"], "topics": c.get("topics", []),
             "connected_at": 0.0, "last_seen": 0.0}
            for c in clients
        ]}
    return _fn


def test_counters_by_role():
    cs = ConnectionStats(_reg_snap_factory([
        {"client_id": "c1", "username": "p1", "roles": ["publisher"]},
        {"client_id": "c2", "username": "s1", "roles": ["subscriber"]},
        {"client_id": "c3", "username": "b1", "roles": ["publisher", "subscriber"]},
    ]))
    cnt = cs.counters()
    assert cnt["online_users"] == 3
    assert cnt["online_producers"] == 2  # p1 + b1
    assert cnt["online_consumers"] == 2  # s1 + b1


def test_event_ring_eviction():
    cs = ConnectionStats(_reg_snap_factory([]), ring_size=3)
    for i in range(5):
        cs.on_connect(f"c{i}", f"u{i}", "ep", "consumer")
    evts = cs.recent_events(50)
    assert len(evts) == 3  # ring 溢出丢旧


def test_recent_events_limit():
    cs = ConnectionStats(_reg_snap_factory([]), ring_size=100)
    for i in range(10):
        cs.on_auth(f"u{i}", "ep", success=(i % 2 == 0), reason=None)
    assert len(cs.recent_events(limit=5)) == 5


def test_on_auth_records_failure_reason():
    cs = ConnectionStats(_reg_snap_factory([]))
    cs.on_auth("bob", "ep", success=False, reason="invalid_password")
    e = cs.recent_events(10)[0]
    assert e.level == "WARNING"
    assert "invalid_password" in e.message
    assert e.type == "AUTH"


def test_online_clients_snapshot():
    cs = ConnectionStats(_reg_snap_factory([
        {"client_id": "c1", "username": "alice", "roles": ["subscriber"], "topics": ["a.*"]},
    ]))
    clients = cs.online_clients()
    assert len(clients) == 1
    assert isinstance(clients[0], ClientSnapshot)
    assert clients[0].username == "alice"
    assert clients[0].role == "consumer"
