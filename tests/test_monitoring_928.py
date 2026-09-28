"""9.2.8 监控扩展测试：数据面健康 / 心跳质量 / 缺口归档 / 对账 / 告警 /
Prometheus / per-worker 与 confirm 上报。

单元（无 Server）：DataPlaneStats / HeartbeatMonitor / GapStats / DropStats 累计
与历史 / TrafficStats 累计 / ClientProcStats 扩展（count_cum、e2e latch、
confirm、workers）/ AlertManager（规则、冷却、阈值 0 关闭、webhook）/
StatsStorage drops+gaps 归档 / BufferManager credit 与 starved。
Admin 级：/metrics、drops/gaps history、system/status 扩展、realtime 新段。
e2e：Server + pub(confirm) + sub(worker 池) → clients/realtime 新字段可见。
"""
from __future__ import annotations

import asyncio
import json
import socket as _sock
import time

import pytest

from tests import mp_callbacks


def _free_port() -> int:
    s = _sock.socket(_sock.AF_INET, _sock.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


async def _get(port: int, path: str, token: str | None = None,
               timeout: float = 3.0) -> tuple[int, str]:
    reader, writer = await asyncio.wait_for(
        asyncio.open_connection("127.0.0.1", port), timeout=timeout)
    h = f"Authorization: Bearer {token}\r\n" if token else ""
    writer.write(f"GET {path} HTTP/1.1\r\nHost: x\r\n{h}Connection: close\r\n\r\n".encode())
    await writer.drain()
    data = await asyncio.wait_for(reader.read(), timeout=timeout)
    writer.close()
    text = data.decode(errors="replace")
    head, _, body = text.partition("\r\n\r\n")
    status = int(head.split(" ", 2)[1])
    return status, body


# ---------------------------------------------------------------------------
# DataPlaneStats
# ---------------------------------------------------------------------------

def test_dataplane_stats_window_and_cumulative():
    from pulsemq.stats.dataplane import DataPlaneStats
    dp = DataPlaneStats()
    dp.record_loop(1_000_000)
    dp.record_loop(3_000_000)
    dp.record_in(10)
    dp.record_out(8)
    dp.record_eagain()
    dp.record_starved(5)
    time.sleep(0.02)
    s1 = dp.snapshot()
    assert s1["window_loops"] == 2
    assert s1["loop_avg_ms"] == pytest.approx(2.0)
    assert s1["loop_max_ms"] == pytest.approx(3.0)
    assert s1["msgs_in"] == 10
    assert s1["msgs_out"] == 8
    assert s1["eagain"] == 1
    assert s1["credit_starved"] == 5
    # 第二次快照：窗口清零、累计保留
    s2 = dp.snapshot()
    assert s2["window_loops"] == 0
    assert s2["loop_avg_ms"] == 0.0
    assert s2["msgs_in"] == 10
    assert s2["credit_starved"] == 5


# ---------------------------------------------------------------------------
# HeartbeatMonitor
# ---------------------------------------------------------------------------

def test_heartbeat_monitor_intervals_and_kicks():
    from pulsemq.stats.health import HeartbeatMonitor
    hb = HeartbeatMonitor()
    hb.record("c1")          # 首次（无间隔）
    time.sleep(0.03)
    hb.record("c1")
    hb.kick("c2", "bob")
    snap = hb.snapshot()
    assert snap["kick_total"] == 1
    assert snap["recent_kicks"][0]["client_id"] == "c2"
    c = snap["clients"]["c1"]
    assert c["samples"] == 1
    assert c["interval_max_s"] >= 0.03
    hb.remove("c1")
    assert "c1" not in hb.snapshot()["clients"]


# ---------------------------------------------------------------------------
# GapStats / DropStats：累计 + 分钟历史 + 归档行
# ---------------------------------------------------------------------------

def test_gap_stats_record_roll_history():
    from pulsemq.stats.gaps import GapStats
    g = GapStats()
    g.record("t", 5)
    g.record("t", 3)
    g.record("other", 1)
    assert g.snapshot() == {"t": 8, "other": 1}
    rows = g.roll_minute()
    by_topic = {r[0]: r for r in rows}
    assert by_topic["t"][2] == 8
    assert by_topic["other"][2] == 1
    hist = g.history(60)
    assert hist[-1]["topics"] == {"t": 8, "other": 1}
    # roll 后 current 清空，但累计保留（对账）
    assert g.snapshot() == {"t": 8, "other": 1}
    assert g.roll_minute() == []


def test_drop_stats_cumulative_and_history():
    from pulsemq.stats.drops import DropStats
    d = DropStats()
    d.record("t", 2)
    rows = d.roll_minute()
    assert rows[0][0] == "t" and rows[0][2] == 2
    snap = d.snapshot()
    assert snap["t"]["drops_cum"] == 2
    assert snap["t"]["drops_last_min"] == 2
    hist = d.history(60)
    assert hist[-1]["topics"] == {"t": 2}


def test_traffic_stats_msg_count_cum():
    from pulsemq.stats.traffic import TrafficStats
    ts = TrafficStats()
    for _ in range(7):
        ts.record("t", 1, 100)
    snap = ts.all_topics_snapshot()
    assert snap["t"]["msg_count_cum"] == 7


# ---------------------------------------------------------------------------
# ClientProcStats：count_cum / e2e latch / confirm / workers
# ---------------------------------------------------------------------------

def test_proc_stats_count_cum_accumulates():
    from pulsemq.stats.throughput import ClientProcStats
    s = ClientProcStats()
    s.record("c1", "u", {"t": {"count": 10, "rate": 10.0}})
    s.record("c1", "u", {"t": {"count": 5, "rate": 5.0}})
    s.record("c1", "u", {"t": {"count": 3, "rate": 3.0, "proc_avg_ns": 100}})
    entry = s.client_entry("c1")
    assert entry["topics"]["t"]["count_cum"] == 18
    # 聚合视图带 processed_cum
    assert s.topics()["t"]["processed_cum"] == 18


def test_proc_stats_e2e_latch_across_windows():
    from pulsemq.stats.throughput import ClientProcStats
    s = ClientProcStats()
    s.record("c1", "u", {"t": {"count": 5, "rate": 5.0,
                               "e2e_avg_ns": 2_000_000, "e2e_max_ns": 9_000_000}})
    # 下一窗口无 e2e（worker 统计晚到）：latch 保留
    s.record("c1", "u", {"t": {"count": 1, "rate": 1.0, "proc_avg_ns": 50}})
    topics = s.topics()
    assert topics["t"]["e2e_avg_ms"] == pytest.approx(2.0)
    assert topics["t"]["e2e_max_ms"] == pytest.approx(9.0)
    assert topics["t"]["processed_cum"] == 6


def test_proc_stats_record_extra_confirm_and_workers():
    from pulsemq.stats.throughput import ClientProcStats
    s = ClientProcStats()
    s.record_extra("pub1", confirm={"acks": 4, "ack_ns": 8_000_000,
                                    "ack_max_ns": 3_000_000, "timeouts": 1})
    e = s.client_entry("pub1")
    # acks/timeouts 服务端累计；延迟取窗口值（与 count/rate 同为窗口量）
    assert e["confirm"]["ack_avg_ms"] == pytest.approx(2.0)
    assert e["confirm"]["ack_max_ms"] == pytest.approx(3.0)
    s.record_extra("pub1", confirm={"acks": 2, "ack_ns": 2_000_000,
                                    "ack_max_ns": 1_000_000, "timeouts": 2})
    e = s.client_entry("pub1")
    assert e["confirm"]["acks"] == 6
    assert e["confirm"]["timeouts"] == 3
    assert e["confirm"]["ack_avg_ms"] == pytest.approx(1.0)
    # workers 快照整体覆盖；record() 不丢扩展字段
    s.record_extra("pub1", workers=[{"worker": 0, "processed": 9}])
    s.record("pub1", "u", {"t": {"count": 1, "rate": 1.0}})
    e2 = s.client_entry("pub1")
    assert e2["workers"] == [{"worker": 0, "processed": 9}]
    assert e2["confirm"]["acks"] == 6


def test_summarize_includes_e2e():
    from pulsemq.stats.throughput import ClientProcStats
    s = ClientProcStats()
    s.record("c1", "u", {"t": {"count": 4, "rate": 4.0,
                                "e2e_avg_ns": 1_500_000, "e2e_max_ns": 7_000_000}})
    out = ClientProcStats.summarize(s.client_entry("c1"))
    assert out["e2e_avg_ms"] == pytest.approx(1.5)
    assert out["e2e_max_ms"] == pytest.approx(7.0)


# ---------------------------------------------------------------------------
# StatsStorage：drops/gaps 表读写
# ---------------------------------------------------------------------------

def test_storage_drop_gap_roundtrip(tmp_path):
    from pulsemq.stats.storage import StatsStorage
    st = StatsStorage(str(tmp_path / "s.sqlite"))
    st.connect()
    try:
        st.save_drop_minutes([("t", 100, 5), ("t2", 100, 1)])
        st.save_gap_minutes([("t", 100, 3)])
        drops = st.load_drop_history("t", 0)
        assert drops == [{"timestamp": 100, "count": 5}]
        gaps = st.load_gap_history("t", 0)
        assert gaps == [{"timestamp": 100, "count": 3}]
        assert st.load_gap_history("missing", 0) == []
    finally:
        st.close()


async def test_archive_writer_rows_kinds(tmp_path):
    from pulsemq.stats.storage import AsyncArchiveWriter, StatsStorage
    st = StatsStorage(str(tmp_path / "a.sqlite"))
    st.connect()
    w = AsyncArchiveWriter(st, batch_size=10)
    await w.start()
    try:
        await w.enqueue_rows("drops", [("t", 200, 7)])
        await w.enqueue_rows("gaps", [("t", 200, 2)])
        await asyncio.sleep(0.2)
        assert st.load_drop_history("t", 0) == [{"timestamp": 200, "count": 7}]
        assert st.load_gap_history("t", 0) == [{"timestamp": 200, "count": 2}]
    finally:
        await w.stop()
        st.close()


# ---------------------------------------------------------------------------
# BufferManager：credit 视图 + starved 计数
# ---------------------------------------------------------------------------

def test_buffer_manager_credit_snapshot_and_starved():
    from pulsemq.buffering import BufferManager
    from pulsemq.stats.dataplane import DataPlaneStats
    dp = DataPlaneStats()
    bm = BufferManager(metrics=dp)
    ident = b"sub1"
    bm.set_policy(ident, "drop_old", {})
    bm.set_send_fn(lambda i, f: True)
    for _ in range(5):
        bm.enqueue(ident, "t", b"x" * 8)
    # credit=3：首轮只发 3 帧，剩 2 帧积压
    bm.set_credit(ident, 3)
    n = bm.drain_all()
    assert n == 3
    snap = bm.snapshot()["subscribers"]["sub1"]
    assert snap["credit"] == 3
    assert snap["credit_remaining"] == 0
    assert snap["depth"] == 2
    assert snap["starved_rounds"] == 0
    # 额度耗尽 + 仍有积压 → 后续每轮 starved +1
    bm.drain_all()
    bm.drain_all()
    snap = bm.snapshot()["subscribers"]["sub1"]
    assert snap["starved_rounds"] == 2
    assert snap["depth"] == 2          # 帧保留，不丢
    s = dp.snapshot()
    assert s["credit_starved"] == 2
    # 新心跳开新窗口：额度恢复，积压清空
    bm.set_credit(ident, 10)
    assert bm.drain_all() == 2
    assert bm.snapshot()["subscribers"]["sub1"]["depth"] == 0
    assert bm.credit_snapshot(b"nobody") is None


# ---------------------------------------------------------------------------
# AlertManager
# ---------------------------------------------------------------------------

async def test_alert_manager_rules_and_cooldown():
    from pulsemq.alerts import AlertManager
    am = AlertManager(drop_per_min=10, gap_per_min=100, buffer_age_s=5.0,
                      starved_per_s=10, loop_stall_ms=100, cooldown_s=60)
    # 首轮（建立缺口基线）应只有满足绝对阈值的规则
    fired = await am.check({"drops_last_min_total": 50, "gaps_total": 0,
                            "buffer_max_age_s": 0.0, "starved_per_s": 0.0,
                            "loop_max_ms": 0.0, "hb_kick_delta": 0})
    assert [a["rule"] for a in fired] == ["drops_burst"]
    time.sleep(0.01)
    fired = await am.check({"drops_last_min_total": 50,   # 冷却中不重发
                            "gaps_total": 10_000,         # >> 100/min
                            "buffer_max_age_s": 6.0,
                            "starved_per_s": 50.0,
                            "loop_max_ms": 300.0,
                            "hb_kick_delta": 1})
    rules = sorted(a["rule"] for a in fired)
    assert rules == ["buffer_backlog", "client_kicked", "credit_starved",
                     "dataplane_stall", "gap_burst"], rules


async def test_alert_manager_zero_threshold_disables():
    from pulsemq.alerts import AlertManager
    am = AlertManager(drop_per_min=0, gap_per_min=0, buffer_age_s=0.0,
                      starved_per_s=0, loop_stall_ms=0, cooldown_s=60)
    fired = await am.check({"drops_last_min_total": 10**9, "gaps_total": 10**9,
                            "buffer_max_age_s": 999.0, "starved_per_s": 10**9,
                            "loop_max_ms": 10**9, "hb_kick_delta": 0})
    assert fired == []


async def test_alert_manager_webhook_post():
    from pulsemq.alerts import AlertManager
    received: list[dict] = []

    async def handler(reader, writer):
        line = await reader.readline()
        headers = {}
        while True:
            h = await reader.readline()
            if h in (b"\r\n", b"\n", b""):
                break
            k, _, v = h.decode().partition(":")
            headers[k.strip().lower()] = v.strip()
        body = await reader.readexactly(int(headers.get("content-length", 0)))
        received.append(json.loads(body))
        writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
        await writer.drain()
        writer.close()

    srv = await asyncio.start_server(handler, "127.0.0.1", 0)
    port = srv.sockets[0].getsockname()[1]
    try:
        am = AlertManager(webhook=f"http://127.0.0.1:{port}/hook",
                          drop_per_min=10, gap_per_min=0, buffer_age_s=0.0,
                          starved_per_s=0, loop_stall_ms=0, cooldown_s=60)
        await am.check({"drops_last_min_total": 99, "gaps_total": 0})
        assert len(received) == 1
        assert received[0]["rule"] == "drops_burst"
        assert received[0]["level"] == "WARNING"
    finally:
        srv.close()
        await srv.wait_closed()


# ---------------------------------------------------------------------------
# Admin 级：/metrics + history 端点 + system/status + realtime 新段
# ---------------------------------------------------------------------------

def _build_admin(tmp_path):
    from pulsemq.admin.auth import TokenAuth
    from pulsemq.admin.server import AdminServer
    from pulsemq.stats.connections import ConnectionStats
    from pulsemq.stats.dataplane import DataPlaneStats
    from pulsemq.stats.drops import DropStats
    from pulsemq.stats.gaps import GapStats
    from pulsemq.stats.health import HeartbeatMonitor
    from pulsemq.stats.storage import StatsStorage
    from pulsemq.stats.throughput import ClientProcStats
    from pulsemq.stats.traffic import TrafficStats

    registry = {"clients": [{
        "client_id": "c1", "username": "alice", "endpoint": "x",
        "roles": ["sub"], "topics": ["t"], "connected_at": time.time(),
    }]}
    cs = ConnectionStats(lambda: registry, ring_size=10)
    cs.on_connect("c1", "alice", "x", "consumer")
    traffic = TrafficStats()
    traffic.record("t", 1, 10)
    drops = DropStats()
    drops.record("t", 3)
    gaps = GapStats()
    gaps.record("t", 2)
    dp = DataPlaneStats()
    dp.record_loop(2_500_000)
    hb = HeartbeatMonitor()
    hb.record("c1")
    time.sleep(0.01)
    hb.record("c1")
    hb.kick("c9", "eve")
    ps = ClientProcStats()
    ps.record("c1", "alice", {"t": {"count": 4, "rate": 4.0,
                                    "e2e_avg_ns": 2_000_000,
                                    "e2e_max_ns": 5_000_000}})
    storage = StatsStorage(str(tmp_path / "adm.sqlite"))
    storage.connect()
    adm = AdminServer(
        bind="127.0.0.1:0", token_auth=TokenAuth("T"),
        traffic_stats=traffic, stats_storage=storage,
        connection_stats=cs, drop_stats=drops, proc_stats=ps,
        gap_stats=gaps, dataplane_stats=dp, hb_monitor=hb,
        credit_fn=lambda cid: {"credit": 100, "credit_remaining": 60,
                               "starved_rounds": 0},
        admin_thread=False,
    )
    return adm, storage


async def test_admin_metrics_and_new_endpoints(tmp_path):
    adm, storage = _build_admin(tmp_path)
    await adm.start()
    try:
        port = adm._server.sockets[0].getsockname()[1]
        # /metrics 需 token；文本 exposition 含核心指标
        assert (await _get(port, "/metrics"))[0] == 401
        status, body = await _get(port, "/metrics", token="T")
        assert status == 200
        assert "# TYPE pulsemq_up gauge" in body
        assert "pulsemq_dataplane_loop_ms" in body
        assert "pulsemq_heartbeat_kicks_total 1" in body
        assert 'topic="t"' in body
        assert "pulsemq_processing_e2e_avg_ms" in body
        # 事件流分隔（ exposition 有效性粗检：每行非 # 开头都有值）
        bad = [l for l in body.splitlines()
               if l and not l.startswith("#") and " " not in l]
        assert not bad
        # drops/gaps history
        s, d_raw = await _get(port, "/api/v1/stats/drops/history?minutes=60", token="T")
        d = json.loads(d_raw)
        assert s == 200 and d["history"][-1]["count"] == 3
        s, g_raw = await _get(port, "/api/v1/stats/gaps/history?topic=t", token="T")
        g = json.loads(g_raw)
        assert s == 200 and g["history"][-1]["count"] == 2
        # system/status 扩展
        s, sysd = await _get(port, "/api/v1/system/status", token="T")
        assert s == 200
        assert "pid" in sysd and "threads" in sysd
        # realtime 新段 + 对账
        s, rt_raw = await _get(port, "/api/v1/stats/realtime", token="T")
        rt = json.loads(rt_raw)
        assert "dataplane" in rt and rt["dataplane"]["loops"] == 1
        assert "heartbeat" in rt and rt["heartbeat"]["kick_total"] == 1
        recon = rt["reconciliation"]["t"]
        assert recon["published_cum"] == 1
        assert recon["missing_cum"] == 2
        assert recon["dropped_cum"] == 3
        assert recon["processed_cum"] == 4
        # clients：credit + confirm/workers 字段透传
        s, cl_raw = await _get(port, "/api/v1/clients", token="T")
        cl = json.loads(cl_raw)
        assert cl["clients"][0]["credit"]["credit_remaining"] == 60
    finally:
        await adm.stop()
        storage.close()


# ---------------------------------------------------------------------------
# e2e：Server + pub(confirm) + sub(worker 池) → 新指标全链路
# ---------------------------------------------------------------------------

async def test_e2e_monitoring_fields(record_dir):
    from pulsemq.client import ConsumerClient, ProducerClient
    from pulsemq.server import Server

    dp, cp, ap = _free_port(), _free_port(), _free_port()
    srv = Server(
        data_endpoint=f"tcp://127.0.0.1:{dp}",
        control_endpoint=f"tcp://127.0.0.1:{cp}",
        admin_endpoint=f"127.0.0.1:{ap}",
        credentials={"pub": "p", "sub": "s"},
    )
    await srv.start()
    sub = ConsumerClient(
        data_endpoint=f"tcp://127.0.0.1:{dp}",
        control_endpoint=f"tcp://127.0.0.1:{cp}",
        username="sub", password="s", client_id="sub-mon",
        workers=2,
    )
    pub = ProducerClient(
        data_endpoint=f"tcp://127.0.0.1:{dp}",
        control_endpoint=f"tcp://127.0.0.1:{cp}",
        username="pub", password="p", client_id="pub-mon",
    )
    try:
        await sub.start()
        await pub.start()
        await sub.subscribe("mon.t", mp_callbacks.on_msg_record)
        await asyncio.sleep(0.5)
        # confirm 发布 × 5（ack 统计），普通发布 × 25（流量/对账/worker e2e）
        for i in range(5):
            await pub.publish("mon.t", {"i": i}, confirm=True)
        for i in range(25):
            await pub.publish("mon.t", {"i": i})
        await mp_callbacks.wait_records(record_dir, 25, timeout=8.0)

        # 轮询等心跳（worker 统计 ≥1s 批量 flush + 1s 心跳 + proc latch）
        clients = realtime = None
        deadline = time.monotonic() + 12.0
        while time.monotonic() < deadline:
            _, cl_raw = await _get(ap, "/api/v1/clients", token=srv.admin_token)
            _, rt_raw = await _get(ap, "/api/v1/stats/realtime",
                                   token=srv.admin_token)
            clients = json.loads(cl_raw)
            realtime = json.loads(rt_raw)
            by_id = {c["client_id"]: c for c in clients["clients"]}
            sub_e = by_id.get("sub-mon", {})
            proc = sub_e.get("processing", {})
            if ("e2e_avg_ms" in proc and "workers" in sub_e
                    and "confirm" in by_id.get("pub-mon", {})):
                break
            await asyncio.sleep(0.5)

        by_id = {c["client_id"]: c for c in clients["clients"]}
        sub_e = by_id["sub-mon"]
        # per-worker 明细：2 个 worker，字段齐全
        ws = sub_e["workers"]
        assert len(ws) == 2
        assert all("worker" in w and "pending_bytes" in w and "cpu_cores" in w
                   for w in ws)
        # 处理 e2e（worker 出环时刻 - 生产时间戳）存在且 ≥ 0
        assert "e2e_avg_ms" in sub_e["processing"]
        assert sub_e["processing"]["e2e_avg_ms"] >= 0
        # confirm：发布端 ack 统计（5 次 confirm，无超时）
        cf = by_id["pub-mon"]["confirm"]
        assert cf["acks"] >= 5
        assert cf["timeouts"] == 0
        assert cf["ack_avg_ms"] > 0
        # 信用视图：订阅者有 credit 上报
        assert "credit" in sub_e
        # 对账：发布累计 = 处理累计（零丢失），在途 0
        recon = realtime["reconciliation"]["mon.t"]
        assert recon["published_cum"] == 30
        assert recon["processed_cum"] == 30
        assert recon["in_flight_est"] == 0
        assert recon.get("missing_cum", 0) == 0
        # 心跳质量：sub-mon 有间隔样本
        hb = realtime["heartbeat"]["clients"]
        assert "sub-mon" in hb
        assert hb["sub-mon"]["samples"] >= 1
        # 数据面健康：有收发帧
        dp_snap = realtime["dataplane"]
        assert dp_snap["msgs_in"] >= 30
    finally:
        await sub.stop()
        await pub.stop()
        await srv.stop()
