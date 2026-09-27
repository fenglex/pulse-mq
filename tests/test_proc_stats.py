"""心跳处理统计（proc 字段）测试：_ProcStats / ClientProcStats 单元 + Server e2e。

覆盖：
- 客户端 ``_ProcStats``：record/drain 折算（count/rate/recv_avg_ns/proc_avg_ns）。
- 服务端 ``ClientProcStats``：record/clients/topics 聚合/summarize/remove/sweep_stale。
- e2e：9.2.4 唯一消费模式（多进程池）下，worker 统计随心跳上报，可在
  ``/api/v1/clients``（processing）与 ``/api/v1/stats/realtime``
  （processing_by_topic）查询到正的速率与延迟。
"""
from __future__ import annotations

import asyncio
import json
import socket as _sock
import time

import pytest

from pulsemq.client import ConsumerClient, ProducerClient, _ProcStats
from pulsemq.server import Server
from pulsemq.stats.throughput import ClientProcStats
from tests import mp_callbacks


def _free_port() -> int:
    s = _sock.socket(_sock.AF_INET, _sock.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


async def _get_json(port: int, path: str, token: str | None = None,
                    timeout: float = 3.0) -> dict:
    reader, writer = await asyncio.wait_for(
        asyncio.open_connection("127.0.0.1", port), timeout=timeout
    )
    h = f"Authorization: Bearer {token}\r\n" if token else ""
    writer.write(
        f"GET {path} HTTP/1.1\r\nHost: x\r\n{h}Connection: close\r\n\r\n".encode()
    )
    await writer.drain()
    data = await asyncio.wait_for(reader.read(), timeout=timeout)
    writer.close()
    text = data.decode(errors="replace")
    body = text.split("\r\n\r\n", 1)[1]
    return json.loads(body)


# ---------------------------------------------------------------------------
# _ProcStats（客户端）
# ---------------------------------------------------------------------------

def test_proc_stats_drain_math():
    ps = _ProcStats()
    for _ in range(10):
        ps.record_recv("t.a", 1_000_000)   # 1ms
        ps.record_proc("t.a", 2_000_000)   # 2ms
    out = ps.drain()
    assert set(out) == {"t.a"}
    e = out["t.a"]
    assert e["count"] == 10
    assert e["recv_avg_ns"] == 1_000_000
    assert e["proc_avg_ns"] == 2_000_000
    assert e["rate"] > 0
    # drain 后清零：再 drain 无数据
    assert ps.drain() == {}


def test_proc_stats_recv_without_proc():
    """只记接收（例如 header_only + 异常路径）时 proc_avg_ns 缺省。"""
    ps = _ProcStats()
    ps.record_recv("t.b", 500_000)
    out = ps.drain()
    assert out["t.b"]["count"] == 1
    assert out["t.b"]["recv_avg_ns"] == 500_000
    assert "proc_avg_ns" not in out["t.b"]


def test_proc_stats_empty_drain_returns_empty():
    ps = _ProcStats()
    time.sleep(0.01)
    assert ps.drain() == {}


def test_proc_stats_rate_uses_elapsed():
    """rate 按实际流逝时间折算：sleep 0.2s 发 10 条 ≈ 50/s（容忍区间）。"""
    ps = _ProcStats()
    time.sleep(0.2)
    for _ in range(10):
        ps.record_recv("t.c", 1)
    rate = ps.drain()["t.c"]["rate"]
    assert 20 <= rate <= 200


# ---------------------------------------------------------------------------
# ClientProcStats（服务端）
# ---------------------------------------------------------------------------

def test_client_proc_stats_record_and_clients():
    s = ClientProcStats()
    s.record("cid1", "alice", {"t.a": {"count": 10, "rate": 10.0,
                                       "recv_avg_ns": 1_000_000}})
    cs = s.clients()
    assert len(cs) == 1
    assert cs[0]["client_id"] == "cid1"
    assert cs[0]["username"] == "alice"
    assert "t.a" in cs[0]["topics"]


def test_client_proc_stats_topics_aggregate():
    s = ClientProcStats()
    s.record("c1", "u1", {"t.a": {"count": 10, "rate": 10.0,
                                  "recv_avg_ns": 1_000_000,
                                  "proc_avg_ns": 2_000_000}})
    s.record("c2", "u2", {"t.a": {"count": 30, "rate": 30.0,
                                  "recv_avg_ns": 3_000_000}})
    topics = s.topics()
    assert topics["t.a"]["rate_per_sec"] == 40.0
    # 加权平均 recv：(1ms*10 + 3ms*30) / 40 = 2.5ms
    assert topics["t.a"]["recv_avg_ms"] == pytest.approx(2.5, abs=0.01)
    # proc 仅 c1 上报（权重 10）→ 2ms
    assert topics["t.a"]["proc_avg_ms"] == pytest.approx(2.0, abs=0.01)


def test_client_proc_stats_summarize():
    s = ClientProcStats()
    s.record("c1", "u1", {"t.a": {"count": 10, "rate": 10.0,
                                  "recv_avg_ns": 1_000_000},
                          "t.b": {"count": 30, "rate": 30.0,
                                  "recv_avg_ns": 3_000_000}})
    entry = s.client_entry("c1")
    assert entry is not None
    summary = ClientProcStats.summarize(entry)
    assert summary["rate_per_sec"] == 40.0
    assert summary["count"] == 40
    assert summary["recv_avg_ms"] == pytest.approx(2.5, abs=0.01)


def test_client_proc_stats_remove_and_stale():
    s = ClientProcStats(stale_seconds=0.05)
    s.record("c1", "u1", {"t.a": {"count": 1, "rate": 1.0}})
    assert s.client_entry("c1") is not None
    s.remove("c1")
    assert s.client_entry("c1") is None

    s.record("c2", "u2", {"t.a": {"count": 1, "rate": 1.0}})
    time.sleep(0.1)
    s.sweep_stale()
    assert s.client_entry("c2") is None
    assert s.topics() == {}


# ---------------------------------------------------------------------------
# Server e2e：心跳 proc 上报 → admin API 可见
# ---------------------------------------------------------------------------

async def _run_e2e(record_dir: str) -> dict:
    """启动 server + 1 pub + 1 sub（多进程池），发布一批消息，等待心跳后取 /clients。"""
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
        username="sub", password="s", client_id="sub-proc",
    )
    pub = ProducerClient(
        data_endpoint=f"tcp://127.0.0.1:{dp}",
        control_endpoint=f"tcp://127.0.0.1:{cp}",
        username="pub", password="p", client_id="pub-proc",
    )
    try:
        await sub.start()
        await pub.start()
        await sub.subscribe("proc.t", mp_callbacks.on_msg_record)
        await asyncio.sleep(0.5)
        for i in range(60):
            await pub.publish("proc.t", {"i": i})
        await mp_callbacks.wait_records(record_dir, 30, timeout=8.0)
        # 轮询等心跳上报（worker 统计 1s 空闲批量 flush + 1s 心跳间隔 + 服务端
        # 可见；全量回归高负载下可能要多个周期，固定 sleep 会偶发不足）。
        clients = await _get_json(ap, "/api/v1/clients", token=srv.admin_token)
        realtime = await _get_json(ap, "/api/v1/stats/realtime",
                                   token=srv.admin_token)
        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline:
            by_id = {c["client_id"]: c for c in clients["clients"]}
            p = by_id.get("sub-proc", {}).get("processing", {})
            if "proc_avg_ms" in p and "recv_avg_ms" in p:
                break
            await asyncio.sleep(0.5)
            clients = await _get_json(ap, "/api/v1/clients", token=srv.admin_token)
            realtime = await _get_json(ap, "/api/v1/stats/realtime",
                                       token=srv.admin_token)
        return {"clients": clients, "realtime": realtime}
    finally:
        await sub.stop()
        await pub.stop()
        await srv.stop()


async def test_e2e_heartbeat_proc_pool_mode(record_dir):
    """多进程池模式：worker 统计随心跳上报，出现在 admin API。"""
    result = await _run_e2e(record_dir)
    by_id = {c["client_id"]: c for c in result["clients"]["clients"]}
    sub_entry = by_id["sub-proc"]
    assert "processing" in sub_entry, f"缺少 processing: {sub_entry}"
    p = sub_entry["processing"]
    assert p["rate_per_sec"] > 0
    assert p["count"] >= 30
    assert "recv_avg_ms" in p and p["recv_avg_ms"] >= 0
    assert "proc_avg_ms" in p and p["proc_avg_ms"] >= 0
    # publisher 不订阅 → 无 processing
    assert "processing" not in by_id["pub-proc"]
    # realtime 聚合包含该 topic
    topic_agg = result["realtime"]["processing_by_topic"].get("proc.t")
    assert topic_agg is not None
    assert topic_agg["rate_per_sec"] > 0
