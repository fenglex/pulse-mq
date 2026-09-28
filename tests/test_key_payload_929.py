"""9.2.9 key 拆分载荷契约：仅 dict/str 支持、DataFrame 显式不支持 + 回退
topic 路由、精确 topic 订阅强制。

- _peek_payload：dict/str 返回真实载荷；DataFrame/bytes/坏帧抛
  _KeyNotExtractable（9.2.8 前一律返回 {}，key="None" 全帧静默落单 worker）。
- route()：字段路由 sticky；str 载荷内容本身即 key；DataFrame/字段缺失/
  callable 异常或返回 None → 回退 stable_worker_index(topic) + 计数 +
  每原因一次告警（take_key_fallback delta 语义）。
- subscribe()：key 拆分（字段名/callable）+ 通配符 → ValueError；
  key="topic"/None + 通配符不受限。
- record_extra(key_fallback=...)：服务端累计 key_fallback_cum。
- e2e：DataFrame 发布 + key="sym" 消费 → 全帧落同 worker（topic 回退），
  心跳上报后服务端客户端条目出现 key_fallback_cum。
"""
from __future__ import annotations

import asyncio
import json
import os
import socket as _sock
import tempfile

import pytest

from pulsemq.client import ConsumerClient, ProducerClient
from pulsemq.protocol import frames
from pulsemq.server import Server
from pulsemq.stats.throughput import ClientProcStats
from pulsemq.worker_pool import (
    ShmRing,
    WorkerPool,
    _KeyNotExtractable,
    _peek_payload,
    stable_worker_index,
)
from tests import mp_callbacks

pd = pytest.importorskip("pandas")


# ---------------- _peek_payload ----------------

def test_peek_returns_real_dict_and_str_payload():
    assert _peek_payload(frames.encode("t", {"sym": "A"})) == {"sym": "A"}
    assert _peek_payload(frames.encode("t", "AAPL")) == "AAPL"


def test_peek_rejects_unsupported_payloads():
    with pytest.raises(_KeyNotExtractable) as e:
        _peek_payload(frames.encode("t", pd.DataFrame({"sym": ["A"]})))
    assert "DataFrame" in e.value.reason
    with pytest.raises(_KeyNotExtractable) as e:
        _peek_payload(frames.encode("t", b"\x01raw"))
    assert "bytes" in e.value.reason
    with pytest.raises(_KeyNotExtractable) as e:
        _peek_payload(b"garbage-not-a-frame")
    assert e.value.reason == "decode_failed"


# ---------------- route() ----------------

def _make_pool(n=3, key_mode=None):
    pool = WorkerPool(n, 1024 * 1024, [], key_mode, None)
    pool._rings = [ShmRing(None, 1024 * 1024, create=True) for _ in range(n)]
    return pool


def _drain(ring: ShmRing) -> None:
    while ring.read_batch(64):
        pass


def _drain_all(pool: WorkerPool) -> None:
    for r in pool._rings:
        _drain(r)


def _occupied(pool: WorkerPool) -> list[int]:
    return [i for i, r in enumerate(pool._rings) if r.pending_bytes() > 0]


def test_route_field_key_sticky_and_spread():
    """字段路由：同 sym 恒定同环（内容变化不影响），不同 sym 有分散性。"""
    pool = _make_pool(3, "sym")
    try:
        pool.route("t", frames.encode("t", {"sym": "A"}))
        (idx_a,) = _occupied(pool)
        _drain_all(pool)
        for i in range(5):
            pool.route("t", frames.encode("t", {"sym": "A", "i": i}))
        assert _occupied(pool) == [idx_a]
        _drain_all(pool)
        used = set()
        for s in [f"S{i}" for i in range(30)]:
            pool.route("t", frames.encode("t", {"sym": s}))
            used.update(_occupied(pool))
            _drain_all(pool)
        assert len(used) >= 2, "30 个不同 sym 应分散到多个环"
        assert pool.key_fallback == 0
    finally:
        for r in pool._rings:
            r.close(unlink=True)


def test_route_str_payload_content_is_key():
    """str 载荷 + 字段名模式：消息内容本身即 key（同内容同环）。"""
    pool = _make_pool(3, "sym")
    try:
        pool.route("t", frames.encode("t", "AAPL"))
        (idx,) = _occupied(pool)
        _drain_all(pool)
        for _ in range(3):
            pool.route("t", frames.encode("t", "AAPL"))
        assert _occupied(pool) == [idx]
        assert pool.key_fallback == 0
    finally:
        for r in pool._rings:
            r.close(unlink=True)


def test_route_dataframe_falls_back_to_topic():
    """DataFrame + 字段路由：显式回退 topic 路由（同 topic 同环、保序），
    计数 + 每原因一次告警 + take_key_fallback delta 语义。"""
    pool = _make_pool(3, "sym")
    try:
        f = frames.encode("t", pd.DataFrame({"sym": ["A", "B"], "px": [1, 2]}))
        for _ in range(10):
            assert pool.route("t", f)
        assert _occupied(pool) == [stable_worker_index("t", 3)]
        assert pool.key_fallback == 10
        assert "payload_type:DataFrame" in pool._key_warned
        assert pool.take_key_fallback() == 10
        assert pool.take_key_fallback() == 0
        # 再来 5 帧：计数重新累计，但不再新增告警原因
        for _ in range(5):
            pool.route("t", f)
        assert pool.take_key_fallback() == 5
        assert len(pool._key_warned) == 1
    finally:
        for r in pool._rings:
            r.close(unlink=True)


def test_route_missing_field_falls_back():
    pool = _make_pool(2, "sym")
    try:
        assert pool.route("t", frames.encode("t", {"other": 1}))
        assert pool.key_fallback == 1
        assert "missing_field:sym" in pool._key_warned
    finally:
        for r in pool._rings:
            r.close(unlink=True)


def test_route_callable_receives_real_payload():
    """callable 收到真实载荷（dict/str 本身），可正常提取 key。"""
    seen = []

    def k(payload, topic):
        seen.append((payload, topic))
        return payload["sym"] if isinstance(payload, dict) else payload

    pool = _make_pool(3, k)
    try:
        pool.route("t1", frames.encode("t1", {"sym": "A"}))
        pool.route("t2", frames.encode("t2", "MSFT"))
        assert seen == [({"sym": "A"}, "t1"), ("MSFT", "t2")]
        assert pool.key_fallback == 0
    finally:
        for r in pool._rings:
            r.close(unlink=True)


def test_route_callable_error_and_none_fall_back():
    def boom(payload, topic):
        raise RuntimeError("user bug")

    def none_key(payload, topic):
        return None

    for mode in (boom, none_key):
        pool = _make_pool(2, mode)
        try:
            assert pool.route("t", frames.encode("t", {"sym": "A"}))
            assert pool.key_fallback == 1
            assert any(r.startswith("error:") or r == "callable 返回 None"
                       for r in pool._key_warned)
        finally:
            for r in pool._rings:
                r.close(unlink=True)


# ---------------- subscribe() 精确 topic 校验 ----------------

def _consumer(key) -> ConsumerClient:
    return ConsumerClient("tcp://127.0.0.1:1", "tcp://127.0.0.1:2",
                          "u", "p", workers=2, key=key)


async def test_subscribe_wildcard_rejected_for_key_split():
    """key 拆分（字段名/callable）+ 通配符 → 订阅即报错（fail fast）。"""
    c = _consumer("sym")
    with pytest.raises(ValueError, match="通配符"):
        await c.subscribe("fld.*", mp_callbacks.on_msg_record)

    c2 = _consumer(lambda p, t: "x")
    with pytest.raises(ValueError, match="通配符"):
        await c2.subscribe("fld.*", mp_callbacks.on_msg_record)
    # 精确 topic 不受限
    await c.subscribe("fld.x", mp_callbacks.on_msg_record)


async def test_subscribe_wildcard_ok_for_topic_and_none():
    """key="topic"（按消息 topic 路由）与 key=None 不受精确 topic 约束。"""
    c1 = _consumer("topic")
    await c1.subscribe("fld.*", mp_callbacks.on_msg_record)
    c2 = _consumer(None)
    await c2.subscribe("fld.*", mp_callbacks.on_msg_record)
    assert "fld.*" in c1._subscriptions and "fld.*" in c2._subscriptions


# ---------------- 服务端累计 ----------------

def test_record_extra_key_fallback_accumulates():
    st = ClientProcStats()
    st.record_extra("c1", key_fallback=5)
    st.record_extra("c1", key_fallback=7)
    e = st.client_entry("c1")
    assert e is not None and e["key_fallback_cum"] == 12


# ---------------- e2e ----------------

def _free_port() -> int:
    s = _sock.socket(_sock.AF_INET, _sock.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


async def _start_server(creds):
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


def _load_records(base: str):
    import glob
    recs = []
    for f in glob.glob(base + ".*"):
        with open(f, encoding="utf-8") as fh:
            for x in fh.read().strip().splitlines():
                if not x:
                    continue
                try:
                    recs.append(json.loads(x))
                except json.JSONDecodeError:
                    pass
    return recs


async def test_dataframe_key_split_e2e_fallback_visible():
    """DataFrame + key="sym"：全帧落同 worker（topic 回退、同 topic 保序），
    心跳上报后服务端客户端条目累计 key_fallback_cum（可观测闭环）。"""
    os.environ["PULSEMP_RECORD"] = str(
        base := tempfile.mkstemp(suffix=".jsonl")[1])
    srv, dp, cp = await _start_server({"p": "p", "c": "c"})
    try:
        c = ConsumerClient(
            f"tcp://127.0.0.1:{dp}", f"tcp://127.0.0.1:{cp}",
            "c", "c", buffer_policy=None,
            workers=2, key="sym", worker_ring_mb=4,
            worker_init=mp_callbacks.init_recorder)
        await c.start()
        await c.subscribe("dfkey.x", mp_callbacks.on_msg_record)
        await asyncio.sleep(0.5)
        p = ProducerClient(
            f"tcp://127.0.0.1:{dp}", f"tcp://127.0.0.1:{cp}", "p", "p")
        await p.start()
        for i in range(20):
            await p.publish(
                "dfkey.x", pd.DataFrame({"sym": ["A", "B", "C"],
                                         "i": [i, i, i]}))
        for _ in range(40):
            await asyncio.sleep(0.5)
            if len(_load_records(base)) >= 20:
                break
        await asyncio.sleep(2.5)  # 等心跳把 key_fallback delta 报上来
        # 先查服务端条目再 stop（DISCONNECT 会清除客户端统计条目）
        e = srv._proc_stats.client_entry(c._client_id)
        recs = _load_records(base)
        await p.stop()
        await c.stop()
        assert len(recs) == 20, f"全量送达失败: {len(recs)}/20"
        # 回退 topic 路由：同 topic 全部落同一个 worker（不静默散落/不丢）
        pids = {r["pid"] for r in recs}
        assert len(pids) == 1, f"DataFrame 应回退 topic 路由落同 worker: {pids}"
        assert e is not None and e.get("key_fallback_cum", 0) >= 20
    finally:
        os.environ.pop("PULSEMP_RECORD", None)
        await srv.stop()
