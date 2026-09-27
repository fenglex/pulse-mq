"""多进程消费池（9.2.3）：SM 环单元测 + e2e（key 路由 / 同 key 保序 / worker 均衡）。"""
from __future__ import annotations

import asyncio
import json
import os
import socket as _sock
import tempfile
import time

import pytest

from pulsemq.client import ConsumerClient, ProducerClient
from pulsemq.server import Server
from pulsemq.worker_pool import ShmRing, stable_worker_index
from tests import mp_callbacks


# ---------------- ShmRing 单元测 ----------------

def test_ring_write_read_roundtrip():
    ring = ShmRing(None, 4096, create=True)
    try:
        assert ring.write(b"hello")
        assert ring.write(b"world" * 3)
        batch = ring.read_batch(10)
        assert batch == [b"hello", b"world" * 3]
        assert ring.read_batch(10) == []
        assert ring.pending_bytes() == 0
    finally:
        ring.close(unlink=True)


def test_ring_wraparound():
    """小环 + 回绕：填充标记跳过，数据完整。"""
    ring = ShmRing(None, 64, create=True)
    try:
        payloads = [b"x" * 10, b"y" * 10, b"z" * 10, b"w" * 10]
        written = 0
        for p in payloads:
            if ring.write(p):
                written += 1
        out = ring.read_batch(16)
        assert out == payloads[:written]  # 前缀完整（drop-new 只丢装不下的新帧）
    finally:
        ring.close(unlink=True)


def test_ring_full_drop_new():
    ring = ShmRing(None, 64, create=True)
    try:
        assert ring.write(b"a" * 40)       # 44B 占用
        assert not ring.write(b"b" * 40)   # 满 → drop-new
        assert ring.read_batch(4) == [b"a" * 40]
        assert ring.write(b"c" * 40)       # 释放后可写
    finally:
        ring.close(unlink=True)


def test_stable_worker_index_deterministic():
    assert stable_worker_index("mkt.a", 4) == stable_worker_index("mkt.a", 4)
    assert stable_worker_index("mkt.a", 4) < 4
    # 不同 key 应有分散性（抽查）
    spread = {stable_worker_index(f"t{i}", 4) for i in range(100)}
    assert len(spread) == 4


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
                    pass  # worker 正在写的半行，跳过
    return recs


async def test_workers_key_routing_and_ordering(tmp_path):
    """workers=2 + key=topic：全量送达、同 topic 落同 worker、双 worker 均衡、
    同 topic 内 seq 保序。回调经模块级函数写 JSONL 记录文件（跨进程可验证）。"""
    os.environ["PULSEMP_RECORD"] = str(tmp_path / "rec.jsonl")
    rec_path = tmp_path / "rec.jsonl"
    srv, dp, cp = await _start_server({"p": "p", "c": "c"})
    try:
        c = ConsumerClient(
            f"tcp://127.0.0.1:{dp}", f"tcp://127.0.0.1:{cp}",
            "c", "c",
            buffer_policy=None, workers=2, key="topic",
            worker_ring_mb=4, worker_init=mp_callbacks.init_recorder,
        )
        await c.start()
        await c.subscribe("mp.*", mp_callbacks.on_msg_record)
        await asyncio.sleep(0.5)
        p = ProducerClient(
            f"tcp://127.0.0.1:{dp}", f"tcp://127.0.0.1:{cp}", "p", "p")
        await p.start()
        # 挑选确定落入不同 worker 的 4 个 topic（各 2 个），保证双 worker 均衡
        from pulsemq.worker_pool import stable_worker_index
        sides = {0: [], 1: []}
        for i in range(50):
            t = f"mp.t{i}"
            sides[stable_worker_index(t, 2)].append(t)
        topics = sides[0][:2] + sides[1][:2]
        assert len({stable_worker_index(t, 2) for t in topics}) == 2
        n_per = 30
        for t in topics:
            for i in range(n_per):
                await p.publish(t, {"seq": i})
        # 等 worker 排空 + 写盘
        # 等 worker 排空 + 写盘
        for _ in range(40):
            await asyncio.sleep(0.5)
            if len(_load_records(str(rec_path))) >= len(topics) * n_per:
                break
        await p.stop()
        await c.stop()

        recs = _load_records(str(rec_path))
        assert len(recs) == len(topics) * n_per, f"应全量送达，实收 {len(recs)}"
        # 同 topic → 同 worker；两 worker 都被用到；同 topic 内 seq 保序
        topic_pid, topic_seqs = {}, {}
        pids = set()
        for r in recs:
            pids.add(r["pid"])
            topic_pid.setdefault(r["topic"], r["pid"])
            assert topic_pid[r["topic"]] == r["pid"], f"{r['topic']} 跨 worker！"
            topic_seqs.setdefault(r["topic"], []).append(r["seq"])
        assert len(pids) == 2, f"应使用 2 个 worker 进程，实际 {pids}"
        for t, seqs in topic_seqs.items():
            assert seqs == sorted(seqs), f"{t} 顺序被破坏: {seqs[:10]}"
    finally:
        os.environ.pop("PULSEMP_RECORD", None)
        await srv.stop()


async def test_workers_rejects_lambda_and_async():
    """9.2.4 唯一消费模式：lambda / 局部异步闭包在订阅时即报错（不启动池）；
    模块级异步函数（可 pickle）合法，worker 进程内事件循环执行。"""
    srv, dp, cp = await _start_server({"c": "c"})
    try:
        c = ConsumerClient(
            f"tcp://127.0.0.1:{dp}", f"tcp://127.0.0.1:{cp}",
            "c", "c", workers=2)
        with pytest.raises(ValueError, match="lambda|跨进程|模块级"):
            await c.subscribe("x.*", lambda m: None)
        async def async_cb(m):
            pass
        with pytest.raises(ValueError, match="模块级"):
            await c.subscribe("x.*", async_cb)
        # 池未创建
        assert c._pool is None
        # 模块级异步函数（可 pickle）合法
        await c.subscribe("y.*", mp_callbacks.on_msg_record_async)
        assert c._pool is None  # 未 start，池仍懒创建
    finally:
        await srv.stop()


async def test_workers_roundrobin_and_field_key():
    """key=payload 字段名按值路由（同 sym 同 worker）；key=None 轮询不在此覆盖
    （不保序语义，ring 层已有单测）。"""
    os.environ["PULSEMP_RECORD"] = str(tmp_path_rec := tempfile.mkstemp(suffix=".jsonl")[1])
    srv, dp, cp = await _start_server({"p": "p", "c": "c"})
    try:
        c = ConsumerClient(
            f"tcp://127.0.0.1:{dp}", f"tcp://127.0.0.1:{cp}",
            "c", "c", buffer_policy=None,
            workers=2, key="sym", worker_ring_mb=4,
            worker_init=mp_callbacks.init_recorder)
        await c.start()
        await c.subscribe("fld.*", mp_callbacks.on_msg_record)
        await asyncio.sleep(0.5)
        p = ProducerClient(
            f"tcp://127.0.0.1:{dp}", f"tcp://127.0.0.1:{cp}", "p", "p")
        await p.start()
        for i in range(40):
            await p.publish("fld.x", {"seq": i, "sym": "A" if i % 2 else "B"})
        for _ in range(40):
            await asyncio.sleep(0.5)
            if len(_load_records(tmp_path_rec)) >= 40:
                break
        await p.stop()
        await c.stop()
        recs = _load_records(tmp_path_rec)
        assert len(recs) == 40
        # payload 字段 key="sym"：同 sym 同 worker（两个 topic 相同 → 按 sym 分流）
        sym_pid = {}
        for r in recs:
            sym = "A" if r["seq"] % 2 else "B"
            sym_pid.setdefault(sym, r["pid"])
            assert sym_pid[sym] == r["pid"], f"sym={sym} 跨 worker"
        # key 为字段名 → 接收侧需解码 payload（覆盖该路径不抛错即通过）
    finally:
        os.environ.pop("PULSEMP_RECORD", None)
        await srv.stop()

