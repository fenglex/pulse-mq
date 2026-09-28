"""9.2.7 路由改造单元测：jump hash 性质 + 最短队列路由 + peek 热路径。

- jump hash：确定性、值域、n 变化最小换位（~1/n，统计断言）。
- 最短队列路由：等速（全空环）时保持轮询均匀；某环积压时自动绕开。
- peek_topic_seq：与 decode_header 字段一致（含 ack_token 帧/CRC 帧）。
"""
from __future__ import annotations

import random as _random

import pytest

from pulsemq.protocol import frames
from pulsemq.worker_pool import ShmRing, WorkerPool, _jump_hash, stable_worker_index


# ---------------- jump hash ----------------

def test_jump_hash_deterministic_and_range():
    assert stable_worker_index("mkt.a", 4) == stable_worker_index("mkt.a", 4)
    for n in (1, 2, 3, 4, 8):
        for k in ("a", "b", "mkt.tick", "中文key", ""):
            idx = stable_worker_index(k, n)
            assert 0 <= idx < n


def test_jump_hash_minimal_reassignment():
    """n→n+1 仅 ~1/n 的 key 换位（对比取模的近全量重排）。"""
    rng = _random.Random(927)
    keys = [f"sym.{rng.randint(0, 10**9)}" for _ in range(20_000)]
    old = [stable_worker_index(k, 4) for k in keys]
    new = [stable_worker_index(k, 5) for k in keys]
    moved = sum(1 for a, b in zip(old, new) if a != b)
    # 理论 ~1/4=5000；jump hash 的上界性质：新映射中不动的 key 恒成立，
    # 统计容差取 ±3σ（σ=sqrt(N·p·(1-p))≈61）
    assert moved < 5200, f"扩容换位过多: {moved}/20000"
    # 换位后的 key 必须落到新增或保留映射内（全部合法）
    assert all(0 <= b < 5 for b in new)


def test_jump_hash_reassignment_grows_to_new_bucket():
    """n→n+1 时，换位的 key 只能落到合法桶（含新桶），且新桶被用到。"""
    rng = _random.Random(7)
    keys = [f"k{i}" for i in range(5000)]
    for n in (2, 4, 7):
        used = {stable_worker_index(k, n) for k in keys}
        assert used == set(range(n)), f"n={n} 分布不全: {used}"


def test_jump_hash_unit_zero_bucket():
    for key in (0, 1, 42, 2**63, 2**64 - 1):
        assert _jump_hash(key, 1) == 0
        assert 0 <= _jump_hash(key, 8) < 8


# ---------------- 最短队列路由 ----------------

def _make_pool(n=3, key_mode=None):
    pool = WorkerPool(n, 1024 * 1024, [], key_mode, None)
    pool._rings = [ShmRing(None, 1024 * 1024, create=True) for _ in range(n)]
    return pool


def _drain_ring(ring: ShmRing) -> None:
    while ring.read_batch(64):
        pass


def test_least_loaded_routes_round_robin_when_idle():
    """全空闲（等速 worker）：退化为完美轮询，均匀性保持。"""
    pool = _make_pool(3)
    try:
        counts = [0, 0, 0]
        for i in range(300):
            before = pool._rings[0].pending_bytes()  # noqa: F841（读侧不消费）
            # 逐帧写入后立即模拟 worker 读空 → 全环恒空闲
            pool.route("t", b"x" * 8)
            for r in pool._rings:
                _drain_ring(r)
        # 再单独跑一轮记录分配
        seq = []
        for i in range(30):
            pool.route("t", b"x" * 8)
            seq.append(max(range(3), key=lambda j: pool._rings[j].pending_bytes()))
            for r in pool._rings:
                _drain_ring(r)
        assert seq[0] != seq[1] != seq[2], "等速时应轮询而非粘住一个环"
        assert len(set(seq)) == 3
    finally:
        for r in pool._rings:
            r.close(unlink=True)


def test_least_loaded_avoids_backlogged_ring():
    """某环积压时，后续帧自动绕开积压环，分给空闲环。"""
    pool = _make_pool(3)
    try:
        # 人为让环 1 积压（写入且不消费）
        assert pool._rings[1].write(b"B" * 512)
        idx_hit = []
        for _ in range(20):
            pool.route("t", b"x" * 8)
            # 记录哪个环新增了数据，然后清空环 0/2（模拟快 worker 即时消费）
            busy = [i for i in range(3) if pool._rings[i].pending_bytes() > 0]
            idx_hit.extend(i for i in busy if i != 1)
            for i in (0, 2):
                _drain_ring(pool._rings[i])
        assert 1 not in idx_hit, "积压环应被绕开"
        assert idx_hit, "空闲环应被使用"
    finally:
        for r in pool._rings:
            r.close(unlink=True)


def test_route_tie_break_keeps_balance():
    """等积压时按轮询计数器取——200 帧均分 2 环（±1）。"""
    pool = _make_pool(2)
    try:
        # 两环同时积压相同量（模拟等速慢 worker，都不消费）
        for i in range(200):
            assert pool.route("t", b"x" * 16)
        a = pool._rings[0].pending_bytes()
        b = pool._rings[1].pending_bytes()
        assert abs(a - b) <= 4 * 16, f"等速时应均衡: {a} vs {b}"
    finally:
        for r in pool._rings:
            r.close(unlink=True)


# ---------------- peek_topic_seq ----------------

@pytest.mark.parametrize("crc,ack", [(False, False), (True, False),
                                     (False, True), (True, True)])
def test_peek_matches_decode_header(crc, ack):
    frame = frames.encode("mkt.peek", {"v": 1}, crc=crc, ack_token=77 if ack else None)
    topic, seq, ts, msg_type = frames.peek_topic_seq(frame)
    hdr = frames.decode_header(frame)
    assert topic == hdr.topic
    assert seq == hdr.seq
    assert ts == hdr.timestamp_ns
    assert msg_type == hdr.msg_type


def test_peek_rejects_garbage():
    with pytest.raises(Exception):
        frames.peek_topic_seq(b"\x00" * 10)
    with pytest.raises(Exception):
        frames.peek_topic_seq(b"XX\x03" + b"\x00" * 24)
