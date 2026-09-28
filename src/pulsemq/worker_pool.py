"""消费端多进程 worker 池：共享内存变长字节环 + worker 进程主循环。

架构（ConsumerClient(workers=N) 启用）：
  接收循环（主进程，批量排空 recv）──按 key 路由──> N 个 SM 字节环
  （每 worker 一个 SPSC 环）                └→ worker 进程：出环 → 解码 → 用户回调

路由模式（key 参数）：None=最短队列（等速时即轮询）、"topic"/字段名/
callable=jump consistent hash 粘性（同 key 恒定同 worker，n 变化仅
~1/n 换位）。

环布局（共享内存）：[tail:8][head:8][data:cap]，tail/head 为单调累计字节
（tail 仅写者写，head 仅读者写，x86 TSO 下"先数据后序号 / 先序号后数据"
安全；ARM 需内存栅栏，暂不支持）。
记录格式：[len:4 LE][payload:len]；len=0xFFFFFFFF 为回绕填充标记
（跳到数据区物理起点）。
满语义：drop-new（丢最新帧并计数）。信用窗口守恒（服务端按心跳快照放行）
使环在正常运行时不会满；仅 worker 停滞时兜底。

worker 内用户回调必须是模块级可导入函数（经 pickle 按引用传递），同步或
异步均可（异步回调在 worker 进程内的事件循环上执行，同一 worker 内串行）；
回调异常被捕获记日志，不影响 worker 存活。worker 进程不与主进程共享内存，
闭包捕获的变量在 worker 内是独立拷贝。
"""
from __future__ import annotations

import inspect
import os
import struct
import threading
import time
import zlib

from pulsemq.logging_setup import logger
from pulsemq.protocol import frames
from pulsemq.routing import SubscriptionTable

_PAD = 0xFFFFFFFF
_LEN = struct.Struct("<I")
_U64 = struct.Struct("<Q")
_META = 32  # tail + head + 诊断计数（read_n/proc_n）


class ShmRing:
    """变长字节环（单写单读）。ingestor 侧为写者，worker 侧为读者。"""

    def __init__(self, name: str | None, size_bytes: int, create: bool):
        from multiprocessing import shared_memory
        self._size = size_bytes
        if create:
            self._sm = shared_memory.SharedMemory(
                name=name, create=True, size=_META + size_bytes, track=False)
            self._buf = self._sm.buf
            self._buf[0:8] = b"\x00" * 8   # tail = 0
            self._buf[8:16] = b"\x00" * 8  # head = 0
        else:
            self._sm = shared_memory.SharedMemory(name=name)
            self._buf = self._sm.buf
        self.name = self._sm.name

    # -- 写者（ingestor）--

    def write(self, payload: bytes) -> bool:
        """写入一帧。环满返回 False（drop-new，调用方计数）。"""
        buf = self._buf
        tail = _U64.unpack_from(buf, 0)[0]
        head = _U64.unpack_from(buf, 8)[0]
        cap = self._size
        need = 4 + len(payload)
        free = cap - (tail - head)
        if need > free:
            return False
        off = _META + tail % cap
        contig = cap - (tail % cap)
        if contig < need:
            # 回绕：尾部碎片写填充标记，tail 推到物理起点后重试
            pad = cap - (tail % cap)
            _LEN.pack_into(buf, off, _PAD)
            tail += pad
            free -= pad
            if need > free:
                _U64.pack_into(buf, 0, tail)
                return False
            off = _META
        _LEN.pack_into(buf, off, len(payload))
        buf[off + 4: off + 4 + len(payload)] = payload
        _U64.pack_into(buf, 0, tail + need)
        return True

    def free_bytes(self) -> int:
        buf = self._buf
        return self._size - (_U64.unpack_from(buf, 0)[0]
                             - _U64.unpack_from(buf, 8)[0])

    # -- 读者（worker）--

    def read_batch(self, max_items: int = 64) -> list[bytes]:
        """批量出环（最多 max_items 帧），返回 payload 列表。"""
        buf = self._buf
        cap = self._size
        out = []
        head = _U64.unpack_from(buf, 8)[0]
        tail = _U64.unpack_from(buf, 0)[0]
        while head < tail and len(out) < max_items:
            off = _META + head % cap
            (ln,) = _LEN.unpack_from(buf, off)
            if ln == _PAD:
                head += cap - (head % cap)  # 跳过回绕填充，到物理起点
                continue
            out.append(bytes(buf[off + 4: off + 4 + ln]))
            head += 4 + ln
        if head != _U64.unpack_from(buf, 8)[0]:
            _U64.pack_into(buf, 8, head)
        return out

    def diag_counters(self) -> tuple[int, int]:
        """(worker 已读帧数, 已处理帧数)——worker 侧写入的强诊断计数。"""
        r = _U64.unpack_from(self._buf, 16)[0]
        pr = _U64.unpack_from(self._buf, 24)[0]
        return r, pr

    def add_diag(self, read_n: int, proc_n: int) -> None:
        t = _U64.unpack_from(self._buf, 16)[0] + read_n
        struct.pack_into("<Q", self._buf, 16, t)
        p = _U64.unpack_from(self._buf, 24)[0] + proc_n
        struct.pack_into("<Q", self._buf, 24, p)

    def pending_bytes(self) -> int:
        buf = self._buf
        return _U64.unpack_from(buf, 0)[0] - _U64.unpack_from(buf, 8)[0]

    def close(self, unlink: bool = False) -> None:
        self._sm.close()
        if unlink:
            try:
                self._sm.unlink()
            except Exception:
                pass


def _jump_hash(key: int, n_buckets: int) -> int:
    """Lamping-Voss 跳跃一致性哈希（整数版，O(log n)，跨平台确定）。

    性质：bucket 数 n→n+1 时仅 ~1/n 的 key 换位（对比取模的近全量重排）。
    """
    b, j = -1, 0
    while j < n_buckets:
        b = j
        key = (key * 2862933555777941757 + 1) & 0xFFFFFFFFFFFFFFFF
        j = (b + 1) * (1 << 31) // ((key >> 33) + 1)
    return b


def stable_worker_index(key: str, n_workers: int) -> int:
    """确定性 key → worker 映射（jump consistent hash over crc32）。

    - n 不变：同 key 恒定映射（跨进程/重启一致，禁用内置 hash——有随机化）。
    - n 变化（扩容重启）：仅 ~1/n 的 key 换位，per-key 状态/粘性大体保留。
    """
    if n_workers <= 1:
        return 0
    return _jump_hash(zlib.crc32(key.encode("utf-8")), n_workers)


WORKER_POLL_SLEEP = 0.0002


def _worker_main(worker_index: int, ring_name: str, ring_bytes: int,
                 subs: list, worker_init, stats_q, stop_ev, sub_q) -> None:
    """worker 进程入口：出环 → 解码 → 回调分发。sub_q 接收动态订阅。"""
    import asyncio
    ring = ShmRing(ring_name, ring_bytes, create=False)
    if worker_init is not None:
        try:
            worker_init(worker_index)
        except Exception:
            logger.exception("worker_init 异常（worker {}）", worker_index)
    sub_table = SubscriptionTable()
    cb_map: dict[str, tuple] = {}
    for pattern, cb, ho in subs:
        sub_table.subscribe(pattern.encode("utf-8"), pattern)
        cb_map[pattern] = (cb, ho)
    # 异步回调运行在 worker 进程自有事件循环上（同一 worker 内串行）
    aloop = asyncio.new_event_loop()

    def _drain_subs() -> None:
        while True:
            try:
                op, pattern, cb, ho = sub_q.get_nowait()
            except Exception:
                return
            if op == "add":
                sub_table.subscribe(pattern.encode("utf-8"), pattern)
                cb_map[pattern] = (cb, ho)
            elif op == "remove":
                sub_table.unsubscribe(pattern.encode("utf-8"), pattern)
                cb_map.pop(pattern, None)

    def _dispatch(cb, target) -> None:
        """执行单个回调：同步直接调用，异步在 worker 事件循环上串行执行。"""
        if inspect.iscoroutinefunction(cb):
            aloop.run_until_complete(cb(target))
        else:
            cb(target)
    stats = {"processed": 0, "rows": 0, "proc": {}, "read": 0, "e2e": {}}
    trace_path = os.environ.get("PULSEMP_TRACE")
    trace_fd = os.open(f"{trace_path}.{worker_index}",
                       os.O_APPEND | os.O_CREAT | os.O_WRONLY) if trace_path else None
    last_stat = time.monotonic()
    cpu0, t0 = time.process_time(), time.perf_counter()
    empty_polls = 0
    while True:
        _drain_subs()
        batch = ring.read_batch(64)
        if not batch:
            # 空闲路径也要定期 flush：否则最后一批统计滞留（无新帧触发不了 flush）
            if time.monotonic() - last_stat >= 1.0:
                _flush_stats(stats, stats_q, worker_index, cpu0, t0)
                last_stat = time.monotonic()
            if stop_ev.is_set():
                empty_polls += 1
                if empty_polls >= 3:  # ~150ms 无新帧确认排空
                    break
                time.sleep(0.05)
                continue
            time.sleep(WORKER_POLL_SLEEP)
            continue
        empty_polls = 0
        now_ns = time.time_ns()
        stats["read"] += len(batch)
        ring.add_diag(len(batch), 0)
        for fb in batch:
            try:
                hdr = frames.decode_header(fb)
            except Exception:
                logger.debug("worker 帧头解码失败，丢弃")
                if trace_fd:
                    os.write(trace_fd, b"BADHDR\n")
                continue
            if trace_fd:
                os.write(trace_fd, f"R {hdr.topic} {hdr.timestamp_ns}\n".encode())
            # 处理时端到端延迟（9.2.8）：出环时刻 - 帧内生产时间戳。
            # 含环内排队等待——积压时该值直接反映消费延迟，随统计批量上报。
            e2e = now_ns - hdr.timestamp_ns
            ent = stats["e2e"].get(hdr.topic)
            if ent is None:
                stats["e2e"][hdr.topic] = [e2e, 1, e2e]
            else:
                ent[0] += e2e
                ent[1] += 1
                if e2e > ent[2]:
                    ent[2] = e2e
            matched = [cb_map[p.decode("utf-8")]
                       for p in sub_table.match(hdr.topic)
                       if p.decode("utf-8") in cb_map]
            if not matched:
                continue
            t_proc0 = time.perf_counter_ns()
            need_decode = any(not ho for _, ho in matched)
            msg = None
            if need_decode:
                try:
                    msg = frames.decode(fb)
                except Exception:
                    logger.debug("worker 帧解码失败，丢弃")
                    continue
            for cb, ho in matched:
                target = hdr if ho else msg
                try:
                    _dispatch(cb, target)
                except Exception:
                    logger.exception("worker 订阅回调异常（worker {}）", worker_index)
            stats["processed"] += 1
            ring.add_diag(0, 1)
            stats["rows"] += hdr.record_count
            _acc(stats["proc"], hdr.topic, time.perf_counter_ns() - t_proc0)
        if time.monotonic() - last_stat >= 1.0:
            _flush_stats(stats, stats_q, worker_index, cpu0, t0)
            last_stat = time.monotonic()
    _flush_stats(stats, stats_q, worker_index, cpu0, t0)
    aloop.close()
    ring.close()



def _acc(d: dict, topic: str, ns: int) -> None:
    e = d.get(topic)
    if e is None:
        d[topic] = [ns, 1]
    else:
        e[0] += ns
        e[1] += 1


def _flush_stats(stats: dict, stats_q, worker_index: int,
                 cpu0: float, t0: float) -> None:
    payload = {
        "worker": worker_index,
        "read": stats["read"],
        "processed": stats["processed"],
        "rows": stats["rows"],
        "cpu_cores": (time.process_time() - cpu0) / max(1e-9, time.perf_counter() - t0),
        "proc": {t: v for t, v in stats["proc"].items()},
        # 处理时 e2e：{topic: [累计ns, 条数, 最大ns]}（9.2.8）
        "e2e": {t: v for t, v in stats["e2e"].items()},
    }
    stats["processed"] = 0
    stats["rows"] = 0
    stats["read"] = 0
    stats["proc"].clear()
    stats["e2e"].clear()
    try:
        stats_q.put_nowait(payload)
    except Exception:
        pass


class WorkerPool:
    """多进程消费池：ingestor 侧持有 N 个环 + N 个 worker 进程。"""

    def __init__(self, n_workers: int, ring_bytes: int, subs: list,
                 key_mode, worker_init, on_stats=None) -> None:
        self.n_workers = n_workers
        self.ring_bytes = ring_bytes
        self.subs = subs                # [(pattern, callback, header_only)]
        self.key_mode = key_mode        # None | "topic" | callable(payload, topic)->str | 字段名
        self.worker_init = worker_init
        self._on_stats = on_stats       # 心跳前回调：聚合 worker 统计
        self._rings: list[ShmRing] = []
        self._procs = []
        self._stop_ev = None
        self._stats_q = None
        self._sub_qs = None
        self._max_frame = 1024
        self._drained_stats = []
        self._rr = 0
        self.routed = 0
        self.write_fail = 0
        self.key_fallback = 0        # key 拆分失败回退 topic 路由的帧数（delta 上报）
        self._key_warned: set[str] = set()

    def start(self) -> None:
        ctx = __import__("multiprocessing").get_context("spawn")
        self._stop_ev = ctx.Event()
        self._stats_q = ctx.Queue()
        self._sub_qs = [ctx.Queue() for _ in range(self.n_workers)]
        for i in range(self.n_workers):
            ring = ShmRing(None, self.ring_bytes, create=True)
            self._rings.append(ring)
            p = ctx.Process(
                target=_worker_main,
                args=(i, ring.name, self.ring_bytes, self.subs,
                      self.worker_init, self._stats_q, self._stop_ev,
                      self._sub_qs[i]),
                daemon=True, name=f"pulsemq-worker-{i}")
            p.start()
            self._procs.append(p)
        # 等 worker attach（Windows SM 创建后立即可 attach，短暂 sleep 保险）
        time.sleep(0.3)

    def route(self, topic: str, payload_bytes: bytes) -> bool:
        """按 key 路由一帧。返回 False 表示环满（drop-new，调用方计数）。

        key_mode=None → 最短队列路由：优先积压字节数最少的环（慢 worker
        自动降载、快 worker 自动补位），并列时按轮询计数器取——worker
        等速时退化为完美轮询，均匀性不丢。其余模式见 stable_worker_index。
        """
        n = self.n_workers
        if self.key_mode is None:
            pend = [r.pending_bytes() for r in self._rings]
            target = min(pend)
            idx = -1
            for k in range(n):
                i = (self._rr + k) % n
                if pend[i] == target:
                    idx = i
                    self._rr += 1
                    break
        elif self.key_mode == "topic":
            idx = stable_worker_index(topic, n)
        else:
            # key 拆分（payload 字段名 / callable）：仅支持 dict / str 载荷。
            # DataFrame/bytes、字段缺失、callable 异常/返回 None、解码失败
            # → 显式告警并回退 topic 路由（确定性、同 topic 保序），
            # 不再静默退化成单 worker（9.2.9）。
            try:
                obj = _peek_payload(payload_bytes)
                if callable(self.key_mode):
                    key = self.key_mode(obj, topic)
                    if key is None:
                        raise _KeyNotExtractable("callable 返回 None")
                elif isinstance(obj, dict):
                    key = obj.get(self.key_mode)
                    if key is None:
                        raise _KeyNotExtractable(
                            f"missing_field:{self.key_mode}")
                else:  # str 载荷：消息内容本身即 key
                    key = obj
                idx = stable_worker_index(str(key), n)
            except _KeyNotExtractable as e:
                idx = self._key_fallback(topic, e.reason)
            except Exception as e:
                idx = self._key_fallback(topic, f"error:{type(e).__name__}")
        ok = self._rings[idx].write(payload_bytes)
        self.routed += 1
        if not ok:
            self.write_fail += 1
        if ok:
            if len(payload_bytes) > self._max_frame:
                self._max_frame = len(payload_bytes)
        return ok

    def _key_fallback(self, topic: str, reason: str) -> int:
        """key 拆分失败 → 回退 topic 路由（确定性、同 topic 保序）。

        每个原因只告警一次（含首帧计数）；key_fallback 计数经心跳
        take_key_fallback() 上报服务端（/clients 的 key_fallback 字段）。
        """
        self.key_fallback += 1
        if reason not in self._key_warned:
            self._key_warned.add(reason)
            logger.warning(
                "key 路由回退为 topic 路由（原因 {}，topic {!r}）："
                "key 拆分仅支持 dict/str 载荷，DataFrame 不支持——"
                "请在发布端按 key 拆分后逐组发布，或改用 key='topic'/None",
                reason, topic)
        return stable_worker_index(topic, self.n_workers)

    def take_key_fallback(self) -> int:
        """返回并清零 key 回退计数（心跳 delta 上报用）。"""
        v = self.key_fallback
        self.key_fallback = 0
        return v

    def add_subscription(self, pattern: str, cb, header_only: bool) -> None:
        """向全部 worker 下发新订阅（start 后调用 subscribe 时）。"""
        if self._sub_qs is None:
            return
        for q in self._sub_qs:
            try:
                q.put_nowait(("add", pattern, cb, header_only))
            except Exception:
                pass

    def remove_subscription(self, pattern: str) -> None:
        """向全部 worker 下发退订（unsubscribe 时）。幂等。"""
        self.subs = [(p, cb, ho) for p, cb, ho in self.subs if p != pattern]
        if self._sub_qs is None:
            return
        for q in self._sub_qs:
            try:
                q.put_nowait(("remove", pattern, None, None))
            except Exception:
                pass

    def free_credit(self) -> int:
        """心跳 credit：全部环空闲字节 / 观测最大帧长（保守帧数上界）。"""
        free = sum(r.free_bytes() for r in self._rings)
        return max(1, free // self._max_frame)

    def diag_counters(self) -> tuple[int, int]:
        """(全部 worker 已读帧数, 已处理帧数)——聚合各环的强诊断计数。"""
        rs = ps = 0
        for r in self._rings:
            a, b = r.diag_counters()
            rs += a
            ps += b
        return rs, ps

    def ring_snapshots(self) -> list[dict]:
        """每 worker 环的即时快照（9.2.8，心跳上报用）：
        [{pending_bytes, free_bytes, read_n, proc_n}]，按 worker 序排列。"""
        out = []
        for r in self._rings:
            read_n, proc_n = r.diag_counters()
            out.append({
                "pending_bytes": r.pending_bytes(),
                "free_bytes": r.free_bytes(),
                "read_n": read_n,
                "proc_n": proc_n,
            })
        return out

    def pending_bytes(self) -> int:
        return sum(r.pending_bytes() for r in self._rings)

    def drain_stats(self) -> list[dict]:
        out = []
        if self._stats_q is not None:
            while True:
                try:
                    out.append(self._stats_q.get_nowait())
                except Exception:
                    break
        return out

    def stop(self, timeout: float = 5.0) -> None:
        if self._stop_ev is not None:
            self._stop_ev.set()
        deadline = time.monotonic() + timeout
        for p in self._procs:
            p.join(timeout=max(0.1, deadline - time.monotonic()))
        for p in self._procs:
            if p.is_alive():
                p.terminate()
        for r in self._rings:
            r.close(unlink=True)
        self._rings.clear()
        self._procs.clear()


class _KeyNotExtractable(Exception):
    """payload 不支持 key 拆分（DataFrame/bytes 载荷、字段缺失、解码失败）。

    9.2.9：key 拆分（字段名/callable 路由）仅支持 dict / str 载荷；
    DataFrame 明确不支持——多股票 DataFrame 请在发布端按 key 拆分
    （groupby 后逐组发布 dict/str 消息），见 README「消费」一节。
    """

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


def _peek_payload(frame_bytes: bytes):
    """ingestor 侧 key 拆分路由的解码（有代价，文档已注明）。

    返回真实 payload：dict（字段路由/callable 收到 dict 本身）或 str
    （消息内容本身即 key）。其它载荷抛 _KeyNotExtractable，由 route()
    显式告警并回退 topic 路由——9.2.9 前此处对非 dict 载荷一律返回 {}，
    key="None" 会把全部帧静默打到同一个 worker（DataFrame 场景必踩）。
    """
    try:
        p = frames.decode(frame_bytes).payload
    except Exception:
        raise _KeyNotExtractable("decode_failed") from None
    if isinstance(p, (dict, str)):
        return p
    raise _KeyNotExtractable(f"payload_type:{type(p).__name__}")
