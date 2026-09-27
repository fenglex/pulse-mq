"""服务端每订阅者缓冲：慢消费者策略（drop_old / conflate）。

三重上限：条数（max_messages）、字节（max_bytes）、存活时间（max_age_s，
0 = 不限）。策略在 REGISTER 时按订阅者声明（payload["buffer"]），未声明
的订阅者走原有直发路径（zmq SNDHWM 语义），行为完全不变。

调用方线程模型：
- enqueue / drain：同步数据面线程（热路径，临界区尽量小）。
- set_policy / clear：asyncio 控制面线程。
- snapshot：admin 线程。
统一用一把 threading.Lock 保护（订阅者数量级小，竞争可忽略）。

丢弃语义：
- evicted：drop_old 队列满/超字节，淘汰最旧（计入 DropStats）。
- expired：drain 时超过 max_age_s，视为过期丢弃（计入 DropStats）。
- conflated：conflate 策略同 topic 新帧覆盖旧帧（策略预期行为，
  不计入 DropStats，仅在 snapshot 中单列）。
"""
from __future__ import annotations

import gc
import threading
import time
from collections import deque
from dataclasses import dataclass

POLICY_DROP_OLD = "drop_old"
POLICY_CONFLATE = "conflate"
VALID_POLICIES = (POLICY_DROP_OLD, POLICY_CONFLATE)

# drain 时每个订阅者单轮最多发送帧数：避免一个深积压订阅者占满整个数据面线程
_DRAIN_BUDGET = 512

# GC 防护（9.2.1）：深积压时数十万 _Entry 对象挂在容器里，CPython 分代 GC
# 的 gen2 全堆扫描会造成数据面停顿（实测 60 万条积压时吞吐掉 5 倍）。
# 积压超阈值后周期性只跑 0/1 代回收，绕开 gen2 全堆扫描。
GC_BACKLOG_THRESHOLD = 100_000   # 缓冲总帧数阈值
GC_INTERVAL_S = 5.0              # 两次回收的最小间隔

# 信用窗口（9.2.1）：credit 是心跳（默认 1s）上报的队列剩余容量快照。若每次
# drain_all 都按满 credit 发送（poll ~10ms 一次 → 每窗口超发 ~100 倍），积压
# 会击穿消费端解码队列互相挤兑（drop-oldest），服务端缓冲被架空。故在窗口内
# 做总量守恒：本窗口累计发送 ≤ 窗口开始时的 credit 快照。
_CREDIT_WINDOW_S = 0.9


@dataclass(slots=True)
class _Entry:
    topic: str
    frame: bytes
    nbytes: int
    enqueued_at: float


class SubscriberBuffer:
    """单个订阅者的应用层缓冲（仅数据面线程经 BufferManager 访问）。"""

    def __init__(self, ident: bytes, policy: str,
                 max_messages: int, max_bytes: int, max_age_s: float,
                 on_drop) -> None:
        self.ident = ident
        self.policy = policy
        self.max_messages = max_messages
        self.max_bytes = max_bytes
        self.max_age_s = max_age_s
        self._on_drop = on_drop  # (topic, count, reason)
        self._q: deque[_Entry] = deque()          # drop_old
        self._latest: dict[str, _Entry] = {}      # conflate: topic -> 最新帧
        self._bytes = 0
        # 统计（snapshot 用）
        self.evicted = 0
        self.expired = 0
        self.conflated = 0
        self.sent = 0

    # -- 数据面线程 --

    def enqueue(self, topic: str, frame: bytes) -> None:
        now = time.monotonic()
        e = _Entry(topic, frame, len(frame), now)
        if self.policy == POLICY_CONFLATE:
            old = self._latest.get(topic)
            if old is not None:
                self._bytes -= old.nbytes
                self.conflated += 1
            self._latest[topic] = e
            self._bytes += e.nbytes
            # 字节超限：按入队时间淘汰最旧 topic 条目
            while self.max_bytes > 0 and self._bytes > self.max_bytes \
                    and len(self._latest) > 1:
                oldest_key = min(self._latest, key=lambda k: self._latest[k].enqueued_at)
                old = self._latest.pop(oldest_key)
                self._bytes -= old.nbytes
                self.evicted += 1
                self._on_drop(old.topic, 1, "evicted")
            return
        # drop_old
        if self.max_messages > 0 and len(self._q) >= self.max_messages:
            old = self._q.popleft()
            self._bytes -= old.nbytes
            self.evicted += 1
            self._on_drop(old.topic, 1, "evicted")
        self._q.append(e)
        self._bytes += e.nbytes
        while self.max_bytes > 0 and self._bytes > self.max_bytes and self._q:
            old = self._q.popleft()
            self._bytes -= old.nbytes
            self.evicted += 1
            self._on_drop(old.topic, 1, "evicted")

    def drain(self, send_fn, credit: int, budget: int = -1) -> int:
        """尽量把积压发给订阅者。send_fn(frame)->bool（False=EAGAIN，本轮停止）。

        credit == 0（消费端解码队列满）时跳过发送，帧留在缓冲。
        budget >= 0 时为本轮发送硬上限（信用窗口守恒用），否则不限。
        过期帧（超 max_age_s）直接丢弃并计数。返回本轮发送帧数。
        """
        now = time.monotonic()
        sent = 0
        if self.policy == POLICY_CONFLATE:
            for topic in list(self._latest):
                e = self._latest.get(topic)
                if e is None:
                    continue
                if self.max_age_s > 0 and now - e.enqueued_at > self.max_age_s:
                    del self._latest[topic]
                    self._bytes -= e.nbytes
                    self.expired += 1
                    self._on_drop(e.topic, 1, "expired")
                    continue
                if credit == 0 or budget == 0:
                    break
                if not send_fn(e.frame):
                    break
                del self._latest[topic]
                self._bytes -= e.nbytes
                self.sent += 1
                sent += 1
                if budget > 0:
                    budget -= 1
                if sent >= _DRAIN_BUDGET:
                    break
            return sent
        while self._q and sent < _DRAIN_BUDGET:
            e = self._q[0]
            if self.max_age_s > 0 and now - e.enqueued_at > self.max_age_s:
                self._q.popleft()
                self._bytes -= e.nbytes
                self.expired += 1
                self._on_drop(e.topic, 1, "expired")
                continue
            if credit == 0 or budget == 0:
                break
            if not send_fn(e.frame):
                break
            self._q.popleft()
            self._bytes -= e.nbytes
            self.sent += 1
            sent += 1
            if budget > 0:
                budget -= 1
        return sent

    def is_empty(self) -> bool:
        if self.policy == POLICY_CONFLATE:
            return not self._latest
        return not self._q

    def depth(self) -> int:
        if self.policy == POLICY_CONFLATE:
            return len(self._latest)
        return len(self._q)

    def oldest_age_ms(self, now: float) -> float:
        # drop_old 是严格 FIFO：队头即最旧，O(1)（此前 min() 全量扫描，
        # 60 万条积压时 admin 每 1s 轮询都要付 O(n)）。
        if self.policy == POLICY_CONFLATE:
            oldest = None
            for e in self._latest.values():
                if oldest is None or e.enqueued_at < oldest:
                    oldest = e.enqueued_at
        elif self._q:
            oldest = self._q[0].enqueued_at
        else:
            oldest = None
        return round((now - oldest) * 1000, 1) if oldest is not None else 0.0


class BufferManager:
    """全部订阅者缓冲的管理器（跨线程，Lock 保护）。"""

    def __init__(self, *, max_messages: int = 100_000,
                 max_bytes: int = 10 * 1024 * 1024,
                 max_age_s: float = 0.0,
                 on_drop=None) -> None:
        self.max_messages = max(1, int(max_messages))
        self.max_bytes = max(0, int(max_bytes))
        self.max_age_s = max(0.0, float(max_age_s))
        self._on_drop = on_drop or (lambda topic, n, reason: None)
        self._credit_fn = None      # ident -> credit（-1 未知/无限制）
        self._send_fn = None        # (ident, frame) -> bool，DONTWAIT 直发
        self._buffers: dict[bytes, SubscriberBuffer] = {}
        self._lock = threading.Lock()
        self._last_gc = 0.0
        # 信用窗口：ident -> [窗口开始 monotonic, credit 快照, 窗口内已发送]
        self._credit_windows: dict[bytes, list] = {}

    # -- 装配（Server 启动时）--

    def set_credit_fn(self, fn) -> None:
        self._credit_fn = fn

    def set_send_fn(self, fn) -> None:
        """注册数据面线程内的 DONTWAIT 发送函数：(ident, frame) -> bool。"""
        self._send_fn = fn

    # -- 数据面线程 --

    def maybe_gc(self) -> None:
        """深积压防护（9.2.1）：超阈值时周期性降代回收（0/1 代）。

        只在数据面线程调用（SyncDataThread._loop 每轮 poll 后）。阈值 0 可
        经 GC_BACKLOG_THRESHOLD 常量关闭。gen2 全堆扫描才是停顿元凶，collect(1)
        绕开它；启动期对象已在 Server.start() 里 gc.freeze() 进永久代。
        """
        total = 0
        for b in self._buffers.values():
            total += len(b._q) + len(b._latest)
        if total < GC_BACKLOG_THRESHOLD:
            return
        now = time.monotonic()
        if now - self._last_gc < GC_INTERVAL_S:
            return
        self._last_gc = now
        gc.collect(1)

    # -- 控制面线程 --

    def set_credit(self, ident: bytes, credit: int) -> None:
        """心跳到达（控制面线程）：以新信用快照开启新发送窗口。

        窗口语义：本窗口内数据面累计发送 ≤ credit（心跳时刻消费端队列剩余
        容量）。严格按心跳开窗，杜绝旧快照复用导致的额度双花。
        """
        self._credit_windows[ident] = [time.monotonic(), credit, 0]

    def set_policy(self, ident: bytes, policy: str,
                   overrides: dict | None = None) -> bool:
        """REGISTER 时设置订阅者缓冲策略。客户端只能请求更小的上限。"""
        if policy not in VALID_POLICIES:
            return False
        ov = overrides or {}
        max_messages = self._clamp(int(ov.get("max_messages", self.max_messages)),
                                   self.max_messages)
        max_bytes = self._clamp(int(ov.get("max_bytes", self.max_bytes)),
                                self.max_bytes)
        max_age_s = self._clamp(float(ov.get("max_age_s", self.max_age_s)),
                                self.max_age_s)
        with self._lock:
            self._buffers[ident] = SubscriberBuffer(
                ident, policy, max_messages, max_bytes, max_age_s,
                on_drop=self._on_drop,
            )
        return True

    @staticmethod
    def _clamp(requested: float, server_cap: float) -> float:
        """服务端上限为 0 表示不限；否则客户端只能请求不大于上限的值。"""
        if requested <= 0 or server_cap <= 0:
            return requested if requested > 0 else server_cap
        return min(requested, server_cap)

    def clear(self, ident: bytes) -> None:
        with self._lock:
            self._buffers.pop(ident, None)
            self._credit_windows.pop(ident, None)
            self._last_gc = 0.0

    # -- 数据面线程 --

    def get(self, ident: bytes) -> SubscriberBuffer | None:
        return self._buffers.get(ident)

    def enqueue(self, ident: bytes, topic: str, frame: bytes) -> None:
        buf = self._buffers.get(ident)
        if buf is not None:
            buf.enqueue(topic, frame)

    def has_backlog(self) -> bool:
        if not self._buffers:
            return False
        return any(not b.is_empty() for b in self._buffers.values())

    def drain_all(self) -> int:
        """数据面线程每轮 poll 后调用：向所有有积压的订阅者发送。

        信用流控（9.2.1 窗口守恒）：每个心跳窗口内累计发送不超过该窗口
        上报的 credit 快照（消费端队列剩余容量），避免窗口内反复按满额度
        超发击穿消费端解码队列。credit=-1（未知/未启用）时不限流。
        窗口内额度用尽则不再发送，帧留在服务端缓冲。
        """
        if not self._buffers or self._send_fn is None:
            return 0
        total = 0
        for ident, buf in list(self._buffers.items()):
            if buf.is_empty():
                continue
            credit = self._credit_fn(ident) if self._credit_fn else -1
            budget = -1
            w = self._credit_windows.get(ident)
            if credit >= 0 or w is not None:
                if w is None:
                    continue  # 尚未收到首个心跳信用，保守等待
                budget = w[1] - w[2]
                if budget <= 0:
                    continue  # 本窗口额度已用尽，帧留在服务端缓冲
            n = buf.drain(
                lambda frame, i=ident: self._send_fn(i, frame), -1, budget)
            if w is not None:
                w[2] += n
            total += n
        return total

    # -- admin 线程 --

    def snapshot(self) -> dict:
        now = time.monotonic()
        with self._lock:
            subs = {}
            for ident, b in self._buffers.items():
                subs[ident.decode("utf-8", "replace")] = {
                    "policy": b.policy,
                    "depth": b.depth(),
                    "bytes": b._bytes,
                    "oldest_age_ms": b.oldest_age_ms(now),
                    "evicted": b.evicted,
                    "expired": b.expired,
                    "conflated": b.conflated,
                    "sent": b.sent,
                    "max_messages": b.max_messages,
                    "max_bytes": b.max_bytes,
                    "max_age_s": b.max_age_s,
                }
            return {
                "defaults": {
                    "max_messages": self.max_messages,
                    "max_bytes": self.max_bytes,
                    "max_age_s": self.max_age_s,
                },
                "subscribers": subs,
            }
