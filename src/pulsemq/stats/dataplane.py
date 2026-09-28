"""服务端数据面线程健康统计（9.2.8）。

回答的问题：数据面线程是否接近饱和？慢消费者被信用窗口掐了多少？
发送侧背压（EAGAIN）是否频繁？

线程模型：数据面线程高频写（record_*，全部为单个整型属性自增或加法，
GIL 下原子），admin 线程低频读 snapshot()（持锁取走窗口累加值并重置）。
窗口 = 两次 snapshot 的实际间隔（随 SSE 轮询 ~1s），速率按间隔折算。
"""
from __future__ import annotations

import threading
import time


class DataPlaneStats:
    """数据面线程计数器：循环耗时 / 收发帧数 / EAGAIN / 信用拦截。

    - ``loops`` / ``loop_avg_ms`` / ``loop_max_ms``：本轮 poll 循环的次数与
      单次耗时（含 on_message 回调 + drain）——avg 升高说明转发路径变贵，
      max 飙升说明有停顿（GC / 深积压扫描 / 阻塞回调）。
    - ``msgs_in`` / ``msgs_out``：本轮收（生产者帧）/ 发（直发 + 缓冲 drain）
      帧数，速率为窗口折算值。
    - ``eagain``：DONTWAIT 发送命中对端队列满的次数（背压强度）。
    - ``credit_starved``：信用窗口拦截计数——直发路径因 credit=0 跳过的帧
      + 缓冲 drain 因窗口额度耗尽而保留积压的（订阅者×轮次）。
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        # 累计值（跨窗口单调，自启动以来）
        self._c_loops = 0
        self._c_in = 0
        self._c_out = 0
        self._c_eagain = 0
        self._c_starved = 0
        # 当前窗口累加（snapshot 时取走重置）
        self._w_loops = 0
        self._w_loop_ns = 0
        self._w_loop_max_ns = 0
        self._w_in = 0
        self._w_out = 0
        self._w_eagain = 0
        self._w_starved = 0
        # 上次快照（算窗口速率用）
        self._last_ts = time.monotonic()
        self._last_in = 0
        self._last_out = 0
        self._last_eagain = 0
        self._last_starved = 0
        self._last_loops = 0

    # ---- 数据面线程（高频，无锁）----

    def record_loop(self, duration_ns: int) -> None:
        self._w_loops += 1
        self._w_loop_ns += duration_ns
        if duration_ns > self._w_loop_max_ns:
            self._w_loop_max_ns = duration_ns

    def record_in(self, n: int = 1) -> None:
        self._w_in += n

    def record_out(self, n: int = 1) -> None:
        self._w_out += n

    def record_eagain(self) -> None:
        self._w_eagain += 1

    def record_starved(self, n: int = 1) -> None:
        self._w_starved += n

    # ---- admin 线程 ----

    def snapshot(self) -> dict:
        now = time.monotonic()
        with self._lock:
            elapsed = max(1e-9, now - self._last_ts)
            self._last_ts = now
            loops = self._w_loops
            loop_ns = self._w_loop_ns
            loop_max_ns = self._w_loop_max_ns
            w_in, w_out = self._w_in, self._w_out
            w_eagain, w_starved = self._w_eagain, self._w_starved
            self._c_loops += loops
            self._c_in += w_in
            self._c_out += w_out
            self._c_eagain += w_eagain
            self._c_starved += w_starved
            self._w_loops = 0
            self._w_loop_ns = 0
            self._w_loop_max_ns = 0
            self._w_in = 0
            self._w_out = 0
            self._w_eagain = 0
            self._w_starved = 0
            d_in = self._c_in - self._last_in
            d_out = self._c_out - self._last_out
            d_eagain = self._c_eagain - self._last_eagain
            d_starved = self._c_starved - self._last_starved
            d_loops = self._c_loops - self._last_loops
            self._last_in = self._c_in
            self._last_out = self._c_out
            self._last_eagain = self._c_eagain
            self._last_starved = self._c_starved
            self._last_loops = self._c_loops
            c = {
                "loops": self._c_loops, "msgs_in": self._c_in,
                "msgs_out": self._c_out, "eagain": self._c_eagain,
                "credit_starved": self._c_starved,
            }
        return {
            **c,
            "window_s": round(elapsed, 3),
            "window_loops": loops,
            "loop_avg_ms": round(loop_ns / loops / 1e6, 3) if loops else 0.0,
            "loop_max_ms": round(loop_max_ns / 1e6, 3),
            "in_per_s": round(d_in / elapsed, 1),
            "out_per_s": round(d_out / elapsed, 1),
            "loops_per_s": round(d_loops / elapsed, 1),
            "eagain_per_s": round(d_eagain / elapsed, 2),
            "starved_per_s": round(d_starved / elapsed, 2),
        }
