# PulseMQ v2 · Spec 3 监控扩展 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在 Spec 1+2 之上扩展监控：在线 Client 概览、端到端延迟分位（P50/P95/P99，1% 采样）、最近事件流（环形缓冲）、admin HTTP 独立线程、SQLite 异步批量落盘，数据路径零阻塞；web_ui 新增对应区块。

**Architecture:** 依赖方向（spec §2）：`stats/{traffic,storage,connections,latency}` 独立无业务依赖；`admin` 依赖 stats + routing/registry snapshot（经 Server 的 `snapshot_fn` 桥接）+ Spec 2 token；`Server` 在数据面 `_data_loop` 采延迟、在 `_dispatch_control`/心跳扫描/ZAP `on_auth` 发连接/断开/认证事件给 `ConnectionStats`。**沿用不动接口**：TrafficStats、AdminServer 路由骨架、web_ui 视觉风格、Spec 2 token 中间件。

**Tech Stack:** 沿用 pyzmq/msgspec/loguru/pytest；新增仅标准库（`threading`/`collections.deque`/`asyncio.Queue`）；无新第三方依赖。

## Global Constraints

（逐条抄自 Spec 3，所有 task 隐式遵守）

- **数据路径零阻塞**：`TrafficStats.record()` / `LatencyStats.record()` / `ConnectionStats.on_*` 单写者 + GIL，不引入互斥锁；数据线程不直接写 SQLite、不格式化日志字符串。
- **延迟采样默认 1%**（`latency_sample_rate`，可配全量 1.0）；未命中不调 `record`。延迟 = Server 收帧时 `time.time_ns() - msg.timestamp_ns`（帧已有 ts）。
- **事件流仅内存环形**（`deque(maxlen=event_ring_size)`，默认 200），溢出丢旧，不持久化。
- **SQLite 异步批量**：分钟归档经 `asyncio.Queue` + 独立 consumer 任务 `executemany`（`stats_archive_batch_size=50`），表结构不变。
- **admin HTTP 独立线程**（`admin_thread=true` 默认）：AdminServer 运行在独立线程的独立 asyncio loop，HTTP 请求不抢占 ZMQ 数据线程；通过 GIL 安全的只读快照读 Server 状态。
- **SSE 反压**：客户端队列 `maxsize=64`，满则取消该连接（沿用现有 `_sse_clients` 策略）。
- **复用 Spec 2 token**：新增路由（`/api/v1/clients`、`/api/v1/events`）自动继承 AdminServer 现有 token 中间件（`/healthz` 除外）。
- **跨平台 loop**：admin 线程用独立 loop（沿用全局 `WindowsSelectorEventLoopPolicy` 即可，纯 HTTP 不受 pyzmq 限制）；与数据线程隔离。
- 沿用测试：`test_stats.py`、Spec 1+2 e2e 继续通过。

---

## File Structure

| 路径 | 动作 | 职责 |
|------|------|------|
| `src/pulsemq/config.py` | 扩展 | ServerConfig 加 `[monitoring]` 字段 |
| `src/pulsemq/stats/latency.py` | 新增 | `LatencyStats`：采样 + 固定桶直方图 + P50/P95/P99 |
| `src/pulsemq/stats/connections.py` | 新增 | `ConnectionStats`/`ClientSnapshot`/`LifecycleEvent`：事件环 + 计数 + 在线快照 |
| `src/pulsemq/stats/storage.py` | 扩展 | `AsyncArchiveWriter`：queue + consumer 包裹 StatsStorage 批量写 |
| `src/pulsemq/transport/router.py` | 改 | `bind(..., on_auth=)` 透传给 AsyncZAPHandler |
| `src/pulsemq/admin/server.py` | 改 | `admin_thread` 独立线程模式 + `/api/v1/clients`、`/api/v1/events` 路由 + realtime 扩展（latency/online 计数） |
| `src/pulsemq/admin/web_ui.py` | 扩展 | 在线 Client 卡片 + 延迟分位图 + 事件流 + Client 详情弹窗 |
| `src/pulsemq/server.py` | 改 | 构建 LatencyStats/ConnectionStats/AsyncArchiveWriter；数据面采延迟；发连接/断开/认证事件；async 分钟归档；snapshot_fn 扩展；AdminServer 接入新依赖 |
| `tests/test_latency_stats.py` | 新增 | 采样命中/未命中、P50/P95/P99、全量 |
| `tests/test_connections_stats.py` | 新增 | 计数、事件环 maxlen 丢弃、recent_events 截断、online_clients |
| `tests/test_storage_async.py` | 新增 | 异步批量写入、不阻塞、重启可读 |
| `tests/test_admin_v3.py` | 新增 | /clients、/events、401、SSE 反压、独立线程不阻塞数据线程 |
| `tests/test_monitoring_perf.py` | 新增 | 高吞吐无锁路径基线 |

依赖方向约束：`stats/*` 不依赖 admin/server/transport；`admin` 经 `snapshot_fn` 只读快照，不直接 import server 内部。

---

## Task 1: config [monitoring] 扩展

**Files:**
- Modify: `src/pulsemq/config.py`
- Test: `tests/test_config.py`（追加）

**Interfaces:**
- Produces: ServerConfig 加 `sse_interval: float=1.0`、`latency_sample_rate: float=0.01`、`event_ring_size: int=200`、`stats_archive_batch_size: int=50`、`admin_thread: bool=True`、`ui_enabled: bool=True`、`retention_days: int=7`。`load_server_config` 读 `[monitoring]`。

- [ ] **Step 1: 失败测试**

```python
def test_monitoring_defaults():
    cfg = ServerConfig()
    assert cfg.sse_interval == 1.0
    assert cfg.latency_sample_rate == 0.01
    assert cfg.event_ring_size == 200
    assert cfg.stats_archive_batch_size == 50
    assert cfg.admin_thread is True
    assert cfg.ui_enabled is True
    assert cfg.retention_days == 7


def test_load_monitoring_block(tmp_path):
    p = tmp_path / "s.toml"
    p.write_text('[monitoring]\nlatency_sample_rate = 0.5\nevent_ring_size = 50\n'
                 'admin_thread = false\n', encoding="utf-8")
    cfg = load_server_config(str(p))
    assert cfg.latency_sample_rate == 0.5
    assert cfg.event_ring_size == 50
    assert cfg.admin_thread is False
```

- [ ] **Step 2: Run → FAIL** — `pytest tests/test_config.py -v` → AttributeError.

- [ ] **Step 3: 实现**

`ServerConfig` 追加字段（在 `admin_token_file` 之后）：
```python
    sse_interval: float = 1.0
    latency_sample_rate: float = 0.01
    event_ring_size: int = 200
    stats_archive_batch_size: int = 50
    admin_thread: bool = True
    ui_enabled: bool = True
    retention_days: int = 7
```
`load_server_config` 构造 cfg 时从 `m = data.get("monitoring", {})` 读：
```python
        sse_interval=float(m.get("sse_interval", ServerConfig.sse_interval)),
        latency_sample_rate=float(m.get("latency_sample_rate", ServerConfig.latency_sample_rate)),
        event_ring_size=int(m.get("event_ring_size", ServerConfig.event_ring_size)),
        stats_archive_batch_size=int(m.get("stats_archive_batch_size", ServerConfig.stats_archive_batch_size)),
        admin_thread=bool(m.get("admin_thread", ServerConfig.admin_thread)),
        ui_enabled=bool(m.get("ui_enabled", ServerConfig.ui_enabled)),
        retention_days=int(m.get("retention_days", ServerConfig.retention_days)),
```

- [ ] **Step 4: Run → PASS** — `pytest tests/test_config.py -v`.

- [ ] **Step 5: Commit**
```bash
git add src/pulsemq/config.py tests/test_config.py
git commit -m "feat(config): [monitoring] 块完整化（sse/latency/events/admin_thread 等）"
```

---

## Task 2: stats/latency.LatencyStats

**Files:**
- Create: `src/pulsemq/stats/latency.py`
- Test: `tests/test_latency_stats.py`

**Interfaces:**
- Produces: `LatencyStats(sample_rate=0.01)`：`should_sample()->bool`、`record(latency_ns:int)`、`percentiles()->{"p50_ms","p95_ms","p99_ms"}`、`snapshot()->dict`。

> 固定桶（ns）：`[0, 50_000, 100_000, 500_000, 1_000_000, 5_000_000, 10_000_000, 50_000_000, ∞]`（即 0.05/0.1/0.5/1/5/10/50 ms 边界）。单写者无锁。

- [ ] **Step 1: 失败测试**

```python
# tests/test_latency_stats.py
import pytest
from pulsemq.stats.latency import LatencyStats


def test_sample_rate_controls_sampling(monkeypatch):
    s = LatencyStats(sample_rate=0.0)
    assert s.should_sample() is False
    s2 = LatencyStats(sample_rate=1.0)
    assert s2.should_sample() is True


def test_record_and_percentiles_full_sampling():
    s = LatencyStats(sample_rate=1.0)
    # 100 个样本，线性分布 0..99ms（ns）
    for i in range(100):
        s.record(i * 1_000_000)  # i ms
    p = s.percentiles()
    assert p["p50_ms"] >= 0
    assert p["p95_ms"] >= p["p50_ms"]
    assert p["p99_ms"] >= p["p95_ms"]
    # p50 应在 50ms 附近（桶估算）
    assert 40 <= p["p50_ms"] <= 60


def test_no_record_when_not_sampled():
    s = LatencyStats(sample_rate=0.0)
    for i in range(1000):
        if s.should_sample():
            s.record(i * 1_000_000)
    assert s.percentiles()["p99_ms"] == 0.0  # 无样本


def test_snapshot_shape():
    s = LatencyStats(sample_rate=1.0)
    s.record(100_000)
    snap = s.snapshot()
    assert set(snap) == {"p50_ms", "p95_ms", "p99_ms", "count"}
    assert snap["count"] == 1
```

- [ ] **Step 2: Run → FAIL** — ModuleNotFoundError.

- [ ] **Step 3: 实现**

```python
# src/pulsemq/stats/latency.py
"""端到端延迟统计：采样 + 固定桶直方图 + P50/P95/P99。单写者无锁。"""
from __future__ import annotations

import bisect
import random

# 桶上界（ns）：0.05 / 0.1 / 0.5 / 1 / 5 / 10 / 50 ms + ∞
_BUCKET_BOUNDS_NS = [50_000, 100_000, 500_000, 1_000_000,
                     5_000_000, 10_000_000, 50_000_000]
# 每个桶的代表值（ns），用于分位估算
_BUCKET_REPR_NS = [25_000, 75_000, 300_000, 750_000, 3_000_000,
                   7_500_000, 30_000_000, 60_000_000]


class LatencyStats:
    def __init__(self, sample_rate: float = 0.01) -> None:
        self._rate = max(0.0, min(1.0, sample_rate))
        self._counts = [0] * (len(_BUCKET_BOUNDS_NS) + 1)  # 末桶=∞
        self._total = 0

    def should_sample(self) -> bool:
        if self._rate >= 1.0:
            return True
        if self._rate <= 0.0:
            return False
        return random.random() < self._rate

    def record(self, latency_ns: int) -> None:
        idx = bisect.bisect_left(_BUCKET_BOUNDS_NS, latency_ns)
        # bisect_left: latency < bound[0] -> 0; latency >= last bound -> len(bounds)
        if latency_ns >= _BUCKET_BOUNDS_NS[-1]:
            idx = len(_BUCKET_BOUNDS_NS)
        self._counts[idx] += 1
        self._total += 1

    def _percentile_ms(self, pct: float) -> float:
        if self._total == 0:
            return 0.0
        target = pct * self._total
        running = 0
        for i, c in enumerate(self._counts):
            running += c
            if running >= target:
                return _BUCKET_REPR_NS[i] / 1_000_000.0
        return _BUCKET_REPR_NS[-1] / 1_000_000.0

    def percentiles(self) -> dict:
        return {
            "p50_ms": self._percentile_ms(0.50),
            "p95_ms": self._percentile_ms(0.95),
            "p99_ms": self._percentile_ms(0.99),
        }

    def snapshot(self) -> dict:
        p = self.percentiles()
        p["count"] = self._total
        return p
```

- [ ] **Step 4: Run → PASS** — `pytest tests/test_latency_stats.py -v`.

- [ ] **Step 5: Commit**
```bash
git add src/pulsemq/stats/latency.py tests/test_latency_stats.py
git commit -m "feat(stats): LatencyStats 采样+固定桶直方图+P50/P95/P99"
```

---

## Task 3: stats/connections.ConnectionStats

**Files:**
- Create: `src/pulsemq/stats/connections.py`
- Test: `tests/test_connections_stats.py`

**Interfaces:**
- Consumes: `OnlineRegistry.snapshot()` 形状（经注入的 callable）。
- Produces: `ClientSnapshot`、`LifecycleEvent`、`ConnectionStats(registry_snapshot_fn, ring_size=200)`：`on_connect/on_disconnect/on_auth`、`online_clients()->list[ClientSnapshot]`、`recent_events(limit=50)`、`counters()`。

> `on_*` 只做 deque.append + 计数累加，无锁。在线列表经 `registry_snapshot_fn` 现取。

- [ ] **Step 1: 失败测试**

```python
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
```

- [ ] **Step 2: Run → FAIL** — ModuleNotFoundError.

- [ ] **Step 3: 实现**

```python
# src/pulsemq/stats/connections.py
"""在线 Client 与生命周期事件统计。事件环有界，on_* 无锁（单写者+GIL）。"""
from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass
from typing import Callable


@dataclass
class ClientSnapshot:
    client_id: str
    username: str
    role: str            # producer / consumer / both
    endpoint: str
    topics: list[str]
    connected_at: float
    duration_seconds: float


@dataclass
class LifecycleEvent:
    ts: float
    level: str           # INFO / WARNING / ERROR
    type: str            # AUTH / CLIENT
    message: str


def _role_of(roles: list[str]) -> str:
    has_pub = any("pub" in r for r in roles)
    has_sub = any("sub" in r for r in roles)
    if has_pub and has_sub:
        return "both"
    if has_pub:
        return "producer"
    if has_sub:
        return "consumer"
    return "consumer"


class ConnectionStats:
    def __init__(self, registry_snapshot_fn: Callable[[], dict],
                 ring_size: int = 200) -> None:
        self._reg_snap = registry_snapshot_fn
        self._events: deque[LifecycleEvent] = deque(maxlen=ring_size)

    # ---- 事件埋点（Server 数据线程调用）----
    def on_connect(self, client_id: str, username: str, endpoint: str, role: str) -> None:
        self._events.append(LifecycleEvent(
            ts=time.time(), level="INFO", type="CLIENT",
            message=f"{username} 上线 role={role} endpoint={endpoint}"))

    def on_disconnect(self, client_id: str, reason: str) -> None:
        self._events.append(LifecycleEvent(
            ts=time.time(), level="INFO", type="CLIENT",
            message=f"{client_id} 离线 reason={reason}"))

    def on_auth(self, username: str, endpoint: str, success: bool,
                reason: str | None) -> None:
        level = "INFO" if success else "WARNING"
        msg = (f"{username} 认证成功" if success
               else f"{username} 认证失败: {reason or 'unknown'}")
        self._events.append(LifecycleEvent(ts=time.time(), level=level, type="AUTH", message=msg))

    # ---- 读取（admin 线程调用，只读快照）----
    def online_clients(self) -> list[ClientSnapshot]:
        data = self._reg_snap() or {}
        now = time.time()
        out: list[ClientSnapshot] = []
        for c in data.get("clients", []):
            connected_at = float(c.get("connected_at", 0.0))
            out.append(ClientSnapshot(
                client_id=str(c.get("client_id", "")),
                username=str(c.get("username", "")),
                role=_role_of(list(c.get("roles", []))),
                endpoint=str(c.get("endpoint", "")),
                topics=list(c.get("topics", [])),
                connected_at=connected_at,
                duration_seconds=max(0.0, now - connected_at) if connected_at else 0.0,
            ))
        return out

    def recent_events(self, limit: int = 50) -> list[LifecycleEvent]:
        if limit <= 0:
            return []
        items = list(self._events)
        return items[-limit:]

    def counters(self) -> dict:
        clients = self.online_clients()
        producers = sum(1 for c in clients if c.role in ("producer", "both"))
        consumers = sum(1 for c in clients if c.role in ("consumer", "both"))
        total_subs = sum(len(c.topics) for c in clients)
        return {
            "online_users": len(clients),
            "online_producers": producers,
            "online_consumers": consumers,
            "total_subscriptions": total_subs,
        }
```

- [ ] **Step 4: Run → PASS** — `pytest tests/test_connections_stats.py -v`.

- [ ] **Step 5: Commit**
```bash
git add src/pulsemq/stats/connections.py tests/test_connections_stats.py
git commit -m "feat(stats): ConnectionStats 在线快照+事件环+计数"
```

---

## Task 4: StatsStorage 异步批量落盘（AsyncArchiveWriter）

**Files:**
- Modify: `src/pulsemq/stats/storage.py`（追加 `AsyncArchiveWriter`）
- Test: `tests/test_storage_async.py`

**Interfaces:**
- Consumes: `StatsStorage.save_minutes_batch(dict[str, MinuteSlot])`。
- Produces: `AsyncArchiveWriter(storage, batch_size=50)`：`async start()`、`async enqueue(archived: dict)`、`async stop()`（drain 剩余）。内部 `asyncio.Queue` + consumer 任务批量合并写。

> SQLite 写仅在 consumer 任务（不在数据 recv 循环）。表结构不变。

- [ ] **Step 1: 失败测试**

```python
# tests/test_storage_async.py
import asyncio
import pytest
from pulsemq.stats.storage import StatsStorage, AsyncArchiveWriter
from pulsemq.stats.traffic import MinuteSlot


async def test_async_writer_batches_and_persists(tmp_path):
    db = f"sqlite://{tmp_path / 's.sqlite'}"
    storage = StatsStorage(db)
    storage.connect()
    writer = AsyncArchiveWriter(storage, batch_size=2)
    await writer.start()
    try:
        await writer.enqueue({"a": MinuteSlot(timestamp=1000, msg_count=1, record_count=1, bytes_total=10)})
        await writer.enqueue({"b": MinuteSlot(timestamp=2000, msg_count=2, record_count=2, bytes_total=20)})
        await asyncio.sleep(0.2)  # consumer 写完
        hist = storage.load_history("a", 0)
        assert any(h["timestamp"] == 1000 for h in hist)
        hist_b = storage.load_history("b", 0)
        assert any(h["timestamp"] == 2000 for h in hist_b)
    finally:
        await writer.stop()
        storage.close()


async def test_async_writer_drains_on_stop(tmp_path):
    db = f"sqlite://{tmp_path / 's2.sqlite'}"
    storage = StatsStorage(db); storage.connect()
    writer = AsyncArchiveWriter(storage, batch_size=100)  # 大 batch，不自动 flush
    await writer.start()
    await writer.enqueue({"c": MinuteSlot(timestamp=3000, msg_count=1, record_count=1, bytes_total=5)})
    await writer.stop()  # stop 时 drain
    hist = storage.load_history("c", 0)
    assert any(h["timestamp"] == 3000 for h in hist)
    storage.close()
```

- [ ] **Step 2: Run → FAIL** — ImportError AsyncArchiveWriter.

- [ ] **Step 3: 实现**（追加到 `src/pulsemq/stats/storage.py`）

```python
class AsyncArchiveWriter:
    """分钟归档异步批量写：enqueue 进 queue，consumer 任务批量 save_minutes_batch。

    SQLite 写仅在 consumer 任务，数据接收循环不阻塞。
    """
    def __init__(self, storage: "StatsStorage", batch_size: int = 50) -> None:
        self._storage = storage
        self._batch_size = batch_size
        self._queue: asyncio.Queue | None = None
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        self._queue = asyncio.Queue()
        self._task = asyncio.create_task(self._consume())

    async def enqueue(self, archived: dict) -> None:
        if self._queue is None:
            return
        await self._queue.put(archived)

    async def _consume(self) -> None:
        assert self._queue is not None
        while True:
            try:
                merged: dict = {}
                # 阻塞取第一个，再批量取最多 batch_size-1 个
                first = await self._queue.get()
                merged.update(first)
                for _ in range(self._batch_size - 1):
                    try:
                        more = self._queue.get_nowait()
                        merged.update(more)
                    except asyncio.QueueEmpty:
                        break
                if merged:
                    self._storage.save_minutes_batch(merged)
                self._queue.task_done() if False else None
            except asyncio.CancelledError:
                # drain 剩余
                self._drain()
                break
            except Exception:
                pass  # 单批失败不杀 consumer

    def _drain(self) -> None:
        if self._queue is None:
            return
        merged: dict = {}
        while True:
            try:
                merged.update(self._queue.get_nowait())
            except asyncio.QueueEmpty:
                break
        if merged:
            try:
                self._storage.save_minutes_batch(merged)
            except Exception:
                pass

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
        self._task = None
        self._queue = None
```

- [ ] **Step 4: Run → PASS** — `pytest tests/test_storage_async.py -v`（2 用例）。再 `pytest tests/test_stats.py -v` 确保 storage 单测不破。

- [ ] **Step 5: Commit**
```bash
git add src/pulsemq/stats/storage.py tests/test_storage_async.py
git commit -m "feat(stats): AsyncArchiveWriter 队列+consumer 批量落盘，不阻塞数据循环"
```

---

## Task 5: Transport.bind 透传 on_auth

**Files:**
- Modify: `src/pulsemq/transport/router.py`
- Test: `tests/test_transport_router.py`（追加）

**Interfaces:**
- Produces: `Transport.bind(endpoint, role, *, auth=None, on_auth=None)` —— `on_auth` 传给 `AsyncZAPHandler`。ZAP 是 ctx 单例，仅首次 auth bind 创建 handler，故 `on_auth` 必须在首次 auth bind 时提供（Server 数据面 bind 在前，传 on_auth；控制面 bind 不再传，复用同一 ZAP）。

> ⚠️ 顺序约束：Server 必须在数据面 bind 时传 on_auth（首个 auth bind），否则控制面 bind 时 ZAP 已启动，on_auth 丢失。Server 接线（Task 8）保证此顺序。

- [ ] **Step 1: 失败测试**

```python
async def test_bind_forwards_on_auth(ctx):
    from pulsemq.transport.router import Transport, PlainAuthDict
    import socket as _sock
    def _fp():
        s = _sock.socket(); s.bind(("127.0.0.1", 0)); p = s.getsockname()[1]; s.close(); return p
    dp = _fp()
    seen = []
    async def on_auth(username, address, ok):
        seen.append((username, ok))
    server = Transport(ctx=ctx)
    await server.bind(f"tcp://127.0.0.1:{dp}", "server_ingress",
                      auth=PlainAuthDict({"alice": "pw"}), on_auth=on_auth)
    # 触发一次 ZAP：client 连 + 错密码
    client = Transport(ctx=ctx)
    await client.connect(f"tcp://127.0.0.1:{dp}", "consumer",
                         credentials=("alice", "WRONG"))
    await asyncio.sleep(0.3)
    assert any(u == "alice" for u, _ in seen)  # on_auth 被调用
    await client.close(); await server.close()
```

- [ ] **Step 2: Run → FAIL** — `bind() got unexpected keyword 'on_auth'`.

- [ ] **Step 3: 实现**

`Transport.bind` 签名加 `on_auth`，且在创建 ZAP 时传入：
```python
    async def bind(self, endpoint: str, role: str,
                   *, auth: PlainAuthDict | None = None,
                   on_auth: "AuthCallback | None" = None) -> None:
        ...
        if auth is not None:
            sock.plain_server = True
            if not self._zap_started:
                zap = AsyncZAPHandler(self._ctx, auth, on_auth=on_auth)
                await zap.start()
                self._zaps.append(zap)
                self._zap_started = True
        sock.bind(endpoint)
        ...
```
（`AsyncZAPHandler.__init__` 已接受 `on_auth`，无需改。）

- [ ] **Step 4: Run → PASS** — `pytest tests/test_transport_router.py -v`。

- [ ] **Step 5: Commit**
```bash
git add src/pulsemq/transport/router.py tests/test_transport_router.py
git commit -m "feat(transport): bind 透传 on_auth 回调（认证事件源）"
```

---

## Task 6: AdminServer 独立线程 + 新路由 + realtime 扩展

**Files:**
- Modify: `src/pulsemq/admin/server.py`
- Test: `tests/test_admin_v3.py`

**Interfaces:**
- Consumes: Spec 2 `TokenAuth`、`ConnectionStats`/`LatencyStats`（经新参数注入）。
- Produces: AdminServer 加 `admin_thread: bool=True`、`connection_stats`、`latency_stats` 参数；`start()`/`stop()` 在 admin_thread 模式下管理独立线程+loop；新增 `/api/v1/clients`、`/api/v1/events` 路由；`_realtime_snapshot` 扩展 latency + online 计数。

> 跨线程：`snapshot_fn`、`connection_stats`、`latency_stats` 都是只读快照（GIL 安全），admin 线程读取、数据线程写入。

- [ ] **Step 1: 失败测试**

```python
# tests/test_admin_v3.py
import asyncio, socket as _sock, pytest
from pulsemq.server import Server
from pulsemq.stats.connections import ConnectionStats
from pulsemq.stats.latency import LatencyStats


def _port():
    s = _sock.socket(); s.bind(("127.0.0.1", 0)); p = s.getsockname()[1]; s.close(); return p


async def _get(port, path, token=None, timeout=3.0):
    r, w = await asyncio.wait_for(asyncio.open_connection("127.0.0.1", port), timeout=timeout)
    h = f"Authorization: Bearer {token}\r\n" if token else ""
    w.write(f"GET {path} HTTP/1.1\r\nHost: x\r\n{h}Connection: close\r\n\r\n".encode())
    await w.drain()
    data = await asyncio.wait_for(r.read(), timeout=timeout)
    w.close(); return data.decode(errors="replace")


async def test_admin_clients_and_events_routes_require_token():
    dp, cp, ap = _port(), _port(), _port()
    srv = Server(data_endpoint=f"tcp://127.0.0.1:{dp}", control_endpoint=f"tcp://127.0.0.1:{cp}",
                 admin_endpoint=f"127.0.0.1:{ap}", credentials={"a": "b"}, admin_token="T")
    await srv.start()
    try:
        await asyncio.sleep(0.4)
        assert "401" in await _get(ap, "/api/v1/clients")
        assert "401" in await _get(ap, "/api/v1/events")
        assert "200" in await _get(ap, "/api/v1/clients", token="T")
        assert "200" in await _get(ap, "/api/v1/events", token="T")
    finally:
        await srv.stop()


async def test_realtime_has_latency_and_counters():
    dp, cp, ap = _port(), _port(), _port()
    srv = Server(data_endpoint=f"tcp://127.0.0.1:{dp}", control_endpoint=f"tcp://127.0.0.1:{cp}",
                 admin_endpoint=f"127.0.0.1:{ap}", credentials={"a": "b"}, admin_token="T")
    await srv.start()
    try:
        await asyncio.sleep(0.3)
        resp = await _get(ap, "/api/v1/stats/realtime", token="T")
        assert "latency_p50_ms" in resp and "online_users" in resp
    finally:
        await srv.stop()


async def test_admin_runs_on_independent_thread():
    # 数据线程（Server loop）应不被 HTTP 慢请求阻塞：发一个请求的同时数据面仍可收发。
    dp, cp, ap = _port(), _port(), _port()
    srv = Server(data_endpoint=f"tcp://127.0.0.1:{dp}", control_endpoint=f"tcp://127.0.0.1:{cp}",
                 admin_endpoint=f"127.0.0.1:{ap}", credentials={"p": "p", "c": "c"}, admin_token="T")
    await srv.start()
    try:
        from pulsemq.client import ConsumerClient, ProducerClient
        cons = ConsumerClient(f"tcp://127.0.0.1:{dp}", f"tcp://127.0.0.1:{cp}", "c", "c")
        prod = ProducerClient(f"tcp://127.0.0.1:{dp}", f"tcp://127.0.0.1:{cp}", "p", "p")
        await cons.start(); await prod.start()
        got = []
        await cons.subscribe("t.*", lambda m: got.append(m.payload))
        await asyncio.sleep(0.3)
        # 并发：持续 HTTP 请求 + 发布，数据面应正常收
        async def _poll():
            for _ in range(5):
                await _get(ap, "/api/v1/stats/realtime", token="T", timeout=2.0)
                await asyncio.sleep(0.05)
        await asyncio.gather(_poll(), prod.publish("t.x", {"k": 1}))
        await asyncio.sleep(0.5)
        assert got == [{"k": 1}]
        await cons.stop(); await prod.stop()
    finally:
        await srv.stop()
```

> 这三个测试依赖 Server 接入新依赖（Task 8）。**Task 6 Step 1 先只跑可直接单测的部分**（route 存在性/realtime 字段需 Server 接线）。实际：Task 6 实现 AdminServer 改造，Task 8 接线后这三个测试转 GREEN。为避免阻塞，Task 6 可先加一个纯 AdminServer 单测（构造 AdminServer + connection_stats/latency_stats，验证 `/api/v1/clients`、`/api/v1/events`、realtime 字段），再在 Task 8 解锁集成测试。

- [ ] **Step 2: Run → FAIL** — AdminServer 无 connection_stats/latency_stats 参数与新路由。

- [ ] **Step 3: 实现**

`src/pulsemq/admin/server.py`：

1. 顶部 `import threading`；`__init__` 加参数：
```python
    def __init__(self, bind="0.0.0.0:9090", traffic_stats=None, topic_buffers=None,
                 stats_storage=None, snapshot_fn=None, start_time=None,
                 token_auth=None, *, connection_stats=None, latency_stats=None,
                 admin_thread: bool = True):
```
   存 `self._connections = connection_stats`、`self._latency = latency_stats`、`self._admin_thread = admin_thread`；新增 `self._thread=None`、`self._loop=None`、`self._thread_started=threading.Event()`。

2. 独立线程模式（`start()`/`stop()` 改为兼容两种模式）：
```python
    async def start(self) -> None:
        if self._admin_thread:
            self._thread = threading.Thread(target=self._run_thread, daemon=True, name="pulsemq-admin")
            self._thread.start()
            self._thread_started.wait(timeout=5.0)
        else:
            await self._serve()  # 原在 caller loop 内运行

    def _run_thread(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._loop = loop
        try:
            loop.run_until_complete(self._serve())
        finally:
            loop.close()

    async def _serve(self) -> None:
        self._server = await asyncio.start_server(self._handle_request, self._host, self._port)
        self._sse_task = asyncio.create_task(self._sse_broadcast_loop())
        self._thread_started.set()
        async with self._server:
            await self._server.serve_forever()

    async def stop(self) -> None:
        if self._admin_thread and self._loop is not None:
            fut = asyncio.run_coroutine_threadsafe(self._stop_serve(), self._loop)
            try:
                fut.result(timeout=5.0)
            except Exception:
                pass
            if self._thread:
                self._thread.join(timeout=5.0)
        else:
            await self._stop_serve()

    async def _stop_serve(self) -> None:
        # 取消 SSE 客户端 + broadcast + 关 server（原 stop 逻辑搬此）
        for _qid, (_q, task) in list(self._sse_clients.items()):
            task.cancel()
        self._sse_clients.clear()
        if self._sse_task:
            self._sse_task.cancel()
            try: await self._sse_task
            except (asyncio.CancelledError, Exception): pass
        if self._server:
            self._server.close()
            await self._server.wait_closed()
```

3. `_route` 加两条路由（token 中间件已在 `_handle_request` 前置，自动覆盖）：
```python
        if method == "GET" and path == "/api/v1/clients":
            await self._respond_json(writer, 200, self._clients_snapshot())
            return
        if method == "GET" and path == "/api/v1/events":
            limit = 50
            try: limit = int(query.get("limit", ["50"])[0])
            except (ValueError, IndexError): pass
            await self._respond_json(writer, 200, self._events_snapshot(limit))
            return
```
4. `_realtime_snapshot` 扩展（在 `snap.update(self._snapshot_fn())` 之后）：
```python
        if self._latency is not None:
            snap.update(self._latency.percentiles())  # latency_p50_ms/p95/p99
        if self._connections is not None:
            snap.update(self._connections.counters())  # online_users/producers/consumers/total_subscriptions
```
5. 新数据方法：
```python
    def _clients_snapshot(self) -> dict:
        if self._connections is None:
            return {"clients": []}
        import time as _t
        clients = []
        for c in self._connections.online_clients():
            clients.append({
                "client_id": c.client_id, "username": c.username, "role": c.role,
                "endpoint": c.endpoint, "topics": list(c.topics),
                "connected_at_iso": _iso(c.connected_at),
                "duration_seconds": round(c.duration_seconds, 1),
            })
        return {"clients": clients}

    def _events_snapshot(self, limit: int) -> dict:
        if self._connections is None:
            return {"events": []}
        events = []
        for e in self._connections.recent_events(limit):
            events.append({"ts_iso": _iso(e.ts), "level": e.level, "type": e.type, "message": e.message})
        return {"events": events}
```
   加模块级 helper：
```python
    def _iso(ts: float) -> str:
        import time as _t
        return _t.strftime("%Y-%m-%dT%H:%M:%SZ", _t.gmtime(ts)) if ts else ""
```
   （`_iso` 定义为模块级函数；上面 `_clients_snapshot`/`_events_snapshot` 引用它。）

- [ ] **Step 4: Run → PASS（Task 6 纯 AdminServer 单测）**

加一个纯单测（不经 Server）验证路由 + realtime：
```python
async def test_admin_server_routes_directly():
    from pulsemq.admin.server import AdminServer
    from pulsemq.admin.auth import TokenAuth
    cs = ConnectionStats(lambda: {"clients": []}, ring_size=10)
    cs.on_connect("c1", "alice", "x", "consumer")
    ls = LatencyStats(sample_rate=1.0); ls.record(100_000)
    adm = AdminServer(bind="127.0.0.1:0", token_auth=TokenAuth("T"),
                      connection_stats=cs, latency_stats=ls, admin_thread=False)
    # 0 端口由 OS 分配；需先 bind 取实际端口——AdminServer 用 host:port，0 端口可行
    await adm.start()
    try:
        port = adm._server.sockets[0].getsockname()[1]
        assert "200" in await _get(port, "/api/v1/clients", token="T")
        assert "alice" in await _get(port, "/api/v1/clients", token="T")
        assert "200" in await _get(port, "/api/v1/events", token="T")
        rt = await _get(port, "/api/v1/stats/realtime", token="T")
        assert "latency_p50_ms" in rt and "online_users" in rt
    finally:
        await adm.stop()
```
Run: `pytest tests/test_admin_v3.py::test_admin_server_routes_directly -v` → PASS。

- [ ] **Step 5: Commit**
```bash
git add src/pulsemq/admin/server.py tests/test_admin_v3.py
git commit -m "feat(admin): 独立线程模式 + /clients /events 路由 + realtime 扩展(latency/计数)"
```

---

## Task 7: web_ui 新区块（在线 Client / 延迟 / 事件流 / 详情弹窗）

**Files:**
- Modify: `src/pulsemq/admin/web_ui.py`
- Test: 人工验证 + `test_admin_v3.py` 数据接口覆盖

**Interfaces:**
- Produces: INDEX_HTML 新增：在线 Client 概览卡片行（4 张）、延迟分位 ECharts 图、最近事件流列表、Client 详情弹窗；JS state 扩展 `online_clients/latency/events`，SSE 消费时更新这些区块。

> UI 单文件 HTML，主要人工验证；数据接口由 test_admin_v3 覆盖。沿用深色玻璃态风格 + token 携带（Spec 2 已加）。

- [ ] **Step 1: 改 INDEX_HTML**

- `#overview-cards` 下新增一行卡片：在线用户（`#v-online-users`）、在线生产者（`#v-online-producers`）、在线消费者（`#v-online-consumers`）、总订阅（`#v-total-subs`）。点击「在线用户」卡片打开详情弹窗（`#client-modal`，从 `/api/v1/clients` 取列表）。
- 在流量趋势图之后新增 `.chart-section`：延迟分位（`#latency-chart`，ECharts 柱状，P50/P95/P99）。
- 再新增 `.chart-section`：最近事件流（`#event-stream`，列表，最新在上，自动滚动）。
- JS：`state` 加 `onlineUsers/onlineProducers/onlineConsumers/totalSubs/latency/events`；SSE `onmessage` 解析新字段后调 `renderOverview()`/`renderLatency()`/`renderEvents()`；`renderLatency()` 用 ECharts 柱状（lazy init `#latency-chart`）；详情弹窗用 `fetch(_withToken('/api/v1/clients'), {headers:_authHeaders()})`。

- [ ] **Step 2: 验证不破坏**

Run: `pytest tests/test_admin_v3.py tests/test_admin_token.py -v` → 数据接口测试仍 GREEN。
Sanity: `uv run python -c "from pulsemq.admin.web_ui import INDEX_HTML; assert all(s in INDEX_HTML for s in ['v-online-users','latency-chart','event-stream','client-modal']); print('web_ui sections present')"`。

- [ ] **Step 3: Commit**
```bash
git add src/pulsemq/admin/web_ui.py
git commit -m "feat(web_ui): 在线 Client/延迟分位/事件流/详情弹窗区块"
```

---

## Task 8: Server 接线（latency/connections/async 归档/事件/on_auth）

**Files:**
- Modify: `src/pulsemq/server.py`
- Test: `tests/test_admin_v3.py`（解锁集成测试）+ `tests/test_monitoring_perf.py`

**Interfaces:**
- Consumes: LatencyStats、ConnectionStats、AsyncArchiveWriter、`Transport.bind(on_auth=)`。
- Produces: Server 构建 `_latency`、`_connections`、`_archive_writer`；`_data_loop` 采延迟；`_dispatch_control`（REGISTER/DISCONNECT）+ `_heartbeat_sweep_loop`（timeout）发连接/断开事件；`on_auth` 异步回调发认证事件；`_minute_roll_loop` 改 enqueue；`start()`/`stop()` 管理 archive_writer；AdminServer 接入 connection_stats/latency_stats/admin_thread；`bind` 数据面传 on_auth（首次 auth bind）。

- [ ] **Step 1: 失败测试**

```python
# tests/test_monitoring_perf.py
import asyncio, socket as _sock
import pytest
from pulsemq.server import Server
from pulsemq.client import ProducerClient, ConsumerClient


def _port():
    s = _sock.socket(); s.bind(("127.0.0.1", 0)); p = s.getsockname()[1]; s.close(); return p


async def test_latency_recorded_on_data_plane():
    dp, cp, ap = _port(), _port(), _port()
    srv = Server(data_endpoint=f"tcp://127.0.0.1:{dp}", control_endpoint=f"tcp://127.0.0.1:{cp}",
                 admin_endpoint=f"127.0.0.1:{ap}", credentials={"p": "p", "c": "c"},
                 latency_sample_rate=1.0, admin_token="T")
    await srv.start()
    try:
        prod = ProducerClient(f"tcp://127.0.0.1:{dp}", f"tcp://127.0.0.1:{cp}", "p", "p")
        cons = ConsumerClient(f"tcp://127.0.0.1:{dp}", f"tcp://127.0.0.1:{cp}", "c", "c")
        await cons.start(); await prod.start()
        await cons.subscribe("t.*", lambda m: None)
        await asyncio.sleep(0.3)
        for _ in range(20):
            await prod.publish("t.x", {"k": 1})
        await asyncio.sleep(0.5)
        p = srv._latency.percentiles()
        assert p["count"] > 0          # 延迟被采
        await cons.stop(); await prod.stop()
    finally:
        await srv.stop()


async def test_connection_events_emitted():
    dp, cp, ap = _port(), _port(), _port()
    srv = Server(data_endpoint=f"tcp://127.0.0.1:{dp}", control_endpoint=f"tcp://127.0.0.1:{cp}",
                 admin_endpoint=f"127.0.0.1:{ap}", credentials={"c": "c"}, admin_token="T")
    await srv.start()
    try:
        cons = ConsumerClient(f"tcp://127.0.0.1:{dp}", f"tcp://127.0.0.1:{cp}", "c", "c")
        await cons.start()
        await asyncio.sleep(0.3)
        evts = srv._connections.recent_events(50)
        # 至少有 connect 事件（REGISTER）或 auth 事件
        assert any(e.type == "CLIENT" for e in evts) or any(e.type == "AUTH" for e in evts)
        await cons.stop()
    finally:
        await srv.stop()
```

- [ ] **Step 2: Run → FAIL** — Server 无 latency_sample_rate 参数 / _latency / _connections。

- [ ] **Step 3: 实现**

`src/pulsemq/server.py`：

1. import：
```python
from pulsemq.stats.latency import LatencyStats
from pulsemq.stats.connections import ConnectionStats
from pulsemq.stats.storage import AsyncArchiveWriter
```
2. `__init__` 签名加 `latency_sample_rate: float | None = None`。构造体内（在 `self._auth` 后）：
```python
        self._latency = LatencyStats(
            sample_rate=self._cfg.latency_sample_rate if latency_sample_rate is None else latency_sample_rate)
        self._connections = ConnectionStats(
            registry_snapshot_fn=self._registry.snapshot, ring_size=self._cfg.event_ring_size)
        self._archive_writer = AsyncArchiveWriter(self._storage, batch_size=self._cfg.stats_archive_batch_size)
        # ZAP on_auth 回调（认证事件）
        self._auth_on_auth = self._on_auth_event
```
3. `start()`：
   - 数据面 bind 传 `on_auth=self._auth_on_auth`（首次 auth bind）：
     `await self._transport.bind(self._data_endpoint, "server_ingress", auth=self._auth, on_auth=self._auth_on_auth)`
   - 控制面 bind 不传 on_auth（复用 ZAP）：不变。
   - `await self._archive_writer.start()`（在 admin 启动前/后均可，需在 minute_roll 前）。
   - AdminServer 构造加：
     `connection_stats=self._connections, latency_stats=self._latency, admin_thread=self._cfg.admin_thread`
4. `_data_loop`：decode 后、转发前采延迟：
```python
            self._stats.record(msg.topic, msg.record_count, len(msg.raw_payload))
            if self._latency.should_sample():
                import time as _t
                self._latency.record(_t.time_ns() - msg.timestamp_ns)
```
5. `_dispatch_control`：
   - REGISTER OK 后：`self._connections.on_connect(cid, info.username, info.endpoint, _role(info.roles))`
   - DISCONNECT：`self._connections.on_disconnect(cid, "disconnect")`
   （`_role` 复用 connections 模块的逻辑，或内联简单判断。）
6. `_heartbeat_sweep_loop`：对每个 offline：`self._connections.on_disconnect(c.client_id, "heartbeat_timeout")`
7. 新增 `_on_auth_event`（async，ZAP 回调签名 `(username, address, ok)`）：
```python
    async def _on_auth_event(self, username: str, address: str, ok: bool) -> None:
        reason = None if ok else "invalid_password"
        self._connections.on_auth(username, address, success=ok, reason=reason)
```
8. `_minute_roll_loop`：改 enqueue：
```python
            archived = self._stats.roll_minute()
            if archived:
                await self._archive_writer.enqueue(archived)
```
9. `stop()`：在 `self._admin.stop()` 之后加 `await self._archive_writer.stop()`（drain 剩余）。顺序：停 admin → 停 archive_writer（drain）→ transport.close → storage.close。注意 storage.close 须在 archive_writer.stop 之后。

- [ ] **Step 4: Run → PASS**

Run: `pytest tests/test_monitoring_perf.py tests/test_admin_v3.py -v` → GREEN。
Run: `pytest -v` → 全量 GREEN（Spec 1+2 e2e 不破；admin 测试带 `admin_token=""`/`admin_token="T"`）。

- [ ] **Step 5: Commit**
```bash
git add src/pulsemq/server.py tests/test_monitoring_perf.py tests/test_admin_v3.py
git commit -m "feat(server): 数据面采延迟+连接/认证事件+async 归档；AdminServer 接入监控依赖"
```

---

## Task 9: 全量回归 + perf 基线收尾

**Files:**
- Test: 全量

- [ ] **Step 1: 全量回归**

Run: `pytest -v`
Expected: 全绿（Spec 1 + 2 + 3 全部）。重点：
  - `test_stats.py`（traffic/storage 单测）继续通过——`AsyncArchiveWriter` 是追加，`StatsStorage` 接口不变。
  - Spec 1+2 e2e 继续通过（admin 测试带 token）。
  - 新增 test_latency_stats/test_connections_stats/test_storage_async/test_admin_v3/test_monitoring_perf 全通过。

- [ ] **Step 2: perf 基线（人工/可选）**

确认高吞吐下数据面无锁路径不抛错（test_monitoring_perf 的 `test_latency_recorded_on_data_plane` 跑 20 条已覆盖基本路径）。

- [ ] **Step 3: Commit（如有调整）**

如全绿无需改代码，跳过 commit；否则修小问题后 commit。

---

## Self-Review（写计划后自查）

**1. Spec coverage（逐节核对 Spec 3）：**
- §1.1 目标：在线 Client 概览（Task 3+6+8）、延迟分位采样（Task 2+8）、事件流环形（Task 3+8）、admin 独立线程（Task 6）、数据路径零阻塞（Task 4+8）、token 复用（Task 6 沿用）、web_ui 新区块（Task 7）。✅
- §3.1 ConnectionStats：on_connect/disconnect/auth、online_clients、recent_events、counters ✅。
- §3.2 LatencyStats：sample_rate、should_sample、record、percentiles ✅；延迟采集点用帧已有 ts（`now_ns - msg.timestamp_ns`）✅。
- §3.3 StatsStorage 异步批量：AsyncArchiveWriter（queue+consumer+batch_size）✅；表结构不变 ✅；事件流不持久化 ✅。
- §4 monitoring 事件总线：简化为 Server 直连 ConnectionStats（on_* 无锁 + 有界 deque），等价于「有界事件队列 + 单写者」，不另起 queue 解耦（YAGNI；on_* 已是 O(1) append）。**记录此简化。**
- §5.1 admin 独立线程：Task 6 admin_thread 模式（thread+loop）✅；§5.3 新路由 ✅；§5.4 数据格式（clients/events/realtime 扩展）✅；§5.5 SSE 反压（沿用 maxsize=64）✅。
- §6 web_ui：Task 7 ✅（人工验证为主）。
- §7 性能：无锁采集（Task 2/3）、有界事件环（Task 3）、异步批量（Task 4）、admin 独立线程（Task 6）、SSE 反压（沿用）✅。
- §8 config：Task 1 ✅。
- §9 测试：test_latency/connections/storage_async/admin_v3/monitoring_perf ✅。

**2. 缺口/注意：**
- **monitoring 事件总线简化**（§4）：不实现独立 queue+多处理器；Server 直连 ConnectionStats。on_* 是 O(1) 无锁 append，等价满足「有界事件队列 + 不反压数据路径」。日志与事件环的「不重复埋点」由 Server 在同一 hook 既调 `log_event`（Spec 1 已有）又调 ConnectionStats.on_* 保证。
- **admin 线程 loop**：沿用全局 `WindowsSelectorEventLoopPolicy`（纯 HTTP 不受 pyzmq 限制），不强制 Proactor（§11.8 的 Proactor 是优化非必需）。
- **on_auth 顺序**：Server 数据面 bind 须在控制面 bind 前且传 on_auth（首次 auth bind 创建 ZAP）。Task 8 保证。
- **storage.close 顺序**：在 archive_writer.stop（drain）之后。
- **web_ui 人工验证**：数据接口由 test_admin_v3 覆盖；UI 截图人工。
- **bcrypt 校验延迟**（Spec 2 遗留）：本 spec 不在消息路径，无影响。

**3. 类型一致性：** `LatencyStats.percentiles()` 键 `p50_ms/p95_ms/p99_ms` 与 AdminServer `_realtime_snapshot` `update(self._latency.percentiles())` 一致 ✅；`ConnectionStats.counters()` 键 `online_users/online_producers/online_consumers/total_subscriptions` 与 realtime/SSE 一致 ✅；`ConnectionStats.online_clients()` 返回 `ClientSnapshot`，AdminServer `_clients_snapshot` 映射字段一致 ✅；`AsyncArchiveWriter.enqueue(dict)` 与 `_minute_roll_loop` 的 `archived`（dict[str,MinuteSlot]）一致 ✅；`Transport.bind(on_auth=)` 与 Server 传入的 `_on_auth_event(username,address,ok)` 一致 ✅。

**4. Placeholder 扫描：** 无 TBD/TODO；每个 code step 含完整代码或精确编辑指令（Task 7 web_ui 是精确区块指令，非占位）。

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-06-27-pulsemq-v2-spec3-implementation.md`. 执行方式：subagent-driven-development，逐 task 派 implementer + reviewer，最后 opus 全分支 review。Task 顺序：1→2→3→4→5→6→7→8→9。
