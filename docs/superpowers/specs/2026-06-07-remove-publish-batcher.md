# 移除 publish 端 (含服务端引擎) batcher 策略

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**日期**: 2026-06-07
**状态**: approved
**作者**: brainstorming session

## 目标与动机

完全移除 PulseMQ 当前版本中与"批量发布"相关的策略与代码,回归到"一条 PUB,一条广播"的简单语义。涵盖三个独立但同时设计的层面:

1. **客户端 Batcher** (`pulsemq.client.batcher.Batcher`): 客户端在 `publish()` 入口处按 count+time+max_wait 双触发攒批,攒够 N 条后合成 `MsgType.BATCH` 帧发送。
2. **BATCH 协议**: `MsgType.BATCH = 0x0C` 帧类型 + `FrameCodec.encode/decode_batch_payload` 序列化/反序列化。
3. **服务端 `_handle_batch`**: 收到 BATCH 帧后拆 N 条,每条走现有 PUB 路径。
4. **服务端引擎 `_adapt_batch_size`**: 引擎主循环在低负载时按"自适应批大小"排空 socket 缓冲区,按 `(identity, topic)` 分组并发派发。
5. **用户级 batch 配置**: `users` 表的 `batch_size` / `batch_interval_ms` / `batch_max_wait_ms` 字段 + `PermissionService.get/set_batch_config` API + Web UI 批量配置 Tab + REST 端点。

**动机**:

- 性能数据未达预期。`docs/perf-comparison.md` 显示 Batcher 在 df-msgpack 路径上仅带来 +12% 吞吐 (3,209 → 3,604 msg/s),距 v1.0 spec 目标的 5-7k msg/s 相差甚远。
- 代码复杂度高。一个看似简单的客户端攒批器涉及: 锁、后台 task、单 topic 假设、topic 切换、max_wait 计时、first-of-both 触发、与 BATCH 协议深度耦合、与 `_handle_batch` 双向配套。服务端 `_adapt_batch_size` 又叠加了一层自适应调参 (`h >= effective * 0.8` 条件、grow/shrink 振荡 —— 已被 `known-issues.md` I34 记录)。
- 用户体感弱。`batch_size=1` 是默认,即绝大多数用户从未触发 Batcher 路径,功能 ≈ 死代码。
- 协议污染。`MsgType.BATCH` 引入了一种新 msg_type,即便无人用,所有客户端 SDK 都必须保留"识别 BATCH / 拒绝 / 忽略"分支。

**明确不动**:

- 版本号 (保持 v1.0.x,本设计是 v1.0 内"撤销"而非发新版)
- 服务端 XPUB 一次 send 多帧的能力 (这是 ZMQ 原生特性,与 batcher 策略无关)
- 订阅、查询、鉴权、流控等其他子系统
- DataFrame 序列化层 (msgspec/json/pyarrow) 的优化

---

## 移除清单

### 文件级删除 (整文件消失)

| 路径 | 性质 |
|------|------|
| `src/pulsemq/client/batcher.py` | 客户端 Batcher 实现 |
| `tests/unit/test_client_batcher.py` | Batcher 单元测试 |
| `tests/unit/test_batch_msg_type.py` | BATCH msg_type 单元测试 |
| `tests/integration/test_batcher_e2e.py` | Batcher 端到端测试 |
| `scripts/bench_100k_with_batcher.py` | 专门测 Batcher 的 100k 压测脚本 (合并到 bench_baseline.py 的直发模式) |

### 文件级修改

| 路径 | 变更 |
|------|------|
| `src/pulsemq/client/async_client.py` | 删 `Batcher` import、删构造参数 `batch_size/batch_interval_ms/batch_max_wait_ms`、删 `self._batcher`、`_batcher_send` 方法、`publish()` 中 batch 路由分支、`connect()` / `disconnect()` 中 Batcher 生命周期 |
| `src/pulsemq/protocol/msg_type.py` | 删 `BATCH = 0x0C` |
| `src/pulsemq/protocol/frames.py` | 删 `FrameCodec.encode_batch_payload` / `decode_batch_payload` 静态方法 |
| `src/pulsemq/engine/handlers.py` | 删 `_handle_batch` 方法、删 `_dispatch_internal` 中 `MsgType.BATCH` 分支 |
| `src/pulsemq/engine/engine.py` | 删 `_effective_batch_size` / `_max_batch_size` / `_drain_timeout_ms` / `_batch_history` / `_adapt_window` 字段;删 `_adapt_batch_size` 方法;简化 `_drain_socket` → 空操作或删除调用;简化 `_dispatch_batch` → 单条 `_dispatch_one`;删 `EngineMetrics.effective_batch_size` 字段;`server.py` 启动日志去掉 `max_batch_size=...` |
| `src/pulsemq/config.py` | 删 `max_batch_size: int = 64` / `drain_timeout_ms: int = 1` 字段、对应 `PULSEMQ_BATCH_SIZE` / `PULSEMQ_DRAIN_TIMEOUT` 环境变量 |
| `src/pulsemq/auth/permission.py` | 删 `get_batch_config` / `set_batch_config` 方法 |
| `src/pulsemq/auth/models.py` (或 models 同位置) | 删 `User` dataclass 的 `batch_size` / `batch_interval_ms` / `batch_max_wait_ms` 字段 |
| `src/pulsemq/storage/sqlite_user.py` | `users` 表 schema 删三列;INSERT/SELECT 删三列读写;启动时检测旧库有这三列则 `ALTER TABLE users DROP COLUMN` (SQLite 3.35+) |
| `src/pulsemq/monitoring/admin_server.py` | 删 `GET /api/v1/users/{user_id}/batch_config` 与 `PUT /api/v1/users/{user_id}/batch_config` 路由 + handler |
| `src/pulsemq/monitoring/admin_backend.py` | 删 `get_batch_config` / `set_batch_config` 委托方法 |
| `src/pulsemq/monitoring/web_ui.py` | HTML 删 `<button data-tab="batch">` 与 `<div id="tab-batch">`;JS 删 `loadBatchConfig` / `populateBatchSelect` / 批量配置 PUT 调用;概览页删 "引擎批大小" 行 |
| `src/pulsemq/server.py` | 启动日志去掉 `max_batch_size=...` 字段 |
| `tests/unit/test_engine.py` | 删 `_adapt_batch_size` 相关测试 |
| `tests/unit/test_admin_server.py` | 删 `batch_config` 端点测试 |
| `tests/unit/test_config.py` | 删 `PULSEMQ_BATCH_SIZE` / `PULSEMQ_DRAIN_TIMEOUT` 测试 |
| `tests/unit/test_monitoring_api.py` | 删 `effective_batch_size` 指标测试 |
| `tests/unit/test_monitoring_realtime.py` | 删 `effective_batch_size` 相关断言 |
| `tests/unit/test_monitoring_minute.py` | 删 `effective_batch_size` 相关断言 |
| `tests/integration/test_engine_transport.py` | 删 BATCH 帧相关断言 |
| `tests/conftest.py` | 删 `client_with_batcher` 之类 fixture (如有) |
| `scripts/bench_market_data.py` | 删 `BATCH_SIZE` 常量与 `PulseClient(..., batch_size=...)` 传参 |
| `scripts/bench_1m.py` | 同上 |
| `docs/known-issues.md` | 删 line 77 I34 `_adapt_batch_size` 振荡条目 (修复对象已不存在) |
| `docs/superpowers/specs/2026-06-07-pulsemq-v1-optimization.md` | 文首加 banner 指向本 spec |
| `docs/perf-100k-batched-data.md` | 文首加 banner 标注历史数据 |
| `docs/perf-100k-batched-report.md` | 同上 |
| `docs/perf-comparison.md` | 同上 |

### 保留 (历史档案)

| 路径 | 原因 |
|------|------|
| `docs/perf-100k-batched-data.md` | 100k batcher 性能数据,加 banner 保留 |
| `docs/perf-100k-batched-report.md` | 100k batcher 性能报告,加 banner 保留 |
| `docs/perf-comparison.md` | 三阶段对比表,加 banner 保留 |
| `docs/superpowers/specs/2026-06-07-pulsemq-v1-optimization.md` | v1.0 整体设计,加 banner 保留 |

---

## 协议层 (wire format)

### 变更

- 删除 `MsgType.BATCH = 0x0C`。
- 删除 `FrameCodec.encode_batch_payload(payloads: list, comp: str) -> bytes`。
- 删除 `FrameCodec.decode_batch_payload(payload: bytes, comp: str) -> list[(ser_fmt, payload_bytes)]`。

### 客户端发送

`publish()` 唯一路径:

```python
frames = FrameCodec.encode(MsgType.PUB, topic, record_count, payload, ser_fmt, compression)
await self._send_with_retry(frames, retry, retry_delay)
```

无分支,无 Batcher。

### 服务端接收

`_dispatch_internal` 中 `MsgType.BATCH` 分支删除。`_handle_batch` 方法删除。

收到未识别 msg_type 走现有"其他类型暂忽略"路径 (`handlers.py` line 205)。

### 向后兼容 (旧 client)

- 旧 client 传 `batch_size=...` 关键字 → 新 `PulseClient.__init__` 抛 `TypeError: unexpected keyword argument` (符合 Python 惯例,用户修改成本低)。
- 旧 client 实际发 BATCH 帧 → 新 server 静默忽略,无 ERROR 帧,无 ERROR 日志 (与现有"未知 msg_type 忽略"行为一致)。

---

## 客户端 (`async_client.py`)

### 构造参数

```python
def __init__(
    self,
    address: str,
    api_key: str | None = None,
    xpub_address: str | None = None,
    auto_reconnect: bool = True,
    reconnect_initial_delay: float = 1.0,
    reconnect_max_delay: float = 30.0,
    reconnect_backoff: float = 2.0,
    heartbeat_interval: float = 10.0,
    recv_timeout: float = 5.0,
    connect_timeout: float = 5.0,
    identity: bytes | None = None,
):
```

### 字段

- 移除: `self._batch_size` / `self._batch_interval_ms` / `self._batch_max_wait_ms` / `self._batcher`
- 移除: `from pulsemq.client.batcher import Batcher`

### `connect()`

```python
async def connect(self) -> None:
    self._ctx = zmq.asyncio.Context()
    self._dealer = self._ctx.socket(zmq.DEALER)
    self._dealer.setsockopt(zmq.IDENTITY, self._identity)
    self._dealer.setsockopt(zmq.HEARTBEAT_IVL, 2000)
    self._dealer.setsockopt(zmq.HEARTBEAT_TIMEOUT, 5000)
    self._dealer.connect(self._address)
    self._sub = self._ctx.socket(zmq.SUB)
    self._sub.connect(self._xpub_address)
    self._connected = True
    self._reconnect_count = 0
    logger.info("已连接到 %s", self._address)
```

无 Batcher 创建。

### `disconnect()`

```python
async def disconnect(self) -> None:
    self._connected = False
    if self._sub:
        self._sub.close(linger=0)
        self._sub = None
    if self._dealer:
        self._dealer.close(linger=0)
        self._dealer = None
    if self._ctx:
        self._ctx.term()
        self._ctx = None
    logger.info("已断开连接")
```

无 Batcher 关闭。

### `publish()`

删除 "批模式入 Batcher" 分支:

```python
async def publish(
    self,
    topic: str,
    data: str | bytes | pd.DataFrame,
    format: str | None = None,
    compression: str = "none",
    retry: int = 0,
    retry_delay: float = 0.1,
) -> None:
    ser_fmt = self._resolve_format(data, format)
    self._validate_data(data, ser_fmt)
    record_count = self._infer_record_count(data)
    if isinstance(data, pd.DataFrame) and ser_fmt in ("json", "msgpack"):
        payload_obj = data.to_dict(orient="records")
    else:
        payload_obj = data
    payload = FrameCodec.encode_payload(payload_obj, ser_fmt, compression)
    frames = FrameCodec.encode(
        MsgType.PUB, topic, record_count, payload, ser_fmt, compression
    )
    await self._send_with_retry(frames, retry, retry_delay)
```

### `_batcher_send` 方法

整方法删除。

### API 兼容性

- 删除 `batch_size` 等 3 个关键字参数后,旧 client 传参会 `TypeError`。
- 文档 (`docs/api-reference.md`) 需相应更新: 删除 Batcher 相关章节。

---

## 服务端引擎 (`engine.py`)

### 核心简化

主循环从"一条 recv → 排空 socket 至 N → 派发 batch"改为"一条 recv → 派发单条"。

### 字段删除

- `_effective_batch_size: int`
- `_max_batch_size: int` (来自 `config.max_batch_size`)
- `_drain_timeout_ms: int` (来自 `config.drain_timeout_ms`)
- `_batch_history: list[int]`
- `_adapt_window: int`

### 方法删除 / 简化

- `_adapt_batch_size(self, actual: int)` → 整方法删除。
- `_drain_socket(self, batch: list)` → 删除,主循环不再调用。
- `_dispatch_batch(self, batch: list)` → 简化为 `_dispatch_one(self, frames: list[bytes])`,对单条帧直接 `await self._handlers.dispatch(frames)` 或现有 fast path。
- 移除 `_pub_fast_path` 内的"batch 循环"分支(单条处理无需循环)。

### 主循环

低负载路径:

```python
while self._running:
    try:
        if self._pending_tasks >= self._max_concurrency:
            self._metrics.backpressure_events += 1
            logger.debug("背压触发: pending=%d", self._pending_tasks)
            await asyncio.sleep(0.001)
            continue

        consumed = await self._drain_buffers()
        if consumed > 0:
            continue

        frames = await self._transport.recv()
        load_ratio = self._pending_tasks / self._max_concurrency if self._max_concurrency else 0
        if load_ratio > self._backpressure_threshold:
            self._dual_buffer.enqueue(frames)
        else:
            await self._dispatch_one(frames)

    except asyncio.CancelledError:
        break
    except Exception:
        logger.exception("Engine 消息循环异常")
        if self._running:
            continue
        break
```

无 `_drain_socket`、无 `_dispatch_batch`、无 `_adapt_batch_size`。

### `_dispatch_one`

新方法,取代 `_dispatch_batch` 的单条情形:

```python
async def _dispatch_one(self, frames: list[bytes]) -> None:
    """派发单条消息。
    
    优先走 PUB 快速路径 (绕过拦截器链),否则走拦截器链 (含鉴权/拦截器)。
    """
    if self._pub_fast_path and self._is_pub_frames(frames):
        try:
            await self._handlers.dispatch_pub_fast(frames)
            self._metrics.total_messages += 1
        except Exception as e:
            self._metrics.total_errors += 1
            logger.debug("快速路径处理错误: %s", e)
        return
    # 拦截器链路径: SUB/UNSUB/PING/QUERY/非 PUB 走这里
    await self._process_single(frames)
```

复用现有 `_process_single` (line 307),它已调用 `await self._handlers.dispatch(frames)` 并处理 metrics 与异常。

### `EngineMetrics`

```python
@dataclass
class EngineMetrics:
    pending_tasks: int = 0
    concurrency_usage: float = 0.0
    backpressure_events: int = 0
    total_messages: int = 0
    total_errors: int = 0
```

`effective_batch_size` 字段删除。

### `server.py` 启动日志

```python
logger.info(
    "Engine 启动: max_concurrency=%d, fast_path=%s",
    self._max_concurrency, self._pub_fast_path,
)
```

去掉 `max_batch_size=...`。

---

## 配置 (`config.py`)

### 字段删除

```python
@dataclass
class ServerConfig:
    # 引擎层
    max_concurrency: int = 100
    use_uvloop: bool = True
    object_pool_size: int = 4096
    # 删除: max_batch_size: int = 64
    # 删除: drain_timeout_ms: int = 1
```

### 环境变量删除

```python
_ENV_MAP: dict[str, tuple[str, type]] = {
    "PULSEMQ_CONCURRENCY": ("max_concurrency", int),
    # 删除: "PULSEMQ_BATCH_SIZE": ("max_batch_size", int),
    # 删除: "PULSEMQ_DRAIN_TIMEOUT": ("drain_timeout_ms", int),
}
```

---

## 用户模型与存储

### `User` 模型

`src/pulsemq/auth/permission.py` 中的 `User` (或 `models.py`):

```python
@dataclass
class User:
    user_id: int
    username: str
    api_key_hash: str
    is_admin: bool = False
    is_active: bool = True
    created_at: float = 0.0
    # 删除: batch_size: int = 1
    # 删除: batch_interval_ms: int = 10
    # 删除: batch_max_wait_ms: int = 50
```

### `PermissionService`

```python
class PermissionService:
    # 删除: get_batch_config
    # 删除: set_batch_config
```

### `sqlite_user.py` schema

```sql
CREATE TABLE users (
    user_id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT UNIQUE NOT NULL,
    api_key_hash TEXT NOT NULL,
    is_admin INTEGER DEFAULT 0,
    is_active INTEGER DEFAULT 1,
    created_at REAL NOT NULL
    -- 删除: batch_size INTEGER DEFAULT 1
    -- 删除: batch_interval_ms INTEGER DEFAULT 10
    -- 删除: batch_max_wait_ms INTEGER DEFAULT 50
);
```

INSERT / SELECT 同步删三列读写。

### 启动迁移

```python
async def _migrate_drop_batch_columns(self) -> None:
    """启动时检测旧库,自动 DROP COLUMN batch_size/batch_interval_ms/batch_max_wait_ms。
    
    SQLite 3.35+ 原生支持 DROP COLUMN。若 SQLite 版本过低抛 RuntimeError 拒绝启动。
    """
    async with self._conn.execute("PRAGMA table_info(users)") as cursor:
        existing_cols = {row[1] for row in await cursor.fetchall()}
    
    drops = []
    for col in ("batch_size", "batch_interval_ms", "batch_max_wait_ms"):
        if col in existing_cols:
            drops.append(f"ALTER TABLE users DROP COLUMN {col}")
    
    for sql in drops:
        try:
            await self._conn.execute(sql)
        except sqlite3.Error as e:
            raise RuntimeError(f"迁移失败: {sql} → {e}") from e
    if drops:
        await self._conn.commit()
        logger.info("已删除 users 表遗留 batch_* 列 (%d 个)", len(drops))
```

在 `init_schema` / `connect` 入口处调用一次。

**失败行为**: 迁移失败抛 `RuntimeError` 并停机 (fail-fast),日志清晰说明哪条 SQL 失败。**不**写 fallback 让列留下 — 与"彻底移除"目标一致。

---

## 管理后台与 Web UI

### REST API 端点删除

```
DELETE /api/v1/users/{user_id}/batch_config    (GET, PUT)
```

`admin_server.py` 中删路由 + handler + OpenAPI 注释。

### `admin_backend.py`

```python
class AdminBackend:
    # 删除: get_batch_config
    # 删除: set_batch_config
```

### `web_ui.py`

HTML (`<button data-tab="batch">` + `<div id="tab-batch">` 块): 整段删除。

JS:

- `loadBatchConfig()` 函数删除。
- `populateBatchSelect` 函数删除。
- `if (btn.dataset.tab === 'batch') { loadUsers().then(populateBatchSelect); }` 删除。
- 概览页 `r.engine_batch_size` 引用删除 (字段已从 metrics 删除)。
- 批量配置 `bs-size` / `bs-interval` / `bs-wait` 表单与 PUT 调用删除。

---

## 测试

### 文件级删除

| 文件 | 原因 |
|------|------|
| `tests/unit/test_client_batcher.py` | Batcher 单元测试 |
| `tests/unit/test_batch_msg_type.py` | BATCH msg_type 单元测试 |
| `tests/integration/test_batcher_e2e.py` | Batcher 端到端测试 |

### 文件级修改

| 文件 | 删/改 |
|------|------|
| `tests/unit/test_engine.py` | 删 `_adapt_batch_size` 相关测试;删 batch 派发分组相关测试 |
| `tests/unit/test_admin_server.py` | 删 `batch_config` 端点测试 (GET/PUT) |
| `tests/unit/test_config.py` | 删 `PULSEMQ_BATCH_SIZE` / `PULSEMQ_DRAIN_TIMEOUT` 测试 |
| `tests/unit/test_monitoring_api.py` | 删 `effective_batch_size` 指标相关 |
| `tests/unit/test_monitoring_realtime.py` | 删 `effective_batch_size` 断言 |
| `tests/unit/test_monitoring_minute.py` | 删 `effective_batch_size` 断言 |
| `tests/integration/test_engine_transport.py` | 删 BATCH 帧相关断言 |
| `tests/conftest.py` | 删 `client_with_batcher` 之类 fixture |

### 保留测试 (验证回归)

- `tests/unit/test_client_subscribe.py` 保留 (与 batcher 无关)
- `tests/integration/test_server_lifecycle.py` 保留
- `tests/integration/test_monitoring_e2e.py` 保留
- `tests/integration/test_admin_server.py` 保留 (改掉 batch_config 部分即可)
- `tests/integration/test_admin_permissions.py` 保留
- `tests/integration/test_json_e2e.py` 保留
- `tests/security/*` 保留 (ZAP 鉴权,无关 batcher)
- 性能与 soak 脚本: 见下节

---

## 压测脚本

### 删除

`scripts/bench_100k_with_batcher.py` 整个文件 (脚本名带 `_with_batcher`,移除后无意义)。

### 修改

`scripts/bench_market_data.py`:

- 删 `BATCH_SIZE` / `BATCH_INTERVAL_MS` / `BATCH_MAX_WAIT_MS` 常量。
- 删 `PulseClient(..., batch_size=..., batch_interval_ms=..., batch_max_wait_ms=...)` 传参。

`scripts/bench_1m.py`: 同上。

`scripts/bench_baseline.py`: 应已是直发模式 (`batch_size=1` 默认),无需修改。

`scripts/bench_concurrent.py` / `bench_soak.py`: 检查是否有 `batch_size` 引用,如有则删。

---

## 文档

### 新增

- 当前文件: `docs/superpowers/specs/2026-06-07-remove-publish-batcher.md` (本设计)

### 修改

**`docs/superpowers/specs/2026-06-07-pulsemq-v1-optimization.md`**: 文首加 banner:

```markdown
> ⚠️ **部分撤销**: 客户端 Batcher / BATCH 协议 / `_handle_batch` / `_adapt_batch_size` 已被
> [2026-06-07-remove-publish-batcher](./2026-06-07-remove-publish-batcher.md) 移除。
> 本文档仅作历史档案保留,不要按本文档第 1.2、1.3 节实施。
```

**`docs/perf-100k-batched-data.md`**: 文首加:

```markdown
> ⚠️ **历史数据**: 本文件基于 v1.0 Batcher 实现 (size=10, interval=10ms)。
> Batcher 策略已在 [2026-06-07-remove-publish-batcher](../superpowers/specs/2026-06-07-remove-publish-batcher.md) 中移除。
> 数据仅作历史对比参考。
```

**`docs/perf-100k-batched-report.md`** + **`docs/perf-comparison.md`**: 同上。

**`docs/known-issues.md`**: 删除 line 77 I34 条目:

```markdown
- [P0][日期 2026-06-07] I34: `_adapt_batch_size` 在 batch_size=1 时 grow/shrink 振荡 ...
```

(修复对象已不存在,问题不再相关。)

**`docs/api-reference.md`**: 删除 Batcher / 批量配置相关章节。

**`README.md`**: 搜索 `batcher` / `BATCH` 关键字,如有引用则改或删。

---

## 错误处理

| 场景 | 行为 |
|------|------|
| 旧 client 传 `batch_size=...` 关键字 | `PulseClient.__init__` 抛 `TypeError` |
| 旧 client 实际发 BATCH 帧 | server 静默忽略,无 ERROR 帧,无 ERROR 日志 |
| 旧 SQLite 库 `users` 表有 `batch_*` 列 | 启动时自动 `ALTER TABLE ... DROP COLUMN` (×3) |
| 迁移失败 (SQLite 版本低 / 文件锁 / 权限) | 抛 `RuntimeError` 停机,fail-fast,日志记录失败 SQL |
| Web UI 旧版本 (缓存) 访问 `batch_config` 端点 | server 返回 404 (路由已删) |

---

## 验证

### 回归测试

```bash
uv run pytest tests/unit tests/integration
```

**预期**: 全绿,~20 个 batcher 相关测试被删除,剩余测试零回归。

### 手动 smoke

```bash
# 1. 启动 server
uv run python scripts/test_server_runner.py --port 16900 &

# 2. 跑基线 (直发模式,16 组合)
uv run python scripts/bench_baseline.py --port 16900

# 3. 跑端到端
uv run python scripts/test_e2e_all.py
```

**预期**: 16/16 e2e 通过,16 组合基线与 v0.6 数一致(原本就是直发模式)。

### 迁移验证

```bash
# 旧库升级
sqlite3 pulse_mq.db "PRAGMA table_info(users);"
# 应看到 batch_size / batch_interval_ms / batch_max_wait_ms 三列

# 启动 server,日志应输出:
# INFO 已删除 users 表遗留 batch_* 列 (3 个)

sqlite3 pulse_mq.db "PRAGMA table_info(users);"
# 三列消失
```

### 性能基线

不主动跑 100k 压测 (因 perf 数据已存档)。如需对比,跑 `bench_baseline.py` 即可,与 v0.6 数据对比。

---

## 实施步骤 (供 writing-plans 参考)

1. **协议层先行** (无依赖):
   - 删 `MsgType.BATCH`
   - 删 `FrameCodec.encode/decode_batch_payload`
   - 跑 `uv run pytest tests/unit/test_protocol_*.py` 确认未误伤

2. **服务端 handlers** (依赖 1):
   - 删 `_handle_batch` + dispatch 分支
   - 跑 `tests/unit/test_handlers.py` + `tests/integration/test_engine_transport.py`

3. **服务端 engine** (依赖 1):
   - 删 `_adapt_batch_size` / `_drain_socket` / `_dispatch_batch` 等
   - 简化为单条派发
   - 跑 `tests/unit/test_engine.py` + `tests/integration/test_*.py`

4. **config.py** (无强依赖):
   - 删 `max_batch_size` / `drain_timeout_ms` 字段 + 环境变量
   - 跑 `tests/unit/test_config.py`

5. **User 模型 + storage** (依赖 1):
   - 删 `User` 字段
   - 改 schema
   - 加 `_migrate_drop_batch_columns`
   - 跑 `tests/unit/test_storage_sqlite_user.py`

6. **permission.py** (依赖 5):
   - 删 `get/set_batch_config`

7. **Admin + Web UI** (依赖 5、6):
   - 删 REST 端点
   - 删 Web UI Tab
   - 跑 `tests/unit/test_admin_server.py` + `tests/unit/test_web_ui.py`

8. **客户端** (依赖 1):
   - 改 `async_client.py` (删 Batcher 集成、参数、_batcher_send)
   - 跑 `tests/unit/test_client_subscribe.py` (剩余) + 手动 e2e

9. **测试删除** (依赖 1-8):
   - 删 `test_client_batcher.py` / `test_batch_msg_type.py` / `test_batcher_e2e.py`
   - 改 `test_engine.py` / `test_admin_server.py` / `test_config.py` / `test_monitoring_*.py` / `test_engine_transport.py`
   - 改 `conftest.py` 删 fixture

10. **压测脚本** (依赖 8):
    - 删 `bench_100k_with_batcher.py`
    - 改 `bench_market_data.py` / `bench_1m.py`

11. **文档** (依赖全部):
    - 加 banner
    - 删 I34
    - 改 `api-reference.md` / `README.md`

12. **全量回归**:
    - `uv run pytest tests/unit tests/integration`
    - `uv run python scripts/test_e2e_all.py`
    - 确认全绿

---

## 风险与缓解

| 风险 | 缓解 |
|------|------|
| 引擎单条派发后高负载场景吞吐下降 | 现有双缓冲 (`_drain_buffers` + `DualBuffer`) 已在高负载路径中保留,背压信号量 + 双缓冲分担。`_dispatch_one` 不创建后台 task,低负载时延迟更低。 |
| 删 BATCH msg_type 后旧 client 兼容 | 默认 `batch_size=1` 即直发,绝大多数用户无感;启用过 batcher 的用户 `TypeError` 提示清晰。 |
| SQLite 迁移失败 | fail-fast,日志清晰,不静默。文档中显式说明: SQLite < 3.35 不支持 DROP COLUMN,需手动删列或升级。 |
| 删 `bench_100k_with_batcher.py` 失去历史对比 | 已有 `docs/perf-100k-batched-data.md` 存档,加 banner。 |

---

## 不在范围

- 分布式 (单 server 不变)
- 协议加密 (CURVE 留 v1.1+)
- 客户端 SDK 重构
- 第三方集成 (Kafka/Pulsar 桥接)
- 性能优化 (DataFrame 序列化层等的优化保持)

---

## 验收清单

- [ ] 上述"文件级删除"全部完成
- [ ] 上述"文件级修改"全部完成
- [ ] `uv run pytest tests/unit tests/integration` 全绿
- [ ] `uv run python scripts/test_e2e_all.py` 16/16 通过
- [ ] 旧库迁移测试: 旧库启动后 `batch_*` 列自动消失
- [ ] 文档 banner + I34 删除 + README 检查完成
- [ ] git 历史干净: 1 个 commit (或按子系统分组的少量 commit)
- [ ] 现有 16 组合基线 (`bench_baseline.py`) 与 v0.6 一致或更优
