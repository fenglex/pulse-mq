# 延迟监控与客户端 API 优化 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 为 PulseMQ 新增按 topic 的半程/全程延迟监控（8h 折线图），修复客户端生命周期 API（消除 sleep 模式 + 致命错误吞没），并完成 13 项性能/易用性优化。

**Architecture:** `LatencyStatsRegistry` 按 topic+分钟窗口对齐 `TrafficStats`；全程延迟由 consumer 按采样率经控制面 `LATENCY_REPORT` 回传，数据面热路径零新增开销。客户端 `run_forever` 上提到 `Client` 基类。`SubscriptionTable` 改 COW 无锁读。

**Tech Stack:** Python 3.13、pyzmq、msgspec、pyarrow、loguru、ECharts（Web UI）、uv+pytest

## Global Constraints

- 分支：master（用户确认）
- 语言：所有注释/commit message/对话用中文
- 验证：每个 Task 后 `uv run pytest`；提交前 `uv run pytest` 全量通过
- 规则：Bash 工具用 POSIX 语法（`/dev/null`，禁 `nul`/`dir`/裸反斜杠路径）
- 不修改 `D:\source` 下任何文件
- Windows 平台，`.venv` 为 uv 创建

## 前置调查发现（已调整 D1 范围）

1. **`PlainAuthDict` 保留不删**：它是 `transport/router.py` 的 auth 接口类型契约（被 `AsyncZAPHandler`/`SyncZAPHandler`/`Transport.bind`/`bind_sync_data` 用作类型注解，实际传 `PlainAuth`），删除会断类型注解。从 D1 移除。
2. **`connections.py` 的 `"consumer"` 不改**：B3 只改 `client.py` 的 transport socket role；`connections.py._role_of` 的 `"consumer"` 是角色分类（前端依赖），不动。
3. `MsgType.HEARTBEAT/ADMIN`/`ControlCmd.KICK` 仅被 `tests/test_protocol.py` 引用。
4. `cache_size`/`inject_sender`/`TopicBufferRegistry` 引用：`producers/types.py`、`producers/manager.py`、`server.py`、`client.py`、`admin/server.py`、`admin/web_ui.py`、`cache/topic_buffer.py`。

## 文件结构

| 文件 | 本次职责 |
|------|---------|
| `src/pulsemq/protocol/serialization.py` | C1：序列化器 import 提模块级 |
| `src/pulsemq/client.py` | A1/A2/A3/A4/B2/B3/C3/D3 + 删 cache_size/inject_sender |
| `src/pulsemq/routing.py` | B1：COW 无锁化 |
| `src/pulsemq/config.py` | B2/C2/D2：ClientConfig 接入、环境变量、HWM |
| `src/pulsemq/transport/router.py` | D2：HWM 默认值 |
| `src/pulsemq/control.py` | C3：request_id；新增 LATENCY_REPORT（删 KICK） |
| `src/pulsemq/stats/latency.py` | 新增 LatencyStatsRegistry |
| `src/pulsemq/server.py` | 延迟 registry 接入 + dispatch + 删 cache 引用 |
| `src/pulsemq/admin/server.py` | 延迟 API + 删 topic_buffers/cache_sizes |
| `src/pulsemq/admin/web_ui.py` | 延迟折线图/列表 + 流量 8H + 删 cache 引用 |
| `src/pulsemq/producers/{types,manager}.py` | 删 cache_size/inject_sender/PublisherSender |
| `src/pulsemq/protocol/msg_type.py` | 删 HEARTBEAT/ADMIN |
| `src/pulsemq/cache/` | 整个目录删除 |
| `src/pulsemq/stats/storage.py` | 删 save_minute（单条） |

---

## Phase 1：基础清理（D1 部分 + C1 + B3）

### Task 1.1：序列化器 import 提模块级（C1）

**Files:**
- Modify: `src/pulsemq/protocol/serialization.py`
- Test: `tests/test_protocol.py`（现有，跑通即可）

**Interfaces:** 无变化（公开 API 不变）

- [ ] **Step 1：在 serialization.py 顶部模块级缓存 import**

在 `from io import BytesIO` 之后加入：
```python
# 模块级缓存后端 import，避免热路径每次 serialize/deserialize 重复 import 查找
# （与 frames.py 的 _pd 模式对齐）
try:
    import msgspec as _msgspec
except ImportError:
    _msgspec = None

try:
    import pyarrow as _pa
except ImportError:
    _pa = None

try:
    import pandas as _pd
except ImportError:
    _pd = None
```

- [ ] **Step 2：替换各序列化器内的局部 import**

`MsgpackSerializer.serialize/deserialize` 内 `import msgspec` → `_msgspec`；`JsonSerializer` 同理用 `_msgspec`；`PyArrowSerializer` 内 `import pyarrow as pa`→`_pa`、`import pandas as pd`→`_pd`。

注意：`_msgspec` 为 None 时（未装 msgspec）应抛明确错误。在 `MsgpackSerializer.serialize` 开头加：
```python
if _msgspec is None:
    raise ImportError("msgspec 未安装")
```
其余类同理。

- [ ] **Step 3：验证**
```bash
uv run pytest tests/test_protocol.py tests/test_frames_v2.py -v
```
Expected: PASS

- [ ] **Step 4：Commit**
```bash
git add src/pulsemq/protocol/serialization.py
git commit -m "refactor: 序列化器后端 import 提模块级，与 frames.py 模式对齐"
```

---

### Task 1.2：socket role 命名 consumer→data（B3）

**Files:**
- Modify: `src/pulsemq/client.py`

**Interfaces:** 内部 role 字符串 `"consumer"`→`"data"`（transport 层，非公开 API）

- [ ] **Step 1：全文替换 client.py 中 transport socket role**

`client.py` 中数据面 socket 的 role 名 `"consumer"` → `"data"`（涉及 `connect(..., "consumer", ...)`、`send(..., role="consumer")`、`recv("consumer")`，共约 5 处：行 141、304、512、524 附近）。

**精确范围**：只改 transport role 字符串。**绝不改** `connections.py` 的 `_role_of` 返回值（那是角色分类）。

- [ ] **Step 2：验证**
```bash
uv run pytest tests/test_client_lifecycle.py tests/test_e2e_client_server.py tests/test_client_reconnect.py -v
```
Expected: PASS（role 是内部字符串，e2e 测试验证连通性）

- [ ] **Step 3：Commit**
```bash
git add src/pulsemq/client.py
git commit -m "refactor: 数据面 socket role consumer→data，消除命名误导"
```

---

### Task 1.3：删除 MsgType.HEARTBEAT/ADMIN 与 ControlCmd.KICK（D1）

**Files:**
- Modify: `src/pulsemq/protocol/msg_type.py`、`src/pulsemq/control.py`
- Modify: `tests/test_protocol.py`

- [ ] **Step 1：删除常量定义**

`msg_type.py`：删 `HEARTBEAT = 0x03` 和 `ADMIN = 0x04`（保留 `DATA`/`CONTROL`）。同步更新类 docstring 注释。

`control.py`：删 `KICK` 常量（`ControlCmd` 类内）。

- [ ] **Step 2：迁移 test_protocol.py 中对这些常量的引用**

`grep -n "MsgType.HEARTBEAT\|MsgType.ADMIN\|ControlCmd.KICK" tests/test_protocol.py`，删除或改写相关断言（这些常量已不存在，测试断言应移除）。

- [ ] **Step 3：验证**
```bash
uv run pytest tests/test_protocol.py -v
uv run python -c "from pulsemq.protocol.msg_type import MsgType; from pulsemq.control import ControlCmd; print('OK')"
```
Expected: PASS / OK

- [ ] **Step 4：Commit**
```bash
git add src/pulsemq/protocol/msg_type.py src/pulsemq/control.py tests/test_protocol.py
git commit -m "refactor: 删除未使用的 MsgType.HEARTBEAT/ADMIN 与 ControlCmd.KICK"
```

---

### Task 1.4：删除 producers 死代码（cache_size/inject_sender/PublisherSender）（D1）

**Files:**
- Modify: `src/pulsemq/producers/types.py`、`src/pulsemq/producers/manager.py`、`src/pulsemq/client.py`、`src/pulsemq/server.py`

- [ ] **Step 1：types.py 清理**

删除：`PublisherSender`/`PulsePublisher` 前向字符串引用、`SenderProducerCallback` 别名、`ProducerCallback` 中的 sender 分支。保留 `PubData`、`SimpleProducerCallback`。

- [ ] **Step 2：manager.py 清理**

`ProducerSpec`：删 `cache_size` 字段、`inject_sender` 字段。
`register()`/`register_burst()`：删 `cache_size`/`inject_sender` 参数及其传递。
删除 `inject_sender=True` 的 `RuntimeError` 分支（`manager.py:148` 附近）。

- [ ] **Step 3：调用方清理**

`server.py` 的 `@srv.producer`/`@srv.burst_producer` 装饰器：删 `cache_size` 参数。
`client.py` 的 `ProducerClient.producer`/`burst_producer`：删 `cache_size` 参数及传给 register 的 `inject_sender=False`。

- [ ] **Step 4：验证**
```bash
uv run pytest tests/test_producer_scheduling.py -v
uv run pytest tests/test_server.py tests/test_e2e_client_server.py -v
```
Expected: PASS

- [ ] **Step 5：Commit**
```bash
git add src/pulsemq/producers/ src/pulsemq/client.py src/pulsemq/server.py
git commit -m "refactor: 删除 producers 未接入的 cache_size/inject_sender/PublisherSender"
```

---

### Task 1.5：删除 cache/ 模块 + admin 引用（D1）

**Files:**
- Delete: `src/pulsemq/cache/`（整个目录）
- Modify: `src/pulsemq/admin/server.py`、`src/pulsemq/server.py`、`src/pulsemq/admin/web_ui.py`

- [ ] **Step 1：删除 cache 目录**
```bash
git rm -r src/pulsemq/cache
```

- [ ] **Step 2：admin/server.py 清理**

删除 `from pulsemq.cache.topic_buffer import TopicBufferRegistry` import；`AdminServer.__init__` 删 `topic_buffers` 参数；`_realtime_snapshot`/`_list_topics` 删 `cache_sizes` 输出字段。

- [ ] **Step 3：server.py 清理**

删 `topic_buffers=None` 传参给 AdminServer（`server.py:152` 附近）；删相关注释。

- [ ] **Step 4：web_ui.py 清理**

删除前端对 `cache_sizes` 字段的引用（如有）。

- [ ] **Step 5：验证**
```bash
uv run pytest tests/test_server_admin.py tests/test_admin_v3.py tests/test_admin_token.py tests/test_admin_bytes_keys.py -v
uv run python -c "import pulsemq.admin.server; print('OK')"
```
Expected: PASS / OK

- [ ] **Step 6：Commit**
```bash
git add -A
git commit -m "refactor: 删除未接入的 cache/ 模块及 admin 的 cache_sizes 引用"
```

---

### Task 1.6：删除 StatsStorage.save_minute 单条方法（D1）

**Files:**
- Modify: `src/pulsemq/stats/storage.py`、`tests/test_storage_async.py`、`tests/test_stats.py`

- [ ] **Step 1：storage.py 删除 `save_minute` 方法**（保留 `save_minutes_batch`）

- [ ] **Step 2：迁移测试**

`grep -n "save_minute\b" tests/test_storage_async.py tests/test_stats.py`。把调用 `save_minute`（单条）的测试改为 `save_minutes_batch([item])`。

- [ ] **Step 3：验证**
```bash
uv run pytest tests/test_storage_async.py tests/test_stats.py -v
```
Expected: PASS

- [ ] **Step 4：Commit**
```bash
git add src/pulsemq/stats/storage.py tests/test_storage_async.py tests/test_stats.py
git commit -m "refactor: 删除未使用的 StatsStorage.save_minute 单条方法"
```

---

### Task 1.7：Phase 1 全量回归

- [ ] **Step 1：全量测试**
```bash
uv run pytest
```
Expected: 全部 PASS（无回归）

- [ ] **Step 2：Phase 1 完成，标记里程碑**
```bash
git log --oneline -7
```
确认 6 个 commit 就位。

---

## Phase 2：客户端生命周期 API（A1-A4 + B2）

### Task 2.1：Client 基类 run_forever + 致命错误重抛（A1 + A2）

**Files:**
- Modify: `src/pulsemq/client.py`
- Test: `tests/test_client_lifecycle.py`

**Interfaces:**
- Produces: `Client.run_forever()`、`Client._wait_stop_and_raise_fatal()`（内部 helper）

- [ ] **Step 1：写失败测试**

在 `tests/test_client_lifecycle.py` 加：
```python
async def test_consumer_run_forever_replaces_sleep():
    """ConsumerClient.run_forever 替代 asyncio.sleep，收到 stop 后退出。"""
    cons = ConsumerClient("tcp://localhost:5555", "tcp://localhost:5556",
                          username="u", password="p")
    # 用 monkeypatch 替换 start 为 no-op，避免真实连接
    async def fake_start():
        cons._connected = True
    cons.start = fake_start  # type: ignore
    # 0.1s 后触发 stop
    async def stopper():
        await asyncio.sleep(0.1)
        cons._stop.set()
    asyncio.create_task(stopper())
    await cons.run_forever()  # 应在 stopper 触发后正常返回


async def test_run_forever_reraises_reconnect_fatal():
    """重连致命错误应在 run_forever 主上下文重新抛出。"""
    cons = ConsumerClient("tcp://localhost:5555", "tcp://localhost:5556",
                          username="u", password="p")
    async def fake_start():
        cons._connected = True
    cons.start = fake_start  # type: ignore
    cons._reconnect_fatal = AuthenticationError("认证失败", reason="invalid_password")
    async def stopper():
        await asyncio.sleep(0.05)
        cons._stop.set()
    asyncio.create_task(stopper())
    with pytest.raises(AuthenticationError):
        await cons.run_forever()
```

- [ ] **Step 2：运行测试，确认失败**
```bash
uv run pytest tests/test_client_lifecycle.py::test_consumer_run_forever_replaces_sleep -v
```
Expected: FAIL（`ConsumerClient` 无 `run_forever`）

- [ ] **Step 3：实现基类 run_forever**

在 `Client` 基类（`client.py` 的 `Client` 类，`stop` 方法附近）加入：
```python
async def _wait_stop_and_raise_fatal(self) -> None:
    """等待 _stop 被设置；退出时若有重连致命错误则重新抛出。"""
    try:
        await self._stop.wait()
    finally:
        fatal = self._reconnect_fatal
        if fatal is not None:
            self._reconnect_fatal = None
            raise fatal

async def run_forever(self) -> None:
    """连接 + 注册，运行直到 stop() 或重连致命错误。

    替代手写 asyncio.sleep 的维持模式。重连遇到致命错误（如认证失败）时，
    在主任务上下文重新抛出，使 CLI 经 exit_code_for 拿到 exit 3。
    """
    await self.start()
    try:
        await self._wait_stop_and_raise_fatal()
    finally:
        await self.stop()
```

- [ ] **Step 4：ProducerClient.run_forever 改用基类 helper**

替换 `ProducerClient.run_forever`（`client.py:687`）为：
```python
async def run_forever(self) -> None:
    await self.start()
    try:
        await self._producer_mgr.start_all(self._on_produce, sender_factory=None)
        await self._wait_stop_and_raise_fatal()
    finally:
        await self._producer_mgr.stop_all()
        await self.stop()
```

- [ ] **Step 5：运行测试，确认通过**
```bash
uv run pytest tests/test_client_lifecycle.py -v
```
Expected: PASS

- [ ] **Step 6：Commit**
```bash
git add src/pulsemq/client.py tests/test_client_lifecycle.py
git commit -m "feat: Client 基类 run_forever，修复消费者致命错误吞没（A1+A2）"
```

---

### Task 2.2：subscribe 支持 start 前注册（A3）

**Files:**
- Modify: `src/pulsemq/client.py`
- Test: `tests/test_client_lifecycle.py`

**Interfaces:** `Client.subscribe` 行为变化——未连接时缓存不报错

- [ ] **Step 1：写失败测试**
```python
async def test_subscribe_before_start_is_cached():
    """start 前调用 subscribe 应缓存，不抛异常。"""
    cons = ConsumerClient("tcp://localhost:5555", "tcp://localhost:5556",
                          username="u", password="p")
    # 未 start，transport 未就绪
    await cons.subscribe("market.*", lambda m: None)
    assert "market.*" in cons._subscriptions
```

- [ ] **Step 2：运行确认失败**（当前 subscribe 会调 `_send_subscribe` 触发未连接错误）
```bash
uv run pytest tests/test_client_lifecycle.py::test_subscribe_before_start_is_cached -v
```
Expected: FAIL

- [ ] **Step 3：修改 subscribe**

`Client.subscribe`（`client.py:489`）改为：
```python
async def subscribe(self, topic_pattern: str, callback, *, header_only: bool = False) -> None:
    self._subscriptions[topic_pattern] = callback
    self._sub_header_only[topic_pattern] = header_only
    # 仅在已连接时立即发送；未连接时缓存，start() 末尾会 flush
    if self._connected:
        await self._send_subscribe(topic_pattern)
```
> 确认 `self._connected` 标志存在（`client.py:154` start 成功时置 True）。

- [ ] **Step 4：运行测试通过**
```bash
uv run pytest tests/test_client_lifecycle.py -v
```
Expected: PASS

- [ ] **Step 5：Commit**
```bash
git add src/pulsemq/client.py tests/test_client_lifecycle.py
git commit -m "feat: subscribe 支持 start 前预注册缓存（A3）"
```

---

### Task 2.3：run_forever 信号处理（A4）

**Files:**
- Modify: `src/pulsemq/client.py`
- Test: `tests/test_client_lifecycle.py`

- [ ] **Step 1：在 run_forever 内注册信号**

修改基类 `run_forever`，在 `await self.start()` 后、`_wait_stop_and_raise_fatal` 前加入信号注册（复用 `lifecycle.py:22` 的 Windows 跳过模式）：
```python
import signal

async def run_forever(self) -> None:
    await self.start()
    loop = asyncio.get_running_loop()
    try:
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, self._stop.set)
            except (NotImplementedError, RuntimeError):
                # Windows 不支持 add_signal_handler，静默跳过
                break
    except Exception:
        pass
    try:
        await self._wait_stop_and_raise_fatal()
    finally:
        await self.stop()
```
> SIGTERM 在 Windows 不存在，用 try/except 包裹或仅注册 SIGINT on Windows。优先用 `getattr(signal, 'SIGTERM', None)` 守卫。

- [ ] **Step 2：验证**
```bash
uv run pytest tests/test_client_lifecycle.py -v
```
Expected: PASS

- [ ] **Step 3：Commit**
```bash
git add src/pulsemq/client.py
git commit -m "feat: run_forever 注册 SIGINT/SIGTERM 优雅退出（A4）"
```

---

### Task 2.4：ClientConfig 接入 + Client 参数化（B2）

**Files:**
- Modify: `src/pulsemq/config.py`、`src/pulsemq/client.py`
- Test: `tests/test_config.py`

**Interfaces:**
- Produces: `Client.__init__` 新增可选参数（默认=现有常量）

- [ ] **Step 1：写失败测试**
```python
def test_client_accepts_reconnect_params():
    from pulsemq.client import Client
    c = Client("tcp://localhost:5555", "tcp://localhost:5556",
               username="u", password="p",
               heartbeat_interval=2.5, reconnect_max_delay=60.0)
    assert c._heartbeat_interval == 2.5
    assert c._reconnect_max_delay == 60.0
```

- [ ] **Step 2：运行确认失败**

- [ ] **Step 3：Client.__init__ 接受新参数**

`Client.__init__` 签名加入（默认值取自现有模块常量 `client.py:51-61`）：
```python
def __init__(self, data_endpoint, control_endpoint, *,
             username="", password="", client_id=None,
             heartbeat_interval: float = _HEARTBEAT_INTERVAL,
             reconnect_initial_delay: float = _RECONNECT_INITIAL_DELAY,
             reconnect_max_delay: float = _RECONNECT_MAX_DELAY,
             reconnect_backoff_multiplier: float = _RECONNECT_BACKOFF_MULTIPLIER,
             startup_timeout: float = _STARTUP_MONITOR_TIMEOUT,
             register_reply_timeout: float = _REGISTER_REPLY_TIMEOUT):
```
将这些存为 `self._heartbeat_interval` 等实例属性，并把方法体里对模块常量的引用改为 `self._xxx`。

- [ ] **Step 4：config.py ClientConfig 对齐**

确认 `ClientConfig`（`config.py:40`）字段与上述参数一致；保持不变（它已被定义，B2 只是让 Client 能用它）。

- [ ] **Step 5：运行测试通过**
```bash
uv run pytest tests/test_config.py tests/test_client_lifecycle.py -v
```

- [ ] **Step 6：Commit**
```bash
git add src/pulsemq/client.py src/pulsemq/config.py tests/test_config.py
git commit -m "feat: Client 接入重连/心跳参数，ClientConfig 不再是死配置（B2）"
```

---

### Task 2.5：Phase 2 全量回归

- [ ] **Step 1：全量测试**
```bash
uv run pytest
```
Expected: PASS

---

## Phase 3：性能与配置（B1 + D2 + C2 + C3）

### Task 3.1：SubscriptionTable COW 无锁化（B1）

**Files:**
- Modify: `src/pulsemq/routing.py`
- Test: `tests/test_routing.py`

**Interfaces:** `SubscriptionTable` 公开方法签名不变；`match()` 变为无锁读

- [ ] **Step 1：写失败测试（含并发安全）**

`tests/test_routing.py` 加：
```python
def test_match_returns_results_without_lock():
    """match 不应持有锁（COW 后读路径无锁）。"""
    table = SubscriptionTable()
    table.subscribe(b"id1", "foo.*")
    assert table.match("foo.bar") == {b"id1"}

def test_concurrent_subscribe_and_match():
    """并发：一线程高频 match，另一线程 subscribe/unsubscribe，不抛异常。"""
    import threading
    table = SubscriptionTable()
    table.subscribe(b"id1", "foo.*")
    errors = []
    def reader():
        for _ in range(10000):
            try:
                table.match("foo.bar")
            except Exception as e:
                errors.append(e)
    def writer():
        for i in range(1000):
            table.subscribe(b"id2", f"topic{i}.*")
            table.unsubscribe(b"id2", f"topic{i}.*")
    t1 = threading.Thread(target=reader)
    t2 = threading.Thread(target=writer)
    t1.start(); t2.start()
    t1.join(); t2.join()
    assert not errors
```

- [ ] **Step 2：运行确认失败**（当前 match 持 RLock，并发测试可能通过但目标是无锁实现）

- [ ] **Step 3：重写 SubscriptionTable 为 COW**

核心思路：维护一个不可变快照引用 `_read_index`，写时在 `_write_lock` 内构建新快照原子替换，读路径直接读引用。
```python
from dataclasses import dataclass

@dataclass(frozen=True)
class _Index:
    """不可变路由快照（COW 读路径直接持有引用）。"""
    exact: dict[str, frozenset[bytes]]
    wild: dict[str, frozenset[bytes]]
    by_identity: dict[bytes, frozenset[str]]

_EMPTY = frozenset()

class SubscriptionTable:
    def __init__(self) -> None:
        self._read_index = _Index(exact={}, wild={}, by_identity={})
        self._write_lock = threading.Lock()

    def match(self, topic: str) -> set[bytes]:
        idx = self._read_index  # 原子读引用，无锁
        matched: set[bytes] = set(idx.exact.get(topic, _EMPTY))
        matched |= idx.wild.get(topic, _EMPTY)
        parts = topic.split(".")
        for i in range(len(parts) - 1, 0, -1):
            matched |= idx.wild.get(".".join(parts[:i]), _EMPTY)
        return matched

    def subscribe(self, identity: bytes, topic_pattern: str) -> None:
        with self._write_lock:
            base = self._read_index
            # 拷贝并更新（写频率低，拷贝成本可忽略）
            by_id = dict(base.by_identity)
            pats = set(by_id.get(identity, _EMPTY))
            pats.add(topic_pattern)
            by_id[identity] = frozenset(pats)
            exact = dict(base.exact)
            wild = dict(base.wild)
            if topic_pattern.endswith(".*"):
                prefix = topic_pattern[:-2]
                s = set(wild.get(prefix, _EMPTY)); s.add(identity)
                wild[prefix] = frozenset(s)
            else:
                s = set(exact.get(topic_pattern, _EMPTY)); s.add(identity)
                exact[topic_pattern] = frozenset(s)
            self._read_index = _Index(exact, wild, by_id)

    def unsubscribe(self, identity: bytes, topic_pattern: str) -> None:
        with self._write_lock:
            base = self._read_index
            by_id = dict(base.by_identity)
            pats = set(by_id.get(identity, _EMPTY))
            pats.discard(topic_pattern)
            if pats:
                by_id[identity] = frozenset(pats)
            else:
                by_id.pop(identity, None)
            exact = dict(base.exact); wild = dict(base.wild)
            if topic_pattern.endswith(".*"):
                prefix = topic_pattern[:-2]
                s = set(wild.get(prefix, _EMPTY)); s.discard(identity)
                if s:
                    wild[prefix] = frozenset(s)
                else:
                    wild.pop(prefix, None)
            else:
                s = set(exact.get(topic_pattern, _EMPTY)); s.discard(identity)
                if s:
                    exact[topic_pattern] = frozenset(s)
                else:
                    exact.pop(topic_pattern, None)
            self._read_index = _Index(exact, wild, by_id)

    def remove(self, identity: bytes) -> None:
        with self._write_lock:
            base = self._read_index
            by_id = dict(base.by_identity)
            pats = by_id.pop(identity, _EMPTY)
            exact = dict(base.exact); wild = dict(base.wild)
            for pattern in pats:
                if pattern.endswith(".*"):
                    prefix = pattern[:-2]
                    s = set(wild.get(prefix, _EMPTY)); s.discard(identity)
                    if s: wild[prefix] = frozenset(s)
                    else: wild.pop(prefix, None)
                else:
                    s = set(exact.get(pattern, _EMPTY)); s.discard(identity)
                    if s: exact[pattern] = frozenset(s)
                    else: exact.pop(pattern, None)
            self._read_index = _Index(exact, wild, by_id)

    def subscribers_of(self, identity: bytes) -> set[str]:
        return set(self._read_index.by_identity.get(identity, _EMPTY))

    def snapshot(self) -> dict:
        idx = self._read_index
        return {
            (k.decode("utf-8", "replace") if isinstance(k, (bytes, bytearray)) else k):
                sorted(v) for k, v in idx.by_identity.items()
        }
```
> identity 类型注意：现有代码 identity 是 bytes（server 传 bytes ident）。保持 bytes key。

- [ ] **Step 4：运行测试通过**
```bash
uv run pytest tests/test_routing.py -v
```
Expected: PASS

- [ ] **Step 5：Commit**
```bash
git add src/pulsemq/routing.py tests/test_routing.py
git commit -m "perf: SubscriptionTable 改 COW 无锁读，消除热路径锁开销（B1）"
```

---

### Task 3.2：SNDHWM/RCVHWM 默认值提升 + 可配（D2）

**Files:**
- Modify: `src/pulsemq/transport/router.py`、`src/pulsemq/config.py`

- [ ] **Step 1：router.py 默认值 1000→10000**

`Transport.__init__`（`router.py:277`）：`sndhwm: int = 10000, rcvhwm: int = 10000`。`SyncDataThread.__init__`（`router.py:184`）默认值同步。

- [ ] **Step 2：config.py 加字段**

`ServerConfig` 加 `sndhwm: int = 10000`、`rcvhwm: int = 10000`（带环境变量 `PULSEMQ_SNDHWM`/`PULSEMQ_RCVHWM`）。`ClientConfig` 同理。

- [ ] **Step 3：Server/Client 构造时传入**（如果 Transport 构造在 server.py，传 config 值）

- [ ] **Step 4：验证**
```bash
uv run pytest tests/test_transport_router.py tests/test_config.py -v
```

- [ ] **Step 5：Commit**
```bash
git add src/pulsemq/transport/router.py src/pulsemq/config.py
git commit -m "feat: HWM 默认值 1000→10000 并支持配置（D2）"
```

---

### Task 3.3：ServerConfig 环境变量覆盖（C2）

**Files:**
- Modify: `src/pulsemq/config.py`

- [ ] **Step 1：为以下字段加环境变量覆盖**

在 `load_server_config`（`config.py` 的加载逻辑）中，为这些字段补 `os.environ.get`：
- `heartbeat_timeout` ← `PULSEMQ_HEARTBEAT_TIMEOUT`
- `latency_sample_rate` ← `PULSEMQ_LATENCY_SAMPLE_RATE`
- `retention_days` ← `PULSEMQ_RETENTION_DAYS`
- `bcrypt_cost` ← `PULSEMQ_BCRYPT_COST`
- `sse_interval` ← `PULSEMQ_SSE_INTERVAL`
- `stats_retention_minutes` ← `PULSEMQ_STATS_RETENTION_MINUTES`

- [ ] **Step 2：写测试验证环境变量覆盖**
```python
def test_env_overrides(monkeypatch):
    monkeypatch.setenv("PULSEMQ_HEARTBEAT_TIMEOUT", "10.0")
    cfg = load_server_config(None)
    assert cfg.heartbeat_timeout == 10.0
```

- [ ] **Step 3：验证 + Commit**
```bash
uv run pytest tests/test_config.py -v
git add src/pulsemq/config.py tests/test_config.py
git commit -m "feat: ServerConfig 常用字段支持环境变量覆盖（C2）"
```

---

### Task 3.4：控制面 request_id 关联 ack（C3）

**Files:**
- Modify: `src/pulsemq/control.py`、`src/pulsemq/client.py`、`src/pulsemq/server.py`
- Test: `tests/test_control.py`、`tests/test_client_reconnect.py`

**Interfaces:** 控制帧 payload 可选 `request_id` 字段

- [ ] **Step 1：写失败测试**
```python
async def test_register_reply_carries_request_id():
    """REGISTER 带 request_id，reply 原样回带，client 按 id 匹配。"""
    # e2e 或 mock 验证 reply payload 含相同 request_id
```

- [ ] **Step 2：client 侧生成 request_id 并按 id 匹配 recv**

`client.py` 的 `_register`/`_send_subscribe` 发送时生成 `request_id = uuid4().hex` 放入 payload；recv 时按 `request_id` 匹配回执（超时兜底退化为"最近未匹配"）。

- [ ] **Step 3：server 侧 reply 原样回带 request_id**

`server.py._dispatch_control`：从 `cmd_msg.payload` 取 `request_id`，构造 reply 时回带。

- [ ] **Step 4：验证**
```bash
uv run pytest tests/test_control.py tests/test_client_reconnect.py tests/test_e2e_client_server.py -v
```

- [ ] **Step 5：Commit**
```bash
git add src/pulsemq/control.py src/pulsemq/client.py src/pulsemq/server.py tests/
git commit -m "feat: 控制面 reply 关联 request_id，解决多订阅 ack 串扰（C3）"
```

---

### Task 3.5：Phase 3 全量回归

- [ ] **Step 1：**
```bash
uv run pytest
```
Expected: PASS

---

## Phase 4：延迟监控功能

### Task 4.1：LatencyStatsRegistry 数据结构

**Files:**
- Modify: `src/pulsemq/stats/latency.py`
- Test: `tests/test_latency_stats.py`

**Interfaces:**
- Produces: `LatencyStatsRegistry(sample_rate, retention_minutes)`，方法 `should_sample()→bool`、`record(topic, latency_ns)→None`、`roll_minute()→None`、`snapshot()→dict`、`get_history(topic, minutes)→list[dict]`

- [ ] **Step 1：写失败测试**
```python
def test_registry_record_and_snapshot():
    reg = LatencyStatsRegistry(sample_rate=1.0, retention_minutes=480)
    assert reg.should_sample()  # rate=1.0 总是 True
    reg.record("market.tick", 1_000_000)  # 1ms
    reg.record("market.tick", 2_000_000)  # 2ms
    snap = reg.snapshot()
    assert "market.tick" in snap
    assert snap["market.tick"]["count"] == 2

def test_registry_roll_minute_appends_history():
    reg = LatencyStatsRegistry(sample_rate=1.0)
    reg.record("market.tick", 1_000_000)
    reg.roll_minute()
    hist = reg.get_history("market.tick", 60)
    assert len(hist) == 1
    assert "p50_ms" in hist[0]
    # roll 后 current 清空
    assert reg.snapshot().get("market.tick") is None or reg.snapshot()["market.tick"]["count"] == 0

def test_registry_history_capped_at_retention():
    reg = LatencyStatsRegistry(sample_rate=1.0, retention_minutes=3)
    for _ in range(5):
        reg.record("t", 500_000)
        reg.roll_minute()
    assert len(reg.get_history("t", 100)) == 3  # maxlen=3
```

- [ ] **Step 2：运行确认失败**

- [ ] **Step 3：实现 LatencyStatsRegistry**

在 `latency.py` 保留现有 `LatencyStats` 类（作为 per-topic 桶单元），新增：
```python
from collections import deque
from dataclasses import dataclass

@dataclass
class MinuteLatency:
    timestamp: int
    p50_ms: float
    p95_ms: float
    p99_ms: float
    count: int

class LatencyStatsRegistry:
    """按 topic + 分钟窗口的延迟统计。线程安全（Lock）。"""

    def __init__(self, sample_rate: float = 0.01, retention_minutes: int = 480) -> None:
        self._rate = max(0.0, min(1.0, sample_rate))
        self._retention = retention_minutes
        self._current: dict[str, LatencyStats] = {}
        self._history: dict[str, deque[MinuteLatency]] = {}
        self._lock = threading.Lock()

    def should_sample(self) -> bool:
        if self._rate >= 1.0: return True
        if self._rate <= 0.0: return False
        return random.random() < self._rate

    def record(self, topic: str, latency_ns: int) -> None:
        with self._lock:
            ls = self._current.get(topic)
            if ls is None:
                ls = LatencyStats(sample_rate=1.0)  # 内部不再采样，由 registry 控制
                self._current[topic] = ls
            ls.record(latency_ns)

    def roll_minute(self) -> None:
        ts = int(time.time()) // 60 * 60
        with self._lock:
            for topic, ls in self._current.items():
                snap = ls.snapshot()
                if snap.get("count", 0) > 0:
                    ml = MinuteLatency(timestamp=ts, p50_ms=snap["p50_ms"],
                                       p95_ms=snap["p95_ms"], p99_ms=snap["p99_ms"],
                                       count=snap["count"])
                    dq = self._history.get(topic)
                    if dq is None:
                        dq = deque(maxlen=self._retention)
                        self._history[topic] = dq
                    dq.append(ml)
            self._current.clear()

    def snapshot(self) -> dict[str, dict]:
        with self._lock:
            return {t: ls.snapshot() for t, ls in self._current.items()}

    def get_history(self, topic: str, minutes: int = 60) -> list[dict]:
        dq = self._history.get(topic)
        if not dq: return []
        return [{"timestamp": m.timestamp, "p50_ms": m.p50_ms, "p95_ms": m.p95_ms,
                 "p99_ms": m.p99_ms, "count": m.count}
                for m in list(dq)[-minutes:]]
```
> 需要 `import time` 在文件顶部。

- [ ] **Step 4：运行测试通过**
```bash
uv run pytest tests/test_latency_stats.py -v
```

- [ ] **Step 5：Commit**
```bash
git add src/pulsemq/stats/latency.py tests/test_latency_stats.py
git commit -m "feat: 新增 LatencyStatsRegistry 按 topic+分钟窗口延迟统计"
```

---

### Task 4.2：server 半程延迟 registry 接入

**Files:**
- Modify: `src/pulsemq/server.py`
- Test: `tests/test_server.py`

**Interfaces:**
- Consumes: `LatencyStatsRegistry`
- Produces: `self._lat_half`（半程）、`self._lat_e2e`（全程，Task 4.3 用）

- [ ] **Step 1：Server 构造时实例化两个 registry**

`server.py` `Server.__init__`：将现有 `self._latency = LatencyStats(...)` 替换为：
```python
from pulsemq.stats.latency import LatencyStatsRegistry
self._lat_half = LatencyStatsRegistry(sample_rate=latency_sample_rate or config.latency_sample_rate,
                                      retention_minutes=config.stats_retention_minutes)
self._lat_e2e = LatencyStatsRegistry(sample_rate=..., retention_minutes=...)
```

- [ ] **Step 2：_on_data_message 改用 half registry**

`server.py:346-347` 改为：
```python
if self._lat_half.should_sample():
    self._lat_half.record(hdr.topic, time.time_ns() - hdr.timestamp_ns)
```

- [ ] **Step 3：_on_server_produce 同步改**

`server.py:326-327` 同上用 `self._lat_half`。

- [ ] **Step 4：_minute_roll_loop 加 latency roll**

`server.py:487` 附近，`roll_minute` 后加：
```python
self._lat_half.roll_minute()
self._lat_e2e.roll_minute()
```

- [ ] **Step 5：更新对旧 self._latency 的所有引用**

`grep -n "_latency" src/pulsemq/server.py src/pulsemq/admin/server.py`，把取全局延迟快照的地方改为从 registry 派生（admin 在 Task 4.5 处理）。

- [ ] **Step 6：验证**
```bash
uv run pytest tests/test_server.py -v
```

- [ ] **Step 7：Commit**
```bash
git add src/pulsemq/server.py
git commit -m "feat: server 半程延迟改用 LatencyStatsRegistry 按 topic 统计"
```

---

### Task 4.3：LATENCY_REPORT 控制命令 + server dispatch

**Files:**
- Modify: `src/pulsemq/control.py`、`src/pulsemq/server.py`
- Test: `tests/test_control.py`

**Interfaces:**
- Produces: `ControlCmd.LATENCY_REPORT`

- [ ] **Step 1：control.py 加常量**
```python
class ControlCmd:
    ...
    LATENCY_REPORT = "LATENCY_REPORT"
```

- [ ] **Step 2：写失败测试**
```python
async def test_latency_report_dispatch():
    """server 收到 LATENCY_REPORT 应记入 _lat_e2e registry。"""
    # 构造 ControlMessage(cmd=LATENCY_REPORT, payload={topic, latency_ns})
    # 调 server._dispatch_control(b"id", msg)
    # 断言 _lat_e2e.snapshot() 含 topic
```

- [ ] **Step 3：server _dispatch_control 加分支**

`server.py:377` `_dispatch_control` 加：
```python
elif cmd_msg.cmd == ControlCmd.LATENCY_REPORT:
    topic = cmd_msg.payload.get("topic", "")
    latency_ns = int(cmd_msg.payload.get("latency_ns", 0))
    if topic and latency_ns > 0:
        self._lat_e2e.record(topic, latency_ns)
    # 无 ack（fire-and-forget）
```

- [ ] **Step 4：验证 + Commit**
```bash
uv run pytest tests/test_control.py -v
git add src/pulsemq/control.py src/pulsemq/server.py tests/test_control.py
git commit -m "feat: 新增 LATENCY_REPORT 控制命令与 server dispatch"
```

---

### Task 4.4：consumer 回传延迟

**Files:**
- Modify: `src/pulsemq/client.py`
- Test: `tests/test_e2e_client_server.py`

- [ ] **Step 1：client _recv_loop 加采样回传**

`client.py:516` `_recv_loop`，在 `decode_header` 后、回调分发后加：
```python
# 端到端延迟采样回传（复用 sample_rate）
if self._latency_sample_rate > 0 and random.random() < self._latency_sample_rate:
    try:
        latency_ns = time.time_ns() - hdr.timestamp_ns
        rep = frames.encode_control(
            ControlCmd.LATENCY_REPORT,
            {"topic": hdr.topic, "latency_ns": latency_ns},
        )
        await self._transport.send(b"", rep, role="control")
    except Exception:
        logger.debug("延迟回传发送失败", exc_info=True)
```
> `self._latency_sample_rate` 需在 `Client.__init__` 接受（默认 0.01，来自 config 或常量）。

- [ ] **Step 2：写 e2e 测试验证回传**

`tests/test_e2e_client_server.py` 加：producer 发消息 → consumer 收到 → 等待回传到达 server → 断言 `server._lat_e2e.snapshot()` 含 topic（sample_rate=1.0 确保回传）。

- [ ] **Step 3：验证**
```bash
uv run pytest tests/test_e2e_client_server.py -v
```

- [ ] **Step 4：Commit**
```bash
git add src/pulsemq/client.py tests/test_e2e_client_server.py
git commit -m "feat: consumer 采样回传端到端延迟到 server"
```

---

### Task 4.5：admin API（realtime 注入 + history 端点）

**Files:**
- Modify: `src/pulsemq/admin/server.py`

- [ ] **Step 1：_realtime_snapshot 注入 latency 字段**

`admin/server.py` 的 `_realtime_snapshot` 加：
```python
snapshot["latency"] = {
    "half": latency_half_registry.snapshot(),
    "e2e": latency_e2e_registry.snapshot(),
}
```
（registry 引用从 Server 传入 AdminServer）

- [ ] **Step 2：新增 history 端点**

加路由 `GET /api/v1/latency/topics/{topic}/history?minutes=60&kind=half`：
```python
# 解析 topic/minutes/kind，调对应 registry.get_history(topic, minutes)
```

- [ ] **Step 3：验证**
```bash
uv run pytest tests/test_server_admin.py tests/test_admin_v3.py -v
```

- [ ] **Step 4：Commit**
```bash
git add src/pulsemq/admin/server.py
git commit -m "feat: admin API 注入延迟快照 + 延迟历史端点"
```

---

### Task 4.6：Web UI 延迟折线图 + 底部列表

**Files:**
- Modify: `src/pulsemq/admin/web_ui.py`

- [ ] **Step 1：延迟折线图（替换现有全局延迟柱状图）**

ECharts line，复用流量趋势图的 JS 模式：1H/8H 切换、多 topic 叠加（LRU）、30s 刷新。默认 P50/P95/P99 三线，可切 half/e2e。数据源 `/api/v1/latency/topics/{topic}/history`。

- [ ] **Step 2：底部端到端延迟列表表格**

页面底部新增表格：每行 topic，列 half(P50/P95/P99)、e2e(P50/P95/P99)、采样数。数据源 realtime `latency.half`/`latency.e2e`。

- [ ] **Step 3：在线客户端延迟**

client 详情弹窗加延迟展示：producer→其发送 topic 的 half，consumer→其订阅 topic 的 e2e（按 client.topics 派生）。

- [ ] **Step 4：验证**（手动启动 server 看 UI，或跑 admin 测试）
```bash
uv run pytest tests/test_server_admin.py -v
```

- [ ] **Step 5：Commit**
```bash
git add src/pulsemq/admin/web_ui.py
git commit -m "feat: Web UI 延迟折线图/底部列表/客户端延迟展示"
```

---

### Task 4.7：流量趋势图 8H 切换

**Files:**
- Modify: `src/pulsemq/admin/web_ui.py`

- [ ] **Step 1：流量趋势图切换选项 1H/6H → 1H/8H**

定位流量趋势图的切换按钮 JS，把 `6H` 改为 `8H`（数据源 `TrafficStats` 内存窗口已是 480 分钟=8h，无需后端改动）。

- [ ] **Step 2：Commit**
```bash
git add src/pulsemq/admin/web_ui.py
git commit -m "feat: 流量趋势图最大历史 6H→8H，与延迟折线图统一"
```

---

### Task 4.8：Phase 4 全量回归

- [ ] **Step 1：**
```bash
uv run pytest
```
Expected: PASS

---

## Phase 5：客户端订阅索引（D3）

### Task 5.1：client _recv_loop 前缀索引匹配

**Files:**
- Modify: `src/pulsemq/client.py`
- Test: `tests/test_client_lifecycle.py`

**Interfaces:** 内部优化，`_subscriptions` 匹配从 O(n) 遍历改为索引

- [ ] **Step 1：写失败测试（多订阅性能/正确性）**
```python
async def test_recv_dispatches_to_multiple_subscriptions():
    """多个订阅（含通配）应正确分发，不遗漏。"""
    # mock transport.recv 返回构造帧，验证各回调被调用
```

- [ ] **Step 2：client 维护精确+通配索引**

在 `Client` 内引入类似 `SubscriptionTable` 的双索引（精确 dict + 通配 dict），subscribe/unsubscribe 时更新，`_recv_loop` 用索引 O(1) 匹配替代遍历 `_subscriptions`。

可复用 `routing.SubscriptionTable`（它已是 COW），或 client 内建轻量索引。优先复用 `SubscriptionTable`。

- [ ] **Step 3：验证**
```bash
uv run pytest tests/test_client_lifecycle.py tests/test_e2e_client_server.py -v
```

- [ ] **Step 4：Commit**
```bash
git add src/pulsemq/client.py tests/test_client_lifecycle.py
git commit -m "perf: client 订阅匹配改前缀索引，消除 O(订阅数) 遍历（D3）"
```

---

### Task 5.2：最终全量回归 + 文档更新

- [ ] **Step 1：全量测试**
```bash
uv run pytest
```
Expected: 全部 PASS

- [ ] **Step 2：更新 DESIGN.md 与 README（如有版本号/端口/功能描述变化）**

更新 `docs/DESIGN.md` 的遗留项表（cache/ 已删）、`LatencyStats` 描述（改 registry）、新增延迟监控小节。

- [ ] **Step 3：bump 版本号**

`_version.py` 版本号提升（如 7.3.0 → 7.4.0）。

- [ ] **Step 4：Commit**
```bash
git add -A
git commit -m "docs: 更新设计文档与版本号，完成延迟监控与客户端优化"
```

---

## Self-Review 结论

**Spec 覆盖**：A1-A4（Task 2.1-2.3）、B1-B3（3.1/1.2/2.4）、C1-C3（1.1/3.3/3.4）、D1-D3（1.3-1.6/3.2/5.1）、延迟功能（4.1-4.7）、流量 8H（4.7）均有对应 Task。

**类型一致性**：`LatencyStatsRegistry` 在 4.1 定义、4.2-4.5 使用，签名一致。`ControlCmd.LATENCY_REPORT` 在 4.3 定义、4.4 使用。`run_forever`/`_wait_stop_and_raise_fatal` 在 2.1 定义、2.3 扩展。

**风险点**：C3（request_id）改动 client+server 协同，需充分 e2e 测试；B1 COW 需并发测试覆盖。
