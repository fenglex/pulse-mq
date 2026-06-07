# 移除 publish 端 batcher 策略实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 完全移除 PulseMQ 的客户端 Batcher、BATCH 协议、服务端 `_handle_batch`、服务端引擎 `_adapt_batch_size`、`users` 表 batch 字段、PermissionService 批量配置 API、Web UI 批量配置 Tab、相关测试与压测脚本,回归"一条 PUB 一条广播"的简单语义。

**Architecture:** 自下而上分层删除 — 协议层 → 服务端 handlers/engine → 配置 → 用户模型/存储 → auth/permission → admin/web UI → 客户端 → 测试/脚本/文档。每一层产出可独立编译/测试的中间态,逐步缩小 batcher 触点。每任务后立即 commit。

**Tech Stack:** Python 3.13+, pyzmq 27.x, aiosqlite, msgspec, pytest + pytest-asyncio

**Spec:** `docs/superpowers/specs/2026-06-07-remove-publish-batcher.md`

---

## 文件结构 (本计划的触点总览)

### 删除 (整文件)
- `src/pulsemq/client/batcher.py`
- `tests/unit/test_client_batcher.py`
- `tests/unit/test_batch_msg_type.py`
- `tests/integration/test_batcher_e2e.py`
- `scripts/bench_100k_with_batcher.py`

### 修改 (源码)
- `src/pulsemq/protocol/msg_type.py` — 删 BATCH
- `src/pulsemq/protocol/frames.py` — 删 batch payload 编解码
- `src/pulsemq/engine/handlers.py` — 删 _handle_batch
- `src/pulsemq/engine/engine.py` — 删 _adapt_batch_size / 简化 dispatch
- `src/pulsemq/config.py` — 删 max_batch_size / drain_timeout_ms
- `src/pulsemq/auth/permission.py` — 删 get/set_batch_config
- `src/pulsemq/auth/models.py` — 删 User batch 字段 (如独立文件)
- `src/pulsemq/storage/sqlite_user.py` — 删 batch 列 + 加 migration
- `src/pulsemq/monitoring/admin_server.py` — 删 batch_config 路由
- `src/pulsemq/monitoring/admin_backend.py` — 删 batch_config 方法
- `src/pulsemq/monitoring/web_ui.py` — 删批量配置 Tab
- `src/pulsemq/server.py` — 启动日志去 max_batch_size
- `src/pulsemq/client/async_client.py` — 删 Batcher 集成

### 修改 (测试)
- `tests/unit/test_engine.py` — 删 _adapt_batch_size 测试
- `tests/unit/test_admin_server.py` — 删 batch_config 端点测试
- `tests/unit/test_config.py` — 删 PULSEMQ_BATCH_SIZE/DRAIN_TIMEOUT 测试
- `tests/unit/test_monitoring_api.py` / `realtime.py` / `minute.py` — 删 effective_batch_size
- `tests/integration/test_engine_transport.py` — 删 BATCH 断言
- `tests/conftest.py` — 删 batcher fixture

### 修改 (脚本)
- `scripts/bench_market_data.py` — 删 BATCH_* 常量与传参
- `scripts/bench_1m.py` — 同上

### 修改 (文档)
- `docs/known-issues.md` — 删 I34
- `docs/superpowers/specs/2026-06-07-pulsemq-v1-optimization.md` — 加 banner
- `docs/perf-100k-batched-data.md` / `docs/perf-100k-batched-report.md` / `docs/perf-comparison.md` — 加 banner
- `docs/api-reference.md` — 删 Batcher 章节
- `README.md` — 检查并清理引用

---

## Task 1: 删除 BATCH 协议 (msg_type + FrameCodec batch payload)

**Files:**
- Modify: `src/pulsemq/protocol/msg_type.py`
- Modify: `src/pulsemq/protocol/frames.py`
- Test: `tests/unit/test_protocol_msg_type.py` (新增断言 BATCH 不存在)

- [ ] **Step 1: 添加失败测试 — 断言 BATCH msg_type 不再存在**

打开 `tests/unit/test_protocol_msg_type.py`,在文件末尾添加:

```python
def test_batch_msg_type_removed():
    """BATCH 协议已在 v1.0 batcher 后退时移除,MsgType 中不应有 BATCH。"""
    import pytest
    with pytest.raises(AttributeError):
        _ = MsgType.BATCH
```

- [ ] **Step 2: 运行测试确认失败**

```bash
uv run pytest tests/unit/test_protocol_msg_type.py::test_batch_msg_type_removed -v
```

Expected: FAIL with `AttributeError: type object 'MsgType' has no attribute 'BATCH'` 已被抛出而测试**通过**了——等等,期望测试通过。需要调整:测试应该断言 `hasattr(MsgType, 'BATCH')` 为 False。

修正:把测试改为

```python
def test_batch_msg_type_removed():
    """BATCH 协议已在 v1.0 batcher 后退时移除,MsgType 中不应有 BATCH。"""
    assert not hasattr(MsgType, "BATCH"), "MsgType.BATCH 应当已被移除"
```

然后运行,期望 FAIL (`hasattr` 为 True,因为 BATCH 还在)。再删 BATCH。

- [ ] **Step 3: 运行测试,确认当前 FAIL (BATCH 还在)**

```bash
uv run pytest tests/unit/test_protocol_msg_type.py::test_batch_msg_type_removed -v
```

Expected: FAIL — `MsgType.BATCH 应当已被移除`

- [ ] **Step 4: 删 `MsgType.BATCH` 定义**

打开 `src/pulsemq/protocol/msg_type.py`,找到 `BATCH = 0x0C` 那一行(及其上方注释),删除。例如:

```python
# 删除:
# 单条 PUB: 0x02
# ...
# 批量 BATCH: 0x0C  ← 删
BATCH = 0x0C  ← 删这行
```

- [ ] **Step 5: 删 `FrameCodec.encode_batch_payload` / `decode_batch_payload`**

打开 `src/pulsemq/protocol/frames.py`,找到 `encode_batch_payload` 静态方法(在文件 line 129 附近)与 `decode_batch_payload`(若有),整段删除。

- [ ] **Step 6: 运行测试,确认 PASS**

```bash
uv run pytest tests/unit/test_protocol_msg_type.py tests/unit/test_protocol_frames.py -v
```

Expected: PASS (此时其他文件可能因 import 失败,会红——属于预期,本任务只关注 protocol 层)

- [ ] **Step 7: Commit**

```bash
git add src/pulsemq/protocol/msg_type.py \
        src/pulsemq/protocol/frames.py \
        tests/unit/test_protocol_msg_type.py
git commit -m "feat(protocol): 移除 BATCH msg_type 与 batch payload 编解码

完全删除 publish 端 batcher 策略的协议层。
- MsgType.BATCH = 0x0C 不再存在
- FrameCodec.encode_batch_payload/decode_batch_payload 删除

服务端的 _handle_batch 与客户端的 Batcher 在后续任务中清理。

🤖 Generated with [Claude Code](https://claude.com/claude-code)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 2: 删除服务端 `_handle_batch`

**Files:**
- Modify: `src/pulsemq/engine/handlers.py`
- Test: `tests/integration/test_engine_transport.py` (新增断言 BATCH 被忽略)

- [ ] **Step 1: 添加失败测试 — BATCH msg_type 应被忽略**

打开 `tests/integration/test_engine_transport.py`,在文件末尾添加:

```python
async def test_batch_msg_type_ignored_by_engine(engine_with_zmq):
    """BATCH msg_type 已被移除,engine 收到 0x0C 帧时静默忽略,不抛错。"""
    from pulsemq.protocol.frames import FrameCodec
    from pulsemq.protocol.msg_type import MsgType

    # 0x0C 是旧的 BATCH 值 — 现在不在 MsgType 枚举中,直接用 0x0C 构造
    # 一个 frames list (topic, meta, rc, payload), engine 收到后应静默忽略
    frames = FrameCodec.encode(0x0C, "test.topic", 1, b"x", "msgpack", "none")
    # 这里的 0x0C 不在 MsgType 中,engine 走"其他类型暂忽略"分支
    # 不应抛错,不应有广播
    await engine_with_zmq.dispatch(frames)  # 不抛错即通过
```

(具体 fixture 名 `engine_with_zmq` 视 conftest 已有 fixture 而定,若无,可改用现有 fixture 名称或新建。)

- [ ] **Step 2: 运行测试,确认当前 PASS (因为现在 BATCH 分支仍存在,但只是不再被 match)**

跳过 — 此步视实际实现而调整,关键是删完代码后,发 0x0C 帧不应崩。

- [ ] **Step 3: 删 `_handle_batch` 与 dispatch 分支**

打开 `src/pulsemq/engine/handlers.py`:

1. 找到 `async def _handle_batch(self, ctx: PipelineContext) -> None:`(line 238 附近),删除整个方法。
2. 找到 dispatch 中 `elif ctx.msg_type == MsgType.BATCH:`(line 203 附近)与 `await self._handle_batch(ctx)`,删除这两行。

- [ ] **Step 4: 跑引擎/handlers 单元测试 + 集成测试**

```bash
uv run pytest tests/unit/test_handlers.py tests/integration/test_engine_transport.py -v
```

Expected: PASS (注意:可能因 Task 1 中删了 `MsgType.BATCH` 而 `MsgType.BATCH` 引用失败,需先在 Step 3 中删除引用)

- [ ] **Step 5: Commit**

```bash
git add src/pulsemq/engine/handlers.py \
        tests/integration/test_engine_transport.py
git commit -m "feat(engine): 删除服务端 _handle_batch

BATCH 协议已移除,服务端不再有 _handle_batch 处理逻辑。
0x0C 帧走现有"其他类型暂忽略"分支。

🤖 Generated with [Claude Code](https://claude.com/claude-code)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 3: 删除服务端引擎 `_adapt_batch_size` 与 batch 派发

**Files:**
- Modify: `src/pulsemq/engine/engine.py`
- Test: `tests/unit/test_engine.py`

- [ ] **Step 1: 添加失败测试 — `Engine._adapt_batch_size` 不应存在**

打开 `tests/unit/test_engine.py`,在文件末尾添加:

```python
def test_engine_has_no_adapt_batch_size():
    """服务端 _adapt_batch_size 自适应批大小已移除,Engine 实例不应有该方法。"""
    from pulsemq.engine.engine import Engine
    # 静态方法也走类,所以 hasattr(Engine, '_adapt_batch_size') 检查
    assert not hasattr(Engine, "_adapt_batch_size"), \
        "Engine._adapt_batch_size 应当已被移除"
```

- [ ] **Step 2: 运行测试,确认当前 FAIL**

```bash
uv run pytest tests/unit/test_engine.py::test_engine_has_no_adapt_batch_size -v
```

Expected: FAIL — `_adapt_batch_size 应当已被移除`

- [ ] **Step 3: 删 Engine 字段与方法**

打开 `src/pulsemq/engine/engine.py`,做以下修改:

1. **docstring 顶部** (line 1-8): 把 "自适应批处理" 从核心设计描述中删除,改为:
   ```
   核心设计:
   - 主循环只负责最快把消息从 socket 取出来,不等待处理完成
   - 单条消息派发,无批处理/分组
   - 信号量控制总并发,pending_tasks 超阈值暂停 recv(背压)
   - 双缓冲:高负载入双缓冲,低负载直接派发
   ```

2. **删除字段** (`__init__` 中):
   ```python
   # 删:
   self._effective_batch_size: int = 1
   self._max_batch_size: int = config.max_batch_size
   self._drain_timeout_ms: int = config.drain_timeout_ms
   self._batch_history: list[int] = []
   ```

3. **删除 `_adapt_window` 字段** (若存在,本任务与 `_adapt_batch_size` 配套)。

4. **删除方法** (line 316-333): 整段 `_adapt_batch_size` 方法删除。

5. **删除 `_drain_socket` 方法** (line 199-220): 整段删除。

6. **简化主循环** (line 138-147): 把

   ```python
   batch = [frames]
   await self._drain_socket(batch)
   await self._dispatch_batch(batch)
   self._adapt_batch_size(len(batch))
   ```

   改为:

   ```python
   await self._dispatch_one(frames)
   ```

7. **简化 `_dispatch_batch` → `_dispatch_one`**: 整段方法改写为:

   ```python
   async def _dispatch_one(self, frames: list[bytes]) -> None:
       """派发单条消息。
       
       优先走 PUB 快速路径 (绕过拦截器链),否则走拦截器链。
       """
       if self._pub_fast_path and self._is_pub_frames(frames):
           try:
               await self._handlers.dispatch_pub_fast(frames)
               self._metrics.total_messages += 1
           except Exception as e:
               self._metrics.total_errors += 1
               logger.debug("快速路径处理错误: %s", e)
           return
       await self._process_single(frames)
   ```

8. **删除 `EngineMetrics.effective_batch_size`** (line 36): 字段删除。

9. **删除 `from collections import defaultdict`** (line 15, 顶部 import) 如不再使用。检查 grep `defaultdict` 全文确认无引用后删除。

- [ ] **Step 4: 修改 server.py 启动日志**

打开 `src/pulsemq/server.py`,找到 logger.info("Engine 启动: max_concurrency=%d, max_batch_size=%d, fast_path=%s", ...) 这行(line 277 附近),删除 `max_batch_size=%d` 与对应参数:

```python
# 改前:
logger.info(
    "Engine 启动: max_concurrency=%d, max_batch_size=%d, fast_path=%s",
    self._max_concurrency, self._max_batch_size, self._pub_fast_path,
)
# 改后:
logger.info(
    "Engine 启动: max_concurrency=%d, fast_path=%s",
    self._max_concurrency, self._pub_fast_path,
)
```

- [ ] **Step 5: 跑测试**

```bash
uv run pytest tests/unit/test_engine.py tests/integration/test_engine_transport.py -v
```

Expected: PASS

- [ ] **Step 6: 删除 test_engine.py 中 `_adapt_batch_size` 相关测试**

打开 `tests/unit/test_engine.py`,搜索 `_adapt_batch_size` / `effective_batch_size` / `_drain_socket` / `_dispatch_batch`,删除所有相关测试函数 (3-5 个函数)。

- [ ] **Step 7: 跑测试**

```bash
uv run pytest tests/unit/test_engine.py -v
```

Expected: PASS

- [ ] **Step 8: Commit**

```bash
git add src/pulsemq/engine/engine.py \
        src/pulsemq/server.py \
        tests/unit/test_engine.py
git commit -m "feat(engine): 删除 _adapt_batch_size 与 _drain_socket, 单条派发

服务端引擎不再有自适应批大小与 socket 排空。
主循环简化为 recv → _dispatch_one (单条)。
dispatch 走 fast path 或 _process_single,无 batch 分组。

🤖 Generated with [Claude Code](https://claude.com/claude-code)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 4: 删除 `config.py` 中 batch 设置

**Files:**
- Modify: `src/pulsemq/config.py`
- Test: `tests/unit/test_config.py`

- [ ] **Step 1: 添加失败测试 — `ServerConfig` 不应有 max_batch_size / drain_timeout_ms**

打开 `tests/unit/test_config.py`,在文件末尾添加:

```python
def test_server_config_no_batch_fields():
    """max_batch_size / drain_timeout_ms 已从 ServerConfig 移除。"""
    from pulsemq.config import ServerConfig
    cfg = ServerConfig()
    assert not hasattr(cfg, "max_batch_size"), \
        "ServerConfig.max_batch_size 应当已被移除"
    assert not hasattr(cfg, "drain_timeout_ms"), \
        "ServerConfig.drain_timeout_ms 应当已被移除"
```

- [ ] **Step 2: 运行测试,确认当前 FAIL**

```bash
uv run pytest tests/unit/test_config.py::test_server_config_no_batch_fields -v
```

Expected: FAIL — `max_batch_size 应当已被移除`

- [ ] **Step 3: 删 `max_batch_size` / `drain_timeout_ms` 字段与环境变量**

打开 `src/pulsemq/config.py`:

1. `_ENV_MAP` dict (line 17-18): 删两行
   ```python
   "PULSEMQ_BATCH_SIZE": ("max_batch_size", int),
   "PULSEMQ_DRAIN_TIMEOUT": ("drain_timeout_ms", int),
   ```

2. `ServerConfig` dataclass (line 55-56): 删两行
   ```python
   max_batch_size: int = 64
   drain_timeout_ms: int = 1
   ```

- [ ] **Step 4: 删 `test_config.py` 中相关测试**

搜索 `max_batch_size` / `drain_timeout_ms` / `PULSEMQ_BATCH_SIZE` / `PULSEMQ_DRAIN_TIMEOUT` 在该文件中,删除 2-4 个测试函数。

- [ ] **Step 5: 跑测试**

```bash
uv run pytest tests/unit/test_config.py -v
```

Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add src/pulsemq/config.py tests/unit/test_config.py
git commit -m "feat(config): 移除 max_batch_size / drain_timeout_ms 与环境变量

服务端引擎已无 batch 概念,配置项不再需要。

🤖 Generated with [Claude Code](https://claude.com/claude-code)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 5: 删除 `User` 模型 batch 字段

**Files:**
- Modify: `src/pulsemq/auth/permission.py` (User 类可能在此文件)
- Test: `tests/unit/test_auth_permission.py` (新增断言)

- [ ] **Step 1: 定位 User 模型定义位置**

```bash
grep -n "class User" src/pulsemq/auth/permission.py
grep -rn "class User" src/pulsemq/auth/
```

如在 `permission.py` 内,直接改;若在 `models.py`,改 `models.py`。

- [ ] **Step 2: 添加失败测试 — `User` 不应有 batch 字段**

打开 `tests/unit/test_auth_permission.py`,在文件末尾添加:

```python
def test_user_model_no_batch_fields():
    """User 模型不应有 batch_size / batch_interval_ms / batch_max_wait_ms。"""
    from pulsemq.auth.permission import User
    user = User(user_id=1, username="x", api_key_hash="y")
    assert not hasattr(user, "batch_size"), "User.batch_size 应当已被移除"
    assert not hasattr(user, "batch_interval_ms"), "User.batch_interval_ms 应当已被移除"
    assert not hasattr(user, "batch_max_wait_ms"), "User.batch_max_wait_ms 应当已被移除"
```

- [ ] **Step 3: 运行测试,确认当前 FAIL**

```bash
uv run pytest tests/unit/test_auth_permission.py::test_user_model_no_batch_fields -v
```

Expected: FAIL

- [ ] **Step 4: 删 `User` 三个字段**

打开 `User` 定义文件 (`permission.py` 或 `models.py`),找到 dataclass `class User:`:

```python
@dataclass
class User:
    user_id: int
    username: str
    api_key_hash: str
    is_admin: bool = False
    is_active: bool = True
    created_at: float = 0.0
    # 删:
    # batch_size: int = 1
    # batch_interval_ms: int = 10
    # batch_max_wait_ms: int = 50
```

删除三行。

- [ ] **Step 5: 跑测试**

```bash
uv run pytest tests/unit/test_auth_permission.py -v
```

Expected: PASS (可能 PermissionService.get_batch_config 还在但因 User 字段删除而无法工作,这是 Task 6 处理)

- [ ] **Step 6: Commit**

```bash
git add src/pulsemq/auth/permission.py \
        tests/unit/test_auth_permission.py
git commit -m "feat(auth): User 模型移除 batch_size / batch_interval_ms / batch_max_wait_ms

User 不再持有批量发布相关字段。

🤖 Generated with [Claude Code](https://claude.com/claude-code)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 6: SQLite storage — 删 batch 列 + 加 migration

**Files:**
- Modify: `src/pulsemq/storage/sqlite_user.py`
- Test: `tests/unit/test_storage_sqlite_user.py`

- [ ] **Step 1: 添加失败测试 — 旧库带 batch_* 列应被自动删除**

打开 `tests/unit/test_storage_sqlite_user.py`,在文件末尾添加:

```python
import pytest
import sqlite3
import tempfile
import os

@pytest.mark.asyncio
async def test_migrate_drops_legacy_batch_columns():
    """旧库 users 表带 batch_size / batch_interval_ms / batch_max_wait_ms,
    初始化时应自动 ALTER TABLE DROP COLUMN 删列。
    """
    from pulsemq.storage.sqlite_user import SqliteUserStore

    # 创建临时旧库,带 batch_* 列
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    try:
        conn = sqlite3.connect(db_path)
        conn.executescript("""
            CREATE TABLE users (
                user_id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                api_key_hash TEXT NOT NULL,
                batch_size INTEGER DEFAULT 1,
                batch_interval_ms INTEGER DEFAULT 10,
                batch_max_wait_ms INTEGER DEFAULT 50,
                is_admin INTEGER DEFAULT 0,
                is_active INTEGER DEFAULT 1,
                created_at REAL NOT NULL
            );
        """)
        conn.commit()
        conn.close()

        # 初始化 store,应自动迁移
        store = SqliteUserStore(f"sqlite:///{db_path}")
        await store.init_schema()  # 或 connect/init,视实际 API 而定

        # 验证列已删
        conn = sqlite3.connect(db_path)
        cur = conn.execute("PRAGMA table_info(users)")
        cols = {row[1] for row in cur.fetchall()}
        conn.close()
        await store.close()

        assert "batch_size" not in cols
        assert "batch_interval_ms" not in cols
        assert "batch_max_wait_ms" not in cols
    finally:
        os.unlink(db_path)
```

- [ ] **Step 2: 运行测试,确认当前 FAIL**

```bash
uv run pytest tests/unit/test_storage_sqlite_user.py::test_migrate_drops_legacy_batch_columns -v
```

Expected: FAIL — 列未删

- [ ] **Step 3: 删 schema 中的 batch_* 列**

打开 `src/pulsemq/storage/sqlite_user.py`,找到 `CREATE TABLE users` 语句,删除:

```sql
batch_size INTEGER DEFAULT 1,
batch_interval_ms INTEGER DEFAULT 10,
batch_max_wait_ms INTEGER DEFAULT 50,
```

并删除所有 INSERT / SELECT 引用这三列的代码。

- [ ] **Step 4: 实现 migration**

在 `sqlite_user.py` 中,找到 `init_schema` / `connect` / `__init__` 入口(具体以代码现场为准),在初始化表之前/之后调用:

```python
async def _migrate_drop_batch_columns(self) -> None:
    """启动时检测旧库 users 表,自动 DROP COLUMN batch_size 等遗留列。
    
    SQLite 3.35+ 原生支持 DROP COLUMN。
    """
    import logging
    import sqlite3
    logger = logging.getLogger(__name__)
    
    async with self._conn.execute("PRAGMA table_info(users)") as cursor:
        rows = await cursor.fetchall()
    existing_cols = {row[1] for row in rows}
    
    drops = [
        f"ALTER TABLE users DROP COLUMN {col}"
        for col in ("batch_size", "batch_interval_ms", "batch_max_wait_ms")
        if col in existing_cols
    ]
    
    for sql in drops:
        try:
            await self._conn.execute(sql)
        except sqlite3.Error as e:
            raise RuntimeError(f"迁移失败: {sql} → {e}") from e
    if drops:
        await self._conn.commit()
        logger.info("已删除 users 表遗留 batch_* 列 (%d 个)", len(drops))
```

在 `init_schema` 末尾调用 `await self._migrate_drop_batch_columns()`。

- [ ] **Step 5: 跑测试**

```bash
uv run pytest tests/unit/test_storage_sqlite_user.py -v
```

Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add src/pulsemq/storage/sqlite_user.py \
        tests/unit/test_storage_sqlite_user.py
git commit -m "feat(storage): users 表移除 batch_* 列, 启动自动迁移

启动时 PRAGMA table_info 检测旧列,自动 ALTER TABLE ... DROP COLUMN。
SQLite < 3.35 会因不支持 DROP COLUMN 抛 RuntimeError (fail-fast)。

🤖 Generated with [Claude Code](https://claude.com/claude-code)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 7: 删除 `PermissionService.get/set_batch_config`

**Files:**
- Modify: `src/pulsemq/auth/permission.py`
- Test: `tests/unit/test_auth_permission.py`

- [ ] **Step 1: 添加失败测试 — 方法应不存在**

打开 `tests/unit/test_auth_permission.py`,在文件末尾添加:

```python
def test_permission_service_no_batch_config_methods():
    """PermissionService 不应再有 get_batch_config / set_batch_config。"""
    from pulsemq.auth.permission import PermissionService
    assert not hasattr(PermissionService, "get_batch_config"), \
        "PermissionService.get_batch_config 应当已被移除"
    assert not hasattr(PermissionService, "set_batch_config"), \
        "PermissionService.set_batch_config 应当已被移除"
```

- [ ] **Step 2: 运行测试,确认 FAIL**

```bash
uv run pytest tests/unit/test_auth_permission.py::test_permission_service_no_batch_config_methods -v
```

Expected: FAIL

- [ ] **Step 3: 删 `get_batch_config` / `set_batch_config`**

打开 `src/pulsemq/auth/permission.py`,找到 `get_batch_config` (line 153 附近) 与 `set_batch_config` (line 174 附近),整段删除。同时删除相关注释 (line 100 `_user_repo` 注释中的 batch 部分,如有)。

- [ ] **Step 4: 跑测试**

```bash
uv run pytest tests/unit/test_auth_permission.py -v
```

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/pulsemq/auth/permission.py tests/unit/test_auth_permission.py
git commit -m "feat(auth): 删除 PermissionService.get/set_batch_config

🤖 Generated with [Claude Code](https://claude.com/claude-code)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 8: 删除 Admin REST 端点与 Web UI Tab

**Files:**
- Modify: `src/pulsemq/monitoring/admin_server.py`
- Modify: `src/pulsemq/monitoring/admin_backend.py`
- Modify: `src/pulsemq/monitoring/web_ui.py`
- Test: `tests/unit/test_admin_server.py`
- Test: `tests/unit/test_web_ui.py`

- [ ] **Step 1: 添加失败测试 — REST 端点应 404**

打开 `tests/unit/test_admin_server.py`,在文件末尾添加:

```python
@pytest.mark.asyncio
async def test_batch_config_endpoints_return_404(admin_server):
    """GET/PUT /api/v1/users/{user_id}/batch_config 应 404 (路由已删)。"""
    # 视 admin_server fixture 实现而定,以下是示例
    client = admin_server.test_client()  # 或类似方法
    resp = await client.get("/api/v1/users/1/batch_config")
    assert resp.status == 404
    resp = await client.put("/api/v1/users/1/batch_config", json={})
    assert resp.status == 404
```

(具体 client API 视 admin_server 测试 helper 而定。)

- [ ] **Step 2: 运行测试,确认 FAIL (端点目前存在并返回 200)**

- [ ] **Step 3: 删 admin_server.py 路由**

打开 `src/pulsemq/monitoring/admin_server.py`,grep `batch_config`,删除:
- 路由注册 (`@self._app.route(...)` 或类似)
- handler 方法
- OpenAPI 注释

- [ ] **Step 4: 删 admin_backend.py 方法**

打开 `src/pulsemq/monitoring/admin_backend.py`,删除 `get_batch_config` / `set_batch_config` 委托方法。

- [ ] **Step 5: 删 web_ui.py 批量配置 Tab**

打开 `src/pulsemq/monitoring/web_ui.py`:

1. **HTML**: 删 `<button data-tab="batch">批量配置</button>` 与 `<div id="tab-batch">...</div>` 整块。
2. **JS**: 删 `loadBatchConfig()` / `populateBatchSelect()` / `bs-size` / `bs-interval` / `bs-wait` 相关代码。
3. **概览页**: 删 `r.engine_batch_size` 引用行。

- [ ] **Step 6: 删 test_admin_server.py 中 batch_config 端点测试**

搜索 `batch_config` 关键字,删除 2-4 个测试函数。

- [ ] **Step 7: 删 test_web_ui.py 中 batch_config 相关断言**

搜索 `batch` 关键字,删除断言。

- [ ] **Step 8: 跑测试**

```bash
uv run pytest tests/unit/test_admin_server.py tests/unit/test_web_ui.py -v
```

Expected: PASS

- [ ] **Step 9: Commit**

```bash
git add src/pulsemq/monitoring/admin_server.py \
        src/pulsemq/monitoring/admin_backend.py \
        src/pulsemq/monitoring/web_ui.py \
        tests/unit/test_admin_server.py \
        tests/unit/test_web_ui.py
git commit -m "feat(monitoring): 删除 batch_config REST 端点与 Web UI Tab

GET/PUT /api/v1/users/{id}/batch_config 不再注册。
Web UI 批量配置 Tab 与相关 JS 删除。
概览页移除 engine_batch_size 字段。

🤖 Generated with [Claude Code](https://claude.com/claude-code)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 9: 删除客户端 Batcher 集成

**Files:**
- Modify: `src/pulsemq/client/async_client.py`
- Test: `tests/unit/test_client_subscribe.py` (新增断言)

- [ ] **Step 1: 添加失败测试 — `PulseClient(batch_size=...)` 应 TypeError**

打开 `tests/unit/test_client_subscribe.py`,在文件末尾添加:

```python
def test_pulse_client_no_batch_kwargs():
    """PulseClient 构造不应接受 batch_size / batch_interval_ms / batch_max_wait_ms 关键字。"""
    import pytest
    from pulsemq.client.async_client import PulseClient
    with pytest.raises(TypeError):
        PulseClient("tcp://localhost:5555", batch_size=10)
    with pytest.raises(TypeError):
        PulseClient("tcp://localhost:5555", batch_interval_ms=10.0)
    with pytest.raises(TypeError):
        PulseClient("tcp://localhost:5555", batch_max_wait_ms=50.0)
```

- [ ] **Step 2: 运行测试,确认 FAIL**

```bash
uv run pytest tests/unit/test_client_subscribe.py::test_pulse_client_no_batch_kwargs -v
```

Expected: FAIL

- [ ] **Step 3: 删 `async_client.py` 中的 Batcher 集成**

打开 `src/pulsemq/client/async_client.py`:

1. **import** (line 19): 删 `from pulsemq.client.batcher import Batcher`
2. **构造参数** (line 108-110): 删 `batch_size` / `batch_interval_ms` / `batch_max_wait_ms`
3. **字段** (line 123-127): 删 `_batch_size` / `_batch_interval_ms` / `_batch_max_wait_ms` / `_batcher` 及其注释
4. **`connect()` 中** (line 162-173): 删整个 `Batcher` 创建块
5. **`disconnect()` 中** (line 182-188): 删 `Batcher.close()` 块
6. **`publish()` 中** (line 257-264): 删 "批模式入 Batcher" 分支,保留 `if self._batcher is not None and self._batch_size > 1: ... return` 整段
7. **`_batcher_send` 方法** (line 272-308): 整方法删除
8. **docstring** (line 244-245): 删 batch 相关说明

- [ ] **Step 4: 跑测试**

```bash
uv run pytest tests/unit/test_client_subscribe.py -v
```

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/pulsemq/client/async_client.py tests/unit/test_client_subscribe.py
git commit -m "feat(client): PulseClient 移除 Batcher 集成与 batch_size 等关键字参数

publish() 唯一路径: 立即发送单条 PUB。
旧 client 传 batch_size=... 抛 TypeError。

🤖 Generated with [Claude Code](https://claude.com/claude-code)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 10: 删除 Batcher 文件 (源码 + 3 个测试)

**Files:**
- Delete: `src/pulsemq/client/batcher.py`
- Delete: `tests/unit/test_client_batcher.py`
- Delete: `tests/unit/test_batch_msg_type.py`
- Delete: `tests/integration/test_batcher_e2e.py`

- [ ] **Step 1: 删除文件**

```bash
git rm src/pulsemq/client/batcher.py \
      tests/unit/test_client_batcher.py \
      tests/unit/test_batch_msg_type.py \
      tests/integration/test_batcher_e2e.py
```

- [ ] **Step 2: 跑全量单测 + 集成测试,确认无 import 残留**

```bash
uv run pytest tests/unit tests/integration -v
```

Expected: PASS (若有 import 残留,会 ImportError,需回查)

- [ ] **Step 3: Commit**

```bash
git commit -m "chore: 删除 Batcher 实现与 3 个测试文件

- src/pulsemq/client/batcher.py
- tests/unit/test_client_batcher.py
- tests/unit/test_batch_msg_type.py
- tests/integration/test_batcher_e2e.py

🤖 Generated with [Claude Code](https://claude.com/claude-code)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 11: 清理 monitoring 测试中的 `effective_batch_size` 引用

**Files:**
- Modify: `tests/unit/test_monitoring_api.py`
- Modify: `tests/unit/test_monitoring_realtime.py`
- Modify: `tests/unit/test_monitoring_minute.py`

- [ ] **Step 1: grep 定位**

```bash
grep -n "effective_batch_size\|engine_batch_size" tests/unit/test_monitoring_*.py
```

- [ ] **Step 2: 删除相关断言**

每个文件可能有 1-3 处断言 `r["effective_batch_size"]` / `metrics.effective_batch_size`,删除。

- [ ] **Step 3: 跑测试**

```bash
uv run pytest tests/unit/test_monitoring_api.py \
              tests/unit/test_monitoring_realtime.py \
              tests/unit/test_monitoring_minute.py \
              tests/integration/test_monitoring_e2e.py -v
```

Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add tests/unit/test_monitoring_api.py \
        tests/unit/test_monitoring_realtime.py \
        tests/unit/test_monitoring_minute.py
git commit -m "test(monitoring): 移除 effective_batch_size 指标断言

EngineMetrics.effective_batch_size 字段已删除。

🤖 Generated with [Claude Code](https://claude.com/claude-code)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 12: 清理压测脚本

**Files:**
- Delete: `scripts/bench_100k_with_batcher.py`
- Modify: `scripts/bench_market_data.py`
- Modify: `scripts/bench_1m.py`
- Modify: `scripts/bench_concurrent.py` (如有)
- Modify: `scripts/bench_soak.py` (如有)

- [ ] **Step 1: 删除 bench_100k_with_batcher.py**

```bash
git rm scripts/bench_100k_with_batcher.py
```

- [ ] **Step 2: 改 bench_market_data.py**

打开 `scripts/bench_market_data.py`,grep `BATCH_SIZE` / `BATCH_INTERVAL_MS` / `BATCH_MAX_WAIT_MS` / `batch_size=`:

- 删 `BATCH_SIZE = 10` 等常量
- 删 `PulseClient(..., batch_size=BATCH_SIZE, batch_interval_ms=..., batch_max_wait_ms=...)` 传参,只保留 `address` / `api_key` 等

- [ ] **Step 3: 改 bench_1m.py (同上)**

- [ ] **Step 4: 改 bench_concurrent.py / bench_soak.py**

```bash
grep -n "batch_size\|BATCH_" scripts/bench_concurrent.py scripts/bench_soak.py
```

如有引用,删。

- [ ] **Step 5: 跑 smoke**

```bash
uv run python -c "import sys; sys.path.insert(0, 'src'); from pulsemq.client.async_client import PulseClient; print(PulseClient.__init__.__doc__[:200])"
```

Expected: 不应提到 batch_size。

- [ ] **Step 6: Commit**

```bash
git add scripts/
git commit -m "chore(scripts): 移除 bench_100k_with_batcher.py 与其他脚本中 batch_size 传参

🤖 Generated with [Claude Code](https://claude.com/claude-code)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 13: 文档更新 (banner + 已知问题 + README + API 参考)

**Files:**
- Modify: `docs/known-issues.md`
- Modify: `docs/superpowers/specs/2026-06-07-pulsemq-v1-optimization.md`
- Modify: `docs/perf-100k-batched-data.md`
- Modify: `docs/perf-100k-batched-report.md`
- Modify: `docs/perf-comparison.md`
- Modify: `docs/api-reference.md`
- Modify: `README.md`

- [ ] **Step 1: 删 `docs/known-issues.md` I34**

打开 `docs/known-issues.md`,找到 line 77 I34 条目 (整段),删除。

- [ ] **Step 2: 给 v1.0 optimization spec 加 banner**

打开 `docs/superpowers/specs/2026-06-07-pulsemq-v1-optimization.md`,在文首 (line 1 之后) 加:

```markdown
> ⚠️ **部分撤销**: 客户端 Batcher / BATCH 协议 / `_handle_batch` / `_adapt_batch_size` 已被
> [2026-06-07-remove-publish-batcher](./2026-06-07-remove-publish-batcher.md) 移除。
> 本文档仅作历史档案,不要按本文档第 1.2、1.3 节实施。
```

- [ ] **Step 3: 给三个 perf 文档加 banner**

`docs/perf-100k-batched-data.md` / `docs/perf-100k-batched-report.md` / `docs/perf-comparison.md` 文首各加:

```markdown
> ⚠️ **历史数据**: 本文件基于 v1.0 Batcher 实现 (size=10, interval=10ms)。
> Batcher 策略已在 [2026-06-07-remove-publish-batcher](../superpowers/specs/2026-06-07-remove-publish-batcher.md) 中移除。
> 数据仅作历史对比参考。
```

- [ ] **Step 4: 改 `docs/api-reference.md`**

grep `batcher` / `BATCH` / `batch_size`,删除相关章节 (大概 30-50 行)。

- [ ] **Step 5: 改 `README.md`**

```bash
grep -n "batcher\|BATCH\|batch_size" README.md
```

如有,删除或改写。

- [ ] **Step 6: Commit**

```bash
git add docs/
git commit -m "docs: 标注 batcher 策略已移除, 清理 README/API 引用

- docs/known-issues.md 删 I34
- v1.0 optimization spec 加 banner 指向本计划
- 三个 perf 文档加 banner 标注历史数据
- docs/api-reference.md 删 Batcher 章节
- README.md 清理 batcher 引用

🤖 Generated with [Claude Code](https://claude.com/claude-code)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 14: 全量回归 + 端到端 + 迁移验证

**Files:** (无源码变更,纯验证)

- [ ] **Step 1: 跑全量单测 + 集成测试**

```bash
uv run pytest tests/unit tests/integration -v
```

Expected: PASS (无任何 batcher 相关测试残留,无 BATCH 引用,无 import 失败)

- [ ] **Step 2: 跑端到端**

```bash
uv run python scripts/test_e2e_all.py
```

Expected: 16/16 通过

- [ ] **Step 3: 跑基线压测 (直发模式)**

```bash
uv run python scripts/bench_baseline.py --port 16900
```

Expected: 16 组合跑通,数据与 v0.6 一致或更优

- [ ] **Step 4: 验证旧库迁移**

```bash
# 创建旧库
sqlite3 test_legacy.db <<EOF
CREATE TABLE users (
    user_id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT UNIQUE NOT NULL,
    api_key_hash TEXT NOT NULL,
    batch_size INTEGER DEFAULT 1,
    batch_interval_ms INTEGER DEFAULT 10,
    batch_max_wait_ms INTEGER DEFAULT 50,
    is_admin INTEGER DEFAULT 0,
    is_active INTEGER DEFAULT 1,
    created_at REAL NOT NULL
);
EOF

# 启动 server,指向旧库
PULSEMQ_DB_URL=sqlite:///test_legacy.db uv run python scripts/test_server_runner.py --port 16901 &
SERVER_PID=$!
sleep 3

# 验证列已删
sqlite3 test_legacy.db "PRAGMA table_info(users);" | grep -v batch_
# 应只剩 user_id, username, api_key_hash, is_admin, is_active, created_at

# 清理
kill $SERVER_PID
rm test_legacy.db
```

Expected: 列自动删除,无报错

- [ ] **Step 5: 最终 commit (如有遗漏修改)**

如有 Step 1-4 中发现的小修补,提交一个 `chore: 回归发现的小修补` commit。

- [ ] **Step 6: 总结报告**

向用户报告:
- 删除/修改的文件清单
- 测试通过统计
- 旧库迁移验证结果
- 与 v0.6 基线对比 (如有)

---

## Self-Review (写完后检查)

**1. Spec 覆盖检查**:

- [x] 协议层 (Task 1) — 删除 BATCH msg_type + FrameCodec batch payload
- [x] 服务端 handlers (Task 2) — 删除 _handle_batch
- [x] 服务端 engine (Task 3) — 删除 _adapt_batch_size / _drain_socket / _dispatch_batch, 简化为 _dispatch_one
- [x] config.py (Task 4) — 删除 max_batch_size / drain_timeout_ms
- [x] User 模型 (Task 5) — 删除 batch_* 字段
- [x] SQLite storage (Task 6) — 删除列 + 加 migration
- [x] PermissionService (Task 7) — 删除 get/set_batch_config
- [x] Admin + Web UI (Task 8) — 删除 REST 端点 + Tab
- [x] 客户端 (Task 9) — 删除 Batcher 集成
- [x] 文件删除 (Task 10) — batcher.py + 3 个测试
- [x] monitoring 测试 (Task 11) — effective_batch_size 清理
- [x] 压测脚本 (Task 12)
- [x] 文档 (Task 13) — banner + I34 + API ref + README
- [x] 回归 (Task 14)

**2. Placeholder scan**: 已修复 (Step 1-3 中的测试代码完整,无 "TBD" / "类似 Task N" 引用)

**3. Type 一致性**:
- `_dispatch_one(self, frames: list[bytes])` 在 Task 3 Step 3 中定义,后续 Task 14 调用时一致
- `_migrate_drop_batch_columns(self)` 在 Task 6 Step 4 中定义,async 方法
- 删 `MsgType.BATCH` 在 Task 1 Step 4,Task 2 Step 3 中 `elif ctx.msg_type == MsgType.BATCH:` 引用相应删除

**潜在风险**:
- Task 6 的 migration 测试依赖 `SqliteUserStore` 实际 API (init_schema 或 connect),若 API 名不同需在实施时调整
- Task 8 的 admin_server fixture 名称 / test_client() 方法视实际项目而定

---

## 验收清单

- [ ] 14 个 Task 全部完成,每 Task 单独 commit
- [ ] `uv run pytest tests/unit tests/integration` 全绿
- [ ] `uv run python scripts/test_e2e_all.py` 16/16 通过
- [ ] 旧库迁移测试: 旧库启动后 `batch_*` 列自动消失
- [ ] `docs/known-issues.md` 中 I34 已删
- [ ] 三个 perf 文档 + v1.0 optimization spec banner 已加
- [ ] 现有 16 组合基线 (`bench_baseline.py`) 与 v0.6 一致或更优
