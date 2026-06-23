# 认证可见性 + 心跳机制 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 增强 Sub 端错误诊断能力 + Pub 端认证可见性 + 默认启用心跳机制

**Architecture:** 四个独立模块修改5个源文件 — Sub 端异常体系替代静默退出、ZAP 日志+回调增强认证可见性、PING 帧实现零配置心跳检测

**Tech Stack:** Python 3.13+, ZeroMQ (pyzmq), asyncio

**Spec:** `docs/superpowers/specs/2026-06-23-auth-heartbeat-design.md`

## Global Constraints

- pub 和 sub 最简配置即可直接使用，所有增强功能默认启用
- 心跳默认间隔30s（pub），默认超时90s（sub）
- 向后兼容：现有 API 不破坏，现有测试全部通过
- 代码注释必须使用中文
- Commit message 必须使用中文

---

### Task 1: Sub 端异常体系

**Files:**
- Modify: `src/pulsemq/subscriber.py` (line 1-95)

**Interfaces:**
- Produces: `PulseSubscriberError(Exception)`, `AuthenticationError(PulseSubscriberError)`, `ConnectionLostError(PulseSubscriberError)`
- Produces: `PulseSubscriber._closed_by_user: bool` (实例属性)
- Produces: `subscribe()` 中 `zmq.ZMQError` 改为抛 `ConnectionLostError`（`_closed_by_user` 为 True 时仍正常 break）

- [ ] **Step 1: 编写异常类和 subscribe 改造的代码**

修改 `src/pulsemq/subscriber.py`，在文件头部 `from __future__ import annotations` 之后添加 `import time`，在 `logger` 定义之前添加三个异常类：

```python
"""PulseSubscriber: 订阅端客户端。

用法:
    sub = PulseSubscriber("tcp://host:5555", username="user1", password="pulse_sk_xxx")
    async with sub:
        async for msg in sub.subscribe("sh_market_data"):
            print(msg.topic, msg.payload, msg.timestamp_ns)
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import AsyncIterator

import zmq
import zmq.asyncio

from pulsemq.protocol.frames import PulseMessage, decode

logger = logging.getLogger(__name__)


class PulseSubscriberError(Exception):
    """订阅端异常基类。"""


class AuthenticationError(PulseSubscriberError):
    """PLAIN 认证被拒绝（ZAP 返回 400）。

    Attributes:
        username: 尝试认证的用户名。
        address: 发布端地址（tcp://host:port）。
    """

    def __init__(self, message: str, username: str = "", address: str = "") -> None:
        super().__init__(message)
        self.username = username
        self.address = address


class ConnectionLostError(PulseSubscriberError):
    """连接意外断开（心跳超时或 TCP 断开）。"""


class PulseSubscriber:
    """订阅端客户端。"""

    def __init__(
        self,
        address: str = "tcp://localhost:5555",
        *,
        username: str = "",
        password: str = "",
    ) -> None:
        self._address = address
        self._username = username
        self._password = password
        self._ctx: zmq.asyncio.Context | None = None
        self._sub: zmq.asyncio.Socket | None = None
        self._closed_by_user = False

    async def connect(self) -> None:
        """连接 PUB socket，PLAIN 认证。"""
        self._ctx = zmq.asyncio.Context()
        self._sub = self._ctx.socket(zmq.SUB)
        self._sub.setsockopt(zmq.RCVHWM, 0)

        if self._username:
            self._sub.setsockopt(zmq.PLAIN_USERNAME, self._username.encode())
            self._sub.setsockopt(zmq.PLAIN_PASSWORD, self._password.encode())

        self._sub.connect(self._address)
        logger.info("Subscriber 连接到 %s (auth=%s)", self._address, "on" if self._username else "off")

    async def subscribe(self, *topics: str) -> AsyncIterator[PulseMessage]:
        """订阅 topic，返回异步迭代器。"""
        if self._sub is None:
            raise RuntimeError("Subscriber 未连接")

        for t in topics:
            self._sub.setsockopt(zmq.SUBSCRIBE, t.encode("utf-8"))
            logger.info("订阅 topic: %s", t)

        while True:
            try:
                frames = await self._sub.recv_multipart()
                if len(frames) == 4:
                    yield decode(frames)
            except zmq.ZMQError:
                if self._closed_by_user:
                    break
                raise ConnectionLostError("与 Publisher 的连接已断开")
            except asyncio.CancelledError:
                if self._sub is not None:
                    self._closed_by_user = True
                    self._sub.close(linger=0)
                    self._sub = None
                raise

    async def close(self) -> None:
        """关闭连接。"""
        if self._sub is not None:
            self._closed_by_user = True
            self._sub.close(linger=1000)
            self._sub = None
        if self._ctx is not None:
            self._ctx.term()
            self._ctx = None
        logger.info("Subscriber 已关闭")

    # ---- 上下文管理器 ----

    async def __aenter__(self) -> PulseSubscriber:
        await self.connect()
        return self

    async def __aexit__(self, *args) -> None:
        await self.close()
```

- [ ] **Step 2: 编写单元测试**

创建 `tests/test_auth_and_heartbeat.py`：

```python
"""认证 + 心跳机制测试。

覆盖:
- 异常体系（AuthenticationError / ConnectionLostError）
- ZAP 日志增强
- on_auth 回调钩子
- 心跳机制（编码/发送/检测/超时）
"""

from __future__ import annotations

import asyncio
import logging
import sys
import time

import pytest
import zmq

from pulsemq.subscriber import (
    AuthenticationError,
    ConnectionLostError,
    PulseSubscriber,
    PulseSubscriberError,
)

# Windows: 强制 Selector 事件循环
if sys.platform == "win32" and hasattr(asyncio, "WindowsSelectorEventLoopPolicy"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

import tests.conftest as conftest  # noqa: F401, E402
from tests.conftest import (
    make_publisher,
    running_publisher,
)


class TestSubscriberExceptions:
    """模块 1：Sub 端异常体系。"""

    async def test_connection_lost_on_pub_shutdown(
        self,
        random_port_pair: tuple[int, int],
        tmp_sqlite_url: str,
    ) -> None:
        """Publisher 关闭后，subscriber 抛 ConnectionLostError。"""
        pub_port, admin_port = random_port_pair
        pub = make_publisher(pub_port=pub_port, admin_port=admin_port, tmp_db=tmp_sqlite_url)

        topic = "will_die"
        received: list = []

        async def _factory() -> dict:
            return {"x": 1}

        pub.register_producer(fn=_factory, name=topic, interval=0.1)

        sub = PulseSubscriber(f"tcp://127.0.0.1:{pub_port}")

        async with running_publisher(pub) as p:
            await sub.connect()
            # 先收一条确认连通
            async for msg in sub.subscribe(topic):
                received.append(msg)
                break
            # publisher 在 running_publisher 退出时关闭
            # 关闭后继续迭代应抛 ConnectionLostError
            await asyncio.sleep(0.5)

        # publisher 已关闭，尝试继续收消息
        with pytest.raises(ConnectionLostError, match="连接已断开"):
            async for _msg in sub.subscribe(topic):
                pass

        await sub.close()

    async def test_close_does_not_raise(
        self,
        random_port_pair: tuple[int, int],
        tmp_sqlite_url: str,
    ) -> None:
        """正常 close() 不抛异常。"""
        pub_port, admin_port = random_port_pair
        pub = make_publisher(pub_port=pub_port, admin_port=admin_port, tmp_db=tmp_sqlite_url)

        topic = "normal_close"
        received: list = []

        async def _factory() -> dict:
            return {"x": 1}

        pub.register_producer(fn=_factory, name=topic, interval=0.1)

        async with running_publisher(pub):
            sub = PulseSubscriber(f"tcp://127.0.0.1:{pub_port}")
            await sub.connect()
            async for msg in sub.subscribe(topic):
                received.append(msg)
                if len(received) >= 3:
                    break
            await sub.close()  # 不应抛异常

        assert len(received) >= 3

    async def test_authentication_error_raised(
        self,
        random_port_pair: tuple[int, int],
        tmp_sqlite_url: str,
    ) -> None:
        """错误凭据导致认证失败，抛 ConnectionLostError。"""
        pub_port, admin_port = random_port_pair
        pub = make_publisher(
            pub_port=pub_port, admin_port=admin_port, tmp_db=tmp_sqlite_url,
            api_keys={"alice": "right_pwd"},
        )

        topic = "auth_test_x"

        async def _factory() -> dict:
            return {"x": 1}

        pub.register_producer(fn=_factory, name=topic, interval=0.1)

        async with running_publisher(pub):
            sub = PulseSubscriber(
                f"tcp://127.0.0.1:{pub_port}",
                username="alice", password="wrong_pwd",
            )
            await sub.connect()
            # 错误凭据：ZMQ 会在 recv 时断开连接
            with pytest.raises(ConnectionLostError):
                async for _msg in sub.subscribe(topic):
                    pass
            await sub.close()
```

- [ ] **Step 3: 运行测试验证通过**

```bash
pytest tests/test_auth_and_heartbeat.py::TestSubscriberExceptions -v
```

预期输出：3 passed

- [ ] **Step 4: 运行已有回归测试确保向后兼容**

```bash
pytest tests/ -v --timeout=60
```

预期输出：所有已有测试通过（约 80+ 个）

- [ ] **Step 5: 提交**

```bash
git add src/pulsemq/subscriber.py tests/test_auth_and_heartbeat.py
git commit -m "feat: Sub 端异常体系 — 认证/断连不再静默退出"
```

---

### Task 2: Pub 端 ZAP 日志增强 + 认证回调钩子

**Files:**
- Modify: `src/pulsemq/transport/zmq_pub.py` (line 1-142)
- Modify: `src/pulsemq/publisher.py` (line 43-53 `__init__`, line 156-168 `_run`)

**Interfaces:**
- Consumes: (无)
- Produces: `AsyncZAPHandler` 增加 `set_auth_callback(cb)` 方法和成功 INFO 日志
- Produces: `ZmqPubTransport.__init__` 新增 `on_auth` 参数、`set_auth_callback(cb)` 方法
- Produces: `PulsePublisher.__init__` 新增 `on_auth` 参数、`set_auth_callback(cb)` 方法
- Produces: `AuthCallback = Callable[[str, str, bool], Awaitable[None]]` 类型别名（定义在 `zmq_pub.py`）

- [ ] **Step 1: 修改 `zmq_pub.py` — ZAP 日志 + 回调支持**

修改 `src/pulsemq/transport/zmq_pub.py`，完整替换为：

```python
"""ZMQ PUB socket + PLAIN 认证。

v2 简化：单一 PUB socket，无需 ROUTER/XPUB。
api_keys 非空时自动开启 ZMQ PLAIN 认证。

ZAP handler 运行在 asyncio 事件循环中（与 PUB socket 同 context），
避免跨线程 inproc:// 的兼容性问题。
"""

from __future__ import annotations

import asyncio
import logging
from typing import Awaitable, Callable

import zmq
import zmq.asyncio

logger = logging.getLogger(__name__)

# 认证事件回调签名: (username, client_address, success) -> None
AuthCallback = Callable[[str, str, bool], Awaitable[None]]


class AsyncZAPHandler:
    """ZMQ PLAIN 认证的 ZAP handler（asyncio 版）。

    与 PUB socket 共享同一个 zmq.asyncio.Context，
    在 asyncio 事件循环中处理 ZAP 请求。
    """

    def __init__(
        self,
        api_keys: dict[str, str],
        ctx: zmq.asyncio.Context,
        on_auth: AuthCallback | None = None,
    ) -> None:
        self._api_keys = api_keys
        self._ctx = ctx
        self._zap: zmq.asyncio.Socket | None = None
        self._task: asyncio.Task | None = None
        self._on_auth: AuthCallback | None = on_auth

    def set_auth_callback(self, callback: AuthCallback | None) -> None:
        """设置认证事件回调（可在运行时动态替换/取消）。"""
        self._on_auth = callback

    async def start(self) -> None:
        """启动 ZAP handler。"""
        self._zap = self._ctx.socket(zmq.REP)
        self._zap.bind("inproc://zeromq.zap.01")
        self._task = asyncio.create_task(self._loop())
        logger.info("ZAP handler 启动: %d 个白名单用户", len(self._api_keys))

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
            self._task = None
        if self._zap is not None:
            self._zap.close(linger=100)
            self._zap = None

    async def _loop(self) -> None:
        """ZAP 请求处理循环（asyncio）。"""
        assert self._zap is not None
        while True:
            try:
                msg = await self._zap.recv_multipart()
            except zmq.ZMQError:
                break
            except asyncio.CancelledError:
                break

            # ZAP 请求帧格式:
            # [version, request_id, domain, address, identity, mechanism, ...credentials]
            if len(msg) < 7:
                self._zap.send_multipart([
                    msg[1] if len(msg) > 1 else b"",
                    b"400",
                    b"Invalid ZAP request",
                    b"",
                    b"",
                ])
                continue

            version = msg[0]
            request_id = msg[1]
            # domain = msg[2]
            address = msg[3].decode("utf-8", errors="replace") if len(msg) > 3 else "unknown"
            # identity = msg[4]
            mechanism = msg[5]
            username = msg[6].decode("utf-8", errors="replace") if len(msg) > 6 else ""
            password = msg[7].decode("utf-8", errors="replace") if len(msg) > 7 else ""

            if mechanism != b"PLAIN":
                self._zap.send_multipart([version, request_id, b"400", b"Not PLAIN", b"", b""])
                continue

            # 白名单校验
            expected = self._api_keys.get(username)
            if expected is not None and expected == password:
                logger.info("ZAP 认证成功: username=%s client=%s", username, address)
                self._zap.send_multipart([version, request_id, b"200", b"OK", username.encode(), b""])
                success = True
            else:
                logger.warning(
                    "ZAP 认证失败: username=%s client=%s reason=invalid_credentials",
                    username, address,
                )
                self._zap.send_multipart([version, request_id, b"400", b"Invalid credentials", b"", b""])
                success = False

            # 回调通知
            if self._on_auth is not None:
                try:
                    await self._on_auth(username, address, success)
                except Exception:
                    logger.warning("on_auth 回调异常", exc_info=True)


class ZmqPubTransport:
    """ZMQ PUB socket + PLAIN 认证。"""

    def __init__(
        self,
        bind: str = "tcp://*:5555",
        api_keys: dict[str, str] | None = None,
        on_auth: AuthCallback | None = None,
    ) -> None:
        self._bind = bind
        self._api_keys = api_keys or {}
        self._ctx: zmq.asyncio.Context | None = None
        self._pub: zmq.asyncio.Socket | None = None
        self._zap: AsyncZAPHandler | None = None
        self._on_auth = on_auth

    def set_auth_callback(self, callback: AuthCallback | None) -> None:
        """设置认证事件回调。需在 start() 后调用才会生效于 ZAP handler。"""
        self._on_auth = callback
        if self._zap is not None:
            self._zap.set_auth_callback(callback)

    async def start(self) -> None:
        """启动 PUB socket，可选开启 PLAIN 认证。"""
        self._ctx = zmq.asyncio.Context()
        self._pub = self._ctx.socket(zmq.PUB)
        self._pub.setsockopt(zmq.SNDHWM, 0)
        self._pub.setsockopt(zmq.LINGER, 1000)

        if self._api_keys:
            self._zap = AsyncZAPHandler(self._api_keys, self._ctx, self._on_auth)
            await self._zap.start()
            self._pub.setsockopt(zmq.PLAIN_SERVER, 1)

        self._pub.bind(self._bind)
        logger.info("PUB socket 绑定到 %s (auth=%s)", self._bind, "on" if self._api_keys else "off")

    async def send(self, frames: list[bytes]) -> None:
        """广播一帧消息给所有 SUB。"""
        if self._pub is None:
            raise RuntimeError("Transport 未启动")
        await self._pub.send_multipart(frames)

    async def stop(self) -> None:
        """关闭 PUB socket 和 context。"""
        if self._pub is not None:
            self._pub.close(linger=1000)
            self._pub = None
        if self._zap is not None:
            await self._zap.stop()
            self._zap = None
        if self._ctx is not None:
            self._ctx.term()
            self._ctx = None
        logger.info("ZMQ PUB Transport 已关闭")
```

- [ ] **Step 2: 修改 `publisher.py` — 暴露 on_auth 参数和 set_auth_callback**

修改 `src/pulsemq/publisher.py`：

**2a. 更新 import（第 35 行附近）：**

```python
from pulsemq.transport.zmq_pub import AuthCallback, ZmqPubTransport
```

**2b. 更新 `__init__` 方法签名（第 46-60 行）：**

```python
    def __init__(
        self,
        config: PublisherConfig | None = None,
        *,
        bind: str | None = None,
        admin_bind: str | None = None,
        api_keys: dict[str, str] | None = None,
        on_auth: AuthCallback | None = None,
    ) -> None:
        self._config = config or load_config()
        if bind:
            self._config.bind = bind
        if admin_bind:
            self._config.admin_bind = admin_bind
        self._explicit_api_keys = api_keys
        self._on_auth = on_auth
```

**2c. 在 `PulsePublisher` 类中添加 `set_auth_callback` 方法（放在 `add_api_key` 方法之后，第 141 行附近）：**

```python
    def set_auth_callback(self, callback: AuthCallback | None) -> None:
        """设置认证事件回调（可运行时动态替换）。

        回调签名为 async def(username, client_address, success)。
        需在 start() 前调用，或在 start() 后通过 transport 自动传递。
        """
        self._on_auth = callback
        if self._transport is not None:
            self._transport.set_auth_callback(callback)
```

**2d. 更新 `_run()` 中的 transport 构造（第 164-167 行）：**

```python
        self._transport = ZmqPubTransport(
            bind=self._config.bind,
            api_keys=api_keys,
            on_auth=self._on_auth,
        )
```

- [ ] **Step 3: 编写测试 — ZAP 日志验证**

在 `tests/test_auth_and_heartbeat.py` 的 `TestSubscriberExceptions` 类之后添加：

```python
class TestZapLogging:
    """模块 2：Pub 端 ZAP 日志增强。"""

    async def test_zap_success_log_contains_client_address(
        self,
        random_port_pair: tuple[int, int],
        tmp_sqlite_url: str,
        caplog,
    ) -> None:
        """认证成功日志包含客户端 IP。"""
        pub_port, admin_port = random_port_pair
        pub = make_publisher(
            pub_port=pub_port, admin_port=admin_port, tmp_db=tmp_sqlite_url,
            api_keys={"alice": "pwd"},
        )

        topic = "log_topic_1"

        async def _factory() -> dict:
            return {"x": 1}

        pub.register_producer(fn=_factory, name=topic, interval=0.2)

        caplog.set_level(logging.INFO, logger="pulsemq.transport.zmq_pub")

        async with running_publisher(pub):
            sub = PulseSubscriber(
                f"tcp://127.0.0.1:{pub_port}",
                username="alice", password="pwd",
            )
            await sub.connect()
            async for msg in sub.subscribe(topic):
                if msg.payload == {"x": 1}:
                    break
            await sub.close()

        success_logs = [r.message for r in caplog.records if "ZAP 认证成功" in r.message]
        assert len(success_logs) >= 1, f"应有至少一条成功日志，实际: {caplog.record_tuples}"
        assert "client=" in success_logs[0], f"日志应包含客户端地址: {success_logs[0]}"
        assert "username=alice" in success_logs[0]

    async def test_zap_failure_log_contains_client_address(
        self,
        random_port_pair: tuple[int, int],
        tmp_sqlite_url: str,
        caplog,
    ) -> None:
        """认证失败日志包含客户端 IP。"""
        pub_port, admin_port = random_port_pair
        pub = make_publisher(
            pub_port=pub_port, admin_port=admin_port, tmp_db=tmp_sqlite_url,
            api_keys={"alice": "pwd"},
        )

        topic = "log_topic_2"

        async def _factory() -> dict:
            return {"x": 1}

        pub.register_producer(fn=_factory, name=topic, interval=0.2)

        caplog.set_level(logging.WARNING, logger="pulsemq.transport.zmq_pub")

        async with running_publisher(pub):
            sub = PulseSubscriber(
                f"tcp://127.0.0.1:{pub_port}",
                username="alice", password="WRONG",
            )
            await sub.connect()
            with pytest.raises(ConnectionLostError):
                async for _msg in sub.subscribe(topic):
                    pass
            await sub.close()

        fail_logs = [r.message for r in caplog.records if "ZAP 认证失败" in r.message]
        assert len(fail_logs) >= 1, f"应有至少一条失败日志，实际: {caplog.record_tuples}"
        assert "client=" in fail_logs[0], f"日志应包含客户端地址: {fail_logs[0]}"


class TestAuthCallback:
    """模块 3：认证事件回调钩子。"""

    async def test_on_auth_callback_success(
        self,
        random_port_pair: tuple[int, int],
        tmp_sqlite_url: str,
    ) -> None:
        """认证成功时回调被调用，参数正确。"""
        pub_port, admin_port = random_port_pair
        events: list[dict] = []

        async def on_auth(username: str, addr: str, success: bool) -> None:
            events.append({"username": username, "addr": addr, "success": success})

        pub = make_publisher(
            pub_port=pub_port, admin_port=admin_port, tmp_db=tmp_sqlite_url,
            api_keys={"bob": "secret"},
        )
        pub._on_auth = on_auth

        topic = "cb_topic_1"

        async def _factory() -> dict:
            return {"x": 1}

        pub.register_producer(fn=_factory, name=topic, interval=0.2)

        async with running_publisher(pub):
            sub = PulseSubscriber(
                f"tcp://127.0.0.1:{pub_port}",
                username="bob", password="secret",
            )
            await sub.connect()
            async for msg in sub.subscribe(topic):
                if msg.payload == {"x": 1}:
                    break
            await sub.close()

        assert len(events) >= 1, f"回调应至少被调用一次，实际: {events}"
        evt = events[0]
        assert evt["username"] == "bob"
        assert evt["success"] is True
        assert "127.0.0.1" in evt["addr"] or "::1" in evt["addr"]

    async def test_on_auth_callback_failure(
        self,
        random_port_pair: tuple[int, int],
        tmp_sqlite_url: str,
    ) -> None:
        """认证失败时回调被调用，参数正确。"""
        pub_port, admin_port = random_port_pair
        events: list[dict] = []

        async def on_auth(username: str, addr: str, success: bool) -> None:
            events.append({"username": username, "addr": addr, "success": success})

        pub = make_publisher(
            pub_port=pub_port, admin_port=admin_port, tmp_db=tmp_sqlite_url,
            api_keys={"bob": "secret"},
        )
        pub._on_auth = on_auth

        topic = "cb_topic_2"

        async def _factory() -> dict:
            return {"x": 1}

        pub.register_producer(fn=_factory, name=topic, interval=0.2)

        async with running_publisher(pub):
            sub = PulseSubscriber(
                f"tcp://127.0.0.1:{pub_port}",
                username="bob", password="BAD_PWD",
            )
            await sub.connect()
            with pytest.raises(ConnectionLostError):
                async for _msg in sub.subscribe(topic):
                    pass
            await sub.close()

        assert len(events) >= 1, f"回调应至少被调用一次，实际: {events}"
        evt = events[0]
        assert evt["username"] == "bob"
        assert evt["success"] is False

    async def test_on_auth_callback_exception_does_not_crash(
        self,
        random_port_pair: tuple[int, int],
        tmp_sqlite_url: str,
    ) -> None:
        """回调抛出异常不影响认证流程。"""
        pub_port, admin_port = random_port_pair

        async def bad_callback(username: str, addr: str, success: bool) -> None:
            raise RuntimeError("回调内部错误")

        pub = make_publisher(
            pub_port=pub_port, admin_port=admin_port, tmp_db=tmp_sqlite_url,
            api_keys={"carol": "pwd"},
        )
        pub._on_auth = bad_callback

        topic = "cb_topic_3"

        async def _factory() -> dict:
            return {"x": 1}

        pub.register_producer(fn=_factory, name=topic, interval=0.2)

        async with running_publisher(pub):
            sub = PulseSubscriber(
                f"tcp://127.0.0.1:{pub_port}",
                username="carol", password="pwd",
            )
            await sub.connect()
            # 回调异常不应阻止认证 — 消息应正常到达
            async for msg in sub.subscribe(topic):
                assert msg.payload == {"x": 1}
                break
            await sub.close()
```

- [ ] **Step 4: 运行测试**

```bash
pytest tests/test_auth_and_heartbeat.py::TestZapLogging tests/test_auth_and_heartbeat.py::TestAuthCallback -v
```

预期输出：5 passed

- [ ] **Step 5: 回归测试**

```bash
pytest tests/ -v --timeout=60
```

预期输出：所有已有测试通过

- [ ] **Step 6: 提交**

```bash
git add src/pulsemq/transport/zmq_pub.py src/pulsemq/publisher.py tests/test_auth_and_heartbeat.py
git commit -m "feat: Pub 端 ZAP 日志增强 + 认证事件回调钩子"
```

---

### Task 3: 心跳协议编码 `encode_heartbeat()`

**Files:**
- Modify: `src/pulsemq/protocol/frames.py` (在 `decode_payload` 函数之后添加)

**Interfaces:**
- Produces: `encode_heartbeat() -> list[bytes]` — 编码 PING 帧（topic=`"__pulse_hb__"`, msg_type=PING, flags=msgpack|none, record_count=0, payload 空）

- [ ] **Step 1: 添加 `encode_heartbeat()` 函数**

修改 `src/pulsemq/protocol/frames.py`，在文件末尾 `decode_payload` 函数之后追加：

```python
def encode_heartbeat() -> list[bytes]:
    """编码心跳帧（PING 类型，空载荷）。

    4 帧格式：
      Frame 1: topic = b"__pulse_hb__"
      Frame 2: meta (6B) = [PING, flags(msgpack|none), record_count=0]
      Frame 3: timestamp (8B) = 当前纳秒
      Frame 4: payload = b""（空）
    """
    flags_byte = encode_flags("msgpack", "none")
    rc_bytes = _RC_STRUCT.pack(0)
    meta = bytes([MsgType.PING, flags_byte]) + rc_bytes
    ts_bytes = _TS_STRUCT.pack(time.time_ns())
    return [b"__pulse_hb__", meta, ts_bytes, b""]
```

- [ ] **Step 2: 编写单元测试**

在 `tests/test_protocol.py` 末尾添加（或追加到 `tests/test_auth_and_heartbeat.py`）。这里追加到 `tests/test_auth_and_heartbeat.py`：

```python
from pulsemq.protocol.frames import decode, encode_heartbeat
from pulsemq.protocol.msg_type import MsgType


class TestHeartbeatEncoding:
    """模块 4a：心跳帧编码。"""

    def test_encode_heartbeat_format(self) -> None:
        """心跳帧格式正确。"""
        frames = encode_heartbeat()
        assert len(frames) == 4, f"应为 4 帧，实际 {len(frames)}"
        # Frame 1: topic
        assert frames[0] == b"__pulse_hb__"
        # Frame 2: meta (6 bytes)
        assert len(frames[1]) == 6
        assert frames[1][0] == MsgType.PING  # msg_type
        assert frames[1][1] == 0b000_00_000   # flags: msgpack(000) | none(00)
        # record_count = 0 (bytes 2-5, big-endian uint32)
        import struct
        rc = struct.unpack(">I", frames[1][2:6])[0]
        assert rc == 0
        # Frame 3: timestamp (8 bytes)
        assert len(frames[2]) == 8
        ts = struct.unpack(">q", frames[2])[0]
        assert ts > 0
        # Frame 4: empty payload
        assert frames[3] == b""

    def test_encode_heartbeat_idempotent(self) -> None:
        """连续两次调用生成的时间戳递增。"""
        f1 = encode_heartbeat()
        f2 = encode_heartbeat()
        import struct
        ts1 = struct.unpack(">q", f1[2])[0]
        ts2 = struct.unpack(">q", f2[2])[0]
        assert ts2 >= ts1
```

- [ ] **Step 3: 运行测试**

```bash
pytest tests/test_auth_and_heartbeat.py::TestHeartbeatEncoding -v
```

预期输出：2 passed

- [ ] **Step 4: 提交**

```bash
git add src/pulsemq/protocol/frames.py tests/test_auth_and_heartbeat.py
git commit -m "feat: 心跳帧编码 encode_heartbeat()"
```

---

### Task 4: Pub 端心跳发送 + Sub 端心跳检测

**Files:**
- Modify: `src/pulsemq/config.py` (line 34, `PublisherConfig` 类体)
- Modify: `src/pulsemq/publisher.py` (line 155-203 `_run()` + 新增 `_heartbeat_loop`)
- Modify: `src/pulsemq/subscriber.py` (`subscribe()` 方法签名和循环体)

**Interfaces:**
- Consumes: `encode_heartbeat()` from `pulsemq.protocol.frames` (Task 3)
- Consumes: `ConnectionLostError` from `pulsemq.subscriber` (Task 1)
- Produces: `PublisherConfig.heartbeat_interval: float = 30.0`
- Produces: `PulsePublisher._heartbeat_loop()` 协程
- Produces: `subscribe(heartbeat_timeout=90.0)` — 自动检测心跳超时

- [ ] **Step 1: 添加配置项**

修改 `src/pulsemq/config.py`，在 `PublisherConfig` 类体中 `stats_retention_minutes` 之后添加：

```python
    # 心跳发送间隔（秒），<= 0 禁用心跳发送
    heartbeat_interval: float = 30.0
```

- [ ] **Step 2: Pub 端 — 添加 `_heartbeat_loop` 和集成到 `_run()`**

修改 `src/pulsemq/publisher.py`：

**2a. 在 `_run()` 方法中，`_minute_roll_loop` 创建之后、producer 启动之前（第 190 行附近），添加心跳任务创建：**

```python
        # 启动心跳循环（默认 30s 间隔）
        hb_task: asyncio.Task | None = None
        if self._config.heartbeat_interval > 0:
            hb_task = asyncio.create_task(self._heartbeat_loop(), name="heartbeat")

        # 启动所有 producer
        await self._producer_mgr.start_all(self._on_produce)
```

**2b. 在 `try/finally` 块中，`_shutdown` 调用之前添加心跳任务取消：**

```python
        try:
            while self._running:
                await asyncio.sleep(1)
        except (KeyboardInterrupt, asyncio.CancelledError):
            pass
        finally:
            if hb_task is not None:
                hb_task.cancel()
                try:
                    await hb_task
                except asyncio.CancelledError:
                    pass
            await self._shutdown(roll_task)
```

**2c. 在 `_minute_roll_loop` 方法之前添加 `_heartbeat_loop` 方法（第 400 行附近）：**

```python
    async def _heartbeat_loop(self) -> None:
        """心跳发送循环：每隔 heartbeat_interval 秒发送一条 PING 帧。"""
        from pulsemq.protocol.frames import encode_heartbeat

        while self._running:
            await asyncio.sleep(self._config.heartbeat_interval)
            if not self._running:
                break
            try:
                frames = encode_heartbeat()
                if self._transport is not None:
                    await self._transport.send(frames)
            except asyncio.CancelledError:
                break
            except Exception:
                logger.warning("心跳发送异常", exc_info=True)
```

- [ ] **Step 3: Sub 端 — 修改 `subscribe()` 支持心跳检测**

修改 `src/pulsemq/subscriber.py` 中的 `subscribe` 方法：

```python
    async def subscribe(
        self, *topics: str,
        heartbeat_timeout: float = 90.0,
    ) -> AsyncIterator[PulseMessage]:
        """订阅 topic，返回异步迭代器。

        Args:
            *topics: 要订阅的 topic 列表。
            heartbeat_timeout: 心跳超时时间（秒），默认 90s。
                设为 0 禁用心跳检测。超时后抛 ConnectionLostError。
        """
        if self._sub is None:
            raise RuntimeError("Subscriber 未连接")

        for t in topics:
            self._sub.setsockopt(zmq.SUBSCRIBE, t.encode("utf-8"))
            logger.info("订阅 topic: %s", t)

        # 心跳检测：自动订阅内部心跳 topic
        _hb_enabled = heartbeat_timeout > 0
        if _hb_enabled:
            self._sub.setsockopt(zmq.SUBSCRIBE, b"__pulse_hb__")
            last_recv: float | None = None  # 第一条消息到达后才开始计时
            logger.info("心跳检测已启用 (timeout=%.0fs)", heartbeat_timeout)

        while True:
            try:
                if _hb_enabled:
                    frames = await asyncio.wait_for(
                        self._sub.recv_multipart(), timeout=1.0
                    )
                else:
                    frames = await self._sub.recv_multipart()

                if len(frames) != 4:
                    continue

                # 检查是否为 PING 帧（内部心跳，不暴露给用户）
                if _hb_enabled:
                    msg_type = frames[1][0]
                    if msg_type == 0x02:  # MsgType.PING
                        last_recv = time.monotonic()
                        continue

                # DATA 帧：刷新心跳计时器
                if _hb_enabled:
                    last_recv = time.monotonic()

                yield decode(frames)

            except asyncio.TimeoutError:
                # recv 超时 1s：检查心跳
                if _hb_enabled and last_recv is not None:
                    elapsed = time.monotonic() - last_recv
                    if elapsed > heartbeat_timeout:
                        raise ConnectionLostError(
                            f"心跳超时: 已 {elapsed:.0f}s 未收到消息"
                        )
                continue
            except zmq.ZMQError:
                if self._closed_by_user:
                    break
                raise ConnectionLostError("与 Publisher 的连接已断开")
            except asyncio.CancelledError:
                if self._sub is not None:
                    self._closed_by_user = True
                    self._sub.close(linger=0)
                    self._sub = None
                raise
```

- [ ] **Step 4: 编写心跳端到端测试**

在 `tests/test_auth_and_heartbeat.py` 末尾追加：

```python
import struct


class TestHeartbeatEndToEnd:
    """模块 4：心跳机制端到端。"""

    async def test_heartbeat_frames_filtered_from_user(
        self,
        random_port_pair: tuple[int, int],
        tmp_sqlite_url: str,
    ) -> None:
        """用户迭代器不收到 PING 帧。"""
        pub_port, admin_port = random_port_pair
        pub = make_publisher(pub_port=pub_port, admin_port=admin_port, tmp_db=tmp_sqlite_url)
        # 缩短心跳间隔以加速测试
        pub._config.heartbeat_interval = 1.0

        topic = "hb_filter_test"
        received: list = []

        async def _factory() -> dict:
            return {"v": 1}

        pub.register_producer(fn=_factory, name=topic, interval=0.5)

        async with running_publisher(pub, warmup=1.5):
            sub = PulseSubscriber(f"tcp://127.0.0.1:{pub_port}")
            await sub.connect()
            async for msg in sub.subscribe(topic, heartbeat_timeout=60.0):
                received.append(msg)
                if len(received) >= 3:
                    break
            await sub.close()

        # 所有收到的消息 topic 应为业务 topic，不应包含 __pulse_hb__
        for msg in received:
            assert msg.topic == topic, f"不应收到心跳 topic: {msg.topic}"
        assert len(received) >= 3

    async def test_heartbeat_timeout_raises_connection_lost(
        self,
        random_port_pair: tuple[int, int],
        tmp_sqlite_url: str,
    ) -> None:
        """心跳超时后抛 ConnectionLostError。"""
        pub_port, admin_port = random_port_pair
        pub = make_publisher(pub_port=pub_port, admin_port=admin_port, tmp_db=tmp_sqlite_url)
        pub._config.heartbeat_interval = 2.0  # pub 每 2s 发心跳

        topic = "hb_timeout_test"

        async def _factory() -> dict:
            return {"v": 1}

        pub.register_producer(fn=_factory, name=topic, interval=0.3)

        async with running_publisher(pub) as p:
            sub = PulseSubscriber(f"tcp://127.0.0.1:{pub_port}")
            await sub.connect()
            # 先收一条确认连通
            async for msg in sub.subscribe(topic, heartbeat_timeout=5.0):
                assert msg.topic == topic
                break
            # publisher 关闭后，心跳停止，应在 5s 内超时
            # (running_publisher 会在退出时关闭 publisher)
            await asyncio.sleep(0.5)

        # publisher 已关闭，继续迭代应因心跳超时抛异常
        with pytest.raises(ConnectionLostError, match="心跳超时"):
            async for _msg in sub.subscribe(topic, heartbeat_timeout=5.0):
                pass

        await sub.close()

    async def test_heartbeat_timeout_zero_disabled(
        self,
        random_port_pair: tuple[int, int],
        tmp_sqlite_url: str,
    ) -> None:
        """heartbeat_timeout=0 时心跳检测完全禁用。"""
        pub_port, admin_port = random_port_pair
        pub = make_publisher(pub_port=pub_port, admin_port=admin_port, tmp_db=tmp_sqlite_url)
        pub._config.heartbeat_interval = 1.0

        topic = "hb_disabled_test"
        received: list = []

        async def _factory() -> dict:
            return {"v": 1}

        pub.register_producer(fn=_factory, name=topic, interval=0.3)

        async with running_publisher(pub):
            sub = PulseSubscriber(f"tcp://127.0.0.1:{pub_port}")
            await sub.connect()
            # heartbeat_timeout=0 禁用：不应自动订阅 __pulse_hb__
            async for msg in sub.subscribe(topic, heartbeat_timeout=0.0):
                received.append(msg)
                if len(received) >= 3:
                    break
            await sub.close()

        assert len(received) >= 3
        for msg in received:
            assert msg.topic == topic

    async def test_zeroconf_pubsub_works(
        self,
        random_port_pair: tuple[int, int],
        tmp_sqlite_url: str,
    ) -> None:
        """最简配置端到端：pub 和 sub 零配置即可工作。"""
        pub_port, admin_port = random_port_pair
        pub = make_publisher(pub_port=pub_port, admin_port=admin_port, tmp_db=tmp_sqlite_url)
        # 使用默认 heartbeat_interval=30，测试用较短间隔
        pub._config.heartbeat_interval = 2.0

        topic = "zero_config"
        received: list = []

        async def _factory() -> dict:
            return {"ok": True}

        pub.register_producer(fn=_factory, name=topic, interval=0.3)

        async with running_publisher(pub):
            # 最简 subscriber：不传任何额外参数
            sub = PulseSubscriber(f"tcp://127.0.0.1:{pub_port}")
            await sub.connect()
            # 最简 subscribe：不传 heartbeat_timeout，使用默认 90s
            async for msg in sub.subscribe(topic):
                received.append(msg)
                if len(received) >= 3:
                    break
            await sub.close()

        assert len(received) >= 3
        for msg in received:
            assert msg.payload == {"ok": True}
```

- [ ] **Step 5: 运行新测试**

```bash
pytest tests/test_auth_and_heartbeat.py::TestHeartbeatEndToEnd -v
```

预期输出：4 passed

- [ ] **Step 6: 运行完整回归测试**

```bash
pytest tests/ -v --timeout=120
```

预期输出：所有测试通过（约 85+ 个）

- [ ] **Step 7: 提交**

```bash
git add src/pulsemq/config.py src/pulsemq/publisher.py src/pulsemq/subscriber.py tests/test_auth_and_heartbeat.py
git commit -m "feat: 心跳机制 — Pub 定期发送 PING + Sub 超时检测"
```

---

## 完成验证清单

- [ ] `pytest tests/ -v --timeout=120` 全部通过
- [ ] 最简配置示例代码可运行（pub 无 api_keys, sub 无 username）
- [ ] 认证失败时 sub 抛 `ConnectionLostError` 而非静默退出
- [ ] pub 端日志可见认证成功/失败 + 客户端 IP
- [ ] `on_auth` 回调可接收成功和失败事件
- [ ] `git log --oneline -5` 显示 3 个清晰的中文 commit
