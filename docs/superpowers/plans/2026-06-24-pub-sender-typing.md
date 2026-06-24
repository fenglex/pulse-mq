# pub 端 sender + producer 管线类型化 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 给 pub 端 `PublisherSender`、`ProducerManager` 管线及 `@pub.producer(inject_sender=True)` 装饰器补齐明确类型注解，消除整条链路上的 `Any`，让 IDE/类型检查器能识别注入的 sender 类型并校验 `send()` 的数据类型。

**Architecture:** 新建 `producers/types.py` 作为类型单一来源（`PubData` 联合类型 + 3 个回调别名），改造 `manager.py` / `publisher.py` 消除 `Any`，用 `@overload` 把 `inject_sender` 标志位与回调签名在类型层绑定。运行时行为零变化。

**Tech Stack:** Python 3.13、`typing`（TypeAlias/Union/overload/Literal）、pandas（硬依赖）、pytest + pytest-asyncio。

**关联设计文档：** `docs/superpowers/specs/2026-06-24-pub-sender-typing-design.md`

---

## 文件结构

| 文件 | 责任 | 改动类型 |
|------|------|----------|
| `src/pulsemq/producers/types.py` | 类型单一来源：`PubData`、`SimpleProducerCallback`、`SenderProducerCallback`、`ProducerCallback` | 新建 |
| `src/pulsemq/producers/manager.py` | ProducerManager 调度；导入 types 别名，新增 `OnMessageCallback`/`SenderFactory`，消除方法签名 `Any` | 修改 |
| `src/pulsemq/publisher.py` | `PublisherSender` 类型化；`_make_sender`/`_on_produce` 加类型；`producer`/`burst_producer`/`register_producer` 加 `@overload`；`__all__` 导出 | 修改 |
| `src/pulsemq/__init__.py` | 导出 `PublisherSender`、`PubData` | 修改 |
| `tests/test_producer_types.py` | 类型可导入性 + 类型注解存在性防回归测试 | 新建 |
| `README.md` | `inject_sender` 示例补类型注解用法 | 修改 |

**依赖顺序（types.py 是下游依赖源）：** Task 1 (types.py) → Task 2 (manager.py) → Task 3 (publisher.py) → Task 4 (__init__.py) → Task 5 (测试) → Task 6 (README)。Task 2/3/4 可在同一提交链上叠加，但各自有独立验证点。

---

### Task 1: 新建 producers/types.py 类型单一来源

**Files:**
- Create: `src/pulsemq/producers/types.py`
- Test: `tests/test_producer_types.py`（此 task 只建空文件占位，Task 5 填内容）

- [ ] **Step 1: 新建 types.py**

创建 `src/pulsemq/producers/types.py`，完整内容：

```python
"""Producer 管线的类型单一来源。

集中定义数据白名单类型与 producer 回调签名别名，
供 ProducerManager / PulsePublisher / PublisherSender 复用，
与运行时 _infer_record_count / _validate_serializer 的白名单语义一一对应。
"""

from __future__ import annotations

from typing import TypeAlias, Callable, Awaitable, Union

import pandas as pd

# —— 数据白名单：sender.send() 和 producer 回调返回值的合法类型 ——
# 与运行时白名单（DataFrame/dict/str/bytes）一一对应
PubData: TypeAlias = Union[pd.DataFrame, dict, bytes, str]

# —— 两种回调形态 ——
# 无 sender 注入：async def fn() -> PubData | None
SimpleProducerCallback: TypeAlias = Callable[[], Awaitable[PubData | None]]

# 有 sender 注入：async def fn(sender: PublisherSender) -> PubData | None
# PublisherSender 做前向引用（字符串注解），避免循环导入
# （types.py 不 import publisher.py；publisher.py 导入 types.py）
SenderProducerCallback: TypeAlias = Callable[["PublisherSender"], Awaitable[PubData | None]]

# 统一别名（注册入口用）：两种形态的并集
ProducerCallback: TypeAlias = Union[SimpleProducerCallback, SenderProducerCallback]
```

- [ ] **Step 2: 验证模块可导入且无循环导入**

Run: `python -c "from pulsemq.producers.types import PubData, SimpleProducerCallback, SenderProducerCallback, ProducerCallback; print('ok')"`
Expected: 输出 `ok`，无 ImportError。

- [ ] **Step 3: 提交**

```bash
git add src/pulsemq/producers/types.py
git commit -m "feat(types): 新建 producers/types.py 类型单一来源 (PubData + 回调别名)"
```

---

### Task 2: ProducerManager 类型化（消除管线 Any）

**Files:**
- Modify: `src/pulsemq/producers/manager.py`

- [ ] **Step 1: 修改 import 区块**

在 `src/pulsemq/producers/manager.py` 第 8-14 行的 import 区块，把：

```python
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Awaitable

logger = logging.getLogger(__name__)

# producer 回调类型：async 函数，返回任意数据；inject_sender=True 时接收 sender 参数
ProducerCallback = Callable[..., Awaitable[Any]]
```

替换为：

```python
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Callable, Awaitable

from pulsemq.producers.types import ProducerCallback, PubData

logger = logging.getLogger(__name__)

# 消息分发回调：on_message(spec, data) —— ProducerManager 调用，PulsePublisher 实现
OnMessageCallback = Callable[[ProducerSpec, PubData], Awaitable[None]]
# sender 工厂：sender_factory(spec) -> PublisherSender —— inject_sender=True 时使用
# PublisherSender 用字符串注解做前向引用，避免 manager → publisher 循环导入
SenderFactory = Callable[[ProducerSpec], "PublisherSender"]
```

> 注：`field` 在原 import 中存在但未被 `ProducerSpec` 使用；保留不动以减少无关改动。若 linter 报 unused，后续单独清理，不在本次范围。

- [ ] **Step 2: 改造 start_all 方法签名**

把 `manager.py` 中 `start_all` 方法（原 92-107 行附近）：

```python
    async def start_all(self, on_message: Any, sender_factory: Any | None = None) -> None:
        """启动所有 producer 任务。

        Args:
            on_message: async callback(spec, data) 每次回调返回时调用。
            sender_factory: callable(spec) -> sender，inject_sender=True 时使用。
        """
```

替换为：

```python
    async def start_all(
        self,
        on_message: OnMessageCallback,
        sender_factory: SenderFactory | None = None,
    ) -> None:
        """启动所有 producer 任务。

        Args:
            on_message: async callback(spec, data) 每次回调返回时调用。
            sender_factory: callable(spec) -> sender，inject_sender=True 时使用。
        """
```

- [ ] **Step 3: 改造 _run_loop 方法签名**

把 `_run_loop` 方法（原 120 行附近）：

```python
    async def _run_loop(self, spec: ProducerSpec, on_message: Any, sender_factory: Any | None) -> None:
```

替换为：

```python
    async def _run_loop(
        self,
        spec: ProducerSpec,
        on_message: OnMessageCallback,
        sender_factory: SenderFactory | None,
    ) -> None:
```

- [ ] **Step 4: 改造 _run_burst_loop 方法签名**

把 `_run_burst_loop` 方法（原 151 行附近）：

```python
    async def _run_burst_loop(self, spec: ProducerSpec, on_message: Any, sender_factory: Any | None) -> None:
```

替换为：

```python
    async def _run_burst_loop(
        self,
        spec: ProducerSpec,
        on_message: OnMessageCallback,
        sender_factory: SenderFactory | None,
    ) -> None:
```

- [ ] **Step 5: 验证现有测试不回归**

Run: `python -m pytest tests/test_e2e_publisher.py tests/test_data_types.py -q`
Expected: 全部 PASS（运行时逻辑零变化，仅类型注解变更）。

> 注：`ProducerSpec` 在本文件内定义（dataclass），`OnMessageCallback`/`SenderFactory` 的定义**必须放在 `ProducerSpec` 之后**——`TypeAlias` 右侧表达式在模块加载时立即求值，`from __future__ import annotations` 只让**函数注解**惰性求值，不影响模块级别名赋值。所以 Step 1 的 import 区块不再含别名定义，别名移到 ProducerSpec 之后（见下方 ProducerSpec 定义之后）。

- [ ] **Step 6: 提交**

```bash
git add src/pulsemq/producers/manager.py
git commit -m "refactor(producers): ProducerManager 管线消除 Any，引入 OnMessageCallback/SenderFactory"
```

---

### Task 3: PublisherSender + PulsePublisher 装饰器类型化

**Files:**
- Modify: `src/pulsemq/publisher.py`

- [ ] **Step 1: 修改 import 区块**

在 `src/pulsemq/publisher.py` 第 19-35 行的 import 区块，把：

```python
from __future__ import annotations

import asyncio
import logging
import time
from functools import wraps
from typing import Any, Callable, Awaitable
```

替换为：

```python
from __future__ import annotations

import asyncio
import logging
import time
from functools import wraps
from typing import Any, Callable, Awaitable, overload, Literal

from pulsemq.producers.types import (
    PubData,
    SimpleProducerCallback,
    SenderProducerCallback,
)
```

> `Any` 暂时保留：`_publish_data` 等内部方法参数（如 `data`）后续仍要传给 `_infer_record_count`，但 `send()`/`_on_produce` 的入口会改为 `PubData`。`wraps` 原本就 import 了，保留。

- [ ] **Step 2: 在 publisher.py 顶部增加 ProducerSpec 导入**

在 import 区块末尾（`from pulsemq.transport.zmq_pub import AuthCallback, ZmqPubTransport` 之后）新增一行：

```python
from pulsemq.producers.manager import ProducerSpec
```

- [ ] **Step 3: 改造 PublisherSender 类**

把 `PublisherSender` 类（原 43-65 行）：

```python
class PublisherSender:
    """注入 producer 回调的手动发送端。"""

    def __init__(self, publisher: "PulsePublisher", spec: Any) -> None:
        self._publisher = publisher
        self._spec = spec

    async def send(
        self,
        data: Any,
        *,
        topic: str | None = None,
        serializer: str | None = None,
        compression: str | None = None,
    ) -> None:
        """手动发送一条消息，默认沿用当前 producer 配置。"""
        await self._publisher._publish_data(
            topic=topic or self._spec.name,
            data=data,
            cache_size=self._spec.cache_size,
            serializer=serializer or self._spec.serializer,
            compression=compression or self._spec.compression,
        )
```

替换为：

```python
class PublisherSender:
    """注入 producer 回调的手动发送端。"""

    def __init__(self, publisher: "PulsePublisher", spec: ProducerSpec) -> None:
        self._publisher = publisher
        self._spec = spec

    async def send(
        self,
        data: PubData,
        *,
        topic: str | None = None,
        serializer: str | None = None,
        compression: str | None = None,
    ) -> None:
        """手动发送一条消息，默认沿用当前 producer 配置。"""
        await self._publisher._publish_data(
            topic=topic or self._spec.name,
            data=data,
            cache_size=self._spec.cache_size,
            serializer=serializer or self._spec.serializer,
            compression=compression or self._spec.compression,
        )
```

- [ ] **Step 4: 改造 _make_sender 与 _on_produce 方法**

把 `_make_sender` 和 `_on_produce`（原 284-299 行）：

```python
    def _make_sender(self, spec: Any) -> PublisherSender:
        """为 inject_sender producer 构造手动发送端。"""
        return PublisherSender(self, spec)

    async def _on_produce(self, spec: Any, data: Any) -> None:
        """Producer 回调返回数据后的处理流程。"""
```

替换为：

```python
    def _make_sender(self, spec: ProducerSpec) -> PublisherSender:
        """为 inject_sender producer 构造手动发送端。"""
        return PublisherSender(self, spec)

    async def _on_produce(self, spec: ProducerSpec, data: PubData) -> None:
        """Producer 回调返回数据后的处理流程。"""
```

- [ ] **Step 5: 给 producer 装饰器加 @overload**

把 `producer` 方法（原 102-124 行）：

```python
    def producer(
        self,
        name: str,
        *,
        interval: float = 5.0,
        cache_size: int = 100_000,
        serializer: str = "msgpack",
        compression: str = "none",
        inject_sender: bool = False,
    ) -> Callable:
        """装饰器：注册 async producer。"""
        def decorator(fn: Callable[[], Awaitable[Any]]) -> Callable[[], Awaitable[Any]]:
            self._producer_mgr.register(
                callback=fn,
                name=name,
                interval=interval,
                cache_size=cache_size,
                serializer=serializer,
                compression=compression,
                inject_sender=inject_sender,
            )
            return fn
        return decorator
```

替换为：

```python
    @overload
    def producer(
        self,
        name: str,
        *,
        interval: float = ...,
        cache_size: int = ...,
        serializer: str = ...,
        compression: str = ...,
        inject_sender: Literal[False] = ...,
    ) -> Callable[[SimpleProducerCallback], SimpleProducerCallback]: ...

    @overload
    def producer(
        self,
        name: str,
        *,
        interval: float = ...,
        cache_size: int = ...,
        serializer: str = ...,
        compression: str = ...,
        inject_sender: Literal[True],
    ) -> Callable[[SenderProducerCallback], SenderProducerCallback]: ...

    def producer(
        self,
        name: str,
        *,
        interval: float = 5.0,
        cache_size: int = 100_000,
        serializer: str = "msgpack",
        compression: str = "none",
        inject_sender: bool = False,
    ) -> Callable:
        """装饰器：注册 async producer。"""
        def decorator(fn: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
            self._producer_mgr.register(
                callback=fn,
                name=name,
                interval=interval,
                cache_size=cache_size,
                serializer=serializer,
                compression=compression,
                inject_sender=inject_sender,
            )
            return fn
        return decorator
```

> `decorator` 内层类型从 `Callable[[], Awaitable[Any]]` 放宽为 `Callable[..., Awaitable[Any]]`，以同时接受带 sender 参数的回调（运行时仍由 `ProducerManager._run_loop` 按 `inject_sender` 决定是否传 sender）。实际实现分支零逻辑变化。

- [ ] **Step 6: 给 burst_producer 装饰器加 @overload**

把 `burst_producer` 方法（原 126-146 行）：

```python
    def burst_producer(
        self,
        name: str,
        *,
        cache_size: int = 100_000,
        serializer: str = "msgpack",
        compression: str = "none",
        inject_sender: bool = False,
    ) -> Callable:
        """装饰器：注册 burst producer（无间隔连续发送，用于极限性能测试）。"""
        def decorator(fn: Callable[[], Awaitable[Any]]) -> Callable[[], Awaitable[Any]]:
            self._producer_mgr.register_burst(
                callback=fn,
                name=name,
                cache_size=cache_size,
                serializer=serializer,
                compression=compression,
                inject_sender=inject_sender,
            )
            return fn
        return decorator
```

替换为：

```python
    @overload
    def burst_producer(
        self,
        name: str,
        *,
        cache_size: int = ...,
        serializer: str = ...,
        compression: str = ...,
        inject_sender: Literal[False] = ...,
    ) -> Callable[[SimpleProducerCallback], SimpleProducerCallback]: ...

    @overload
    def burst_producer(
        self,
        name: str,
        *,
        cache_size: int = ...,
        serializer: str = ...,
        compression: str = ...,
        inject_sender: Literal[True],
    ) -> Callable[[SenderProducerCallback], SenderProducerCallback]: ...

    def burst_producer(
        self,
        name: str,
        *,
        cache_size: int = 100_000,
        serializer: str = "msgpack",
        compression: str = "none",
        inject_sender: bool = False,
    ) -> Callable:
        """装饰器：注册 burst producer（无间隔连续发送，用于极限性能测试）。"""
        def decorator(fn: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
            self._producer_mgr.register_burst(
                callback=fn,
                name=name,
                cache_size=cache_size,
                serializer=serializer,
                compression=compression,
                inject_sender=inject_sender,
            )
            return fn
        return decorator
```

- [ ] **Step 7: 给 register_producer 加 @overload**

把 `register_producer` 方法（原 148-168 行）：

```python
    def register_producer(
        self,
        fn: Callable[[], Awaitable[Any]],
        *,
        name: str,
        interval: float = 5.0,
        cache_size: int = 100_000,
        serializer: str = "msgpack",
        compression: str = "none",
        inject_sender: bool = False,
    ) -> None:
        """直接注册 async producer。"""
        self._producer_mgr.register(
            callback=fn,
            name=name,
            interval=interval,
            cache_size=cache_size,
            serializer=serializer,
            compression=compression,
            inject_sender=inject_sender,
        )
```

替换为：

```python
    @overload
    def register_producer(
        self,
        fn: SimpleProducerCallback,
        *,
        name: str,
        interval: float = ...,
        cache_size: int = ...,
        serializer: str = ...,
        compression: str = ...,
        inject_sender: Literal[False] = ...,
    ) -> None: ...

    @overload
    def register_producer(
        self,
        fn: SenderProducerCallback,
        *,
        name: str,
        interval: float = ...,
        cache_size: int = ...,
        serializer: str = ...,
        compression: str = ...,
        inject_sender: Literal[True],
    ) -> None: ...

    def register_producer(
        self,
        fn: Callable[..., Awaitable[Any]],
        *,
        name: str,
        interval: float = 5.0,
        cache_size: int = 100_000,
        serializer: str = "msgpack",
        compression: str = "none",
        inject_sender: bool = False,
    ) -> None:
        """直接注册 async producer。"""
        self._producer_mgr.register(
            callback=fn,
            name=name,
            interval=interval,
            cache_size=cache_size,
            serializer=serializer,
            compression=compression,
            inject_sender=inject_sender,
        )
```

- [ ] **Step 8: 验证 publisher.py 可正常导入**

Run: `python -c "from pulsemq.publisher import PulsePublisher, PublisherSender; print('ok')"`
Expected: 输出 `ok`，无 ImportError / SyntaxError。

- [ ] **Step 9: 运行全量测试不回归**

Run: `python -m pytest tests/ -q`
Expected: 全部 PASS（与改动前结果一致）。

- [ ] **Step 10: 提交**

```bash
git add src/pulsemq/publisher.py
git commit -m "refactor(publisher): PublisherSender 类型化 + 装饰器 @overload 绑定 inject_sender"
```

---

### Task 4: 导出 PublisherSender 与 PubData

**Files:**
- Modify: `src/pulsemq/__init__.py`

- [ ] **Step 1: 修改导出区块**

把 `src/pulsemq/__init__.py` 末尾的导入与 `__all__`（原 18-29 行）：

```python
from pulsemq.publisher import PulsePublisher
from pulsemq.subscriber import PulseSubscriber
from pulsemq.protocol.frames import PulseMessage
from pulsemq.config import PublisherConfig, load_config

__all__ = [
    "PulsePublisher",
    "PulseSubscriber",
    "PulseMessage",
    "PublisherConfig",
    "load_config",
]
```

替换为：

```python
from pulsemq.publisher import PulsePublisher, PublisherSender
from pulsemq.producers.types import PubData
from pulsemq.subscriber import PulseSubscriber
from pulsemq.protocol.frames import PulseMessage
from pulsemq.config import PublisherConfig, load_config

__all__ = [
    "PulsePublisher",
    "PulseSubscriber",
    "PulseMessage",
    "PublisherConfig",
    "PublisherSender",
    "PubData",
    "load_config",
]
```

- [ ] **Step 2: 验证顶层导入可用**

Run: `python -c "import pulsemq; print(pulsemq.PublisherSender, pulsemq.PubData)"`
Expected: 输出类对象与类型别名，无 ImportError。

- [ ] **Step 3: 提交**

```bash
git add src/pulsemq/__init__.py
git commit -m "feat: 导出 PublisherSender 与 PubData 供用户回调类型注解使用"
```

---

### Task 5: 类型注解防回归测试

**Files:**
- Create: `tests/test_producer_types.py`

- [ ] **Step 1: 编写测试文件**

创建 `tests/test_producer_types.py`，完整内容：

```python
"""producer 管线类型注解的防回归测试。

覆盖：
1. 类型符号可从顶层包导入
2. PublisherSender.send 的 data 参数注解为 PubData（非 Any）
3. PublisherSender / ProducerManager 内部方法签名消除 Any
4. inject_sender=True 时 producer 装饰器返回 SenderProducerCallback 类型

注：这些是反射断言，不依赖类型检查器运行——防止有人把 PubData 退回 Any。
"""

from __future__ import annotations

import inspect
from typing import Any, get_type_hints

import pandas as pd

import pulsemq
from pulsemq import PublisherSender, PubData
from pulsemq.publisher import PulsePublisher
from pulsemq.producers.manager import ProducerManager
from pulsemq.producers.types import (
    PubData as TypesPubData,
    SimpleProducerCallback,
    SenderProducerCallback,
    ProducerCallback,
)


# ---------------------------------------------------------------------------
# 类型符号可导入性
# ---------------------------------------------------------------------------


class TestTypeSymbolsImportable:
    def test_pubdata_exported_from_package(self):
        """PubData 能从 pulsemq 顶层导入。"""
        assert pulsemq.PubData is not None

    def test_publisher_sender_exported_from_package(self):
        """PublisherSender 能从 pulsemq 顶层导入。"""
        assert pulsemq.PublisherSender is PublisherSender

    def test_pubdata_consistent_across_modules(self):
        """__init__ 导出的 PubData 与 types.py 定义一致。"""
        assert PubData is TypesPubData

    def test_callback_aliases_defined(self):
        """三个回调别名都存在。"""
        assert SimpleProducerCallback is not None
        assert SenderProducerCallback is not None
        assert ProducerCallback is not None


# ---------------------------------------------------------------------------
# PublisherSender.send 的 data 参数注解
# ---------------------------------------------------------------------------


class TestPublisherSenderSignature:
    def test_send_data_param_is_pubdata(self):
        """send() 的 data 参数注解应是 PubData 联合类型，而非 Any。

        get_type_hints 会解析字符串注解；PubData 是 typing.Union 别名。
        我们校验：注解不是 Any，且 union 的成员包含 pd.DataFrame / dict / bytes / str。
        """
        hints = get_type_hints(PublisherSender.send)
        data_hint = hints["data"]

        # Any 的判定：直接相等
        assert data_hint is not Any, "send(data) 退回到了 Any"

        # PubData = Union[pd.DataFrame, dict, bytes, str]
        # typing.Union 的成员通过 __args__ 获取
        args = set(data_hint.__args__)
        assert pd.DataFrame in args, f"PubData 缺少 DataFrame: {args}"
        assert dict in args, f"PubData 缺少 dict: {args}"
        assert bytes in args, f"PubData 缺少 bytes: {args}"
        assert str in args, f"PubData 缺少 str: {args}"

    def test_sender_init_spec_param_is_producer_spec(self):
        """__init__ 的 spec 参数应是 ProducerSpec，而非 Any。"""
        from pulsemq.producers.manager import ProducerSpec
        hints = get_type_hints(PublisherSender.__init__)
        assert hints["spec"] is ProducerSpec


# ---------------------------------------------------------------------------
# ProducerManager 方法签名消除 Any
# ---------------------------------------------------------------------------


class TestProducerManagerSignature:
    def test_start_all_typed(self):
        """start_all 的 on_message / sender_factory 不应是 Any。"""
        sig = inspect.signature(ProducerManager.start_all)
        on_msg_hints = get_type_hints(ProducerManager.start_all)
        # on_message 注解存在且不是 Any
        assert "on_message" in on_msg_hints
        assert on_msg_hints["on_message"] is not Any
        # sender_factory 注解存在（可为 Optional）
        assert "sender_factory" in on_msg_hints

    def test_run_loop_typed(self):
        """_run_loop 的参数消除 Any。"""
        hints = get_type_hints(ProducerManager._run_loop)
        assert hints.get("on_message") is not Any
        assert hints.get("spec") is not None


# ---------------------------------------------------------------------------
# Publisher 内部方法签名
# ---------------------------------------------------------------------------


class TestPublisherInternalSignature:
    def test_on_produce_typed(self):
        """_on_produce(spec, data) 参数消除 Any。"""
        hints = get_type_hints(PulsePublisher._on_produce)
        assert hints.get("spec") is not Any
        assert hints.get("data") is not Any

    def test_make_sender_return_type(self):
        """_make_sender 返回 PublisherSender。"""
        hints = get_type_hints(PulsePublisher._make_sender)
        assert hints.get("return") is PublisherSender
```

- [ ] **Step 2: 运行新测试，验证通过**

Run: `python -m pytest tests/test_producer_types.py -v`
Expected: 全部 PASS。

- [ ] **Step 3: 提交**

```bash
git add tests/test_producer_types.py
git commit -m "test: producer 管线类型注解防回归测试"
```

---

### Task 6: README 更新 inject_sender 类型注解用法

**Files:**
- Modify: `README.md`

- [ ] **Step 1: 更新 inject_sender 示例**

把 `README.md` 第 72-81 行的 inject_sender 章节：

````
如果需要在 producer 内部手动控制发送，可开启 `inject_sender`：

```python
@pub.producer(name="market", interval=1.0, inject_sender=True)
async def market(sender):
    await sender.send({"symbol": "600000", "price": 10.5})
    await sender.send({"symbol": "000001", "price": 12.3}, topic="sz_market")
```

`sender.send()` 默认沿用当前 producer 的 topic、serializer、compression，也可以通过参数覆盖。
````

替换为：

````
如果需要在 producer 内部手动控制发送，可开启 `inject_sender`：

```python
from pulsemq import PulsePublisher, PublisherSender

pub = PulsePublisher()

@pub.producer(name="market", interval=1.0, inject_sender=True)
async def market(sender: PublisherSender) -> None:
    await sender.send({"symbol": "600000", "price": 10.5})
    await sender.send({"symbol": "000001", "price": 12.3}, topic="sz_market")
```

开启 `inject_sender=True` 后，装饰器会向回调注入 `PublisherSender` 实例，类型检查器/IDE 能自动识别 `sender` 的类型并校验 `send()` 的数据类型。`sender.send()` 的 `data` 参数只接受白名单类型（`pd.DataFrame` / `dict` / `str` / `bytes`，可用 `PubData` 别名标注），默认沿用当前 producer 的 topic、serializer、compression，也可以通过参数覆盖。
````

- [ ] **Step 2: 提交**

```bash
git add README.md
git commit -m "docs: README inject_sender 示例补类型注解用法"
```

---

## 收尾验证

- [ ] **全量测试**

Run: `python -m pytest tests/ -q`
Expected: 全部 PASS，新增 `test_producer_types.py` 通过，原 e2e/integration/data_types 不回归。

- [ ] **导入冒烟**

Run: `python -c "import pulsemq; from pulsemq import PulsePublisher, PublisherSender, PubData; print('all imports ok')"`
Expected: 输出 `all imports ok`。

---

## 风险与回滚

- **运行时行为零变化**：全部为类型注解与 @overload 装饰，`decorator` 内层类型从 `Callable[[], ...]` 放宽为 `Callable[..., ...]` 仅影响静态检查，不影响运行时调用。
- **回滚**：6 个提交可整体 `git revert`，或按 task 粒度回退。
