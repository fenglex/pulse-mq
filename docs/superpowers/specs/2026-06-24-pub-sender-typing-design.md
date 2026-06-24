# 设计文档：pub 端 sender + producer 管线类型化

- **日期**：2026-06-24
- **状态**：已批准（待实现）
- **版本影响**：patch（无 breaking change，纯类型增强）
- **范围**：pub 端 sender 注入链路 + ProducerManager 管线

## 背景与动机

v3.1.0 引入了 `inject_sender=True` 注入模式，允许 producer 回调在内部手动 `await sender.send(...)` 发送消息。但整条 producer 管线的类型注解严重缺失，全部使用 `Any`：

- `PublisherSender.__init__(self, publisher, spec: Any)`
- `PublisherSender.send(self, data: Any, ...)`
- `_make_sender(self, spec: Any)` / `_on_produce(self, spec: Any, data: Any)`
- `ProducerCallback = Callable[..., Awaitable[Any]]`
- `start_all(on_message: Any, sender_factory: Any | None = None)`

**核心痛点**：用户写 `@pub.producer(inject_sender=True)` 时，类型检查器/IDE 无法识别注入的 `sender` 参数类型，用户回调里 `await sender.send(...)` 没有补全、没有类型校验——`send` 的 `data` 参数本应只接受 4 种白名单类型（DataFrame/dict/str/bytes），但类型层完全无感知。

## 目标

1. 给 `PublisherSender.send()` 的 `data` 参数明确的联合类型，与运行时白名单语义对齐
2. 把 `inject_sender` 标志位与回调签名在类型层绑定（配错能在静态检查层报错）
3. 消除 producer 管线上的 `Any`（ProducerManager 的回调、factory、dispatch 回调）
4. **运行时行为零变化**——纯类型注解增强，不改任何执行逻辑

## 非目标

- 不做 `serializer`/`compression` 参数的 `Literal[...]` 收敛（pyarrow 可选依赖导致枚举不稳定，留作后续）
- 不引入 `Generic[T]` 泛型 sender（统一用 `PubData` 联合类型）
- 不改协议帧格式、不改运行时校验逻辑

## 设计决策

| 决策点 | 选择 | 理由 |
|--------|------|------|
| 改动范围 | sender + producer 管线 | 集中消除整条链路的 Any，而非只点 sender |
| `data` 类型表达 | 联合类型 `Union[pd.DataFrame, dict, bytes, str]` | 直接对应运行时白名单 |
| pandas 注解写法 | 顶层直接 `import pandas as pd` | pandas 是硬依赖（`pyproject.toml` 已声明），无需 TYPE_CHECKING 守卫 |
| 回调形态表达 | 类型别名区分两种回调 + Union 合并 | `SimpleProducerCallback`（无参）/`SenderProducerCallback`（带 sender）分开定义 |
| 装饰器入口 | `@overload` 按 `inject_sender` 拆分签名 | 把标志位和回调类型绑定，让 IDE 能识别注入的 sender 类型 |

## 架构设计

### §1 核心类型定义（新建 `src/pulsemq/producers/types.py`）

集中放置 producer 管线类型，作为整条链路的类型单一来源：

```python
# src/pulsemq/producers/types.py
from __future__ import annotations

from typing import TypeAlias, Callable, Awaitable, Union

import pandas as pd

# —— 数据白名单：sender.send() 和 producer 回调返回值的合法类型 ——
# 与运行时 _infer_record_count / _validate_serializer 的白名单一一对应
PubData: TypeAlias = Union[pd.DataFrame, dict, bytes, str]

# —— 两种回调形态 ——
# 无 sender 注入：async def fn() -> PubData | None
SimpleProducerCallback: TypeAlias = Callable[[], Awaitable[PubData | None]]

# 有 sender 注入：async def fn(sender: PublisherSender) -> PubData | None
# PublisherSender 做前向引用（字符串注解），避免循环导入
SenderProducerCallback: TypeAlias = Callable[["PublisherSender"], Awaitable[PubData | None]]

# 统一别名（注册入口用）
ProducerCallback: TypeAlias = Union[SimpleProducerCallback, SenderProducerCallback]
```

**放置位置理由**：放 `producers/types.py` 而非 `protocol/`。这些类型描述的是 producer 管线的契约，与协议帧编解码（`protocol/`）职责不同；和 `ProducerSpec`、`ProducerManager` 同属 producers 域。

**循环导入规避**：`types.py` 只写别名，不实例化 `PublisherSender`；用字符串注解 `"PublisherSender"` 做前向引用。导入方向是 publisher/manager → types（单向），types 不 import publisher，无循环风险。

### §2 PublisherSender 类型化（`src/pulsemq/publisher.py`）

```python
from pulsemq.producers.types import PubData
from pulsemq.producers.manager import ProducerSpec

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

**配套修改（`publisher.py` 内）：**
- `_make_sender(self, spec: Any)` → `_make_sender(self, spec: ProducerSpec) -> PublisherSender`
- `_on_produce(self, spec: Any, data: Any)` → `_on_produce(self, spec: ProducerSpec, data: PubData)`

**决策：`serializer`/`compression` 保持 `str | None`**——不做 `Literal[...]` 收敛。pyarrow 是可选依赖，且本次只收敛 `data` 类型，序列化器名留作后续。

### §3 ProducerManager 类型化（`src/pulsemq/producers/manager.py`）

```python
from pulsemq.producers.types import (
    ProducerCallback, PubData,
)

# 消息分发回调：on_message(spec, data)
OnMessageCallback = Callable[[ProducerSpec, PubData], Awaitable[None]]
# sender 工厂：sender_factory(spec) -> PublisherSender
SenderFactory = Callable[[ProducerSpec], "PublisherSender"]

class ProducerManager:
    async def start_all(
        self,
        on_message: OnMessageCallback,
        sender_factory: SenderFactory | None = None,
    ) -> None: ...

    async def _run_loop(
        self, spec: ProducerSpec, on_message: OnMessageCallback,
        sender_factory: SenderFactory | None,
    ) -> None: ...

    async def _run_burst_loop(
        self, spec: ProducerSpec, on_message: OnMessageCallback,
        sender_factory: SenderFactory | None,
    ) -> None: ...
```

**改动点：**
- 删除模块内本地别名 `ProducerCallback = Callable[..., Awaitable[Any]]`，改从 `types.py` 导入
- `start_all` / `_run_loop` / `_run_burst_loop` 的 `on_message: Any` / `sender_factory: Any | None` → 明确类型
- `register` / `register_burst` 的 `callback: ProducerCallback` 保持（类型来源从本地变为导入）

**内部运行时逻辑不变**：`spec.inject_sender` 分支判断、`sender_factory(spec)` 调用、`await spec.callback(...)` 全部保持原样。

### §3' 装饰器入口 @overload（`src/pulsemq/publisher.py`）

三个注册入口用 `@overload` 按 `inject_sender` 拆分签名，把标志位和回调类型绑定：

```python
from typing import overload, Literal
from pulsemq.producers.types import (
    SimpleProducerCallback, SenderProducerCallback,
)

class PulsePublisher:

    # ---- producer 装饰器 ----
    @overload
    def producer(
        self, name: str, *, interval: float = ...,
        cache_size: int = ..., serializer: str = ...,
        compression: str = ..., inject_sender: Literal[False] = False,
    ) -> Callable[[SimpleProducerCallback], SimpleProducerCallback]: ...

    @overload
    def producer(
        self, name: str, *, interval: float = ...,
        cache_size: int = ..., serializer: str = ...,
        compression: str = ..., inject_sender: Literal[True],
    ) -> Callable[[SenderProducerCallback], SenderProducerCallback]: ...

    def producer(self, name, *, interval=5.0, cache_size=100_000,
                 serializer="msgpack", compression="none",
                 inject_sender=False):
        # 运行时实现：原样不动
        def decorator(fn):
            self._producer_mgr.register(
                callback=fn, name=name, interval=interval,
                cache_size=cache_size, serializer=serializer,
                compression=compression, inject_sender=inject_sender,
            )
            return fn
        return decorator
```

**`burst_producer` 和 `register_producer` 同样套用 `@overload`**：
- `burst_producer`：无 `interval` 参数，其余签名一致
- `register_producer`：直接注册（非装饰器），`fn` 参数类型随 `inject_sender` 切换

**效果**：

```python
@pub.producer(name="market", inject_sender=True)
async def market(sender: PublisherSender) -> None:  # ← 类型检查器识别 sender 类型
    await sender.send({"symbol": "600000", "price": 10.5})
    #     ^^^^ IDE 补全 send()，并校验 data 必须是 PubData
```

- `inject_sender=True` → 装饰器类型化为 `Callable[[SenderProducerCallback], SenderProducerCallback]`
- `inject_sender=False`（默认）→ `Callable[[SimpleProducerCallback], SimpleProducerCallback]`
- 配错（`inject_sender=True` 但回调没 sender 参数）→ 静态检查报错

**运行时行为零变化**：overload 的实际实现分支保持原样，`register`/`register_burst` 内部逻辑不动。

### §4 导出与测试

**导出（`src/pulsemq/__init__.py`）：**

```python
from pulsemq.publisher import PulsePublisher, PublisherSender
from pulsemq.producers.types import PubData

__all__ = [
    "PulsePublisher",
    "PulseSubscriber",
    "PulseMessage",
    "PublisherConfig",
    "PublisherSender",   # 新增：供 inject_sender 回调注解用
    "PubData",           # 新增：白名单数据类型
    "load_config",
]
```

**测试新增（`tests/test_producer_types.py`）：**

1. **类型可导入性**：`from pulsemq import PublisherSender, PubData` 不报错
2. **类型注解存在性**：反射检查 `PublisherSender.send` 的参数注解包含 `PubData`（防回归）
3. **现有功能不回归**：跑一遍 `inject_sender=True` 的 e2e（`tests/test_e2e_publisher.py` 已覆盖，确认通过即可）

**不新增运行时校验测试**——`send()` 收到非法类型时，底层 `_publish_data` → `_infer_record_count` 已经会抛 `TypeError`，类型注解只是提前到静态检查层。

**README 更新**：在 inject_sender 示例旁补类型注解用法：

```python
from pulsemq import PulsePublisher, PublisherSender

@pub.producer(name="market", interval=1.0, inject_sender=True)
async def market(sender: PublisherSender) -> None:
    await sender.send({"symbol": "600000", "price": 10.5})
```

## 改动文件清单

| 文件 | 改动类型 | 内容 |
|------|----------|------|
| `src/pulsemq/producers/types.py` | 新建 | PubData、3 个回调别名 |
| `src/pulsemq/producers/manager.py` | 修改 | 删除本地别名，导入 types；start_all/_run_loop/_run_burst_loop 加类型；新增 OnMessageCallback/SenderFactory 别名 |
| `src/pulsemq/publisher.py` | 修改 | PublisherSender 加类型；_make_sender/_on_produce 加类型；producer/burst_producer/register_producer 加 @overload；__all__ 增加 PublisherSender |
| `src/pulsemq/__init__.py` | 修改 | 导出 PublisherSender、PubData |
| `tests/test_producer_types.py` | 新建 | 类型注解防回归测试 |
| `README.md` | 修改 | inject_sender 示例补类型注解用法 |

## 验证计划

- 运行时：全量 `pytest` 通过（现有 e2e 覆盖 `inject_sender` 真实路径）
- 静态：mypy/pyright 能识别 `@pub.producer(inject_sender=True)` 注入的 sender 类型（手动验证）
- 导入：`from pulsemq import PublisherSender, PubData` 成功

## 风险与回滚

- **风险极低**：纯类型注解增强，运行时行为零变化
- **回滚**：`git revert` 单次提交即可
