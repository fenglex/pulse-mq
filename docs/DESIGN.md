# PulseMQ 模块设计文档

> 版本：v3.2.2 ｜ 最后更新：2026-06-25
>
> 本文档按模块分节，描述 pulse-mq 全部模块的职责、核心类与接口、关键数据结构、以及模块间的依赖关系。面向项目维护者，帮助理解架构与定位代码。

---

## 目录

1. [总体架构](#1-总体架构)
2. [配置模块 `config.py`](#2-配置模块-configpy)
3. [协议模块 `protocol/`](#3-协议模块-protocol)
   - 3.1 帧编解码 `protocol/frames.py`
   - 3.2 序列化 `protocol/serialization.py`
   - 3.3 压缩 `protocol/compression.py`
   - 3.4 flags 位域 `protocol/flags.py`
   - 3.5 消息类型常量 `protocol/msg_type.py`
4. [Producer 管线 `producers/`](#4-producer-管线-producers)
   - 4.1 类型定义 `producers/types.py`
   - 4.2 Producer 调度 `producers/manager.py`
5. [发布端核心 `publisher.py`](#5-发布端核心-publisherpy)
6. [传输层 `transport/zmq_pub.py`](#6-传输层-transportzmq_pubpy)
7. [订阅端 `subscriber.py`](#7-订阅端-subscriberpy)
8. [缓存模块 `cache/topic_buffer.py`](#8-缓存模块-cachetopic_bufferpy)
9. [统计模块 `stats/`](#9-统计模块-stats)
   - 9.1 内存统计 `stats/traffic.py`
   - 9.2 持久化 `stats/storage.py`
10. [管理后台 `admin/`](#10-管理后台-admin)
    - 10.1 HTTP 服务 `admin/server.py`
    - 10.2 Web UI `admin/web_ui.py`
11. [包入口 `__init__.py`](#11-包入口-__init__py)
12. [附录：端到端数据流](#12-附录端到端数据流)

---

## 1. 总体架构

pulse-mq 是一个**高性能纯 pub→sub 消息系统**，基于 ZeroMQ，无中间 broker。核心特征：

- **零 broker**：PUB socket 直接广播给 SUB，无路由代理，延迟低
- **单进程 asyncio**：publisher 的 producer 调度、统计、心跳、admin 服务全在同一个 asyncio 事件循环（单线程，同步函数不会互相抢占）
- **类型保真（v3）**：协议层记录原始 Python 类型 (`data_type` 字段)，订阅端能还原（如 `DataFrame`→`DataFrame`，而非降级为 `list[dict]`）
- **强类型绑定**：数据类型与序列化器一一对应，编译期即可发现配置错误（如 `str` 数据误配 `msgpack` 序列化器→抛 `TypeError`）
- **可观测**：内置 admin Web UI + REST + SSE，实时监控流量、缓存、系统状态；认证/连接事件直接 `print` 到 stderr，不依赖 logging 配置

### 模块依赖关系

```
                    ┌─────────────┐
                    │  config.py  │ （环境变量加载，纯数据）
                    └──────┬──────┘
                           │
        ┌──────────────────┼──────────────────┐
        ▼                  ▼                  ▼
  ┌──────────┐      ┌────────────┐     ┌───────────┐
  │ protocol │◄─────│ publisher  │────►│ transport │
  │  (帧/编解码)│     │   (核心)   │     │ (ZMQ PUB) │
  └────┬─────┘      └─────┬──────┘     └───────────┘
       │                  │
       │            ┌─────┼──────┬──────────┐
       │            ▼     ▼      ▼          ▼
       │      ┌──────┐┌──────┐┌──────┐┌──────────┐
       │      │producers│stats│ cache │  admin   │
       │      └──────┘└──────┘└──────┘└──────────┘
       │                                    │
       └────────────────────────────────────┘
              subscriber.py（独立客户端，仅依赖 protocol 解码）
```

**依赖方向（严格单向，无循环导入）**：
- `publisher.py` 依赖所有其他模块（它是编排中心）
- `protocol/` 是最底层，不依赖任何业务模块
- `producers/` 依赖 `protocol/`（类型引用），`types.py` 用字符串前向引用避免循环导入
- `transport/` 仅依赖 `zmq`，不依赖 `protocol/`（只传输 raw frames）
- `cache/` 独立，只存 raw frames
- `stats/` 独立，只记录数值
- `admin/` 依赖 `cache/`、`stats/`（读取数据展示）
- `subscriber.py` 仅依赖 `protocol/frames.py` 和 `protocol/msg_type.py`

### 运行时组件拓扑

publisher 单进程内运行 5 类 asyncio 任务：

| 任务 | 触发 | 职责 |
|------|------|------|
| producer 循环（N 个） | 每个 producer 一个 task | 按间隔/burst 调度回调，产出数据 |
| 分钟滚动循环 | 每 60s | 归档统计到滚动窗口 + 落库 SQLite |
| 心跳循环 | 每 `heartbeat_interval`（默认 30s） | 广播 PING 帧给所有订阅端 |
| admin 服务 | 常驻 | HTTP/SSE 服务 |
| 主循环 | 常驻 | `while running: sleep(1)` 保活 |

---

## 2. 配置模块 `config.py`

**职责**：加载 publisher 配置，优先级为「环境变量 > 代码参数 > 默认值」。纯数据模块，无副作用。

**核心类**：

```python
@dataclass
class PublisherConfig:
    bind: str = "tcp://*:5555"               # ZMQ PUB 绑定地址
    admin_bind: str = "0.0.0.0:9090"         # Admin 后台绑定地址
    stats_db: str = "pulse_stats.db"         # 统计 SQLite 路径
    stats_retention_minutes: int = 480        # 内存统计窗口（8 小时）
    heartbeat_interval: float = 30.0          # 心跳间隔（秒），<=0 禁用
    api_keys_str: str = ""                    # API Keys，空=关闭认证
```

**关键接口**：
- `PublisherConfig.api_keys`（property）：解析 `api_keys_str`（`"user1:pass1,user2:pass2"` 格式）为 `{username: password}` 字典
- `load_config()`：遍历 `_ENV_MAP`，用环境变量覆盖对应字段。支持的环境变量：`PULSEMQ_BIND`、`PULSEMQ_ADMIN_BIND`、`PULSEMQ_STATS_DB`、`PULSEMQ_API_KEYS`

**依赖**：无（仅 stdlib dataclasses）。

---

## 3. 协议模块 `protocol/`

**职责**：定义消息帧格式、序列化、压缩、flags 编码。所有与「数据如何在网络上表示」的逻辑都集中在此。

### 3.1 帧编解码 `protocol/frames.py`

**职责**：定义 4 帧格式并提供 `encode` / `decode` / `encode_heartbeat`。

**帧格式（v3）**：

```
┌─────────┬───────────┬───────────┬─────────┐
│ Frame 1 │  Frame 2  │  Frame 3  │ Frame 4 │
│  topic  │  meta(7B) │ ts(8B)    │ payload │
│ UTF-8   │           │ int64 BE  │ 变长    │
└─────────┴───────────┴───────────┴─────────┘
```

**meta 帧布局（7 字节）**：

| 字节 | 字段 | 编码 | 说明 |
|------|------|------|------|
| 0 | `msg_type` | uint8 | `0x01`=DATA，`0x02`=PING |
| 1 | `flags` | bitfield | 低 3 位=序列化，bit[3:4]=压缩 |
| 2 | `data_type` | uint8 | `0x00`=UNKNOWN, `0x01`=DICT, `0x02`=DATAFRAME, `0x03`=STR, `0x04`=BYTES |
| 3-6 | `record_count` | big-endian uint32 | 本帧记录数，上限 1,000,000 |

**核心数据结构**：

```python
@dataclass
class PulseMessage:
    """订阅端解码后的消息。"""
    topic: str
    payload: Any              # 解码后数据（已按 data_type 还原原始 Python 类型）
    raw_payload: bytes        # 解压后、反序列化前的原始字节（调试用）
    record_count: int
    timestamp_ns: int         # 纳秒精度时间戳
    serializer: str           # 使用的序列化器名称
    compression: str          # 使用的压缩器名称
    data_type: int = DataType.UNKNOWN  # 原始数据类型标记（v3 新增）
```

**核心接口**：

```python
def encode(topic, data, serializer="msgpack", compression="none",
           record_count=1, data_type=DataType.UNKNOWN) -> list[bytes]:
    """编码数据为 4 帧。
    1. 从注册表获取序列化器，serialize(data) → bytes
    2. 从注册表获取压缩器，compress(bytes) → payload
    3. 组装 [topic_bytes, meta(7B), ts(8B), payload]
    """

def decode(frames: list[bytes]) -> PulseMessage:
    """解码 4 帧为 PulseMessage。
    1. 拆分 4 帧，从 meta 提取 msg_type / flags / data_type / record_count
    2. decode_flags → serializer/compression 名称
    3. decompress → deserialize → _restore_type() 还原原始类型
    4. 返回 PulseMessage
    """

def encode_heartbeat() -> list[bytes]:
    """生成 PING 心跳帧：topic=b"__pulse_hb__"，meta 统一 7 字节，空 payload。
    v3 修复：补写 data_type 字节，此前漏写导致 sub 端 decode 越界崩溃。
    """
```

**类型还原 `_restore_type(payload, data_type)`（v3 核心）**：

| data_type | 还原逻辑 |
|-----------|---------|
| `STR` / `BYTES` / `UNKNOWN` | 原样返回（序列化器天然保真） |
| `DATAFRAME` | `list[dict]`（msgpack/json 路径）或 `pa.Table`（pyarrow 路径）→ `pd.DataFrame` |
| `DICT` | `pa.Table`（1 行）→ 单个 `dict` |

**关键设计**：
- `encode` 调用链：`serialize` → `compress` → 组帧，timestamp 在 `encode()` 内部生成（`time.time_ns()`），而非由调用方传入，保证时间戳与实际编码时刻一致
- `record_count` encode 时 `& 0xFFFFFFFF` 掩码；decode 侧信任 4 字节不做边界校验
- `_TS_STRUCT = struct.Struct(">q")`：大端 int64，纳秒精度
- 心跳帧复用同一 7 字节 meta 布局（`msg_type=PING`，`data_type=UNKNOWN`，`record_count=0`）

**依赖**：`protocol.compression`、`protocol.serialization`、`protocol.flags`、`protocol.msg_type`。

### 3.2 序列化 `protocol/serialization.py`

**职责**：序列化注册表 + 5 种内置实现。

**抽象接口**：

```python
class Serializer(ABC):
    def serialize(self, obj: Any) -> bytes: ...
    def deserialize(self, data: bytes) -> Any: ...
```

**内置实现**：

| 类 | 注册名 | 后端 | 特性 |
|----|--------|------|------|
| `StringSerializer` | `str` | UTF-8 | str↔bytes，bytes 透传 |
| `BytesSerializer` | `bytes` | 无 | 纯字节透传 |
| `MsgpackSerializer` | `msgpack` | msgspec.msgpack | 通用二进制，dict/list 标量 |
| `JsonSerializer` | `json` | stdlib json | 拒绝 bytes（避免 base64→str 变形）|
| `PyArrowSerializer` | `pyarrow` | pyarrow IPC | 支持 DataFrame/Table/dict/list[dict]，**可选依赖** |

**注册表接口**：`register(name, serializer)`、`get(name)`（未注册抛 `KeyError`）、`available()`。

**关键设计**：
- pyarrow 用 `try/except ImportError` 守卫，未安装时跳过注册，不影响库导入（**降级处理，与压缩模块不对称——见 3.3 注意事项**）
- msgspec 采用函数内 lazy import（`import msgspec` 在 `serialize()`/`deserialize()` 方法内），延迟加载
- `JsonSerializer` 显式拒绝 `bytes` 数据，防止 Python json 模块将其 base64 编码为字符串造成静默类型变形

**依赖**：msgspec（硬依赖）、pyarrow（可选）。

### 3.3 压缩 `protocol/compression.py`

**职责**：压缩注册表 + 4 种内置实现。

**抽象接口**：

```python
class Compressor(ABC):
    def compress(self, data: bytes) -> bytes: ...
    def decompress(self, data: bytes) -> bytes: ...
```

**内置实现**：

| 类 | 注册名 | 后端 |
|----|--------|------|
| `NoneCompressor` | `none` | 无（透传）|
| `SnappyCompressor` | `snappy` | python-snappy |
| `Lz4Compressor` | `lz4` | lz4.frame |
| `ZstdCompressor` | `zstd` | zstandard |

**注册表接口**：与序列化模块对称（`register` / `get` / `available`）。

**⚠️ 注意事项**：snappy/lz4/zstd 在各自的 `__init__` 里直接 `import snappy` / `import lz4.frame` / `import zstandard`，`_init_builtins()` 在模块加载时即实例化并注册，**没有 ImportError 守卫**。当前 pyproject.toml 把三者列为硬依赖所以能运行，但若改为可选依赖会导致 `import pulsemq` 崩溃。与序列化层的 pyarrow 守卫做法不一致，是已知的健壮性隐患。

**依赖**：python-snappy、lz4、zstandard（均为硬依赖）。

### 3.4 flags 位域 `protocol/flags.py`

**职责**：把（序列化格式, 压缩算法）编码为单字节 bitfield。

**位布局**：

```
bit:  7  6  5  4  3  2  1  0
      └reserved┘ └comp┘ └─ser─┘
         (3位)   (2位)  (3位)
```

**编码映射**：

- 序列化（低 3 位）：
  - `000` = msgpack
  - `001` = bytes
  - `010` = pyarrow
  - `100` = str
  - `101` = json
- 压缩（bit[3:4]）：
  - `00` = none
  - `01` = snappy
  - `10` = lz4
  - `11` = zstd

**接口**：
- `encode_flags(serializer_name, compression_name) -> int`
- `decode_flags(byte_val) -> tuple[str, str]`

5 序列化 × 4 压缩 = 20 种合法组合，编码成 20 个互不冲突字节，可无损往返。

**依赖**：`protocol.serialization`、`protocol.compression`（仅查注册表编号，不实际调用序列化/压缩）。

### 3.5 消息类型常量 `protocol/msg_type.py`

**职责**：定义两个常量类，无逻辑。

```python
class MsgType:
    DATA = 0x01   # 业务数据帧
    PING = 0x02   # 心跳帧

class DataType:
    UNKNOWN   = 0x00  # 兜底，无法推断时使用
    DICT      = 0x01
    DATAFRAME = 0x02
    STR       = 0x03
    BYTES     = 0x04
```

**依赖**：无。

---

## 4. Producer 管线 `producers/`

**职责**：定义 producer 回调类型 + 调度 producer 任务。这是「数据从哪里来」的抽象层。

### 4.1 类型定义 `producers/types.py`

**职责**：producer 管线的类型单一来源。

**核心类型**：

```python
# 数据白名单（4 种）：list 已移除
PubData: TypeAlias = Union[pd.DataFrame, dict, bytes, str]

# 回调类型：无 sender 注入
SimpleProducerCallback = Callable[[], Awaitable[PubData | None]]

# 回调类型：有 sender 注入（PublisherSender 用字符串前向引用，避免循环导入）
SenderProducerCallback = Callable[["PublisherSender"], Awaitable[None]]

# 联合类型
ProducerCallback = Union[SimpleProducerCallback, SenderProducerCallback]
```

**关键设计**：
- 数据白名单仅 4 种：`pd.DataFrame` / `dict` / `str` / `bytes`。`list` 已在 v3.2.x 移除（此前 list 无法保真其元素类型）
- `PublisherSender` 用**字符串前向引用**（`"PublisherSender"`），`types.py` 不 import `publisher.py`，避免循环导入（publisher → producers.manager → types → publisher 的环）
- 文件开头 `from __future__ import annotations` 保证所有注解惰性求值

**依赖**：pandas（硬依赖，仅用作类型标注）。

### 4.2 Producer 调度 `producers/manager.py`

**职责**：注册 producer 规格并按调度策略运行 asyncio task。

**核心数据结构**：

```python
@dataclass
class ProducerSpec:
    """单个 producer 的配置。"""
    name: str                       # topic 名（同时也是 producer 名）
    callback: ProducerCallback      # async 回调
    interval: float = 5.0           # 推送间隔（秒）；0.0 = burst 模式
    cache_size: int = 100_000       # 环形缓存大小（按记录数）
    serializer: str = "msgpack"     # 序列化格式
    compression: str = "none"       # 压缩格式
    inject_sender: bool = False     # 是否向回调注入手动发送端 (PublisherSender)

# 管理器内部使用的回调别名（定义在 ProducerSpec 之后，确保右侧类型已定义）
OnMessageCallback = Callable[[ProducerSpec, PubData], Awaitable[None]]
SenderFactory = Callable[[ProducerSpec], "PublisherSender"]
```

**核心类**：

```python
class ProducerManager:
    """管理所有注册的 producer：回调注册 + 并发调度。"""
    def __init__()
    def register(callback, name, interval, cache_size, serializer, compression, inject_sender)
    def register_burst(callback, name, cache_size, serializer, compression, inject_sender)
    async def start_all(on_message, sender_factory=None)
    async def stop_all()
```

**调度策略**：

| 模式 | 条件 | 行为 |
|------|------|------|
| 固定间隔 | `interval > 0` | `执行 → sleep(interval - elapsed)`，不积压（超时则 `sleep(0)` 让出控制权） |
| Burst | `interval == 0.0` | 无间隔连续执行，回调返回 `None` 即结束；异常冷却 0.1s 避免空转 |

**`_run_loop` 实现细节**：

```python
async def _run_loop(self, spec, on_message, sender_factory):
    while self._running:
        start = time.monotonic()
        try:
            if spec.inject_sender:
                data = await spec.callback(sender_factory(spec))
            else:
                data = await spec.callback()
            if data is not None:
                await on_message(spec, data)
        except asyncio.CancelledError:
            break
        except Exception:
            logger.warning("Producer %s 回调异常", spec.name, exc_info=True)
        elapsed = time.monotonic() - start
        sleep_time = max(0.0, spec.interval - elapsed)
        await asyncio.sleep(sleep_time if sleep_time > 0 else 0)
```

**`_run_burst_loop` 实现细节**：

```python
async def _run_burst_loop(self, spec, on_message, sender_factory):
    while self._running:
        try:
            if spec.inject_sender:
                data = await spec.callback(sender_factory(spec))
            else:
                data = await spec.callback()
            if data is None:
                break  # 回调主动结束
            await on_message(spec, data)
        except asyncio.CancelledError:
            break
        except Exception:
            logger.warning("Burst Producer %s 回调异常", spec.name, exc_info=True)
            await asyncio.sleep(0.1)  # 硬编码冷却，不可配置
```

**`inject_sender` 分发**：回调签名按 `spec.inject_sender` 决定是否传入 `sender_factory(spec)`。若 `inject_sender=True` 但 `sender_factory is None`，抛 `RuntimeError`。

**容错**：回调异常只记 warning 日志，不崩溃循环。

**`stop_all`**：取消所有 task → `asyncio.gather(*tasks, return_exceptions=True)` 等待完成 → 清空 task 字典。

**依赖**：`producers.types`。

---

## 5. 发布端核心 `publisher.py`

**职责**：单进程 publisher 的编排中心。串联 producer 调度、帧编码、传输、缓存、统计、admin。这是整个系统的「主控制器」。

**核心类**：

```python
class PulsePublisher:
    # ---- 构造 ----
    def __init__(self, config=None, *, bind=None, admin_bind=None, api_keys=None, on_auth=None)

    # ---- Producer 注册（3 种方式，均支持 inject_sender，用 @overload 绑定类型） ----
    def producer(name, *, interval, cache_size, serializer, compression, inject_sender)
        → Callable[[Callback], Callback]      # 装饰器：注册固定间隔 producer
    def burst_producer(name, *, cache_size, serializer, compression, inject_sender)
        → Callable[[Callback], Callback]      # 装饰器：注册 burst producer
    def register_producer(fn, *, name, interval, ...)  # 函数式注册

    # ---- API Key 管理 ----
    def add_api_key(username, password)
    def set_auth_callback(callback)

    # ---- 生命周期 ----
    def start()              # 阻塞启动（asyncio.run）
    async def start_async()  # 异步启动（嵌入其他 asyncio 程序）
    async def _run()         # 主运行循环（初始化 + 保活 + 优雅关闭）
    async def _shutdown(roll_task)  # 资源释放（先停 producer，再归档统计，最后停 admin/transport/storage）

    # ---- 手动发送端 ----
    class PublisherSender:
        """注入 producer 回调的手动发送端。"""
        async def send(data, *, topic=None, serializer=None, compression=None)
```

### 构造参数

| 参数 | 类型 | 说明 |
|------|------|------|
| `config` | `PublisherConfig \| None` | 配置对象，未传则调用 `load_config()` 从环境变量加载 |
| `bind` | `str \| None` | 覆盖 config.bind |
| `admin_bind` | `str \| None` | 覆盖 config.admin_bind |
| `api_keys` | `dict[str,str] \| None` | 编程式 API Key 字典，优先级高于 config.api_keys_str |
| `on_auth` | `AuthCallback \| None` | 认证事件回调 `async (username, addr, success)` |

### 生命周期 `_run()` 详解

```python
async def _run(self):
    self._running = True
    self._start_time = time.time()
    api_keys = self._explicit_api_keys or self._config.api_keys

    # 初始化阶段创建的资源（transport/storage/admin/tasks）必须在 try 块内，
    # 这样任一初始化步骤抛异常时 finally 的 _shutdown() 仍会执行，释放已创建的资源。
    roll_task = hb_task = None
    try:
        # 1. 初始化传输层 (ZMQ context + PUB socket + 可选 ZAP handler)
        self._transport = ZmqPubTransport(self._config.bind, api_keys, self._on_auth)
        await self._transport.start()

        # 2. 初始化统计存储 (SQLite WAL 连接 + 建表)
        self._storage = StatsStorage(self._config.stats_db)
        self._storage.connect()

        # 3. 为所有 producer 创建 topic 缓存
        for name, spec in self._producer_mgr.specs.items():
            self._buffers.get_or_create(name, spec.cache_size)

        # 4. 初始化 Admin 后台
        self._admin = AdminServer(self._config.admin_bind, self._traffic,
                                   self._buffers, self._storage,
                                   self._system_snapshot, self._start_time)
        await self._admin.start()

        # 5. 启动后台任务
        roll_task = asyncio.create_task(self._minute_roll_loop())
        if self._config.heartbeat_interval > 0:
            hb_task = asyncio.create_task(self._heartbeat_loop(), name="heartbeat")

        # 6. 启动所有 producer
        await self._producer_mgr.start_all(self._on_produce, self._make_sender)

        # 7. 保活主循环
        while self._running:
            await asyncio.sleep(1)
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        if hb_task is not None:
            hb_task.cancel()
            try: await hb_task
            except asyncio.CancelledError: pass
        await self._shutdown(roll_task)
```

**`_shutdown` 关闭顺序（关键）**：

```python
async def _shutdown(self, roll_task):
    self._running = False                         # 1. 停止标志
    await self._producer_mgr.stop_all()           # 2. 先停所有 producer（停止产生新消息）
    if roll_task is not None:
        roll_task.cancel(); await roll_task        # 3. 停分钟滚动
    archived = self._traffic.roll_minute()         # 4. 最后归档统计数据
    if self._storage and archived:
        self._storage.save_minutes_batch(archived) # 5. 落库 SQLite
    if self._admin: await self._admin.stop()       # 6. 停 Admin（先于 transport）
    if self._transport: await self._transport.stop() # 7. 停 ZMQ（LINGER=1000ms 用于刷新缓冲）
    if self._storage: self._storage.close()         # 8. 关 SQLite 连接
```

### 发布管线 `_publish_data()`（6 步）

```python
async def _publish_data(self, *, topic, data, cache_size, serializer, compression):
    # 1. 类型白名单 + record_count 推断
    record_count = self._infer_record_count(data)

    # 2. 强类型绑定校验（str→str, bytes→bytes, DataFrame/dict→msgpack/json/pyarrow）
    self._validate_serializer(data, serializer)

    # 3. 推断原始数据类型标记（v3：供 sub 端还原）
    data_type = self._infer_data_type(data)

    # 4. DataFrame 预处理：to_dict("records") → list[dict]
    payload_obj = self._prepare_payload(data)

    # 5. 序列化 + 压缩 + 组帧
    encoded_frames = frame_codec.encode(
        topic, payload_obj, serializer=serializer,
        compression=compression, record_count=record_count, data_type=data_type)

    # 6. 并行分发
    await self._transport.send(encoded_frames)                   # a. ZMQ PUB 广播
    ts_ns = frame_codec._TS_STRUCT.unpack(encoded_frames[2])[0]  # 从帧中读取实际时间戳
    self._buffers.get_or_create(topic, cache_size).append(       # b. 追加缓存
        ts_ns, encoded_frames, record_count)
    self._traffic.record(topic, record_count, len(encoded_frames[3]))  # c. 记录统计
```

### 核心静态方法

**`_infer_record_count(data)`**：推断记录数
- `pd.DataFrame` → `len(df)`（行数）
- `dict` / `str` / `bytes` → 1
- `list` → 显式抛 `TypeError`（不再支持）
- 其他（标量 int/float、pa.Table、set 等）→ 抛 `TypeError`

**`_infer_data_type(data)`**（v3 新增）：推断 DataType 常量
- `pd.DataFrame` → `DataType.DATAFRAME`
- `dict` → `DataType.DICT`
- `str` → `DataType.STR`
- `bytes` → `DataType.BYTES`
- 其他 → `DataType.UNKNOWN`

**`_validate_serializer(data, serializer)`**：强类型绑定校验
- `str` 数据 → 只允许 `serializer='str'`，否则抛 `TypeError`
- `bytes` 数据 → 只允许 `serializer='bytes'`，否则抛 `TypeError`
- `DataFrame` / `dict` → 只允许 `msgpack` / `json` / `pyarrow`，否则抛 `TypeError`

**`_prepare_payload(data)`**：DataFrame → `data.to_dict(orient="records")`，其余原样返回

### PublisherSender（inject_sender 模式）

```python
class PublisherSender:
    """注入 producer 回调的手动发送端。

    持有 publisher 引用 + producer spec，send() 默认沿用 producer spec 配置，
    可按消息覆盖 topic/serializer/compression。
    """
    async def send(self, data, *, topic=None, serializer=None, compression=None):
        await self._publisher._publish_data(
            topic=topic or self._spec.name,
            data=data,
            cache_size=self._spec.cache_size,
            serializer=serializer or self._spec.serializer,
            compression=compression or self._spec.compression,
        )
```

### 心跳循环 `_heartbeat_loop()`

```python
async def _heartbeat_loop(self):
    while self._running:
        await asyncio.sleep(self._config.heartbeat_interval)
        if not self._running: break
        try:
            frames = encode_heartbeat()
            if self._transport is not None:
                await self._transport.send(frames)
        except asyncio.CancelledError: break
        except Exception: logger.warning("心跳发送异常", exc_info=True)
```

### 分钟滚动循环 `_minute_roll_loop()`

```python
async def _minute_roll_loop(self):
    while self._running:
        now = time.time()
        next_minute = (int(now) // 60 + 1) * 60
        await asyncio.sleep(next_minute - now)  # 对齐到整分钟
        if not self._running: break
        archived = self._traffic.roll_minute()
        if self._storage and archived:
            self._storage.save_minutes_batch(archived)
        if int(next_minute) % 3600 < 70:  # 每小时触发一次清理
            if self._storage: self._storage.cleanup()
```

### 启动信息格式化 `format_startup_table()`

```python
def format_startup_table(cfg, api_keys=None, version=__version__) -> str:
    """生成 ASCII 表格，输出 bind、admin_bind、auth 状态。

    auth 脱敏显示：最多展示 10 个用户名，超过则截断（前 5 个 + "+N more"）。
    """
```

### CLI 入口 `main()`

```python
def main():
    """CLI 入口点，由 pyproject.toml [project.scripts] 注册为 pulse-mq 命令。"""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s", stream=sys.stderr)
    pub = PulsePublisher()
    print(format_startup_table(pub._config, pub._explicit_api_keys), file=sys.stderr)
    print("用法: 参考 PulsePublisher 文档注册 producer", file=sys.stderr)
    pub.start()  # 阻塞运行
```

**依赖**：`transport.ZmqPubTransport`、`producers.ProducerManager`、`cache.TopicBufferRegistry`、`stats.TrafficStats`、`stats.StatsStorage`、`admin.AdminServer`、`protocol.frames`、`config.PublisherConfig`。

---

## 6. 传输层 `transport/zmq_pub.py`

**职责**：封装 ZMQ PUB socket + 可选 PLAIN 认证（ZAP handler）。强调容错性和可观测性，面向运维场景设计。

**类型别名**：

```python
AuthCallback = Callable[[str, str, bool], Awaitable[None]]
# async def callback(username: str, client_addr: str, success: bool) -> None
```

### 6.1 ZmqPubTransport — PUB socket 封装

```python
class ZmqPubTransport:
    def __init__(self, bind="tcp://*:5555", api_keys=None, on_auth=None)
    async def start()       # 创建 ctx + PUB socket，可选启动 ZAP，bind
    async def send(frames)  # 广播一帧给所有 SUB
    async def stop()        # 关 PUB → 停 ZAP → term ctx
    def set_auth_callback(callback)
```

**`start()` 流程**：
1. 创建 `zmq.asyncio.Context()`
2. 创建 `zmq.PUB` socket
3. 设置 `SNDHWM=0`（发送缓冲无上限，burst 模式不丢消息）
4. 设置 `LINGER=1000`（关闭时等待 1 秒刷新缓冲）
5. 若有 api_keys：**ZAP handler 必须在 PUB bind 之前启动**（否则 SUB 连接时 ZAP 还没就绪）
6. `self._pub.bind(self._bind)`

**`stop()` 流程**：
1. `self._pub.close(linger=1000)` → 关 PUB socket
2. `await self._zap.stop()` → 停 ZAP handler
3. `self._ctx.term()` → 销毁 ZMQ context

### 6.2 AsyncZAPHandler — PLAIN 认证处理器

```python
class AsyncZAPHandler:
    def __init__(self, api_keys: dict[str,str], ctx: zmq.asyncio.Context, on_auth=None)
    async def start()       # 绑定 inproc://zeromq.zap.01，启动 _loop task
    async def stop()        # cancel task → close ZAP socket
    async def _loop()       # 循环 recv ZAP 请求 → 校验 → 发响应
    async def _send_zap_reply(request_id, status_code, status_text, ...)  # 统一 await + 异常保护
    async def _notify_auth(username, client_addr, success)  # 调用回调，异常不影响认证
```

**认证流程（`_loop`）**：

```
1. recv_multipart() 收到 ZAP 请求（7-8 帧）
   帧: [version, request_id, domain, address, identity, mechanism, username, password]
2. 校验帧数 < 7 → 400 Invalid ZAP request
3. 提取 mechanism
   ├─ mechanism != b"PLAIN" → _notice() + 400 Not PLAIN
   └─ mechanism == b"PLAIN" → 进入白名单校验
4. 白名单校验: api_keys[username] == password
   ├─ 匹配 → _notice("[SUB 上线] auth=OK") + _notify_auth(True) + 200 OK
   └─ 不匹配 → _notice("[SUB 认证失败] auth=FAIL") + _notify_auth(False) + 400 Invalid credentials
```

**`_notice()` 辅助函数**（关键设计）：

```python
def _notice(msg: str) -> None:
    """关键认证事件直接输出到 stderr，不依赖用户配置 logging。

    Python 默认 logging 无 handler 时，info/warning 级日志会被 lastResort 吞掉，
    导致用户看不到 sub 上线/认证失败提示。print 到 stderr 保证始终可见。
    """
    print(msg, file=sys.stderr, flush=True)
```

**`_send_zap_reply()` 历史修复**：
- **v3.1.1 之前**：`send_multipart` 返回协程未 `await`，响应永不发送，SUB 认证永久挂死
- **v3.1.1 之前**：send 异常未保护，一次失败让 `_loop` 整体退出，ZAP task 静默死亡，后续所有 SUB 认证全部失效
- **v3.1.1 修复**：统一 `await` + `try/except`，单次 send 失败仅记日志、不影响循环

**认证事件回调 `_notify_auth()`**：
- 调用 `self._on_auth(username, client_addr, success)`（若设置）
- 回调异常不传播，仅记 `logger.warning`

**依赖**：pyzmq（asyncio）。

---

## 7. 订阅端 `subscriber.py`

**职责**：独立客户端，连接 PUB socket 订阅消息。与 publisher 完全解耦，仅依赖 protocol 解码。

**核心类**：

```python
class PulseSubscriber:
    def __init__(address, *, username="", password="")
    async def connect()                       # 创建 ctx + SUB socket，可选 PLAIN 认证 + monitor
    async def subscribe(*topics) -> AsyncIterator[PulseMessage]  # 异步迭代器
    async def close()                         # 先关 monitor，再关 SUB，最后 term ctx
```

### 连接流程 `connect()`

1. 创建 `zmq.asyncio.Context()` + `zmq.SUB` socket
2. `RCVHWM=0`（接收缓冲无上限）
3. 设置 PLAIN 用户名/密码（可选）
4. **始终启用 monitor**：`socket.get_monitor_socket(_MONITOR_MASK)`
5. `socket.connect(address)`

### Monitor 事件掩码（始终监听）

```python
_MONITOR_MASK = (
    zmq.EVENT_HANDSHAKE_SUCCEEDED        # 握手成功（含 PLAIN 认证通过）
    | zmq.EVENT_HANDSHAKE_FAILED_AUTH     # 凭证错误
    | zmq.EVENT_HANDSHAKE_FAILED_PROTOCOL # 非 PLAIN 等协议失败
    | zmq.EVENT_HANDSHAKE_FAILED_NO_DETAIL # 其它握手失败
    | zmq.EVENT_DISCONNECTED              # 连接断开
)
```

### 订阅循环 `subscribe()`

```python
async def subscribe(self, *topics) -> AsyncIterator[PulseMessage]:
    # 1. 设置 SUBSCRIBE 过滤器（topic 前缀匹配）
    for t in topics:
        self._socket.setsockopt(zmq.SUBSCRIBE, t.encode())

    # 2. 启动后台 monitor task（监听握手/断开事件）
    self._event_task = asyncio.create_task(self._watch_events())

    # 3. 主循环：recv 与事件通知竞争（asyncio.wait FIRST_COMPLETED）
    while True:
        recv_task = asyncio.create_task(self._socket.recv_multipart())
        done, _ = await asyncio.wait(
            [recv_task, self._event_sig.wait()],
            return_when=asyncio.FIRST_COMPLETED
        )
        if self._event_sig.is_set():
            recv_task.cancel()
            kind = self._event_kind
            if kind == "ok":
                self._event_sig.clear()  # 握手成功，重置信号，继续接收
                continue
            elif kind in ("fail", "disconnect"):
                break  # 认证失败或断线 → 结束迭代

        # 4. 收到消息
        frames = recv_task.result()
        meta = frames[1]
        if meta[0] == MsgType.PING:
            continue  # 过滤 PING 心跳，不交付用户
        yield frame_codec.decode(frames)
```

### Monitor 事件处理 `_watch_events()`

- 持续 `recv_multipart()` monitor socket
- 事件码前 2 字节（小端 uint16）识别事件类型
- 按类型 `print` 到 stderr + 置位 `_event_sig`：
  - `HANDSHAKE_SUCCEEDED` → `[SUB 上线] 认证成功`，`kind="ok"`
  - `HANDSHAKE_FAILED_AUTH` → `[SUB 认证失败]`，`kind="fail"`
  - `HANDSHAKE_FAILED_PROTOCOL` / `NO_DETAIL` → `[SUB 认证失败]`，`kind="fail"`
  - `DISCONNECTED` → `[SUB 断线]`，`kind="disconnect"`

### 关闭顺序 `close()`（关键）

```python
async def close(self):
    if self._monitor:
        self._monitor.close(linger=0)   # 1. 先关 monitor socket
        self._monitor = None             #    （必须在 SUB 之前，否则 ctx.term() 卡死）
    if self._socket:
        self._socket.close(linger=1000)  # 2. 关 SUB socket
        self._socket = None
    if self._ctx:
        self._ctx.term()                 # 3. 销毁 context
        self._ctx = None
```

**⚠️ 历史坑位**：monitor socket 必须在 SUB socket 之前关闭，否则 `ctx.term()` 卡死（pyzmq 已知行为）。

**依赖**：pyzmq（asyncio）、`protocol.frames.decode`、`protocol.msg_type.MsgType`。

---

## 8. 缓存模块 `cache/topic_buffer.py`

**职责**：每个 topic 一个环形缓存，按**记录总数**淘汰，用于新订阅者补发历史消息。

**核心数据结构**：

```python
@dataclass
class CachedMessage:
    timestamp_ns: int
    frames: list[bytes]       # 原始 4 帧（可直接重发）
    record_count: int = 1

class TopicBuffer:
    """单个 topic 的环形缓存（按累计记录数淘汰）。

    淘汰策略：当 _total_records + record_count > max_records 时，
    从队首不断 popleft 直到能容纳。单帧即使超限也至少保留一帧（避免缓存为空）。
    """
    def append(timestamp_ns, frames, record_count=1)
    def snapshot(since_ns=0, limit=100) -> list[CachedMessage]
    @property size: int            # 帧数
    @property total_records: int   # 累计记录数
    @property max_records: int     # 记录数上限

class TopicBufferRegistry:
    """所有 topic 缓存的注册表。
    管理多个 TopicBuffer，Admin 后台通过此对象查询各 topic 缓存状态。
    """
    def get_or_create(topic, max_size=100_000) -> TopicBuffer
    def snapshot() -> dict[str, dict]  # 返回 {topic: {current, max, total}} 供 admin UI 展示
```

**设计要点**：
- 淘汰按「记录数」而非「帧数」——DataFrame 一批 1000 行占 1000 配额，而非仅计 1 帧
- `max_size` 由 producer 注册时的 `cache_size` 指定
- **已知局限性**：`get_or_create` 首次创建后，后续相同 topic 不同 `max_size` 的调用被静默忽略

**依赖**：无（仅 stdlib `collections.deque`）。

---

## 9. 统计模块 `stats/`

**职责**：分钟粒度流量统计，内存 8 小时窗口 + SQLite 持久化。

### 9.1 内存统计 `stats/traffic.py`

**职责**：每 topic 的分钟级时序数据，自动滚动淘汰过期分钟。

**核心数据结构**：

```python
@dataclass
class MinuteSlot:
    timestamp: int           # 整分钟秒（Unix timestamp 对齐到分钟）
    msg_count: int = 0       # 消息条数
    record_count: int = 0    # 记录数（DataFrame 行数累加）
    bytes_total: int = 0     # 总字节数

class TrafficStats:
    def __init__(retention_minutes=480)
    def record(topic, record_count, payload_size)    # 记录一条消息（实时更新当前分钟）
    def roll_minute() -> dict[str, MinuteSlot]        # 整分钟归档，返回待落库数据
    def get_history(topic, minutes=60) -> list[dict]  # 取最近 N 分钟历史
    def snapshot() -> dict                            # 实时快照（仅当前分钟累积）
    def all_topics_snapshot() -> dict                 # 完整快照（含 1min 滚动速率估算）
```

**内部结构**：
- `_current: dict[topic, MinuteSlot]`：当前分钟累积器（实时写入）
- `_slots: dict[topic, deque[MinuteSlot]]`：滚动窗口（deque maxlen=retention_minutes）
- `_current_minute: int`：当前整分钟秒

**分钟切换**：`roll_minute()` 把 `_current` 各 topic 的 MinuteSlot 归档进 `_slots`，清空 `_current`。`_ensure_current()` 是兜底，检测到分钟落后时自动先调 `roll_minute()`。

**1min 滚动速率估算**：`all_topics_snapshot()` 用「当前分钟累积 + 上一分钟按 `(60 - elapsed_seconds) / 60` 比例补齐」估算近 60 秒速率，提供更平滑的实时数据。

**线程模型**：文档标注「单写者 publisher + 多读者 admin，依赖 GIL」。实际为单线程 asyncio，同步函数不会互相抢占，无真实并发问题。

**依赖**：无（仅 stdlib `collections.deque` + `time`）。

### 9.2 持久化 `stats/storage.py`

**职责**：分钟统计的 SQLite 持久化，进程重启后可恢复历史图表。

**核心类**：

```python
class StatsStorage:
    def __init__(db_path="pulse_stats.db")
    def connect()                           # 建连接 + 建表（WAL 模式）
    def save_minute(topic, slot)            # 写一条
    def save_minutes_batch(data: dict)      # 批量写（topic → MinuteSlot）
    def load_history(topic, since_ts) -> list[dict]  # 按时间戳读历史
    def cleanup(retention_days=7) -> int    # 清理过期数据，返回删除行数
    def close()
```

**表结构**：

```sql
CREATE TABLE IF NOT EXISTS minute_stats (
    topic        TEXT    NOT NULL,
    timestamp    INTEGER NOT NULL,
    msg_count    INTEGER DEFAULT 0,
    record_count INTEGER DEFAULT 0,
    bytes_total  INTEGER DEFAULT 0,
    PRIMARY KEY (topic, timestamp)
);
```

**设计要点**：
- WAL 模式提升并发读写
- 所有查询用 `?` 占位符参数化（防 SQL 注入，topic 名来自 producer 可含特殊字符）
- 速率计算：`record_count / 60.0`、`bytes_total / 60.0`，除以常量 `60.0`，无除零风险
- 落库策略：`roll_minute()` 之后同步写入（注释称"异步"，实际 `execute` 同步，会短暂阻塞事件循环，但 SQLite WAL 写入极快）

**依赖**：stdlib `sqlite3`。

---

## 10. 管理后台 `admin/`

**职责**：内置 HTTP + SSE + REST + Web UI，实时监控。基于 stdlib asyncio，手写 HTTP 解析，不引入框架。

### 10.1 HTTP 服务 `admin/server.py`

**职责**：接收 HTTP 请求、路由、提供 REST API 与 SSE 推送。

**核心类**：

```python
class AdminServer:
    def __init__(bind, traffic_stats, topic_buffers, stats_storage, snapshot_fn, start_time)
    async def start()  # asyncio.start_server + 启动 SSE 广播循环
    async def stop()   # 关闭 SSE 客户端 + server
```

**HTTP 解析 `_handle_request()`**：
- request line 超时：5s（`asyncio.wait_for(reader.readline())`）
- header 读取：逐行读到空行（`\r\n` 或 `\n`）
- body 读取：按 `Content-Length` 精确读取，超时 10s
- 异常处理：`TimeoutError` / `ConnectionResetError` / `BrokenPipeError` 静默关闭
- **所有非 SSE 连接**在 finally 中关闭 writer（`Connection: close`，无 HTTP keep-alive）
- SSE 连接通过 `writer._sse_takeover = True` 标记绕过 finally 的 close

**REST 端点**：

| 方法 | 路径 | 功能 | 说明 |
|------|------|------|------|
| GET | `/` | 深色 Web UI 首页 | 返回 `INDEX_HTML` |
| GET | `/static/{path}` | 静态资源 | ECharts 等，有路径穿越防护 |
| GET | `/api/v1/stats/realtime` | 实时指标 JSON | 所有 topic 的 1min 滚动速率 + 缓存大小 + 系统快照 |
| GET | `/api/v1/stats/stream` | SSE 实时推送 | `text/event-stream`，每 1s 推送一帧 |
| GET | `/api/v1/topics` | 所有 topic 列表 | 含 msg_rate_1min、当前累积指标、缓存状态 |
| GET | `/api/v1/topics/{topic}/history` | 分钟级历史 | 内存 + SQLite 合并，按 timestamp 去重排序 |
| GET | `/api/v1/system/status` | 系统状态 | version、start_time、uptime_seconds |
| GET | `/healthz` | 健康检查 | `{"status": "ok"}` |

**SSE 实现**：
- 每个 SSE 客户端分配一个 `asyncio.Queue(maxsize=64)` + writer task
- 广播循环 `_sse_broadcast_loop()`：每 1s 推一次 `_realtime_snapshot()` 到所有客户端队列
- 队列满（客户端断开/消费过慢）→ 主动取消该连接，从 `_sse_clients` 移除（避免死客户端残留内存泄漏）
- SSE writer task `_sse_writer`：从 queue 取数据 → `writer.write()` → `drain()`，空前全部断开时自动结束
- `_sse_id` 递增分配客户端 ID

**历史合并 `_topic_history()`**：
- 内存数据优先（`TrafficStats.get_history()`）
- 内存数据覆盖请求范围时（`len(mem_history) >= minutes - 1`，因为当前分钟未归档）直接返回
- 不足部分查 SQLite 补充（`StatsStorage.load_history()`）
- 按 timestamp 去重（内存数据优先级高于 SQLite，同一分钟以内存为准）
- 按 timestamp 升序排序

**路径穿越防护 `_route_static()`**：
```python
rel = path[len("/static/"):]
if ".." in rel.split("/") or rel.startswith("/") or "\\" in rel:
    → 400 Bad Request
full = STATIC_ROOT / rel
if not full.is_file() or not full.resolve().is_relative_to(STATIC_ROOT):
    → 404 Not Found
```

**安全性总结**：
- 静态资源路径双验证（字符串检测 + `Path.resolve().is_relative_to()` 二次验证）
- 请求超时保护：request line / header / body 都有 `wait_for` 超时
- 版本号从 `pulsemq._version` 统一读取，避免与包版本脱节
- **已知局限性**：Admin HTTP 端口无认证，任何人可访问

**依赖**：`cache.TopicBufferRegistry`、`stats.TrafficStats`、`stats.StatsStorage`、`admin.web_ui.INDEX_HTML`。

### 10.2 Web UI `admin/web_ui.py`

**职责**：单文件 HTML（内嵌 CSS + JS），提供深色玻璃态监控面板。

**内容**：
- 顶部导航栏：连接状态指示 + 版本号
- 指标卡片区：4 个渐变发光统计卡片（中文标签 + emoji），含消息速率 / 记录速率 / 字节速率 / Producer 数
- 图表区：ECharts 多 topic 流量曲线（分钟粒度），1H / 6H 时间范围切换，最多 5 个 topic 叠加（LRU 淘汰），SSE 实时更新
- 底部 topic 卡片网格：各 topic 当前指标
- 30 秒自动刷新历史（`setInterval`）

**实现**：`INDEX_HTML` 常量字符串，由 `admin/server.py` 的 `_respond_html` 返回。ECharts 通过 `/static/echarts.min.js` 提供。

**依赖**：无（纯静态资源，仅引用 Admin Server 的 REST/SSE 端点）。

---

## 11. 包入口 `__init__.py`

**职责**：统一导出公开 API + 设置 Windows 事件循环策略。

**导出**：

```python
from pulsemq.publisher import PulsePublisher, PublisherSender
from pulsemq.producers.types import PubData
from pulsemq.subscriber import PulseSubscriber
from pulsemq.protocol.frames import PulseMessage
from pulsemq.config import PublisherConfig, load_config

__all__ = [
    "PulsePublisher", "PulseSubscriber", "PulseMessage", "PublisherConfig",
    "PublisherSender", "PubData", "load_config",
]
```

**平台适配**：

```python
import sys, asyncio

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
```

**关键细节**：
- 事件循环修正**必须在 import 时、任何 asyncio 事件循环创建之前执行**
- pyzmq 的 asyncio 集成不支持 Windows 默认的 `ProactorEventLoop`，不切换会导致 SUB 端静默收不到消息
- **已知局限**：该修正是有副作用的全局操作，可能与宿主应用的已有事件循环策略冲突

**依赖**：`pulsemq.publisher`、`pulsemq.subscriber`、`pulsemq.protocol.frames`、`pulsemq.config`、`pulsemq.producers.types`。

---

## 12. 附录：端到端数据流

### 发布路径（producer → 网络）

```
用户 @pub.producer 回调返回 data (DataFrame/dict/str/bytes)
        │
        ▼
ProducerManager._run_loop / _run_burst_loop
   调用 on_message(spec, data)
   [inject_sender 模式: await callback(sender_factory(spec))]
        │
        ▼
PulsePublisher._on_produce(spec, data) → _publish_data(...)
   1. _infer_record_count(data)     → 记录数（DataFrame=行数, dict/str/bytes=1）
   2. _validate_serializer(data, serializer) → 强类型绑定校验
   3. _infer_data_type(data)        → DataType 常量
   4. _prepare_payload(data)        → DataFrame.to_dict("records")
   5. frame_codec.encode(topic, data, serializer, compression, record_count, data_type)
        │  ├─ serializer_registry.get(name).serialize(data)
        │  ├─ compressor_registry.get(name).compress(bytes)
        │  └─ 组装 [topic_bytes, meta(7B), ts(8B), payload]
   6a. transport.send(frames)        → ZMQ PUB send_multipart → 网络广播
   6b. buffers.get_or_create(topic, cache_size).append(ts_ns, frames, rc) → 缓存
   6c. traffic.record(topic, record_count, len(payload))                   → 统计
```

### 订阅路径（网络 → 用户）

```
ZMQ SUB socket recv_multipart → 4 帧
        │
        ▼
PulseSubscriber.subscribe() 循环
   ├─ 认证场景：recv 与 event_sig 竞争 (asyncio.wait FIRST_COMPLETED)
   │    ├─ event: "ok" → 重置信号，继续循环
   │    ├─ event: "fail" / "disconnect" → 结束迭代
   │    └─ recv 就绪 → 进入解码
   ├─ meta[0] == MsgType.PING? → 心跳帧，跳过
   └─ frame_codec.decode(frames):
        │  ├─ 拆分 4 帧: topic / meta(7B) / timestamp / payload
        │  ├─ decode_flags(meta[1]) → (serializer_name, compression_name)
        │  ├─ compressor.decompress(payload) → decompressed bytes
        │  ├─ serializer.deserialize(decompressed) → payload_obj
        │  └─ _restore_type(payload_obj, data_type)
        │       DATAFRAME → pd.DataFrame(list[dict] or pa.Table)
        │       DICT      → dict
        │       STR/BYTES → 原样
        ▼
yield PulseMessage  → 用户的 async for 循环
```

### 心跳路径

```
PulsePublisher._heartbeat_loop（asyncio Task, 每 heartbeat_interval 秒）
   └─ encode_heartbeat() → [b"__pulse_hb__", meta(7B: PING|msgpack|none|UNKNOWN|0), ts(8B), b""]
   └─ transport.send(frames) → PUB socket 广播
        │
   订阅端: meta[0] == MsgType.PING → 跳过，不交付用户
```

### 认证路径

```
SUB 连接 PUB (设置 PLAIN_USERNAME/PASSWORD)
        │
        ▼
ZMQ 内部 ZAP 协议触发
        │
        ▼
AsyncZAPHandler._loop() 收到 ZAP 请求
   ├─ mechanism != PLAIN → 400 Not PLAIN, _notice("auth=FAIL reason=not-PLAIN")
   ├─ api_keys[username] != password → 400 Invalid credentials, _notice("auth=FAIL")
   └─ api_keys[username] == password → 200 OK, _notice("auth=OK")
        │
        ▼
SUB 端 monitor socket 收到事件
   ├─ EVENT_HANDSHAKE_SUCCEEDED → "ok", 继续接收
   ├─ EVENT_HANDSHAKE_FAILED_AUTH → "fail", 结束迭代
   └─ EVENT_DISCONNECTED → "disconnect", 结束迭代
```

### 统计路径

```
每次 _publish_data → traffic.record(topic, record_count, payload_size)
   → 实时累积到 _current[topic].msg_count / record_count / bytes_total
        │
每整分钟: _minute_roll_loop 触发
   └─ traffic.roll_minute()
        ├─ _current[topic] → 归档到 _slots[topic] (deque, maxlen=retention)
        ├─ 清空 _current[topic]
        └─ 返回 archived: {topic: MinuteSlot, ...}
   └─ storage.save_minutes_batch(archived)
        └─ INSERT OR REPLACE INTO minute_stats (WAL 模式)
        │
每小时 (next_minute % 3600 < 70):
   └─ storage.cleanup() → DELETE 过期行
        │
Admin UI / SSE:
   读取 traffic.all_topics_snapshot() (内存) + storage.load_history() (SQLite)
```

### Admin SSE 实时推送路径

```
AdminServer._sse_broadcast_loop（asyncio Task, 每 1s）
   └─ _realtime_snapshot()
        ├─ traffic.all_topics_snapshot() → 各 topic 1min 滚动速率
        ├─ buffers.snapshot() → 各 topic 缓存大小
        └─ snapshot_fn() → 系统快照 (producer_count, start_time)
   └─ 组装 JSON → utf-8 编码
   └─ 推送到所有 SSE 客户端 Queue
        │
   各 _sse_writer task:
        └─ queue.get() → writer.write(payload) → writer.drain()
        └─ 异常 → self._sse_clients.pop(cid), 关闭 writer
```

---

> **文档维护约定**：当代码有结构性变更（新增模块、改变帧格式、调整依赖关系、修改生命周期流程）时，应同步更新本文档对应章节。版本号应与 `src/pulsemq/_version.py` 保持一致。
