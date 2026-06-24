# PulseMQ 模块设计文档

> 版本：v3.1.1 ｜ 最后更新：2026-06-24
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
- **类型保真（v3）**：协议层记录原始 Python 类型，订阅端能还原（如 DataFrame→DataFrame，而非降级为 list[dict]）
- **可观测**：内置 admin Web UI + REST + SSE，实时监控流量、缓存、系统状态

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
              subscriber.py（独立客户端，依赖 protocol 解码）
```

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

**职责**：加载 publisher 配置，优先级为「环境变量 > 默认值」。纯数据模块，无副作用。

**核心类**：

```python
@dataclass
class PublisherConfig:
    bind: str = "tcp://*:5555"               # ZMQ PUB 绑定地址
    admin_bind: str = "0.0.0.0:9090"         # Admin 后台绑定地址
    stats_db: str = "sqlite://./stats.sqlite" # 统计 SQLite 路径
    stats_retention_minutes: int = 480        # 内存统计窗口（8 小时）
    heartbeat_interval: float = 30.0          # 心跳间隔（秒），<=0 禁用
    api_keys_str: str = ""                    # API Keys，空=关闭认证
```

**关键接口**：
- `PublisherConfig.api_keys`（property）：解析 `api_keys_str`（`"user1:pass1,user2:pass2"` 格式）为 `{username: password}` 字典
- `load_config()`：遍历 `_ENV_MAP`，用环境变量覆盖对应字段。支持的环境变量：`PULSEMQ_BIND`、`PULSEMQ_ADMIN_BIND`、`PULSEMQ_STATS_DB`、`PULSEMQ_API_KEYS`

**依赖**：无（仅 stdlib）。

---

## 3. 协议模块 `protocol/`

**职责**：定义消息帧格式、序列化、压缩、flags 编码。所有与「数据如何在网络上表示」的逻辑都集中在此。

### 3.1 帧编解码 `protocol/frames.py`

**职责**：定义 4 帧格式并提供 `encode` / `decode`。

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
| 2 | `data_type` | uint8 | `0x00`=UNKNOWN, `0x01`=dict, `0x02`=DataFrame, `0x03`=str, `0x04`=bytes |
| 3-6 | `record_count` | big-endian uint32 | 本帧记录数，上限 1,000,000 |

**核心接口**：

```python
@dataclass
class PulseMessage:
    topic: str
    payload: Any              # 解码后数据（已按 data_type 还原）
    raw_payload: bytes
    record_count: int
    timestamp_ns: int         # 纳秒
    serializer: str
    compression: str
    data_type: int = DataType.UNKNOWN

def encode(topic, data, serializer="msgpack", compression="none",
           record_count=1, data_type=DataType.UNKNOWN) -> list[bytes]
def decode(frames: list[bytes]) -> PulseMessage
def encode_heartbeat() -> list[bytes]   # 心跳帧（topic=b"__pulse_hb__"，空 payload）
```

**关键设计**：
- `encode`：先 `serialize` 再 `compress`，组装 4 帧
- `decode`：反向 `decompress` → `deserialize`，再按 `data_type` 还原原始类型
- `record_count` encode 时 `& 0xFFFFFFFF` 掩码；decode 侧信任 4 字节不做边界校验
- 心跳帧复用同一 7 字节 meta 布局（`msg_type=PING`，`data_type=UNKNOWN`）

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

| 类 | 名称 | 后端 | 特性 |
|----|------|------|------|
| `StringSerializer` | `str` | UTF-8 | str↔bytes，bytes 透传 |
| `BytesSerializer` | `bytes` | 无 | 纯字节透传 |
| `MsgpackSerializer` | `msgpack` | msgspec | 通用二进制 |
| `JsonSerializer` | `json` | msgspec.json | 拒绝 bytes（避免 base64→str 变形）|
| `PyArrowSerializer` | `pyarrow` | pyarrow IPC | 支持 DataFrame/Table/dict/list[dict]，**可选依赖** |

**注册表接口**：`register(name, serializer)`、`get(name)`（未注册抛 `KeyError`）、`available()`。

**关键设计**：
- pyarrow 用 `try/except ImportError` 守卫，未安装时跳过注册，不影响库导入（**降级处理，与压缩模块不对称——见 3.3 注意事项**）
- msgspec 采用函数内 lazy import（`import msgspec` 在方法内），延迟加载

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

| 类 | 名称 | 后端 |
|----|------|------|
| `NoneCompressor` | `none` | 无（透传）|
| `SnappyCompressor` | `snappy` | python-snappy |
| `Lz4Compressor` | `lz4` | lz4.frame |
| `ZstdCompressor` | `zstd` | zstandard |

**注册表接口**：与序列化模块对称（`register` / `get` / `available`）。

**⚠️ 注意事项**：snappy/lz4/zstd 在 `__init__` 里直接 import，`_init_builtins()` 模块加载时即实例化，**没有 ImportError 守卫**。当前 pyproject.toml 把三者列为硬依赖所以能跑，但若改为可选依赖会导致 `import pulsemq` 崩溃。与序列化层的 pyarrow 守卫不对称，是已知的健壮性隐患。

**依赖**：python-snappy、lz4、zstandard（均为硬依赖）。

### 3.4 flags 位域 `protocol/flags.py`

**职责**：把（序列化格式, 压缩算法）编码为单字节。

**位布局**：
```
bit:  7  6  5  4  3  2  1  0
      └reserved┘ └comp┘ └─ser─┘
                 (2位)  (3位)
```

- 序列化（低 3 位）：`000`=msgpack, `001`=bytes, `010`=pyarrow, `100`=str, `101`=json
- 压缩（bit[3:4]）：`00`=none, `01`=snappy, `10`=lz4, `11`=zstd

**接口**：`encode_flags(ser_fmt, comp) -> int`、`decode_flags(byte_val) -> tuple[str, str]`。

5 序列化 × 4 压缩 = 20 种合法组合，编码成 20 个互不冲突字节，可无损往返。

**依赖**：无。

### 3.5 消息类型常量 `protocol/msg_type.py`

**职责**：定义两个常量类，无逻辑。

```python
class MsgType:
    DATA = 0x01   # 数据帧
    PING = 0x02   # 心跳帧

class DataType:
    UNKNOWN = 0x00    # 兜底
    DICT = 0x01
    DATAFRAME = 0x02
    STR = 0x03
    BYTES = 0x04
```

**依赖**：无。

---

## 4. Producer 管线 `producers/`

**职责**：定义 producer 回调类型 + 调度 producer 任务。这是「数据从哪里来」的抽象层。

### 4.1 类型定义 `producers/types.py`

**职责**：producer 管线的类型单一来源。

**核心类型**：
```python
PubData: TypeAlias = Union[pd.DataFrame, dict, bytes, str]
SimpleProducerCallback = Callable[[], Awaitable[PubData | None]]       # 无 sender 注入
SenderProducerCallback = Callable[["PublisherSender"], Awaitable[PubData | None]]  # 有 sender
ProducerCallback = Union[SimpleProducerCallback, SenderProducerCallback]
```

**依赖**：pandas（硬依赖）。`PublisherSender` 用字符串前向引用，types.py 不 import publisher.py，避免循环导入。

### 4.2 Producer 调度 `producers/manager.py`

**职责**：注册 producer 规格并按调度策略运行 asyncio task。

**核心数据结构**：
```python
@dataclass
class ProducerSpec:
    name: str                       # topic 名（同时也是 producer 名）
    callback: ProducerCallback
    interval: float = 5.0           # 推送间隔（秒）；0.0 = burst 模式
    cache_size: int = 100_000
    serializer: str = "msgpack"
    compression: str = "none"
    inject_sender: bool = False

# 管理器内部使用的回调别名（定义在 ProducerSpec 之后）
OnMessageCallback = Callable[[ProducerSpec, PubData], Awaitable[None]]
SenderFactory = Callable[[ProducerSpec], "PublisherSender"]
```

**调度策略**：

| 模式 | 条件 | 行为 |
|------|------|------|
| 固定间隔 | `interval > 0` | `执行 → sleep(interval - elapsed)`，不积压（超时则 sleep(0)）|
| Burst | `interval == 0.0` | 无间隔连续执行，回调返回 `None` 即结束；异常冷却 0.1s |

**核心接口**：
- `register(callback, name, ...)` / `register_burst(...)`：注册规格
- `start_all(on_message, sender_factory)`：为每个规格创建 asyncio task
- `stop_all()`：取消所有 task 并 gather
- `inject_sender` 分发：回调签名按 `spec.inject_sender` 决定是否传入 `sender_factory(spec)`

**容错**：回调异常只记 warning 日志，不崩溃循环。

**依赖**：`producers.types`、`producers.types.ProducerCallback`。

---

## 5. 发布端核心 `publisher.py`

**职责**：单进程 publisher 的编排中心。串联 producer 调度、帧编码、传输、缓存、统计、admin。这是整个系统的「主控制器」。

**核心类**：

```python
class PulsePublisher:
    # 注册入口（3 种，均支持 inject_sender，用 @overload 绑定类型）
    def producer(name, *, interval, cache_size, serializer, compression, inject_sender)
    def burst_producer(name, *, cache_size, serializer, compression, inject_sender)
    def register_producer(fn, *, name, ...)

    # 生命周期
    def start()              # 阻塞启动（asyncio.run）
    async def start_async()  # 异步启动（嵌入其他 asyncio 程序）
    async def _run()         # 主运行循环（初始化 + 保活 + 优雅关闭）

    # 手动发送端
    class PublisherSender:   # inject_sender 模式下注入回调，支持 await sender.send(...)
```

**生命周期 `_run()`**（关键）：
1. 初始化阶段（创建 transport/storage/admin/tasks）+ 运行循环**全部包在同一 `try/finally`** 中
2. `finally` 调 `_shutdown()`：取消任务、最后归档统计落库、关闭 admin/transport/storage
3. 这样设计保证初始化中途失败也能释放资源（避免 ZMQ context/SQLite 泄漏）

**发布管线 `_publish_data()`**（6 步）：
1. `_infer_record_count(data)`：推断记录数（DataFrame=行数，其余=1），白名单外抛 TypeError
2. `_validate_serializer(data, serializer)`：强类型绑定（str→str，bytes→bytes，DataFrame/dict→msgpack/json/pyarrow）
3. `_infer_data_type(data)`：推断 DataType 常量，写入 meta 供 sub 还原
4. `_prepare_payload(data)` → `frame_codec.encode(...)`：序列化+压缩+组帧
5. `await self._transport.send(frames)`：广播
6. 同步追加缓存 + 记录统计

**白名单**：`pd.DataFrame` / `dict` / `str` / `bytes`（list 已移除）。

**心跳循环 `_heartbeat_loop()`**：每 `heartbeat_interval` 调用 `encode_heartbeat()` + `transport.send()` 广播 PING。

**分钟滚动循环 `_minute_roll_loop()`**：每 60s 调用 `traffic.roll_minute()` 归档，结果落库 SQLite。

**依赖**：`transport.ZmqPubTransport`、`producers.ProducerManager`、`cache.TopicBufferRegistry`、`stats.TrafficStats`、`stats.StatsStorage`、`admin.AdminServer`、`protocol.frames`、`config.PublisherConfig`。

---

## 6. 传输层 `transport/zmq_pub.py`

**职责**：封装 ZMQ PUB socket + 可选 PLAIN 认证（ZAP handler）。

**核心类**：

```python
class ZmqPubTransport:
    async def start()   # 创建 ctx + PUB socket，可选启动 ZAP，bind
    async def send(frames: list[bytes])  # 广播一帧
    async def stop()    # 关闭 PUB/ZAP/ctx
    def set_auth_callback(callback)

class AsyncZAPHandler:
    """ZMQ PLAIN 认证 ZAP handler（asyncio 版，与 PUB 共享 ctx）"""
    async def start()   # 绑定 inproc://zeromq.zap.01，启动 _loop task
    async def _loop()   # 循环 recv ZAP 请求 → 校验 → 发响应
    async def _send_zap_reply(request_id, status_code, status_text, ...)  # 统一 await + 异常保护
```

**认证流程**（`_loop`）：
1. recv ZAP 请求（8 帧：version/request_id/domain/address/identity/mechanism/username/password）
2. 校验 mechanism==PLAIN、白名单 username/password 匹配
3. 发 ZAP 响应（6 帧：version/request_id/status_code/status_text/user_id/metadata）
4. 调用认证事件回调 `on_auth(username, client_addr, success)`

**关键设计**：
- ZAP handler 与 PUB socket 共享同一 `zmq.asyncio.Context`，在同一事件循环运行，避免跨线程 inproc 问题
- ZAP 响应通过 `_send_zap_reply()` 统一 `await` + `try/except`：单次 send 失败仅记日志、不影响循环（避免一次失败导致认证全线瘫痪）
- SUB 连接日志：`[SUB 上线] user=xxx addr=xxx auth=OK` / `[SUB 认证失败] ... reason=...`

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
    async def close()
```

**订阅循环**（`subscribe()`）：
1. 设置 SUBSCRIBE 过滤器（topic 前缀匹配）
2. 认证场景：启动后台 monitor task 监听握手结果（成功 + 各类失败事件掩码）
3. 循环 `recv_multipart`：认证场景下 recv 与「握手结果事件」竞争（`asyncio.wait` FIRST_COMPLETED）
4. 收到 4 帧后**过滤 PING 心跳帧**（`meta[0]==MsgType.PING` 的跳过，不交付用户）
5. `yield decode(frames)` 给用户

**握手可见性**（认证场景）：monitor 始终启用（无论是否认证），监听掩码
`HANDSHAKE_SUCCEEDED | HANDSHAKE_FAILED_AUTH | HANDSHAKE_FAILED_PROTOCOL | HANDSHAKE_FAILED_NO_DETAIL | DISCONNECTED`。
`_watch_events()` 持续 recv 事件，按事件码分发，**关键事件直接 `print` 到 stderr**（不依赖用户配置 logging，保证始终可见）：
- 成功 → `[SUB 上线] 认证成功`，继续接收
- 任意失败 → `[SUB 认证失败]`，自动结束迭代
- 断线（`DISCONNECTED`）→ `[SUB 断线]`，自动结束迭代（pub 停止时 sub 不卡死）

这样 pub 端（`[SUB 上线] user=... auth=OK/FAIL`）和 sub 端（`[SUB 上线]`/`[SUB 认证失败]`/`[SUB 断线]`）双向都有可见性，且不依赖 logging 配置。

**依赖**：pyzmq（asyncio）、`protocol.frames.decode`、`protocol.msg_type.MsgType`。

---

## 8. 缓存模块 `cache/topic_buffer.py`

**职责**：每个 topic 一个环形缓存，按**记录总数**淘汰，用于新订阅者补发历史。

**核心数据结构**：

```python
@dataclass
class CachedMessage:
    timestamp_ns: int
    frames: list[bytes]       # 原始 4 帧（可重发）
    record_count: int = 1

class TopicBuffer:
    """单个 topic 的环形缓存（按累计记录数淘汰）"""
    def append(timestamp_ns, frames, record_count=1)  # 超限从队首 popleft
    def snapshot(since_ns=0, limit=100) -> list[CachedMessage]  # 按时间戳查询
    size: int            # 帧数
    total_records: int   # 累计记录数
    max_records: int     # 记录数上限

class TopicBufferRegistry:
    """所有 topic 缓存的注册表"""
    def get_or_create(topic, max_size=100_000)
    def snapshot() -> dict  # 给 admin 显示用
```

**淘汰策略**：当 `_total_records + record_count > max_records` 时，从队首不断 popleft 直到能容纳。单帧即使超限也至少保留一帧（避免缓存为空）。

**设计要点**：淘汰按「记录数」而非「帧数」——DataFrame 一批 1000 行占 1000 配额，而非 1。`max_size` 由 producer 注册时的 `cache_size` 指定。

**依赖**：无（仅 stdlib collections.deque）。

---

## 9. 统计模块 `stats/`

**职责**：分钟粒度流量统计，内存 8 小时窗口 + SQLite 持久化。

### 9.1 内存统计 `stats/traffic.py`

**职责**：每 topic 的分钟级时序数据，自动淘汰过期分钟。

**核心数据结构**：

```python
@dataclass
class MinuteSlot:
    timestamp: int           # 整分钟秒
    msg_count: int = 0
    record_count: int = 0
    bytes_total: int = 0

class TrafficStats:
    def record(topic, record_count, payload_size)   # 记录一条消息
    def roll_minute() -> dict[str, MinuteSlot]       # 整分钟归档，返回待落库数据
    def get_history(topic, minutes=60) -> list[dict] # 取 N 分钟历史
    def snapshot() -> dict                           # 实时快照（当前分钟累积）
    def all_topics_snapshot() -> dict                # 完整快照（含 1min 滚动速率）
```

**内部结构**：
- `_current: dict[topic, MinuteSlot]`：当前分钟累积器
- `_slots: dict[topic, deque[MinuteSlot]]`：滚动窗口（deque maxlen=retention）
- `_current_minute: int`：当前整分钟秒

**分钟切换**：`roll_minute()` 把 `_current` 归档进 `_slots`，清空 `_current`。`_ensure_current()` 是兜底，检测到分钟落后时自动调用 `roll_minute()`。

**1min 滚动速率**：`all_topics_snapshot()` 用「当前分钟累积 + 上一分钟按 (60-elapsed)/60 比例补齐」估算近 60 秒速率。

**线程模型**：文档标注「单写者 publisher + 多读者 admin，依赖 GIL」。实际为单线程 asyncio，同步函数不会互相抢占，故无真实并发问题。

**依赖**：无。

### 9.2 持久化 `stats/storage.py`

**职责**：分钟统计的 SQLite 持久化，进程重启后可恢复图表。

**核心类**：

```python
class StatsStorage:
    def connect()    # 建连接 + 建表（WAL 模式）
    def save_minute(topic, slot)               # 写一条
    def save_minutes_batch(data: dict)         # 批量写
    def load_history(topic, since_ts) -> list  # 读历史（按时间戳）
    def cleanup(retention_days=7) -> int       # 清理过期数据
    def close()
```

**表结构**：

```sql
CREATE TABLE minute_stats (
    topic TEXT NOT NULL,
    timestamp INTEGER NOT NULL,
    msg_count INTEGER DEFAULT 0,
    record_count INTEGER DEFAULT 0,
    bytes_total INTEGER DEFAULT 0,
    PRIMARY KEY (topic, timestamp)
)
```

**设计要点**：
- WAL 模式提升并发读写
- 所有查询用 `?` 占位符参数化（防 SQL 注入，topic 名来自 producer 可含特殊字符）
- 速率计算除以常量 `60.0`，无除零风险
- 落库策略：`roll_minute()` 之后同步写入（注释称「异步」，实际同步，会短暂阻塞事件循环，但 SQLite 写入快）

**依赖**：stdlib sqlite3。

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

**REST 端点**：

| 方法 | 路径 | 功能 |
|------|------|------|
| GET | `/` | 深色 Web UI 首页 |
| GET | `/static/{path}` | 静态资源（ECharts 等）|
| GET | `/api/v1/stats/realtime` | 实时指标 JSON |
| GET | `/api/v1/stats/stream` | SSE 实时推送（1s 一帧）|
| GET | `/api/v1/topics` | 所有 topic 列表 + 指标 |
| GET | `/api/v1/topics/{topic}/history` | 分钟级历史（内存 + SQLite 合并去重）|
| GET | `/api/v1/system/status` | 系统状态（uptime, version）|
| GET | `/healthz` | 健康检查 |

**SSE 实现**：
- 每个 SSE 客户端一个 `asyncio.Queue(maxsize=64)` + writer task
- 广播循环每 1s 推一次 `realtime_snapshot`
- 队列满（客户端断开/消费慢）→ 主动取消该连接，避免死客户端残留内存泄漏
- `_handle_request` 设置 `_sse_takeover` 标记，SSE 连接不被 finally 关闭

**历史合并**：`_topic_history` 内存数据优先，不足时查 SQLite 补充，按 timestamp 去重排序。内存最多 `minutes-1` 条（当前分钟未归档），故 `>= minutes - 1` 即视为覆盖。

**安全性**：
- 静态资源路径校验：拒绝 `..`、绝对路径、反斜杠，并用 `resolve().is_relative_to(STATIC_ROOT)` 二次验证
- 请求超时保护：request line / header / body 都有 `wait_for` 超时
- 版本号从 `pulsemq._version` 统一读取，避免与包版本脱节

**依赖**：`cache.TopicBufferRegistry`、`stats.TrafficStats`、`stats.StatsStorage`、`admin.web_ui.INDEX_HTML`。

### 10.2 Web UI `admin/web_ui.py`

**职责**：单文件 HTML（内嵌 CSS + JS），提供深色玻璃态监控面板。

**内容**：
- 顶部导航栏 + 连接状态 + 版本
- 指标卡片区：4 个渐变发光统计卡片（中文标签 + emoji）
- 图表区：ECharts 多 topic 流量曲线（分钟粒度，1H/6H 切换，实时更新，最多 5 topic 叠加 LRU 淘汰）
- 底部 topic 卡片网格

**实现**：`INDEX_HTML` 常量字符串，由 `admin/server.py` 的 `_respond_html` 返回。ECharts 等静态资源通过 `/static/` 提供。

**依赖**：无（纯静态资源）。

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

**平台适配**：Windows 下强制 `WindowsSelectorEventLoopPolicy`（pyzmq 的 asyncio 集成不支持 Proactor）。

---

## 12. 附录：端到端数据流

### 发布路径（producer → 网络）

```
用户 @pub.producer 回调返回 data (DataFrame/dict/str/bytes)
        │
        ▼
ProducerManager._run_loop / _run_burst_loop
   调用 on_message(spec, data)  [或 inject_sender: await callback(sender)]
        │
        ▼
PulsePublisher._on_produce(spec, data) → _publish_data(...)
   1. _infer_record_count      → 记录数（DataFrame=行数）
   2. _validate_serializer      → 强类型绑定校验
   3. _infer_data_type          → DataType 常量
   4. frame_codec.encode        → [topic, meta(7B), ts(8B), payload]
        │  ├─ serialization.serialize
        │  └─ compression.compress
   5. transport.send(frames)    → ZMQ PUB 广播
   6. buffers.get_or_create(topic).append(...)  → 缓存
      traffic.record(topic, record_count, payload_size)  → 统计
```

### 订阅路径（网络 → 用户）

```
ZMQ SUB socket recv_multipart → 4 帧
        │
        ▼
PulseSubscriber.subscribe() 循环
   ├─ 认证场景：recv 与 auth_failed 事件竞争
   ├─ 4 帧校验
   ├─ 过滤 PING 心跳帧（meta[0]==PING 跳过）
   └─ frame_codec.decode(frames)
        │  ├─ compression.decompress
        │  ├─ serialization.deserialize
        │  └─ _restore_type(payload, data_type)  → 还原原始类型
        ▼
yield PulseMessage  → 用户的 async for 循环
```

### 心跳路径

```
PulsePublisher._heartbeat_loop（每 heartbeat_interval 秒）
   └─ encode_heartbeat() → [b"__pulse_hb__", meta(7B, msg_type=PING), ts, b""]
   └─ transport.send(frames)  → 广播
        │
   订阅端：meta[0]==PING → 过滤，不交付用户
```

### 统计路径

```
每次 _publish_data → traffic.record(...)  [当前分钟累积]
        │
每 60s：_minute_roll_loop
   └─ traffic.roll_minute() → 归档到 _slots + 返回 archived
   └─ storage.save_minutes_batch(archived)  → SQLite 落库
        │
Admin UI / SSE：读取 traffic.all_topics_snapshot() + storage.load_history()
```

---

> **文档维护约定**：当代码有结构性变更（新增模块、改变帧格式、调整依赖关系）时，应同步更新本文档对应章节。
