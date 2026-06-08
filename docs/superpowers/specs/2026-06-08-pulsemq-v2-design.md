# PulseMQ v2 架构设计

> 日期: 2026-06-08
> 状态: 已确认

## 1. 概述

完全移除 broker/server 中间层，重构为纯 **pub → sub** 单进程架构。publisher 进程同时承担数据生产、权限控制、流量统计和后台管理界面。

### 核心决策

| 需求 | 决定 |
|------|------|
| 架构 | 单进程 pub → sub，无 broker |
| 权限 | ZMQ PLAIN 认证，api_key 白名单 |
| Producer | async 回调 + 固定延迟调度，并发执行 |
| 数据格式 | str/bytes/DataFrame/list[str]/list[bytes]/list[dict] |
| 序列化 | json / msgpack / pyarrow |
| 压缩 | none / snappy / lz4 / zstd |
| 时间戳 | 纳秒精度 |
| 统计 | 分钟粒度，内存 8h 窗口，定期 SQLite 落库 |
| 缓存 | 每 topic 环形缓存 deque(maxlen=100_000)，约 10 个 topic |
| 后台 | 深色卡片式 Web UI，SSE 实时推送，stdlib HTTP |
| 启动 | 零配置，所有值有默认 |
| 场景 | 个人内网使用 |

### 旧代码处理

全部删除 `src/pulsemq/` 下现有代码，从零重写。不考虑向后兼容。

## 2. 项目结构

```
src/pulsemq/
  __init__.py              # 导出 PulsePublisher, PulseSubscriber
  publisher.py             # PulsePublisher 入口类（编排各层）
  subscriber.py            # PulseSubscriber 客户端类
  config.py                # 配置：环境变量 > 默认值

  transport/
    __init__.py
    zmq_pub.py             # ZMQ PUB socket + PLAIN ZAP 认证

  producers/
    __init__.py
    manager.py             # ProducerManager: 回调注册 + asyncio Task 调度

  protocol/
    __init__.py
    frames.py              # 帧编解码（4帧格式）
    flags.py               # ser_fmt / comp 标志位
    msg_type.py            # 消息类型常量
    serialization.py       # 序列化注册表（json/msgpack/pyarrow/str/bytes）
    compression.py         # 压缩注册表（none/snappy/lz4/zstd）

  stats/
    __init__.py
    traffic.py             # TrafficStats: 分钟聚合 + 内存 8h 窗口
    storage.py             # SQLite 落库（定期持久化）

  cache/
    __init__.py
    topic_buffer.py        # 每 topic 环形缓存 deque(maxlen=100_000)

  admin/
    __init__.py
    server.py              # AdminServer: HTTP + SSE + REST API
    web_ui.py              # 深色 Web UI HTML 字符串
```

## 3. 传输层

### ZMQ PUB + PLAIN 认证

```python
class ZmqPubTransport:
    """ZMQ PUB socket + PLAIN 认证。"""

    def __init__(self, bind="tcp://*:5555", api_keys=None):
        self._bind = bind
        self._api_keys = api_keys or {}  # {username: password}

    async def start(self):
        self._ctx = zmq.asyncio.Context()
        self._pub = self._ctx.socket(zmq.PUB)
        if self._api_keys:
            self._pub.setsockopt(zmq.PLAIN_SERVER, 1)
            self._zap = PulseZAPHandler(self._api_keys)
            self._zap.start()
        self._pub.bind(self._bind)

    async def send(self, frames: list[bytes]):
        """广播一帧消息给所有 SUB。"""
        await self._pub.send_multipart(frames)

    async def stop(self):
        self._pub.close(linger=1000)
        self._ctx.term()
```

认证行为：
- `api_keys` 非空时自动开启 ZMQ PLAIN 认证，SUB 端必须提供 username/password
- `api_keys` 为空时认证关闭，零配置裸跑
- ZAP handler 校验白名单，拒绝非法连接

### 协议帧格式（4 帧）

```
Frame 1: topic (UTF-8 bytes)
Frame 2: meta (3 bytes)
  Byte 0: msg_type (0x01=DATA, 0x02=PING)
  Byte 1: flags (ser_fmt + comp 编码)
  Byte 2: reserved
Frame 3: timestamp (8 bytes, big-endian int64, 纳秒)
Frame 4: payload (序列化+压缩后的 bytes)
```

变化（对比 v1）：
- 时间戳独立成帧（8 字节纳秒精度），不编码进 payload
- meta 扩展为 3 字节，预留 1 字节
- msg_type 简化：去掉 AUTH/SUB/UNSUB/QUERY 等（无 broker 不需要），只保留 DATA(0x01) 和 PING(0x02)

## 4. 序列化与压缩

从现有代码搬过来 `serialization.py` + `compression.py` 注册表模式，不改。

| 序列化 | 压缩 |
|--------|------|
| json, msgpack, pyarrow, str, bytes | none, snappy, lz4, zstd |

DataFrame 自动推断格式：json（默认）/ msgpack / pyarrow。

### 回调返回值 → 消息映射

Producer 回调支持以下返回类型：

```python
# 单条
return "hello"                  # str → 1 record
return b"\x01\x02"             # bytes → 1 record
return df                      # DataFrame → N records (行数)

# 批量
return ["msg1", "msg2", ...]   # list[str] → N records
return [b"\x01", b"\x02"]      # list[bytes] → N records
return [df1, df2]              # list[DataFrame] → sum(行数) records
return [{"a":1}, {"b":2}]      # list[dict] → N records
```

内部处理：
1. 推断类型 + 计算 record_count
2. 序列化 + 压缩为 payload
3. record_count 写入帧格式，订阅端可知本帧含多少条记录
4. 一条 ZMQ send_multipart 原子发送

订阅端解码：payload 统一解码为 list。单条时 `["hello"]`，批量时完整列表。

## 5. Producer 管理

### 注册方式

```python
pub = PulsePublisher()  # 零配置启动

# 装饰器风格
@pub.producer(name="sh_market_data", cache_size=100_000, interval=5.0)
async def sh_market():
    data = fetch_sh_data()
    return data

# 或直接注册
pub.register_producer(fn, name="sz_market_data", interval=3.0, compression="lz4")
pub.start()
```

### ProducerManager

```python
class ProducerManager:
    """管理所有注册的 producer：回调注册 + 并发调度。"""

    def register(self, callback, name, interval=5.0, cache_size=100_000,
                 serializer="msgpack", compression="none"):
        """注册一个 producer。"""

    async def start_all(self, on_message):
        """启动所有 producer 任务。每个 producer 独立 asyncio.Task。"""

    async def _run_loop(self, spec, on_message):
        """固定延迟调度：执行 → sleep(interval - elapsed) → 执行 → ...

        - elapsed < interval: sleep 剩余时间
        - elapsed >= interval: sleep(0)，不积压
        """
```

调度特性：
- 每个 producer 独立 `asyncio.Task`，并发执行互不阻塞
- 固定延迟模式：`sleep(interval - elapsed)`，执行超时则 `sleep(0)`
- 异常不崩溃，warning 日志后继续下一轮

### 多 producer 时间线

```
Producer A (interval=5s, 执行~3s):
  |--执行3s--|---等2s---|--执行3s--|---等2s---|
  0         3          5          8         10

Producer B (interval=5s, 执行~4s):
  |--执行4s--|--等1s--|--执行4s--|--等1s--|
  0         4         5          9        10

Producer C (interval=3s, 执行~1s):
  |-1s-|--等2s--|-1s-|--等2s--|-1s-|--等2s--|
  0    1       3    4       6    7       9

三者并发，互不干扰
```

## 6. 完整消息流

```
Producer 回调返回 data
  │
  ▼
Publisher.on_produce(spec, data)
  │
  ├─ 1. 推断类型 + record_count
  │     str / list[str] / DataFrame / bytes / list[bytes] / list[dict]
  │
  ├─ 2. 序列化 + 压缩
  │     FrameCodec.encode_payload(data, spec.serializer, spec.compression)
  │
  ├─ 3. 获取纳秒时间戳
  │     timestamp_ns = time.time_ns()
  │
  ├─ 4. 编码帧 [topic, meta(3B), timestamp_ns(8B), payload]
  │
  ├─ 5. 并行分发：
  │     ├─→ Transport.send(frames)      # await，广播给所有 SUB
  │     ├─→ TopicBuffer.append(...)     # 同步，写入环形缓存
  │     └─→ TrafficStats.record(...)    # 同步，记录流量统计
  │
  └─ done
```

transport.send 是 await（确认发出），stats 和 buffer 是同步操作（内存 dict/deque），不阻塞主流程。

## 7. 流量统计

### TrafficStats

内存中维护每个 topic 的分钟级时序数据，8 小时窗口自动淘汰。

```python
@dataclass
class MinuteSlot:
    """一个 topic 一分钟的统计快照。"""
    timestamp: int           # 整分钟秒
    msg_count: int = 0       # 消息条数（帧数）
    record_count: int = 0    # 记录条数（含批量拆分）
    bytes_total: int = 0     # payload 总字节数
    msg_rate: float = 0.0    # msg/s

class TrafficStats:
    """分钟粒度流量统计，内存 8 小时窗口。"""

    def __init__(self, retention_minutes=480):  # 480 = 8h
        self._slots: dict[str, deque[MinuteSlot]] = {}
        self._retention = retention_minutes
        self._current: dict[str, MinuteSlot] = {}  # 当前分钟累积器

    def record(self, topic, record_count, payload_size):
        """记录一条消息（同步，无锁）。"""

    def roll_minute(self):
        """整分钟时调用：归档当前累积器 → 滚动窗口淘汰过期数据。"""

    def get_history(self, topic, minutes=60) -> list[dict]:
        """获取 topic 最近 N 分钟流量数据（给 Admin 曲线用）。"""

    def snapshot(self) -> dict:
        """所有 topic 实时快照（给 Admin 卡片指标用）。"""
```

内存计算：
- 10 topic × 480 分钟 = 4800 个 MinuteSlot
- 每个 Slot ~100 bytes → 总计 ~480 KB，完全可放内存

### StatsStorage（SQLite 落库）

```python
class StatsStorage:
    """分钟统计 SQLite 持久化。"""

    async def save_minute(self, topic, slot: MinuteSlot):
        """写入一条分钟记录。"""

    async def load_history(self, topic, since_ts) -> list[dict]:
        """加载历史数据（进程重启后恢复图表用）。"""

    async def cleanup(self, retention_days=7):
        """清理过期数据。"""
```

落库策略：`roll_minute()` 之后异步写入，不阻塞主流程。内存 8 小时是热数据（图表展示），SQLite 是冷备份（重启恢复）。

## 8. Topic 缓存

```python
class TopicBuffer:
    """单个 topic 的环形缓存。"""

    def __init__(self, topic, max_size=100_000):
        self._buf: deque[tuple[int, bytes]] = deque(maxlen=max_size)
        # ↑ (timestamp_ns, frame_bytes)

    def append(self, timestamp_ns, frame_bytes):
        """追加一条消息。满时自动淘汰最旧。"""

    def snapshot(self, since_ns=0, limit=100) -> list:
        """按时间戳查询（给新 sub 补数据用）。"""

    @property
    def size(self) -> int:
        return len(self._buf)
```

- 每个 producer 注册时指定 cache_size，各自独立
- 满时自动淘汰（deque(maxlen=N) 特性），无需手动清理
- 10 topic × 100K × ~500 bytes ≈ 500 MB 上限（实际远低于此，因为淘汰）

## 9. Admin 后台

### AdminServer

stdlib asyncio HTTP，手写请求解析，不引入框架。

REST 端点：

```
GET  /                              深色 Web UI 首页
GET  /api/v1/stats/realtime         实时指标 JSON
GET  /api/v1/stats/stream           SSE 实时推送（1s 一帧）
GET  /api/v1/topics                 所有 topic 列表 + 当前指标
GET  /api/v1/topics/{topic}/history 分钟级历史（最近 N 分钟）
GET  /api/v1/system/status          系统状态（uptime, version）
GET  /healthz                       健康检查
```

SSE 推送内容（每秒一次）：

```json
{
  "topics": {
    "sh_market_data": {
      "msg_count": 312,
      "record_count": 1560,
      "bytes_total": 245760,
      "msg_rate": 5.2,
      "cache_size": 82340
    }
  },
  "subscribers": 12,
  "uptime_seconds": 9240
}
```

### Web UI

单文件 HTML，内嵌 CSS + JS + SVG 绘图，深色主题：
- 顶部：4 个指标卡片（Topics / Subscribers / Msg/s / Uptime）
- 中部：当前选中 topic 的流量折线图（SVG，最近 60 分钟）
- 底部：topic 列表，点击切换图表
- 数据源：`EventSource('/api/v1/stats/stream')` 实时刷新

## 10. Subscriber 客户端

```python
class PulseSubscriber:
    """订阅端客户端。"""

    def __init__(self, address, username="", password=""):
        self._address = address
        self._username = username
        self._password = password

    async def connect(self):
        """连接 PUB socket，PLAIN 认证。"""
        self._ctx = zmq.asyncio.Context()
        self._sub = self._ctx.socket(zmq.SUB)
        if self._username:
            self._sub.setsockopt(zmq.PLAIN_USERNAME, self._username.encode())
            self._sub.setsockopt(zmq.PLAIN_PASSWORD, self._password.encode())
        self._sub.connect(self._address)

    async def subscribe(self, *topics) -> AsyncIterator[PulseMessage]:
        """订阅 topic，返回异步迭代器。"""
        for t in topics:
            self._sub.setsockopt(zmq.SUBSCRIBE, t.encode("utf-8"))
        while self._connected:
            frames = await self._sub.recv_multipart()
            yield self._decode(frames)
```

用法：

```python
sub = PulseSubscriber("tcp://host:5555", username="user1", password="pulse_sk_xxx")
async with sub:
    async for msg in sub.subscribe("sh_market_data"):
        print(msg.topic, msg.payload, msg.timestamp_ns)
```

### PulseMessage 结构

```python
@dataclass
class PulseMessage:
    topic: str
    payload: Any              # 解码后数据（str / list / bytes / DataFrame）
    raw_payload: bytes        # 原始字节
    record_count: int         # 本帧包含的记录数
    timestamp_ns: int         # 纳秒时间戳
    serializer: str           # 序列化格式名
    compression: str          # 压缩格式名
```

对比 v1：去掉 msg_type / meta_flags（内部字段），新增 timestamp_ns / record_count / serializer / compression（对用户有用的元数据）。

## 11. 配置

零配置默认值，所有配置可通过环境变量覆盖：

| 配置项 | 默认值 | 环境变量 |
|--------|--------|----------|
| PUB bind | `tcp://*:5555` | `PULSEMQ_BIND` |
| Admin bind | `0.0.0.0:9090` | `PULSEMQ_ADMIN_BIND` |
| Stats DB | `sqlite://./stats.sqlite` | `PULSEMQ_STATS_DB` |
| Stats 内存窗口 | 8 小时（480 分钟） | — |
| 白名单 | `{}`（空=关闭认证） | `PULSEMQ_API_KEYS=user1:pass1,user2:pass2` |

空白名单时认证关闭，任何连接都能订阅。也可通过 `pub.add_api_key("user1", "pass1")` 编程式添加。

## 12. 依赖

与 v1 一致：
- pyzmq >= 26.0
- msgspec >= 0.18
- python-snappy >= 0.7
- lz4 >= 4.0
- zstandard >= 0.22
- pyarrow >= 14.0
- pandas >= 2.0

Python >= 3.13。

## 13. 待实现后考虑的扩展点

以下不在 v2 范围内，但架构上预留了空间：
- 通配符订阅（当前 ZMQ SUB 原生前缀匹配已部分支持）
- 新 sub 连接后补发缓存数据（TopicBuffer.snapshot 已预留接口）
- 多 publisher 协作（当前设计为单 publisher 独立运行）
