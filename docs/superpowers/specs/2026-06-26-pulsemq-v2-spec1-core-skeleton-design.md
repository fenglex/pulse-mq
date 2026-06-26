# PulseMQ v2 重构 · Spec 1：核心架构骨架设计

> 版本：v1.0 ｜ 日期：2026-06-26
> 范围：PulseMQ 第二代架构重构的第一份子 spec，聚焦 Client/Server 模型的核心骨架
> 关联文档：`docs/PulseMQ_重构架构设计_Client_Server.md`（总体重构设计）
> 执行策略：原地大改（不保过渡）、不兼容旧版协议
> 后续 spec：Spec 2 = 安全（security/auth/admin token），Spec 3 = 监控扩展（在线 Client/延迟分位/事件流/web_ui 新区块）

---

## 1. 目标与非目标

### 1.1 目标

重构后系统以 Client/Server 模型跑通"发布 → 中继路由 → 订阅"全链路，满足：

- 对外只暴露 `Client` / `Server` / `ProducerClient` / `ConsumerClient`，不暴露任何 ZeroMQ 术语。
- 数据面与控制面分离，分别走独立端口。
- 强制 PLAIN 用户名/密码认证，不可关闭。
- Server 端维护 topic→订阅表与在线用户表，支持前缀匹配订阅、单用户单在线、动态订阅。
- Client 启动期硬失败（连接/认证/注册任一失败立即退出，非零退出码）；运行期断线自动重连 + 重新认证 + 恢复订阅。
- 零配置优先：`Server()` / `Client()` 无参即可启动。

### 1.2 非目标（留给后续 spec）

- 密码哈希存储、默认凭据自动生成、用户管理 CLI、admin token 认证 → **Spec 2**
- 在线 Client 详情、端到端延迟分位、最近事件流、web_ui 新区块 → **Spec 3**
- 多 Server 实例、按 topic ACL、Prometheus 导出、CURVE/TLS → **阶段 5（更后续）**
- 消息持久化与断线补发 → **明确不做**（内存转发，尽力投递）

### 1.3 认证衔接边界（重要）

PLAIN 是强制的，但 Spec 1 只实现**最简凭据源**：复用现有 ZAP + `api_keys` 风格（明文 dict 或简单 TOML 白名单），让系统跑通认证闭环。密码哈希、默认凭据自动生成、CLI 管理、admin token 等 `security` 能力全部留给 Spec 2，届时替换凭据源即可，不动 `transport`/`auth` 接口。

---

## 2. 执行策略与兼容性

- **原地大改**：直接在 `src/pulsemq/` 上重写，删除 `publisher.py` / `subscriber.py` 旧角色，不保留过渡形态。
- **不兼容旧版协议**：帧格式重设（新增魔数/版本/帧类型），旧客户端无法连接新 Server，需同步升级。本次重构作为大版本升级（pyproject 当前 4.0.2，目标升至 5.0.0）。
- **沿用不动的模块**：`protocol/serialization.py`、`protocol/compression.py`、`stats/traffic.py`、`stats/storage.py`、`admin/server.py`、`admin/web_ui.py`。这些模块的现有测试应继续通过。

---

## 3. 模块清单与依赖关系

### 3.1 本 spec 涉及模块

| 模块 | 状态 | 职责 |
|------|------|------|
| `errors` | 新增 | 统一异常体系 + 退出码 |
| `config` | 重写 | TOML/env 统一配置，全默认值 |
| `logging` | 新增 | loguru 结构化日志 + Client 生命周期事件规范 |
| `protocol/frames` | 重写 | 按新帧格式编解码 |
| `protocol/serialization` | 沿用 | 不动 |
| `protocol/compression` | 沿用 | 不动 |
| `protocol/flags` | 沿用/微调 | ser/comp 编码复用，扩展 flags 位容纳 CRC 开关 |
| `protocol/msg_type` | 扩展 | 增加 CONTROL / HEARTBEAT / ADMIN 类型 |
| `transport` | 重写 | ROUTER/DEALER 抽象，ZAP PLAIN，monitor |
| `routing` | 新增 | topic→订阅表，前缀匹配 |
| `control` | 新增 | 注册/心跳/在线用户表/动态订阅 |
| `client` | 新增 | Client/ProducerClient/ConsumerClient |
| `server` | 新增 | Server 组装入口 |
| `lifecycle` | 新增 | 启动顺序/优雅关闭/信号处理 |
| `producers` | 保留 | 作为 ProducerClient 之上的可选调度 helper |

### 3.2 依赖图（严格单向、无环）

```
errors ← config ← logging                 （基础设施，无业务依赖）
  │
  ▼
protocol/ (frame 重设 + 沿用 serialization/compression/flags/msg_type)
  │
  ▼
transport (ROUTER/DEALER, ZAP, monitor) ── routing ── control
  │                                            │        │
  └────────────── server (组装) ←──────────────┴────────┘
                       │
                       ▼
                   lifecycle
                       │
              client (Client/ProducerClient/ConsumerClient)
                       │
              producers (可选 helper, 复用)
```

约束：
- `transport` 是唯一直接 `import zmq` 的模块。
- `routing` / `control` 不依赖 `transport` 的 zmq 细节，只消费 transport 暴露的 `(identity, frame)` 抽象。
- `client` / `server` 通过 `transport` + `control` + `routing` 组合，不直接碰 zmq。

---

## 4. 帧格式设计（重设）

### 4.1 帧布局

```
┌──────────┬──────┬──────────┬────────┬──────────┬────────────┬─────────┬──────────┬──────────────┬───────────┬────────┐
│ magic(2B)│ver(1)│msg_type(1)│flags(1)│data_type(1)│topic_len  │ topic(N)│ ts(8B BE)│record_count │ payload   │CRC32?  │
│ b"PM"    │      │          │        │          │ (uint16 BE)│         │  ns      │ (uint32 BE) │ (变长)    │(4B,可选)│
└──────────┴──────┴──────────┴────────┴──────────┴────────────┴─────────┴──────────┴──────────────┴───────────┴────────┘
```

定长帧头 = magic(2) + ver(1) + msg_type(1) + flags(1) + data_type(1) + topic_len(2) + topic(N) + ts(8) + record_count(4)。

字段说明：

| 字段 | 长度 | 说明 |
|------|------|------|
| magic | 2B | 固定 `b"PM"`，用于帧同步与乱序剔除 |
| version | 1B | 协议版本号，本 spec 为 `0x01` |
| msg_type | 1B | `0x01`=DATA, `0x02`=CONTROL, `0x03`=HEARTBEAT, `0x04`=ADMIN |
| flags | 1B | 位域：低 3 位=序列化类型，bit[3:4]=压缩类型，bit7=CRC 开关（见 §4.2） |
| data_type | 1B | 原始数据类型标记（沿用 `DataType`：UNKNOWN/DICT/DATAFRAME/STR/BYTES），供订阅端类型还原 |
| topic_len | 2B | topic 字节长度（big-endian uint16），上限 65535 |
| topic | N B | topic 字符串 UTF-8 |
| ts | 8B | 纳秒时间戳（big-endian int64），发送方写入 |
| record_count | 4B | 本帧记录数（big-endian uint32），DataFrame 为行数，其余为 1；统计与缓存配额用 |
| payload | 变长 | 序列化+压缩后的负载 |
| CRC32 | 4B | 可选，覆盖 magic..payload，由 flags.bit7 决定是否启用 |

### 4.2 flags 位域

沿用现有 flags 编码（ser 低 3 位 + comp bit[3:4]），新增 bit7 作为 CRC 开关：

```
bit:  7        6 5      4 3      2 1 0
      CRC_on   reserved  comp     ser
```

- `bit7=1` 表示帧尾带 CRC32，接收方校验失败抛 `FrameError`。
- `bit[5:6]` 保留，供未来协议演进。
- Spec 1 默认 `bit7=0`（CRC 关闭），仅留字段与编解码通路，不强制启用。
- `ser` / `comp` 编码与现有 `protocol/flags.py` 完全一致，5 序列化 × 4 压缩 = 20 种合法组合不变。

### 4.3 与 ZeroMQ multipart 的关系

`frame` 只负责单条消息的字节流编解码。`transport` 负责把帧放进 zmq multipart：

- 数据面 DEALER→ROUTER：`[identity, frame_bytes]`（identity 由 ROUTER 自动添加）。
- 控制面 DEALER→ROUTER：`[identity, cmd_frame_bytes]`，Server 回 `[identity, reply_frame_bytes]`。

帧格式与 zmq multipart 完全解耦，未来替换传输层不影响 `frame`。

### 4.4 核心接口

```python
def encode(topic, data, *, msg_type=MsgType.DATA, serializer="msgpack",
           compression="none", record_count=1, data_type=DataType.UNKNOWN,
           crc=False, ts_ns=None) -> bytes: ...

def decode(frame_bytes: bytes) -> PulseMessage: ...

def encode_control(cmd: ControlCmd, payload: dict | None = None,
                   serializer="msgpack") -> bytes: ...

def decode_control(frame_bytes: bytes) -> ControlMessage: ...
```

`PulseMessage` 沿用现有结构（topic/payload/raw_payload/record_count/timestamp_ns/serializer/compression/data_type），增加 `msg_type` 字段。

---

## 5. transport 模块（ROUTER/DEALER）

### 5.1 对外接口

```python
class Transport:
    def bind(endpoint, role)                 # Server 侧
    def connect(endpoint, role, credentials) # Client 侧
    def send(identity, frame_bytes)          # 定向发送（ROUTER 用）
    def broadcast(frame_bytes)               # 广播（如心跳，仅控制面/兜底）
    def recv() -> (identity, frame_bytes)    # 接收
    def close()
    def enable_monitor(callback)
```

`role` 取值（内部，不对外）：`producer` / `consumer` / `server_ingress` / `server_egress` / `control`。

### 5.2 socket 拓扑

| 平面 | Server 端 | Client 端 | 端口默认 |
|------|----------|----------|---------|
| 数据面 | ROUTER bind `tcp://0.0.0.0:5555` | DEALER connect | 5555 |
| 控制面 | ROUTER bind `tcp://0.0.0.0:5556` | DEALER connect | 5556 |

- 数据面：Server 从 ROUTER 收 `[identity, frame]`，按 routing 表把帧转发到匹配订阅者的 identity：`send(matched_identity, frame)`。
- 控制面：请求-响应。Client 发 `[identity, cmd_frame]`，Server 回 `[identity, reply_frame]`。

### 5.3 认证（ZAP PLAIN）

- 两套 socket 都设置 `PLAIN_USERNAME` / `PLAIN_PASSWORD`（Client 侧）与 inproc ZAP handler（Server 侧）。
- ZAP handler 沿用现有 `AsyncZAPHandler` 实现思路（v3.1.1 修复后的版本：统一 `await` + 异常保护，单次 send 失败不杀循环）。
- 凭据源在 Spec 1 为最简 dict / TOML 白名单（明文），Spec 2 替换为 `security` 模块。
- 认证只在连接建立阶段发生，通过后进入正常收发。
- 关键事件（上线/认证失败）继续 `print` 到 stderr 保证可见（沿用现有 `_notice` 策略），同时走 `logging`。

### 5.4 monitor（Client 侧）

Client 端 DEALER 始终挂 monitor socket，掩码沿用现有 `_MONITOR_MASK`（握手成功/认证失败/协议失败/断开）。monitor 事件驱动 Client 运行期重连状态机（见 §8）。

### 5.5 关闭顺序（沿用现有坑位经验）

- monitor socket 必须在业务 socket 之前关闭，否则 `ctx.term()` 卡死。
- 关闭顺序：停 monitor → close 数据面 socket → close 控制面 socket → 停 ZAP → term ctx。
- `LINGER=1000` 保证关闭时刷新缓冲。

---

## 6. routing 模块

### 6.1 订阅表

```python
class SubscriptionTable:
    def subscribe(identity, topic_pattern)      # 注册订阅
    def unsubscribe(identity, topic_pattern)    # 取消订阅
    def remove(identity)                        # Client 掉线时清理其所有订阅
    def match(topic) -> set[identity]           # 返回匹配该 topic 的所有 identity
    def subscribers_of(identity) -> set[str]    # 返回某 identity 的所有订阅 pattern
    def snapshot() -> dict                      # 供监控（Spec 3）
```

### 6.2 匹配规则

- 前缀匹配：`market.stock.*` 匹配 `market.stock.600000`、`market.stock.sh.600001`。
- `*` 作为通配尾缀；精确 topic（无 `*`）仅匹配自身。
- 单 identity 可订阅多个 pattern；同一 pattern 重复订阅幂等。

### 6.3 与 control 的关系

订阅表的变更只由 control 面的 `SUBSCRIBE` / `UNSUBSCRIBE` 命令驱动，数据面只读 `match()`。这样路由逻辑与传输解耦，可独立测试。

---

## 7. control 模块

### 7.1 命令集

| 命令 | 方向 | 载荷 | 响应 |
|------|------|------|------|
| `REGISTER` | C→S | username, client_id, role, topics[] | OK / ALREADY_ONLINE / REJECTED |
| `HEARTBEAT` | C→S | client_id | OK |
| `SUBSCRIBE` | C→S | client_id, topic_pattern | OK |
| `UNSUBSCRIBE` | C→S | client_id, topic_pattern | OK |
| `DISCONNECT` | C→S | client_id | OK |
| `KICK`（预留） | admin→S | username | OK（Spec 2/3 接入） |

控制面帧用 `msg_type=CONTROL`，payload 用 msgpack 序列化的 dict。

### 7.2 在线用户表

```python
@dataclass
class ClientInfo:
    client_id: str
    username: str
    endpoint: str
    roles: list[str]
    topics: list[str]
    connected_at: float
    last_seen: float

class OnlineRegistry:
    def register(info) -> RegisterResult    # OK / ALREADY_ONLINE
    def heartbeat(client_id)                # 更新 last_seen
    def unregister(client_id)
    def sweep_timeout(timeout_s) -> list[ClientInfo]  # 清理超时，返回掉线列表
    def snapshot() -> dict                  # 供监控（Spec 3）
```

- key 为 `username`（单用户单在线）：同一 username 已在线时，新 REGISTER 返回 `ALREADY_ONLINE`，Client 启动期直接退出（exit 4）。
- `client_id` 由 Client 首次启动生成（UUID），重连时复用，便于 Server 识别为同一连接的恢复。
- Server 后台心跳扫描任务：`last_seen` 超 6s 判定掉线，清理在线表 + 调 `routing.remove(identity)`，输出 `[CLIENT] ... reason=heartbeat_timeout` 日志。

### 7.3 控制面收发

Server 控制面 ROUTER 收到 `[identity, cmd_frame]` → `decode_control` → 分发到对应 handler → 生成 reply → `send(identity, reply_frame)`。Client 控制面 DEALER 收 reply 后唤醒等待的请求（用 `asyncio.Future` 配对请求-响应）。

---

## 8. client 模块

### 8.1 类层次

| 类 | 说明 |
|---|---|
| `Client` | 通用客户端，可发布 + 可订阅 |
| `ProducerClient` | 只发布（继承 Client，屏蔽 subscribe） |
| `ConsumerClient` | 只订阅（继承 Client，屏蔽 publish） |

### 8.2 启动期硬失败

`Client.start()` / `run_forever()` 启动阶段按序执行，任一失败立即抛异常并退出，不静默重试：

| 步骤 | 失败异常 | 退出码 |
|------|---------|--------|
| 1. 解析配置凭据 | `ConfigurationError` | 6 |
| 2. 连接数据面 DEALER | `ClientStartupError(reason=CONNECT_FAILED)` | 4 |
| 3. PLAIN 认证 | `AuthenticationError(reason=...)` | 3 |
| 4. 连接控制面 DEALER | `ClientStartupError(reason=CONTROL_CONNECT_FAILED)` | 4 |
| 5. REGISTER | `ClientStartupError(reason=ALREADY_ONLINE / REGISTER_REJECTED)` | 4 |
| 6. 进入运行循环 | — | — |

认证失败原因细分：`user_not_found` / `invalid_password` / `user_disabled`（Spec 1 凭据源简版，`user_disabled` 留接口，Spec 2 实现 enabled 字段）。

错误信息必须包含：失败原因、目标 Server 地址、用户名、建议排查方向。

### 8.3 运行期自动重连 + 重新认证

monitor 检测到断线（心跳超时 / socket disconnected）后：

1. 进入重连循环，指数退避：首次 1s，倍数 2，上限 30s。
2. 重连成功 → 重新 PLAIN 认证。
3. 重新 REGISTER（携带原 client_id、topics）。
4. 恢复之前的订阅（SUBSCRIBE 所有原 pattern）。
5. 继续接收消息。

边界：
- 重连后认证失败 → 按启动期认证失败处理，直接退出（exit 3）。
- 重连后注册失败（如 username 仍在线）→ 直接退出（exit 4），由外部进程管理器处理。
- 恢复订阅对业务层透明，无需业务代码重新调用 `subscribe()`。

### 8.4 心跳

Client 主动发 `HEARTBEAT`，默认 1s 间隔；Server 6s 未收到判定断线。重连成功后心跳恢复。

### 8.5 状态前置校验装饰器

```python
def require_connected(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        if not self._connected or not self._authenticated:
            raise ConnectionError("Client 未连接或认证失败，无法执行操作")
        return func(self, *args, **kwargs)
    return wrapper

class Client:
    @require_connected
    def publish(self, topic, data): ...
    @require_connected
    def subscribe(self, topic, callback): ...
```

另设 `@require_registered` 用于控制面注册完成后的操作。统一收敛边界检查，主循环更简洁。

### 8.6 连接事件回调

`on_connected` / `on_disconnected` / `on_reconnecting`，可选，供业务层感知连接状态。

### 8.7 producer 调度管线复用

`producers/manager.py` 的装饰器 + 间隔/burst 调度保留，作为 `ProducerClient` 之上的可选 helper：

```python
producer = ProducerClient(credentials=creds)
@producer.schedule("market.futures.IC2406", interval=0.5)
async def gen():
    return df
producer.run_forever()
```

调度管线产出的数据交给 `ProducerClient.publish()`，复用 §8.5 的发布路径。现有 `ProducerSpec` / `ProducerManager` 接口基本不变，仅 `on_message` 回调改为调用 `publish`。

---

## 9. server 模块

### 9.1 组装

```python
class Server:
    def __init__(self, data_endpoint="tcp://0.0.0.0:5555",
                 control_endpoint="tcp://0.0.0.0:5556",
                 admin_endpoint="0.0.0.0:9090",
                 credentials: dict | None = None,   # Spec 1 最简凭据源
                 config: ServerConfig | None = None):
        ...
    def start()
    def wait_for_shutdown()
```

组件：transport（ingress ROUTER + egress 复用 ingress + control ROUTER）→ auth（ZAP）→ routing → control → monitoring 占位（Spec 3 填充）→ lifecycle。

### 9.2 运行任务

| 任务 | 触发 | 职责 |
|------|------|------|
| 数据面接收循环 | 常驻 | ROUTER recv → decode → routing.match → 逐 identity 转发 |
| 控制面循环 | 常驻 | ROUTER recv → decode_control → 分发命令 → 回 reply |
| 心跳扫描循环 | 每 1s | `OnlineRegistry.sweep_timeout(6.0)` 清理掉线 |
| 分钟滚动循环 | 每 60s | 沿用现有 stats 归档 + SQLite 落库 |
| admin 服务 | 常驻 | 沿用现有 AdminServer（Spec 3 才扩展） |

### 9.3 关键行为

- 同一 username 同时只能有一个 Client 在线。
- 不持久化消息，所有消息内存转发，尽力投递。
- 多个 ProducerClient 可同时发布，Server 汇总后分发。

---

## 10. lifecycle 模块

- 统一 Server / Client 启动顺序与关闭顺序。
- `SIGTERM` / `SIGINT` 信号处理 → 触发优雅关闭。
- Client 启动失败：任一环节失败立即清理已创建资源（按 §5.5 关闭顺序），返回非零退出码。
- Server 关闭顺序：停接收 → 停分发 → 停 control → 归档统计 → 停 admin → 停 ZAP → close socket → term ctx。
- 复用现有 `_shutdown` 经验：先停 producer（停止产生新消息）→ 归档统计 → 停 admin → 停 transport → 关 storage。

---

## 11. 基础设施模块

### 11.1 errors

```python
class PulseMQError(Exception): ...
class TransportError(PulseMQError): ...        # exit 2
class ConnectionError(PulseMQError): ...       # exit 2
class AuthenticationError(PulseMQError): ...   # exit 3
class ClientStartupError(PulseMQError): ...    # exit 4，封装底层原因 + 目标地址 + 用户名
class FrameError(PulseMQError): ...            # exit 5
class SerializationError(PulseMQError): ...    # exit 5
class ConfigurationError(PulseMQError): ...    # exit 6
class ResourceExhaustedError(PulseMQError): ...# exit 7
```

`ClientStartupError` 必须封装底层 reason（`CONNECT_FAILED` / `CONTROL_CONNECT_FAILED` / `ALREADY_ONLINE` / `REGISTER_REJECTED`），并附带目标地址与用户名。CLI 工具根据异常类型返回对应退出码。

### 11.2 config

TOML + 环境变量，所有项默认值（照总体设计文档 1.4 / 3.2.7 节）：

```toml
[server]
data_endpoint = "tcp://0.0.0.0:5555"
control_endpoint = "tcp://0.0.0.0:5556"
admin_endpoint = "0.0.0.0:9090"

[auth]
type = "plain"                       # Spec 1 固定 plain
credentials_file = "./pulsemq_users.toml"  # Spec 2 才实现哈希；Spec 1 读明文白名单

[protocol]
serialization = "msgpack"
compression = "none"

[client]
heartbeat_interval = 1.0
heartbeat_timeout = 6.0
reconnect_initial_delay = 1.0
reconnect_max_delay = 30.0
reconnect_backoff_multiplier = 2.0
```

环境变量覆盖：`PULSEMQ_DATA_ENDPOINT`、`PULSEMQ_CONTROL_ENDPOINT`、`PULSEMQ_ADMIN_BIND` 等。`Server()` / `Client()` 零参即可运行。

### 11.3 logging

- loguru，格式 `{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {module}:{line} | {message}`。
- 所有模块 `logger = logging.getLogger(__name__)` 风格命名，便于按模块过滤。
- Client 生命周期事件按总体设计文档 3.2.9 节那张表结构化输出（连接尝试/认证成功失败/上线/重复登录拒绝/订阅/断开/心跳超时/重连/重连失败）。
- 关键认证事件同时 `print` 到 stderr 保证可见（沿用现有 `_notice` 策略）。
- 支持结构化（JSON）输出，便于 ELK/Loki 采集（Spec 1 留配置开关，默认文本）。

---

## 12. 消息流

### 12.1 发布流

```
ProducerClient.publish(topic, data)
  → frame.encode(topic, data, msg_type=DATA, ...)  # serialize + compress + 组帧
  → transport.send(server_identity, frame)          # 数据面 DEALER→ROUTER
Server.transport.recv()
  → frame.decode(frame_bytes)
  → routing.match(topic) → {identity1, identity2, ...}
  → for id in matched: transport.send(id, frame)    # 转发到匹配订阅者
ConsumerClient.transport.recv() → frame.decode → 回调
```

### 12.2 订阅流

```
ConsumerClient.subscribe(topic, callback)
  → control.SUBSCRIBE(client_id, topic)   # 控制面 DEALER→ROUTER
  → Server 接收 → routing.subscribe(identity, topic) → reply OK
后续该 topic 消息由 Server 数据面转发到此 identity
```

### 12.3 启动失败流

照 §8.2 表，任一步失败抛对应异常 → lifecycle 清理资源 → CLI 按异常类型返回非零退出码。

### 12.4 运行期重连流

照 §8.3：monitor 检测断线 → 指数退避重连 → 重新认证 → 重新 REGISTER → 恢复订阅 → 继续收消息。

---

## 13. 测试策略

### 13.1 沿用测试（应继续通过）

`test_protocol.py`（serialization/compression 部分）、`test_stats.py`、`test_data_types.py`、`test_producer_types.py` 中不依赖旧 PUB/SUB 角色的部分。

### 13.2 重写测试

- `test_e2e_publisher.py` / `test_e2e_subscriber.py` → 改写为 Client/Server e2e：发布→中继→订阅收达、多 ProducerClient 并发、单用户多 topic 订阅。
- `test_zap_resilience.py` → 改写为 Client 启动期认证失败 + 运行期重连认证的 ZAP 韧性测试。
- `test_publisher_shutdown.py` → 改写为 Server/Client 优雅关闭测试。

### 13.3 新增测试

- `test_frames_v2.py`：新帧格式往返、魔数/版本校验、CRC 开关、乱序帧剔除。
- `test_routing.py`：前缀匹配、单 identity 多 pattern、`remove` 清理、幂等订阅。
- `test_control.py`：REGISTER/ALREADY_ONLINE/心跳超时清理/SUBSCRIBE/UNSUBSCRIBE。
- `test_client_lifecycle.py`：启动期各失败场景（Server 不在线、密码错、用户已在线、注册被拒）+ 退出码；运行期断线重连+恢复订阅的 e2e。
- `test_lifecycle.py`：信号触发的优雅关闭、资源回收、关闭顺序。

### 13.4 性能基线

沿用现有 lock-free 统计约束：`TrafficStats.record()` 不引入锁；本 spec 不在消息路径加锁。重连/控制面逻辑不得阻塞数据面接收循环（控制面独立 socket + 独立任务）。

---

## 14. 关键设计决策（Spec 1 范围）

| 决策 | 说明 |
|------|------|
| 原地大改、不保过渡 | 系统干净切换，不背过渡包袱；过渡期不可运行 |
| ROUTER/DEALER 双平面 | Server 掌握 identity，支持精确路由/在线表/踢人；数据面控制面分端口 |
| 帧格式重设 | 加魔数/版本/帧类型/可选 CRC，支持协议演进；不兼容旧版 |
| ser/comp 编码复用 | 沿用 flags 编码与 serialization/compression 注册表，减少改动 |
| 认证接口稳定、凭据源可替换 | Spec 1 用最简白名单，Spec 2 替换为 security 模块，不动 transport/auth |
| 启动硬失败 + 运行期重连 | 启动期配置错误快速退出；运行期断线自动恢复，满足行情高时效 |
| 单用户单在线 | Server 在线表 key=username，重复登录拒绝 |
| client_id 复用 | 重连时复用原 client_id，便于 Server 识别为恢复而非新连接 |
| producer 管线保留 | 作为 ProducerClient 之上的可选 helper，复用调度能力 |
| topic 缓存保留 | 收敛为"最近值快照"语义，不做断线补发（与总体设计"不补发"一致） |
| 不持久化消息 | 内存转发，尽力投递 |

---

## 15. 边界与注意事项

1. **过渡期不可运行**：原地大改意味着重构进行中系统无法启动，需在分支上完成整体 Spec 1 后再合并。
2. **不兼容旧客户端**：帧格式变更，旧 PulseSubscriber 无法连新 Server。
3. **PLAIN 明文传输**：密码在 TCP 上明文，仅适用于可信内网；跨不可信网络留待阶段 5（TLS/CURVE）。
4. **Spec 1 凭据为明文**：仅用于跑通认证闭环，生产强度哈希/默认生成/CLI 在 Spec 2 完成前不可用于生产。
5. **背压与水位**：需合理配置 ROUTER/DEALER 的 SNDHWM/RCVHWM，防止慢消费者拖垮 Server（沿用现有 `SNDHWM=0` 策略需在 ROUTER 侧重新评估）。
6. **monitor 关闭顺序**：monitor socket 必须先于业务 socket 关闭，否则 `ctx.term()` 卡死（pyzmq 已知行为）。
7. **控制面不得阻塞数据面**：控制面独立 socket + 独立 asyncio 任务，请求-响应用 Future 配对，不与数据面 recv 循环竞争。
8. **心跳扫描频率**：Server 心跳扫描每 1s 一次，6s 超时；扫描任务异常不得杀掉整个 Server。
9. **日志隐私**：下线日志含 topics，若 topic 含敏感数据需考虑脱敏（Spec 1 留 TODO，后续评估）。

---

## 16. 与后续 spec 的衔接

- **Spec 2（安全）**：实现 `security` 模块（bcrypt 哈希、默认凭据自动生成、`pulsemq.users` CLI），替换 Spec 1 的最简凭据源；`auth.PlainAuth` 从 `security` 取白名单做决策；admin HTTP 加随机 token 认证。接口已在本 spec 预留（`credentials` 参数、`user_disabled` reason）。
- **Spec 3（监控扩展）**：`monitoring` 填充 Spec 1 留的占位：在线 Client 详情、端到端延迟分位（`LatencyStats`）、最近事件流（`ConnectionStats` + 事件环）、web_ui 新区块、admin 新路由（`/api/v1/clients`、`/api/v1/events`）。`routing.snapshot()` / `OnlineRegistry.snapshot()` 已预留供监控读取。
