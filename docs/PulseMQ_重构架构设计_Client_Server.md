# PulseMQ 重构架构设计（基于 ZeroMQ，仅暴露 Client/Server）

> 版本：v1.0  
> 定位：面向金融行情的高性能消息分发系统  
> 设计原则：ZeroMQ 作为底层传输实现，对外仅保留 Client 与 Server 两种角色。  
> 安全约束：**仅支持 PLAIN 用户名/密码认证，且为强制开启，不可关闭。密码在传输层为明文，适用于可信内网。**

---

## 1. 设计哲学

### 1.1 统一角色：Client / Server

| 角色 | 说明 |
|------|------|
| **Client** | 消息的生产者或消费者。一个 Client 可以同时发布（publish）和订阅（subscribe），也可以只扮演其中一种角色。 |
| **Server** | 消息的中继与分发中心。负责接收来自多个 Client 的消息，并根据主题（topic）转发给所有感兴趣的 Consumer Client。 |

- 对外 API 只有 `Client`、`Server`、`ProducerClient`、`ConsumerClient`。
- 不暴露 `PUB/SUB`、`XPUB/XSUB`、`ROUTER/DEALER` 等 ZeroMQ 术语。
- ZeroMQ 仅作为 `transport` 模块的内部实现细节。

### 1.2 数据面与控制面分离

| 平面 | 职责 | 说明 |
|------|------|------|
| **数据面** | 传输业务消息 | Client → Server → Client 的消息流 |
| **控制面** | 连接注册、心跳、管理命令 | 用于状态维护和可观测性 |

### 1.3 协议栈可插拔

消息协议由三个独立层组合：

```text
消息帧（frame） → 序列化（serialization） → 压缩（compression） → 传输（transport）
```

三者可以独立扩展、独立注册，通过配置动态组合。

### 1.4 零配置优先

系统应遵循“默认即可运行”原则，避免用户为了启动一个 Server 而编写大量配置。

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `data_endpoint` | `tcp://0.0.0.0:5555` | 数据面默认端口 |
| `control_endpoint` | `tcp://0.0.0.0:5556` | 控制面默认端口 |
| `admin_endpoint` | `http://0.0.0.0:9090` | 监控 UI 默认端口 |
| `auth.type` | `plain` | 固定 PLAIN，强制启用 |
| `auth.credentials_file` | `./pulsemq_users.toml` | 默认凭据文件，不存在时自动生成 |
| `auth.allow_auto_generated_credentials` | `true` | 无凭据时自动生成 admin 用户 |
| `protocol.serialization` | `msgpack` | 默认序列化 |
| `protocol.compression` | `none` | 默认不压缩 |
| `client.heartbeat_interval` | `1.0` | 心跳发送间隔 |
| `client.heartbeat_timeout` | `6.0` | 心跳超时判定 |
| `client.reconnect_initial_delay` | `1.0` | 首次重连间隔 |
| `client.reconnect_max_delay` | `30.0` | 最大重连间隔 |
| `monitoring.ui_enabled` | `true` | 默认启用 Web 监控界面 |
| `monitoring.storage` | `sqlite://./pulsemq_stats.sqlite` | 默认 SQLite 持久化 |

用户最少只需要运行即可启动完整服务，默认凭据会自动生成并打印到日志：

```text
python -m pulsemq.server
# 日志输出：
# [SECURITY] 默认用户 admin / 密码 Rx9#mK2pL$vQ
```

也支持通过环境变量指定默认密码：

```text
PULSEMQ_ADMIN_PASSWORD=mypassword python -m pulsemq.server
```

```python
from pulsemq import Server
server = Server()   # 全部使用默认配置，自动初始化凭据与监控 UI
server.start()
```

### 1.5 启动期硬失败与运行期自动重连（Client 侧）

**启动期**：Client 在启动阶段遇到以下任一情况时，必须**立即主动退出**，不做静默重试或无限重连：

1. **无法连接到 Server**（Server 不在线、网络不可达、端口错误）。
2. **PLAIN 认证失败**（用户名不存在、密码错误、用户被禁用）。
3. **控制面注册失败**（Server 接受连接但拒绝注册请求，如 username 已在线）。

**运行期**：Client 进入正常运行后，若因网络闪断或 Server 重启导致连接中断，必须**自动重连并重新认证**（不是直接退出），以满足行情高时效性要求：

- 心跳间隔：**1 秒**
- 心跳超时：**6 秒**（连续 6 秒未收到心跳即判定断线）
- 重连策略：指数退避，首次重连间隔 1 秒，最大间隔 30 秒
- 重连成功后必须重新走完整认证 + 控制面注册流程
- 如果重连过程中认证失败，按启动期认证失败处理（直接退出）

**设计理由**：

- 启动期失败多为配置错误，应快速退出，便于 systemd/K8s/supervisor 感知并重启或告警。
- 运行期断线在行情场景中不可避免（Server 滚动升级、网络抖动），必须自动恢复，不能依赖外部进程管理器重启。
- 重新认证确保断线恢复后的连接仍然可信，且能处理 Server 端凭据变更。

**实现要求**：

- `Client.start()` / `Client.run_forever()` 在启动阶段失败时抛出 `ClientStartupError`。
- 运行期断线由 `Client` 内部循环处理：断开 → 重连 → 重新认证 → 重新注册 → 重新订阅。
- 错误信息必须包含失败原因、目标 Server 地址、用户名、建议排查方向。
- 启动期失败返回非零退出码，便于 shell 脚本和进程管理器识别。

---

## 2. 总体架构

```text
┌────────────────────────────────────────────────────────────────────────────┐
│                              Client Layer                                  │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────────────┐  │
│  │ ProducerClient  │    │ ConsumerClient  │    │        Client         │  │
│  │   (仅发布)       │    │   (仅订阅)       │    │   (可发布+可订阅)      │  │
│  └────────┬────────┘    └────────┬────────┘    └───────────┬─────────────┘  │
│           │                      │                         │                │
│           ▼                      ▼                         ▼                │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                         Client SDK                                  │   │
│  │  publish() / subscribe() / reconnect() / health_report()          │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
└────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      │ 数据面 + 控制面
                                      ▼
┌────────────────────────────────────────────────────────────────────────────┐
│                              Server Layer                                  │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                         Server                                      │   │
│  │  ┌──────────────┐  ┌──────────────┐  ┌─────────────────────────┐  │   │
│  │  │  接收端       │  │  分发端       │  │       控制端             │  │   │
│  │  │ 接收所有 Client│  │ 向所有感兴趣  │  │ 注册 / 心跳 / 管理命令   │  │   │
│  │  │ 发布的消息     │  │ 的 Client 转发 │  │                          │  │   │
│  │  └──────┬───────┘  └──────┬───────┘  └────────────┬───────────────┘  │   │
│  │         │                 │                       │                   │   │
│  │         └─────────────────┼───────────────────────┘                   │   │
│  │                           ▼                                           │   │
│  │                  ┌─────────────────┐                                    │   │
│  │                  │  主题路由与分发   │                                    │   │
│  │                  │  topic matching  │                                    │   │
│  │                  └─────────────────┘                                    │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
└────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌────────────────────────────────────────────────────────────────────────────┐
│                          Infrastructure Layer                              │
│  ┌─────────┐ ┌───────────────┐ ┌────────────┐ ┌─────────┐ ┌──────────┐  │
│  │  frame  │ │ serialization │ │ compression│ │  auth   │ │ security │  │
│  └─────────┘ └───────────────┘ └────────────┘ └─────────┘ └──────────┘  │
│  ┌─────────┐ ┌───────────────┐ ┌────────────┐ ┌─────────┐ ┌──────────┐  │
│  │ transport│ │   monitoring  │ │  control   │ │ config  │ │  errors  │  │
│  └─────────┘ └───────────────┘ └────────────┘ └─────────┘ └──────────┘  │
│  ┌─────────┐ ┌───────────────┐ ┌────────────┐                           │
│  │ lifecycle│ │    admin/api  │ │  logging   │                           │
│  └─────────┘ └───────────────┘ └────────────┘                             │
└────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. 模块清单

### 3.1 必含模块（用户指定）

#### 3.1.1 `frame` — 消息帧模块

**职责**：定义消息在传输前的二进制格式，包括：

- 魔数（magic number）与版本号
- 消息类型（data / control / heartbeat / admin）
- 主题（topic）长度与内容
- 时间戳（timestamp）
- 序列化类型标识
- 压缩类型标识
- payload 长度与内容
- 校验位（可选 CRC32）

**设计要点**：

- 帧格式与 ZeroMQ 的 multipart 解耦。`frame` 只负责字节流编解码，
  `transport` 负责把帧放进合适的传输原语中。
- 预留扩展字段，用于未来协议演进。

#### 3.1.2 `serialization` — 序列化模块

**职责**：提供数据对象到字节流的转换能力。

**内置实现**：

| 名称 | 说明 | 适用场景 |
|------|------|---------|
| `raw` | 原始 bytes | 已序列化数据透传 |
| `str` | UTF-8 字符串 | 文本消息 |
| `json` | JSON 格式 | 通用配置/命令 |
| `msgpack` | MessagePack 二进制 | 高性能通用序列化 |
| `pyarrow` | Arrow 格式 | 行情 DataFrame |

**注册机制**：

```text
SerializationRegistry.register(name, serializer)
```

#### 3.1.3 `compression` — 压缩格式模块

**职责**：提供 payload 压缩/解压能力。

**内置实现**：

| 名称 | 说明 | 适用场景 |
|------|------|---------|
| `none` | 无压缩 | 小消息或已压缩数据 |
| `snappy` | 高速压缩 | 延迟敏感 |
| `lz4` | 高压缩比+高速 | 通用场景 |
| `zstd` | 高压缩比 | 大消息、批量数据 |

**统一 ImportError 守卫**：若某压缩库未安装，注册时抛出明确异常，避免运行时才发现缺失。

#### 3.1.4 `auth` — 鉴权模块

**职责**：决定“谁可以连接到 Server”。

**抽象接口**：

```text
AuthProvider.authenticate(credentials) -> AuthResult
```

**实现类**：

| 名称 | 说明 | 风险 |
|------|------|------|
| `PlainAuth` | 用户名 + 密码 | **本项目唯一支持且强制开启** |

> 注：本文档中不再列出 `NullAuth` 与 `CurveAuth`。PLAIN 为当前项目的强制约束，所有连接必须使用用户名/密码认证。

**设计要点**：

- 鉴权只在连接建立阶段发生，通过后再进入正常消息收发。
- 鉴权结果支持：允许、拒绝、拒绝原因（用于日志与监控）。
- 用户名/密码白名单由配置文件或持久化存储提供，Server 启动时加载。
- ZAP 认证请求由 `transport` 转发给 `PlainAuth`，业务逻辑不直接依赖 ZeroMQ ZAP 细节。
- 密码在存储侧必须做哈希处理（如 bcrypt/argon2），严禁明文保存；传输侧为 PLAIN 协议原生的明文，因此只适用于可信内网。

#### 3.1.5 `monitoring` — 监控模块

**职责**：全链路可观测性，并提供开箱即用、对消息路径零阻塞的 Web 监控界面。

**内部组成（沿用并优化现有 PulseMQ 监控体系）**：

| 子模块 | 来源/复用 | 职责调整 |
|--------|----------|---------|
| `TrafficStats` | `stats/traffic.py` | 内存中按 topic 分钟级聚合，8 小时滚动窗口；采集路径保持 lock-free |
| `StatsStorage` | `stats/storage.py` | SQLite 分钟统计持久化；改为**异步批量写入**，不阻塞主循环 |
| `ConnectionStats` | 新增 | 在线用户/Client 快照、认证事件、连接/断线事件 |
| `LatencyStats` | 新增 | 端到端延迟直方图与分位值（P50/P99） |
| `AdminServer` | `admin/server.py` | 基于 asyncio 的 HTTP 服务，提供 REST + SSE；运行在**独立事件循环/线程** |
| `Web UI` | `admin/web_ui.py` | 单文件 HTML，内嵌 ECharts 深色玻璃态面板；沿用现有风格并扩展 |

**提供能力**：

- 连接事件：连接建立、断开、认证成功/失败、用户重复登录
- 在线状态：当前在线用户列表、Client ID、角色、订阅 topic、连接时长
- 流量指标：发送速率、接收速率、topic 分布
- 分钟级历史曲线：支持 1H / 6H 切换，最多 5 个 topic 叠加
- 端到端延迟：P50 / P99 延迟分位值
- SSE 实时推送：1 秒一帧，仅推送 diff
- topic 卡片网格与系统状态
- SQLite 持久化：仅持久化分钟级聚合，默认 7 天清理

**设计要点**：

- Web UI 默认随 Server 一起启动，无需额外部署；可通过配置关闭。
- **监控不得阻塞消息数据路径**：所有监控事件先进入无锁/有界队列，由独立 consumer 处理。
- **统计采集 lock-free**：`TrafficStats` 继续利用 Python GIL 保证单写者多读者安全，不引入互斥锁。
- **SQLite 写入异步化**：分钟归档数据通过 `asyncio.Queue` 交给独立任务批量写入，避免磁盘 I/O 阻塞主循环。
- **Admin HTTP 独立运行**：AdminServer 可运行在独立线程的事件循环中，ZeroMQ 数据线程不被 HTTP 请求打断。
- SSE 客户端队列设置 `maxsize=64`，慢消费者自动丢帧，防止反压至 Server。

---

### 3.2 建议补充模块

#### 3.2.1 `security` — 安全凭证模块

**职责**：与 `auth` 解耦，专门负责 PLAIN 认证凭据的安全管理：

- 管理用户名/密码白名单
- 密码哈希生成与校验（如 bcrypt/argon2）
- 从文件、环境变量或外部安全服务加载凭据
- 支持凭据热更新（reload）
- **默认凭据自动生成**：若未提供凭据文件，Server 启动时自动生成默认用户并输出到日志

**与 `auth` 的关系**：

- `security` 管凭据的存储、哈希、加载。
- `auth.PlainAuth` 用 `security` 提供的白名单做认证决策。

**默认凭据策略**：

| 场景 | 行为 | Server 日志输出 |
|------|------|----------------|
| 已提供 `pulsemq_users.toml` | 加载白名单 | `[SECURITY] 已加载 N 个用户` |
| 未提供或文件不存在 | 自动生成 `admin` + 随机密码 | `[SECURITY] 默认用户 admin / 密码 <random>，请尽快创建正式用户` |
| 环境变量 `PULSEMQ_ADMIN_PASSWORD` | 使用指定密码覆盖随机密码 | `[SECURITY] 已使用环境变量设置 admin 密码` |

**自动生成示例**：

```toml
# pulsemq_users.toml（自动生成）
[users.admin]
hashed_password = "$2b$12$..."
roles = ["publisher", "subscriber"]
enabled = true
```

Server 启动日志：

```text
[2026-06-26 16:20:01] [SECURITY] 未检测到凭据文件，已生成默认用户
[2026-06-26 16:20:01] [SECURITY] username=admin, password=Rx9#mK2pL$vQ
[2026-06-26 16:20:01] [SECURITY] 提示：默认凭据仅用于首次启动，请通过 CLI 或 admin 接口创建正式用户
```

**CLI 工具（最简用户管理）**：

```text
python -m pulsemq.users add alice --password secret --roles publisher,subscriber
python -m pulsemq.users passwd alice
python -m pulsemq.users list
python -m pulsemq.users disable bob
```

> 由于本项目仅支持 PLAIN，`security` 模块不处理非对称密钥；若未来扩展 CURVE，可再引入密钥管理子模块。

#### 3.2.2 `transport` — 传输抽象模块

**职责**：项目中唯一直接使用 pyzmq 的地方，对外隐藏所有 ZeroMQ 细节。

**对外接口（示例）**：

```text
Transport.bind(endpoint, role)
Transport.connect(endpoint, role, credentials)
Transport.send(topic, payload)
Transport.recv() -> (topic, payload, metadata)
Transport.close()
Transport.enable_monitor(callback)
```

**内部角色（不对外暴露）**：

- `producer`：向 Server 发送数据
- `consumer`：从 Server 接收数据
- `server_ingress`：Server 接收 Client 数据
- `server_egress`：Server 向 Client 分发数据
- `control`：控制面

**设计要点**：

- 未来若替换为 Aeron、QUIC 或自研 TCP，只需替换此模块。
- ZeroMQ 的 socket 类型、multipart 帧、ZAP 认证都封装在内部。

#### 3.2.3 `routing` — 主题路由模块

**职责**：

- 维护 topic 到 Consumer Client 的订阅表
- 支持前缀匹配（如 `market.stock.*`）
- 支持未来按 topic 做访问控制（ACL）
- 记录每个 topic 的订阅数与转发量

**为什么单独一个模块**：

- 路由逻辑是 Server 的核心，未来可能引入按 topic 的限流、审计、权限控制。
- 与传输层分离后，可以独立测试与替换路由算法。

#### 3.2.4 `control` — 控制面模块

**职责**：替代当前自定义的 PUSH/PULL 心跳通道，负责：

- Client 注册与身份声明（username、role、client_id、订阅 topic 列表）
- 心跳保活（1 秒间隔，6 秒超时）
- 优雅断开通知
- 动态 topic 订阅/取消订阅
- Server 管理命令（如强制踢人、查询连接）
- 维护 Server 端在线用户表，防止同一 username 启动多个 Client

**设计要点**：

- 控制面与数据面使用独立端口，避免业务流量影响控制信令。
- 控制面使用请求-响应语义，便于管理命令返回结果。
- 心跳由 Client 主动发送，Server 维护 `last_seen` 并在 6 秒超时后清理。
- Server 端在线用户表 key 为 `username`，value 包括：`client_id`、`endpoint`、`roles`、`topics`、`connected_at`、`last_seen`。
- 同一 `username` 已有在线连接时，新连接注册失败，返回 `ALREADY_ONLINE`，Client 启动期直接退出。

**在线用户表示例**：

```text
{
  "alice": {
    "client_id": "uuid-1234",
    "endpoint": "192.168.1.10:54321",
    "roles": ["publisher", "subscriber"],
    "topics": ["market.stock.*", "market.futures.IC2406"],
    "connected_at": 1700000000.0,
    "last_seen": 1700000006.0
  }
}
```

#### 3.2.5 `client` — 客户端 SDK 模块

**职责**：统一当前 producer 与 subscriber 的客户端逻辑。

**提供类**：

| 类 | 说明 |
|---|---|
| `Client` | 通用客户端，可发布、可订阅 |
| `ProducerClient` | 只发布 |
| `ConsumerClient` | 只订阅 |

**内置能力**：

- **启动期硬失败**：首次连接、认证、控制面注册失败时立即抛出异常并退出，不静默重试。
- **运行期自动重连 + 重新认证**：断线后自动重连并重新走认证、注册、订阅流程。
- **心跳**：默认 1 秒发送一次，Server 6 秒未收到则判定断线。
- **断线恢复后自动重新订阅**：重连成功后恢复之前的订阅列表。
- **发布失败缓存与重试**：运行期行为，可选开启。
- **连接事件回调**：`on_connected`, `on_disconnected`, `on_reconnecting`。

**默认连接参数**：

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `heartbeat_interval` | `1.0` | 心跳发送间隔（秒） |
| `heartbeat_timeout` | `6.0` | 心跳超时判定（秒） |
| `reconnect_initial_delay` | `1.0` | 首次重连间隔（秒） |
| `reconnect_max_delay` | `30.0` | 最大重连间隔（秒） |
| `reconnect_backoff_multiplier` | `2.0` | 指数退避倍数 |

**参考现有项目 + 装饰器简化**：

Client 的连接、认证、心跳、重连逻辑可参考现有 `subscriber.py` 的实现模式。为避免重复的状态判断和错误处理代码，建议使用装饰器或上下文管理器包装“必须在连接状态下执行”的操作：

```python
from functools import wraps

def require_connected(func):
    """装饰器：仅在连接+认证成功后才允许执行"""
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        if not self._connected or not self._authenticated:
            raise ConnectionError("Client 未连接或认证失败，无法执行操作")
        return func(self, *args, **kwargs)
    return wrapper

class Client:
    @require_connected
    def publish(self, topic: str, data):
        ...

    @require_connected
    def subscribe(self, topic: str, callback):
        ...
```

类似地，可以定义 `@require_registered` 用于控制面注册完成后的操作。这样 publish/subscribe/heartbeat 等方法的边界检查可以统一收敛，主循环代码更简洁。

**启动失败即退出示例**：

```text
from pulsemq import Client, PlainCredentials

try:
    client = Client(credentials=PlainCredentials(username="alice", password="secret"))
    client.run_forever()
except ClientStartupError as e:
    # 连接失败 / 认证失败 / 注册失败均已在此抛出
    logger.error(f"Client 启动失败: {e}")
    sys.exit(1)
```

#### 3.2.6 `server` — 服务端入口模块

**职责**：组装所有 Server 组件，提供单一入口。

```text
Server(
    data_endpoint="tcp://0.0.0.0:5555",
    control_endpoint="tcp://0.0.0.0:5556",
    admin_endpoint="0.0.0.0:9090",
    auth=PlainAuth.from_file("./pulsemq_users.toml"),
    # monitoring 默认启用，无需显式传入
)
```

**组成**：

- 接收端（transport）：允许多个 ProducerClient 同时发布消息
- 分发端（transport + routing）：按 topic 转发给所有 ConsumerClient
- 控制端（control）：维护在线用户表、心跳、注册、管理命令
- 鉴权（auth + security）
- 监控（monitoring）
- 生命周期管理（lifecycle）

**关键行为**：

- 同一 `username` 同时只能有一个 Client 在线。
- 不持久化消息，所有消息均为内存转发，尽力投递。
- 多个 ProducerClient 可同时向 Server 发布数据，Server 汇总后分发。

#### 3.2.7 `config` — 配置模块

**职责**：

- 统一加载 TOML/YAML/环境变量配置
- 提供默认值与类型校验
- 避免配置散落在各模块中

**建议结构（所有项均有默认值）**：

```toml
[server]
data_endpoint = "tcp://0.0.0.0:5555"      # 数据面
control_endpoint = "tcp://0.0.0.0:5556"   # 控制面
admin_endpoint = "0.0.0.0:9090"           # 监控 UI

[auth]
type = "plain"                            # 本项目固定为 plain
credentials_file = "./pulsemq_users.toml" # 用户名/密码白名单
allow_auto_generated_credentials = true   # 无凭据文件时自动生成 admin 用户

[protocol]
serialization = "msgpack"                 # json / msgpack / pyarrow / raw
compression = "none"                      # none / snappy / lz4 / zstd

[client]
heartbeat_interval = 1.0                  # 心跳发送间隔（秒）
heartbeat_timeout = 6.0                     # 心跳超时判定（秒）
reconnect_initial_delay = 1.0               # 首次重连间隔（秒）
reconnect_max_delay = 30.0                  # 最大重连间隔（秒）
reconnect_backoff_multiplier = 2.0          # 指数退避倍数

[monitoring]
ui_enabled = true                         # 是否启动内置 Web 面板
storage = "sqlite://./pulsemq_stats.sqlite"
retention_days = 7
sse_interval = 1.0                        # SSE 推送间隔（秒）
latency_sample_rate = 0.01                # 延迟采样率（1% 采样，可配置为 1.0 全量）
event_ring_size = 200                     # 内存事件环大小
stats_archive_batch_size = 50             # SQLite 批量写入条数
admin_thread = true                       # Admin HTTP 是否运行在独立线程
```

#### 3.2.8 `admin/api` — 管理接口与监控 UI 模块

**职责**：提供 HTTP/SSE 管理接口与内置 Web 监控面板；管理接口不影响消息数据路径。

**复用现有实现**：

- 继续使用 `admin/server.py` 的 asyncio HTTP server（不引入外部 Web 框架，避免依赖膨胀）。
- 继续使用 `admin/web_ui.py` 的单文件深色 ECharts 面板，保持现有视觉风格。

**路由保持并扩展**：

| 路由 | 说明 | 认证 |
|------|------|------|
| `GET /` | 监控面板首页 | 需要 `token` query/header |
| `GET /static/{path}` | ECharts 等静态资源 | 需要 `token` |
| `GET /api/v1/stats/realtime` | 实时指标 JSON | 需要 `token` |
| `GET /api/v1/stats/stream` | SSE 实时推送（1s/帧） | 需要 `token` |
| `GET /api/v1/topics` | topic 列表 + 当前指标 | 需要 `token` |
| `GET /api/v1/topics/{topic}/history` | 分钟级历史（1H/6H） | 需要 `token` |
| `GET /api/v1/clients` | 当前在线 Client 列表（新增） | 需要 `token` |
| `GET /api/v1/events` | 最近连接/认证事件流（新增） | 需要 `token` |
| `GET /api/v1/system/status` | 系统状态 | 需要 `token` |
| `GET /healthz` | 健康检查 | **无需 token** |

**必须增加认证**：

- 当前项目 `:9090` 完全开放，是明显安全缺口。
- 采用启动时生成随机 token 的方案，默认写入 `./pulsemq_admin.token` 并输出到日志。
- 同时支持通过环境变量 `PULSEMQ_ADMIN_TOKEN` 或配置文件预置 token。
- 管理接口与数据接口分离，即使 admin 被攻击也不影响消息收发。
- UI 页面通过 `?token=xxx` 或 Header 携带 token。

**新增 Web 界面区块**：

| 区块 | 说明 |
|------|------|
| 顶部概览卡片 | 活跃主题数、消息量/秒、流量/秒、运行时间（沿用现有） |
| 在线 Client 卡片 | 当前在线用户数、生产者数、消费者数、总订阅 topic 数 |
| 流量趋势图 | ECharts 折线图，1H/6H，最多 5 个 topic 叠加（沿用现有） |
| 延迟分位图 | ECharts 柱状/折线图，展示 P50/P95/P99 端到端延迟 |
| 事件流 | 最近 50 条连接/认证/断线事件，自动滚动 |
| Topic 列表 | 卡片网格，点击叠加曲线（沿用现有） |

#### 3.2.9 `logging` — 日志模块

**职责**：统一日志格式、logger 命名、结构化输出。

- 继续使用 loguru 或标准 logging。
- 所有模块使用 `logging.getLogger(__name__)` 风格，便于按模块过滤。
- 关键事件（连接、认证、断开）必须结构化输出，便于监控采集。
- **Client 上线下线必须在 Server 端输出明确日志**，包括 username、endpoint、topics、连接时长等信息。

**必须记录的 Client 生命周期事件**：

| 事件 | 日志级别 | 内容示例 |
|------|---------|---------|
| Client 尝试连接 | INFO | `[AUTH] username=alice, endpoint=192.168.1.10:54321, status=attempt` |
| 认证成功 | INFO | `[AUTH] username=alice, endpoint=192.168.1.10:54321, status=success` |
| 认证失败 | WARNING | `[AUTH] username=bob, endpoint=192.168.1.11:54322, status=failed, reason=invalid_password` |
| Client 上线（控制面注册完成） | INFO | `[CLIENT] username=alice, client_id=uuid-1234, endpoint=192.168.1.10:54321, status=online, topics=[market.stock.*]` |
| 用户重复登录被拒绝 | WARNING | `[CLIENT] username=alice, endpoint=192.168.1.12:54323, status=register_rejected, reason=already_online, existing_client_id=uuid-1234` |
| Client 订阅 topic | INFO | `[CLIENT] username=alice, status=subscribed, topic=market.stock.*` |
| Client 取消订阅 | INFO | `[CLIENT] username=alice, status=unsubscribed, topic=market.stock.*` |
| Client 主动断开 | INFO | `[CLIENT] username=alice, client_id=uuid-1234, endpoint=192.168.1.10:54321, status=offline, reason=client_disconnect, duration=120s` |
| Client 心跳超时 | WARNING | `[CLIENT] username=alice, client_id=uuid-1234, endpoint=192.168.1.10:54321, status=offline, reason=heartbeat_timeout, duration=300s` |
| 连接被 Server 强制关闭 | WARNING | `[CLIENT] username=alice, status=offline, reason=kicked_by_admin` |
| Client 运行期开始重连 | INFO | `[CLIENT] username=alice, status=reconnecting, attempt=1, delay=1.0s` |
| Client 运行期重连成功 | INFO | `[CLIENT] username=alice, client_id=uuid-1234, status=reconnected` |
| Client 运行期重连认证失败 | ERROR | `[CLIENT] username=alice, status=reconnect_failed, reason=authentication_failed` |

**日志格式建议**：

```text
{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {module}:{line} | {message}
```

**结构化输出**：所有 Client 生命周期日志应支持 JSON 输出，便于日志系统（如 ELK/Loki）采集。

#### 3.2.10 `errors` — 异常体系模块

**职责**：定义统一异常体系、错误码与进程退出码。

| 类别 | 异常 | 说明 | 建议退出码 |
|------|------|------|-----------|
| 网络 | `ConnectionError`, `TransportError` | 连接与传输异常 | 2 |
| 认证 | `AuthenticationError` | 鉴权失败 | 3 |
| 启动 | `ClientStartupError` | Client 启动阶段失败 | 4 |
| 协议 | `FrameError`, `SerializationError` | 帧/序列化异常 | 5 |
| 配置 | `ConfigurationError` | 配置错误 | 6 |
| 资源 | `ResourceExhaustedError` | 内存/队列超限 | 7 |

**设计要点**：

- 所有业务异常继承自 `PulseMQError`。
- 错误码便于客户端与监控系统识别。
- `ClientStartupError` 必须封装底层原因（连接失败 / 认证失败 / 注册失败），并附带目标地址与用户名。
- CLI 工具根据异常类型返回对应退出码，便于 systemd / K8s / CI 判断。

#### 3.2.11 `lifecycle` — 生命周期模块

**职责**：

- Server/Client 启动顺序
- 优雅关闭（graceful shutdown）
- SIGTERM / SIGINT 信号处理
- 资源回收（socket、线程、连接）
- **Client 启动失败处理**：连接、认证、注册任一失败时立即清理资源并返回非零退出码

**为什么单独抽出**：

- 当前 publisher.py 和 subscriber.py 中都有各自的启动/关闭逻辑，重复且容易遗漏资源释放。
- 统一 lifecycle 后，Server 和 Client 共用同一套关闭顺序。
- 明确 Client 启动失败必须快速失败，避免半连接状态残留。

---

## 4. 消息流（Client → Server → Client）

### 4.1 发布流程

```text
ProducerClient
    │
    ▼
┌─────────────────┐
│  client.publish │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ serialization   │  对象 → 字节
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  compression    │  压缩
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│      frame      │  加帧头、topic、元数据
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│    transport    │  → 发送到 Server
└────────┬────────┘
         │
         ▼
     Server
         │
         ▼
┌─────────────────┐
│  transport.recv │  接收
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│      frame      │  解码
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  decompression  │  解压
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  deserialization│  字节 → 对象
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│     routing     │  根据 topic 匹配订阅者
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  transport.send │  转发到所有匹配 ConsumerClient
└─────────────────┘
```

> **消息持久化声明**：PulseMQ 不做消息持久化。所有消息在 Server 端均为内存转发，尽力投递；Client 断线期间未收到的消息不会补发。该设计符合金融行情“高时效、可丢弃旧数据”的场景。

### 4.2 订阅流程

```text
ConsumerClient
    │
    ▼
┌────────────────────┐
│ client.subscribe()   │  声明订阅 topic
└──────────┬───────────┘
           │
           ▼
┌────────────────────┐
│     control        │  发送订阅请求到 Server
└──────────┬───────────┘
           │
           ▼
        Server
           │
           ▼
┌────────────────────┐
│      routing       │  更新订阅表
└──────────┬───────────┘
           │
           ▼
┌────────────────────┐
│    transport.send  │  后续该 topic 消息转发给此 Client
└────────────────────┘
```

### 4.3 客户端启动失败处理流程

Client 启动阶段的连接、认证、注册任一环节失败时，必须立即主动退出，不向业务层隐瞒错误。

```text
Client.start() / run_forever()
        │
        ▼
┌───────────────────────┐
│  1. 解析配置与凭据     │  配置错误 → 抛 ConfigurationError → exit(6)
└───────────┬───────────┘
            │
            ▼
┌───────────────────────┐
│  2. 连接 Server 数据面 │  连接失败 → 抛 ClientStartupError(reason=CONNECT_FAILED) → exit(4)
└───────────┬───────────┘
            │
            ▼
┌───────────────────────┐
│  3. PLAIN 认证         │  认证失败 → 抛 AuthenticationError(reason=INVALID_CREDENTIALS) → exit(3)
└───────────┬───────────┘
            │
            ▼
┌───────────────────────┐
│  4. 连接 Server 控制面 │  连接失败 → 抛 ClientStartupError(reason=CONTROL_CONNECT_FAILED) → exit(4)
└───────────┬───────────┘
            │
            ▼
┌───────────────────────┐
│  5. 控制面注册         │  注册失败 → 抛 ClientStartupError(reason=REGISTER_REJECTED / ALREADY_ONLINE) → exit(4)
└───────────┬───────────┘
            │
            ▼
┌───────────────────────┐
│  6. 进入正常运行循环   │  运行期断线自动重连 + 重新认证 + 重新订阅
└───────────────────────┘
```

**失败场景与退出码**：

| 失败场景 | 触发条件 | 抛出异常 | 退出码 | Client 日志示例 |
|---------|---------|---------|--------|----------------|
| Server 不在线 | TCP 连接超时/拒绝 | `ClientStartupError` | 4 | `[CLIENT] 连接 Server 失败: endpoint=tcp://127.0.0.1:5555, reason=connection_refused` |
| 网络不可达 | 路由失败、DNS 失败 | `ClientStartupError` | 4 | `[CLIENT] 连接 Server 失败: endpoint=tcp://127.0.0.1:5555, reason=network_unreachable` |
| 用户名不存在 | ZAP 认证返回 denied | `AuthenticationError` | 3 | `[CLIENT] 认证失败: username=alice, reason=user_not_found` |
| 密码错误 | ZAP 认证返回 denied | `AuthenticationError` | 3 | `[CLIENT] 认证失败: username=alice, reason=invalid_password` |
| 用户被禁用 | security 白名单 enabled=false | `AuthenticationError` | 3 | `[CLIENT] 认证失败: username=alice, reason=user_disabled` |
| 用户已在线 | Server 在线用户表已存在该 username | `ClientStartupError` | 4 | `[CLIENT] 控制面注册失败: username=alice, reason=already_online` |
| 控制面注册被拒绝 | Server 返回注册失败 | `ClientStartupError` | 4 | `[CLIENT] 控制面注册失败: reason=role_not_allowed` |

**运行期断线自动恢复**：

```text
检测到断线（心跳超时 / socket disconnected）
        │
        ▼
  进入重连循环（指数退避，1s → 30s）
        │
        ▼
  重连成功
        │
        ▼
  重新进行 PLAIN 认证
        │
        ▼
  重新控制面注册（携带原 client_id、topics）
        │
        ▼
  恢复之前的订阅
        │
        ▼
  继续接收消息
```

- 重连成功后认证失败：按启动期认证失败处理，直接退出。
- 重连成功后注册失败（如用户名仍在线）：直接退出，由外部进程管理器处理。
- 重连成功后的恢复对业务层透明，无需业务代码重新调用 `subscribe()`。

---

## 5. 安全模型

### 5.1 认证层

| 模式 | 传输加密 | 认证方式 | 本项目状态 |
|------|---------|---------|----------|
| `plain` | 否（明文） | 用户名 + 密码 | **唯一支持且强制开启** |
| `null` | 否 | 无 | 不支持 |
| `curve` | 是 | 非对称密钥 | 不支持 |

**认证流程**：

```text
Client 连接 Server
        │
        ▼
   transport 层发起认证
        │
        ▼
   auth 模块校验凭据
        │
        ▼
  ┌─────────────┐
  │  通过       │ → 进入正常消息收发
  │  拒绝       │ → 断开连接，记录日志与监控
  └─────────────┘
```

### 5.2 凭据管理

`security` 模块负责 PLAIN 认证所需的用户凭据：

- 维护用户名/密码白名单（`pulsemq_users.toml` 或数据库）
- 密码必须哈希存储（bcrypt / argon2），禁止明文保存
- 支持白名单热更新（reload）
- 支持用户状态：启用 / 禁用 / 过期
- **默认凭据自动生成**：若未提供凭据文件，Server 启动时自动生成 `admin` 用户并输出随机密码到日志；该特性可进入生产环境，但建议通过 `allow_auto_generated_credentials` 控制

**默认凭据生成规则**：

| 优先级 | 来源 | 行为 |
|--------|------|------|
| 1 | `pulsemq_users.toml` 存在 | 加载该文件，不生成默认用户 |
| 2 | 环境变量 `PULSEMQ_ADMIN_PASSWORD` | 生成 `admin` 用户，密码为环境变量值 |
| 3 | 无配置 | 生成 `admin` 用户，密码为 16 位随机字符串 |

**凭据文件示例**：

```toml
[users.alice]
hashed_password = "$2b$12$..."
roles = ["subscriber", "publisher"]
enabled = true

[users.bob]
hashed_password = "$2b$12$..."
roles = ["subscriber"]
enabled = true
```

**自动生成时的 Server 日志**：

```text
[2026-06-26 16:20:01] [SECURITY] 未检测到 pulsemq_users.toml，已生成默认用户
[2026-06-26 16:20:01] [SECURITY] username=admin, password=Rx9#mK2pL$vQ
[2026-06-26 16:20:01] [SECURITY] 提示：默认凭据仅用于首次启动，请使用 `pulsemq.users` CLI 创建正式用户
```

> 由于 PLAIN 在传输过程中为明文，该模式只适用于可信内网。若未来需要跨公网或不可信网络，必须引入 TLS 隧道或切换为 CURVE。

### 5.3 Admin 接口认证

- 必须强制启用认证，不能默认开放。
- 采用启动时生成随机 token 的方案，默认写入 `./pulsemq_admin.token` 并输出到日志。
- 同时支持通过环境变量 `PULSEMQ_ADMIN_TOKEN` 或配置文件预置 token。
- 管理接口与数据接口分离，避免安全耦合。
- UI 页面通过 `?token=xxx` 或 Header 携带 token。

### 5.4 Topic 级授权（未来扩展）

`auth` 解决“谁能连”，`routing` 未来可扩展为“谁能订阅哪些 topic”：

```text
user: "trader_a"
allowed_topics: ["market.stock.*", "market.futures.010*"]
```

Server 在收到订阅请求时，由 `routing` 结合 `auth` 的授权信息决定是否允许。

---

## 6. 监控模型

### 6.1 监控事件来源

| 来源 | 事件 | 说明 |
|------|------|------|
| transport 连接 | connected, disconnected, reconnect | 连接生命周期 |
| transport 认证 | handshake_succeeded, handshake_failed_auth | 认证结果 |
| 控制面 | register, heartbeat, unregister | 客户端注册与心跳 |
| 数据面 | send, recv, topic_distribution | 流量指标 |
| 系统 | cpu, memory, fd_count | 资源使用 |

### 6.2 监控事件统一入口

```text
所有 socket 事件 ──→ transport monitor ──→ monitoring 事件总线
控制面事件      ──→ control           ──→ monitoring 事件总线
业务埋点        ──→ 各模块            ──→ monitoring 事件总线
                          │
                          ▼
              ┌─────────────────────┐
              │   monitoring 处理器   │
              │  - 日志输出           │
              │  - SQLite 存储        │
              │  - HTTP/SSE 推送     │
              │  - Prometheus 导出   │
              └─────────────────────┘
```

### 6.3 建议指标

| 指标 | 类型 | 用途 |
|------|------|------|
| `pulsemq_connections_total` | counter | 连接数 |
| `pulsemq_messages_sent_total` | counter | 发送消息数 |
| `pulsemq_messages_received_total` | counter | 接收消息数 |
| `pulsemq_message_latency_seconds` | histogram | 端到端延迟 |
| `pulsemq_auth_failures_total` | counter | 认证失败数 |
| `pulsemq_topic_subscribers` | gauge | 每个 topic 订阅数 |
| `pulsemq_queue_bytes` | gauge | 发送队列积压 |

### 6.4 监控 Web 界面设计

监控 Web 界面沿用现有 PulseMQ 深色 ECharts 风格，在保留核心功能的基础上有针对性地扩展，**不引入冗余功能**。

#### 6.4.1 界面布局

```text
┌────────────────────────────────────────────────────────────┐
│  PulseMQ 监控面板                [在线]  [版本号]          │
├────────────────────────────────────────────────────────────┤
│  活跃主题  │  消息量/秒  │  流量/秒  │  运行时间             │
│  在线用户  │  在线生产者 │ 在线消费者│ 总订阅 topic          │
├────────────────────────────────────────────────────────────┤
│  流量趋势（记录数/秒）        [1H] [6H]                    │
│  ┌────────────────────────────────────────────────────┐    │
│  │                                                    │    │
│  │              ECharts 折线图                        │    │
│  │                                                    │    │
│  └────────────────────────────────────────────────────┘    │
├────────────────────────────────────────────────────────────┤
│  端到端延迟（P50 / P95 / P99）                             │
│  ┌────────────────────────────────────────────────────┐    │
│  │              ECharts 柱状/折线图                    │    │
│  └────────────────────────────────────────────────────┘    │
├────────────────────────────────────────────────────────────┤
│  最近事件流                          [自动滚动]            │
│  08:12:01  [AUTH]    bob 认证失败: invalid_password         │
│  08:12:00  [CLIENT]  alice 上线, topics=[...]               │
│  08:11:58  [CLIENT]  carol 心跳超时 offline                  │
├────────────────────────────────────────────────────────────┤
│  Topic 列表（卡片网格，点击叠加到流量趋势图）                │
└────────────────────────────────────────────────────────────┘
```

#### 6.4.2 沿用现有功能（不变）

| 功能 | 说明 |
|------|------|
| 深色玻璃态视觉风格 | 使用 CSS 渐变 + backdrop-filter，保持现有审美 |
| 4 个顶部指标卡片 | 活跃主题、消息量/秒、流量/秒、运行时间 |
| ECharts 流量趋势图 | 分钟级，1H/6H 切换，最多 5 个 topic 叠加 |
| Topic 卡片网格 | 展示速率与缓存大小，点击选中/取消 |
| SSE 实时推送 | 1 秒一帧更新顶部卡片和趋势图当前分钟 |

#### 6.4.3 新增/完善功能

| 功能 | 必要性 | 说明 |
|------|--------|------|
| 在线用户概览卡片 | 高 | 展示在线用户数、生产者数、消费者数、总订阅数 |
| 端到端延迟图 | 高 | 在 Client 发布时打时间戳，Server 收到后计算延迟并聚合 P50/P95/P99 |
| 最近事件流 | 高 | 展示最近 50 条连接/认证/断线事件，便于快速排查 |
| 在线 Client 详情弹窗 | 中 | 点击在线用户卡片，弹出当前在线 Client 列表（username、client_id、角色、订阅 topic、连接时长） |

> 新增功能均不引入复杂交互（如拖拽配置、多面板自定义），保持界面精简。

#### 6.4.4 数据接口

```text
GET /api/v1/stats/realtime
{
  "topics": { "market.stock.600000": { "record_rate_1min": 1204.3, ... } },
  "online_users": 3,
  "online_producers": 1,
  "online_consumers": 2,
  "total_subscriptions": 5,
  "latency_p50_ms": 0.12,
  "latency_p95_ms": 0.45,
  "latency_p99_ms": 0.82,
  "server_time": 170...
}

GET /api/v1/clients
{
  "clients": [
    {
      "client_id": "uuid-1234",
      "username": "alice",
      "role": "consumer",
      "endpoint": "192.168.1.10:54321",
      "topics": ["market.stock.*"],
      "connected_at": "2026-06-26T08:00:01Z",
      "duration_seconds": 725
    }
  ]
}

GET /api/v1/events?limit=50
{
  "events": [
    {"ts": "2026-06-26T08:12:01Z", "level": "WARNING", "type": "AUTH", "message": "bob 认证失败: invalid_password"},
    {"ts": "2026-06-26T08:12:00Z", "level": "INFO", "type": "CLIENT", "message": "alice 上线"}
  ]
}
```

### 6.5 监控性能设计

金融行情系统对消息路径的延迟和 CPU 占用极度敏感，监控实现必须遵循**数据路径零阻塞**原则。

#### 6.5.1 数据路径隔离

| 设计 | 说明 |
|------|------|
| 统计采集无锁 | `TrafficStats.record()` 继续利用 Python GIL 做单写者多读者，不引入锁 |
| 事件队列有界 | socket monitor 事件、控制面事件先进入 `collections.deque(maxlen=N)`，溢出自动丢弃旧事件 |
| 异步批量落盘 | 分钟归档数据通过 `asyncio.Queue` 交给独立任务，批量 `executemany` 写入 SQLite |
| Admin HTTP 独立运行 | AdminServer 运行在独立线程的事件循环，HTTP 请求不抢占 ZeroMQ 数据线程 CPU |
| SSE 反压保护 | SSE 客户端队列 `maxsize=64`，慢消费者自动丢帧，避免反压 |

#### 6.5.2 监控采集点最小化

```text
ProducerClient.publish(data)
        │
        ▼
┌────────────────────┐
│  1. frame + serialize + compress   │  业务路径
│  2. 在 frame 中写入 client_send_ts │  仅 1 次 time.time_ns()
└─────────┬──────────┘
          │
          ▼
     transport.send()
          │
          ▼
Server.transport.recv()
          │
          ▼
┌────────────────────┐
│  3. TrafficStats.record(topic, count, bytes) │  内存累加，无锁
│  4. LatencyStats.record(now_ns - client_send_ts) │  直方图更新，无锁
└─────────┬──────────┘
          │
          ▼
     routing.dispatch()
```

#### 6.5.3 避免的性能陷阱

| 禁止项 | 原因 | 替代方案 |
|--------|------|---------|
| 在消息接收线程中直接写 SQLite | 磁盘 I/O 会阻塞数据路径 | 异步批量写入 |
| 在消息接收线程中格式化日志字符串 | 字符串拼接和 I/O 开销 | 延迟格式化，使用结构化日志 |
| 无限制缓存所有历史事件 | 内存无限增长 | 事件流使用固定长度环形缓冲区 |
| Admin 请求触发全量扫描 | 大 topic 量时卡顿 | API 返回预聚合快照 |
| 每条消息都采集延迟 | 高频场景下 time_ns() 也会累积 | 采样采集（如每 100 条采集 1 条） |

#### 6.5.4 推荐配置

```toml
[monitoring]
ui_enabled = true
storage = "sqlite://./pulsemq_stats.sqlite"
retention_days = 7
sse_interval = 1.0              # SSE 推送间隔（秒）
latency_sample_rate = 0.01      # 延迟采样率：1%（可配置为 1.0 全量）
event_ring_size = 200           # 内存事件环大小
stats_archive_batch_size = 50   # SQLite 批量写入条数
admin_thread = true             # Admin HTTP 运行在独立线程
```

---

## 7. 对外 API 设计

### 7.1 Server API（最简启动）

```text
from pulsemq import Server

# 全部使用默认配置，自动加载 ./pulsemq_users.toml
server = Server()
server.start()
server.wait_for_shutdown()
```

### 7.2 Server API（自定义端点与凭据）

```text
from pulsemq import Server, PlainAuth

server = Server(
    data_endpoint="tcp://0.0.0.0:5555",
    control_endpoint="tcp://0.0.0.0:5556",
    admin_endpoint="0.0.0.0:9090",
    auth=PlainAuth.from_file("./users.toml"),
)
server.start()
server.wait_for_shutdown()
```

### 7.3 Client API（最简启动）

```text
from pulsemq import Client, PlainCredentials

client = Client(
    credentials=PlainCredentials(username="alice", password="secret"),
)
client.subscribe("market.stock.*", callback=on_stock_message)
client.publish("market.stock.600000", data)
client.run_forever()
```

### 7.4 Client API（自定义端点）

```text
from pulsemq import Client, PlainCredentials

client = Client(
    server_endpoint="tcp://broker.example.com:5555",
    control_endpoint="tcp://broker.example.com:5556",
    credentials=PlainCredentials(username="alice", password="secret"),
)

client.subscribe("market.stock.*", callback=on_stock_message)
client.publish("market.stock.600000", data)
client.run_forever()
```

### 7.5 ProducerClient / ConsumerClient

```text
from pulsemq import ProducerClient, ConsumerClient, PlainCredentials

creds = PlainCredentials(username="alice", password="secret")

producer = ProducerClient(credentials=creds)
producer.publish("market.futures.IC2406", data)

consumer = ConsumerClient(credentials=creds)
consumer.subscribe("market.futures.*", callback=on_futures)
consumer.run_forever()
```

---

## 8. 从现有 PulseMQ 的迁移路线

### 阶段 1：模块拆分（保持现有行为不变）

1. 将 `protocol/frames.py`、`serialization.py`、`compression.py` 独立为模块。
2. 抽象 `auth` 模块，将现有 `PLAIN` 认证封装为 `PlainAuth`。
3. 新增 `errors`、`logging`、`config` 统一规范。

### 阶段 2：引入 transport 抽象

1. 新建 `transport` 模块，封装所有 ZeroMQ socket 操作。
2. Publisher/Subscriber 改为通过 `transport` 收发，但外部名称可暂时保留。

### 阶段 3：统一为 Client/Server 模型

1. 新增 `client` 模块，统一 producer 与 subscriber 的客户端逻辑。
2. 新增 `server` 模块，将现有 publisher 拆分为“接收 + 分发 + 控制”三个角色。
3. 引入 `control` 模块，替代自定义 PUSH/PULL 心跳通道，维护 Server 端在线用户表。
4. 实现启动期硬失败与运行期自动重连 + 重新认证。

### 阶段 4：补齐安全与监控

1. 新增 `security` 模块，负责 PLAIN 用户名/密码白名单与密码哈希存储，支持默认凭据自动生成。
2. `auth` 仅实现 `PlainAuth`，并强制启用。
3. `monitoring` 覆盖 Server 的接收端、分发端、控制端，以及 Client 两端。
4. Admin HTTP 接口增加随机 token 认证。

### 阶段 5：生产级增强

1. 在 `routing` 中实现按用户/角色的 topic ACL。
2. 多 Server 实例与 Client 自动重连到可用 Server。
3. Prometheus 指标导出替代或补充 SQLite 实时观测。
4. 若未来需要跨不可信网络，再评估引入 TLS 隧道或 CURVE。

---

## 9. 关键设计决策

| 决策 | 说明 |
|------|------|
| **保留 ZeroMQ 作为传输层** | 避免自研 TCP 的复杂度，利用其重连、队列、认证、监控能力 |
| **对外隐藏 ZeroMQ 术语** | 只暴露 Client/Server，降低使用门槛，便于未来替换传输层 |
| **数据面与控制面分离** | 控制信令不影响业务消息，管理命令可请求-响应 |
| **零配置优先** | 所有参数均有默认值，`Server()` 即可启动完整服务 |
| **默认凭据自动生成** | 未提供凭据文件时自动生成 `admin` 用户并输出到日志 |
| **内置监控 UI** | 沿用现有 AdminServer + ECharts 面板，默认开启，开箱即用 |
| **监控界面功能明确化** | 保留 4 张指标卡片 + 流量趋势图 + Topic 列表；新增在线用户卡片、延迟分位图、最近事件流 |
| **数据路径零阻塞** | 统计采集无锁、事件队列有界、SQLite 异步批量写入、Admin HTTP 独立运行 |
| **延迟可采样采集** | 默认 1% 采样，避免高频消息下 time_ns() 开销累积；可配置为全量 |
| **强制 PLAIN 认证** | 项目约束：所有连接必须通过用户名/密码认证，不可关闭 |
| **鉴权与凭据管理分离** | `auth` 管认证决策，`security` 管密码哈希与白名单生命周期 |
| **心跳走 control 面** | 替代自定义 PUSH/PULL 通道，端口语义更清晰 |
| **启动期硬失败** | Client 连接失败、认证失败、注册失败时立即退出，不静默重试 |
| **运行期自动重连 + 重新认证** | 断线后指数退避重连，重新认证并恢复订阅，满足行情高时效性 |
| **心跳 1s / 超时 6s** | 高频率心跳确保 Server 快速感知 Client 掉线 |
| **Client 生命周期全日志** | 连接、认证、上线、订阅、下线必须在 Server 端输出结构化日志 |
| **单用户单客户端在线** | Server 维护在线用户表，同一 username 不可多开 Client |
| **单用户可订阅多 topic** | 同一用户可声明多个订阅主题，由 routing 维护 |
| **多 ProducerClient 并发发布** | Server 接收端聚合多个 ProducerClient 的消息 |
| **不持久化消息** | 内存转发，尽力投递，简化实现 |
| **Admin 必须认证** | 当前项目 `:9090` 完全开放，强制启用随机 token 认证 |
| **路由独立模块** | 为后续 topic ACL、限流、审计预留扩展空间 |

---

## 10. 需要特别注意的边界

1. **Client 启动失败必须主动退出**：连接失败、认证失败、控制面注册失败均不得进入无限重试或静默等待，必须立即返回非零退出码。
2. **运行期断线必须自动重连 + 重新认证**：断线后指数退避重连，成功后重新认证、注册、恢复订阅；若重新认证失败则直接退出。
3. **单用户单客户端在线**：同一 `username` 同时只能有一个 Client 在线，Server 通过在线用户表维护；同一用户可订阅多个 topic。
4. **多 ProducerClient 同时发布**：Server 接收端允许多个 ProducerClient 同时发布消息，按 topic 汇总后分发。
5. **不持久化消息**：所有消息均为内存转发，尽力投递，不保证消息落盘或断线期间消息不丢失。
6. **不兼容旧版 PulseMQ 客户端协议**：本次重构作为大版本升级，旧客户端需同步升级。
7. **PLAIN 明文传输**：密码在 TCP 上为明文，必须部署在可信内网；跨不可信网络需额外 TLS 隧道。
8. **默认凭据可进入生产**：`admin` 自动生成凭据可进入生产，但建议通过配置关闭自动生成并预置凭据。
9. **认证粒度**：当前为连接级认证，无法对单条消息鉴权。若需消息级鉴权，需在应用层实现。
10. **背压与队列**：需合理配置 `transport` 层的发送/接收高水位，防止慢消费者拖垮 Server。
11. **监控性能**：
    - socket monitor 事件量可能很大，需使用有界队列 + 异步批处理。
    - 延迟采集建议采样（默认 1%），全量采集仅在调试或低吞吐场景开启。
    - Admin HTTP 必须运行在独立线程/事件循环，不得与 ZeroMQ 数据线程共享 CPU。
    - SSE 客户端慢消费时必须丢帧而非阻塞 Server。
12. **日志隐私**：Client 下线日志可包含 topics 信息，若 topic 含敏感数据需考虑脱敏。

---

## 11. 总结

PulseMQ 的下一步演进方向是：**继续以 ZeroMQ 为底层传输，对外统一为 Client/Server 模型，强制使用 PLAIN 用户名/密码认证（明文传输，可信内网），默认凭据自动生成并允许进入生产，默认内置 Web 监控界面（沿用现有 ECharts 深色风格并新增在线用户、延迟分位、事件流），监控实现必须保证数据路径零阻塞（无锁统计、异步落盘、独立 Admin 线程、采样延迟采集），Client 启动阶段连接失败/认证失败/注册失败必须立即主动退出，运行期断线必须自动重连并重新认证，Server 维护在线用户表确保同一 username 单客户端在线、支持单用户订阅多 topic、支持多 ProducerClient 并发发布、消息不持久化，并通过模块化解耦协议、安全、路由、控制与监控。**

本次重构中不出现任何“broker”概念，Server 作为唯一的中继与分发角色存在；不兼容旧版 PulseMQ 客户端协议。用户指定的 `frame`、`serialization`、`compression`、`auth`、`monitoring` 五个模块是基础，其中 `auth` 固定为 `PlainAuth` 并强制启用；`monitoring` 默认启用内嵌 AdminServer + ECharts 面板并覆盖 Client 生命周期事件；`client` 与 `lifecycle` 必须实现启动期硬失败 + 运行期自动重连重新认证；`control` 必须维护 Server 端在线用户表。同时必须补充 `transport`、`security`（PLAIN 凭据管理，含默认用户自动生成）、`routing`、`control`、`client`、`server`、`config`、`admin/api`、`logging`（Client 上线下线日志规范）、`errors`（含退出码）、`lifecycle` 等模块，才能构成一个完整、可维护、可扩展的第二代架构。
