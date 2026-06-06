# PulseMQ 实现策略

## 概述

基于 `design/` 目录下的 6 份设计文档（架构总览、协议规范、引擎设计、安全模型、客户端 SDK、运维与测试），将 PulseMQ 高性能消息中间件按依赖顺序分为 5 个阶段实现。每个阶段结束时产出可运行、可测试的最小系统。

## 技术栈

- **语言**: Python 3.10+
- **异步框架**: asyncio（uvloop 可选，Linux/macOS 下自动启用）
- **消息传输**: ZMQ (pyzmq)，ROUTER-DEALER + XPUB-SUB
- **序列化**: msgpack（默认）、raw、pyarrow（可选）、protobuf（用户注册）
- **压缩**: none（默认）、snappy、lz4、zstd
- **存储**: SQLite（V1），通过 Repository 抽象未来可切换 MySQL
- **包管理**: uv
- **测试**: pytest + pytest-asyncio

## 项目结构

```
pulse-mq/
├── src/
│   └── pulsemq/
│       ├── __init__.py
│       ├── config.py              # 配置加载（文件 + 环境变量 + CLI）
│       ├── event_loop.py          # uvloop / asyncio 事件循环
│       ├── models.py              # 领域模型（User, Topic, Message, Permission）
│       ├── protocol/
│       │   ├── __init__.py
│       │   ├── frames.py          # 6 帧编解码（FrameCodec）
│       │   ├── msg_type.py        # msg_type 枚举
│       │   └── flags.py           # flags bitfield 编解码
│       ├── serialization/
│       │   ├── __init__.py
│       │   ├── registry.py        # Serializer/Compressor 注册表
│       │   ├── msgpack_ser.py     # msgpack 序列化器
│       │   ├── raw_ser.py         # bytes 序列化器
│       │   └── compressors.py     # none/snappy/lz4/zstd
│       ├── transport/
│       │   ├── __init__.py
│       │   └── zmq_transport.py   # ZMQ ROUTER + XPUB 适配器
│       ├── storage/
│       │   ├── __init__.py
│       │   ├── interfaces.py      # Repository ABC
│       │   ├── sqlite_user.py     # UserRepository SQLite 实现
│       │   ├── sqlite_perm.py     # PermissionGroupRepo SQLite 实现
│       │   └── sqlite_metrics.py  # MetricsRepository SQLite 实现
│       ├── auth/
│       │   ├── __init__.py
│       │   ├── zap_handler.py     # ZMQ ZAP 认证处理器
│       │   ├── memory_store.py    # 内存鉴权缓存（identity → user）
│       │   └── permission.py      # 权限展开 + 缓存 + 通配符匹配
│       ├── engine/
│       │   ├── __init__.py
│       │   ├── engine.py          # 消息主循环（批处理 + 并发）
│       │   ├── router.py          # TopicRegistry + SubscriptionManager + ConnectionManager
│       │   ├── message_buffer.py  # 环形缓冲区
│       │   ├── pipeline.py        # 拦截器链
│       │   ├── interceptors.py    # Auth/Permission/Monitor 拦截器
│       │   ├── handlers.py        # 消息类型分发
│       │   ├── pool.py            # 对象池（Message + Context）
│       │   └── overload.py        # 双缓冲 + 过载保护
│       ├── monitoring/
│       │   ├── __init__.py
│       │   ├── realtime.py        # 实时指标（EWMA + SlidingWindow）
│       │   ├── minute.py          # 分钟聚合槽
│       │   └── api.py             # HTTP 监控 API
│       ├── client/
│       │   ├── __init__.py
│       │   └── async_client.py    # PulseClient 异步客户端
│       └── server.py              # 启动器（组装各层）
├── tests/
│   ├── unit/
│   ├── integration/
│   ├── chaos/
│   └── benchmarks/
├── design/                        # 设计文档（已存在）
├── pyproject.toml
└── pulse_mq.toml                  # 默认配置文件示例
```

## 分阶段实现计划

### Phase 1: 基础骨架

**目标**: 能跑通 PUB → SUB 端到端的最小 服务端。

**包含模块**:
1. 项目初始化（pyproject.toml、依赖声明）
2. `config.py` — 配置加载（TOML + 环境变量 + 默认值）
3. `event_loop.py` — uvloop/asyncio 选择
4. `models.py` — 核心数据结构（User, Topic, Message, AuthUser 等）
5. `protocol/` — 帧编解码（6 帧固定格式、meta flags、msg_type 枚举）
6. `serialization/` — 注册表 + msgpack + raw + 4 种压缩
7. `transport/zmq_transport.py` — ZMQ ROUTER + XPUB 适配器
8. `engine/router.py` — Router（仅精确 topic，不含通配符展开）
9. `engine/handlers.py` — PUB/SUB/UNSUB/PING/PONG 消息处理
10. `server.py` — 组装各层并启动

**不包含**: 认证、权限、通配符、批处理、背压、监控、客户端 SDK。

**验证标准**: 两个测试客户端通过 ZMQ DEALER 连接 服务端，一个 PUB 消息，另一个 SUB 后收到 BROADCAST。

### Phase 2: 认证与权限

**目标**: 带认证和权限校验的 服务端。

**包含模块**:
1. `storage/interfaces.py` — Repository ABC
2. `storage/sqlite_user.py` — User CRUD
3. `storage/sqlite_perm.py` — PermissionGroup CRUD
4. 数据库初始化 + 默认 admin 用户
5. `auth/zap_handler.py` — ZMQ PLAIN/ZAP 认证
6. `auth/memory_store.py` — 内存 identity → user 映射
7. `auth/permission.py` — 权限展开 + 缓存 + 通配符匹配（`*` / `>`）
8. `engine/pipeline.py` — 拦截器链框架
9. `engine/interceptors.py` — AuthInterceptor + PermissionInterceptor
10. Router 增加通配符订阅展开（SubscriptionManager 扩展）

**验证标准**: admin 全权限通过，普通用户按权限组正确授权/拒绝，断线后资源清理。

### Phase 3: 引擎优化

**目标**: 高性能消息主循环，满足吞吐目标。

**包含模块**:
1. `engine/engine.py` — 批处理 + 自适应批大小 + 同 topic 有序分组
2. 信号量并发控制（max_concurrency=100）
3. 背压传导（pending_tasks 阈值 → 暂停 recv）
4. `engine/pool.py` — MessagePool + MessageContextPool
5. `engine/overload.py` — 双缓冲（data_buffer + ctrl_buffer）+ 优先级丢弃
6. `engine/interceptors.py` — MonitorInterceptor
7. ZMQ socket 参数调优（HWM、heartbeat 等）

**验证标准**: 单 Producer 吞吐 > 50,000 msg/s（本地），同 topic 消息严格有序。

### Phase 4: 高级功能

**目标**: 功能完整的 服务端。

**包含模块**:
1. `engine/message_buffer.py` — 环形缓冲区（1000 条/topic）+ 序列号
2. HISTORY_REPLAY 请求处理
3. `monitoring/realtime.py` — EWMA + SlidingWindow 实时指标
4. `monitoring/minute.py` — 分钟聚合槽 + SQLite 写入
5. `monitoring/api.py` — HTTP 监控 API（GET /api/v1/metrics/realtime）
6. `storage/sqlite_metrics.py` — MetricsRepository
7. QUERY handler（system_status）

**验证标准**: 新订阅者通过 HISTORY_REPLAY 获取历史数据，监控 API 返回实时指标。

### Phase 5: 客户端与 CLI

**目标**: 完整可用的系统。

**包含模块**:
1. `client/async_client.py` — PulseClient（connect/auth/pub/sub/unsub/query/ping）
2. Context manager 支持（`async with PulseClient(...) as client`）
3. publish_batch + 带重试的 publish
4. 自动重连（指数退避）
5. PING/PONG 心跳（延迟采样）
6. CLI 入口（`pulse-mq` 命令，零配置启动）
7. 错误类型体系（PulseError/ConnectionError/AuthError/PermissionError/ServerError）

**验证标准**: `pip install` 后可直接 `pulse-mq` 启动服务，Python 代码可直接 `from pulsemq import PulseClient` 使用。

## 设计约束

- **V1 不做**: CURVE 加密、gRPC 管理 API、WebSocket 桥接、集群、持久化消息
- **Windows 兼容**: uvloop 自动回退，功能无损失
- **单线程模型**: Router/Engine 在 asyncio 单线程中运行，无锁
- **元数据持久化**: User/Permission 存 SQLite；消息/订阅/连接纯内存
