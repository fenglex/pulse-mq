# PulseMQ 模块设计文档

> 版本：**7.1.0** ｜ 以源码为准，最后核对：2026-07-24
>
> 本文档描述 PulseMQ 当前源码的模块职责、核心类与接口、关键数据结构与模块间依赖。所有引用形如 `path:line`，可在 IDE/CLI 中直接跳转。面向项目维护者。

---

## 目录

1. [总体架构](#1-总体架构)
2. [模块依赖关系](#2-模块依赖关系)
3. [异常与退出码体系 `errors.py`](#3-异常与退出码体系-errorspy)
4. [启动与生命周期 `lifecycle.py`](#4-启动与生命周期-lifecyclepy)
5. [服务端核心 `server.py`](#5-服务端核心-serverpy)
6. [客户端 `client.py`](#6-客户端-clientpy)
7. [传输层 `transport/router.py`](#7-传输层-transportrouterpy)
8. [协议模块 `protocol/`](#8-协议模块-protocol)
9. [路由模块 `routing.py`](#9-路由模块-routingpy)
10. [控制面 `control.py`](#10-控制面-controlpy)
11. [认证模块 `auth.py` 与 `security.py`](#11-认证模块-authpy-与-securitypy)
12. [统计模块 `stats/`](#12-统计模块-stats)
13. [管理后台 `admin/`](#13-管理后台-admin)
14. [Producer 管线 `producers/`](#14-producer-管线-producers)
15. [Topic 缓存 `cache/`（预留，未接入）](#15-topic-缓存-cache预留未接入)
16. [配置 `config.py`](#16-配置-configpy)
17. [日志 `logging_setup.py`](#17-日志-logging_setuppy)
18. [包入口与 CLI](#18-包入口与-cli)
19. [附录：端口 / 遗留项 / 与旧文档差异](#19-附录端口--遗留项--与旧文档差异)

---

## 1. 总体架构

PulseMQ 是 **Client/Server 消息中间件**，基于 ZeroMQ ROUTER/DEALER + ZAP PLAIN(bcrypt) 认证。不含中间 broker，服务端为无状态消息路由器。

### 核心设计原则

- **数据面/控制面分离**：数据面用同步 ZMQ ROUTER + 独立线程（低延迟），控制面用异步 ZMQ ROUTER + asyncio 事件循环。两者独立 ctx、独立 ZAP，互不阻塞（`server.py:139`、`transport/router.py:102`）。
- **数据面同步线程**：`SyncDataThread` 在独立线程中用 `zmq.Poller` 轮询 ROUTER recv + PULL（内置 producer 发送请求），`_on_data_message` 回调全程同步执行（decode_header -> 统计 -> 路由 -> send），无 asyncio 调度延迟（`transport/router.py:153`、`server.py:334`）。
- **服务端不解压/不反序列化**：`decode_header` 仅提取 topic/record_count/timestamp_ns，payload 盲转发（`server.py:334`）。
- **路由键 = ROUTER 的 bytes identity**，不是 client_id 字符串。`send_sync_direct` 调 `send_multipart([identity_bytes, frame])`（`transport/router.py:249`）。
- **Server 持有 `client_id -> bytes ident` 映射**，用于心跳超时清理 routing：registry 只存 client_id 字符串，清理 routing 需反查 bytes ident（`server.py:129`、`server.py:465`）。
- **单用户单在线**：`OnlineRegistry` 以 username 为唯一键，重复 REGISTER 返回 ALREADY_ONLINE（`control.py:48`）。
- **当前不启用 topic 缓存**：Server 传 `topic_buffers=None`（`server.py:152`）。`cache/` 模块已实现但**未接入主流程**，见 [§15](#15-topic-缓存-cache预留未接入)。

### 端口

| 端口 | 协议 | 用途 |
|------|------|------|
| 5555 | ROUTER | 数据面（消息收发） |
| 5556 | ROUTER | 控制面（REGISTER/HEARTBEAT/SUBSCRIBE/DISCONNECT） |
| 9090 | HTTP | 监控 Web UI + REST + SSE |

### 数据流

```
DEALER -> [同步 ROUTER 线程] -> decode_header -> TrafficStats.record -> LatencyStats.sample
  -> SubscriptionTable.match -> send_sync_direct -> [ROUTER] -> DEALER
```

核心路由调用链（`server.py:334`，在数据面线程中同步执行）：

```python
hdr = frames.decode_header(frame_bytes)          # 只读头部
self._stats.record(hdr.topic, hdr.record_count, len(hdr.raw_payload))
for target in self._routing.match(hdr.topic):    # 前缀索引匹配 -> set[bytes identity]
    self._transport.send_sync_direct(target, frame_bytes)  # 同步 send，无 await
```

内置 producer 通过 `send_sync_data`（PUSH -> PULL）投递到数据面线程（`server.py:328`）。

---

## 2. 模块依赖关系

```
                    ┌─────────────┐
                    │  config.py  │  （TOML + 环境变量，纯数据）
                    └──────┬──────┘
                           │
        ┌──────────────────┼─────────────────────┐
        ▼                  ▼                     ▼
  ┌──────────┐      ┌────────────┐        ┌───────────┐
  │ protocol │◄─────│  server    │───────►│ transport │
  │(帧/编解码) │     │  (核心)     │  ZAP   │(ROUTER套接字)│
  └────┬─────┘      └────┬───────┘        └───────────┘
       │                  │
       │            ┌─────┼──────┬──────────┬──────────┐
       │            ▼     ▼      ▼          ▼          ▼
       │      ┌──────┐┌──────┐┌──────┐┌──────────┐┌──────────┐
       │      │ client│routing│control│ security │  stats   │
       │      │(DEALER)│      │      │(bcrypt)  │  (监控)   │
       │      └──────┘└──────┘└──────┘└──────────┘└────┬─────┘
       │                                                 │
       │                                          ┌──────┴──────┐
       │                                          │  admin/     │
       │                                          │ (HTTP/SSE)  │
       │                                          └──────┬──────┘
       │                                                 │
       │                                          ┌──────┴──────┐
       └──────────────────────────────────────────┤   cache/    │（预留，未接入）
                                                  └─────────────┘
```

**依赖方向（严格单向）**：

- `protocol/`：最底层，仅依赖 `errors`，不依赖任何业务模块。
- `transport/`：仅依赖 `zmq` + `logging_setup`，不依赖 `protocol/`。
- `errors.py`：独立异常体系，被 `protocol`/`config`/`client`/`cli` 等引用。
- `lifecycle.py`：依赖 `logging_setup`，被 `cli/server.py` 调用。
- `server.py`：编排中心，依赖 `transport`/`routing`/`control`/`stats`/`admin`/`security`/`auth`/`producers`。
- `client.py`：独立客户端，依赖 `protocol`/`transport`/`errors`/`control`/`producers`。
- `admin/`：依赖 `stats/`（读取数据）、`cache/`（引用 `TopicBufferRegistry` 类型，但运行时可为 `None`）。
- `cache/`：独立，仅被 `admin/server.py` 引用类型；**未被 `server.py` 实例化**。

---

## 3. 异常与退出码体系 `errors.py`

统一异常基类 `PulseMQError`，每个子类携带 `exit_code` 类属性；`exit_code_for(exc)` 供 CLI 将异常映射为进程退出码（`errors.py:57`）。

| 异常 | exit_code | 触发场景 |
|------|-----------|---------|
| `PulseMQError` | 1 | 通用兜底 |
| `TransportError` | 2 | 传输层错误 |
| `ConnectionError` | 2 | 故意覆盖内置名；包内显式导入（`errors.py:13`） |
| `AuthenticationError` | 3 | 认证失败（启动期 / 重连期），携带 `reason` |
| `ClientStartupError` | 4 | 服务器不可达 / 握手超时 / REGISTER 被拒，携带 `reason`/`address`/`username` |
| `FrameError` | 5 | 帧过短 / 魔数不符 / 版本不支持 / CRC 校验失败 / record_count 超限 |
| `SerializationError` | 5 | 未注册的序列化/压缩格式 |
| `ConfigurationError` | 6 | 配置非法（如 `auth.type` 非 plain） |
| `SecurityError` | 6 | 凭据文件解析失败、哈希格式非法、用户名非法 |
| `ResourceExhaustedError` | 7 | 资源耗尽（预留） |

> 旧文档只提了 exit 3/4，源码实际是完整的 1–7 体系。

---

## 4. 启动与生命周期 `lifecycle.py`

`run_server(server)` 统一启动顺序 + 信号处理（`lifecycle.py:10`）：

1. 注册 `SIGINT` / `SIGTERM` -> `_request_shutdown`（`server.stop()` 幂等，由 `is_shutting_down()` 守护）。
2. Windows 不支持 `add_signal_handler`，静默跳过（`lifecycle.py:22`）。
3. `await server.start()` -> `await server.wait_for_shutdown()`，返回 0。

CLI 入口 `cli/server.py:13` 的 `main()`：`setup_logging()` -> `Server()` -> `asyncio.run(run_server(server))`，异常经 `exit_code_for` 转退出码。

Server 另在 `start()` 末尾安装 `SIGHUP -> reload_credentials`（Linux，`server.py:251`），供 `pulsemq-users reload` 触发凭据热更新。

---

## 5. 服务端核心 `server.py`

`Server` 类是服务端入口，组装传输层、路由、控制面、统计、admin、producer 调度（`server.py:41`）。

### 5.1 构造参数

```python
Server(
    data_endpoint="tcp://0.0.0.0:5555",
    control_endpoint="tcp://0.0.0.0:5556",
    admin_endpoint="0.0.0.0:9090",
    credentials=None,           # 显式明文 dict -> 内存态 CredentialStore
    credentials_file=None,      # 缺省走 config.credentials_file
    allow_auto_generated=None,  # 文件不存在时是否自动生成默认 admin
    config=None,                # ServerConfig；缺省 load_server_config(None)
    admin_token=None,           # 显式传值（含空串=禁用 token）
    admin_token_file=None,
    latency_sample_rate=None,   # 覆盖 config
)
```

显式传值优先于 config；空串视为未传回退到 config（`server.py:58`）。凭据源：`credentials` 为 dict 时走 `CredentialStore.from_dict`（内存态、哈希落值）；否则 `CredentialStore.load()`，文件不存在则生成默认 admin，明文密码输出到 stderr 一次（`server.py:64`）。

### 5.2 生命周期

```python
srv = Server(...)
await srv.start()              # bind + admin + 4 后台任务 + 内置 producer
await srv.wait_for_shutdown()  # 阻塞到 Ctrl+C / srv.stop()
await srv.stop()               # 取消任务 -> 归档 -> 停 admin -> drain -> 关 transport -> 关 storage
```

### 5.3 后台任务

| 任务 | 方法 | 作用 |
|------|------|------|
| 数据面循环 | `_data_loop` (`server.py:331`) | recv -> decode_header -> stats -> match -> send |
| 控制面循环 | `_control_loop` (`server.py:354`) | recv -> decode_control -> `_dispatch_control` |
| 心跳扫描 | `_heartbeat_sweep_loop` (`server.py:465`) | 每秒 `sweep_timeout()`，清理超时 client（默认 6s） |
| 分钟归档 | `_minute_roll_loop` (`server.py:487`) | 每 60s `roll_minute()` -> `AsyncArchiveWriter.enqueue` |

### 5.4 控制命令分发

`_dispatch_control(ident, cmd_msg)` (`server.py:377`)，`ident` 永远是 ROUTER 的 bytes identity，`client_id` 是 payload 里的 app 字符串，两者不是一回事：

| 命令 | 处理 |
|------|------|
| `REGISTER` | registry.register -> OK 时写 `_ident_by_client_id[cid]=ident` + 订阅 topics + `on_connect`；回 `result` |
| `HEARTBEAT` | registry.heartbeat -> 回 OK |
| `SUBSCRIBE` | routing.subscribe + **registry.subscribe（回写 topics）** + `on_subscribe` |
| `UNSUBSCRIBE` | routing.unsubscribe + registry.unsubscribe + `on_unsubscribe` |
| `DISCONNECT` | routing.remove + 清 ident 映射 + registry.unregister + `on_disconnect`；回执失败降 debug（peer 已离开的预期竞态） |
| `KICK` | **未实现**（无处理分支，落入 "未知控制命令" debug） |

> `registry.subscribe/unsubscribe` 回写 client.topics，使在线快照/订阅计数与实际订阅表一致（`control.py:67`）。

### 5.5 内置 Producer 调度

`@srv.producer(topic, interval, serializer, compression)` / `@srv.burst_producer(...)` 装饰器注册到 `ProducerManager`（`server.py:263`）。`start()` 末尾 `start_all(self._on_server_produce)` 启动调度。

`_on_server_produce` (`server.py:309`)：`encode` -> `decode_header`（轻量统计）-> `TrafficStats.record` + 采样延迟 -> `routing.match` -> 广播。复用 `_data_loop` 统计口径，使服务端 producer 的 topic 在监控可见。

### 5.6 admin token 解析

`_resolve_admin_token` (`server.py:184`) 优先级：

1. 显式 `admin_token=` 参数（含空串 -> 禁用 token，向后兼容测试）
2. `config.admin_token`（可被环境变量 `PULSEMQ_ADMIN_TOKEN` 覆盖）
3. 环境变量 `PULSEMQ_ADMIN_TOKEN`（双保险）
4. 随机生成 32 字节 base64url，写 `admin_token_file`（0600，POSIX 校验权限位并告警；Windows 提示目录 ACL 受控）

### 5.7 停止顺序

`stop()` (`server.py:517`)：

1. `_running=False`, `_stop.set()`
2. 取消并等待 4 个后台任务
3. 最后一次 `roll_minute()` 入队归档
4. `producer_mgr.stop_all()`
5. `admin.stop()`（停 SSE + HTTP）
6. `archive_writer.stop()`（drain 剩余 SQLite 写入）
7. `transport.close()`
8. `storage.close()`（**必须在 archive_writer.stop 之后**，否则 drain 写已关闭连接）

---

## 6. 客户端 `client.py`

`Client` 及其子类 `ProducerClient` / `ConsumerClient`（`client.py:76`）。

### 6.1 子类

| 类 | `_roles` | 特有能力 |
|----|---------|---------|
| `Client` | `["publisher","subscriber"]` | 发布 + 订阅 |
| `ProducerClient` | `["publisher"]` | `producer()`/`burst_producer()` + `run_forever()`；`subscribe` 抛 `NotImplementedError` |
| `ConsumerClient` | `["subscriber"]` | `publish` 抛 `NotImplementedError` |

### 6.2 启动流程（monitor-based 认证）

`start()` (`client.py:122`)：

```
数据面 DEALER + PLAIN + monitor -> 等待认证裁定 (_STARTUP_MONITOR_TIMEOUT=5s)
  ├─ handshake_ok -> 继续
  ├─ auth_failed -> AuthenticationError (exit 3)
  └─ 超时/其他 -> ClientStartupError (exit 4)
控制面 DEALER + PLAIN（无 monitor，复用数据面认证态）
REGISTER -> 等待 reply (_REGISTER_REPLY_TIMEOUT=3s)
  ├─ 超时 -> ClientStartupError(reason=REGISTER_REJECTED)
  └─ result != OK -> ClientStartupError(reason=result)
恢复既有订阅（重连场景）-> 启动 recv_loop + heartbeat_loop(1s)
切换到运行期 monitor 回调 _on_runtime_monitor
```

数据面/控制面两个 DEALER 共用同一 bytes identity（`client_id.encode("utf-8")`），使 server 的 routing 表能直接转发到数据面 DEALER（`client.py:133`）。

### 6.3 重连机制

`_on_runtime_monitor("disconnected")` -> `_reconnect_loop` (`client.py:256`)：

```
指数退避：初始 1s，×2，封顶 30s
新 Transport -> PLAIN 重认证 -> REGISTER（同 client_id）-> 恢复订阅 -> 重启 recv/heartbeat
  ├─ auth_failed -> 存 self._reconnect_fatal (AuthenticationError) + set _stop；不在后台任务内 raise
  ├─ ALREADY_ONLINE / 其他暂态 -> 退避重试
  └─ _stop 触发 -> 立即退出
```

**致命错误重抛机制**：`_reconnect_loop` 是后台任务，直接 raise 会被 asyncio GC 吞掉。改为存到 `self._reconnect_fatal`，由 `run_forever`/`stop` 在主任务上下文重新抛出，使 CLI 经 `exit_code_for` 拿到 exit 3（`client.py:656`）。

**in-flight transport 防泄漏**：`in_flight` 指向本次重连尚未完全成功提交的新 transport，`except BaseException`（含 `CancelledError`）统一关闭它，避免半连接 socket/monitor 任务泄漏（`client.py:396`）。

### 6.4 已知偏差与限制

- **ALREADY_ONLINE 偏差**（`client.py:25` 模块 docstring）：Spec 1 §8.3 规定重连收到 ALREADY_ONLINE 应 exit 4，但服务端 stale 记录要等心跳超时扫描（~6s）才释放，立即 exit 4 会让网络闪断后任何重连被旧条目击落。当前实现退避重试，待服务端支持快速 stale 驱逐后再回到严格 exit 4。
- **控制面 ack 串扰**（`client.py:20`）：REGISTER/SUBSCRIBE 各做一次 `recv("control")`，心跳 ack 是 fire-and-forget，可能堆积并串扰下一次 register/subscribe 的 recv。单客户端 e2e 场景可接受。

### 6.5 生命周期回调

`on_connected` / `on_disconnected` / `on_reconnecting`（`client.py:116`），均为可选 async 回调，异常被捕获并记日志，不影响主流程。

---

## 7. 传输层 `transport/router.py`

**唯一直接 import zmq 的模块**（`transport/router.py:8`）。

### 7.1 核心类

| 类 | 用途 |
|----|------|
| `Transport` | ROUTER bind / DEALER connect / send / recv / monitor |
| `AsyncZAPHandler` | inproc ZAP REP socket，异步处理 PLAIN 认证（`transport/router.py:30`） |
| `PlainAuthDict` | Spec 1 兼容层：明文 dict 白名单。**Server 不使用**（用 `PlainAuth(CredentialStore)`），仅保留供测试/兼容 |

### 7.2 ZAP 认证流程

```
DEALER connect (PLAIN)
  -> libzmq 发 ZAP 请求到 inproc://zeromq.zap.01
  -> AsyncZAPHandler._loop recv
  -> _handle: run_in_executor(self._auth.verify, username, password)
    └─ CredentialStore.verify -> bcrypt.checkpw（线程池，~200ms）
  -> _reply (200/400) -> on_auth 回调
```

`verify` 在 `run_in_executor` 中执行，避免 bcrypt.checkpw 阻塞事件循环（`transport/router.py:80`）。

**ZAP 是 ctx 级单例**：同一 ctx 上所有 `plain_server=True` 的 socket 共享 `inproc://zeromq.zap.01` REP socket，仅首次 auth bind 时创建 handler（`transport/router.py:131`）。因此 `on_auth` 必须在首次（数据面）bind 提供，控制面 bind 复用同一 ZAP，`on_auth` 被忽略（`server.py:138`）。

### 7.3 Transport 关键特性

- `ROUTER_MANDATORY=1`：发送到已断开 identity 时抛异常，`_control_loop` 需 try/except 保护（`transport/router.py:124`）。
- `LINGER=1000`：关闭时等待 1 秒完成待发消息。
- monitor 事件：`connected` / `disconnected` / `handshake_ok` / `auth_failed` / `other`（`transport/router.py:177`）。
- `_socket_for(role)`：role 不存在时若仅有一个 socket 则回退到它，让 client 单 DEALER 无需每次传 role（`transport/router.py:190`）。
- `send(identity, frame, role)`：`identity` 非空走 `send_multipart([identity, frame])`，空走 `send(frame)`（DEALER 侧，`transport/router.py:202`）。

---

## 8. 协议模块 `protocol/`

### 8.1 帧编解码 `protocol/frames.py`

**单 bytes 帧格式**（`frames.py:1`）：

```
magic(2) ver(1) msg_type(1) flags(1) data_type(1) topic_len(2 BE)
topic(N) ts(8 BE ns) record_count(4 BE) payload(变长) [CRC32?(4)]
```

- 头定长部分：`magic(2)+ver(1)+msg_type(1)+flags(1)+data_type(1)+topic_len(2)` = 8B，`+ts(8)+rc(4)` = 12B，共 20B（`frames.py:26`）。
- `magic = "PM"`，`VERSION = 0x01`。
- `record_count` 上限 **1,000,000**（`frames.py:219`）。
- CRC 可选，由 flags bit 7 指示。

核心函数：

| 函数 | 用途 | 数据路径 |
|------|------|---------|
| `encode()` (`frames.py:156`) | 序列化 + 压缩 -> bytes | 发送端 |
| `decode()` (`frames.py:236`) | 解压 + 反序列化 + `_restore_type` | 接收端 |
| `decode_header()` (`frames.py:280`) | 仅读头部（不解压 payload） | 服务端 `_data_loop` |
| `encode_control()` (`frames.py:303`) | 控制面帧编码（cmd 作为 topic，`data_type=UNKNOWN`） | 控制面 |
| `decode_control()` (`frames.py:320`) | 控制面帧解码 -> `ControlMessage` | 服务端 `_control_loop` |

### 8.2 `encode` 自动推断（v7.1.0）

`encode` 在未显式传参时自动推断（`frames.py:190`）：

1. **`data_type`**：`_infer_data_type` 按 Python 类型推断（DataFrame/DICT/STR/BYTES，`frames.py:138`）。
2. **`serializer` 默认值**：`_SERIALIZER_RULES` 兼容矩阵（`frames.py:130`）：

   | data_type | 允许的 serializer | 默认 |
   |-----------|-------------------|------|
   | DICT | msgpack, json | msgpack |
   | DATAFRAME | msgpack, json, pyarrow | **pyarrow** |
   | STR | str | str |
   | BYTES | bytes | bytes |

   显式传不兼容的 serializer 抛 `TypeError`。
3. **`record_count`**：`_infer_record_count`（list -> len，DataFrame -> 行数，标量/dict -> 1，`frames.py:110`）。
4. **DataFrame + msgpack/json**：预处理转 `list[dict]`（`frames.py:210`）。

### 8.3 `_restore_type(data, data_type, serializer)`

根据 `data_type` 还原原始 Python 类型（`frames.py:71`）：

| 原始类型 | serializer | 存储格式 | 还原方式 |
|----------|-----------|---------|---------|
| DataFrame | msgpack/json | list[dict] | `pd.DataFrame(data)` |
| DataFrame | pyarrow | pa.Table | `data.to_pandas()` |
| dict | pyarrow | pa.Table | `data.to_pylist()[0]` |
| str | 透传 | 原始类型 | bytes -> UTF-8 |
| bytes | 透传 | 原始类型 | str -> UTF-8 |

### 8.4 序列化 `protocol/serialization.py`

注册机制：`register(name, serializer)` / `get(name)` / `available()`（`serialization.py:148`）。

| 格式 | 类 | 后端 | 备注 |
|------|----|------|------|
| msgpack | MsgpackSerializer | msgspec.msgpack | 通用结构化 |
| json | JsonSerializer | msgspec.json | **拒绝 bytes**（解码后变形为 str，`serialization.py:67`） |
| pyarrow | PyArrowSerializer | pyarrow.ipc | 支持 Table/DataFrame/dict/list[dict]；不支持的类型明确报错（`serialization.py:108`） |
| str | StringSerializer | UTF-8 | 接受 str/bytes |
| bytes | BytesSerializer | 透传 | 仅接受 bytes |

### 8.5 压缩 `protocol/compression.py`

| 算法 | 后端 |
|------|------|
| none | 透传 |
| snappy | python-snappy |
| lz4 | lz4.frame |
| zstd | zstandard |

### 8.6 flags 位域 `protocol/flags.py`

**单字节布局**（`flags.py:1`）：

| 位 | 字段 | 编码 |
|----|------|------|
| bit 0-2 | 序列化格式 | msgpack=000, bytes=001, pyarrow=010, str=100, json=101 |
| bit 3-4 | 压缩算法 | none=00, snappy=01, lz4=10, zstd=11 |
| bit 5-6 | reserved | — |
| **bit 7** | **CRC** | `0b1000_0000`（`flags.py:50`） |

> ⚠️ **纠正旧文档**：DESIGN v2 §5.4 写"CRC（bit 5）"，`flags.py` 顶部注释写"bit[5:7] reserved"，均与实现不符。**实际 CRC 在 bit 7**，由 `_CRC_BIT = 0b1000_0000` 定义（`flags.py:50`）。

### 8.7 消息类型 `protocol/msg_type.py`

```python
class MsgType:   DATA=0x01, CONTROL=0x02, HEARTBEAT=0x03, ADMIN=0x04
class DataType:  UNKNOWN=0, DICT=1, DATAFRAME=2, STR=3, BYTES=4
```

> ⚠️ `HEARTBEAT` / `ADMIN` 常量定义但**未被使用**：HEARTBEAT 实际作为控制命令字符串（`ControlCmd.HEARTBEAT`）走 `MsgType.CONTROL` 帧。注释"Spec 1：DATA/CONTROL/HEARTBEAT/ADMIN"是历史遗留，实际仅 DATA/CONTROL 生效。

---

## 9. 路由模块 `routing.py`

`SubscriptionTable` - topic 前缀匹配路由表（`routing.py:5`）。

```
_by_identity: {identity(bytes) -> set[pattern]}
match(topic) -> set[identity]  (遍历所有 pattern，前缀匹配)
```

匹配规则（`routing.py:33`）：
- `foo.*` -> 匹配 `foo` 和 `foo.<anything>`
- `foo` -> 精确匹配 `foo`

- 订阅/取消订阅由控制面 `SUBSCRIBE`/`UNSUBSCRIBE` 驱动，数据面 `match()` 只读。
- `snapshot()` 把 bytes key decode 成 str（client_id 是 uuid-hex ASCII，decode 无损），供 JSON 序列化（`routing.py:42`）。
- `subscribers_of(identity)`：查某 identity 的订阅模式集合。

---

## 10. 控制面 `control.py`

| 符号 | 用途 |
|------|------|
| `ControlCmd` (`control.py:8`) | REGISTER / HEARTBEAT / SUBSCRIBE / UNSUBSCRIBE / DISCONNECT / **KICK（预留未实现）** |
| `ControlMessage` | `cmd: str` + `payload: dict` |
| `RegisterResult` | OK / ALREADY_ONLINE / REJECTED |
| `ClientInfo` | client_id / username / endpoint / roles / topics / connected_at / last_seen |
| `OnlineRegistry` (`control.py:40`) | 在线用户表，key=username（单用户单在线） |

`OnlineRegistry` 数据结构：

```
_by_client: {client_id -> ClientInfo}
_by_user:   {username -> client_id}
```

方法：

| 方法 | 作用 |
|------|------|
| `register(info)` | username 已存在 -> ALREADY_ONLINE；否则写入 -> OK |
| `heartbeat(client_id)` | 更新 `last_seen` |
| `get_username(client_id)` | 反查 username（供 SUBSCRIBE/UNSUBSCRIBE 事件埋点，`control.py:62`） |
| `subscribe(client_id, pattern)` | **回写 client.topics**（幂等、sorted），使监控订阅计数与实际一致 |
| `unsubscribe(client_id, pattern)` | 回写 client.topics |
| `unregister(client_id)` | 移除两边映射 |
| `sweep_timeout()` | 清理 `now - last_seen > heartbeat_timeout` 的记录，返回离线列表 |
| `snapshot()` | 在线 client 列表（供 admin / ConnectionStats） |

> `heartbeat_timeout` 默认 6.0s（`control.py:43`）。旧文档漏写了 `subscribe`/`unsubscribe`/`get_username`。

---

## 11. 认证模块 `auth.py` 与 `security.py`

### 11.1 `auth.py`

`PlainAuth` - ZAP 认证决策器，委托 `CredentialStore`（`auth.py:7`）。

```python
class PlainAuth:
    def authenticate(username, password) -> AuthResult       # 完整结果
    def verify(username, password) -> tuple[bool, str|None]   # ZAP handler 兼容签名
```

`verify` 与 Spec 1 `PlainAuthDict.verify` 同签名，供 `AsyncZAPHandler` 在 `run_in_executor` 中调用（`auth.py:20`）。

### 11.2 `security.py`

`CredentialStore` - bcrypt 哈希凭据持久化（`security.py:73`）。

| 方法 | 作用 |
|------|------|
| `from_dict(creds)` (classmethod) | 内存态 store（无文件），save/reload 为 no-op；供 Server 接受显式明文 dict 与测试 |
| `load()` | 加载 TOML；不存在则生成默认 admin（返回明文密码供日志输出一次） |
| `save()` | 原子写 TOML（tmp + `os.replace`） |
| `verify(u, p)` | user_not_found / user_disabled / invalid_password / OK -> `AuthResult` |
| `add_user()` | 新增（校验名称合法性，`_NAME_RE = ^[A-Za-z0-9_-]{1,64}$`） |
| `set_password()` / `set_enabled()` | 原地改 |
| `reload()` | 热更新（SIGHUP 触发，原子替换内存白名单） |
| `list_users()` | 全部用户 |

要点：

- **bcrypt cost 默认 12**（约 200ms/次），通过 `run_in_executor` 避免阻塞（`security.py:77`）。
- `password_hash_algo` 非 bcrypt 时告警并回退 bcrypt（argon2 等为预留，`security.py:83`）。
- 名称校验阻断 `.`/`]`/`"`/换行等危险字符流入 `save()` 的 f-string，防 TOML 损坏/注入（`security.py:26`）。
- 默认 admin 密码来源：`PULSEMQ_ADMIN_PASSWORD` 环境变量 > 随机 16 字符（`security.py:134`）。
- 凭据文件格式：

  ```toml
  [users.admin]
  hashed_password = "$2b$12$..."
  roles = ["admin"]
  enabled = true
  created_at = "2026-06-27T00:00:00Z"
  ```

---

## 12. 统计模块 `stats/`

### 12.1 流量统计 `stats/traffic.py`

`TrafficStats` - 分钟粒度 topic 流量，内存窗口（默认 `retention_minutes=480`，即 8 小时，`traffic.py:30`）。

`record(topic, record_count, payload_size)` 数据路径（`traffic.py:38`）：

```
单次 dict.get -> 增 count -> 每 1024 条检查分钟滚动
```

- `roll_minute()` (`traffic.py:56`)：归档当前分钟 -> 追加到 slots deque（maxlen=retention）-> 返回归档数据供 SQLite 落库。
- `all_topics_snapshot()` (`traffic.py:121`)：计算 60 秒滚动均值 = 当前分钟实测 + 上一分钟按 `(60-elapsed)/60` 比例外推。
- `get_history(topic, minutes)`：内存历史（给 admin 曲线用）。
- **线程安全**：单写者（server 数据线程）+ 多读者（admin HTTP 线程），靠 GIL；`all_topics_snapshot` 对 key 集合做快照避免迭代中 `roll_minute` 的 `clear()` 触发 RuntimeError（`traffic.py:127`）。

### 12.2 延迟统计 `stats/latency.py`

`LatencyStats` - 固定桶直方图 + 采样 + P50/P95/P99 线性插值（`latency.py:17`）。

- 桶上界（ns）：50_000 / 100_000 / 500_000 / 1_000_000 / 5_000_000 / 10_000_000 / 50_000_000，末桶 `[50ms, +inf)`（`latency.py:9`）。
- 末桶有限上界 = 末上界 ×2，使插值有定义（`latency.py:14`）。
- `should_sample()`：`random.random() < rate`（`latency.py:30`）。
- `_percentile_ms(pct)`：桶内线性插值，比固定代表值更准（`latency.py:45`）。
- 采样率默认 1%（`config.latency_sample_rate=0.01`）。

### 12.3 连接事件 `stats/connections.py`

`ConnectionStats` - 事件环（deque maxlen=默认 200）+ 在线客户端快照（`connections.py:41`）。

| 方法 | 触发 | 事件 type |
|------|------|----------|
| `on_connect(cid, user, endpoint, role)` | REGISTER 成功 | connect |
| `on_disconnect(cid, reason)` | DISCONNECT / heartbeat_timeout | disconnect |
| `on_auth(user, endpoint, success, reason)` | ZAP 认证回调 | auth |
| `on_subscribe(cid, user, pattern)` | SUBSCRIBE | subscribe |
| `on_unsubscribe(cid, user, pattern)` | UNSUBSCRIBE | unsubscribe |

- 事件 `type` 统一**小写**，与前端 `web_ui` 的 `tCls` 颜色分类对齐（`connections.py:49` 注释）。
- `online_clients()`：经 `registry_snapshot_fn` 反查 registry，构造 `ClientSnapshot`（含 `duration_seconds`）。
- `counters()`：`online_users` / `online_producers` / `online_consumers` / `total_subscriptions`。
- `_role_of(roles)`：含 pub 且含 sub -> both；仅 pub -> producer；仅 sub -> consumer；否则 consumer（`connections.py:29`）。

> 旧文档漏写 `on_subscribe`/`on_unsubscribe`。

### 12.4 持久化 `stats/storage.py`

`StatsStorage` - SQLite (WAL) 分钟统计持久化（`storage.py:20`）。

- 表 `minute_stats`：`(topic, timestamp)` 主键，`INSERT OR REPLACE`。
- **跨线程模型**：`AdminServer` 默认在独立线程读 `load_history`，写 `save_minutes_batch` 在主线程的 `AsyncArchiveWriter` consumer 任务。连接用 `check_same_thread=False` + `threading.Lock` 串行化所有操作（`storage.py:43`）。zmq 数据接收循环从不触碰 SQLite，DB 读写不阻塞 zmq。
- `cleanup(retention_days)`：清理过期数据。

`AsyncArchiveWriter` - asyncio.Queue 批量写入（`storage.py:147`）：

- `enqueue(archived)` -> queue；`_consume` 阻塞取首条 + 批量取最多 `batch_size-1` 条合并 -> `save_minutes_batch`。
- `stop()`：取消 consumer 任务 -> 在主上下文 `_drain()` 剩余项（不依赖被取消任务内执行，可靠）。

---

## 13. 管理后台 `admin/`

### 13.1 HTTP 服务 `admin/server.py`

stdlib asyncio HTTP server，手写请求解析（无框架，`admin/server.py:1`）。

**端点**：

| 端点 | 用途 |
|------|------|
| `GET /` / `/index.html` | Web UI 首页 |
| `GET /static/{path}` | 静态资源（ECharts 等，拒绝 `..`/绝对路径/反斜杠，`server.py:544`） |
| `GET /api/v1/stats/realtime` | 实时指标 JSON |
| `GET /api/v1/stats/stream` | SSE 实时推送（1s/帧） |
| `GET /api/v1/clients` | 在线客户端明细 |
| `GET /api/v1/events?limit=` | 生命周期事件（默认 50） |
| `GET /api/v1/topics` | 主题列表 + 当前指标 + cache 尺寸 |
| `GET /api/v1/topics/{topic}/history?minutes=` | 分钟级历史（内存 + SQLite 合并去重，`server.py:397`） |
| `GET /api/v1/system/status` | 系统状态（uptime, version） |
| `GET /healthz` | 健康检查（**无需 token**） |

**独立线程模式**（默认 `admin_thread=True`，`server.py:100`）：

- HTTP server 运行在独立 daemon 线程 + 独立 asyncio loop，不阻塞 ZMQ 数据线程。
- `start()` 阻塞至线程内 server 就绪（`_thread_started` 置位）后返回。
- `stop()` 通过 `run_coroutine_threadsafe` 在 admin loop 上调度 `_stop_serve()`，再 join 线程；超时/异常被吞，保证调用方不死锁。

**Token 认证**（`admin/auth.py`）：除 `/healthz` 外所有端点需 `?token=` 或 `Authorization: Bearer`。`expected_token` 为 None/空 -> 禁用（放行，向后兼容测试）。用 `hmac.compare_digest` 常量时间比较（`auth.py:37`）。

**SSE**（`server.py:448`）：

- 每个客户端一个 `asyncio.Queue(maxsize=64)`，`_sse_broadcast_loop` 每 1s 广播 `_realtime_snapshot`。
- 队列满（客户端断开/消费过慢）-> 主动取消该连接，避免死客户端残留内存泄漏（`server.py:500`）。
- `_realtime_snapshot` 注入 `start_time` + `server_time`，供前端 SSE 实时计算 uptime（修复 uptime 冻结，`server.py:344`）。
- `sse_events` 字段携带最近 10 条生命周期事件，JS 全量替换 state（无重复）。

### 13.2 Web UI `admin/web_ui.py`

单文件 HTML，内嵌 CSS + JS + ECharts（`web_ui.py:1`）。深色玻璃态主题。

组件：

- 指标卡片（4 个）：活跃主题、消息量/s、流量/s、运行时间
- 客户端卡片（4 个）：在线用户、生产者、消费者、订阅数
- 流量趋势图：ECharts line，分钟级，1H/6H 切换，最多 5 topic 叠加（LRU 淘汰），30s 自动刷新历史
- 延迟柱状图：P50/P95/P99 实时
- 事件流：最近事件，按 type 着色（connect/disconnect/subscribe/unsubscribe/auth）
- 主题卡片网格
- Client 详情弹窗（`/api/v1/clients`）

### 13.3 Token 认证 `admin/auth.py`

`TokenAuth` 见 §13.1。`enabled` 属性供启动日志判断是否输出带 token 的可点击 URL（`server.py:151`）。

---

## 14. Producer 管线 `producers/`

### 14.1 `producers/types.py`

数据白名单类型与回调签名别名（`types.py:1`）：

- `PubData = Union[pd.DataFrame, dict, bytes, str]` - 与运行时白名单一一对应。
- `SimpleProducerCallback` - `async def fn() -> PubData | None`
- `SenderProducerCallback` - `async def fn(sender: PublisherSender) -> PubData | None`（前向字符串引用）
- `ProducerCallback` - 两者并集

> ⚠️ **遗留**：`PublisherSender` / `PulsePublisher` 仅以字符串前向引用出现，**源码中不存在这些类**。`inject_sender=True` 路径会 `RuntimeError("inject_sender=True 需要 sender_factory")`（`manager.py:148`）。该机制为未接入遗留项。

### 14.2 `producers/manager.py`

`ProducerManager` - 回调注册 + asyncio Task 并发调度（`manager.py:42`）。

`ProducerSpec` 字段（`manager.py:21`）：

| 字段 | 默认 | 说明 |
|------|------|------|
| `name` | - | topic 名（同时也是 producer 名） |
| `callback` | - | async 回调 |
| `interval` | 5.0 | 推送间隔（秒）；**0.0 = burst 模式** |
| `cache_size` | 100_000 | ⚠️ **遗留未接入**：Manager 内部从不读取此字段去建 TopicBuffer |
| `serializer` | None | None = encode 时按数据类型自动选择 |
| `compression` | "none" | 压缩格式 |
| `inject_sender` | False | ⚠️ True 会 RuntimeError（见 §14.1） |

两种调度模式：

- **普通 producer**（`_run_loop`，`manager.py:131`）：固定延迟调度，`sleep(max(0, interval - elapsed))`，不积压；异常不崩溃。
- **burst producer**（`_run_burst_loop`，`manager.py:167`）：无间隔连续发送，回调返回 `None` 停止；异常后冷却 0.1s。

API：`register()` / `register_burst()` / `start_all(on_message, sender_factory=None)` / `stop_all()` / `specs` 属性。

### 14.3 两种接入方式

- **服务端内置**：`@srv.producer(...)` / `@srv.burst_producer(...)` -> `Server._on_server_produce`（直接 encode + 路由广播，`server.py:309`）。
- **客户端**：`@ProducerClient.producer(...)` / `@burst_producer(...)` -> `_on_produce`（调 `self.publish`，`client.py:650`）-> `run_forever()` 启动调度。

---

## 15. Topic 缓存 `cache/`（预留，未接入）

`cache/topic_buffer.py` 定义了按**记录数**淘汰的环形缓存，设计用途是"新订阅者补发历史"（`topic_buffer.py:1`）。

| 类 | 作用 |
|----|------|
| `CachedMessage` | `timestamp_ns` / `frame` / `record_count` |
| `TopicBuffer` | 单 topic 环形缓存；`append` 累计 `_total_records`，超 `max_records` 从队首丢帧；`snapshot(since_ns, limit)` 按时间戳查询 |
| `TopicBufferRegistry` | 所有 topic 缓存的注册表；`get_or_create` / `get` / `snapshot`（输出 `{current, max}` 给 admin 显示） |

**当前接入状态**（关键）：

- `admin/server.py:22` 引用 `TopicBufferRegistry` 类型，接受 `topic_buffers` 参数，在 `_realtime_snapshot` / `_list_topics` 输出 `cache_sizes`，但**容忍 `None`**（`server.py:324`）。
- `Server.start()` 显式传 `topic_buffers=None`，注释"Spec 1 不维护 topic 缓存"（`server.py:152`）。
- `ProducerSpec.cache_size` 字段被注册接口接收，但 `ProducerManager` **从不读取**它去创建/写入 `TopicBuffer`。
- `Client._recv_loop` 收到消息后也**不写入**任何缓存。

**结论**：缓存补发能力**未接入主流程**。`cache/` 模块、`cache_size` 参数、`admin` 的 `cache_sizes` 输出均为预留/半成品。当前 Server 行为是纯转发、不缓存。如需启用，需在 `Server` 中实例化 `TopicBufferRegistry`，在 `_data_loop`/`_on_server_produce` 转发前 `append`，并在 `SUBSCRIBE` 时 `snapshot` 补发。

---

## 16. 配置 `config.py`

### 16.1 `ServerConfig`（`config.py:17`）

| 字段 | 默认 | 环境变量覆盖 |
|------|------|-------------|
| `data_endpoint` | `tcp://0.0.0.0:5555` | `PULSEMQ_DATA_ENDPOINT` |
| `control_endpoint` | `tcp://0.0.0.0:5556` | `PULSEMQ_CONTROL_ENDPOINT` |
| `admin_endpoint` | `0.0.0.0:9090` | `PULSEMQ_ADMIN_BIND` |
| `credentials_file` | `./pulsemq_users.toml` | `PULSEMQ_CREDENTIALS_FILE` |
| `heartbeat_timeout` | 6.0 | ❌ |
| `stats_db` | `sqlite://./pulsemq_stats.sqlite` | ❌ |
| `stats_retention_minutes` | 480 | ❌ |
| `allow_auto_generated_credentials` | True | ❌ |
| `password_hash_algo` | "bcrypt" | ❌ |
| `bcrypt_cost` | 12 | ❌ |
| `admin_token` | "" | `PULSEMQ_ADMIN_TOKEN` |
| `admin_token_file` | `./pulsemq_admin.token` | ❌ |
| `sse_interval` | 1.0 | ❌ |
| `latency_sample_rate` | 0.01 | ❌ |
| `event_ring_size` | 200 | ❌ |
| `stats_archive_batch_size` | 50 | ❌ |
| `admin_thread` | True | ❌ |
| `ui_enabled` | True | ❌ |
| `retention_days` | 7 | ❌ |

> 仅 5 个端点/凭据/token 项支持环境变量覆盖，其余只能通过 TOML 配置文件或 `Server(config=...)` 传入。`auth.type` 仅支持 `plain`，否则抛 `ConfigurationError`（`config.py:72`）。

### 16.2 `ClientConfig`（`config.py:40`）

| 字段 | 默认 | 环境变量覆盖 |
|------|------|-------------|
| `data_endpoint` | `tcp://localhost:5555` | `PULSEMQ_DATA_ENDPOINT` |
| `control_endpoint` | `tcp://localhost:5556` | `PULSEMQ_CONTROL_ENDPOINT` |
| `username` | "" | `PULSEMQ_USERNAME` |
| `password` | "" | `PULSEMQ_PASSWORD` |
| `client_id` | `uuid4().hex` | ❌ |
| `heartbeat_interval` | 1.0 | ❌ |
| `reconnect_initial_delay` | 1.0 | ❌ |
| `reconnect_max_delay` | 30.0 | ❌ |
| `reconnect_backoff_multiplier` | 2.0 | ❌ |

> ⚠️ `ClientConfig` 存在但 `Client.__init__` **未使用它**（`Client` 直接接受显式参数）。重连参数实际由 `client.py` 模块常量 `_RECONNECT_*` 控制（`client.py:57`）。`ClientConfig` 目前仅供配置加载/测试，未接入 `Client` 构造路径。

---

## 17. 日志 `logging_setup.py`

`setup_logging()` (loguru)（`logging_setup.py:13`）：

- stderr sink（交互/容器可见）+ 文件 sink（`logs/pulsemq_{time:YYYY-MM-DD}.log`，每日滚动，保留 30 天）。
- `json=True` 切 JSON 结构格式。
- `log_event(level, event_type, **fields)`：结构化输出生命周期事件，`event_type ∈ AUTH/CLIENT/...`（`logging_setup.py:53`）。

---

## 18. 包入口与 CLI

### 18.1 `__init__.py`（`__init__.py:1`）

导出 `Client` / `ProducerClient` / `ConsumerClient` / `Server` / `PulseMessage` / `PubData` / `__version__`。Windows 自动设置 `WindowsSelectorEventLoopPolicy`。版本号从 `_version.py` 单一读取（`_version.py:9`，当前 `7.1.0`）。

### 18.2 CLI 入口（`pyproject.toml:39`）

| 命令 | 入口 | 作用 |
|------|------|------|
| `pulsemq` / `pulsemq-server` | `pulsemq.cli.server:main` | 启动 Server，Ctrl+C 优雅关闭 |
| `pulsemq-users` | `pulsemq.cli.users:main` | 用户管理（不连 Server，直接读写凭据文件） |

`pulsemq-users` 子命令（`cli/users.py:25`）：

```
add <user> [--password] [--roles]    # 密码自动 bcrypt 哈希；--password 可省略改交互输入
list
disable <user> / enable <user>
passwd <user> [--password]
reload                               # 向 Server 发 SIGHUP（需 PULSEMQ_PID，仅 POSIX）
```

`--file <path>` 可放在子命令前或后（parent parser）。`reload` 在 Windows 提示走 admin 接口（`cli/users.py:89`）。

---

## 19. 附录：端口 / 遗留项 / 与旧文档差异

### 19.1 端口

| 端口 | 协议 | 用途 |
|------|------|------|
| 5555 | ROUTER | 数据面（消息收发） |
| 5556 | ROUTER | 控制面（注册/心跳/订阅/断开） |
| 9090 | HTTP | 监控管理界面 |

### 19.2 已知遗留 / 未接入项（源码中存在但未生效）

| 项 | 位置 | 状态 |
|----|------|------|
| `cache/` 模块 | `cache/topic_buffer.py` | 已实现，Server 传 `None` 未接入 |
| `ProducerSpec.cache_size` | `producers/manager.py:27` | 字段被接收，Manager 从不读取 |
| `inject_sender` / `PublisherSender` / `PulsePublisher` | `producers/types.py`、`manager.py:148` | 前向引用，类不存在；True 走 RuntimeError |
| `ControlCmd.KICK` | `control.py:14` | 常量定义，server 无处理分支 |
| `MsgType.HEARTBEAT` / `ADMIN` | `protocol/msg_type.py:7` | 常量定义未被使用 |
| `ClientConfig` | `config.py:40` | 未接入 `Client` 构造路径 |
| `PlainAuthDict` | `transport/router.py:16` | Spec 1 兼容层，Server 用 `PlainAuth(CredentialStore)`；仅测试/兼容 |
| `StatsStorage.save_minute`（单条） | `stats/storage.py:66` | 存在，实际走 `save_minutes_batch` 批量 |

### 19.3 与旧文档（DESIGN v2 / README）的主要差异

| 项 | 旧文档 | 源码实际 |
|----|--------|---------|
| 版本 | v2 | 7.1.0 |
| `errors.py` | 未提 | 完整异常 + 退出码 1–7 体系 |
| `lifecycle.py` | 未提 | `run_server` + 信号处理 |
| `cache/` | "无 topic 持久化缓冲" | 模块存在但未接入（行为与旧文档描述一致，但源码留有死代码） |
| CRC 位 | bit 5 | **bit 7**（`_CRC_BIT = 0b1000_0000`） |
| `MsgType` | DATA/CONTROL | 源码定义 4 个，实际用 2 个（HEARTBEAT/ADMIN 多余） |
| `OnlineRegistry` 方法 | register/heartbeat/sweep_timeout | 多了 subscribe/unsubscribe/get_username |
| `ConnectionStats` 事件 | on_connect/disconnect/auth | 多了 on_subscribe/on_unsubscribe |
| `StatsStorage` | SQLite WAL | 补充：`check_same_thread=False` + `threading.Lock` 跨线程 |
| `encode` 自动推断 | 未提 | v7.1.0 自动推断 data_type/serializer/record_count |
| `Server._ident_by_client_id` | 未提 | client_id↔bytes ident 映射，路由键是 bytes identity |
| `ServerConfig` 字段 | 部分列出 | 19 个字段，环境变量仅覆盖 5 个 |
| 退出码 | 3/4 | 完整 1–7 |
| admin SSE | 未提 start_time | 注入 start_time 修复 uptime 冻结 |
