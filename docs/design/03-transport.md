# 03 传输层设计

> 源码：`src/pulsemq/transport/router.py`（全项目**唯一 import zmq 的模块**）

## 1. 类总览

| 类 | 运行环境 | 职责 |
|----|----------|------|
| `Transport` | 服务端主 loop / 客户端 loop | socket 生命周期管理：异步 bind（服务端控制面）/ connect（客户端）；同步数据面的门面 |
| `SyncDataThread` | 服务端独立线程 + 独立 `zmq.Context` | 低延迟数据面：ROUTER recv → on_message 回调 → ROUTER send |
| `SyncZAPHandler` | 服务端独立线程（同步 ctx 内） | 数据面 PLAIN 认证 |
| `AsyncZAPHandler` | 服务端主 loop（异步 ctx 内） | 控制面 PLAIN 认证 |
| `PlainAuthDict` | — | 认证决策器接口：`verify(user, pw) -> (ok, reason)`；`security.CredentialStore`/`auth.PlainAuth` 均实现该签名 |

`AuthCallback = (username, address, ok, reason) -> Awaitable[None]`：认证事件回调，由 `Server._on_auth_event` 桥接到 `ConnectionStats`。address 当前恒为空串（ZAP 帧内地址字段未取），保留接口。

## 2. Transport（异步部分）

### 2.1 socket 管理

- `_sockets: dict[role, Socket]`，以 role（`"control"`/`"data"`/`"server_ingress"`）索引。
- `_socket_for(role)`：role 缺失且仅有一个 socket 时回退到它——客户端单 DEALER 场景免传 role。
- 公共 socket 选项：`LINGER=1000`；`SNDHWM/RCVHWM` 由构造参数（默认各 10000 帧）。
- 服务端 ROUTER 额外设 `ROUTER_MANDATORY=1`：向已消失的 identity 发送立即抛错（而非静默丢弃），配合各调用点的异常处理显式计丢弃/降日志。

### 2.2 send / recv 语义

```python
send(identity, frame, role)   # identity 非空 → send_multipart([id, frame])（ROUTER 用）
                              # identity 为空 → send(frame)（DEALER 用）
recv(role)                    # 返回 (identity, frame)；DEALER 单帧场景返回 (b"", frame)
```

### 2.3 ZAP 的 ctx 单例语义

`inproc://zeromq.zap.01` 在同一 zmq Context 上只能 bind 一次，即 **ZAP 是 context 级单例**：

- 异步侧：首次带 `auth` 的 `bind()` 创建并启动 `AsyncZAPHandler`（置 `_zap_started`），后续 bind（控制面）复用。
- 推论（源码注释明确）：**`on_auth` 回调必须在首次（数据面）bind 时提供**，后续 bind 传入的被忽略。
- 同步数据面使用**独立 `zmq.Context`**（`bind_sync_data` 内 `zmq.Context()` 新建），因此其 `SyncZAPHandler` 与异步 ZAP 互不冲突，两套 ZAP 并存。

### 2.4 客户端 connect 与 monitor

```python
connect(endpoint, role, credentials=(user, pw), monitor=True, identity=bytes)
```

- PLAIN：`plain_username/plain_password` 选项。
- `identity`：显式设置 `IDENTITY`——客户端数据/控制两个 DEALER 用同一 `client_id.encode()`，使服务端控制面写入的路由表能直接用于数据面转发（架构 D5）。
- monitor：订阅 4 类事件的 monitor socket + 每 socket 一个 `_monitor_loop` 任务，事件翻译为字符串后回调 `set_monitor_callback` 注册的异步回调：
  - `EVENT_CONNECTED` → `"connected"`
  - `EVENT_DISCONNECTED` → `"disconnected"`
  - `EVENT_HANDSHAKE_FAILED_AUTH` → `"auth_failed"`
  - `EVENT_HANDSHAKE_SUCCEEDED` → `"handshake_ok"`
  - 其他 → `"other"`

客户端借此实现「启动期认证裁定」与「运行期断线感知」，见 [05-client.md](05-client.md)。

### 2.5 close 顺序

同步数据面（线程 join）→ 取消 monitor 任务 → 关 monitor socket → 关业务 socket（linger 1000）→ 停 ZAP。

## 3. SyncDataThread（同步数据面）

### 3.1 结构

```
主线程                            数据面线程（daemon）
   │                                   │
   │ PUSH ──────inproc────────▶ PULL ──┤ Poller(ROUTER + PULL, 100ms)
   │ send_sync_data(id, frame)         │  ROUTER 可读 → NOBLOCK 批量 drain
   │                                   │    → on_message(ident, frame)   ← Server._on_data_message
   │                                   │      （回调内可调 broadcast() 直接转发）
   │                                   │  PULL 可读 → NOBLOCK 批量 drain
   │                                   │    → ROUTER.send_multipart([id, frame])
```

- 独立 `zmq.Context` + 独立线程，与异步侧完全隔离（含各自的 ZAP）。
- ROUTER 选项：`LINGER=1000`、`ROUTER_MANDATORY=1`、SNDHWM/RCVHWM。
- **批量 drain**：一次 poll 唤醒后 `recv_multipart(NOBLOCK)` 循环取到 `zmq.Again` 为止，摊薄 poll 开销。
- 100ms poll 超时用于周期性检查 `_running` 退出标志。

### 3.2 broadcast（订阅者扇出）

```python
broadcast(targets, frame_bytes, credits) -> int  # 返回丢弃数
```

逐 target：
1. `credits.get(target, -1) == 0` → 跳过并计丢弃（信用流控；**未知信用视为 -1，不拦**）；
2. `send_multipart(..., DONTWAIT)` 失败（对端 HWM 满 / ROUTER_MANDATORY 不可达）→ 计丢弃。

payload ≥ 1024B 时构造一次 `zmq.Frame(frame)` 以 `copy=False` 复用给所有 target（零拷贝）；小帧直接 send（省 Frame 包装开销）。

### 3.3 Windows 兼容

`stop()` 只 join 线程、**不 close socket**：Windows bundled libzmq 对同步 ctx socket 的 close 可能触发 signaler `Assertion failed`（源码注释）。线程依赖 Poller 100ms 轮询在 `_running=False` 后 100ms 内退出。

## 4. ZAP PLAIN 认证流程

### 4.1 AsyncZAPHandler（控制面，异步）

```
DEALER(plain) ──handshake──▶ ROUTER
                                │ zmq 内部
                                ▼
                REP bind inproc://zeromq.zap.01
                收 [version, request_id, domain, address, identity,
                    mechanism, username, password, ...]（<7 帧丢弃）
                                │
                loop.run_in_executor(None, auth.verify, user, pw)
                                │  ← bcrypt ~200ms 放线程池，防阻塞事件循环
                                ▼
                回 6 帧 [b"1.0", request_id, b"200"/b"400", b"OK"/b"INVALID",
                        user_id(成功时)/b"", b""]
                                │
                await on_auth(username, "", ok, reason)   ← 异常保护，单次失败不杀循环
```

### 4.2 SyncZAPHandler（数据面，同步线程）

- 自有 daemon 线程 + `zmq.Poller(100ms)` 轮询 REP；`verify` 在本线程直接调用（阻塞 ~200ms 不影响事件循环）。
- `on_auth` 经 `asyncio.run_coroutine_threadsafe` 调度回主 loop（构造时传入 loop 引用）。
- 同样遵循「Windows 不 close socket」策略。

### 4.3 认证失败语义

ZAP 回 400 后，客户端侧 monitor 收到 `EVENT_HANDSHAKE_FAILED_AUTH`；服务端侧 `on_auth` 记录失败事件（用户名 + reason：`user_not_found` / `invalid_password` / `user_disabled`），进入 `ConnectionStats` 事件环供监控展示。
