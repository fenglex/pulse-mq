# 认证可见性 + 心跳机制 设计文档

**日期:** 2026-06-23  
**版本:** PulseMQ v2.3.0 → v2.4.0  
**状态:** 待实现  
**原则:** pub 和 sub 最简配置即可直接使用，所有增强功能默认启用

---

## 背景与动机

当前存在两个问题：

1. **Sub 客户端收不到消息时缺乏诊断手段** — 认证失败时 subscriber 静默退
   出迭代器，无任何异常提示；连接断开也无从感知。
2. **Pub 端认证信息不可见** — ZAP handler 仅在认证失败时打 warning 日志，
   成功时不输出任何信息，且缺少客户端 IP 地址等关键字段。外部程序无法
   获知认证事件。

---

## 设计概览

四个独立模块，渐进增强：

```
┌─────────────────────────────────────────────────────────┐
│  1. Sub 端异常体系                                       │
│     AuthenticationError / ConnectionLostError            │
│     认证失败不再静默退出，显式抛异常                       │
├─────────────────────────────────────────────────────────┤
│  2. Pub 端 ZAP 日志增强                                  │
│     成功 + 失败都打日志，包含客户端 IP（从 ZAP 帧提取）     │
├─────────────────────────────────────────────────────────┤
│  3. Pub 端认证事件回调钩子                                │
│     on_auth(username, client_addr, success)              │
│     外部程序可注册自定义处理逻辑                           │
├─────────────────────────────────────────────────────────┤
│  4. 心跳机制（PING 帧）                                   │
│     Pub 定期发 PING → Sub 检测超时 → ConnectionLostError  │
│     默认启用，零配置即可工作                               │
└─────────────────────────────────────────────────────────┘
```

---

## 模块 1：Sub 端异常体系

### 新增异常类 (`subscriber.py`)

```python
class PulseSubscriberError(Exception):
    """订阅端异常基类。"""

class AuthenticationError(PulseSubscriberError):
    """PLAIN 认证被拒绝（ZAP 返回 400）。"""
    def __init__(self, message: str, username: str = "", address: str = ""):
        super().__init__(message)
        self.username = username
        self.address = address

class ConnectionLostError(PulseSubscriberError):
    """连接意外断开（心跳超时或 TCP 断开）。"""
```

### `subscribe()` 改动

- `zmq.ZMQError` 不再静默 `break`，改为 `raise ConnectionLostError`
- 新版 subscriber 连接旧版 publisher（不发心跳）时：第一个 DATA 消息到达
  后启动心跳计数器，之后数据中断超过 `heartbeat_timeout` 才抛异常；
  数据正常流动则不受影响
- 正常的 `close()` 关闭路径：通过 `_closed_by_user` 标志区分

### `connect()` 不变

ZMQ PLAIN 认证是异步的 — `connect()` 不验证凭据，第一次 `recv` 时才触发
ZAP 流程。因此 `connect()` 本身不会报认证错误。

### 文件改动

| 文件 | 改动量 |
|------|--------|
| `src/pulsemq/subscriber.py` | +45 行 |

---

## 模块 2：Pub 端 ZAP 日志增强

### 当前状态

```python
# 仅失败时有日志，无客户端信息
logger.warning("ZAP 拒绝: username=%s", username)
```

### 目标状态

```python
# 成功 → INFO
logger.info("ZAP 认证成功: username=%s client=%s", username, client_addr)

# 失败 → WARNING
logger.warning("ZAP 认证失败: username=%s client=%s reason=invalid_credentials",
               username, client_addr)
```

- `client_addr` 从 ZAP 请求帧第 4 帧（`msg[3]`）提取，ZMQ 内核自动填入
- 同时解码前注释掉的 `address` 字段

### 文件改动

| 文件 | 改动量 |
|------|--------|
| `src/pulsemq/transport/zmq_pub.py` | +8 行 |

---

## 模块 3：认证事件回调钩子

### API

```python
# 回调签名
from typing import Awaitable, Callable

AuthCallback = Callable[[str, str, bool], Awaitable[None]]
# 参数: (username, client_address, success)
```

### 使用方式

```python
async def on_auth(username: str, addr: str, success: bool) -> None:
    print(f"[AUTH] {username}@{addr} — {'成功' if success else '失败'}")

# 方式 1：构造时传入
pub = PulsePublisher(api_keys={...}, on_auth=on_auth)

# 方式 2：启动后设置
pub.set_auth_callback(on_auth)
```

### 实现链路

```
PulsePublisher.set_auth_callback(fn)
  └→ ZmqPubTransport.set_auth_callback(fn)
       └→ AsyncZAPHandler.set_auth_callback(fn)
            └→ _loop() 中 await fn(username, addr, success)
```

回调在 `_loop()` 中用 `try/except` 包裹，异常打 warning 日志后继续，不影响
认证流程。

### 文件改动

| 文件 | 改动量 |
|------|--------|
| `src/pulsemq/publisher.py` | +10 行 |
| `src/pulsemq/transport/zmq_pub.py` | +15 行 |

---

## 模块 4：心跳机制

### 协议层面

复用已定义但未使用的 `MsgType.PING = 0x02`：

| 帧 | DATA (0x01) | PING (0x02) |
|----|-------------|-------------|
| topic | 业务 topic | `"__pulse_hb__"` |
| meta (6B) | 正常编码 | msg_type=PING, flags=msgpack\|none, record_count=0 |
| timestamp (8B) | 发送时间 | 发送时间 |
| payload | 业务数据 | 空字节 |

### 4a. Pub 端 — 心跳发送

在 `PulsePublisher._run()` 中新增 `_heartbeat_loop` 协程：

```
while running:
    await asyncio.sleep(heartbeat_interval)  # 默认 30s
    frames = encode_heartbeat()
    await self._transport.send(frames)
```

新增 `encode_heartbeat()` 函数 (`protocol/frames.py`)：

```python
def encode_heartbeat() -> list[bytes]:
    """编码心跳帧（PING 类型，空载荷）。"""
    flags_byte = encode_flags("msgpack", "none")
    rc_bytes = _RC_STRUCT.pack(0)
    meta = bytes([MsgType.PING, flags_byte]) + rc_bytes
    ts_bytes = _TS_STRUCT.pack(time.time_ns())
    return [b"__pulse_hb__", meta, ts_bytes, b""]
```

### 4b. Sub 端 — 心跳检测

`subscribe()` 新增 `heartbeat_timeout` 参数：

```python
async def subscribe(
    self, *topics: str,
    heartbeat_timeout: float = 90.0,  # 默认 90s（3× heartbeat_interval）
) -> AsyncIterator[PulseMessage]:
```

逻辑：

1. 自动订阅 `"__pulse_hb__"` 内部 topic
2. 记录 `last_recv` 时间戳。**计数器从第一条消息到达时启动**（无论 PING 或
   DATA），避免连接旧版 publisher（不发心跳）时立即超时
3. `recv_multipart()` 外层包 `asyncio.wait_for(timeout=1.0)`：
   - 收到帧 → 检查 msg_type：
     - `PING` → 刷新 `last_recv`，`continue`（不 yield 给用户）
     - `DATA` → 刷新 `last_recv`，正常 `yield PulseMessage`
   - 超时 1s → 检查 `time.monotonic() - last_recv > heartbeat_timeout`：
     - 是 → `raise ConnectionLostError("心跳超时: 已 {elapsed:.0f}s 未收到消息")`
     - 否 → `continue`
4. `heartbeat_timeout=0` 时完全跳过心跳检测（显式禁用）

### 4c. 配置

`PublisherConfig` 新增字段：

```python
heartbeat_interval: float = 30.0  # 心跳发送间隔（秒），默认启用
```

### 最简配置验证

```python
# Publisher — 零配置
pub = PulsePublisher()
@pub.producer(name="test", interval=1.0)
async def test(): return {"data": 1}
pub.start()

# Subscriber — 零配置，心跳自动工作
async with PulseSubscriber("tcp://host:5555") as sub:
    async for msg in sub.subscribe("test"):  # heartbeat_timeout 默认 90s
        print(msg.payload)
```

### 文件改动

| 文件 | 改动量 |
|------|--------|
| `src/pulsemq/protocol/frames.py` | +15 行 |
| `src/pulsemq/publisher.py` | +25 行 |
| `src/pulsemq/config.py` | +2 行 |
| `src/pulsemq/subscriber.py` | +35 行 |

---

## 总体改动量估算

| 文件 | 改动量 |
|------|--------|
| `subscriber.py` | +80 行（异常类 + 心跳检测） |
| `transport/zmq_pub.py` | +23 行（日志 + 回调） |
| `publisher.py` | +35 行（心跳循环 + 回调 API） |
| `protocol/frames.py` | +15 行（encode_heartbeat） |
| `config.py` | +2 行（heartbeat_interval） |
| **合计** | **~155 行** |

---

## 测试计划

| 测试场景 | 覆盖模块 |
|----------|---------|
| 错误凭据 sub 抛 `AuthenticationError` | 模块 1 |
| 无凭据连接开启认证的 pub 后 `recv` 抛异常 | 模块 1 |
| `zmq.ZMQError` 被正确转换为 `ConnectionLostError` | 模块 1 |
| ZAP 成功/失败日志包含客户端 IP | 模块 2 |
| `on_auth` 回调被正确调用（成功/失败各一次） | 模块 3 |
| `on_auth` 回调异常不影响认证流程 | 模块 3 |
| 心跳帧端到端：pub 发送 PING，sub 接收后过滤 | 模块 4 |
| `heartbeat_timeout` 超时抛 `ConnectionLostError` | 模块 4 |
| `heartbeat_timeout=0` 显式禁用心跳 | 模块 4 |
| Sub 连接旧版 pub（无 PING）→ 第一条 DATA 启动计数器 | 模块 4 |
| 最简配置端到端（pub+sub 零配置） | 全部 |
| 向后兼容：现有测试全部通过 | 全部 |

---

## 风险与边界

- **向后兼容性：** `heartbeat_timeout` 默认 90s，从第一条消息到达开始计时。
  连接旧版 publisher 时，数据正常发送就不会超时；数据中断 90s 才触发
  `ConnectionLostError`（对旧版 pub 这也算合理的"连接异常"）。
- **PING 帧对旧版订阅者的影响：** 心跳帧 topic 为 `__pulse_hb__`，旧版
  subscriber 未订阅该 topic，不会收到 → 零影响。
- **`__pulse_hb__` topic 冲突：** 内部保留 topic 名，用户不应使用同名 topic。
  如有冲突风险，后续可加 `$` 前缀（如 `$pulse_hb`）以示系统保留。
- **认证失败检测的可靠性：** ZMQ 认证失败后 SUB 连接被断开，`recv` 会抛
  `zmq.ZMQError`。无法在应用层精确区分"认证断开"和"TCP 断开"——
  统一抛 `ConnectionLostError`。认证失败通常发生在 `connect()` 后第一次
  `recv` 时，通过快速返回可推断为认证问题。
- **心跳对带宽的影响：** 每 30 秒一条空帧（约 20 字节），可忽略。
