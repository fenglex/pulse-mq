# PulseMQ v2 · Spec 1 核心架构骨架 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 PulseMQ 从旧 PUB/SUB 架构原地重写为 Client/Server（ROUTER/DEALER）模型，跑通「发布 → 中继路由 → 订阅」全链路，强制 PLAIN 认证，数据面/控制面分离。

**Architecture:** 严格单向依赖（见 Spec 1 §3.2）：`errors ← config ← logging → protocol → transport → routing+control → server → lifecycle → client`。`transport` 是唯一 `import zmq` 的模块；`routing`/`control` 只消费 `(identity, frame)` 抽象；`client`/`server` 通过组合不碰 zmq。沿用不动：`protocol/serialization.py`、`protocol/compression.py`、`stats/traffic.py`、`stats/storage.py`、`admin/server.py`、`admin/web_ui.py`。

**Tech Stack:** Python ≥3.13、pyzmq（ROUTER/DEALER + ZAP PLAIN）、msgspec（msgpack）、loguru、pytest + pytest-asyncio。

## Global Constraints

（逐条抄自 Spec 1，所有 task 隐式遵守）

- 对外只暴露 `Client` / `Server` / `ProducerClient` / `ConsumerClient`，不暴露任何 ZeroMQ 术语。
- 数据面与控制面分离，独立端口：数据面 `tcp://0.0.0.0:5555`，控制面 `tcp://0.0.0.0:5556`。
- 强制 PLAIN 用户名/密码认证，不可关闭；Spec 1 凭据源为最简明文 dict 白名单。
- Client 启动期硬失败（连接/认证/注册任一失败立即退出，非零退出码）；运行期断线自动重连 + 重新认证 + 恢复订阅。
- 零配置优先：`Server()` / `Client()` 无参即可启动。
- 原地大改、不兼容旧版协议：帧格式重设（magic/version/msg_type/flags/data_type/topic_len/topic/ts/record_count/payload/CRC?），版本升至 `5.0.0`。
- `transport` 是唯一直接 `import zmq` 的模块。
- `routing` / `control` 不依赖 `transport` 的 zmq 细节。
- 重连/控制面逻辑不得阻塞数据面接收循环（独立 socket + 独立 asyncio 任务）。
- `TrafficStats.record()` 不引入锁；消息路径不加锁。
- Windows 上 `WindowsSelectorEventLoopPolicy`（zmq 要求）；Admin 用独立线程（Spec 3 落地，Spec 1 沿用现有 AdminServer）。
- monitor socket 必须在业务 socket 之前关闭，`LINGER=1000`。
- 关键认证事件同时 `print` 到 stderr（沿用 `_notice` 策略）+ loguru。

---

## File Structure

新文件 / 重写文件 / 删除文件一览（锁定分解决策）：

| 路径 | 动作 | 职责 |
|------|------|------|
| `src/pulsemq/errors.py` | 新增 | 统一异常体系 + 退出码 |
| `src/pulsemq/config.py` | 重写 | TOML+env，`ServerConfig`/`ClientConfig` 全默认 |
| `src/pulsemq/logging_setup.py` | 新增 | loguru 初始化 + 生命周期事件 helper |
| `src/pulsemq/protocol/msg_type.py` | 扩展 | 增 `CONTROL/HEARTBEAT/ADMIN`，移除 `PING` |
| `src/pulsemq/protocol/flags.py` | 微调 | 增 `crc` kwarg + `has_crc()`，ser/comp 编码不变 |
| `src/pulsemq/protocol/frames.py` | 重写 | 单 `bytes` 帧 `encode/decode` + `encode_control/decode_control`；`PulseMessage` 加 `msg_type` |
| `src/pulsemq/routing.py` | 新增 | `SubscriptionTable` 前缀匹配 |
| `src/pulsemq/control.py` | 新增 | `ControlCmd`/`ControlMessage`、`ClientInfo`、`OnlineRegistry` |
| `src/pulsemq/transport/router.py` | 新增 | `Transport`（ROUTER/DEALER + ZAP PLAIN + monitor） |
| `src/pulsemq/transport/zmq_pub.py` | 删除 | 旧 PUB 角色 |
| `src/pulsemq/transport/__init__.py` | 改 | 导出 `Transport` |
| `src/pulsemq/server.py` | 新增（替换 `publisher.py`） | `Server` 组装入口 |
| `src/pulsemq/publisher.py` | 删除 | 旧 publisher 角色 |
| `src/pulsemq/lifecycle.py` | 新增 | 启动/关闭顺序 + 信号处理 |
| `src/pulsemq/client.py` | 新增（替换 `subscriber.py`） | `Client`/`ProducerClient`/`ConsumerClient` |
| `src/pulsemq/subscriber.py` | 删除 | 旧 subscriber 角色 |
| `src/pulsemq/producers/manager.py` | 改 | `on_message` 调 `ProducerClient.publish` |
| `src/pulsemq/producers/types.py` | 微调 | `SenderProducerCallback` 指向 `ProducerClient` |
| `src/pulsemq/cli/__init__.py` | 新增 | CLI 包 |
| `src/pulsemq/cli/server.py` | 新增 | `python -m pulsemq` 启动 Server，退出码映射异常 |
| `src/pulsemq/__init__.py` | 重写 | 导出新公共 API + Windows 事件循环策略 |
| `src/pulsemq/_version.py` | 改 | `5.0.0` |
| `pyproject.toml` | 改 | 版本 5.0.0，脚本入口 `pulsemq`、`pulsemq-server` |
| `tests/test_frames_v2.py` | 新增 | 新帧格式往返 + magic/version/CRC |
| `tests/test_routing.py` | 新增 | 前缀匹配 + 清理 + 幂等 |
| `tests/test_control.py` | 新增 | REGISTER/ALREADY_ONLINE/心跳超时/订阅 |
| `tests/test_transport_router.py` | 新增 | ZAP PLAIN + ROUTER/DEALER 收发 |
| `tests/test_client_lifecycle.py` | 新增 | 启动失败 + 退出码 + 重连恢复 |
| `tests/test_lifecycle.py` | 新增 | 信号关闭 + 资源回收 |
| `tests/test_e2e_client_server.py` | 新增（替换 `test_e2e_publisher.py`/`test_e2e_subscriber.py`/`test_integration.py`） | 发布→中继→订阅 e2e |
| `tests/test_zap_resilience.py` | 重写 | Client 启动期认证失败 + 运行期重连认证 |
| `tests/test_publisher_shutdown.py` | 删除（被 `test_lifecycle.py` 取代） | — |
| `tests/test_protocol.py` | 改 | 删除 `TestMsgType`/`TestFrameCodec`/`TestHeartbeat`，保留 `TestDataType`/`TestFlags` |
| `tests/test_data_types.py` | 改 | 改用新 `frames.encode` 签名 |
| `tests/test_producer_types.py` | 改 | 反射目标改为 `ProducerClient` |
| `tests/conftest.py` | 改 | fixtures 指向 Server/Client |

沿用不动：`src/pulsemq/protocol/serialization.py`、`protocol/compression.py`、`stats/traffic.py`、`stats/storage.py`、`cache/topic_buffer.py`、`admin/server.py`、`admin/web_ui.py`。

---

## Task 1: errors 异常体系

**Files:**
- Create: `src/pulsemq/errors.py`
- Test: `tests/test_errors.py`

**Interfaces:**
- Produces: `PulseMQError`（基类，`exit_code=1`）、`TransportError`(2)、`ConnectionError`(2)、`AuthenticationError`(3, 带 `reason`)、`ClientStartupError`(4, 带 `reason`/`address`/`username`)、`FrameError`(5)、`SerializationError`(5)、`ConfigurationError`(6)、`ResourceExhaustedError`(7)、`exit_code_for(exc)`。

- [ ] **Step 1: Write the failing test**

```python
# tests/test_errors.py
import pytest
from pulsemq.errors import (
    PulseMQError, TransportError, ConnectionError, AuthenticationError,
    ClientStartupError, FrameError, SerializationError, ConfigurationError,
    ResourceExhaustedError, exit_code_for,
)


def test_base_exit_code():
    assert PulseMQError.exit_code == 1
    assert PulseMQError("x").exit_code == 1


@pytest.mark.parametrize("exc_cls,code", [
    (TransportError, 2),
    (ConnectionError, 2),
    (AuthenticationError, 3),
    (ClientStartupError, 4),
    (FrameError, 5),
    (SerializationError, 5),
    (ConfigurationError, 6),
    (ResourceExhaustedError, 7),
])
def test_exit_codes(exc_cls, code):
    assert exc_cls.exit_code == code


def test_authentication_error_reason():
    err = AuthenticationError("bad", reason="invalid_password")
    assert err.reason == "invalid_password"
    assert exit_code_for(err) == 3


def test_client_startup_error_fields():
    err = ClientStartupError("nope", reason="CONNECT_FAILED",
                             address="tcp://1.2.3.4:5555", username="alice")
    assert err.reason == "CONNECT_FAILED"
    assert err.address == "tcp://1.2.3.4:5555"
    assert err.username == "alice"
    assert exit_code_for(err) == 4


def test_exit_code_for_unknown_exception():
    assert exit_code_for(ValueError("x")) == 1
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_errors.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'pulsemq.errors'`

- [ ] **Step 3: Write minimal implementation**

```python
# src/pulsemq/errors.py
"""PulseMQ 统一异常体系 + 退出码。"""


class PulseMQError(Exception):
    exit_code: int = 1


class TransportError(PulseMQError):
    exit_code = 2


class ConnectionError(PulseMQError):  # 故意覆盖内置名；包内显式导入
    exit_code = 2


class AuthenticationError(PulseMQError):
    exit_code = 3

    def __init__(self, message: str, *, reason: str | None = None) -> None:
        super().__init__(message)
        self.reason = reason


class ClientStartupError(PulseMQError):
    exit_code = 4

    def __init__(self, message: str, *, reason: str | None = None,
                 address: str | None = None, username: str | None = None) -> None:
        super().__init__(message)
        self.reason = reason
        self.address = address
        self.username = username


class FrameError(PulseMQError):
    exit_code = 5


class SerializationError(PulseMQError):
    exit_code = 5


class ConfigurationError(PulseMQError):
    exit_code = 6


class ResourceExhaustedError(PulseMQError):
    exit_code = 7


def exit_code_for(exc: BaseException) -> int:
    if isinstance(exc, PulseMQError):
        return exc.exit_code
    return 1
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_errors.py -v`
Expected: PASS (7 passed)

- [ ] **Step 5: Commit**

```bash
git add src/pulsemq/errors.py tests/test_errors.py
git commit -m "feat(errors): 新增统一异常体系与退出码"
```

---

## Task 2: config 重写

**Files:**
- Create: `src/pulsemq/config.py`（覆盖旧文件）
- Test: `tests/test_config.py`

**Interfaces:**
- Consumes: `pulsemq.errors.ConfigurationError`
- Produces: `ServerConfig`（`data_endpoint`/`control_endpoint`/`admin_endpoint`/`credentials_file`/`heartbeat_timeout`/`stats_db`）、`ClientConfig`（`data_endpoint`/`control_endpoint`/`username`/`password`/`client_id`/`heartbeat_interval`/`reconnect_initial_delay`/`reconnect_max_delay`/`reconnect_backoff_multiplier`）、`load_server_config(path=None)`、`load_client_config(path=None)`。

- [ ] **Step 1: Write the failing test**

```python
# tests/test_config.py
import os
from pulsemq.config import ServerConfig, ClientConfig, load_server_config, load_client_config
from pulsemq.errors import ConfigurationError


def test_server_config_defaults():
    cfg = ServerConfig()
    assert cfg.data_endpoint == "tcp://0.0.0.0:5555"
    assert cfg.control_endpoint == "tcp://0.0.0.0:5556"
    assert cfg.admin_endpoint == "0.0.0.0:9090"
    assert cfg.heartbeat_timeout == 6.0
    assert cfg.stats_db == "sqlite://./pulsemq_stats.sqlite"


def test_client_config_defaults():
    cfg = ClientConfig()
    assert cfg.data_endpoint == "tcp://localhost:5555"
    assert cfg.control_endpoint == "tcp://localhost:5556"
    assert cfg.heartbeat_interval == 1.0
    assert cfg.reconnect_initial_delay == 1.0
    assert cfg.reconnect_max_delay == 30.0
    assert cfg.reconnect_backoff_multiplier == 2.0
    assert cfg.client_id  # 自动生成非空


def test_load_server_config_from_toml(tmp_path):
    p = tmp_path / "s.toml"
    p.write_text(
        '[server]\ndata_endpoint = "tcp://0.0.0.0:6000"\n'
        '[auth]\ntype = "plain"\n', encoding="utf-8")
    cfg = load_server_config(str(p))
    assert cfg.data_endpoint == "tcp://0.0.0.0:6000"


def test_env_override(monkeypatch):
    monkeypatch.setenv("PULSEMQ_DATA_ENDPOINT", "tcp://1.2.3.4:7000")
    cfg = load_server_config(None)
    assert cfg.data_endpoint == "tcp://1.2.3.4:7000"


def test_invalid_auth_type_rejected(tmp_path):
    p = tmp_path / "bad.toml"
    p.write_text('[auth]\ntype = "curve"\n', encoding="utf-8")
    with __import__("pytest").raises(ConfigurationError):
        load_server_config(str(p))
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_config.py -v`
Expected: FAIL — `AttributeError` / 旧 `PublisherConfig` 字段不符

- [ ] **Step 3: Write minimal implementation**

```python
# src/pulsemq/config.py
"""配置加载：TOML + 环境变量，全默认值。零配置可启动。"""
from __future__ import annotations

import os
import uuid
from dataclasses import dataclass, field
from pathlib import Path

try:
    import tomllib  # py311+
except ModuleNotFoundError:  # pragma: no cover
    import tomli as tomllib  # type: ignore

from pulsemq.errors import ConfigurationError


@dataclass
class ServerConfig:
    data_endpoint: str = "tcp://0.0.0.0:5555"
    control_endpoint: str = "tcp://0.0.0.0:5556"
    admin_endpoint: str = "0.0.0.0:9090"
    credentials_file: str = "./pulsemq_users.toml"
    heartbeat_timeout: float = 6.0
    stats_db: str = "sqlite://./pulsemq_stats.sqlite"
    stats_retention_minutes: int = 480


@dataclass
class ClientConfig:
    data_endpoint: str = "tcp://localhost:5555"
    control_endpoint: str = "tcp://localhost:5556"
    username: str = ""
    password: str = ""
    client_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    heartbeat_interval: float = 1.0
    reconnect_initial_delay: float = 1.0
    reconnect_max_delay: float = 30.0
    reconnect_backoff_multiplier: float = 2.0


def _read_toml(path: str | None) -> dict:
    if not path:
        return {}
    p = Path(path)
    if not p.exists():
        return {}
    with p.open("rb") as f:
        return tomllib.load(f)


def _env(name: str, default: str | None = None) -> str | None:
    return os.environ.get(name, default)


def load_server_config(path: str | None = None) -> ServerConfig:
    data = _read_toml(path)
    s = data.get("server", {})
    a = data.get("auth", {})
    if a.get("type", "plain") not in ("plain", "PLAIN"):
        raise ConfigurationError(f"auth.type 仅支持 plain，拒绝 {a.get('type')!r}")
    cfg = ServerConfig(
        data_endpoint=s.get("data_endpoint", ServerConfig.data_endpoint),
        control_endpoint=s.get("control_endpoint", ServerConfig.control_endpoint),
        admin_endpoint=s.get("admin_endpoint", ServerConfig.admin_endpoint),
        credentials_file=a.get("credentials_file", ServerConfig.credentials_file),
        heartbeat_timeout=float(s.get("heartbeat_timeout", ServerConfig.heartbeat_timeout)),
        stats_db=s.get("stats_db", ServerConfig.stats_db),
        stats_retention_minutes=int(s.get("stats_retention_minutes",
                                          ServerConfig.stats_retention_minutes)),
    )
    # 环境变量覆盖
    if (v := _env("PULSEMQ_DATA_ENDPOINT")):
        cfg.data_endpoint = v
    if (v := _env("PULSEMQ_CONTROL_ENDPOINT")):
        cfg.control_endpoint = v
    if (v := _env("PULSEMQ_ADMIN_BIND")):
        cfg.admin_endpoint = v
    if (v := _env("PULSEMQ_CREDENTIALS_FILE")):
        cfg.credentials_file = v
    return cfg


def load_client_config(path: str | None = None) -> ClientConfig:
    data = _read_toml(path)
    c = data.get("client", {})
    cfg = ClientConfig(
        data_endpoint=c.get("data_endpoint", ClientConfig.data_endpoint),
        control_endpoint=c.get("control_endpoint", ClientConfig.control_endpoint),
        username=c.get("username", ""),
        password=c.get("password", ""),
        heartbeat_interval=float(c.get("heartbeat_interval", ClientConfig.heartbeat_interval)),
        reconnect_initial_delay=float(c.get("reconnect_initial_delay",
                                            ClientConfig.reconnect_initial_delay)),
        reconnect_max_delay=float(c.get("reconnect_max_delay",
                                        ClientConfig.reconnect_max_delay)),
        reconnect_backoff_multiplier=float(c.get("reconnect_backoff_multiplier",
                                                 ClientConfig.reconnect_backoff_multiplier)),
    )
    if (v := _env("PULSEMQ_DATA_ENDPOINT")):
        cfg.data_endpoint = v
    if (v := _env("PULSEMQ_CONTROL_ENDPOINT")):
        cfg.control_endpoint = v
    if (v := _env("PULSEMQ_USERNAME")):
        cfg.username = v
    if (v := _env("PULSEMQ_PASSWORD")):
        cfg.password = v
    return cfg
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_config.py -v`
Expected: PASS (5 passed)

- [ ] **Step 5: Commit**

```bash
git add src/pulsemq/config.py tests/test_config.py
git commit -m "feat(config): 重写为 ServerConfig/ClientConfig，TOML+env 全默认"
```

---

## Task 3: logging_setup loguru 初始化

**Files:**
- Create: `src/pulsemq/logging_setup.py`
- Test: `tests/test_logging_setup.py`

**Interfaces:**
- Produces: `setup_logging(level="INFO", json=False)`、`logger`（loguru）、`log_event(level, event_type, **fields)`。

> 命名 `logging_setup` 而非 `logging`，避免与 stdlib `logging` 模块在 `import logging` 时混淆。

- [ ] **Step 1: Write the failing test**

```python
# tests/test_logging_setup.py
import io
from pulsemq.logging_setup import setup_logging, logger, log_event


def test_setup_logging_text(capfd):
    setup_logging(level="INFO", json=False)
    logger.info("hello")
    out = capfd.readouterr().err
    assert "hello" in out
    assert "INFO" in out


def test_log_event_emits(capfd):
    setup_logging(level="INFO", json=False)
    log_event("INFO", "CLIENT", username="alice", action="online")
    out = capfd.readouterr().err
    assert "alice" in out
    assert "CLIENT" in out
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_logging_setup.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Write minimal implementation**

```python
# src/pulsemq/logging_setup.py
"""loguru 结构化日志初始化 + 生命周期事件规范。"""
from __future__ import annotations

import sys

from loguru import logger

_CONFIGURED = False


def setup_logging(level: str = "INFO", json: bool = False) -> None:
    global _CONFIGURED
    logger.remove()
    fmt = (
        "{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {module}:{line} | {message}"
        if not json else "{message}"
    )
    serialize = json
    logger.add(sys.stderr, level=level, format=fmt, serialize=serialize, enqueue=False)
    _CONFIGURED = True


def log_event(level: str, event_type: str, **fields) -> None:
    """结构化输出一条生命周期事件。event_type ∈ AUTH/CLIENT/..."""
    parts = [f"[{event_type}]"] + [f"{k}={v}" for k, v in fields.items()]
    logger.log(level, " ".join(parts))


__all__ = ["setup_logging", "logger", "log_event"]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_logging_setup.py -v`
Expected: PASS (2 passed)

- [ ] **Step 5: Commit**

```bash
git add src/pulsemq/logging_setup.py tests/test_logging_setup.py
git commit -m "feat(logging): 新增 loguru 初始化与生命周期事件 helper"
```

---

## Task 4: protocol/msg_type + flags 扩展

**Files:**
- Modify: `src/pulsemq/protocol/msg_type.py`
- Modify: `src/pulsemq/protocol/flags.py`
- Modify: `tests/test_protocol.py`（删除 `TestMsgType`，更新以适配新常量；`TestFlags` 因 `decode_flags` 不变继续通过）
- Test: `tests/test_protocol.py`

**Interfaces:**
- Produces: `MsgType.DATA=0x01`/`CONTROL=0x02`/`HEARTBEAT=0x03`/`ADMIN=0x04`；`DataType` 不变；`encode_flags(ser, comp, *, crc=False)`、`decode_flags(byte)->(ser,comp)`、`has_crc(byte)->bool`。

- [ ] **Step 1: Write the failing test**

把 `tests/test_protocol.py` 中 `TestMsgType` 类整体替换为：

```python
class TestMsgType:
    def test_constants(self):
        from pulsemq.protocol.msg_type import MsgType
        assert MsgType.DATA == 0x01
        assert MsgType.CONTROL == 0x02
        assert MsgType.HEARTBEAT == 0x03
        assert MsgType.ADMIN == 0x04
```

并在 `TestFlags` 类末尾追加：

```python
    def test_crc_bit(self):
        from pulsemq.protocol.flags import encode_flags, decode_flags, has_crc
        base = encode_flags("msgpack", "none")
        assert has_crc(base) is False
        with_crc = encode_flags("msgpack", "none", crc=True)
        assert has_crc(with_crc) is True
        # crc 位不影响 ser/comp 解码
        assert decode_flags(with_crc) == ("msgpack", "none")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_protocol.py::TestMsgType tests/test_protocol.py::TestFlags -v`
Expected: FAIL — `MsgType.PING` 引用旧测试残留 / `crc` kwarg 不存在

（若 `TestMsgType` 旧用例引用 `MsgType.PING`，先将其删除再跑。）

- [ ] **Step 3: Write minimal implementation**

```python
# src/pulsemq/protocol/msg_type.py
"""帧类型。Spec 1：DATA/CONTROL/HEARTBEAT/ADMIN。"""


class MsgType:
    DATA = 0x01
    CONTROL = 0x02
    HEARTBEAT = 0x03
    ADMIN = 0x04


class DataType:
    UNKNOWN = 0x00
    DICT = 0x01
    DATAFRAME = 0x02
    STR = 0x03
    BYTES = 0x04
```

```python
# src/pulsemq/protocol/flags.py  —— 在现有文件基础上追加 crc 支持
# 保留现有 _SER_MAP / _COMP_MAP / encode_flags / decode_flags 不变，仅给 encode_flags 加 crc kwarg 并新增 has_crc。

_CRC_BIT = 0b1000_0000


def encode_flags(ser_fmt: str, comp: str, *, crc: bool = False) -> int:
    ser_bits = _SER_MAP.get(ser_fmt, 0b000)
    comp_bits = _COMP_MAP.get(comp, 0b00)
    val = ser_bits | (comp_bits << 3)
    if crc:
        val |= _CRC_BIT
    return val


def decode_flags(byte_val: int) -> tuple[str, str]:
    ser_bits = byte_val & 0b0000_0111
    comp_bits = (byte_val >> 3) & 0b0000_0011
    return _SER_MAP_REV.get(ser_bits, "msgpack"), _COMP_MAP_REV.get(comp_bits, "none")


def has_crc(byte_val: int) -> bool:
    return bool(byte_val & _CRC_BIT)
```

> 实施时用 Edit 在现有 `flags.py` 上精确替换 `encode_flags` 函数体并追加 `_CRC_BIT` 与 `has_crc`，不要重写整个文件以免破坏既有 `_SER_MAP` 等。

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_protocol.py::TestMsgType tests/test_protocol.py::TestFlags tests/test_protocol.py::TestDataType -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/pulsemq/protocol/msg_type.py src/pulsemq/protocol/flags.py tests/test_protocol.py
git commit -m "feat(protocol): msg_type 增 CONTROL/HEARTBEAT/ADMIN，flags 增 CRC 位"
```

---

## Task 5: protocol/frames 重写（新帧格式）

**Files:**
- Rewrite: `src/pulsemq/protocol/frames.py`
- Create: `tests/test_frames_v2.py`
- Modify: `tests/test_protocol.py`（删除 `TestFrameCodec`、`TestHeartbeat` 两类，它们依赖旧 4-frame list API）

**Interfaces:**
- Consumes: `pulsemq.protocol.serialization`（`get`）、`pulsemq.protocol.compression`（`get`）、`pulsemq.protocol.flags`（`encode_flags/decode_flags/has_crc`）、`pulsemq.protocol.msg_type`（`MsgType`/`DataType`）、`pulsemq.control`（`ControlCmd`/`ControlMessage`，Task 7 提供——本 task 先定义 `ControlMessage` 在 control 模块；为避免循环，frames 只依赖 `MsgType.CONTROL` 与 msgpack dict，不 import control）。
- Produces: `PulseMessage(topic, payload, raw_payload, record_count, timestamp_ns, serializer, compression, data_type, msg_type)`、`encode(...)->bytes`、`decode(bytes)->PulseMessage`、`encode_control(cmd, payload=None, serializer="msgpack")->bytes`、`decode_control(bytes)->ControlMessage`、`MAGIC=b"PM"`、`VERSION=0x01`。

> 依赖说明：`decode_control` 返回 `ControlMessage`，但为避免 frames↔control 循环导入，`ControlMessage` 定义在 `control.py`（Task 7），`frames.decode_control` 内部 `from pulsemq.control import ControlMessage`（函数内导入，打破循环）。本 task 测试只验 `encode_control/decode_control` 往返，先 stub：在 Task 5 内不依赖 control，把 `ControlMessage` 临时定义为 frames 内的 dataclass 也行——但为类型一致性，统一放 control.py。**决策：`ControlMessage` 放 `control.py`，frames 用函数内导入。** 因此本 task 测试需 `pulsemq.control` 存在——故 **Task 5 与 Task 7 顺序对调**：先做 Task 7（control 的 `ControlCmd`/`ControlMessage` 数据类部分），再做 Task 5 frames。见下方 Task 6/7。

（见下方调整后的顺序：Task 6 = control 数据类，Task 7 = frames。）

---

## Task 6: control 数据类（ControlCmd / ControlMessage / ClientInfo / OnlineRegistry）

**Files:**
- Create: `src/pulsemq/control.py`
- Test: `tests/test_control.py`

**Interfaces:**
- Consumes: 无业务依赖（纯数据 + 内存表）。
- Produces: `ControlCmd`（常量类）、`ControlMessage(cmd, payload)`、`ClientInfo`、`RegisterResult`（`OK`/`ALREADY_ONLINE`/`REJECTED` 常量）、`OnlineRegistry`（`register/heartbeat/unregister/sweep_timeout/snapshot`）。

> 本 task 只实现「数据类 + OnlineRegistry」，命令收发编解码在 frames（Task 7）；控制面 ROUTER 分发在 server（Task 9）。`OnlineRegistry` 不依赖 transport。

- [ ] **Step 1: Write the failing test**

```python
# tests/test_control.py
import time
from pulsemq.control import (
    ControlCmd, ControlMessage, ClientInfo, OnlineRegistry,
    RegisterResult,
)


def test_control_message_roundtrip():
    m = ControlMessage(cmd=ControlCmd.SUBSCRIBE, payload={"client_id": "c1", "topic": "a.*"})
    assert m.cmd == "SUBSCRIBE"
    assert m.payload["topic"] == "a.*"


def test_register_ok_and_already_online():
    reg = OnlineRegistry(heartbeat_timeout=6.0)
    info = ClientInfo(client_id="c1", username="alice", endpoint="1.2.3.4:1",
                      roles=["consumer"], topics=[], connected_at=time.time())
    assert reg.register(info) == RegisterResult.OK
    assert reg.register(info) == RegisterResult.ALREADY_ONLINE


def test_heartbeat_updates_last_seen():
    reg = OnlineRegistry(heartbeat_timeout=6.0)
    info = ClientInfo(client_id="c1", username="alice", endpoint="x",
                      roles=["consumer"], topics=[], connected_at=time.time())
    reg.register(info)
    before = reg._by_client["c1"].last_seen
    time.sleep(0.01)
    reg.heartbeat("c1")
    assert reg._by_client["c1"].last_seen > before


def test_sweep_timeout_returns_offline():
    reg = OnlineRegistry(heartbeat_timeout=0.0)  # 立即超时
    info = ClientInfo(client_id="c1", username="alice", endpoint="x",
                      roles=["consumer"], topics=["a.*"], connected_at=time.time())
    reg.register(info)
    swept = reg.sweep_timeout()
    assert len(swept) == 1
    assert swept[0].client_id == "c1"
    assert reg.snapshot()["clients"] == []


def test_unregister():
    reg = OnlineRegistry(heartbeat_timeout=6.0)
    info = ClientInfo(client_id="c1", username="alice", endpoint="x",
                      roles=["consumer"], topics=[], connected_at=time.time())
    reg.register(info)
    reg.unregister("c1")
    assert reg.snapshot()["clients"] == []
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_control.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Write minimal implementation**

```python
# src/pulsemq/control.py
"""控制面：命令集 + 在线用户表。不依赖 transport。"""
from __future__ import annotations

import time
from dataclasses import dataclass, field


class ControlCmd:
    REGISTER = "REGISTER"
    HEARTBEAT = "HEARTBEAT"
    SUBSCRIBE = "SUBSCRIBE"
    UNSUBSCRIBE = "UNSUBSCRIBE"
    DISCONNECT = "DISCONNECT"
    KICK = "KICK"


@dataclass
class ControlMessage:
    cmd: str
    payload: dict = field(default_factory=dict)


class RegisterResult:
    OK = "OK"
    ALREADY_ONLINE = "ALREADY_ONLINE"
    REJECTED = "REJECTED"


@dataclass
class ClientInfo:
    client_id: str
    username: str
    endpoint: str
    roles: list[str]
    topics: list[str]
    connected_at: float
    last_seen: float = 0.0


class OnlineRegistry:
    """在线用户表，key=username（单用户单在线）。"""

    def __init__(self, heartbeat_timeout: float = 6.0) -> None:
        self.heartbeat_timeout = heartbeat_timeout
        self._by_client: dict[str, ClientInfo] = {}
        self._by_user: dict[str, str] = {}  # username -> client_id

    def register(self, info: ClientInfo) -> str:
        if info.username in self._by_user:
            return RegisterResult.ALREADY_ONLINE
        if not info.last_seen:
            info.last_seen = time.time()
        self._by_client[info.client_id] = info
        self._by_user[info.username] = info.client_id
        return RegisterResult.OK

    def heartbeat(self, client_id: str) -> None:
        info = self._by_client.get(client_id)
        if info:
            info.last_seen = time.time()

    def unregister(self, client_id: str) -> None:
        info = self._by_client.pop(client_id, None)
        if info:
            self._by_user.pop(info.username, None)

    def sweep_timeout(self) -> list[ClientInfo]:
        now = time.time()
        offline = [c for c in self._by_client.values()
                   if now - c.last_seen > self.heartbeat_timeout]
        for c in offline:
            self.unregister(c.client_id)
        return offline

    def snapshot(self) -> dict:
        return {
            "clients": [
                {
                    "client_id": c.client_id, "username": c.username,
                    "endpoint": c.endpoint, "roles": list(c.roles),
                    "topics": list(c.topics), "connected_at": c.connected_at,
                    "last_seen": c.last_seen,
                }
                for c in self._by_client.values()
            ]
        }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_control.py -v`
Expected: PASS (5 passed)

- [ ] **Step 5: Commit**

```bash
git add src/pulsemq/control.py tests/test_control.py
git commit -m "feat(control): 新增命令集与在线用户表"
```

---

## Task 7: protocol/frames 重写

**Files:**
- Rewrite: `src/pulsemq/protocol/frames.py`
- Create: `tests/test_frames_v2.py`
- Modify: `tests/test_protocol.py`（删除 `TestFrameCodec`、`TestHeartbeat`）

**Interfaces:**
- Consumes: `serialization.get`、`compression.get`、`flags.encode_flags/decode_flags/has_crc`、`msg_type.MsgType/DataType`、`control.ControlMessage`（函数内导入）。
- Produces: `PulseMessage`、`encode/decode/encode_control/decode_control`、`MAGIC=b"PM"`、`VERSION=0x01`。

- [ ] **Step 1: Write the failing test**

```python
# tests/test_frames_v2.py
import struct
import pytest
from pulsemq.protocol import frames
from pulsemq.protocol.frames import PulseMessage, encode, decode, encode_control, decode_control, MAGIC, VERSION
from pulsemq.protocol.msg_type import MsgType, DataType
from pulsemq.errors import FrameError


def test_magic_and_version():
    assert MAGIC == b"PM"
    assert VERSION == 0x01


def test_encode_decode_roundtrip_dict():
    data = {"price": 12.3, "sym": "600000"}
    raw = encode("market.stock", data, serializer="msgpack")
    assert raw[:2] == MAGIC
    assert raw[2] == VERSION
    assert raw[3] == MsgType.DATA
    msg = decode(raw)
    assert isinstance(msg, PulseMessage)
    assert msg.topic == "market.stock"
    assert msg.payload == data
    assert msg.msg_type == MsgType.DATA
    assert msg.record_count == 1


def test_decode_bad_magic():
    bad = b"XX" + b"\x00" * 20
    with pytest.raises(FrameError):
        decode(bad)


def test_decode_bad_version():
    bad = MAGIC + b"\x09" + b"\x00" * 20
    with pytest.raises(FrameError):
        decode(bad)


def test_crc_roundtrip():
    data = {"x": 1}
    raw = encode("t", data, crc=True)
    msg = decode(raw)
    assert msg.payload == data


def test_crc_corruption_detected():
    raw = bytearray(encode("t", {"x": 1}, crc=True))
    raw[-1] ^= 0xFF  # 破坏 CRC
    with pytest.raises(FrameError):
        decode(bytes(raw))


def test_control_roundtrip():
    raw = encode_control("SUBSCRIBE", {"client_id": "c1", "topic": "a.*"})
    msg = decode_control(raw)
    assert msg.cmd == "SUBSCRIBE"
    assert msg.payload["topic"] == "a.*"


def test_timestamp_ns_present():
    raw = encode("t", {"x": 1}, ts_ns=1700000000_000000000)
    msg = decode(raw)
    assert msg.timestamp_ns == 1700000000_000000000


def test_record_count_field():
    raw = encode("t", {"x": 1}, record_count=42)
    msg = decode(raw)
    assert msg.record_count == 42
```

并从 `tests/test_protocol.py` 中**删除** `class TestFrameCodec` 与 `class TestHeartbeat` 整体（它们依赖旧 4-frame list API）。保留 `TestDataType`、`TestMsgType`、`TestFlags`。

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_frames_v2.py -v`
Expected: FAIL — `from pulsemq.protocol.frames import encode` 失败（旧 `encode` 返回 list）

- [ ] **Step 3: Write minimal implementation**

```python
# src/pulsemq/protocol/frames.py
"""PulseMQ v2 帧格式（单 bytes 帧）。

布局: magic(2) ver(1) msg_type(1) flags(1) data_type(1) topic_len(2 BE)
      topic(N) ts(8 BE int64 ns) record_count(4 BE uint32) payload(变长) CRC32?(4)
"""
from __future__ import annotations

import struct
import zlib
from dataclasses import dataclass
from typing import Any

from pulsemq.protocol import serialization, compression
from pulsemq.protocol.flags import encode_flags, decode_flags, has_crc
from pulsemq.protocol.msg_type import MsgType, DataType
from pulsemq.errors import FrameError, SerializationError

MAGIC = b"PM"
VERSION = 0x01

_TS = struct.Struct(">q")
_RC = struct.Struct(">I")
_TL = struct.Struct(">H")
_FIXED = struct.Struct(">2sBBB")  # magic, ver, msg_type, flags, data_type 之后用 H

# 头定长部分（不含 topic 变长）：magic(2)+ver(1)+msg_type(1)+flags(1)+data_type(1)+topic_len(2)+ts(8)+rc(4) = 20
_HEAD_BEFORE_TOPIC = struct.Struct(">2sBBBBH")  # magic ver msg_type flags data_type topic_len  (8B)
_HEAD_AFTER_TOPIC = struct.Struct(">qI")          # ts rc (12B)


@dataclass
class PulseMessage:
    topic: str
    payload: Any
    raw_payload: bytes
    record_count: int
    timestamp_ns: int
    serializer: str
    compression: str
    data_type: int = DataType.UNKNOWN
    msg_type: int = MsgType.DATA


def _encode_payload(obj: Any, serializer: str, compression_fmt: str) -> bytes:
    try:
        ser = serialization.get(serializer)
        comp = compression.get(compression_fmt)
    except KeyError as e:
        raise SerializationError(f"未注册的序列化/压缩: {e}") from e
    raw = ser.serialize(obj)
    return comp.compress(raw)


def _decode_payload(raw: bytes, serializer: str, compression_fmt: str) -> Any:
    ser = serialization.get(serializer)
    comp = compression.get(compression_fmt)
    return ser.deserialize(comp.decompress(raw))


def encode(topic: str, data: Any, *, msg_type: int = MsgType.DATA,
           serializer: str = "msgpack", compression: str = "none",
           record_count: int = 1, data_type: int = DataType.UNKNOWN,
           crc: bool = False, ts_ns: int | None = None) -> bytes:
    import time
    if record_count > 1_000_000:
        raise FrameError("record_count 超限")
    ts = ts_ns if ts_ns is not None else time.time_ns()
    topic_bytes = topic.encode("utf-8")
    if len(topic_bytes) > 65535:
        raise FrameError("topic 过长")
    payload = _encode_payload(data, serializer, compression)
    flags = encode_flags(serializer, compression, crc=crc)
    head = _HEAD_BEFORE_TOPIC.pack(MAGIC, VERSION, msg_type, flags, data_type,
                                   len(topic_bytes))
    tail = _HEAD_AFTER_TOPIC.pack(ts, record_count)
    body = head + topic_bytes + tail + payload
    if crc:
        body += struct.pack(">I", zlib.crc32(body) & 0xFFFFFFFF)
    return body


def decode(frame: bytes) -> PulseMessage:
    if len(frame) < _HEAD_BEFORE_TOPIC.size + _HEAD_AFTER_TOPIC.size:
        raise FrameError("帧过短")
    magic, ver, msg_type, flags, data_type, topic_len = _HEAD_BEFORE_TOPIC.unpack_from(frame, 0)
    if magic != MAGIC:
        raise FrameError("魔数不匹配")
    if ver != VERSION:
        raise FrameError(f"版本不支持: {ver}")
    off = _HEAD_BEFORE_TOPIC.size
    topic = frame[off:off + topic_len].decode("utf-8")
    off += topic_len
    ts, record_count = _HEAD_AFTER_TOPIC.unpack_from(frame, off)
    off += _HEAD_AFTER_TOPIC.size
    payload = frame[off:]
    crc_on = has_crc(flags)
    if crc_on:
        if len(payload) < 4:
            raise FrameError("CRC 缺失")
        body, crc_val = payload[:-4], struct.unpack(">I", payload[-4:])[0]
        if (zlib.crc32(body) & 0xFFFFFFFF) != crc_val:
            raise FrameError("CRC 校验失败")
        payload = body
    serializer, compression_fmt = decode_flags(flags)
    data = _decode_payload(payload, serializer, compression_fmt)
    return PulseMessage(
        topic=topic, payload=data, raw_payload=payload,
        record_count=record_count, timestamp_ns=ts,
        serializer=serializer, compression=compression_fmt,
        data_type=data_type, msg_type=msg_type,
    )


def encode_control(cmd: str, payload: dict | None = None,
                   serializer: str = "msgpack") -> bytes:
    return encode(cmd, payload or {}, msg_type=MsgType.CONTROL,
                  serializer=serializer, compression="none",
                  record_count=1, data_type=DataType.UNKNOWN)


def decode_control(frame: bytes) -> "ControlMessage":  # noqa: F821
    from pulsemq.control import ControlMessage  # 函数内导入，打破循环
    msg = decode(frame)
    if msg.msg_type != MsgType.CONTROL:
        raise FrameError("非 CONTROL 帧")
    return ControlMessage(cmd=msg.topic, payload=msg.payload if isinstance(msg.payload, dict) else {})


__all__ = [
    "PulseMessage", "MAGIC", "VERSION", "encode", "decode",
    "encode_control", "decode_control",
]
```

> 实施注意：现有 `frames.py` 还有 `encode_heartbeat`/`encode_payload`/`decode_payload`/`_restore_type` 等。本 task **整体覆盖**该文件为上面的实现；`encode_heartbeat` 改由控制面 `encode_control(ControlCmd.HEARTBEAT, {...})` 表达，不再保留旧函数。`admin/server.py` 与 `cache/topic_buffer.py` 若引用旧 `frames.encode` 返回 list，需在 Task 9/10 一并改（见下）。

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_frames_v2.py tests/test_protocol.py -v`
Expected: PASS（`TestDataType` 若引用旧 `encode` 返回 list，需先在 Task 10 修复；此处若失败，临时把 `TestDataType` 中 `encode(...)` 调用改为新签名——见 Task 10 一并处理。为避免阻塞，**Task 10 必须紧随 Task 7**。）

- [ ] **Step 5: Commit**

```bash
git add src/pulsemq/protocol/frames.py tests/test_frames_v2.py tests/test_protocol.py
git commit -m "feat(frames): 重写为单 bytes 帧，支持 magic/version/msg_type/CRC/control"
```

---

## Task 8: routing 订阅表

**Files:**
- Create: `src/pulsemq/routing.py`
- Test: `tests/test_routing.py`

**Interfaces:**
- Consumes: 无。
- Produces: `SubscriptionTable.subscribe/unsubscribe/remove/match/subscribers_of/snapshot`。

- [ ] **Step 1: Write the failing test**

```python
# tests/test_routing.py
from pulsemq.routing import SubscriptionTable


def test_prefix_match():
    t = SubscriptionTable()
    t.subscribe("id1", "market.stock.*")
    assert t.match("market.stock.600000") == {"id1"}
    assert t.match("market.stock.sh.600001") == {"id1"}
    assert t.match("market.bond.001") == set()


def test_exact_match():
    t = SubscriptionTable()
    t.subscribe("id1", "market.stock.600000")
    assert t.match("market.stock.600000") == {"id1"}
    assert t.match("market.stock.600001") == set()


def test_multi_pattern_one_identity():
    t = SubscriptionTable()
    t.subscribe("id1", "a.*")
    t.subscribe("id1", "b.*")
    assert t.match("a.x") == {"id1"}
    assert t.match("b.x") == {"id1"}
    assert t.subscribers_of("id1") == {"a.*", "b.*"}


def test_idempotent_subscribe():
    t = SubscriptionTable()
    t.subscribe("id1", "a.*")
    t.subscribe("id1", "a.*")
    assert t.subscribers_of("id1") == {"a.*"}


def test_remove_clears_all():
    t = SubscriptionTable()
    t.subscribe("id1", "a.*")
    t.subscribe("id1", "b.*")
    t.remove("id1")
    assert t.match("a.x") == set()
    assert t.subscribers_of("id1") == set()


def test_unsubscribe():
    t = SubscriptionTable()
    t.subscribe("id1", "a.*")
    t.subscribe("id1", "b.*")
    t.unsubscribe("id1", "a.*")
    assert t.match("a.x") == set()
    assert t.match("b.x") == {"id1"}


def test_snapshot():
    t = SubscriptionTable()
    t.subscribe("id1", "a.*")
    snap = t.snapshot()
    assert "id1" in snap
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_routing.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Write minimal implementation**

```python
# src/pulsemq/routing.py
"""topic→订阅表，前缀匹配。只由 control 面驱动，数据面只读 match()。"""
from __future__ import annotations


class SubscriptionTable:
    def __init__(self) -> None:
        # identity -> set[pattern]
        self._by_identity: dict[str, set[str]] = {}

    def subscribe(self, identity: str, topic_pattern: str) -> None:
        self._by_identity.setdefault(identity, set()).add(topic_pattern)

    def unsubscribe(self, identity: str, topic_pattern: str) -> None:
        pats = self._by_identity.get(identity)
        if pats:
            pats.discard(topic_pattern)
            if not pats:
                self._by_identity.pop(identity, None)

    def remove(self, identity: str) -> None:
        self._by_identity.pop(identity, None)

    def match(self, topic: str) -> set[str]:
        matched: set[str] = set()
        for identity, patterns in self._by_identity.items():
            for p in patterns:
                if self._matches(p, topic):
                    matched.add(identity)
                    break
        return matched

    @staticmethod
    def _matches(pattern: str, topic: str) -> bool:
        if pattern.endswith(".*"):
            prefix = pattern[:-2]
            return topic == prefix or topic.startswith(prefix + ".")
        return pattern == topic

    def subscribers_of(self, identity: str) -> set[str]:
        return set(self._by_identity.get(identity, set()))

    def snapshot(self) -> dict:
        return {k: sorted(v) for k, v in self._by_identity.items()}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_routing.py -v`
Expected: PASS (7 passed)

- [ ] **Step 5: Commit**

```bash
git add src/pulsemq/routing.py tests/test_routing.py
git commit -m "feat(routing): 新增前缀匹配订阅表"
```

---

## Task 9: transport 重写（Transport + ZAP PLAIN）

**Files:**
- Create: `src/pulsemq/transport/router.py`
- Modify: `src/pulsemq/transport/__init__.py`
- Delete: `src/pulsemq/transport/zmq_pub.py`
- Test: `tests/test_transport_router.py`

**Interfaces:**
- Consumes: `zmq.asyncio`、`pulsemq.errors`、`pulsemq.logging_setup`。
- Produces: `Transport`（`bind/connect/send/recv/close`，async）、`PlainAuthDict`（最简凭据源，`verify(user, pw)->(bool, reason)`）、`AsyncZAPHandler`（PLAIN）。

> Spec 1 §5：数据面 ROUTER bind 5555，控制面 ROUTER bind 5556；Client 侧 DEALER connect + PLAIN + monitor。`transport` 是唯一 `import zmq` 的模块。

- [ ] **Step 1: Write the failing test**

```python
# tests/test_transport_router.py
import asyncio
import pytest
import zmq
from pulsemq.transport.router import Transport, PlainAuthDict, AsyncZAPHandler
from pulsemq.protocol import frames


@pytest.fixture()
def ctx():
    c = zmq.asyncio.Context.instance()
    yield c


def test_plain_auth_dict_verify():
    auth = PlainAuthDict({"alice": "secret"})
    ok, reason = auth.verify("alice", "secret")
    assert ok is True and reason is None
    ok, reason = auth.verify("alice", "wrong")
    assert ok is False and reason == "invalid_password"
    ok, reason = auth.verify("bob", "x")
    assert ok is False and reason == "user_not_found"


async def test_router_dealer_roundtrip(ctx, monkeypatch):
    # 选随机端口
    import socket as _sock
    def _free_port():
        s = _sock.socket(_sock.AF_INET, _sock.SOCK_STREAM)
        s.bind(("127.0.0.1", 0))
        p = s.getsockname()[1]
        s.close()
        return p
    dp, cp = _free_port(), _free_port()
    data_ep = f"tcp://127.0.0.1:{dp}"
    ctrl_ep = f"tcp://127.0.0.1:{cp}"

    server = Transport(ctx=ctx)
    auth = PlainAuthDict({"alice": "secret"})
    await server.bind(data_ep, "server_ingress", auth=auth)
    await server.bind(ctrl_ep, "control", auth=auth)

    client = Transport(ctx=ctx)
    await client.connect(data_ep, "consumer", credentials=("alice", "secret"))
    await asyncio.sleep(0.2)  # 等握手 + ZAP

    frame = frames.encode("t", {"x": 1})
    await client.send(b"", frame)  # DEALER 无 identity，首帧空
    ident, recv = await asyncio.wait_for(server.recv(), timeout=2.0)
    assert recv == frame
    await client.close()
    await server.close()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_transport_router.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Write minimal implementation**

```python
# src/pulsemq/transport/router.py
"""Transport：ROUTER/DEALER + ZAP PLAIN + monitor。唯一 import zmq 的模块。"""
from __future__ import annotations

import asyncio
from typing import Awaitable, Callable

import zmq
import zmq.asyncio

from pulsemq.logging_setup import logger

AuthCallback = Callable[[str, str, bool], Awaitable[None]]


class PlainAuthDict:
    """Spec 1 最简凭据源：明文 dict 白名单。Spec 2 替换为 security.CredentialStore。"""

    def __init__(self, credentials: dict[str, str] | None = None) -> None:
        self._creds = dict(credentials or {})

    def verify(self, username: str, password: str) -> tuple[bool, str | None]:
        if username not in self._creds:
            return False, "user_not_found"
        if self._creds[username] != password:
            return False, "invalid_password"
        return True, None


class AsyncZAPHandler:
    """inproc ZAP PLAIN 认证。沿用 v3.1.1 修复：统一 await + 异常保护。"""

    def __init__(self, ctx: zmq.asyncio.Context, auth: PlainAuthDict,
                 on_auth: AuthCallback | None = None) -> None:
        self._ctx = ctx
        self._auth = auth
        self._on_auth = on_auth
        self._socket: zmq.asyncio.Socket | None = None
        self._task: asyncio.Task | None = None
        self._stopping = False

    async def start(self) -> None:
        self._socket = self._ctx.socket(zmq.REP)
        self._socket.bind("inproc://zeromq.zap.01")
        self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        self._stopping = True
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
        if self._socket:
            self._socket.close(linger=0)

    async def _loop(self) -> None:
        assert self._socket is not None
        while not self._stopping:
            try:
                msg = await self._socket.recv_multipart()
            except zmq.ContextTerminated:
                break
            except asyncio.CancelledError:
                break
            except Exception:
                continue
            await self._handle(msg)

    async def _handle(self, msg: list[bytes]) -> None:
        # ZAP 帧：version, request_id, domain, address, identity, mechanism, credentials...
        if len(msg) < 7:
            return
        request_id = msg[1]
        username = msg[6].decode("utf-8", "replace") if len(msg) > 6 else ""
        password = msg[7].decode("utf-8", "replace") if len(msg) > 7 else ""
        ok, reason = self._auth.verify(username, password)
        status = b"200" if ok else b"400"
        text = b"OK" if ok else b"INVALID"
        await self._reply(request_id, status, text, user_id=username.encode() if ok else b"")
        if self._on_auth:
            try:
                await self._on_auth(username, "", ok)
            except Exception:
                logger.exception("on_auth 回调异常")

    async def _reply(self, request_id: bytes, status: bytes, text: bytes,
                     *, user_id: bytes = b"") -> None:
        assert self._socket is not None
        frames = [b"1.0", request_id, status, text, b"", user_id]
        try:
            await self._socket.send_multipart(frames)
        except Exception:
            pass  # 单次 send 失败不杀循环


class Transport:
    """数据面/控制面 ROUTER(serve) 或 DEALER(client)。"""

    def __init__(self, ctx: zmq.asyncio.Context | None = None) -> None:
        self._ctx = ctx or zmq.asyncio.Context.instance()
        self._sockets: dict[str, zmq.asyncio.Socket] = {}
        self._zaps: list[AsyncZAPHandler] = []
        self._monitors: list[zmq.asyncio.Socket] = []
        self._monitor_tasks: list[asyncio.Task] = []
        self._on_monitor: Callable[[str], Awaitable[None]] | None = None

    def set_monitor_callback(self, cb: Callable[[str], Awaitable[None]]) -> None:
        self._on_monitor = cb

    async def bind(self, endpoint: str, role: str,
                   *, auth: PlainAuthDict | None = None) -> None:
        sock = self._ctx.socket(zmq.ROUTER)
        sock.setsockopt(zmq.LINGER, 1000)
        sock.setsockopt(zmq.ROUTER_MANDATORY, 1)
        if auth is not None:
            sock.plain_server = True
            zap = AsyncZAPHandler(self._ctx, auth)
            await zap.start()
            self._zaps.append(zap)
        sock.bind(endpoint)
        self._sockets[role] = sock

    async def connect(self, endpoint: str, role: str,
                      credentials: tuple[str, str] | None = None,
                      *, monitor: bool = True) -> None:
        sock = self._ctx.socket(zmq.DEALER)
        sock.setsockopt(zmq.LINGER, 1000)
        if credentials:
            username, password = credentials
            sock.plain_username = username.encode("utf-8")
            sock.plain_password = password.encode("utf-8")
        if monitor:
            mon = sock.get_monitor_socket(
                zmq.EVENT_CONNECTED | zmq.EVENT_DISCONNECTED
                | zmq.EVENT_HANDSHAKE_FAILED_AUTH | zmq.EVENT_HANDSHAKE_SUCCEEDED
            )
            self._monitors.append(mon)
            self._monitor_tasks.append(asyncio.create_task(self._monitor_loop(mon)))
        sock.connect(endpoint)
        self._sockets[role] = sock

    async def _monitor_loop(self, mon: zmq.asyncio.Socket) -> None:
        import struct
        while True:
            try:
                evt = await mon.recv_multipart()
            except (asyncio.CancelledError, zmq.ContextTerminated):
                break
            except Exception:
                continue
            # pyzmq monitor 帧：[event:uint16, address:bytes]
            event = 0
            if evt and len(evt[0]) >= 2:
                event = struct.unpack("=H", evt[0][:2])[0]
            kind = (
                "connected" if event == zmq.EVENT_CONNECTED
                else "disconnected" if event == zmq.EVENT_DISCONNECTED
                else "auth_failed" if event == zmq.EVENT_HANDSHAKE_FAILED_AUTH
                else "other"
            )
            if self._on_monitor:
                try:
                    await self._on_monitor(kind)
                except Exception:
                    logger.exception("monitor 回调异常")

    async def send(self, identity: bytes, frame_bytes: bytes, *, role: str = "server_ingress") -> None:
        sock = self._sockets[role]
        if identity:
            await sock.send_multipart([identity, frame_bytes])
        else:
            await sock.send(frame_bytes)

    async def recv(self, role: str = "server_ingress") -> tuple[bytes, bytes]:
        sock = self._sockets[role]
        parts = await sock.recv_multipart()
        if len(parts) == 2:
            return parts[0], parts[1]
        # DEALER 收到单帧
        return b"", parts[0]

    async def close(self) -> None:
        # monitor 先于业务 socket 关闭
        for t in self._monitor_tasks:
            t.cancel()
        for t in self._monitor_tasks:
            try:
                await t
            except Exception:
                pass
        self._monitor_tasks.clear()
        for m in self._monitors:
            m.close(linger=0)
        self._monitors.clear()
        for s in self._sockets.values():
            s.close(linger=1000)
        self._sockets.clear()
        for z in self._zaps:
            await z.stop()
        self._zaps.clear()
```

```python
# src/pulsemq/transport/__init__.py
from pulsemq.transport.router import Transport, PlainAuthDict, AsyncZAPHandler

__all__ = ["Transport", "PlainAuthDict", "AsyncZAPHandler"]
```

删除 `src/pulsemq/transport/zmq_pub.py`。

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_transport_router.py -v`
Expected: PASS (2 passed)

- [ ] **Step 5: Commit**

```bash
git add src/pulsemq/transport/router.py src/pulsemq/transport/__init__.py tests/test_transport_router.py
git rm src/pulsemq/transport/zmq_pub.py
git commit -m "feat(transport): 重写为 ROUTER/DEALER Transport + ZAP PLAIN"
```

---

## Task 10: 适配 data_types / cache / admin 对新 frames API 的引用

**Files:**
- Modify: `tests/test_data_types.py`
- Modify: `src/pulsemq/cache/topic_buffer.py`（若其 `append` 接收 `list[bytes]`，改为接收 `bytes`）
- Modify: `src/pulsemq/admin/server.py`（若引用旧 `frames.encode` 返回 list 的地方）
- Modify: `tests/test_stats.py`（`TopicBuffer.append` 调用）
- Test: `tests/test_data_types.py`、`tests/test_stats.py`

**Interfaces:**
- Consumes: 新 `frames.encode(...)->bytes`。
- Produces: `TopicBuffer.append(timestamp_ns, frame_bytes: bytes, record_count=1)`。

- [ ] **Step 1: Write the failing test（确认现状）**

Run: `pytest tests/test_data_types.py tests/test_stats.py -v`
Expected: FAIL — 旧用例调用 `frames.encode(...)` 期望 `list[bytes]`，现在返回 `bytes`；`TopicBuffer.append` 签名不符。

- [ ] **Step 2: 修改 TopicBuffer 签名**

把 `src/pulsemq/cache/topic_buffer.py` 中 `TopicBuffer.append(self, timestamp_ns, frames: list[bytes], record_count=1)` 改为：

```python
    def append(self, timestamp_ns: int, frame_bytes: bytes, record_count: int = 1) -> None:
        while self._total_records + record_count > self._max_records and len(self._buf) > 1:
            ev = self._buf.popleft()
            self._total_records -= ev.record_count
        self._buf.append(CachedMessage(timestamp_ns, frame_bytes, record_count))
        self._total_records += record_count
```

`CachedMessage.frames` 字段重命名为 `frame: bytes`（或保留 `frames` 但存 `bytes`）。为减少改动，保留字段名 `frames` 但类型改 `bytes`：

```python
@dataclass
class CachedMessage:
    timestamp_ns: int
    frames: bytes            # 原 list[bytes]，现单 bytes
    record_count: int = 1
```

`snapshot` 返回不变（只透传）。

- [ ] **Step 3: 更新 test_data_types.py 与 test_stats.py**

把所有 `frames.encode(topic, data, ...)` 返回值的使用从「list 取索引」改为「直接用 bytes」。典型改动：`TopicBuffer.append(ts, frames.encode(...))`（原本可能 `frames.encode(...)` 返回 list 再 append）。

逐处搜索 `tests/test_stats.py`、`tests/test_data_types.py` 中 `.encode(` 与 `decode(` 调用，把 `decode(frames)` 中 `frames` 由 list 改为 bytes。例：

```python
# 旧
raw = frames.encode("t", {"x": 1})
msg = frames.decode(raw)
# 新（签名已变，调用形式一致；decode 入参由 list->bytes）
```

由于 `decode` 签名已从 `decode(frames: list[bytes])` 变为 `decode(frame: bytes)`，凡测试里 `decode([a,b,c,d])` 形式都要改成 `decode(raw_bytes)`。用 Grep 定位后逐一改。

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_data_types.py tests/test_stats.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/pulsemq/cache/topic_buffer.py src/pulsemq/admin/server.py tests/test_data_types.py tests/test_stats.py
git commit -m "refactor(cache,admin,tests): 适配新单 bytes frames API"
```

---

## Task 11: server 组装

**Files:**
- Create: `src/pulsemq/server.py`
- Test: `tests/test_server.py`

**Interfaces:**
- Consumes: `transport.Transport`/`PlainAuthDict`、`routing.SubscriptionTable`、`control.OnlineRegistry`/`ClientInfo`/`ControlCmd`/`RegisterResult`、`protocol.frames`、`stats.traffic.TrafficStats`、`stats.storage.StatsStorage`、`admin.server.AdminServer`、`config.ServerConfig`、`logging_setup`。
- Produces: `Server(data_endpoint=, control_endpoint=, admin_endpoint=, credentials=, config=)`，`async start()`、`async wait_for_shutdown()`、`async stop()`。

> `Server` 组装 transport + routing + control + stats + admin；运行数据面接收循环、控制面循环、心跳扫描循环、分钟滚动循环。不持久化消息，内存转发。

- [ ] **Step 1: Write the failing test**

```python
# tests/test_server.py
import asyncio
import socket as _sock
import pytest
from pulsemq.server import Server
from pulsemq.transport.router import PlainAuthDict


def _free_port():
    s = _sock.socket(_sock.AF_INET, _sock.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


async def test_server_start_stop():
    dp, cp, ap = _free_port(), _free_port(), _free_port()
    srv = Server(data_endpoint=f"tcp://127.0.0.1:{dp}",
                 control_endpoint=f"tcp://127.0.0.1:{cp}",
                 admin_endpoint=f"127.0.0.1:{ap}",
                 credentials={"alice": "secret"})
    task = asyncio.create_task(srv.start())
    await asyncio.sleep(0.5)
    assert srv._running is True
    await srv.stop()
    await task
    assert srv._running is False


async def test_server_routes_data_to_subscriber():
    dp, cp, ap = _free_port(), _free_port(), _free_port()
    srv = Server(data_endpoint=f"tcp://127.0.0.1:{dp}",
                 control_endpoint=f"tcp://127.0.0.1:{cp}",
                 admin_endpoint=f"127.0.0.1:{ap}",
                 credentials={"alice": "secret", "bob": "pw"})
    await srv.start()
    try:
        from pulsemq.client import ConsumerClient, ProducerClient
        consumer = ConsumerClient(data_endpoint=f"tcp://127.0.0.1:{dp}",
                                  control_endpoint=f"tcp://127.0.0.1:{cp}",
                                  username="alice", password="secret")
        producer = ProducerClient(data_endpoint=f"tcp://127.0.0.1:{dp}",
                                  control_endpoint=f"tcp://127.0.0.1:{cp}",
                                  username="bob", password="pw")
        await consumer.start()
        await producer.start()
        received = []
        await consumer.subscribe("market.stock.*", lambda m: received.append(m))
        await asyncio.sleep(0.3)
        await producer.publish("market.stock.600000", {"price": 12.3})
        await asyncio.sleep(0.5)
        assert len(received) == 1
        assert received[0].payload == {"price": 12.3}
        await consumer.stop()
        await producer.stop()
    finally:
        await srv.stop()
```

> 第二个测试依赖 `Client`（Task 12）。**Task 11 的 Step 1 只跑 `test_server_start_stop`**，第二个测试在 Task 12 完成后解锁。

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_server.py::test_server_start_stop -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'pulsemq.server'`

- [ ] **Step 3: Write minimal implementation**

```python
# src/pulsemq/server.py
"""Server：组装 transport+routing+control+stats+admin。内存转发，不持久化消息。"""
from __future__ import annotations

import asyncio
import time

import zmq.asyncio

from pulsemq.config import ServerConfig, load_server_config
from pulsemq.control import (ClientInfo, ControlCmd, ControlMessage, OnlineRegistry,
                             RegisterResult)
from pulsemq.logging_setup import log_event, logger
from pulsemq.protocol import frames
from pulsemq.protocol.msg_type import MsgType
from pulsemq.routing import SubscriptionTable
from pulsemq.stats.storage import StatsStorage
from pulsemq.stats.traffic import TrafficStats
from pulsemq.transport.router import PlainAuthDict, Transport


class Server:
    def __init__(self, data_endpoint: str = "tcp://0.0.0.0:5555",
                 control_endpoint: str = "tcp://0.0.0.0:5556",
                 admin_endpoint: str = "0.0.0.0:9090",
                 credentials: dict[str, str] | None = None,
                 config: ServerConfig | None = None) -> None:
        self._cfg = config or load_server_config(None)
        self._data_endpoint = data_endpoint or self._cfg.data_endpoint
        self._control_endpoint = control_endpoint or self._cfg.control_endpoint
        self._admin_endpoint = admin_endpoint or self._cfg.admin_endpoint
        self._auth = PlainAuthDict(credentials or {"admin": "admin"})
        self._transport = Transport()
        self._routing = SubscriptionTable()
        self._registry = OnlineRegistry(heartbeat_timeout=self._cfg.heartbeat_timeout)
        self._stats = TrafficStats(retention_minutes=self._cfg.stats_retention_minutes)
        self._storage = StatsStorage(self._cfg.stats_db)
        self._admin = None  # AdminServer 延迟构造（沿用现有）
        self._tasks: list[asyncio.Task] = []
        self._running = False
        self._stop = asyncio.Event()

    async def start(self) -> None:
        self._storage.connect()
        await self._transport.bind(self._data_endpoint, "server_ingress", auth=self._auth)
        await self._transport.bind(self._control_endpoint, "control", auth=self._auth)
        self._running = True
        self._tasks = [
            asyncio.create_task(self._data_loop()),
            asyncio.create_task(self._control_loop()),
            asyncio.create_task(self._heartbeat_sweep_loop()),
            asyncio.create_task(self._minute_roll_loop()),
        ]
        logger.info("Server 启动完成 data={} control={}", self._data_endpoint, self._control_endpoint)

    async def _data_loop(self) -> None:
        while self._running:
            try:
                ident, frame_bytes = await self._transport.recv("server_ingress")
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("数据面 recv 异常")
                continue
            try:
                msg = frames.decode(frame_bytes)
            except Exception:
                continue
            self._stats.record(msg.topic, msg.record_count, len(msg.raw_payload))
            for target in self._routing.match(msg.topic):
                await self._transport.send(target, frame_bytes, role="server_ingress")

    async def _control_loop(self) -> None:
        while self._running:
            try:
                ident, frame_bytes = await self._transport.recv("control")
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("控制面 recv 异常")
                continue
            try:
                cmd_msg = frames.decode_control(frame_bytes)
            except Exception:
                continue
            await self._dispatch_control(ident, cmd_msg)

    async def _dispatch_control(self, ident: bytes, cmd_msg: ControlMessage) -> None:
        cid = cmd_msg.payload.get("client_id", "")
        if cmd_msg.cmd == ControlCmd.REGISTER:
            info = ClientInfo(
                client_id=cid, username=cmd_msg.payload.get("username", ""),
                endpoint=cmd_msg.payload.get("endpoint", ""),
                roles=cmd_msg.payload.get("roles", []),
                topics=cmd_msg.payload.get("topics", []),
                connected_at=time.time(),
            )
            result = self._registry.register(info)
            reply = frames.encode_control(cmd_msg.cmd, {"result": result})
            await self._transport.send(ident, reply, role="control")
            log_event("INFO", "CLIENT", username=info.username, action="register", result=result)
        elif cmd_msg.cmd == ControlCmd.HEARTBEAT:
            self._registry.heartbeat(cid)
            await self._transport.send(ident, frames.encode_control(cmd_msg.cmd, {"result": "OK"}), role="control")
        elif cmd_msg.cmd == ControlCmd.SUBSCRIBE:
            pattern = cmd_msg.payload.get("topic", "")
            self._routing.subscribe(ident.decode("utf-8", "replace"), pattern)
            await self._transport.send(ident, frames.encode_control(cmd_msg.cmd, {"result": "OK"}), role="control")
        elif cmd_msg.cmd == ControlCmd.UNSUBSCRIBE:
            pattern = cmd_msg.payload.get("topic", "")
            self._routing.unsubscribe(ident.decode("utf-8", "replace"), pattern)
            await self._transport.send(ident, frames.encode_control(cmd_msg.cmd, {"result": "OK"}), role="control")
        elif cmd_msg.cmd == ControlCmd.DISCONNECT:
            self._registry.unregister(cid)
            self._routing.remove(ident.decode("utf-8", "replace"))
            await self._transport.send(ident, frames.encode_control(cmd_msg.cmd, {"result": "OK"}), role="control")

    async def _heartbeat_sweep_loop(self) -> None:
        while self._running:
            await asyncio.sleep(1.0)
            try:
                offline = self._registry.sweep_timeout()
                for c in offline:
                    self._routing.remove(c.client_id)
                    log_event("WARNING", "CLIENT", username=c.username, reason="heartbeat_timeout")
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("心跳扫描异常")

    async def _minute_roll_loop(self) -> None:
        while self._running:
            await asyncio.sleep(60.0)
            try:
                archived = self._stats.roll_minute()
                self._storage.save_minutes_batch(archived)
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("分钟归档异常")

    async def wait_for_shutdown(self) -> None:
        await self._stop.wait()

    async def stop(self) -> None:
        self._running = False
        self._stop.set()
        for t in self._tasks:
            t.cancel()
        for t in self._tasks:
            try:
                await t
            except Exception:
                pass
        try:
            archived = self._stats.roll_minute()
            self._storage.save_minutes_batch(archived)
        except Exception:
            pass
        await self._transport.close()
        self._storage.close()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_server.py::test_server_start_stop -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/pulsemq/server.py tests/test_server.py
git commit -m "feat(server): 新增 Server 组装入口与运行任务"
```

---

## Task 12: client（Client/ProducerClient/ConsumerClient）

**Files:**
- Create: `src/pulsemq/client.py`
- Test: `tests/test_client_lifecycle.py`

**Interfaces:**
- Consumes: `transport.Transport`、`protocol.frames`、`control.ControlCmd`、`config.ClientConfig`、`errors`、`logging_setup`。
- Produces: `Client`、`ProducerClient(Client)`、`ConsumerClient(Client)`，`require_connected`/`require_registered` 装饰器，`on_connected/on_disconnected/on_reconnecting` 回调。

- [ ] **Step 1: Write the failing test**

```python
# tests/test_client_lifecycle.py
import asyncio
import socket as _sock
import pytest
from pulsemq.server import Server
from pulsemq.client import ConsumerClient, ProducerClient
from pulsemq.errors import AuthenticationError, ClientStartupError


def _free_port():
    s = _sock.socket(_sock.AF_INET, _sock.SOCK_STREAM)
    s.bind(("127.0.0.1", 0)); p = s.getsockname()[1]; s.close(); return p


async def _start_server(creds):
    dp, cp, ap = _free_port(), _free_port(), _free_port()
    srv = Server(data_endpoint=f"tcp://127.0.0.1:{dp}",
                 control_endpoint=f"tcp://127.0.0.1:{cp}",
                 admin_endpoint=f"127.0.0.1:{ap}", credentials=creds)
    await srv.start()
    return srv, dp, cp


async def test_publish_subscribe_roundtrip():
    srv, dp, cp = await _start_server({"alice": "s", "bob": "p"})
    try:
        c = ConsumerClient(data_endpoint=f"tcp://127.0.0.1:{dp}",
                           control_endpoint=f"tcp://127.0.0.1:{cp}",
                           username="alice", password="s")
        p = ProducerClient(data_endpoint=f"tcp://127.0.0.1:{dp}",
                           control_endpoint=f"tcp://127.0.0.1:{cp}",
                           username="bob", password="p")
        await c.start(); await p.start()
        got = []
        await c.subscribe("market.stock.*", lambda m: got.append(m))
        await asyncio.sleep(0.3)
        await p.publish("market.stock.600000", {"price": 12.3})
        await asyncio.sleep(0.5)
        assert len(got) == 1 and got[0].payload == {"price": 12.3}
        await c.stop(); await p.stop()
    finally:
        await srv.stop()


async def test_auth_failure_exits():
    srv, dp, cp = await _start_server({"alice": "s"})
    try:
        c = ConsumerClient(data_endpoint=f"tcp://127.0.0.1:{dp}",
                           control_endpoint=f"tcp://127.0.0.1:{cp}",
                           username="alice", password="WRONG")
        with pytest.raises(AuthenticationError):
            await c.start()
    finally:
        await srv.stop()


async def test_connect_failure_when_server_down():
    dp = _free_port()
    c = ConsumerClient(data_endpoint=f"tcp://127.0.0.1:{dp}",
                       control_endpoint=f"tcp://127.0.0.1:{_free_port()}",
                       username="a", password="b")
    with pytest.raises(ClientStartupError):
        await c.start()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_client_lifecycle.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Write minimal implementation**

```python
# src/pulsemq/client.py
"""Client/ProducerClient/ConsumerClient。启动硬失败 + 运行期重连。"""
from __future__ import annotations

import asyncio
import functools
import uuid
from typing import Any, Awaitable, Callable

import zmq.asyncio

from pulsemq.control import ControlCmd
from pulsemq.errors import (AuthenticationError, ClientStartupError, ConnectionError)
from pulsemq.logging_setup import log_event, logger
from pulsemq.protocol import frames
from pulsemq.protocol.msg_type import MsgType
from pulsemq.transport.router import Transport


def require_connected(func):
    @functools.wraps(func)
    async def wrapper(self, *args, **kwargs):
        if not self._connected or not self._authenticated:
            raise ConnectionError("Client 未连接或认证失败，无法执行操作")
        return await func(self, *args, **kwargs)
    return wrapper


def require_registered(func):
    @functools.wraps(func)
    async def wrapper(self, *args, **kwargs):
        if not self._registered:
            raise ConnectionError("Client 未注册")
        return await func(self, *args, **kwargs)
    return wrapper


class Client:
    def __init__(self, data_endpoint: str = "tcp://localhost:5555",
                 control_endpoint: str = "tcp://localhost:5556",
                 username: str = "", password: str = "",
                 client_id: str | None = None) -> None:
        self._data_endpoint = data_endpoint
        self._control_endpoint = control_endpoint
        self._username = username
        self._password = password
        self._client_id = client_id or uuid.uuid4().hex
        self._transport = Transport()
        self._connected = False
        self._authenticated = False
        self._registered = False
        self._subscriptions: dict[str, Callable] = {}  # pattern -> callback
        self._recv_task: asyncio.Task | None = None
        self._hb_task: asyncio.Task | None = None
        self._stop = asyncio.Event()
        self.on_connected: Callable[[], Awaitable[None]] | None = None
        self.on_disconnected: Callable[[], Awaitable[None]] | None = None
        self.on_reconnecting: Callable[[], Awaitable[None]] | None = None

    async def start(self) -> None:
        await self._connect_and_register()
        self._recv_task = asyncio.create_task(self._recv_loop())
        self._hb_task = asyncio.create_task(self._heartbeat_loop())

    async def _connect_and_register(self) -> None:
        creds = (self._username, self._password) if self._username else None
        try:
            await self._transport.connect(self._data_endpoint, "consumer", credentials=creds)
        except Exception as e:
            raise ClientStartupError(str(e), reason="CONNECT_FAILED",
                                     address=self._data_endpoint, username=self._username) from e
        # PLAIN 认证由 ZAP 在握手期完成；等握手结果
        await asyncio.sleep(0.2)
        self._connected = True
        self._authenticated = True  # 简化：握手成功即认证成功；失败由 ZAP 拒绝→recv 无数据
        # 注：真正的认证失败检测在 monitor/recv，此处简化；Task 13 的 zap_resilience 覆盖
        try:
            await self._transport.connect(self._control_endpoint, "control", credentials=creds)
        except Exception as e:
            raise ClientStartupError(str(e), reason="CONTROL_CONNECT_FAILED",
                                     address=self._control_endpoint, username=self._username) from e
        await self._register()
        # 恢复既有订阅
        for pattern in list(self._subscriptions):
            await self._send_subscribe(pattern)

    async def _register(self) -> None:
        from pulsemq.protocol.frames import encode_control, decode_control
        req = encode_control(ControlCmd.REGISTER, {
            "client_id": self._client_id, "username": self._username,
            "endpoint": self._data_endpoint, "roles": [], "topics": list(self._subscriptions),
        })
        await self._transport.send(b"", req, role="control")
        try:
            _, reply = await asyncio.wait_for(self._transport.recv("control"), timeout=3.0)
        except asyncio.TimeoutError as e:
            raise ClientStartupError("REGISTER 超时", reason="REGISTER_REJECTED",
                                     address=self._control_endpoint, username=self._username) from e
        msg = decode_control(reply)
        result = msg.payload.get("result", "")
        if result != "OK":
            raise ClientStartupError(f"REGISTER 被拒: {result}", reason=result,
                                     address=self._control_endpoint, username=self._username)
        self._registered = True
        log_event("INFO", "CLIENT", username=self._username, action="online")

    async def _send_subscribe(self, pattern: str) -> None:
        req = frames.encode_control(ControlCmd.SUBSCRIBE,
                                    {"client_id": self._client_id, "topic": pattern})
        await self._transport.send(b"", req, role="control")
        try:
            await asyncio.wait_for(self._transport.recv("control"), timeout=2.0)
        except asyncio.TimeoutError:
            pass

    @require_connected
    async def subscribe(self, topic_pattern: str, callback: Callable) -> None:
        self._subscriptions[topic_pattern] = callback
        await self._send_subscribe(topic_pattern)

    @require_connected
    async def publish(self, topic: str, data: Any) -> None:
        frame = frames.encode(topic, data)
        await self._transport.send(b"", frame, role="consumer")

    async def _recv_loop(self) -> None:
        while not self._stop.is_set():
            try:
                _, frame_bytes = await self._transport.recv("consumer")
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("client recv 异常")
                continue
            try:
                msg = frames.decode(frame_bytes)
            except Exception:
                continue
            for pattern, cb in self._subscriptions.items():
                if _matches(pattern, msg.topic):
                    try:
                        await cb(msg) if asyncio.iscoroutinefunction(cb) else cb(msg)
                    except Exception:
                        logger.exception("回调异常")

    async def _heartbeat_loop(self) -> None:
        while not self._stop.is_set():
            try:
                hb = frames.encode_control(ControlCmd.HEARTBEAT, {"client_id": self._client_id})
                await self._transport.send(b"", hb, role="control")
            except Exception:
                pass
            await asyncio.sleep(1.0)

    async def stop(self) -> None:
        self._stop.set()
        for t in (self._recv_task, self._hb_task):
            if t:
                t.cancel()
        for t in (self._recv_task, self._hb_task):
            if t:
                try:
                    await t
                except Exception:
                    pass
        try:
            disc = frames.encode_control(ControlCmd.DISCONNECT, {"client_id": self._client_id})
            await self._transport.send(b"", disc, role="control")
        except Exception:
            pass
        await self._transport.close()


def _matches(pattern: str, topic: str) -> bool:
    if pattern.endswith(".*"):
        prefix = pattern[:-2]
        return topic == prefix or topic.startswith(prefix + ".")
    return pattern == topic


class ProducerClient(Client):
    """只发布。屏蔽 subscribe。"""

    async def subscribe(self, topic_pattern: str, callback: Callable) -> None:  # type: ignore[override]
        raise NotImplementedError("ProducerClient 不支持订阅")


class ConsumerClient(Client):
    """只订阅。屏蔽 publish。"""

    async def publish(self, topic: str, data: Any) -> None:  # type: ignore[override]
        raise NotImplementedError("ConsumerClient 不支持发布")
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_client_lifecycle.py tests/test_server.py -v`
Expected: PASS（含 Task 11 的 `test_server_routes_data_to_subscriber`）

- [ ] **Step 5: Commit**

```bash
git add src/pulsemq/client.py tests/test_client_lifecycle.py
git commit -m "feat(client): 新增 Client/ProducerClient/ConsumerClient + 硬失败 + 订阅"
```

---

## Task 13: lifecycle 信号处理 + CLI 入口 + 版本 + __init__ 导出

**Files:**
- Create: `src/pulsemq/lifecycle.py`
- Create: `src/pulsemq/cli/__init__.py`
- Create: `src/pulsemq/cli/server.py`
- Modify: `src/pulsemq/_version.py`（`5.0.0`）
- Modify: `src/pulsemq/__init__.py`
- Modify: `pyproject.toml`
- Delete: `src/pulsemq/publisher.py`、`src/pulsemq/subscriber.py`
- Test: `tests/test_lifecycle.py`

**Interfaces:**
- Consumes: `server.Server`、`errors.exit_code_for`、`logging_setup`。
- Produces: `lifecycle.run_server(server)`（信号→优雅关闭）、`cli.server.main()`、`pulsemq.__version__="5.0.0"`、公共导出 `Client/Server/ProducerClient/ConsumerClient/PulseMessage`。

- [ ] **Step 1: Write the failing test**

```python
# tests/test_lifecycle.py
import asyncio
import signal
import pytest
from pulsemq import __version__
from pulsemq.lifecycle import run_server
from pulsemq.server import Server


def test_version_bumped():
    assert __version__ == "5.0.0"


async def test_run_server_handles_sigint(monkeypatch):
    # 不真发信号；直接测 run_server 在 stop 后退出
    import socket as _sock
    def _fp():
        s = _sock.socket(_sock.AF_INET, _sock.SOCK_STREAM)
        s.bind(("127.0.0.1", 0)); p = s.getsockname()[1]; s.close(); return p
    srv = Server(data_endpoint=f"tcp://127.0.0.1:{_fp()}",
                 control_endpoint=f"tcp://127.0.0.1:{_fp()}",
                 admin_endpoint=f"127.0.0.1:{_fp()}",
                 credentials={"a": "b"})

    async def _stop_after(coro_srv):
        await asyncio.sleep(0.2)
        await coro_srv.stop()

    srv_task = asyncio.create_task(run_server(srv))
    asyncio.create_task(_stop_after(srv))
    await asyncio.wait_for(srv_task, timeout=3.0)  # 应正常返回
```

> 注：用 `_fp()` 取真实空闲端口，避免 `Server` 固定 endpoint 字符串与端口 0 不兼容。

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_lifecycle.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'pulsemq.lifecycle'`

- [ ] **Step 3: Write minimal implementation**

```python
# src/pulsemq/lifecycle.py
"""统一启动顺序与优雅关闭 + 信号处理。"""
from __future__ import annotations

import asyncio
import signal

from pulsemq.logging_setup import logger


async def run_server(server) -> int:
    """启动 Server，监听 SIGINT/SIGTERM，优雅关闭。返回退出码。"""
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _on_signal():
        logger.info("收到终止信号，开始优雅关闭")
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _on_signal)
        except NotImplementedError:
            # Windows 不支持 add_signal_handler，回退
            pass

    await server.start()
    await stop_event.wait()
    await server.stop()
    return 0
```

```python
# src/pulsemq/cli/__init__.py
```

```python
# src/pulsemq/cli/server.py
"""CLI 入口：python -m pulsemq 启动 Server。"""
from __future__ import annotations

import asyncio
import sys

from pulsemq.errors import PulseMQError, exit_code_for
from pulsemq.lifecycle import run_server
from pulsemq.logging_setup import setup_logging
from pulsemq.server import Server


def main() -> int:
    setup_logging()
    try:
        server = Server()
        return asyncio.run(run_server(server))
    except PulseMQError as e:
        print(f"[FATAL] {e}", file=sys.stderr)
        return exit_code_for(e)
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    sys.exit(main())
```

```python
# src/pulsemq/_version.py
__version__ = "5.0.0"
```

```python
# src/pulsemq/__init__.py
"""PulseMQ v2 — Client/Server 模型消息系统。"""
import sys

if sys.platform == "win32":
    import asyncio
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

from pulsemq._version import __version__
from pulsemq.client import Client, ConsumerClient, ProducerClient
from pulsemq.protocol.frames import PulseMessage
from pulsemq.server import Server

__all__ = [
    "Client", "ProducerClient", "ConsumerClient", "Server",
    "PulseMessage", "__version__",
]
```

`pyproject.toml` 改动：
- `version = "5.0.0"`
- `[project.scripts]` 改为：
  ```
  pulsemq = "pulsemq.cli.server:main"
  pulsemq-server = "pulsemq.cli.server:main"
  ```

删除 `src/pulsemq/publisher.py`、`src/pulsemq/subscriber.py`。

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_lifecycle.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/pulsemq/lifecycle.py src/pulsemq/cli/__init__.py src/pulsemq/cli/server.py \
        src/pulsemq/_version.py src/pulsemq/__init__.py pyproject.toml tests/test_lifecycle.py
git rm src/pulsemq/publisher.py src/pulsemq/subscriber.py
git commit -m "feat(lifecycle,cli): 新增信号关闭+CLI 入口，版本升至 5.0.0，删除旧角色"
```

---

## Task 14: producers 适配 + 旧测试清理

**Files:**
- Modify: `src/pulsemq/producers/manager.py`、`src/pulsemq/producers/types.py`
- Modify: `tests/test_producer_types.py`
- Delete: `tests/test_e2e_publisher.py`、`tests/test_e2e_subscriber.py`、`tests/test_integration.py`、`tests/test_publisher_shutdown.py`
- Modify: `tests/conftest.py`（移除引用旧 `running_publisher`/`connected_subscriber` 的 fixtures，保留 `_loguru_capture`/`tmp_sqlite_url`）
- Test: `tests/test_producer_types.py`

**Interfaces:**
- Consumes: `client.ProducerClient`。
- Produces: `ProducerManager.start_all(on_message, sender_factory)` 其中 sender_factory 返回一个调用 `ProducerClient.publish` 的句柄。

- [ ] **Step 1: Write the failing test**

```python
# tests/test_producer_types.py（重写）
import inspect
import pytest
from pulsemq import ProducerClient
from pulsemq.producers.manager import ProducerManager
from pulsemq.producers.types import PubData, ProducerCallback


def test_pubdata_exported():
    from pulsemq import PubData
    assert PubData is not None


def test_producer_client_publish_signature():
    sig = inspect.signature(ProducerClient.publish)
    assert "topic" in sig.parameters
    assert "data" in sig.parameters


def test_manager_start_all_typed():
    sig = inspect.signature(ProducerManager.start_all)
    assert "on_message" in sig.parameters
    assert "sender_factory" in sig.parameters
```

并从 `pulsemq/__init__.py` 导出 `PubData`（在 `__all__` 加 `"PubData"`，`from pulsemq.producers.types import PubData`）。

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_producer_types.py -v`
Expected: FAIL — `ProducerClient` 存在但 `PubData` 未从 `pulsemq` 导出

- [ ] **Step 3: Write minimal implementation**

`producers/types.py`：把 `SenderProducerCallback` 的 forward-ref 由 `"PublisherSender"` 改为 `"ProducerClient"`（仅类型注解，不影响运行）。

`producers/manager.py`：`ProducerManager` 接口不变（`register/register_burst/start_all/stop_all`）。`start_all` 的 `sender_factory` 由调用方（CLI/示例）提供，返回一个带 `async def send(self, data, *, topic=None, serializer=None, compression=None)` 的对象，内部调 `ProducerClient.publish`。本 task 不改 manager 内部调度逻辑，只确保类型注解指向 `ProducerClient`。

`__init__.py` 追加：
```python
from pulsemq.producers.types import PubData
```
并在 `__all__` 加 `"PubData"`。

删除旧 e2e 测试与 shutdown 测试（它们引用已删除的 `PulsePublisher`/`PulseSubscriber`）：

```bash
git rm tests/test_e2e_publisher.py tests/test_e2e_subscriber.py \
       tests/test_integration.py tests/test_publisher_shutdown.py
```

`tests/conftest.py`：删除 `running_publisher`、`make_publisher`、`connected_subscriber`、`assert_message_roundtrip` 等引用旧角色的 fixtures/helper（保留 `_loguru_capture`、`tmp_sqlite_url`、`random_port_pair` 若不依赖旧角色）。具体用 Grep 定位 `PulsePublisher`/`PulseSubscriber` 引用后删除对应 fixture。

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_producer_types.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/pulsemq/producers/manager.py src/pulsemq/producers/types.py \
        src/pulsemq/__init__.py tests/test_producer_types.py tests/conftest.py
git commit -m "refactor(producers,tests): 适配 ProducerClient，删除旧 e2e/shutdown 测试"
```

---

## Task 15: e2e + zap 韧性测试重写

**Files:**
- Create: `tests/test_e2e_client_server.py`（替换被删的 e2e 测试）
- Rewrite: `tests/test_zap_resilience.py`
- Test: 全量

- [ ] **Step 1: Write the failing test**

```python
# tests/test_e2e_client_server.py
import asyncio
import socket as _sock
import pytest
from pulsemq import Server, ProducerClient, ConsumerClient


def _free_port():
    s = _sock.socket(_sock.AF_INET, _sock.SOCK_STREAM)
    s.bind(("127.0.0.1", 0)); p = s.getsockname()[1]; s.close(); return p


async def _server(creds):
    dp, cp, ap = _free_port(), _free_port(), _free_port()
    srv = Server(data_endpoint=f"tcp://127.0.0.1:{dp}",
                 control_endpoint=f"tcp://127.0.0.1:{cp}",
                 admin_endpoint=f"127.0.0.1:{ap}", credentials=creds)
    await srv.start()
    return srv, dp, cp


async def test_multi_producer_single_consumer():
    srv, dp, cp = await _server({"c": "c", "p1": "p", "p2": "p"})
    try:
        c = ConsumerClient(f"tcp://127.0.0.1:{dp}", f"tcp://127.0.0.1:{cp}", "c", "c")
        p1 = ProducerClient(f"tcp://127.0.0.1:{dp}", f"tcp://127.0.0.1:{cp}", "p1", "p")
        p2 = ProducerClient(f"tcp://127.0.0.1:{dp}", f"tcp://127.0.0.1:{cp}", "p2", "p")
        await c.start(); await p1.start(); await p2.start()
        got = []
        await c.subscribe("market.*", lambda m: got.append(m.topic))
        await asyncio.sleep(0.3)
        await p1.publish("market.stock.a", {"x": 1})
        await p2.publish("market.bond.b", {"x": 2})
        await asyncio.sleep(0.5)
        assert sorted(got) == ["market.bond.b", "market.stock.a"]
        await c.stop(); await p1.stop(); await p2.stop()
    finally:
        await srv.stop()


async def test_single_user_single_online():
    srv, dp, cp = await _server({"alice": "s"})
    try:
        from pulsemq.errors import ClientStartupError
        c1 = ConsumerClient(f"tcp://127.0.0.1:{dp}", f"tcp://127.0.0.1:{cp}", "alice", "s")
        await c1.start()
        c2 = ConsumerClient(f"tcp://127.0.0.1:{dp}", f"tcp://127.0.0.1:{cp}", "alice", "s")
        with pytest.raises(ClientStartupError):
            await c2.start()
        await c1.stop()
    finally:
        await srv.stop()
```

```python
# tests/test_zap_resilience.py（重写）
import pytest
from pulsemq import Server, ConsumerClient
from pulsemq.errors import AuthenticationError
# 复用 _free_port


async def test_auth_failure_on_wrong_password():
    import socket as _sock
    def _fp():
        s = _sock.socket(); s.bind(("127.0.0.1", 0)); p = s.getsockname()[1]; s.close(); return p
    dp, cp, ap = _fp(), _fp(), _fp()
    srv = Server(f"tcp://127.0.0.1:{dp}", f"tcp://127.0.0.1:{cp}", f"127.0.0.1:{ap}",
                 credentials={"alice": "right"})
    await srv.start()
    try:
        c = ConsumerClient(f"tcp://127.0.0.1:{dp}", f"tcp://127.0.0.1:{cp}", "alice", "WRONG")
        with pytest.raises(AuthenticationError):
            await c.start()
    finally:
        await srv.stop()
```

- [ ] **Step 2: Run test to verify it fails / passes**

Run: `pytest tests/test_e2e_client_server.py tests/test_zap_resilience.py -v`
Expected: 若 Client/Server 已实现，PASS；若认证失败路径未抛 `AuthenticationError`，FAIL → 回到 Task 12 在 `_connect_and_register` 中当 ZAP 拒绝时抛 `AuthenticationError(reason="invalid_password")`。

> **Task 12 的 `_connect_and_register` 简化版未真正检测 ZAP 拒绝。** 本 task 要求修正：在 `connect` 后，若 PLAIN 认证失败，pyzmq 的 DEALER 会在 monitor 报 `EVENT_HANDSHAKE_FAILED_AUTH` 且 recv 永不返回数据。最简可靠做法：`connect` 后给 `control` socket 发一个 REGISTER 并 `wait_for(recv, timeout=2.0)`；超时即认定认证失败，抛 `AuthenticationError(reason="invalid_password")`。更新 `client.py::_connect_and_register`：

```python
        # 认证检测：发一个探针，超时即认证失败
        try:
            await self._transport.connect(self._control_endpoint, "control", credentials=creds)
        except Exception as e:
            raise ClientStartupError(str(e), reason="CONTROL_CONNECT_FAILED",
                                     address=self._control_endpoint, username=self._username) from e
        # 探针：先发 REGISTER，若 2s 无回复，认定认证被拒
        await self._register()  # _register 内部已有 3s 超时→ClientStartupError
        # 区分认证失败：若 _register 超时，且用户名/密码可能错，抛 AuthenticationError
```

具体落地：把 `_register` 的超时分支改为抛 `AuthenticationError(reason="invalid_password")`（因为凭据错时 ZAP 拒绝，control 面收不到 reply）。更新 Task 12 的 `_register`：

```python
        except asyncio.TimeoutError as e:
            raise AuthenticationError(
                f"认证或注册超时（用户名/密码可能错误）: {self._username}",
                reason="invalid_password") from e
```

- [ ] **Step 3: 修正 client.py 的认证失败抛错**

按上面对 `client.py::_register` 的超时分支改为 `AuthenticationError`。

- [ ] **Step 4: Run full test suite**

Run: `pytest -v`
Expected: PASS（全部新增测试通过；沿用模块测试通过）

- [ ] **Step 5: Commit**

```bash
git add tests/test_e2e_client_server.py tests/test_zap_resilience.py src/pulsemq/client.py
git commit -m "test(e2e,zap): 重写 Client/Server e2e 与认证韧性测试"
```

---

## Self-Review（写计划后自查）

**1. Spec coverage（逐节核对 Spec 1）：**
- §1 目标：Client/Server 暴露（Task 12/13）、数据/控制面分离（Task 9/11）、强制 PLAIN（Task 9）、topic 订阅表+前缀+单用户单在线+动态订阅（Task 6/8/11）、启动硬失败+运行期重连（Task 12——重连状态机见下「缺口」）、零配置（Task 2/13）。✅ 大部分覆盖。
- §2 执行策略：原地大改（Task 13 删 publisher/subscriber）、版本 5.0.0（Task 13）、沿用模块（Task 10 适配）。✅
- §3 模块清单：errors/config/logging/frames/serialization/compression/flags/msg_type/transport/routing/control/client/server/lifecycle/producers 全部有 task。✅
- §4 帧格式：magic/ver/msg_type/flags/data_type/topic_len/topic/ts/record_count/payload/CRC（Task 7）。✅
- §5 transport：ROUTER/DEALER、ZAP PLAIN、monitor、关闭顺序（Task 9）。✅
- §6 routing：前缀匹配、remove、幂等（Task 8）。✅
- §7 control：命令集、OnlineRegistry、单用户单在线、心跳扫描（Task 6/11）。✅
- §8 client：类层次、启动硬失败表、运行期重连、心跳、装饰器、事件回调、producer 复用（Task 12/14）。⚠ **重连状态机（§8.3 指数退避+重新认证+恢复订阅）在 Task 12 仅留了 `_connect_and_register` 复用路径，未实现 monitor 驱动的自动重连循环。** 见下「缺口」。
- §9 server：组装、运行任务、关键行为（Task 11）。✅
- §10 lifecycle：信号处理、关闭顺序（Task 13）。✅
- §11 基础设施：errors/config/logging（Task 1/2/3）。✅
- §12 消息流：发布/订阅/启动失败/重连（Task 11/12/15）。✅
- §13 测试：沿用/重写/新增（Task 7/10/14/15）。✅
- §14/15/16：决策与边界已体现。

**2. 缺口（需补 task）：**

- **Client 运行期自动重连（Spec 1 §8.3）**：Task 12 只实现了启动期连接，未实现 monitor 检测断线→指数退避→重新认证→恢复订阅的运行期状态机。这是 Spec 1 明确目标（"运行期断线自动重连"）。**应新增 Task 12b：Client 重连状态机**，覆盖 `monitor` 回调驱动重连、指数退避（1s→2x→30s 上限）、重连后重新 REGISTER + 恢复订阅、重连后认证失败直接退出（exit 3）。测试：启动 Server+Client，中途 `srv.stop()` 再重启，验证 Client 自动恢复且订阅仍生效。

- **AdminServer 接入（Spec 1 §9.2「admin 服务：沿用现有 AdminServer」）**：Task 11 的 `Server.start()` 注释了 `self._admin = None`，未真正启动 AdminServer。沿用现有 `AdminServer` 需要把 `traffic_stats`/`snapshot_fn` 传入并 `await admin.start()`，关闭时 `await admin.stop()`。**应在 Task 11 补 AdminServer 接入**（独立线程或 asyncio 任务），否则 Spec 1 §9.2 与 §13.1（test_stats 沿用）的 admin 路由不可用。

- **producer 调度管线与 ProducerClient 的实际接线（§8.7）**：Task 14 只改了类型注解，未给出 `ProducerClient` 上 `@producer.schedule(...)` 装饰器风格的 helper 与 `sender_factory` 实现。Spec §8.7 示例期望 `ProducerClient.schedule(...)`。**应在 Task 14 补 `ProducerClient` 的 `producer`/`burst_producer` 装饰器 + `run_forever()`**，内部用 `ProducerManager` + `sender_factory` 返回调用 `self.publish` 的句柄。

**3. 上述缺口应在执行前补成 Task 12b / Task 11 扩展 / Task 14 扩展。** 本计划当前版本聚焦「跑通发布→中继→订阅 + 启动硬失败 + PLAIN 认证」最小闭环；重连状态机、AdminServer 接入、producer 装饰器作为紧随的补充 task，执行时按下方「补充 task」补齐。

**4. 类型一致性：** `PulseMessage.msg_type`（Task 7）与 `frames.encode` 的 `msg_type` kwarg、`decode_control` 的 `MsgType.CONTROL` 校验一致 ✅。`OnlineRegistry.register` 返回 `RegisterResult` 常量字符串，`Server._dispatch_control` 与 `Client._register` 比对 `"OK"`/`result` 一致 ✅。`Transport.send(role="server_ingress"/"control"/"consumer")` 的 role 字符串在 Server/Client 间一致 ✅。`SubscriptionTable` 用 identity 字符串，Server 在 `_dispatch_control` 用 `ident.decode()` 作 key，与 `routing.subscribe(identity:str)` 一致 ✅。

**5. Placeholder 扫描：** 无 TBD/TODO（Self-Review 中的「缺口」是显式列出的待补 task，非占位符）。每个 code step 均含完整代码。

---

## 补充 task（执行时在 Task 15 后追加）

### Task 12b: Client 运行期重连状态机

**Files:** Modify `src/pulsemq/client.py`；Test: `tests/test_client_reconnect.py`

实现 monitor 回调驱动的重连：`Transport.set_monitor_callback` 在 `EVENT_DISCONNECTED` 时触发 `_reconnect_loop`（指数退避 `reconnect_initial_delay`→`*backoff_multiplier`→`reconnect_max_delay`），重连成功后调 `_connect_and_register()`（复用原 `client_id` + `self._subscriptions` 自动恢复），重连后认证失败抛 `AuthenticationError`（exit 3）。测试：Server 重启后 Client 自动恢复订阅。

### Task 11b: AdminServer 接入

**Files:** Modify `src/pulsemq/server.py`

在 `Server.start()` 构造 `AdminServer(traffic_stats=self._stats, stats_storage=self._storage, snapshot_fn=lambda: {"online": self._registry.snapshot()}, start_time=...)`，`await admin.start()`；`stop()` 时 `await admin.stop()`。沿用现有 AdminServer，不改其内部。

### Task 14b: ProducerClient 调度装饰器

**Files:** Modify `src/pulsemq/client.py`、`src/pulsemq/producers/manager.py`

给 `ProducerClient` 加 `producer(name, *, interval, ...)` / `burst_producer(...)` 装饰器与 `run_forever()`：内部建 `ProducerManager`，`sender_factory` 返回一个 `send(data, *, topic, ...)` 调 `self.publish(topic, data)` 的句柄，`on_message` 调 `publish`。复用现有 `ProducerSpec`/`ProducerManager`。

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-06-26-pulsemq-v2-spec1-implementation.md`. Two execution options:

1. **Subagent-Driven (recommended)** — I dispatch a fresh subagent per task, review between tasks, fast iteration.
2. **Inline Execution** — Execute tasks in this session using executing-plans, batch execution with checkpoints.

Which approach?
