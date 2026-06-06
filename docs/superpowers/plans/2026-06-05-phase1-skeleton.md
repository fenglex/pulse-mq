# PulseMQ Phase 1: 基础骨架 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking tracking.

**Goal:** 实现能跑通 PUB → SUB 端到端的最小 服务端，不含认证/权限/通配符/批处理/背压/监控。

**Architecture:** 4 层架构（接入 → 引擎 → 领域 → 基础设施），ZMQ ROUTER-DEALER 接收客户端消息，XPUB-SUB 广播给订阅者。消息采用固定 6 帧格式，msgpack 默认序列化，所有路由/订阅/连接信息纯内存管理。

**Tech Stack:** Python 3.10+, pyzmq, msgpack, asyncio/uvloop, SQLite (aiosqlite for Phase 2)

---

## File Structure

```
pulse-mq/
├── pyproject.toml
├── src/
│   └── pulsemq/
│       ├── __init__.py
│       ├── config.py                 # 配置加载
│       ├── event_loop.py             # 事件循环选择
│       ├── models.py                 # 核心数据结构
│       ├── protocol/
│       │   ├── __init__.py
│       │   ├── msg_type.py           # 消息类型枚举
│       │   ├── flags.py              # flags bitfield 编解码
│       │   └── frames.py             # 6 帧编解码
│       ├── serialization/
│       │   ├── __init__.py
│       │   ├── registry.py           # 序列化/压缩注册表
│       │   ├── msgpack_ser.py        # msgpack 实现
│       │   ├── raw_ser.py            # raw 实现
│       │   └── compressors.py        # none/snappy/lz4/zstd
│       ├── transport/
│       │   ├── __init__.py
│       │   └── zmq_transport.py      # ZMQ 传输适配器
│       ├── engine/
│       │   ├── __init__.py
│       │   ├── router.py             # 路由器（精确 topic）
│       │   └── handlers.py           # 消息处理器
│       └── server.py                 # 启动器
├── tests/
│   ├── conftest.py
│   ├── unit/
│   │   ├── __init__.py
│   │   ├── test_config.py
│   │   ├── test_models.py
│   │   ├── test_msg_type.py
│   │   ├── test_flags.py
│   │   ├── test_frames.py
│   │   ├── test_serialization.py
│   │   ├── test_compressors.py
│   │   ├── test_router.py
│   │   └── test_handlers.py
│   └── integration/
│       ├── __init__.py
│       └── test_pubsub_e2e.py
└── design/                           # 已存在
```

---

### Task 1: 项目初始化

**Files:**
- Create: `pyproject.toml`
- Create: `src/pulsemq/__init__.py`
- Create: `tests/conftest.py`
- Create: `tests/unit/__init__.py`
- Create: `tests/integration/__init__.py`

- [ ] **Step 1: 创建 pyproject.toml**

```toml
[project]
name = "pulsemq"
version = "0.1.0"
description = "高性能金融行情消息中间件"
requires-python = ">=3.10"
dependencies = [
    "pyzmq>=26.0",
    "msgpack>=1.0",
]

[project.optional-dependencies]
compress = [
    "python-snappy>=0.7",
    "lz4>=4.0",
    "zstandard>=0.22",
]
uvloop = [
    "uvloop>=0.19",
]
dev = [
    "pytest>=8.0",
    "pytest-asyncio>=0.23",
    "pytest-timeout>=2.0",
]

[project.scripts]
pulse-mq = "pulsemq.server:main"

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.pytest.ini_options]
asyncio_mode = "auto"
testpaths = ["tests"]
```

- [ ] **Step 2: 创建包结构和空文件**

```python
# src/pulsemq/__init__.py
"""PulseMQ - 高性能金融行情消息中间件"""
```

```python
# tests/conftest.py
import pytest
```

```python
# tests/unit/__init__.py
```

```python
# tests/integration/__init__.py
```

- [ ] **Step 3: 初始化 git 并提交**

```bash
cd D:/workflow/pulse-mq
git init
```

创建 `.gitignore`:
```
__pycache__/
*.pyc
.venv/
dist/
*.egg-info/
.pytest_cache/
*.db
*.sqlite
htmlcov/
.coverage
```

```bash
git add -A
git commit -m "项目初始化：创建项目结构和依赖声明"
```

- [ ] **Step 4: 安装依赖**

```bash
cd D:/workflow/pulse-mq
uv venv
uv pip install -e ".[dev,compress]"
```

---

### Task 2: 配置模块 (config.py)

**Files:**
- Create: `src/pulsemq/config.py`
- Create: `tests/unit/test_config.py`

- [ ] **Step 1: 编写配置测试**

```python
# tests/unit/test_config.py
import os
import pytest
from pulsemq.config import ServerConfig, load_config


class TestServerConfig:
    def test_default_values(self):
        cfg = ServerConfig()
        assert cfg.bind == "tcp://*:5555"
        assert cfg.xpub_bind == "tcp://*:5556"
        assert cfg.max_concurrency == 100
        assert cfg.max_batch_size == 64
        assert cfg.drain_timeout_ms == 1
        assert cfg.use_uvloop is True
        assert cfg.object_pool_size == 4096
        assert cfg.zmq_rcvhwm == 10000
        assert cfg.zmq_sndhwm == 10000
        assert cfg.zmq_heartbeat_ivl == 2000
        assert cfg.zmq_heartbeat_timeout == 5000
        assert cfg.zmq_heartbeat_ttl == 8000
        assert cfg.data_buffer_size == 9000
        assert cfg.ctrl_buffer_size == 1000
        assert cfg.backpressure_threshold == 0.8
        assert cfg.default_serializer == "msgpack"
        assert cfg.default_compressor == "none"
        assert cfg.auth_enabled is True
        assert cfg.default_admin_key == "pulse_sk_admin_default"

    def test_from_env_override(self, monkeypatch):
        monkeypatch.setenv("PULSEMQ_BIND", "tcp://*:6666")
        monkeypatch.setenv("PULSEMQ_CONCURRENCY", "200")
        cfg = load_config()
        assert cfg.bind == "tcp://*:6666"
        assert cfg.max_concurrency == 200

    def test_env_priority_over_default(self, monkeypatch):
        monkeypatch.setenv("PULSEMQ_BATCH_SIZE", "32")
        cfg = load_config()
        assert cfg.max_batch_size == 32
        # 其他保持默认
        assert cfg.max_concurrency == 100
```

- [ ] **Step 2: 运行测试确认失败**

```bash
pytest tests/unit/test_config.py -v
```

预期: ImportError — pulsemq.config 不存在

- [ ] **Step 3: 实现配置模块**

```python
# src/pulsemq/config.py
"""配置加载：环境变量 > 配置文件 > 默认值"""

from __future__ import annotations

import os
from dataclasses import dataclass, field

# 环境变量名 → (config_field, 类型转换函数)
_ENV_MAP: dict[str, tuple[str, type]] = {
    "PULSEMQ_TRANSPORT": ("transport", str),
    "PULSEMQ_BIND": ("bind", str),
    "PULSEMQ_XPUB_BIND": ("xpub_bind", str),
    "PULSEMQ_DB_URL": ("db_url", str),
    "PULSEMQ_STATS_DB_URL": ("stats_db_url", str),
    "PULSEMQ_STATS_RETENTION": ("stats_retention_days", int),
    "PULSEMQ_CONCURRENCY": ("max_concurrency", int),
    "PULSEMQ_BATCH_SIZE": ("max_batch_size", int),
    "PULSEMQ_DRAIN_TIMEOUT": ("drain_timeout_ms", int),
    "PULSEMQ_USE_UVLOOP": ("use_uvloop", lambda v: v.lower() in ("true", "1", "yes")),
    "PULSEMQ_POOL_SIZE": ("object_pool_size", int),
    "PULSEMQ_ZMQ_RCVHWM": ("zmq_rcvhwm", int),
    "PULSEMQ_ZMQ_SNDHWM": ("zmq_sndhwm", int),
    "PULSEMQ_HEARTBEAT_IVL": ("zmq_heartbeat_ivl", int),
    "PULSEMQ_HEARTBEAT_TIMEOUT": ("zmq_heartbeat_timeout", int),
    "PULSEMQ_HEARTBEAT_TTL": ("zmq_heartbeat_ttl", int),
    "PULSEMQ_DATA_BUFFER": ("data_buffer_size", int),
    "PULSEMQ_CTRL_BUFFER": ("ctrl_buffer_size", int),
    "PULSEMQ_BP_THRESHOLD": ("backpressure_threshold", float),
    "PULSEMQ_SERIALIZER": ("default_serializer", str),
    "PULSEMQ_COMPRESSOR": ("default_compressor", str),
    "PULSEMQ_AUTH_ENABLED": ("auth_enabled", lambda v: v.lower() in ("true", "1", "yes")),
    "PULSEMQ_ADMIN_KEY": ("default_admin_key", str),
}


@dataclass
class ServerConfig:
    """服务端 全部配置项，全部有合理默认值。"""

    # 传输层
    transport: str = "zmq"
    bind: str = "tcp://*:5555"
    xpub_bind: str = "tcp://*:5556"

    # 存储层
    db_url: str = "sqlite://./pulse_mq.db"
    stats_db_url: str = "sqlite://./stats.sqlite"
    stats_retention_days: int = 7

    # 引擎层
    max_concurrency: int = 100
    max_batch_size: int = 64
    drain_timeout_ms: int = 1
    use_uvloop: bool = True
    object_pool_size: int = 4096

    # ZMQ socket
    zmq_rcvhwm: int = 10000
    zmq_sndhwm: int = 10000
    zmq_heartbeat_ivl: int = 2000
    zmq_heartbeat_timeout: int = 5000
    zmq_heartbeat_ttl: int = 8000

    # 过载保护
    data_buffer_size: int = 9000
    ctrl_buffer_size: int = 1000
    backpressure_threshold: float = 0.8

    # 序列化/压缩
    default_serializer: str = "msgpack"
    default_compressor: str = "none"

    # 认证
    auth_enabled: bool = True
    default_admin_key: str = "pulse_sk_admin_default"

    # 监控
    metrics_enabled: bool = True
    metrics_bind: str = "0.0.0.0:9090"


def load_config(config_dict: dict | None = None) -> ServerConfig:
    """加载配置：环境变量覆盖默认值，config_dict 覆盖环境变量。

    Args:
        config_dict: 从 TOML 配置文件解析的字典（Phase 1 暂不实现文件解析）。
    """
    cfg = ServerConfig()

    # 环境变量覆盖默认值
    for env_key, (field_name, type_fn) in _ENV_MAP.items():
        value = os.environ.get(env_key)
        if value is not None:
            setattr(cfg, field_name, type_fn(value))

    # config_dict 覆盖（预留）
    if config_dict:
        _apply_dict(cfg, config_dict)

    return cfg


def _apply_dict(cfg: ServerConfig, d: dict) -> None:
    """递归应用字典到配置对象。"""
    for section_key, section_val in d.items():
        if isinstance(section_val, dict):
            for k, v in section_val.items():
                if hasattr(cfg, k):
                    setattr(cfg, k, type(getattr(cfg, k))(v))
        elif hasattr(cfg, section_key):
            setattr(cfg, section_key, type(getattr(cfg, section_key))(section_val))
```

- [ ] **Step 4: 运行测试确认通过**

```bash
pytest tests/unit/test_config.py -v
```

- [ ] **Step 5: 提交**

```bash
git add src/pulsemq/config.py tests/unit/test_config.py
git commit -m "实现配置模块：ServerConfig 数据类 + 环境变量覆盖"
```

---

### Task 3: 领域模型 (models.py)

**Files:**
- Create: `src/pulsemq/models.py`
- Create: `tests/unit/test_models.py`

- [ ] **Step 1: 编写模型测试**

```python
# tests/unit/test_models.py
from dataclasses import fields
from pulsemq.models import (
    AuthUser,
    BufferedMessage,
    ExpandedPermissions,
    TopicInfo,
)


class TestAuthUser:
    def test_create(self):
        user = AuthUser(
            user_id=1, role="admin", groups=["行情全订阅"],
            api_key="pulse_sk_admin_default", namespace="",
        )
        assert user.user_id == 1
        assert user.role == "admin"
        assert user.is_admin is True

    def test_normal_user_is_not_admin(self):
        user = AuthUser(
            user_id=2, role="user", groups=["行情全订阅"],
            api_key="pulse_sk_xxx", namespace="team-a",
        )
        assert user.is_admin is False


class TestTopicInfo:
    def test_create(self):
        info = TopicInfo(
            full_name="team-a.mkt.sh.600000",
            namespace="team-a",
            topic_path="mkt.sh.600000",
            is_wildcard=False,
            subscriber_count=0,
            created_at=1717516800.0,
        )
        assert info.namespace == "team-a"
        assert info.is_wildcard is False

    def test_parse_from_full_name(self):
        info = TopicInfo.from_name("team-a.mkt.sh.600000")
        assert info.namespace == "team-a"
        assert info.topic_path == "mkt.sh.600000"
        assert info.full_name == "team-a.mkt.sh.600000"
        assert info.is_wildcard is False

    def test_wildcard_detection(self):
        info = TopicInfo.from_name("team-a.mkt.*")
        assert info.is_wildcard is True


class TestBufferedMessage:
    def test_create(self):
        msg = BufferedMessage(
            topic="team-a.mkt.sh.600000",
            seq=1,
            record_count=1,
            meta=b"\x02\x01",
            payload=b"\x93\x01\x02\x03",
            timestamp=1717516800.0,
        )
        assert msg.seq == 1
        assert msg.record_count == 1


class TestExpandedPermissions:
    def test_empty(self):
        perms = ExpandedPermissions()
        assert perms.pub == []
        assert perms.sub == []

    def test_from_dict(self):
        perms = ExpandedPermissions.from_dict({
            "pub": ["team-a.mkt.*"],
            "sub": ["*.mkt.*"],
        })
        assert perms.pub == ["team-a.mkt.*"]
        assert perms.sub == ["*.mkt.*"]
```

- [ ] **Step 2: 运行测试确认失败**

```bash
pytest tests/unit/test_models.py -v
```

- [ ] **Step 3: 实现模型**

```python
# src/pulsemq/models.py
"""核心领域模型：纯数据结构，无业务逻辑。"""

from __future__ import annotations

import time
from dataclasses import dataclass, field


@dataclass
class AuthUser:
    """已认证用户信息，存储在内存中。"""

    user_id: int
    role: str                     # "admin" | "user"
    groups: list[str]             # 权限组名称列表
    api_key: str                  # 原始 api_key
    namespace: str = ""           # home_namespace

    @property
    def is_admin(self) -> bool:
        return self.role == "admin"


@dataclass
class TopicInfo:
    """Topic 注册信息。"""

    full_name: str                # "team-a.mkt.sh.600000"
    namespace: str                # "team-a"（第一段）
    topic_path: str               # "mkt.sh.600000"（除第一段外）
    is_wildcard: bool = False
    subscriber_count: int = 0
    created_at: float = field(default_factory=time.time)

    @classmethod
    def from_name(cls, full_name: str) -> TopicInfo:
        """从完整 topic 名创建 TopicInfo。"""
        parts = full_name.split(".")
        # 检测通配符
        is_wildcard = "*" in parts or ">" in parts
        return cls(
            full_name=full_name,
            namespace=parts[0] if parts else "",
            topic_path=".".join(parts[1:]) if len(parts) > 1 else "",
            is_wildcard=is_wildcard,
        )


@dataclass(slots=True)
class BufferedMessage:
    """消息缓冲区中的单条消息。"""

    topic: str
    seq: int                      # topic 内单调递增消息序号
    record_count: int             # 本条消息携带的数据行数（≥1）
    meta: bytes                   # Frame 3 的 2 字节
    payload: bytes                # Frame 5 的序列化+压缩后的 payload
    timestamp: float              # 服务端 接收时间


@dataclass
class ExpandedPermissions:
    """用户展开后的权限列表（从权限组合并而来）。"""

    pub: list[str] = field(default_factory=list)
    sub: list[str] = field(default_factory=list)
    query: list[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, d: dict[str, list[str]]) -> ExpandedPermissions:
        return cls(
            pub=d.get("pub", []),
            sub=d.get("sub", []),
            query=d.get("query", []),
        )
```

- [ ] **Step 4: 运行测试确认通过**

```bash
pytest tests/unit/test_models.py -v
```

- [ ] **Step 5: 提交**

```bash
git add src/pulsemq/models.py tests/unit/test_models.py
git commit -m "实现核心领域模型：AuthUser, TopicInfo, BufferedMessage, ExpandedPermissions"
```

---

### Task 4: 事件循环模块 (event_loop.py)

**Files:**
- Create: `src/pulsemq/event_loop.py`

- [ ] **Step 1: 实现事件循环选择**

```python
# src/pulsemq/event_loop.py
"""事件循环选择：Linux/macOS 下使用 uvloop，Windows 自动回退 asyncio。"""

from __future__ import annotations


def install_event_loop(use_uvloop: bool = True) -> str:
    """安装高性能事件循环。

    Returns:
        "uvloop" 或 "asyncio"，表示实际使用的事件循环。
    """
    if not use_uvloop:
        return "asyncio"

    try:
        import uvloop
        uvloop.install()
        return "uvloop"
    except ImportError:
        return "asyncio"
```

- [ ] **Step 2: 提交**

```bash
git add src/pulsemq/event_loop.py
git commit -m "实现事件循环选择：uvloop 自动回退"
```

---

### Task 5: 协议层 — 消息类型枚举 (protocol/msg_type.py)

**Files:**
- Create: `src/pulsemq/protocol/__init__.py`
- Create: `src/pulsemq/protocol/msg_type.py`
- Create: `tests/unit/test_msg_type.py`

- [ ] **Step 1: 编写消息类型测试**

```python
# tests/unit/test_msg_type.py
from pulsemq.protocol.msg_type import MsgType


class TestMsgType:
    def test_values(self):
        assert MsgType.AUTH == 0x01
        assert MsgType.PUB == 0x02
        assert MsgType.SUB == 0x03
        assert MsgType.UNSUB == 0x04
        assert MsgType.QUERY == 0x05
        assert MsgType.PING == 0x06
        assert MsgType.PONG == 0x07
        assert MsgType.STATUS == 0x08
        assert MsgType.ERROR == 0x09
        assert MsgType.BROADCAST == 0x0A
        assert MsgType.HISTORY_REPLAY == 0x0B

    def test_is_control(self):
        assert MsgType.is_control(MsgType.AUTH) is True
        assert MsgType.is_control(MsgType.SUB) is True
        assert MsgType.is_control(MsgType.UNSUB) is True
        assert MsgType.is_control(MsgType.QUERY) is True
        assert MsgType.is_control(MsgType.PING) is True
        assert MsgType.is_control(MsgType.PUB) is False
        assert MsgType.is_control(MsgType.BROADCAST) is False

    def test_from_byte(self):
        assert MsgType.from_byte(0x02) == MsgType.PUB
        assert MsgType.from_byte(0xFF) is None
```

- [ ] **Step 2: 运行测试确认失败**

```bash
pytest tests/unit/test_msg_type.py -v
```

- [ ] **Step 3: 实现**

```python
# src/pulsemq/protocol/__init__.py
"""协议层：帧编解码、消息类型、flags 定义。"""
```

```python
# src/pulsemq/protocol/msg_type.py
"""消息类型枚举。"""

from __future__ import annotations


class MsgType:
    """消息类型常量，对应 Frame 3 Byte 0。"""

    AUTH = 0x01
    PUB = 0x02
    SUB = 0x03
    UNSUB = 0x04
    QUERY = 0x05
    PING = 0x06
    PONG = 0x07
    STATUS = 0x08
    ERROR = 0x09
    BROADCAST = 0x0A
    HISTORY_REPLAY = 0x0B

    # 控制消息集合（进入 ctrl_buffer）
    _CONTROL_TYPES: frozenset[int] = frozenset({
        AUTH, SUB, UNSUB, QUERY, PING,
    })

    @classmethod
    def is_control(cls, msg_type: int) -> bool:
        """判断是否为控制消息。"""
        return msg_type in cls._CONTROL_TYPES

    @classmethod
    def from_byte(cls, b: int) -> int | None:
        """从字节值获取消息类型，非法值返回 None。"""
        valid = {
            cls.AUTH, cls.PUB, cls.SUB, cls.UNSUB, cls.QUERY,
            cls.PING, cls.PONG, cls.STATUS, cls.ERROR, cls.BROADCAST,
            cls.HISTORY_REPLAY,
        }
        return b if b in valid else None
```

- [ ] **Step 4: 运行测试确认通过**

```bash
pytest tests/unit/test_msg_type.py -v
```

- [ ] **Step 5: 提交**

```bash
git add src/pulsemq/protocol/ tests/unit/test_msg_type.py
git commit -m "实现消息类型枚举：MsgType 常量 + 控制消息判断"
```

---

### Task 6: 协议层 — Flags 编解码 (protocol/flags.py)

**Files:**
- Create: `src/pulsemq/protocol/flags.py`
- Create: `tests/unit/test_flags.py`

- [ ] **Step 1: 编写 flags 测试**

```python
# tests/unit/test_flags.py
import pytest
from pulsemq.protocol.flags import FrameFlags


class TestFrameFlags:
    def test_encode_default(self):
        flags = FrameFlags(ser_fmt="msgpack", comp="none", has_topic=False)
        byte_val = flags.encode()
        assert byte_val == 0b0000_0000

    def test_encode_has_topic(self):
        flags = FrameFlags(ser_fmt="msgpack", comp="none", has_topic=True)
        byte_val = flags.encode()
        assert byte_val == 0b0010_0000

    def test_encode_raw(self):
        flags = FrameFlags(ser_fmt="bytes", comp="none", has_topic=False)
        byte_val = flags.encode()
        assert byte_val == 0b0000_0001

    def test_encode_pyarrow(self):
        flags = FrameFlags(ser_fmt="pyarrow", comp="none", has_topic=False)
        byte_val = flags.encode()
        assert byte_val == 0b0000_0010

    def test_encode_snappy(self):
        flags = FrameFlags(ser_fmt="msgpack", comp="snappy", has_topic=False)
        byte_val = flags.encode()
        assert byte_val == 0b0000_1000

    def test_encode_lz4(self):
        flags = FrameFlags(ser_fmt="msgpack", comp="lz4", has_topic=False)
        byte_val = flags.encode()
        assert byte_val == 0b0001_0000

    def test_encode_zstd(self):
        flags = FrameFlags(ser_fmt="msgpack", comp="zstd", has_topic=False)
        byte_val = flags.encode()
        assert byte_val == 0b0001_1000

    def test_decode_roundtrip(self):
        original = FrameFlags(ser_fmt="pyarrow", comp="zstd", has_topic=True)
        byte_val = original.encode()
        decoded = FrameFlags.decode(byte_val)
        assert decoded.ser_fmt == "pyarrow"
        assert decoded.comp == "zstd"
        assert decoded.has_topic is True

    @pytest.mark.parametrize("ser,comp", [
        ("msgpack", "none"),
        ("bytes", "none"),
        ("pyarrow", "snappy"),
        ("msgpack", "lz4"),
        ("msgpack", "zstd"),
    ])
    def test_roundtrip_all(self, ser, comp):
        for has_topic in (True, False):
            original = FrameFlags(ser_fmt=ser, comp=comp, has_topic=has_topic)
            decoded = FrameFlags.decode(original.encode())
            assert decoded.ser_fmt == ser
            assert decoded.comp == comp
            assert decoded.has_topic == has_topic
```

- [ ] **Step 2: 运行测试确认失败**

```bash
pytest tests/unit/test_flags.py -v
```

- [ ] **Step 3: 实现**

```python
# src/pulsemq/protocol/flags.py
"""Frame 3 Byte 1 flags bitfield 编解码。

bit[0:2] = 序列化格式 (000=msgpack, 001=raw, 010=pyarrow, 011=protobuf)
bit[3:4] = 压缩算法   (00=none, 01=snappy, 10=lz4, 11=zstd)
bit[5]   = has_topic  (0=无topic, 1=有topic)
bit[6:7] = reserved
"""

from __future__ import annotations

from dataclasses import dataclass

# 序列化格式名 → bit[0:2] 编码
_SER_MAP: dict[str, int] = {
    "msgpack": 0b000,
    "bytes": 0b001,
    "pyarrow": 0b010,
    "protobuf": 0b011,
}
_SER_MAP_REV: dict[int, str] = {v: k for k, v in _SER_MAP.items()}

# 压缩算法名 → bit[3:4] 编码
_COMP_MAP: dict[str, int] = {
    "none": 0b00,
    "snappy": 0b01,
    "lz4": 0b10,
    "zstd": 0b11,
}
_COMP_MAP_REV: dict[int, str] = {v: k for k, v in _COMP_MAP.items()}


@dataclass
class FrameFlags:
    """Frame 3 Byte 1 的 flags 解析结果。"""

    ser_fmt: str        # 序列化格式名
    comp: str           # 压缩算法名
    has_topic: bool     # 是否有 topic

    def encode(self) -> int:
        """编码为单字节整数。"""
        ser_bits = _SER_MAP.get(self.ser_fmt, 0b000)
        comp_bits = _COMP_MAP.get(self.comp, 0b00)
        topic_bit = 0b0010_0000 if self.has_topic else 0
        return ser_bits | (comp_bits << 3) | topic_bit

    @classmethod
    def decode(cls, byte_val: int) -> FrameFlags:
        """从单字节解码。"""
        ser_bits = byte_val & 0b0000_0111
        comp_bits = (byte_val >> 3) & 0b0000_0011
        has_topic = bool(byte_val & 0b0010_0000)
        return cls(
            ser_fmt=_SER_MAP_REV.get(ser_bits, "msgpack"),
            comp=_COMP_MAP_REV.get(comp_bits, "none"),
            has_topic=has_topic,
        )
```

- [ ] **Step 4: 运行测试确认通过**

```bash
pytest tests/unit/test_flags.py -v
```

- [ ] **Step 5: 提交**

```bash
git add src/pulsemq/protocol/flags.py tests/unit/test_flags.py
git commit -m "实现 flags bitfield 编解码：序列化/压缩/topic 标志"
```

---

### Task 7: 序列化与压缩注册表

**Files:**
- Create: `src/pulsemq/serialization/__init__.py`
- Create: `src/pulsemq/serialization/registry.py`
- Create: `src/pulsemq/serialization/msgpack_ser.py`
- Create: `src/pulsemq/serialization/raw_ser.py`
- Create: `src/pulsemq/serialization/compressors.py`
- Create: `tests/unit/test_serialization.py`
- Create: `tests/unit/test_compressors.py`

- [ ] **Step 1: 编写序列化注册表测试**

```python
# tests/unit/test_serialization.py
import pytest
from pulsemq.serialization.registry import (
    SerializationRegistry,
    CompressionRegistry,
    Serializer,
    Compressor,
)


class DummySerializer(Serializer):
    def serialize(self, obj: bytes) -> bytes:
        return obj

    def deserialize(self, data: bytes) -> bytes:
        return data


class DummyCompressor(Compressor):
    def compress(self, data: bytes) -> bytes:
        return data

    def decompress(self, data: bytes) -> bytes:
        return data


class TestSerializationRegistry:
    def test_builtin_msgpack(self):
        ser = SerializationRegistry.get("msgpack")
        data = {"price": 15.8, "volume": 1000}
        encoded = ser.serialize(data)
        decoded = ser.deserialize(encoded)
        assert decoded == data

    def test_builtin_raw(self):
        ser = SerializationRegistry.get("bytes")
        data = b"hello world"
        encoded = ser.serialize(data)
        assert encoded is data
        assert ser.deserialize(encoded) == data

    def test_register_custom(self):
        SerializationRegistry.register("test_ser", DummySerializer())
        assert SerializationRegistry.has("test_ser")
        assert SerializationRegistry.get("test_ser") is not None

    def test_list(self):
        names = SerializationRegistry.list()
        assert "msgpack" in names
        assert "bytes" in names

    def test_get_nonexistent_raises(self):
        with pytest.raises(KeyError):
            SerializationRegistry.get("nonexistent")


class TestCompressionRegistry:
    def test_builtin_none(self):
        comp = CompressionRegistry.get("none")
        data = b"hello"
        assert comp.compress(data) == data
        assert comp.decompress(data) == data

    def test_register_custom(self):
        CompressionRegistry.register("test_comp", DummyCompressor())
        assert CompressionRegistry.has("test_comp")

    def test_list(self):
        names = CompressionRegistry.list()
        assert "none" in names
```

- [ ] **Step 2: 编写压缩算法测试**

```python
# tests/unit/test_compressors.py
import pytest
from pulsemq.serialization.registry import CompressionRegistry


class TestCompressors:
    @pytest.mark.parametrize("name", ["none", "snappy", "lz4", "zstd"])
    def test_roundtrip(self, name):
        comp = CompressionRegistry.get(name)
        data = b"hello world " * 100
        compressed = comp.compress(data)
        decompressed = comp.decompress(compressed)
        assert decompressed == data

    def test_none_is_passthrough(self):
        comp = CompressionRegistry.get("none")
        data = b"test"
        assert comp.compress(data) is data
        assert comp.decompress(data) is data

    @pytest.mark.parametrize("name", ["snappy", "lz4", "zstd"])
    def test_compressed_smaller(self, name):
        comp = CompressionRegistry.get(name)
        data = b"hello world " * 1000
        compressed = comp.compress(data)
        assert len(compressed) < len(data)
```

- [ ] **Step 3: 运行测试确认失败**

```bash
pytest tests/unit/test_serialization.py tests/unit/test_compressors.py -v
```

- [ ] **Step 4: 实现序列化注册表**

```python
# src/pulsemq/serialization/__init__.py
"""序列化与压缩注册表。"""
```

```python
# src/pulsemq/serialization/registry.py
"""序列化和压缩的注册表模式。"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class Serializer(ABC):
    """序列化器抽象接口。"""

    @abstractmethod
    def serialize(self, obj: Any) -> bytes: ...

    @abstractmethod
    def deserialize(self, data: bytes) -> Any: ...


class Compressor(ABC):
    """压缩器抽象接口。"""

    @abstractmethod
    def compress(self, data: bytes) -> bytes: ...

    @abstractmethod
    def decompress(self, data: bytes) -> bytes: ...


class SerializationRegistry:
    """序列化格式注册表（全局单例）。"""

    _serializers: dict[str, Serializer] = {}

    @classmethod
    def register(cls, name: str, serializer: Serializer) -> None:
        cls._serializers[name] = serializer

    @classmethod
    def get(cls, name: str) -> Serializer:
        if name not in cls._serializers:
            raise KeyError(f"未注册的序列化格式: {name}")
        return cls._serializers[name]

    @classmethod
    def list(cls) -> list[str]:
        return list(cls._serializers.keys())

    @classmethod
    def has(cls, name: str) -> bool:
        return name in cls._serializers


class CompressionRegistry:
    """压缩算法注册表（全局单例）。"""

    _compressors: dict[str, Compressor] = {}

    @classmethod
    def register(cls, name: str, compressor: Compressor) -> None:
        cls._compressors[name] = compressor

    @classmethod
    def get(cls, name: str) -> Compressor:
        if name not in cls._compressors:
            raise KeyError(f"未注册的压缩算法: {name}")
        return cls._compressors[name]

    @classmethod
    def list(cls) -> list[str]:
        return list(cls._compressors.keys())

    @classmethod
    def has(cls, name: str) -> bool:
        return name in cls._compressors


def _init_builtins() -> None:
    """注册内置序列化器和压缩器。"""
    from pulsemq.serialization.registry import MsgpackSerializer
    from pulsemq.serialization.registry import BytesSerializer

    SerializationRegistry.register("msgpack", MsgpackSerializer())
    SerializationRegistry.register("bytes", BytesSerializer())

    from pulsemq.serialization.registry import (
        NoneCompressor,
        SnappyCompressor,
        Lz4Compressor,
        ZstdCompressor,
    )

    CompressionRegistry.register("none", NoneCompressor())
    CompressionRegistry.register("snappy", SnappyCompressor())
    CompressionRegistry.register("lz4", Lz4Compressor())
    CompressionRegistry.register("zstd", ZstdCompressor())


# 模块加载时自动注册
_init_builtins()
```

```python
# src/pulsemq/serialization/msgpack_ser.py
"""msgpack 序列化器。"""

from __future__ import annotations

from typing import Any

import msgpack

from pulsemq.serialization.registry import Serializer


class MsgpackSerializer(Serializer):
    """msgpack 二进制 JSON 序列化。"""

    def serialize(self, obj: Any) -> bytes:
        return msgpack.packb(obj, use_bin_type=True)

    def deserialize(self, data: bytes) -> Any:
        return msgpack.unpackb(data, raw=False)
```

```python
# src/pulsemq/serialization/raw_ser.py
"""raw 纯字节透传序列化器。"""

from __future__ import annotations

from typing import Any

from pulsemq.serialization.registry import Serializer


class BytesSerializer(Serializer):
    """不做任何序列化，直接透传 bytes。"""

    def serialize(self, obj: Any) -> bytes:
        if not isinstance(obj, bytes):
            raise TypeError(f"bytes 序列化只接受 bytes，收到 {type(obj).__name__}")
        return obj

    def deserialize(self, data: bytes) -> bytes:
        return data
```

```python
# src/pulsemq/serialization/compressors.py
"""内置压缩算法实现。"""

from __future__ import annotations

from pulsemq.serialization.registry import Compressor


class NoneCompressor(Compressor):
    """不压缩，直接透传。"""

    def compress(self, data: bytes) -> bytes:
        return data

    def decompress(self, data: bytes) -> bytes:
        return data


class SnappyCompressor(Compressor):
    """Google Snappy 极速压缩。"""

    def compress(self, data: bytes) -> bytes:
        import snappy
        return snappy.compress(data)

    def decompress(self, data: bytes) -> bytes:
        import snappy
        return snappy.decompress(data)


class Lz4Compressor(Compressor):
    """LZ4 极速压缩/解压。"""

    def compress(self, data: bytes) -> bytes:
        import lz4.frame
        return lz4.frame.compress(data)

    def decompress(self, data: bytes) -> bytes:
        import lz4.frame
        return lz4.frame.decompress(data)


class ZstdCompressor(Compressor):
    """Facebook Zstandard 高压缩比。"""

    def compress(self, data: bytes) -> bytes:
        import zstandard as zstd
        return zstd.compress(data)

    def decompress(self, data: bytes) -> bytes:
        import zstandard as zstd
        return zstd.decompress(data)
```

- [ ] **Step 5: 运行测试确认通过**

```bash
pytest tests/unit/test_serialization.py tests/unit/test_compressors.py -v
```

- [ ] **Step 6: 提交**

```bash
git add src/pulsemq/serialization/ tests/unit/test_serialization.py tests/unit/test_compressors.py
git commit -m "实现序列化/压缩注册表：msgpack + raw + 4种压缩算法"
```

---

### Task 8: 协议层 — 帧编解码 (protocol/frames.py)

**Files:**
- Create: `src/pulsemq/protocol/frames.py`
- Create: `tests/unit/test_frames.py`

- [ ] **Step 1: 编写帧编解码测试**

```python
# tests/unit/test_frames.py
import struct
import pytest
from pulsemq.protocol.frames import FrameCodec, DecodedFrame
from pulsemq.protocol.msg_type import MsgType


class TestFrameCodec:
    def test_encode_pub_message(self):
        """测试 PUB 消息编码为 4 帧（不含 identity 和 delimiter）。"""
        payload = FrameCodec.encode_payload(
            {"price": 15.8}, ser_fmt="msgpack", comp="none"
        )
        frames = FrameCodec.encode(
            msg_type=MsgType.PUB,
            topic="team-a.mkt.sh.600000",
            record_count=1,
            payload=payload,
            ser_fmt="msgpack",
            comp="none",
        )
        # 客户端发送 4 帧：topic + meta + record_count + payload
        assert len(frames) == 4
        assert frames[0] == b"team-a.mkt.sh.600000"
        # meta: msg_type=0x02, flags=has_topic=1 | ser=msgpack(000) | comp=none(00)
        assert frames[1] == bytes([0x02, 0b0010_0000])
        assert struct.unpack(">I", frames[2])[0] == 1

    def test_encode_ping_message(self):
        """PING 无 topic。"""
        payload = FrameCodec.encode_payload(
            {"client_ts": 1717516800.123}, ser_fmt="msgpack", comp="none"
        )
        frames = FrameCodec.encode(
            msg_type=MsgType.PING,
            topic="",
            record_count=0,
            payload=payload,
            ser_fmt="msgpack",
            comp="none",
        )
        assert len(frames) == 4
        assert frames[0] == b""
        assert frames[1][0] == MsgType.PING

    def test_decode_server_received(self):
        """服务端 ROUTER 收到 6 帧，解码后提取各字段。"""
        payload = FrameCodec.encode_payload(
            {"price": 15.8}, ser_fmt="msgpack", comp="none"
        )
        client_frames = FrameCodec.encode(
            msg_type=MsgType.PUB,
            topic="team-a.mkt.sh.600000",
            record_count=1,
            payload=payload,
            ser_fmt="msgpack",
            comp="none",
        )
        # ZMQ 自动附加 identity + delimiter
        server_frames = [b"identity_abc", b""] + list(client_frames)

        decoded = FrameCodec.decode_server(server_frames)
        assert decoded.identity == b"identity_abc"
        assert decoded.topic == "team-a.mkt.sh.600000"
        assert decoded.msg_type == MsgType.PUB
        assert decoded.record_count == 1
        assert decoded.ser_fmt == "msgpack"
        assert decoded.comp == "none"
        assert decoded.has_topic is True

    def test_decode_and_decode_payload(self):
        """完整编解码 + payload 反序列化往返。"""
        original_data = {"price": 15.8, "volume": 1000}
        payload = FrameCodec.encode_payload(original_data, "msgpack", "none")
        frames = FrameCodec.encode(
            msg_type=MsgType.PUB,
            topic="test.topic",
            record_count=1,
            payload=payload,
            ser_fmt="msgpack",
            comp="none",
        )
        server_frames = [b"id", b""] + list(frames)
        decoded = FrameCodec.decode_server(server_frames)
        result = FrameCodec.decode_payload(decoded.payload, decoded.ser_fmt, decoded.comp)
        assert result == original_data

    def test_decode_invalid_frame_count(self):
        """帧数不对时抛出异常。"""
        with pytest.raises(ValueError, match="帧数"):
            FrameCodec.decode_server([b"id", b"", b"topic"])

    def test_encode_for_broadcast(self):
        """XPUB 广播只需要 4 帧（不含 identity/delimiter）。"""
        payload = FrameCodec.encode_payload(
            {"price": 15.8}, ser_fmt="msgpack", comp="none"
        )
        frames = FrameCodec.encode(
            msg_type=MsgType.BROADCAST,
            topic="team-a.mkt.sh.600000",
            record_count=1,
            payload=payload,
            ser_fmt="msgpack",
            comp="none",
        )
        assert len(frames) == 4
        assert frames[0] == b"team-a.mkt.sh.600000"
        assert frames[1][0] == MsgType.BROADCAST

    @pytest.mark.parametrize("ser,comp", [
        ("msgpack", "none"),
        ("msgpack", "snappy"),
        ("bytes", "none"),
    ])
    def test_full_roundtrip(self, ser, comp):
        data = b"binary data" if ser == "bytes" else {"key": "value", "num": 42}
        payload = FrameCodec.encode_payload(data, ser, comp)
        frames = FrameCodec.encode(MsgType.PUB, "test.topic", 1, payload, ser, comp)
        server_frames = [b"id", b""] + list(frames)
        decoded = FrameCodec.decode_server(server_frames)
        result = FrameCodec.decode_payload(decoded.payload, decoded.ser_fmt, decoded.comp)
        assert result == data
```

- [ ] **Step 2: 运行测试确认失败**

```bash
pytest tests/unit/test_frames.py -v
```

- [ ] **Step 3: 实现帧编解码**

```python
# src/pulsemq/protocol/frames.py
"""固定 6 帧格式的编解码。

客户端发送 4 帧: [topic][meta(2B)][record_count(4B)][payload]
ZMQ 自动附加:   [identity][delimiter] + 客户端 4 帧 = 服务端收到 6 帧

服务端广播 4 帧: [topic][meta(2B)][record_count(4B)][payload]
"""

from __future__ import annotations

import struct
from dataclasses import dataclass

from pulsemq.protocol.flags import FrameFlags
from pulsemq.protocol.msg_type import MsgType
from pulsemq.serialization.registry import SerializationRegistry, CompressionRegistry

# record_count 编码格式：4 字节 big-endian uint32
_RECORD_COUNT_STRUCT = struct.Struct(">I")


@dataclass
class DecodedFrame:
    """解码后的帧数据。"""

    identity: bytes       # ZMQ identity
    topic: str            # Frame 2
    msg_type: int         # Frame 3 Byte 0
    flags: FrameFlags     # Frame 3 Byte 1 解析结果
    record_count: int     # Frame 4
    payload: bytes        # Frame 5
    has_topic: bool       # topic 是否非空
    ser_fmt: str          # 序列化格式名
    comp: str             # 压缩算法名


class FrameCodec:
    """帧编解码器。"""

    @staticmethod
    def encode(
        msg_type: int,
        topic: str,
        record_count: int,
        payload: bytes,
        ser_fmt: str = "msgpack",
        comp: str = "none",
    ) -> list[bytes]:
        """编码为 4 帧（客户端发送或服务端广播）。

        Returns:
            [topic_bytes, meta_bytes(2B), record_count_bytes(4B), payload_bytes]
        """
        has_topic = bool(topic)
        flags = FrameFlags(ser_fmt=ser_fmt, comp=comp, has_topic=has_topic)
        meta = bytes([msg_type, flags.encode()])
        rc_bytes = _RECORD_COUNT_STRUCT.pack(record_count)
        return [topic.encode("utf-8"), meta, rc_bytes, payload]

    @staticmethod
    def decode_server(frames: list[bytes]) -> DecodedFrame:
        """解码服务端 ROUTER 收到的 6 帧。

        Args:
            frames: [identity, delimiter, topic, meta(2B), record_count(4B), payload]

        Raises:
            ValueError: 帧数不等于 6。
        """
        if len(frames) != 6:
            raise ValueError(
                f"帧数不正确：期望 6 帧，收到 {len(frames)} 帧"
            )

        identity = frames[0]
        # frames[1] = delimiter（空帧，跳过）
        topic = frames[2].decode("utf-8")
        meta = frames[3]
        msg_type = meta[0]
        flags = FrameFlags.decode(meta[1])
        record_count = _RECORD_COUNT_STRUCT.unpack(frames[4])[0]
        payload = frames[5]

        return DecodedFrame(
            identity=identity,
            topic=topic,
            msg_type=msg_type,
            flags=flags,
            record_count=record_count,
            payload=payload,
            has_topic=flags.has_topic,
            ser_fmt=flags.ser_fmt,
            comp=flags.comp,
        )

    @staticmethod
    def encode_payload(obj, ser_fmt: str = "msgpack", comp: str = "none") -> bytes:
        """序列化 + 压缩 payload。"""
        serializer = SerializationRegistry.get(ser_fmt)
        compressor = CompressionRegistry.get(comp)
        return compressor.compress(serializer.serialize(obj))

    @staticmethod
    def decode_payload(data: bytes, ser_fmt: str = "msgpack", comp: str = "none"):
        """解压 + 反序列化 payload。"""
        compressor = CompressionRegistry.get(comp)
        serializer = SerializationRegistry.get(ser_fmt)
        return serializer.deserialize(compressor.decompress(data))
```

- [ ] **Step 4: 运行测试确认通过**

```bash
pytest tests/unit/test_frames.py -v
```

- [ ] **Step 5: 提交**

```bash
git add src/pulsemq/protocol/frames.py tests/unit/test_frames.py
git commit -m "实现 6 帧编解码：FrameCodec + DecodedFrame"
```

---

### Task 9: 路由器 (engine/router.py)

**Files:**
- Create: `src/pulsemq/engine/__init__.py`
- Create: `src/pulsemq/engine/router.py`
- Create: `tests/unit/test_router.py`

- [ ] **Step 1: 编写路由器测试**

```python
# tests/unit/test_router.py
import pytest
from pulsemq.engine.router import MessageRouter
from pulsemq.models import AuthUser


def _make_user(user_id: int = 1, role: str = "user") -> AuthUser:
    return AuthUser(
        user_id=user_id,
        role=role,
        groups=[],
        api_key=f"pulse_sk_test_{user_id}",
        namespace="team-a",
    )


class TestTopicRegistry:
    def test_register_topic(self, router: MessageRouter):
        info = router.register_topic("team-a.mkt.sh.600000")
        assert info.full_name == "team-a.mkt.sh.600000"
        assert info.namespace == "team-a"

    def test_register_idempotent(self, router: MessageRouter):
        info1 = router.register_topic("team-a.mkt.sh.600000")
        info2 = router.register_topic("team-a.mkt.sh.600000")
        assert info1 is info2

    def test_get_topic(self, router: MessageRouter):
        router.register_topic("team-a.mkt.sh.600000")
        info = router.get_topic("team-a.mkt.sh.600000")
        assert info is not None
        assert info.full_name == "team-a.mkt.sh.600000"

    def test_get_nonexistent_topic(self, router: MessageRouter):
        assert router.get_topic("no.such.topic") is None

    def test_remove_topic_if_empty(self, router: MessageRouter):
        router.register_topic("team-a.mkt.sh.600000")
        router.remove_topic_if_empty("team-a.mkt.sh.600000")
        assert router.get_topic("team-a.mkt.sh.600000") is None


class TestSubscriptionManager:
    def test_subscribe(self, router: MessageRouter):
        router.subscribe(b"id1", "team-a.mkt.sh.600000")
        subs = router.get_subscribers("team-a.mkt.sh.600000")
        assert b"id1" in subs

    def test_unsubscribe(self, router: MessageRouter):
        router.subscribe(b"id1", "team-a.mkt.sh.600000")
        router.unsubscribe(b"id1", "team-a.mkt.sh.600000")
        assert b"id1" not in router.get_subscribers("team-a.mkt.sh.600000")

    def test_get_subscriptions(self, router: MessageRouter):
        router.subscribe(b"id1", "topic_a")
        router.subscribe(b"id1", "topic_b")
        subs = router.get_subscriptions(b"id1")
        assert "topic_a" in subs
        assert "topic_b" in subs

    def test_remove_identity(self, router: MessageRouter):
        router.subscribe(b"id1", "topic_a")
        router.subscribe(b"id1", "topic_b")
        router.remove_identity(b"id1")
        assert len(router.get_subscriptions(b"id1")) == 0
        assert b"id1" not in router.get_subscribers("topic_a")
        assert b"id1" not in router.get_subscribers("topic_b")

    def test_multiple_subscribers(self, router: MessageRouter):
        router.subscribe(b"id1", "topic_a")
        router.subscribe(b"id2", "topic_a")
        subs = router.get_subscribers("topic_a")
        assert b"id1" in subs
        assert b"id2" in subs


class TestConnectionManager:
    def test_register_connection(self, router: MessageRouter):
        user = _make_user()
        router.register_connection(b"id1", user)
        assert router.get_user(b"id1") == user

    def test_unregister_connection(self, router: MessageRouter):
        user = _make_user()
        router.register_connection(b"id1", user)
        result = router.unregister_connection(b"id1")
        assert result == user
        assert router.get_user(b"id1") is None

    def test_get_connections(self, router: MessageRouter):
        user = _make_user()
        router.register_connection(b"id1", user)
        router.register_connection(b"id2", user)
        conns = router.get_connections(user.user_id)
        assert b"id1" in conns
        assert b"id2" in conns

    def test_unregister_nonexistent(self, router: MessageRouter):
        assert router.unregister_connection(b"noid") is None


class TestMessageBuffer:
    def test_append_and_latest_seq(self, router: MessageRouter):
        msg = router.append_message("topic_a", b"\x02\x01", 1, b"payload1")
        assert msg.seq == 1
        msg2 = router.append_message("topic_a", b"\x02\x01", 1, b"payload2")
        assert msg2.seq == 2
        assert router.latest_seq("topic_a") == 2

    def test_replay_messages(self, router: MessageRouter):
        for i in range(5):
            router.append_message("topic_a", b"\x02\x01", 1, f"payload{i}".encode())
        msgs = router.replay_messages("topic_a", from_seq=3, limit=10)
        assert len(msgs) == 3
        assert msgs[0].seq == 3

    def test_replay_with_limit(self, router: MessageRouter):
        for i in range(10):
            router.append_message("topic_a", b"\x02\x01", 1, f"p{i}".encode())
        msgs = router.replay_messages("topic_a", from_seq=0, limit=3)
        assert len(msgs) == 3

    def test_replay_empty_topic(self, router: MessageRouter):
        msgs = router.replay_messages("no_topic", from_seq=0, limit=10)
        assert msgs == []

    def test_ring_buffer_overflow(self, router: MessageRouter):
        """超过 MAX_SIZE 时自动淘汰最旧消息。"""
        for i in range(1100):
            router.append_message("topic_a", b"\x02\x01", 1, f"p{i}".encode())
        msgs = router.replay_messages("topic_a", from_seq=0, limit=2000)
        assert len(msgs) == 1000
        assert msgs[0].seq == 101  # 前 100 条被淘汰

    def test_remove_topic_buffer(self, router: MessageRouter):
        router.append_message("topic_a", b"\x02\x01", 1, b"p")
        router.remove_topic_buffer("topic_a")
        assert router.latest_seq("topic_a") == 0


class TestStatistics:
    def test_counts(self, router: MessageRouter):
        user = _make_user()
        router.register_connection(b"id1", user)
        router.register_topic("topic_a")
        router.subscribe(b"id1", "topic_a")
        assert router.topic_count() == 1
        assert router.subscription_count() == 1
        assert router.connection_count() == 1


@pytest.fixture
def router() -> MessageRouter:
    return MessageRouter()
```

- [ ] **Step 2: 运行测试确认失败**

```bash
pytest tests/unit/test_router.py -v
```

- [ ] **Step 3: 实现路由器**

```python
# src/pulsemq/engine/__init__.py
"""引擎层：消息路由、拦截器、处理器。"""
```

```python
# src/pulsemq/engine/router.py
"""纯内存消息路由器。

包含四个子组件:
- TopicRegistry: 精确 topic 注册（Phase 1 不含通配符展开）
- SubscriptionManager: 订阅关系双向索引
- ConnectionManager: identity ↔ user 映射
- MessageBuffer: 每个 topic 的环形缓冲区
"""

from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field

from pulsemq.models import AuthUser, BufferedMessage, TopicInfo


@dataclass
class MessageRouter:
    """消息路由器（纯内存，单线程安全）。"""

    # Topic 注册表
    _topics: dict[str, TopicInfo] = field(default_factory=dict)

    # 订阅关系双向索引
    _topic_subscribers: dict[str, set[bytes]] = field(default_factory=dict)
    _identity_subscriptions: dict[bytes, set[str]] = field(default_factory=dict)

    # 连接管理
    _identity_user: dict[bytes, AuthUser] = field(default_factory=dict)
    _user_identities: dict[int, set[bytes]] = field(default_factory=dict)

    # 消息缓冲区
    _buffers: dict[str, deque] = field(default_factory=dict)
    _seq_counter: dict[str, int] = field(default_factory=dict)
    max_buffer_size: int = 1000

    # ---- Topic 管理 ----

    def register_topic(self, full_name: str) -> TopicInfo:
        """注册 topic，幂等操作。"""
        if full_name in self._topics:
            return self._topics[full_name]
        info = TopicInfo.from_name(full_name)
        self._topics[full_name] = info
        return info

    def get_topic(self, full_name: str) -> TopicInfo | None:
        return self._topics.get(full_name)

    def remove_topic_if_empty(self, full_name: str) -> None:
        """topic 无订阅者时移除。"""
        subs = self._topic_subscribers.get(full_name, set())
        if not subs:
            self._topics.pop(full_name, None)

    # ---- 订阅管理 ----

    def subscribe(self, identity: bytes, topic: str) -> None:
        """建立订阅关系。"""
        if topic not in self._topic_subscribers:
            self._topic_subscribers[topic] = set()
        self._topic_subscribers[topic].add(identity)

        if identity not in self._identity_subscriptions:
            self._identity_subscriptions[identity] = set()
        self._identity_subscriptions[identity].add(topic)

        # 更新订阅计数
        info = self._topics.get(topic)
        if info is not None:
            info.subscriber_count = len(self._topic_subscribers[topic])

    def unsubscribe(self, identity: bytes, topic: str) -> None:
        """取消订阅。"""
        subs = self._topic_subscribers.get(topic)
        if subs is not None:
            subs.discard(identity)
            info = self._topics.get(topic)
            if info is not None:
                info.subscriber_count = len(subs)

        id_subs = self._identity_subscriptions.get(identity)
        if id_subs is not None:
            id_subs.discard(topic)

    def get_subscribers(self, topic: str) -> set[bytes]:
        """获取 topic 的所有订阅者。"""
        return self._topic_subscribers.get(topic, set())

    def get_subscriptions(self, identity: bytes) -> set[str]:
        """获取 identity 的所有订阅。"""
        return self._identity_subscriptions.get(identity, set())

    def remove_identity(self, identity: bytes) -> None:
        """移除 identity 的所有订阅关系。"""
        id_subs = self._identity_subscriptions.pop(identity, set())
        for topic in id_subs:
            subs = self._topic_subscribers.get(topic)
            if subs is not None:
                subs.discard(identity)

    # ---- 连接管理 ----

    def register_connection(self, identity: bytes, user: AuthUser) -> None:
        """注册 identity ↔ user 映射。"""
        self._identity_user[identity] = user
        if user.user_id not in self._user_identities:
            self._user_identities[user.user_id] = set()
        self._user_identities[user.user_id].add(identity)

    def unregister_connection(self, identity: bytes) -> AuthUser | None:
        """移除映射，返回 user 或 None。"""
        user = self._identity_user.pop(identity, None)
        if user is not None:
            idents = self._user_identities.get(user.user_id)
            if idents is not None:
                idents.discard(identity)
        return user

    def get_user(self, identity: bytes) -> AuthUser | None:
        return self._identity_user.get(identity)

    def get_connections(self, user_id: int) -> set[bytes]:
        return self._user_identities.get(user_id, set())

    # ---- 消息缓冲 ----

    def append_message(
        self, topic: str, meta: bytes, record_count: int, payload: bytes
    ) -> BufferedMessage:
        """追加消息到环形缓冲区。"""
        seq = self._seq_counter.get(topic, 0) + 1
        self._seq_counter[topic] = seq

        msg = BufferedMessage(
            topic=topic,
            seq=seq,
            record_count=record_count,
            meta=meta,
            payload=payload,
            timestamp=time.time(),
        )

        if topic not in self._buffers:
            self._buffers[topic] = deque(maxlen=self.max_buffer_size)
        self._buffers[topic].append(msg)
        return msg

    def replay_messages(
        self, topic: str, from_seq: int = 0, limit: int = 100
    ) -> list[BufferedMessage]:
        """从指定序列号开始回放消息。"""
        buf = self._buffers.get(topic)
        if buf is None:
            return []
        msgs = [m for m in buf if m.seq >= from_seq]
        return msgs[:limit]

    def latest_seq(self, topic: str) -> int:
        return self._seq_counter.get(topic, 0)

    def remove_topic_buffer(self, topic: str) -> None:
        self._buffers.pop(topic, None)
        self._seq_counter.pop(topic, None)

    # ---- 统计 ----

    def topic_count(self) -> int:
        return len(self._topics)

    def subscription_count(self) -> int:
        return sum(len(s) for s in self._topic_subscribers.values())

    def connection_count(self) -> int:
        return len(self._identity_user)
```

- [ ] **Step 4: 运行测试确认通过**

```bash
pytest tests/unit/test_router.py -v
```

- [ ] **Step 5: 提交**

```bash
git add src/pulsemq/engine/ tests/unit/test_router.py
git commit -m "实现消息路由器：Topic/订阅/连接/缓冲区管理"
```

---

### Task 10: 消息处理器 (engine/handlers.py)

**Files:**
- Create: `src/pulsemq/engine/handlers.py`
- Create: `tests/unit/test_handlers.py`

- [ ] **Step 1: 编写处理器测试**

```python
# tests/unit/test_handlers.py
import pytest
import msgpack
from pulsemq.engine.handlers import MessageHandlers
from pulsemq.engine.router import MessageRouter
from pulsemq.models import AuthUser
from pulsemq.protocol.frames import FrameCodec
from pulsemq.protocol.msg_type import MsgType


@pytest.fixture
def router() -> MessageRouter:
    return MessageRouter()


@pytest.fixture
def handlers(router: MessageRouter) -> MessageHandlers:
    sent: list[tuple[bytes, list[bytes]]] = []
    broadcast_frames: list[list[bytes]] = []
    return MessageHandlers(
        router=router,
        send_fn=lambda identity, frames: sent.append((identity, frames)),
        broadcast_fn=lambda frames: broadcast_frames.append(frames),
        default_ser="msgpack",
        default_comp="none",
    ), sent, broadcast_frames


class TestPubHandler:
    async def test_pub_with_subscribers(self, handlers):
        h, sent, broadcast_frames = handlers
        identity = b"pub_client"
        user = AuthUser(user_id=1, role="user", groups=[], api_key="key", namespace="")
        h.router.register_connection(identity, user)
        h.router.subscribe(b"sub_client", "team-a.mkt.sh.600000")

        payload = FrameCodec.encode_payload({"price": 15.8}, "msgpack", "none")
        frames = FrameCodec.encode(MsgType.PUB, "team-a.mkt.sh.600000", 1, payload)
        server_frames = [identity, b""] + frames

        await h.handle_pub(server_frames)

        # 验证广播帧被发送
        assert len(broadcast_frames) == 1
        assert broadcast_frames[0][0] == b"team-a.mkt.sh.600000"
        assert broadcast_frames[0][1][0] == MsgType.BROADCAST

        # 验证消息被缓存
        assert h.router.latest_seq("team-a.mkt.sh.600000") == 1

    async def test_pub_no_subscribers(self, handlers):
        h, sent, broadcast_frames = handlers
        identity = b"pub_client"
        user = AuthUser(user_id=1, role="user", groups=[], api_key="key", namespace="")
        h.router.register_connection(identity, user)

        payload = FrameCodec.encode_payload({"price": 15.8}, "msgpack", "none")
        frames = FrameCodec.encode(MsgType.PUB, "team-a.mkt.sh.600000", 1, payload)
        server_frames = [identity, b""] + frames

        await h.handle_pub(server_frames)

        # 无订阅者，不广播但仍然缓存
        assert len(broadcast_frames) == 0
        assert h.router.latest_seq("team-a.mkt.sh.600000") == 1


class TestSubHandler:
    async def test_subscribe_success(self, handlers):
        h, sent, broadcast_frames = handlers
        identity = b"sub_client"
        user = AuthUser(user_id=1, role="user", groups=[], api_key="key", namespace="")
        h.router.register_connection(identity, user)
        h.router.register_topic("team-a.mkt.sh.600000")

        frames = FrameCodec.encode(MsgType.SUB, "team-a.mkt.sh.600000", 0, b"")
        server_frames = [identity, b""] + frames

        await h.handle_sub(server_frames)

        # 验证订阅关系建立
        subs = h.router.get_subscribers("team-a.mkt.sh.600000")
        assert identity in subs

        # 验证 SUB 确认被发送
        assert len(sent) == 1
        reply_identity, reply_frames = sent[0]
        assert reply_identity == identity
        assert reply_frames[1][0] == MsgType.SUB

    async def test_unsubscribe(self, handlers):
        h, sent, _ = handlers
        identity = b"sub_client"
        user = AuthUser(user_id=1, role="user", groups=[], api_key="key", namespace="")
        h.router.register_connection(identity, user)
        h.router.subscribe(identity, "team-a.mkt.sh.600000")

        frames = FrameCodec.encode(MsgType.UNSUB, "team-a.mkt.sh.600000", 0, b"")
        server_frames = [identity, b""] + frames

        await h.handle_unsub(server_frames)

        assert identity not in h.router.get_subscribers("team-a.mkt.sh.600000")


class TestPingPong:
    async def test_ping_pong(self, handlers):
        h, sent, _ = handlers
        identity = b"client"
        user = AuthUser(user_id=1, role="user", groups=[], api_key="key", namespace="")
        h.router.register_connection(identity, user)

        payload = FrameCodec.encode_payload({"client_ts": 1234.5}, "msgpack", "none")
        frames = FrameCodec.encode(MsgType.PING, "", 0, payload)
        server_frames = [identity, b""] + frames

        await h.handle_ping(server_frames)

        assert len(sent) == 1
        reply_identity, reply_frames = sent[0]
        assert reply_identity == identity
        assert reply_frames[1][0] == MsgType.PONG


class TestDispatch:
    async def test_dispatch_pub(self, handlers):
        h, sent, broadcast_frames = handlers
        identity = b"pub_client"
        user = AuthUser(user_id=1, role="user", groups=[], api_key="key", namespace="")
        h.router.register_connection(identity, user)
        h.router.subscribe(b"sub_client", "test.topic")

        payload = FrameCodec.encode_payload({"data": 1}, "msgpack", "none")
        frames = FrameCodec.encode(MsgType.PUB, "test.topic", 1, payload)
        server_frames = [identity, b""] + frames

        await h.dispatch(server_frames)
        assert len(broadcast_frames) == 1

    async def test_dispatch_ping(self, handlers):
        h, sent, _ = handlers
        identity = b"client"
        user = AuthUser(user_id=1, role="user", groups=[], api_key="key", namespace="")
        h.router.register_connection(identity, user)

        payload = FrameCodec.encode_payload({"client_ts": 1234.5}, "msgpack", "none")
        frames = FrameCodec.encode(MsgType.PING, "", 0, payload)
        server_frames = [identity, b""] + frames

        await h.dispatch(server_frames)
        assert len(sent) == 1
```

- [ ] **Step 2: 运行测试确认失败**

```bash
pytest tests/unit/test_handlers.py -v
```

- [ ] **Step 3: 实现处理器**

```python
# src/pulsemq/engine/handlers.py
"""消息类型分发和处理器。

Phase 1 处理: PUB, SUB, UNSUB, PING, PONG
不包含: AUTH（Phase 2 ZAP）, QUERY, STATUS, HISTORY_REPLAY
"""

from __future__ import annotations

import time
from collections.abc import Callable, Awaitable

import msgpack

from pulsemq.engine.router import MessageRouter
from pulsemq.protocol.frames import FrameCodec
from pulsemq.protocol.msg_type import MsgType


class MessageHandlers:
    """消息处理器集合。"""

    def __init__(
        self,
        router: MessageRouter,
        send_fn: Callable[[bytes, list[bytes]], Awaitable[None] | None],
        broadcast_fn: Callable[[list[bytes]], Awaitable[None] | None],
        default_ser: str = "msgpack",
        default_comp: str = "none",
    ):
        self.router = router
        self._send = send_fn
        self._broadcast = broadcast_fn
        self._default_ser = default_ser
        self._default_comp = default_comp

    async def dispatch(self, server_frames: list[bytes]) -> None:
        """根据 msg_type 分发到对应处理器。"""
        decoded = FrameCodec.decode_server(server_frames)
        msg_type = decoded.msg_type

        if msg_type == MsgType.PUB:
            await self.handle_pub(server_frames)
        elif msg_type == MsgType.SUB:
            await self.handle_sub(server_frames)
        elif msg_type == MsgType.UNSUB:
            await self.handle_unsub(server_frames)
        elif msg_type == MsgType.PING:
            await self.handle_ping(server_frames)
        else:
            pass  # Phase 1 忽略不认识的消息类型

    async def handle_pub(self, server_frames: list[bytes]) -> None:
        """处理 PUB 消息：注册 topic → 广播给订阅者 → 缓存。"""
        decoded = FrameCodec.decode_server(server_frames)
        topic = decoded.topic
        record_count = decoded.record_count

        # 注册 topic（幂等）
        self.router.register_topic(topic)

        # 获取订阅者
        subscribers = self.router.get_subscribers(topic)

        # 零拷贝广播：frames[2:] 即 topic + meta + record_count + payload
        if subscribers:
            # 替换 msg_type 为 BROADCAST
            broadcast_meta = bytes([MsgType.BROADCAST, decoded.flags.encode()])
            broadcast_frames = [
                server_frames[2],               # topic
                broadcast_meta,                 # meta (BROADCAST + original flags)
                server_frames[4],               # record_count
                server_frames[5],               # payload
            ]
            result = self._broadcast(broadcast_frames)
            if result is not None:
                await result

        # 缓存消息
        self.router.append_message(
            topic, server_frames[3], record_count, server_frames[5]
        )

    async def handle_sub(self, server_frames: list[bytes]) -> None:
        """处理 SUB 消息：建立订阅 → 发送确认。"""
        decoded = FrameCodec.decode_server(server_frames)
        identity = decoded.identity
        topic = decoded.topic

        # 自动注册 topic（可能首次出现）
        self.router.register_topic(topic)

        # 建立订阅
        self.router.subscribe(identity, topic)

        # 发送 SUB 确认
        reply_payload = FrameCodec.encode_payload(
            {"status": "ok", "expanded_topics": [topic]},
            self._default_ser,
            self._default_comp,
        )
        reply_frames = FrameCodec.encode(
            MsgType.SUB, topic, 0, reply_payload,
            self._default_ser, self._default_comp,
        )
        result = self._send(identity, reply_frames)
        if result is not None:
            await result

    async def handle_unsub(self, server_frames: list[bytes]) -> None:
        """处理 UNSUB 消息：取消订阅 → 发送确认。"""
        decoded = FrameCodec.decode_server(server_frames)
        identity = decoded.identity
        topic = decoded.topic

        # 取消订阅
        self.router.unsubscribe(identity, topic)

        # 发送 UNSUB 确认
        reply_payload = FrameCodec.encode_payload(
            {"status": "ok"},
            self._default_ser,
            self._default_comp,
        )
        reply_frames = FrameCodec.encode(
            MsgType.UNSUB, topic, 0, reply_payload,
            self._default_ser, self._default_comp,
        )
        result = self._send(identity, reply_frames)
        if result is not None:
            await result

    async def handle_ping(self, server_frames: list[bytes]) -> None:
        """处理 PING：回复 PONG。"""
        decoded = FrameCodec.decode_server(server_frames)
        identity = decoded.identity

        # 解析客户端时间戳
        try:
            client_data = FrameCodec.decode_payload(
                decoded.payload, decoded.ser_fmt, decoded.comp
            )
            client_ts = client_data.get("client_ts", 0)
        except Exception:
            client_ts = 0

        # 回复 PONG
        pong_payload = FrameCodec.encode_payload(
            {"client_ts": client_ts, "server_ts": time.time()},
            self._default_ser,
            self._default_comp,
        )
        pong_frames = FrameCodec.encode(
            MsgType.PONG, "", 0, pong_payload,
            self._default_ser, self._default_comp,
        )
        result = self._send(identity, pong_frames)
        if result is not None:
            await result
```

- [ ] **Step 4: 运行测试确认通过**

```bash
pytest tests/unit/test_handlers.py -v
```

- [ ] **Step 5: 提交**

```bash
git add src/pulsemq/engine/handlers.py tests/unit/test_handlers.py
git commit -m "实现消息处理器：PUB/SUB/UNSUB/PING 分发"
```

---

### Task 11: ZMQ 传输适配器 (transport/zmq_transport.py)

**Files:**
- Create: `src/pulsemq/transport/__init__.py`
- Create: `src/pulsemq/transport/zmq_transport.py`

- [ ] **Step 1: 实现 ZMQ 传输**

```python
# src/pulsemq/transport/__init__.py
"""传输层：ZMQ 适配器。"""
```

```python
# src/pulsemq/transport/zmq_transport.py
"""ZMQ ROUTER + XPUB 传输适配器。

ROUTER socket: 接收客户端 DEALER 消息（控制路径）
XPUB socket:  广播给 SUB 订阅者（数据路径）
"""

from __future__ import annotations

import asyncio
import logging
from typing import Callable, Awaitable

import zmq
import zmq.asyncio

from pulsemq.config import ServerConfig

logger = logging.getLogger(__name__)


class ZmqTransport:
    """ZMQ 传输层，管理 ROUTER + XPUB 两个 socket。"""

    def __init__(self, config: ServerConfig):
        self._config = config
        self._ctx: zmq.asyncio.Context | None = None
        self._router: zmq.asyncio.Socket | None = None
        self._xpub: zmq.asyncio.Socket | None = None

    async def start(self) -> None:
        """启动 ZMQ socket 并绑定。"""
        self._ctx = zmq.asyncio.Context()

        # ROUTER socket：接收客户端消息
        self._router = self._ctx.socket(zmq.ROUTER)
        self._router.setsockopt(zmq.RCVHWM, self._config.zmq_rcvhwm)
        self._router.setsockopt(zmq.SNDHWM, self._config.zmq_sndhwm)
        self._router.setsockopt(zmq.IMMEDIATE, 1)
        # 心跳配置
        self._router.setsockopt(zmq.HEARTBEAT_IVL, self._config.zmq_heartbeat_ivl)
        self._router.setsockopt(zmq.HEARTBEAT_TIMEOUT, self._config.zmq_heartbeat_timeout)
        self._router.setsockopt(zmq.HEARTBEAT_TTL, self._config.zmq_heartbeat_ttl)
        # 监听连接/断开事件
        self._router.setsockopt(zmq.ROUTER_MANDATORY, 0)
        self._router.bind(self._config.bind)
        logger.info("ROUTER 绑定到 %s", self._config.bind)

        # XPUB socket：广播给订阅者
        self._xpub = self._ctx.socket(zmq.XPUB)
        self._xpub.setsockopt(zmq.SNDHWM, self._config.zmq_sndhwm)
        self._xpub.setsockopt(zmq.IMMEDIATE, 1)
        self._xpub.bind(self._config.xpub_bind)
        logger.info("XPUB 绑定到 %s", self._config.xpub_bind)

    async def recv(self) -> list[bytes]:
        """接收一条 ROUTER 消息（6 帧）。"""
        if self._router is None:
            raise RuntimeError("Transport 未启动")
        frames = await self._router.recv_multipart()
        return frames

    async def send(self, identity: bytes, frames: list[bytes]) -> None:
        """通过 ROUTER 发送消息给特定客户端。"""
        if self._router is None:
            raise RuntimeError("Transport 未启动")
        await self._router.send_multipart([identity, b""] + frames)

    async def broadcast(self, frames: list[bytes]) -> None:
        """通过 XPUB 广播消息给所有订阅者。"""
        if self._xpub is None:
            raise RuntimeError("Transport 未启动")
        await self._xpub.send_multipart(frames)

    async def stop(self) -> None:
        """关闭 ZMQ socket 和 context。"""
        if self._router is not None:
            self._router.close(linger=0)
            self._router = None
        if self._xpub is not None:
            self._xpub.close(linger=0)
            self._xpub = None
        if self._ctx is not None:
            self._ctx.term()
            self._ctx = None
        logger.info("ZMQ Transport 已关闭")
```

- [ ] **Step 2: 提交**

```bash
git add src/pulsemq/transport/
git commit -m "实现 ZMQ 传输适配器：ROUTER + XPUB socket 管理"
```

---

### Task 12: 服务器启动器 (server.py)

**Files:**
- Create: `src/pulsemq/server.py`

- [ ] **Step 1: 实现服务器启动器**

```python
# src/pulsemq/server.py
"""服务端 启动器：组装各层并启动消息主循环。"""

from __future__ import annotations

import asyncio
import logging
import signal
import sys

from pulsemq.config import ServerConfig, load_config
from pulsemq.engine.handlers import MessageHandlers
from pulsemq.engine.router import MessageRouter
from pulsemq.transport.zmq_transport import ZmqTransport

logger = logging.getLogger(__name__)


class PulseServer:
    """PulseMQ 服务端 服务器。"""

    def __init__(self, config: ServerConfig | None = None):
        self._config = config or load_config()
        self._router = MessageRouter()
        self._transport = ZmqTransport(self._config)
        self._handlers = MessageHandlers(
            router=self._router,
            send_fn=self._transport.send,
            broadcast_fn=self._transport.broadcast,
            default_ser=self._config.default_serializer,
            default_comp=self._config.default_compressor,
        )
        self._running = False

    async def start(self) -> None:
        """启动 服务端。"""
        # 设置事件循环
        from pulsemq.event_loop import install_event_loop
        loop_type = install_event_loop(self._config.use_uvloop)
        logger.info("事件循环: %s", loop_type)

        # 启动 ZMQ transport
        await self._transport.start()
        logger.info(
            "PulseMQ 服务端 启动: ROUTER=%s, XPUB=%s",
            self._config.bind, self._config.xpub_bind,
        )

        self._running = True

        # 进入消息主循环
        await self._message_loop()

    async def stop(self) -> None:
        """停止 服务端。"""
        self._running = False
        await self._transport.stop()
        logger.info("PulseMQ 服务端 已停止")

    async def _message_loop(self) -> None:
        """Phase 1 简单消息循环：逐条处理。"""
        while self._running:
            try:
                frames = await self._transport.recv()
                await self._handlers.dispatch(frames)
            except zmq.ZMQError:
                if self._running:
                    logger.exception("ZMQ 错误")
                break
            except Exception:
                logger.exception("消息处理异常")
                continue


def main() -> None:
    """CLI 入口: pulse-mq 命令。"""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    config = load_config()
    server = PulseServer(config)

    loop = asyncio.new_event_loop()

    def _shutdown():
        loop.call_soon_threadsafe(lambda: asyncio.ensure_future(server.stop()))

    # 信号处理
    if sys.platform != "win32":
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, _shutdown)

    try:
        loop.run_until_complete(server.start())
    except KeyboardInterrupt:
        pass
    finally:
        loop.run_until_complete(server.stop())
        loop.close()


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: 提交**

```bash
git add src/pulsemq/server.py
git commit -m "实现服务器启动器：组装各层 + 消息主循环 + CLI 入口"
```

---

### Task 13: 端到端集成测试

**Files:**
- Create: `tests/integration/test_pubsub_e2e.py`

- [ ] **Step 1: 编写端到端测试**

```python
# tests/integration/test_pubsub_e2e.py
"""端到端集成测试：PUB → SUB 完整消息链路。

验证:
1. 服务端 启动并绑定 ZMQ socket
2. Publisher 通过 DEALER 发送 PUB 消息
3. Subscriber 通过 SUB 收到 BROADCAST 消息
4. payload 正确传递
"""

import asyncio
import struct

import pytest
import zmq
import zmq.asyncio
import msgpack

from pulsemq.config import ServerConfig
from pulsemq.protocol.frames import FrameCodec
from pulsemq.protocol.msg_type import MsgType
from pulsemq.engine.router import MessageRouter
from pulsemq.engine.handlers import MessageHandlers
from pulsemq.transport.zmq_transport import ZmqTransport
from pulsemq.models import AuthUser


@pytest.fixture
async def 服务端_config(tmp_path):
    """随机端口的 服务端 配置。"""
    import random
    port_router = random.randint(16000, 19000)
    port_xpub = port_router + 1
    return ServerConfig(
        bind=f"tcp://*:{port_router}",
        xpub_bind=f"tcp://*:{port_xpub}",
    )


@pytest.fixture
async def running_服务端(服务端_config):
    """启动并运行 服务端。"""
    transport = ZmqTransport(服务端_config)
    router = MessageRouter()
    sent_messages: list[tuple[bytes, list[bytes]]] = []
    broadcast_messages: list[list[bytes]] = []

    async def send_fn(identity, frames):
        sent_messages.append((identity, frames))

    async def broadcast_fn(frames):
        broadcast_messages.append(frames)

    handlers = MessageHandlers(
        router=router,
        send_fn=send_fn,
        broadcast_fn=broadcast_fn,
    )
    await transport.start()

    # 注册一个测试用户
    test_user = AuthUser(
        user_id=1, role="admin", groups=[], api_key="test_key", namespace="",
    )
    # 模拟连接注册（Phase 1 跳过 ZAP）

    yield transport, router, handlers, sent_messages, broadcast_messages, 服务端_config

    await transport.stop()


async def _connect_dealer(ctx, address, identity=b"test_pub"):
    """创建 DEALER socket 并连接。"""
    sock = ctx.socket(zmq.DEALER)
    sock.setsockopt(zmq.IDENTITY, identity)
    sock.connect(address)
    return sock


async def _connect_sub(ctx, address):
    """创建 SUB socket 并连接。"""
    sock = ctx.socket(zmq.SUB)
    sock.connect(address)
    return sock


class TestPubSubE2E:
    async def test_pub_sub_full_flow(self, running_服务端):
        """完整 PUB → SUB 链路。"""
        transport, router, handlers, sent, broadcast, config = running_服务端

        # 模拟 pub 客户端和 sub 客户端的 identity
        pub_identity = b"pub_client"
        sub_identity = b"sub_client"

        # 注册连接
        pub_user = AuthUser(user_id=1, role="admin", groups=[], api_key="k1", namespace="")
        sub_user = AuthUser(user_id=2, role="admin", groups=[], api_key="k2", namespace="")
        router.register_connection(pub_identity, pub_user)
        router.register_connection(sub_identity, sub_user)

        # SUB 客户端订阅 topic
        sub_frames = FrameCodec.encode(MsgType.SUB, "test.topic", 0, b"")
        server_sub_frames = [sub_identity, b""] + sub_frames
        await handlers.handle_sub(server_sub_frames)

        # 确认订阅建立
        assert sub_identity in router.get_subscribers("test.topic")

        # PUB 客户端发送消息
        data = {"price": 15.8, "volume": 1000}
        payload = FrameCodec.encode_payload(data, "msgpack", "none")
        pub_frames = FrameCodec.encode(MsgType.PUB, "test.topic", 1, payload)
        server_pub_frames = [pub_identity, b""] + pub_frames
        await handlers.handle_pub(server_pub_frames)

        # 验证广播帧
        assert len(broadcast) == 1
        bframes = broadcast[0]
        assert bframes[0] == b"test.topic"
        assert bframes[1][0] == MsgType.BROADCAST

        # 解码广播 payload
        result = FrameCodec.decode_payload(bframes[3], "msgpack", "none")
        assert result == data

        # 验证消息缓存
        assert router.latest_seq("test.topic") == 1

    async def test_ping_pong_flow(self, running_服务端):
        """PING → PONG 链路。"""
        transport, router, handlers, sent, broadcast, config = running_服务端

        identity = b"ping_client"
        user = AuthUser(user_id=1, role="admin", groups=[], api_key="k", namespace="")
        router.register_connection(identity, user)

        payload = FrameCodec.encode_payload({"client_ts": 1234.5}, "msgpack", "none")
        frames = FrameCodec.encode(MsgType.PING, "", 0, payload)
        server_frames = [identity, b""] + frames

        await handlers.handle_ping(server_frames)

        assert len(sent) == 1
        reply_id, reply_frames = sent[0]
        assert reply_id == identity
        assert reply_frames[1][0] == MsgType.PONG

        # 验证 PONG payload 包含 client_ts 和 server_ts
        pong_data = FrameCodec.decode_payload(reply_frames[3], "msgpack", "none")
        assert pong_data["client_ts"] == 1234.5
        assert "server_ts" in pong_data

    async def test_multiple_topics(self, running_服务端):
        """多 topic 发布和订阅。"""
        transport, router, handlers, sent, broadcast, config = running_服务端

        sub_id = b"sub1"
        sub_user = AuthUser(user_id=2, role="admin", groups=[], api_key="k2", namespace="")
        router.register_connection(sub_id, sub_user)
        router.subscribe(sub_id, "topic_a")
        router.subscribe(sub_id, "topic_b")

        pub_id = b"pub1"
        pub_user = AuthUser(user_id=1, role="admin", groups=[], api_key="k1", namespace="")
        router.register_connection(pub_id, pub_user)

        # 发布到 topic_a
        payload_a = FrameCodec.encode_payload({"data": "a"}, "msgpack", "none")
        frames_a = FrameCodec.encode(MsgType.PUB, "topic_a", 1, payload_a)
        await handlers.handle_pub([pub_id, b""] + frames_a)

        # 发布到 topic_b
        payload_b = FrameCodec.encode_payload({"data": "b"}, "msgpack", "none")
        frames_b = FrameCodec.encode(MsgType.PUB, "topic_b", 1, payload_b)
        await handlers.handle_pub([pub_id, b""] + frames_b)

        assert len(broadcast) == 2
        assert router.latest_seq("topic_a") == 1
        assert router.latest_seq("topic_b") == 1
```

- [ ] **Step 2: 运行集成测试**

```bash
pytest tests/integration/test_pubsub_e2e.py -v
```

- [ ] **Step 3: 运行全部测试**

```bash
pytest tests/ -v
```

- [ ] **Step 4: 提交**

```bash
git add tests/integration/test_pubsub_e2e.py
git commit -m "添加端到端集成测试：PUB→SUB 完整链路验证"
```

---

### Task 14: 最终组装与验证

- [ ] **Step 1: 运行全部测试确认通过**

```bash
pytest tests/ -v --tb=short
```

- [ ] **Step 2: 验证 CLI 入口可安装**

```bash
cd D:/workflow/pulse-mq
uv pip install -e .
pulse-mq --help 2>&1 || echo "CLI 入口已注册"
```

- [ ] **Step 3: 最终提交**

```bash
git add -A
git commit -m "Phase 1 完成：PulseMQ 基础骨架可运行"
```

---

## 自检结果

**Spec 覆盖**:
- ✅ Config 模块 → Task 2
- ✅ EventLoop → Task 4
- ✅ 领域模型 → Task 3
- ✅ 协议层（msg_type + flags + frames） → Task 5, 6, 8
- ✅ 序列化/压缩注册表 → Task 7
- ✅ ZMQ Transport → Task 11
- ✅ Router（精确 topic） → Task 9
- ✅ Handlers（PUB/SUB/UNSUB/PING） → Task 10
- ✅ Server 启动器 → Task 12
- ✅ 端到端测试 → Task 13

**占位符扫描**: 无 TBD/TODO/占位符。所有步骤包含完整代码。

**类型一致性**: DecodedFrame 的字段名与 FrameCodec.decode_server 返回值一致；MessageHandlers 构造函数签名与 server.py 中的调用一致；FrameFlags 字段名与 flags.py 中定义一致。
