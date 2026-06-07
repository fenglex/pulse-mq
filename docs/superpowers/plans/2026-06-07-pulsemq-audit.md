# PulseMQ 全面代码审计实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 对 PulseMQ v0.6.0 全部 9 个模块做系统化审计（代码审读 + 单测 + 集成 + e2e + 压测 + 安全 fuzz），定位并修复逻辑 / 功能 / 性能 / 安全隐患，建立可重复的回归测试网与性能基线。

**Architecture:** 四阶段顺序推进 — A 核心路径（protocol/transport/serialization/engine）→ B 客户端+胶水（client/server/config/event_loop/models）→ C 辅助子系统（auth/storage/monitoring）→ D 集成+性能+安全。**审计类任务的特殊性**：发现什么 bug 取决于代码审读结果，本计划将"代码审读 + 写测试 + 修源码"组织成可重复的模板任务，每个模块套一次，每个 bug 单独 commit。

**Tech Stack:** Python 3.13+, pytest, pytest-asyncio, pyzmq, msgpack, snappy, lz4, zstandard, pyarrow, pandas, sqlite3

**Spec:** `docs/superpowers/specs/2026-06-07-pulsemq-audit-design.md`

**前置条件：**
- 当前 e2e 测试 (`scripts/test_e2e_all.py`) 16/16 通过
- 已修复的 3 个 commit 已在 master: handlers 广播 meta / client 通配符订阅 / ping-query 帧索引
- Python 3.13+, uv 已装

---

## File Structure

| 路径 | 状态 | 职责 |
|------|------|------|
| `tests/conftest.py` | 新增 | 共享 fixtures：server 子进程、客户端、PulseMessage 比较 |
| `tests/unit/` | 新增 | pytest 单测目录 |
| `tests/integration/` | 新增 | 多模块集成测试 |
| `tests/security/` | 新增 | ZAP fuzz / SQL 注入 |
| `scripts/bench_concurrent.py` | 新增 | 并发压测 |
| `scripts/bench_soak.py` | 新增 | 4h 长时间 soak |
| `docs/bench-baseline.md` | 新增 | 性能基线数字 |
| `docs/bench-thresholds.md` | 新增 | 告警阈值 |
| `docs/known-issues.md` | 新增 | 非阻塞 bug 记录 |
| `src/pulsemq/<various>.py` | 按需修 | 发现的 bug 直接修源码 |
| `pyproject.toml` | 可能改 | 补 pytest / hypothesis 依赖 |

---

## Task 0: 测试基础设施搭建（前置，所有阶段依赖）

**Files:**
- Create: `tests/conftest.py`
- Create: `tests/__init__.py`
- Create: `tests/unit/__init__.py`
- Create: `tests/integration/__init__.py`
- Create: `tests/security/__init__.py`
- Modify: `pyproject.toml:30-60` (补 pytest 依赖，若缺)

- [ ] **Step 1: 验证 pytest 已在 pyproject**

Run: `cd D:/workflow/pulse-mq && grep -A3 "test\|pytest" pyproject.toml`

Expected: 看到 `[tool.pytest.ini_options]` 与 `testpaths = ["tests"]`，且 `pytest` + `pytest-asyncio` 在 `dependency-groups` 或 `[project.optional-dependencies]`。如果没有，添加：

```toml
[dependency-groups]
dev = [
    "pytest>=8.0",
    "pytest-asyncio>=0.23",
    "hypothesis>=6.0",
]
```

- [ ] **Step 2: 创建测试目录结构**

```bash
cd D:/workflow/pulse-mq
mkdir -p tests/unit tests/integration tests/security
touch tests/__init__.py tests/unit/__init__.py tests/integration/__init__.py tests/security/__init__.py
```

- [ ] **Step 3: 写 conftest.py 共享 fixtures**

创建 `tests/conftest.py`：

```python
"""pytest 共享 fixtures。

提供:
  - server_subprocess: 启一个无认证无指标的 PulseServer 子进程, 输出 READY 后 yield
  - client_factory: 异步客户端工厂 (连接 server_subprocess)
  - 端口分配: 避免并行测试撞端口
"""
from __future__ import annotations

import asyncio
import os
import socket
import subprocess
import sys
import time
from contextlib import asynccontextmanager
from typing import AsyncIterator

import pytest
import pytest_asyncio


def _free_port() -> int:
    """找一个当前空闲的 TCP 端口。"""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
def port_pair() -> tuple[int, int]:
    """分配一对相邻空闲端口 (router, xpub)。"""
    p = _free_port()
    # 确保 p+1 也空闲
    while True:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.bind(("127.0.0.1", p + 1))
                break
            except OSError:
                p = _free_port()
    return p, p + 1


@pytest_asyncio.fixture
async def server_subprocess(port_pair) -> AsyncIterator[subprocess.Popen]:
    """启一个 server_runner 子进程, 等 READY 后 yield。"""
    port, _ = port_pair
    proc = subprocess.Popen(
        [sys.executable, "scripts/test_server_runner.py", "--port", str(port)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
        env={**os.environ, "PYTHONUNBUFFERED": "1", "PYTHONIOENCODING": "utf-8"},
    )
    deadline = time.time() + 10.0
    while time.time() < deadline:
        line = proc.stdout.readline()
        if not line:
            await asyncio.sleep(0.05)
            if proc.poll() is not None:
                stderr = proc.stderr.read() if proc.stderr else ""
                raise RuntimeError(
                    f"server_runner 提前退出 (rc={proc.returncode})\nstderr: {stderr}"
                )
            continue
        if line.strip() == "READY":
            break
    else:
        proc.kill()
        raise TimeoutError("server_runner 启动超时")
    try:
        yield proc
    finally:
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=2)
```

- [ ] **Step 4: 写一个 smoke test 验证 conftest 工作**

创建 `tests/unit/test_conftest_smoke.py`：

```python
"""验证 conftest 的 server_subprocess fixture 可用。"""
from __future__ import annotations

import pytest

from pulsemq.client.async_client import PulseClient


@pytest.mark.asyncio
async def test_server_subprocess_alive(server_subprocess, port_pair):
    """server_subprocess 启动后能正常接收连接。"""
    port, _ = port_pair
    address = f"tcp://localhost:{port}"
    xpub = f"tcp://localhost:{port + 1}"
    async with PulseClient(
        address=address, xpub_address=xpub, auto_reconnect=False
    ) as c:
        # 不需要收到任何消息, 只需要连接成功
        assert c._dealer is not None
        assert c._sub is not None
```

- [ ] **Step 5: 运行 smoke test**

Run: `cd D:/workflow/pulse-mq && uv run pytest tests/unit/test_conftest_smoke.py -v`

Expected: 1 passed

- [ ] **Step 6: 提交**

```bash
git add tests/ pyproject.toml
git commit -m "test: 建立 pytest 测试基础设施 (conftest + fixtures)"
```

---

## Task 1: 阶段 A 启动 — 代码审读 protocol/

**Files:**
- Read: `src/pulsemq/protocol/flags.py`
- Read: `src/pulsemq/protocol/frames.py`
- Read: `src/pulsemq/protocol/msg_type.py`

> 阶段 A 的目标是定位所有可被单元测试捕获的逻辑 bug。每个模块的审计流程相同：
> 1. **审读**源码，记录可疑点
> 2. **写测试** 把可疑点变成断言
> 3. **跑测试** 哪些失败 = 哪些是 bug
> 4. **修源码** 让测试通过
> 5. **commit** 修复（每个 bug 一个 commit）

- [ ] **Step 1: 审读 protocol/flags.py**

重点：
- `FrameFlags.encode()`: ser_bits 0b000~0b100, comp_bits 0b00~0b11, has_topic 1 bit — 共 6 bits 用满，没冲突？
- `FrameFlags.decode(byte_val)`: 用位掩码提取，map 默认值是什么？未知 ser_fmt / comp 静默回退到 msgpack / none — 正确还是 bug？
- `ser_fmt="str"` 的 bit 0b100 占用 3 bits — 边界 0b1000 (5) 没用到，留给将来？

记录到 `docs/known-issues.md`（若是 bug，登记并继续；若是设计选择，注明"经审读无问题"）。

- [ ] **Step 2: 审读 protocol/frames.py**

重点：
- `FrameCodec.encode()` 4 帧固定格式: [topic, meta(2B), rc(4B), payload] — 大小写正确？
- `FrameCodec.decode_server()` 支持 5/6 帧（无/有 delimiter）— 两种路径都覆盖？
- `decode_payload()` 调用 `compressor.decompress(serializer.serialize(obj))` — 顺序正确（先序列化后压缩）？
- `_RECORD_COUNT_STRUCT` 是 `>I` 大端 4 字节 — 与 encode 一致？

- [ ] **Step 3: 审读 protocol/msg_type.py**

重点：
- `MsgType` 枚举值是否与 `handlers.py` 的 switch 一致？
- 是否有未在 handler 中处理的类型？

- [ ] **Step 4: 把可疑点写到 docs/known-issues.md**

格式：

```markdown
# 已知问题登记

## protocol/flags.py
- [审读日期 2026-06-07] FrameFlags.encode() 的 ser_fmt="str" 占用 0b100 (bit 2), 编码空间剩余 0b101~0b111 (3) — 经审读无问题（设计选择，为未来扩展预留）
- [审读日期 2026-06-07] 未知 ser_fmt 静默回退 msgpack — 决定保持 (设计: 容错, 不抛异常)

## protocol/frames.py
...

## protocol/msg_type.py
...
```

- [ ] **Step 5: 提交审读记录**

```bash
git add docs/known-issues.md
git commit -m "docs: 阶段 A 启动, 记录 protocol/ 审读结果"
```

---

## Task 2: 阶段 A — protocol 单测

**Files:**
- Create: `tests/unit/test_protocol_flags.py`
- Create: `tests/unit/test_protocol_frames.py`
- Create: `tests/unit/test_protocol_msg_type.py`

> 把 Task 1 审读时的"可疑点"变成 pytest 断言。每个测试对应一个行为。
> 如果测试失败 = 找到 bug，按 Task 4 流程修。

- [ ] **Step 1: 写 test_protocol_flags.py**

```python
"""FrameFlags 编码/解码 单测。"""
from __future__ import annotations

import pytest

from pulsemq.protocol.flags import FrameFlags


def test_encode_decode_str_none():
    """str + none + has_topic=True 往返。"""
    f = FrameFlags(ser_fmt="str", comp="none", has_topic=True)
    encoded = f.encode()
    assert isinstance(encoded, int)
    assert 0 <= encoded < 256
    decoded = FrameFlags.decode(encoded)
    assert decoded.ser_fmt == "str"
    assert decoded.comp == "none"
    assert decoded.has_topic is True


def test_encode_decode_all_combinations():
    """5 种 ser × 4 种 comp × 2 种 has_topic = 40 组合全部往返。"""
    sers = ["msgpack", "bytes", "pyarrow", "protobuf", "str"]
    comps = ["none", "snappy", "lz4", "zstd"]
    for ser in sers:
        for comp in comps:
            for has_topic in (True, False):
                f = FrameFlags(ser_fmt=ser, comp=comp, has_topic=has_topic)
                d = FrameFlags.decode(f.encode())
                assert d.ser_fmt == ser, f"ser mismatch for {ser}/{comp}/{has_topic}"
                assert d.comp == comp, f"comp mismatch for {ser}/{comp}/{has_topic}"
                assert d.has_topic is has_topic


def test_decode_unknown_ser_defaults_to_msgpack():
    """未知 ser_bits 应回退到 msgpack (设计选择, 测其行为)。"""
    # 0b1111_1111 = 255, 包含无效 ser_bits=0b111
    f = FrameFlags.decode(0xFF)
    assert f.ser_fmt == "msgpack"  # 静默回退


def test_decode_unknown_comp_defaults_to_none():
    """未知 comp_bits 应回退到 none。"""
    f = FrameFlags.decode(0xFF)
    assert f.comp == "none"


@pytest.mark.parametrize("byte_val", [0, 1, 127, 128, 255])
def test_decode_never_crashes(byte_val):
    """任何单字节都不应让 decode 抛异常。"""
    f = FrameFlags.decode(byte_val)
    assert f is not None
```

- [ ] **Step 2: 写 test_protocol_frames.py**

```python
"""FrameCodec 帧编解码 单测。"""
from __future__ import annotations

import struct

import pytest

from pulsemq.protocol.flags import FrameFlags
from pulsemq.protocol.frames import FrameCodec
from pulsemq.protocol.msg_type import MsgType


def test_encode_4_frames():
    """encode 返回 4 帧: [topic, meta(2B), rc(4B), payload]。"""
    frames = FrameCodec.encode(MsgType.PUB, "test.t", 5, b"hello")
    assert len(frames) == 4
    assert frames[0] == b"test.t"
    assert len(frames[1]) == 2  # meta
    assert len(frames[2]) == 4  # rc
    assert frames[3] == b"hello"


def test_meta_byte_layout():
    """meta[0]=msg_type, meta[1]=flags_byte。"""
    f = FrameFlags(ser_fmt="str", comp="none", has_topic=True)
    frames = FrameCodec.encode(MsgType.PUB, "t", 1, b"", "str", "none")
    msg_type, flags_byte = frames[1][0], frames[1][1]
    assert msg_type == MsgType.PUB
    assert flags_byte == f.encode()


def test_record_count_big_endian():
    """rc 是大端 4 字节 uint32。"""
    frames = FrameCodec.encode(MsgType.PUB, "t", 0x01020304, b"")
    assert frames[2] == b"\x01\x02\x03\x04"


def test_decode_server_5_frames():
    """decode_server 处理 5 帧 (无 delimiter)。"""
    frames = FrameCodec.encode(MsgType.PUB, "t", 1, b"hello", "str", "none")
    server_frames = [b"identity-uuid", *frames]  # 5 帧
    decoded = FrameCodec.decode_server(server_frames)
    assert decoded.identity == b"identity-uuid"
    assert decoded.topic == "t"
    assert decoded.msg_type == MsgType.PUB
    assert decoded.ser_fmt == "str"
    assert decoded.comp == "none"
    assert decoded.record_count == 1
    assert decoded.payload == b"hello"


def test_decode_server_6_frames_with_delimiter():
    """decode_server 处理 6 帧 (含 delimiter)。"""
    frames = FrameCodec.encode(MsgType.PUB, "t", 1, b"hello", "str", "none")
    server_frames = [b"identity-uuid", b"", *frames]  # 6 帧
    decoded = FrameCodec.decode_server(server_frames)
    assert decoded.identity == b"identity-uuid"
    assert decoded.topic == "t"
    assert decoded.ser_fmt == "str"


def test_decode_server_wrong_frame_count_raises():
    """帧数不在 5-6 抛 ValueError。"""
    with pytest.raises(ValueError, match="帧数不正确"):
        FrameCodec.decode_server([b"x", b"y", b"z"])  # 3 帧


def test_encode_decode_payload_roundtrip_all_ser_comp():
    """16 组合 payload roundtrip。"""
    import os
    test_obj = {"k": "v-中文-🚀", "n": 42, "b": os.urandom(8)}
    sers = ["msgpack", "bytes", "str", "pyarrow"]
    comps = ["none", "snappy", "lz4", "zstd"]
    for ser in sers:
        for comp in comps:
            if ser == "str":
                obj = "test string"
            elif ser == "bytes":
                obj = b"test bytes"
            else:
                # msgpack / pyarrow 需要可序列化对象
                obj = test_obj if ser == "msgpack" else None
                if ser == "pyarrow":
                    import pandas as pd
                    obj = pd.DataFrame({"a": [1, 2]})
            enc = FrameCodec.encode_payload(obj, ser, comp)
            dec = FrameCodec.decode_payload(enc, ser, comp)
            if ser == "pyarrow":
                import pandas as pd
                pd.testing.assert_frame_equal(dec.to_pandas(), obj)
            else:
                assert dec == obj, f"roundtrip failed: {ser}/{comp}"
```

- [ ] **Step 3: 写 test_protocol_msg_type.py**

```python
"""MsgType 枚举 单测。"""
from __future__ import annotations

from pulsemq.protocol.msg_type import MsgType


def test_msg_types_distinct():
    """所有 msg_type 值唯一。"""
    values = [getattr(MsgType, name) for name in dir(MsgType)
              if not name.startswith("_") and isinstance(getattr(MsgType, name), int)]
    assert len(values) == len(set(values)), f"重复 msg_type: {values}"


def test_msg_types_in_byte_range():
    """msg_type 必须能用单字节表示。"""
    for name in dir(MsgType):
        if name.startswith("_"):
            continue
        v = getattr(MsgType, name)
        if isinstance(v, int):
            assert 0 <= v < 256, f"{name}={v} 超出单字节范围"
```

- [ ] **Step 4: 跑全部 protocol 测试**

Run: `cd D:/workflow/pulse-mq && uv run pytest tests/unit/test_protocol_*.py -v`

Expected: 全绿。如有失败 = 找到 bug，进入 Task 4 修复。

- [ ] **Step 5: 提交测试**

```bash
git add tests/unit/test_protocol_*.py
git commit -m "test: protocol 模块单测 (flags, frames, msg_type)"
```

---

## Task 3: 阶段 A — serialization 单测

**Files:**
- Create: `tests/unit/test_serialization_registry.py`
- Read: `src/pulsemq/serialization/registry.py`

- [ ] **Step 1: 审读 serialization/registry.py**

记录每个 serializer / compressor 的：
- 接受什么输入
- 输出什么
- 错误路径
- 空对象 / 边界

- [ ] **Step 2: 写 test_serialization_registry.py**

```python
"""SerializationRegistry 与 CompressionRegistry 单测。"""
from __future__ import annotations

import os

import pytest

from pulsemq.serialization.registry import (
    SerializationRegistry,
    CompressionRegistry,
    MsgpackSerializer,
    StrSerializer,
    BytesSerializer,
    PyArrowSerializer,
    ProtobufSerializer,
    NoCompressor,
    SnappyCompressor,
    Lz4Compressor,
    ZstdCompressor,
)


# ---- 序列化器 ----

def test_str_serializer_roundtrip():
    s = StrSerializer()
    assert s.deserialize(s.serialize("hello-世界")) == "hello-世界"


def test_str_serializer_rejects_int():
    s = StrSerializer()
    with pytest.raises(TypeError):
        s.serialize(42)


def test_bytes_serializer_roundtrip():
    s = BytesSerializer()
    data = os.urandom(64)
    assert s.deserialize(s.serialize(data)) == data


def test_msgpack_serializer_roundtrip():
    s = MsgpackSerializer()
    obj = {"a": [1, 2, 3], "b": "中文"}
    decoded = s.deserialize(s.serialize(obj))
    assert decoded == obj


def test_pyarrow_serializer_dataframe():
    import pandas as pd
    s = PyArrowSerializer()
    df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
    enc = s.serialize(df)
    dec = s.deserialize(enc)
    pd.testing.assert_frame_equal(dec.to_pandas(), df)


# ---- 压缩器 ----

def test_no_compressor_roundtrip():
    c = NoCompressor()
    data = b"hello"
    assert c.decompress(c.compress(data)) == data


def test_snappy_compressor_roundtrip():
    c = SnappyCompressor()
    data = b"a" * 1024
    assert c.decompress(c.compress(data)) == data


def test_lz4_compressor_roundtrip():
    c = Lz4Compressor()
    data = b"b" * 1024
    assert c.decompress(c.compress(data)) == data


def test_zstd_compressor_roundtrip():
    c = ZstdCompressor()
    data = b"c" * 1024
    assert c.decompress(c.compress(data)) == data


def test_zstd_actually_compresses():
    """高度可压缩的数据应能压缩到比原数据小。"""
    c = ZstdCompressor()
    data = b"x" * 10000
    compressed = c.compress(data)
    assert len(compressed) < len(data)


# ---- Registry 查找 ----

def test_registry_lookup_all_ser():
    """5 个序列化器全部能通过 name 查到。"""
    for name in ("str", "bytes", "msgpack", "pyarrow", "protobuf"):
        s = SerializationRegistry.get(name)
        assert s is not None, f"未注册的 serializer: {name}"


def test_registry_lookup_all_comp():
    """4 个压缩器全部能通过 name 查到。"""
    for name in ("none", "snappy", "lz4", "zstd"):
        c = CompressionRegistry.get(name)
        assert c is not None, f"未注册的 compressor: {name}"


def test_registry_unknown_ser_raises():
    """未注册的 serializer 应抛 KeyError。"""
    with pytest.raises(KeyError):
        SerializationRegistry.get("nonexistent")


# ---- 16 组合 roundtrip ----

@pytest.mark.parametrize("ser", ["str", "bytes", "msgpack", "pyarrow"])
@pytest.mark.parametrize("comp", ["none", "snappy", "lz4", "zstd"])
def test_full_roundtrip(ser, comp):
    """FrameCodec.encode_payload + decode_payload 16 组合。"""
    from pulsemq.protocol.frames import FrameCodec
    if ser == "str":
        obj = "test"
    elif ser == "bytes":
        obj = b"test"
    elif ser == "msgpack":
        obj = {"k": "v"}
    elif ser == "pyarrow":
        import pandas as pd
        obj = pd.DataFrame({"a": [1]})
    enc = FrameCodec.encode_payload(obj, ser, comp)
    dec = FrameCodec.decode_payload(enc, ser, comp)
    if ser == "pyarrow":
        import pandas as pd
        pd.testing.assert_frame_equal(dec.to_pandas(), obj)
    else:
        assert dec == obj
```

- [ ] **Step 3: 跑测试**

Run: `cd D:/workflow/pulse-mq && uv run pytest tests/unit/test_serialization_registry.py -v`

Expected: 全绿（16 组合 × 5 行为 ≈ 50 个测试用例）

- [ ] **Step 4: 提交**

```bash
git add tests/unit/test_serialization_registry.py
git commit -m "test: serialization/registry 单测 (5 ser × 4 comp + registry 查找)"
```

---

## Task 4: 阶段 A 通用 — "发现 bug → 修源码 → 加测试" 模板

> 阶段 A 的剩余模块（transport / engine/router / engine/handlers / engine/engine / engine/pipeline / engine/pool / engine/overload）都按此模板。每个模块：
> 1. 审读
> 2. 写测试文件
> 3. 跑测试，记录失败
> 4. 失败 = 修源码
> 5. commit

**Files:** 各模块的 `tests/unit/test_<module>.py`，按需修 `src/pulsemq/<module>.py`

- [ ] **Step 1: 对 transport/zmq_transport.py 套模板**

审读要点：
- `broadcast()` 是否检查 `_xpub is None`（已看，是的）— 边界 OK
- `send()` 是否检查 `_router is None` — 看源码
- `stop()` 关闭顺序：先 ROUTER 后 XPUB 后 ctx — 是否会丢 in-flight 消息？
- `linger` 配置：默认 2000ms，关闭时等 2s — 合理？

写 `tests/unit/test_transport.py`（针对不依赖 ZMQ socket 的纯函数 — 实际可能很少，需要靠模块集成测试覆盖大部分）。如果单测覆盖不到，记录到 known-issues.md 并标记为"由集成测试覆盖"。

- [ ] **Step 2: 对 engine/router.py 套模板**

审读要点：
- `topic_match()` 边界：`""` 匹配什么？`"."` 匹配什么？`"*"` 匹配什么？`">"` 匹配什么？
- 递归 `_match_parts` 是否有死循环风险？
- `_wildcard_cache_valid` 在并发下是否线程安全？（单线程 asyncio 应该 OK，但要确认）
- `remove_identity` 后的缓存清理

写 `tests/unit/test_router.py`，重点覆盖 `topic_match`：

```python
from pulsemq.auth.permission import topic_match


def test_topic_match_exact():
    assert topic_match("a.b.c", "a.b.c")
    assert not topic_match("a.b.c", "a.b.x")


def test_topic_match_star_middle():
    assert topic_match("a.*.c", "a.b.c")
    assert not topic_match("a.*.c", "a.b.x.c")


def test_topic_match_star_end():
    assert topic_match("team-a.mkt.*", "team-a.mkt.sh.600000")


def test_topic_match_gt():
    assert topic_match("team-a.>", "team-a.mkt.sh.600000")
    assert topic_match("team-a.>", "team-a.x")


def test_topic_match_edge_cases():
    # 空字符串
    assert topic_match("", "")
    assert not topic_match("", "x")
    # 单段
    assert topic_match("a", "a")
    assert not topic_match("a", "ab")


def test_topic_match_no_false_positive():
    """前缀相似但不是子集的情况。"""
    assert not topic_match("a.b", "a.b.c")  # pattern 短, 不应匹配长 topic
```

- [ ] **Step 3: 对 engine/handlers.py 套模板**

审读要点（已知修了 _build_broadcast_meta）：
- 拦截器链异常是否隔离？
- `_handle_sub` 通配符展开时的缓存是否一致？
- `record_count=0` 行为？
- 5 帧 vs 6 帧分支是否都覆盖？

写 `tests/unit/test_handlers.py`，重点测 `_build_broadcast_meta` 边界：

```python
from pulsemq.engine.handlers import MessageHandlers


def test_build_broadcast_meta_preserves_flags():
    """_build_broadcast_meta 保留 ser/comp 字节, 只换 msg_type。"""
    h = MessageHandlers.__new__(MessageHandlers)  # 跳过 __init__
    wire_meta = bytes([0x04, 0b0010_0100])  # PUB=4, flags=str/none+has_topic
    out = h._build_broadcast_meta(wire_meta)
    assert out[1] == 0b0010_0100  # flags 保留


def test_build_broadcast_meta_short_wire_meta():
    """wire_meta 长度为 1 (无 flags 字节) 时不崩。"""
    h = MessageHandlers.__new__(MessageHandlers)
    out = h._build_broadcast_meta(b"\x04")  # 只有 msg_type
    assert len(out) == 2
    assert out[1] == 0  # flags 默认 0


def test_build_broadcast_meta_empty_wire_meta():
    """wire_meta 为空时不崩。"""
    h = MessageHandlers.__new__(MessageHandlers)
    out = h._build_broadcast_meta(b"")
    assert len(out) == 2
    assert out[1] == 0
```

- [ ] **Step 4: 对 engine/engine.py 套模板**

审读要点：
- `_broadcast_loop` 收到 None 哨兵是否正确退出？
- 关闭顺序：engine.stop → 推 None 进 queue → 等待 loop 退出
- `_adapt_batch_size` 的窗口大小

写 `tests/unit/test_engine.py`（可能需要 mock handlers，参考现有 test 模式）

- [ ] **Step 5: 对 engine/pipeline.py 套模板**

审读要点：
- 拦截器抛异常时是否中断链？其他拦截器还跑吗？
- 拦截器的 before/after 钩子是否对称执行？

写 `tests/unit/test_pipeline.py`：

```python
import pytest
from pulsemq.engine.pipeline import InterceptorChain
from pulsemq.models import AuthUser  # 假设有这个


def test_interceptor_chain_calls_all():
    """每个拦截器都被调用。"""
    calls = []

    class RecordingInterceptor:
        def __init__(self, name): self.name = name
        async def before(self, ctx): calls.append(f"{self.name}.before")
        async def after(self, ctx): calls.append(f"{self.name}.after")

    chain = InterceptorChain([
        RecordingInterceptor("a"),
        RecordingInterceptor("b"),
    ])
    # ... 触发 chain.process(ctx), 验证 calls == ["a.before", "b.before", "b.after", "a.after"]
```

注：具体接口可能不同，参照实际 `pipeline.py` 的实现调整。

- [ ] **Step 6: 对 engine/pool.py 和 engine/overload.py 套模板**

审读要点按文件实际职责。写对应 `tests/unit/test_pool.py` 与 `tests/unit/test_overload.py`。

- [ ] **Step 7: 跑所有阶段 A 测试**

Run: `cd D:/workflow/pulse-mq && uv run pytest tests/unit/test_protocol_*.py tests/unit/test_serialization_registry.py tests/unit/test_router.py tests/unit/test_handlers.py tests/unit/test_pipeline.py -v`

Expected: 全绿

- [ ] **Step 8: 阶段 A commit（按模块或按修复分组）**

```bash
# 每个修过的源码 bug 一个 fix commit
git add src/pulsemq/<module>.py
git commit -m "fix(<module>): <bug 简述>"

# 阶段 A 所有测试一次性 commit（或按模块）
git add tests/unit/
git commit -m "test: 阶段 A 单测覆盖完成 (protocol/transport/serialization/engine)"
```

---

## Task 5: 阶段 A — engine + transport 模块集成测试

**Files:**
- Create: `tests/integration/test_engine_transport.py`

- [ ] **Step 1: 写集成测试**

```python
"""engine + transport 集成测试。

启一个真实 server 子进程, 跑 client pub/sub, 验证 end-to-end 行为。
"""
from __future__ import annotations

import asyncio
import json

import pytest

from pulsemq.client.async_client import PulseClient


@pytest.mark.asyncio
async def test_pubsub_roundtrip_str(server_subprocess, port_pair):
    """str 消息端到端。"""
    port, _ = port_pair
    address = f"tcp://localhost:{port}"
    xpub = f"tcp://localhost:{port + 1}"
    received = []

    async def publisher():
        async with PulseClient(
            address=address, xpub_address=xpub, auto_reconnect=False
        ) as c:
            await asyncio.sleep(0.2)  # 等 subscriber
            await c.publish("test.it.0", "hello-世界", compression="none")

    async def subscriber():
        async with PulseClient(
            address=address, xpub_address=xpub, auto_reconnect=False
        ) as c:
            async for msg in c.subscribe("test.it.>"):
                received.append(msg.payload)
                if len(received) >= 1:
                    return

    await asyncio.wait_for(
        asyncio.gather(publisher(), subscriber()), timeout=10
    )
    assert received == ["hello-世界"]


@pytest.mark.asyncio
async def test_pubsub_concurrent_publishers(server_subprocess, port_pair):
    """多个 publisher 并发发, 单 subscriber 收。"""
    port, _ = port_pair
    address = f"tcp://localhost:{port}"
    xpub = f"tcp://localhost:{port + 1}"
    received = []

    async def publisher(idx: int):
        async with PulseClient(
            address=address, xpub_address=xpub, auto_reconnect=False
        ) as c:
            for j in range(5):
                await c.publish(f"test.con.{idx}", f"msg-{idx}-{j}")
                await asyncio.sleep(0.01)

    async def subscriber():
        async with PulseClient(
            address=address, xpub_address=xpub, auto_reconnect=False
        ) as c:
            async for msg in c.subscribe("test.con.>"):
                received.append(msg.payload)
                if len(received) >= 10:  # 2 pub × 5
                    return

    await asyncio.wait_for(
        asyncio.gather(publisher(0), publisher(1), subscriber()), timeout=15
    )
    assert len(received) == 10
```

- [ ] **Step 2: 跑测试**

Run: `cd D:/workflow/pulse-mq && uv run pytest tests/integration/test_engine_transport.py -v`

Expected: 全绿

- [ ] **Step 3: 提交**

```bash
git add tests/integration/
git commit -m "test: engine+transport 集成测试 (单 pub/sub, 多 pub 并发)"
```

---

## Task 6: 阶段 B 启动 — client/async_client.py 审计

**Files:**
- Read: `src/pulsemq/client/async_client.py`

- [ ] **Step 1: 审读 async_client.py**

重点（已知修了部分）：
- DEALER/SUB 双 socket 生命周期
- 上下文管理器 `__aenter__`/`__aexit__` 异常路径
- `auto_reconnect` 竞态
- `_reconnect()` 是否丢 SUB 过滤
- `unsubscribe()` 取消通配符订阅的语义
- `subscribe()` 通配符边界 (空字符串 / `*` / `>`)

- [ ] **Step 2: 写 test_client_subscribe.py**

```python
"""PulseClient.subscribe 行为单测。

注: 大部分场景需要真实 server, 这里用 server_subprocess fixture。
"""
import asyncio
import pytest
from pulsemq.client.async_client import PulseClient


@pytest.mark.asyncio
async def test_subscribe_exact_topic(server_subprocess, port_pair):
    """精确 topic 订阅。"""
    port, _ = port_pair
    address = f"tcp://localhost:{port}"
    xpub = f"tcp://localhost:{port + 1}"
    received = []

    async def pub():
        async with PulseClient(address=address, xpub_address=xpub, auto_reconnect=False) as c:
            await asyncio.sleep(0.2)
            await c.publish("test.s.0", "a")
            await c.publish("test.s.1", "b")  # 不订阅

    async def sub():
        async with PulseClient(address=address, xpub_address=xpub, auto_reconnect=False) as c:
            async for msg in c.subscribe("test.s.0"):
                received.append(msg.payload)
                return

    await asyncio.wait_for(asyncio.gather(pub(), sub()), timeout=10)
    assert received == ["a"]


@pytest.mark.asyncio
async def test_subscribe_wildcard(server_subprocess, port_pair):
    """通配符订阅。"""
    port, _ = port_pair
    address = f"tcp://localhost:{port}"
    xpub = f"tcp://localhost:{port + 1}"
    received = []

    async def pub():
        async with PulseClient(address=address, xpub_address=xpub, auto_reconnect=False) as c:
            await asyncio.sleep(0.2)
            await c.publish("test.w.a.0", "x")
            await c.publish("test.w.b.0", "y")
            await c.publish("other.topic", "z")  # 不订阅

    async def sub():
        async with PulseClient(address=address, xpub_address=xpub, auto_reconnect=False) as c:
            async for msg in c.subscribe("test.w.>"):
                received.append(msg.payload)
                if len(received) >= 2:
                    return

    await asyncio.wait_for(asyncio.gather(pub(), sub()), timeout=10)
    assert sorted(received) == ["x", "y"]


@pytest.mark.asyncio
async def test_ping_returns_dict(server_subprocess, port_pair):
    """ping() 返回 dict 含 client_ts 与 server_ts。"""
    port, _ = port_pair
    address = f"tcp://localhost:{port}"
    xpub = f"tcp://localhost:{port + 1}"
    async with PulseClient(address=address, xpub_address=xpub, auto_reconnect=False) as c:
        result = await c.ping()
        assert isinstance(result, dict)
        assert "client_ts" in result
        assert "server_ts" in result


@pytest.mark.asyncio
async def test_query_system_status(server_subprocess, port_pair):
    """query() 返回 system_status dict。"""
    port, _ = port_pair
    address = f"tcp://localhost:{port}"
    xpub = f"tcp://localhost:{port + 1}"
    async with PulseClient(address=address, xpub_address=xpub, auto_reconnect=False) as c:
        result = await c.query({"type": "system_status"})
        assert isinstance(result, dict)
```

- [ ] **Step 3: 审读中发现的 bug 进入 Task 4 修复流程**

记录到 `docs/known-issues.md`，每个 fix 一个 commit。

- [ ] **Step 4: 提交**

```bash
git add tests/unit/test_client_subscribe.py src/pulsemq/client/async_client.py
git commit -m "test: client.subscribe 行为单测 (精确/通配符/ping/query)"
```

---

## Task 7: 阶段 B — server/config/event_loop/models 审计

**Files:**
- Read: `src/pulsemq/server.py`, `src/pulsemq/config.py`, `src/pulsemq/event_loop.py`, `src/pulsemq/models.py`
- Create: `tests/unit/test_config.py`, `tests/unit/test_event_loop.py`, `tests/unit/test_models.py`

- [ ] **Step 1: 写 test_config.py**

```python
"""ServerConfig 单测。"""
from pulsemq.config import ServerConfig


def test_config_defaults():
    """默认值合理。"""
    c = ServerConfig()
    assert c.bind  # 非空
    assert c.xpub_bind
    assert c.auth_enabled is False  # 默认关闭


def test_config_env_override(monkeypatch):
    """环境变量可覆盖。"""
    monkeypatch.setenv("PULSEMQ_BIND", "tcp://*:9999")
    c = ServerConfig()
    assert c.bind == "tcp://*:9999"
```

注：实际字段名与 env 名按 `config.py` 调整。

- [ ] **Step 2: 写 test_event_loop.py**

```python
"""event_loop 单测。"""
import sys
from pulsemq.event_loop import install_event_loop


def test_install_idempotent():
    """重复调用不报错。"""
    install_event_loop(use_uvloop=False)
    install_event_loop(use_uvloop=False)


def test_install_uvloop_on_linux(monkeypatch):
    """Linux + uvloop=True 时实际安装 uvloop。"""
    if sys.platform == "win32":
        pytest.skip("Windows 不支持 uvloop")
    ...
```

- [ ] **Step 3: 写 test_models.py**

```python
"""models 数据类单测。"""
from pulsemq.models import ...  # 按实际 model 名


def test_model_field_defaults():
    ...


def test_model_equality():
    ...
```

- [ ] **Step 4: 审读 server.py 启动/关闭顺序**

写 `tests/integration/test_server_lifecycle.py`，验证：
- 启动后能 bind 到 bind/xpub_bind 端口
- 关闭后端口释放

```python
@pytest.mark.asyncio
async def test_server_start_stop_releases_port(port_pair):
    port, _ = port_pair
    # 启 server, 检查端口可连
    proc = ...
    # 停 server, 检查端口可重新 bind
    ...
```

- [ ] **Step 5: 跑 + 修 + 提交**

跑测试，按需修源码。commit 测试 + 修复。

---

## Task 8: 阶段 C — auth/ 审计

**Files:**
- Read: `src/pulsemq/auth/zap_handler.py`, `src/pulsemq/auth/memory_store.py`, `src/pulsemq/auth/permission.py`
- Create: `tests/unit/test_zap_handler.py`, `tests/unit/test_auth_memory_store.py`, `tests/unit/test_auth_permission.py`
- Create: `tests/security/test_zap_fuzz.py`

- [ ] **Step 1: 审读 ZAP handler**

ZAP 协议要点 (RFC 27):
- Client 发送 4 帧: [empty, version, request_id, metadata]
- metadata 包含: [domain, address, identity, mechanism, credentials]
- Server 响应 2 帧: [version, status_code + metadata]
- status_code 200=ok, 300=temp, 400=not_found, 500=server_error

验证当前实现是否合规。

- [ ] **Step 2: 写 test_zap_handler.py**

```python
"""ZAP handler 单测。"""
from pulsemq.auth.zap_handler import ZAPHandler


def test_zap_handler_accepts_valid_plain():
    """合法 PLAIN 请求应被接受。"""
    h = ZAPHandler(...)
    response = h.handle_request([
        b"",           # delimiter
        b"1.0",        # version
        b"req-1",      # request_id
        [              # metadata
            b"global",  # domain
            b"127.0.0.1",  # address
            b"user1",   # identity
            b"PLAIN",   # mechanism
            b"\x00user1\x00pass1",  # credentials
        ],
    ])
    assert response[0] == b"1.0"
    # status_code 在 response[1][0]
    assert response[1][0] in (200, 300, 400, 500)


def test_zap_handler_rejects_wrong_version():
    """非 1.0 版本应被拒绝。"""
    h = ZAPHandler(...)
    response = h.handle_request([b"", b"2.0", b"req-1", [b"", b"", b"", b"", b""]])
    # 期望错误 status
    assert response[1][0] != 200
```

注：实际接口按 `zap_handler.py` 调整。

- [ ] **Step 3: 写 test_zap_fuzz.py**

```python
"""ZAP 协议 fuzz 测试。

构造各种坏包, 验证 server 不会崩, 全部返回错误 status。
"""
import pytest


@pytest.mark.parametrize("malformed", [
    [],                                                  # 空
    [b""],                                               # 1 帧
    [b"", b"1.0"],                                       # 2 帧
    [b"", b"1.0", b"req"],                               # 3 帧
    [b"", b"1.0", b"req", []],                           # metadata 空列表
    [b"", b"1.0", b"req", [b"d"]],                       # metadata 缺字段
    [b"", b"1.0", b"req", [b"", b"", b"", b"UNKNOWN"]], # 未知 mechanism
    [b"", b"", b"req", [b"", b"", b"", b"PLAIN", b""]], # 空版本
    [b"", b"1.0", b"req", [b"", b"", b"", b"PLAIN", b"x"]], # 错误 credentials 格式
])
def test_zap_malformed_does_not_crash(malformed):
    h = ZAPHandler(...)
    try:
        response = h.handle_request(malformed)
    except Exception as e:
        pytest.fail(f"ZAP handler crashed on malformed input: {e!r}")
    # 应返回错误 status (不返回 200)
    if response is not None and len(response) >= 2:
        assert response[1][0] != 200, f"非法包被错误接受: {malformed}"
```

- [ ] **Step 4: 审读 auth/memory_store.py 与 auth/permission.py**

写 `test_auth_memory_store.py`（注册/查询/删除）+ `test_auth_permission.py`（grant/revoke + 匹配）。

- [ ] **Step 5: 跑 + 修 + 提交**

```bash
git add tests/unit/test_zap_handler.py tests/unit/test_auth_*.py tests/security/test_zap_fuzz.py
git commit -m "test: auth 模块单测 + ZAP 协议 fuzz"
```

---

## Task 9: 阶段 C — storage/ 审计（含 SQL 注入测试）

**Files:**
- Read: `src/pulsemq/storage/*.py`
- Create: `tests/unit/test_storage_*.py`
- Create: `tests/security/test_sql_injection.py`

- [ ] **Step 1: 审读 storage**

重点：
- 任何字符串拼接 SQL → 改参数化绑定
- SQLite 连接的线程/异步使用
- 事务边界

- [ ] **Step 2: 写 test_storage_sqlite_user.py**

```python
"""SQLite user storage 单测。"""
import os
import tempfile
import pytest
from pulsemq.storage.sqlite_user import SqliteUserStore


@pytest.fixture
def store():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        path = f.name
    s = SqliteUserStore(path)
    yield s
    s.close()
    os.unlink(path)


def test_register_and_get(store):
    store.register(user_id=1, username="alice", password_hash="h")
    u = store.get(user_id=1)
    assert u.username == "alice"


def test_register_duplicate_raises(store):
    store.register(user_id=1, username="alice", password_hash="h")
    with pytest.raises(...):
        store.register(user_id=1, username="alice2", password_hash="h2")
```

注：按 `sqlite_user.py` 实际接口调整。

- [ ] **Step 3: 写 test_sql_injection.py**

```python
"""SQL 注入测试。

在 username / password / permission 字段注入典型 SQL 注入 payload,
验证全部被参数化绑定, 没有执行注入。
"""
import pytest


@pytest.mark.parametrize("malicious_username", [
    "admin' OR '1'='1",
    "admin'; DROP TABLE users; --",
    "admin'/*",
    "' UNION SELECT * FROM users --",
    "admin\x00injected",
])
def test_sql_injection_in_username(store, malicious_username):
    """username 注入应被当作普通字符串处理, 不执行。"""
    try:
        store.register(user_id=999, username=malicious_username, password_hash="h")
    except Exception:
        pass  # 注册失败可接受
    # 验证表结构没被破坏: 仍能正常注册
    store.register(user_id=1, username="normal", password_hash="h")
    u = store.get(user_id=1)
    assert u.username == "normal"


@pytest.mark.parametrize("malicious_perm", [
    "topic.*'; DROP TABLE permissions; --",
    "topic' OR 1=1 --",
])
def test_sql_injection_in_permission(store, malicious_perm):
    """permission 字段注入应被参数化。"""
    try:
        store.grant_permission(user_id=1, topic_pattern=malicious_perm, action="pub")
    except Exception:
        pass
    # 验证表结构完整
    perms = store.get_permissions(user_id=1)
    assert isinstance(perms, list)  # 不崩
```

注：按实际 grant_permission/get_permissions 接口调整。

- [ ] **Step 4: 跑 + 修 + 提交**

```bash
git add tests/unit/test_storage_*.py tests/security/test_sql_injection.py
git commit -m "test: storage 单测 + SQL 注入 fuzz"
```

---

## Task 10: 阶段 C — monitoring/ 审计

**Files:**
- Read: `src/pulsemq/monitoring/*.py`
- Create: `tests/unit/test_monitoring_*.py`

- [ ] **Step 1: 审读 monitoring**

重点：
- 指标聚合正确性（counter, gauge, histogram）
- 实时 vs 分钟级聚合一致性
- HTTP API 输入校验

- [ ] **Step 2: 写 test_monitoring_realtime.py / test_minute.py / test_api.py**

按实际 API 写：

```python
def test_counter_increments():
    m = RealtimeMetrics()
    m.inc("pubsub.publish")
    m.inc("pubsub.publish")
    assert m.get("pubsub.publish") == 2


def test_minute_aggregation():
    ...


def test_api_endpoint_returns_json():
    ...
```

- [ ] **Step 3: 跑 + 修 + 提交**

---

## Task 11: 阶段 D — 性能基线

**Files:**
- Create: `scripts/bench_baseline.py`
- Create: `docs/bench-baseline.md`

- [ ] **Step 1: 写 bench_baseline.py**

```python
"""性能基线测试。

启 server + 1 pub + 1 sub, 跑 16 组合各 N 条消息, 测吞吐 / p50 / p99。
输出: 表格打印 + 写 docs/bench-baseline.md。
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import statistics
import subprocess
import sys
import time

sys.path.insert(0, "src")

from pulsemq.event_loop import install_event_loop
if sys.platform == "win32":
    install_event_loop(use_uvloop=False)

from pulsemq.client.async_client import PulseClient


DATA_TYPES = ["str", "bytes", "df-msgpack", "df-pyarrow"]
COMPRESSIONS = ["none", "snappy", "lz4", "zstd"]
N_MESSAGES = 10_000


def start_server(port: int) -> subprocess.Popen:
    proc = subprocess.Popen(
        [sys.executable, "scripts/test_server_runner.py", "--port", str(port)],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1,
        env={**os.environ, "PYTHONUNBUFFERED": "1", "PYTHONIOENCODING": "utf-8"},
    )
    deadline = time.time() + 10
    while time.time() < deadline:
        line = proc.stdout.readline()
        if not line:
            time.sleep(0.05)
            continue
        if line.strip() == "READY":
            return proc
    raise RuntimeError("server not ready")


def build_payload(data_type: str, idx: int):
    if data_type == "str":
        return f"msg-{idx}-世界-🚀"
    if data_type == "bytes":
        return os.urandom(128)
    if data_type in ("df-msgpack", "df-pyarrow"):
        import pandas as pd
        return pd.DataFrame({"i": [idx], "s": [f"row{idx}"]})
    raise ValueError(data_type)


async def bench_one(port: int, data_type: str, comp: str) -> dict:
    address = f"tcp://localhost:{port}"
    xpub = f"tcp://localhost:{port + 1}"
    latencies: list[float] = []
    ser = "msgpack" if data_type.startswith("df-") else (
        "bytes" if data_type == "bytes" else "str"
    )

    async def pub():
        async with PulseClient(address=address, xpub_address=xpub, auto_reconnect=False) as c:
            await asyncio.sleep(0.3)  # 等 sub
            for i in range(N_MESSAGES):
                t0 = time.perf_counter()
                payload = build_payload(data_type, i)
                if data_type.startswith("df-"):
                    await c.publish(f"bench.{data_type}", payload, format=ser, compression=comp)
                else:
                    await c.publish(f"bench.{data_type}", payload, compression=comp)
                latencies.append((time.perf_counter() - t0) * 1000)

    received = []

    async def sub():
        async with PulseClient(address=address, xpub_address=xpub, auto_reconnect=False) as c:
            async for msg in c.subscribe("bench.>"):
                received.append(msg)
                if len(received) >= N_MESSAGES:
                    return

    t0 = time.perf_counter()
    await asyncio.wait_for(asyncio.gather(pub(), sub()), timeout=120)
    elapsed = time.perf_counter() - t0

    return {
        "data_type": data_type,
        "compression": comp,
        "n": N_MESSAGES,
        "elapsed_s": round(elapsed, 3),
        "throughput_msg_s": round(N_MESSAGES / elapsed, 0),
        "p50_ms": round(statistics.median(latencies), 3),
        "p99_ms": round(sorted(latencies)[int(N_MESSAGES * 0.99)], 3),
    }


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=16000)
    parser.add_argument("--output", type=str, default="docs/bench-baseline.md")
    args = parser.parse_args()

    proc = start_server(args.port)
    try:
        results = []
        for dt in DATA_TYPES:
            for comp in COMPRESSIONS:
                r = await bench_one(args.port, dt, comp)
                results.append(r)
                print(f"  {dt:12s} {comp:6s} → {r['throughput_msg_s']:>8.0f} msg/s, "
                      f"p99={r['p99_ms']:>6.2f} ms", flush=True)
    finally:
        proc.terminate()
        try: proc.wait(timeout=5)
        except subprocess.TimeoutExpired: proc.kill()

    # 写 markdown
    with open(args.output, "w", encoding="utf-8") as f:
        f.write("# PulseMQ 性能基线\n\n")
        f.write(f"**测试环境**: Python {sys.version.split()[0]}, ")
        f.write(f"{sys.platform}, ")
        f.write(f"每组合 {N_MESSAGES} 条消息\n\n")
        f.write("| data_type | compression | throughput (msg/s) | p50 (ms) | p99 (ms) |\n")
        f.write("|-----------|-------------|-------------------|----------|----------|\n")
        for r in results:
            f.write(f"| {r['data_type']} | {r['compression']} | "
                    f"{r['throughput_msg_s']:.0f} | {r['p50_ms']:.2f} | {r['p99_ms']:.2f} |\n")
    print(f"\n结果写入 {args.output}")


if __name__ == "__main__":
    asyncio.run(main())
```

- [ ] **Step 2: 跑基线**

Run: `cd D:/workflow/pulse-mq && PYTHONIOENCODING=utf-8 uv run python scripts/bench_baseline.py --port 16001 --output docs/bench-baseline.md 2>&1 | tail -30`

Expected: 16 行 throughput/p50/p99 输出, 退出码 0

- [ ] **Step 3: 提交**

```bash
git add scripts/bench_baseline.py docs/bench-baseline.md
git commit -m "test: 添加性能基线脚本 (1pub×1sub, 16 组合)"
```

---

## Task 12: 阶段 D — 并发压测

**Files:**
- Create: `scripts/bench_concurrent.py`

- [ ] **Step 1: 写 bench_concurrent.py**

```python
"""并发压测: N pub × M sub × 16 组合。

测最大吞吐, 写 docs/bench-baseline.md 追加段。
"""
from __future__ import annotations

import argparse
import asyncio
import os
import statistics
import subprocess
import sys
import time

sys.path.insert(0, "src")

from pulsemq.event_loop import install_event_loop
if sys.platform == "win32":
    install_event_loop(use_uvloop=False)

from pulsemq.client.async_client import PulseClient


async def bench_concurrent(port: int, n_pub: int, n_sub: int, n_per_pub: int, data_type: str, comp: str):
    address = f"tcp://localhost:{port}"
    xpub = f"tcp://localhost:{port + 1}"
    ser = "msgpack" if data_type.startswith("df-") else (
        "bytes" if data_type == "bytes" else "str"
    )
    total = n_pub * n_per_pub
    received: list = []
    received_lock = asyncio.Lock()

    async def pub(idx: int):
        async with PulseClient(address=address, xpub_address=xpub, auto_reconnect=False) as c:
            for i in range(n_per_pub):
                payload = f"pub{idx}-msg{i}"
                if data_type.startswith("df-"):
                    import pandas as pd
                    await c.publish(f"bench.cc.{idx}",
                                    pd.DataFrame({"i": [i]}),
                                    format=ser, compression=comp)
                elif data_type == "bytes":
                    await c.publish(f"bench.cc.{idx}", os.urandom(64), compression=comp)
                else:
                    await c.publish(f"bench.cc.{idx}", payload, compression=comp)
                await asyncio.sleep(0.001)

    async def sub(idx: int):
        async with PulseClient(address=address, xpub_address=xpub, auto_reconnect=False) as c:
            async for msg in c.subscribe("bench.cc.>"):
                async with received_lock:
                    received.append(msg)
                if len(received) >= total:
                    return

    t0 = time.perf_counter()
    pub_tasks = [asyncio.create_task(pub(i)) for i in range(n_pub)]
    sub_tasks = [asyncio.create_task(sub(i)) for i in range(n_sub)]
    try:
        await asyncio.wait_for(asyncio.gather(*pub_tasks, *sub_tasks), timeout=300)
    finally:
        for t in pub_tasks + sub_tasks:
            t.cancel()
    elapsed = time.perf_counter() - t0
    return {
        "n_pub": n_pub, "n_sub": n_sub, "n_per_pub": n_per_pub,
        "received": len(received), "expected": total,
        "elapsed_s": round(elapsed, 2),
        "throughput_msg_s": round(len(received) / elapsed, 0),
    }


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=16010)
    parser.add_argument("--n-pub", type=int, default=4)
    parser.add_argument("--n-sub", type=int, default=4)
    parser.add_argument("--n-per-pub", type=int, default=5000)
    args = parser.parse_args()

    proc = subprocess.Popen(
        [sys.executable, "scripts/test_server_runner.py", "--port", str(args.port)],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1,
        env={**os.environ, "PYTHONUNBUFFERED": "1", "PYTHONIOENCODING": "utf-8"},
    )
    deadline = time.time() + 10
    while time.time() < deadline:
        line = proc.stdout.readline()
        if not line.strip() == "READY":
            time.sleep(0.05); continue
        break

    try:
        result = await bench_concurrent(
            args.port, args.n_pub, args.n_sub, args.n_per_pub,
            "str", "none",
        )
        print(f"  {result}", flush=True)
        # 追加到 bench-baseline.md
        with open("docs/bench-baseline.md", "a", encoding="utf-8") as f:
            f.write(f"\n## 并发压测 ({args.n_pub} pub × {args.n_sub} sub)\n\n")
            f.write(f"- 收/发: {result['received']}/{result['expected']}\n")
            f.write(f"- 吞吐: {result['throughput_msg_s']:.0f} msg/s\n")
            f.write(f"- 耗时: {result['elapsed_s']}s\n")
    finally:
        proc.terminate()
        try: proc.wait(timeout=5)
        except subprocess.TimeoutExpired: proc.kill()


if __name__ == "__main__":
    asyncio.run(main())
```

- [ ] **Step 2: 跑压测**

Run: `cd D:/workflow/pulse-mq && PYTHONIOENCODING=utf-8 uv run python scripts/bench_concurrent.py --port 16011 --n-pub 4 --n-sub 4 --n-per-pub 2000 2>&1 | tail -10`

Expected: 收/发 = 8000/8000, 吞吐数字, 退出码 0

- [ ] **Step 3: 提交**

```bash
git add scripts/bench_concurrent.py docs/bench-baseline.md
git commit -m "test: 添加并发压测脚本 (N pub × M sub)"
```

---

## Task 13: 阶段 D — 告警阈值文档

**Files:**
- Create: `docs/bench-thresholds.md`

- [ ] **Step 1: 写阈值文档**

```markdown
# PulseMQ 性能告警阈值

## 基线引用

完整基线数字见 `docs/bench-baseline.md`。以下阈值基于该基线**浮动 20%**。

## 阈值表

| 指标 | 阈值 | 检查方式 |
|------|------|----------|
| 单 pub×单 sub 吞吐 (str/none) | ≥ 基线 × 0.8 | 跑 `bench_baseline.py` |
| p99 延迟 (str/none) | ≤ 基线 × 1.2 | 同上 |
| 并发吞吐 (4 pub × 4 sub) | ≥ 基线 × 0.8 | 跑 `bench_concurrent.py` |
| 内存泄漏 (soak 1h) | 增长 < 5% | 跑 `bench_soak.py` (待加) |
| e2e 16/16 | 必须 16/16 | 跑 `test_e2e_all.py` |
| pytest | 全绿 | `uv run pytest` |

## CI 集成建议

- 每次 PR 跑 `test_e2e_all.py` + `pytest`（耗时约 1 分钟）
- 每日定时跑 `bench_baseline.py`（耗时约 5 分钟），不通过则报警
- 每周跑 `bench_soak.py` 1h 抽检

## 调整阈值

阈值用倍数表达，便于跨硬件。基线重测时同步更新 `docs/bench-baseline.md`，
阈值保持"基线 × N"形式即可。
```

- [ ] **Step 2: 提交**

```bash
git add docs/bench-thresholds.md
git commit -m "docs: 添加性能告警阈值 (基线 × 浮动比例)"
```

---

## Task 14: 阶段 D — 4h Soak 测试 (可选, 视时间)

**Files:**
- Create: `scripts/bench_soak.py`

> 这是一个长时间运行测试, 实际工程中通常跑 4h+. 本任务可推迟或仅跑 30 分钟验证机制.

- [ ] **Step 1: 写 bench_soak.py**

```python
"""Soak 测试: 长时间运行, 检测内存/句柄泄漏。

启 server + 1 pub + 1 sub, 持续收发, 定期采样 RSS 内存。
"""
import argparse
import asyncio
import os
import subprocess
import sys
import time
import tracemalloc

sys.path.insert(0, "src")
from pulsemq.event_loop import install_event_loop
if sys.platform == "win32":
    install_event_loop(use_uvloop=False)
from pulsemq.client.async_client import PulseClient


async def soak(port: int, duration_min: int, rate_msg_s: int):
    address = f"tcp://localhost:{port}"
    xpub = f"tcp://localhost:{port + 1}"
    n_sent = 0
    received_count = 0
    tracemalloc.start()

    async def pub():
        nonlocal n_sent
        async with PulseClient(address=address, xpub_address=xpub, auto_reconnect=False) as c:
            await asyncio.sleep(0.3)
            end = time.time() + duration_min * 60
            while time.time() < end:
                await c.publish("soak.t", f"msg-{n_sent}")
                n_sent += 1
                await asyncio.sleep(1.0 / rate_msg_s)

    async def sub():
        nonlocal received_count
        async with PulseClient(address=address, xpub_address=xpub, auto_reconnect=False) as c:
            end = time.time() + duration_min * 60
            async for msg in c.subscribe("soak.>"):
                received_count += 1
                if time.time() > end:
                    return

    t0 = time.time()
    try:
        await asyncio.wait_for(asyncio.gather(pub(), sub()), timeout=duration_min * 60 + 30)
    except asyncio.TimeoutError:
        pass
    elapsed = time.time() - t0
    current, peak = tracemalloc.get_traced_memory()
    print(f"耗时: {elapsed:.0f}s")
    print(f"发送: {n_sent}, 接收: {received_count}")
    print(f"tracemalloc current={current/1024:.0f} KB, peak={peak/1024:.0f} KB")
    tracemalloc.stop()


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=16020)
    parser.add_argument("--duration-min", type=int, default=30)
    parser.add_argument("--rate-msg-s", type=int, default=100)
    args = parser.parse_args()
    proc = subprocess.Popen(
        [sys.executable, "scripts/test_server_runner.py", "--port", str(args.port)],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1,
        env={**os.environ, "PYTHONUNBUFFERED": "1", "PYTHONIOENCODING": "utf-8"},
    )
    deadline = time.time() + 10
    while time.time() < deadline:
        line = proc.stdout.readline()
        if line.strip() == "READY":
            break
        time.sleep(0.05)
    try:
        await soak(args.port, args.duration_min, args.rate_msg_s)
    finally:
        proc.terminate()
        try: proc.wait(timeout=5)
        except subprocess.TimeoutExpired: proc.kill()


if __name__ == "__main__":
    asyncio.run(main())
```

- [ ] **Step 2: 短时跑通验证机制 (5 分钟)**

Run: `cd D:/workflow/pulse-mq && PYTHONIOENCODING=utf-8 uv run python scripts/bench_soak.py --port 16021 --duration-min 5 --rate-msg-s 50 2>&1 | tail -5`

Expected: 耗时 300s 左右, 发送接收数字, tracemalloc 数字

- [ ] **Step 3: 提交**

```bash
git add scripts/bench_soak.py
git commit -m "test: 添加 soak 测试脚本 (可配置时长与速率)"
```

---

## 验证清单

- [ ] `uv run pytest` 全绿
- [ ] `scripts/test_e2e_all.py` 16/16 通过
- [ ] `scripts/bench_baseline.py` 跑通, `docs/bench-baseline.md` 存在
- [ ] `scripts/bench_concurrent.py` 跑通, 数字写入 baseline
- [ ] `docs/bench-thresholds.md` 存在
- [ ] ZAP fuzz 全部通过
- [ ] SQL 注入全部通过
- [ ] 9 个模块全部走过审读, `docs/known-issues.md` 记录全部发现
- [ ] 全部 commit 干净 (按阶段或按模块分组)

## 不在范围

- 协议扩展
- 客户端 SDK 重构
- 新功能开发
