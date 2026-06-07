# PulseMQ 端到端全格式/全压缩测试实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 新增独立运行的端到端集成测试，覆盖 `str/bytes/df-msgpack/df-pyarrow` × `none/snappy/lz4/zstd` = 16 个组合，验证每条消息都能完整往返。

**Architecture:** 两个独立 Python 脚本：`test_server_runner.py` 启禁用认证的 PulseServer；`test_e2e_all.py` 是入口，用 subprocess 拉起 server，并发跑 publisher 与 subscriber，订阅端直接用 `msg.payload` 验证。**先修一个 client bug**（`PulseClient._decode_message` 硬编码 `msgpack/none`），修复后所有 16 个组合的 `msg.payload` 都能正确解码。退出码 0/1 即通过/失败。

**Tech Stack:** Python 3.13+, asyncio, subprocess, zmq (pyzmq), pandas, pyarrow, msgpack, snappy, lz4, zstandard

**Spec:** `docs/superpowers/specs/2026-06-07-pulsemq-e2e-test-design.md`

**前置修复（必须先做）：**
- `src/pulsemq/client/async_client.py:392-395` 的 `_decode_message` 用 `self._DEFAULT_SER/_DEFAULT_COMP`（`msgpack/none`）硬解码 SUB 收到的消息，**不读 `FrameFlags`**。这导致 `str`（任何压缩）与 `df-pyarrow`（任何压缩）组合的 `msg.payload` 字段都不正确。
- Task 0 必须先修这个 bug：把 `FrameFlags.decode(meta[1])` 拿到真实的 `ser_fmt` / `comp`，再调 `FrameCodec.decode_payload(raw, ser_fmt, comp)`。
- 修完后再写 e2e 测试，测试直接用 `msg.payload` 字段验证，不再走 workaround。

---

## File Structure

| 文件 | 状态 | 职责 |
|------|------|------|
| `src/pulsemq/client/async_client.py` | 修改 | 修 `_decode_message`：用 FrameFlags 正确解码 |
| `scripts/test_server_runner.py` | 新增 | 启禁用认证、关闭指标的 PulseServer，启动后向 stdout 写 `READY\n` |
| `scripts/test_e2e_all.py` | 新增 | 入口：用 subprocess 启动 server，并发 publisher 与 subscriber，跑 16 个组合并验证 |

---

## Task 0: 修复 PulseClient._decode_message（前置）

**Files:**
- Modify: `src/pulsemq/client/async_client.py:1-22`（添加 import）
- Modify: `src/pulsemq/client/async_client.py:383-409`（修 _decode_message）

> 这是阻塞 e2e 测试的 client bug：硬编码用 `msgpack/none` 解码收到的消息，导致 str 与 df-pyarrow 类型的 payload 拿不到正确对象。修复后才能用 `msg.payload` 正常验证。

- [ ] **Step 1: 在 async_client.py 顶部添加 FrameFlags import**

修改 `src/pulsemq/client/async_client.py` 的 import 段（line 20 附近），把 `FrameCodec` 上方加 `FrameFlags`：

```python
from pulsemq.protocol.flags import FrameFlags
from pulsemq.protocol.frames import FrameCodec
from pulsemq.protocol.msg_type import MsgType
```

> 注：当前 line 20 已有 `from pulsemq.protocol.frames import FrameCodec`，保持不变；只需在其上方加 `FrameFlags` 那一行。

- [ ] **Step 2: 修 _decode_message，用 FrameFlags 拿真实 ser_fmt/comp**

在 `src/pulsemq/client/async_client.py` 中，把现有 `_decode_message`（line 383-409）替换为：

```python
def _decode_message(self, frames: list[bytes]) -> PulseMessage | None:
    """解码 SUB 收到的广播消息。

    从 frame[1] (meta 字节) 解析出 ser_fmt / comp，再调 FrameCodec 解码。
    """
    try:
        topic = frames[0].decode("utf-8")
        meta = frames[1]
        msg_type = meta[0]
        flags = (
            FrameFlags.decode(meta[1])
            if len(meta) > 1
            else FrameFlags(ser_fmt="msgpack", comp="none", has_topic=False)
        )
        payload_bytes = frames[3] if len(frames) > 3 else b""

        # 用 wire 上的真实 ser_fmt / comp 解码（不再硬编码 msgpack/none）
        try:
            payload = FrameCodec.decode_payload(
                payload_bytes, flags.ser_fmt, flags.comp
            )
        except Exception:
            payload = None

        return PulseMessage(
            topic=topic,
            msg_type=msg_type,
            payload=payload,
            raw_payload=payload_bytes,
            meta_flags=meta[1] if len(meta) > 1 else 0,
            timestamp=time.time(),
        )
    except Exception as e:
        logger.debug("消息解码失败: %s", e)
        return None
```

- [ ] **Step 3: 验证 import 不重名**

Run: `cd D:/workflow/pulse-mq && uv run python -c "from pulsemq.client.async_client import PulseClient; print('OK')"`

Expected: 输出 `OK`，无 import error。

- [ ] **Step 4: 提交**

```bash
git add src/pulsemq/client/async_client.py
git commit -m "fix: PulseClient._decode_message 读取 FrameFlags 而非硬编码 msgpack/none

之前 _decode_message 用 self._DEFAULT_SER/_DEFAULT_COMP 硬解码收到的
SUB 消息，导致 str 与 df-pyarrow 类型的 payload 无法正确反序列化。
现从 meta[1] 解析 FrameFlags，传入 FrameCodec.decode_payload。"
```

---

## Task 1: 创建 test_server_runner.py

**Files:**
- Create: `scripts/test_server_runner.py`

- [ ] **Step 1: 写最小可运行的 server 启动脚本**

创建 `scripts/test_server_runner.py`：

```python
#!/usr/bin/env python3
"""PulseMQ 测试服务端（端到端测试专用）。

与 scripts/test_server.py 的差异：
- 关闭指标（不需要 HTTP 端口）
- 启动后向 stdout 写 "READY\\n"，便于 e2e 脚本同步
- 默认端口 15555（避开开发端口 5555/5556）

用法:
    python scripts/test_server_runner.py
    python scripts/test_server_runner.py --port 15555
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import sys

from pulsemq.config import ServerConfig
from pulsemq.event_loop import install_event_loop
from pulsemq.server import PulseServer


def main() -> None:
    logging.basicConfig(
        level=logging.WARNING,  # 测试环境降噪
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=15555, help="ROUTER 端口 (XPUB = port+1)")
    args = parser.parse_args()

    config = ServerConfig(
        bind=f"tcp://*:{args.port}",
        xpub_bind=f"tcp://*:{args.port + 1}",
        auth_enabled=False,
        metrics_enabled=False,
        max_concurrency=100,
        data_buffer_size=50_000,
        ctrl_buffer_size=5_000,
    )

    install_event_loop(config.use_uvloop)
    server = PulseServer(config)
    loop = asyncio.new_event_loop()

    def _shutdown() -> None:
        loop.call_soon_threadsafe(lambda: asyncio.ensure_future(server.stop()))

    if sys.platform != "win32":
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, _shutdown)

    async def _run() -> None:
        # 启动 server
        start_task = asyncio.create_task(server.start())
        # 等到 transport 完成 bind（约几十 ms），再打印 READY
        await asyncio.sleep(0.3)
        print("READY", flush=True)
        await start_task

    try:
        loop.run_until_complete(_run())
    except KeyboardInterrupt:
        pass
    finally:
        loop.run_until_complete(server.stop())
        loop.close()


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: 手动启动验证 READY 输出与端口绑定**

Run: `cd D:/workflow/pulse-mq && uv run python scripts/test_server_runner.py --port 15555 &` 然后立即在另一个 shell：

```bash
# 等待 READY
timeout 3 python scripts/test_server_runner.py --port 15555 2>&1 | head -3
```

Expected: 看到 `READY` 字符串输出。

另起一个 shell 验证端口：

```bash
# Windows (Git Bash) - 用 netstat 简单验证
netstat -ano | grep -E "15555|15556"
```

Expected: 看到 15555 与 15556 处于 LISTENING 状态。

然后 `Ctrl+C` 或 `taskkill` 杀掉测试进程。

- [ ] **Step 3: 提交**

```bash
git add scripts/test_server_runner.py
git commit -m "test: 添加端到端测试专用 server runner（关闭认证与指标，输出 READY 信号）"
```

---

## Task 2: 验证 FrameCodec 编解码与确认 client 行为

**Files:**
- Read: `src/pulsemq/protocol/frames.py` （已存在，验证理解）
- Read: `src/pulsemq/client/async_client.py:383-409` （Task 0 修过，确认改动生效）
- Create: `scripts/_probe_codec.py`（临时探测脚本，Task 末会删）

> 这一步是为了在写正式 e2e 之前，确认 `FrameCodec.encode_payload/decode_payload` 在 16 种组合上都能 roundtrip。

- [ ] **Step 1: 写临时探测脚本 _probe_codec.py**

创建 `scripts/_probe_codec.py`：

```python
"""临时探测：验证 FrameCodec 16 组合 roundtrip。

完成后删除。
"""
from __future__ import annotations

import os
import sys

import pandas as pd

# 让脚本能找到 pulsemq
sys.path.insert(0, "src")

from pulsemq.protocol.frames import FrameCodec


def build_payload(data_type: str):
    if data_type == "str":
        return "hello-世界-🚀"
    if data_type == "bytes":
        return os.urandom(64)
    if data_type == "df-msgpack":
        return pd.DataFrame({
            "i": list(range(5)),
            "f": [x * 0.1 for x in range(5)],
            "s": [f"row{x}" for x in range(5)],
            "b": [b"\\x00\\x01" * 3 for _ in range(5)],
        })
    if data_type == "df-pyarrow":
        return pd.DataFrame({
            "i": list(range(5)),
            "f": [x * 0.1 for x in range(5)],
            "s": [f"row{x}" for x in range(5)],
            "b": [b"\\x00\\x01" * 3 for _ in range(5)],
        })
    raise ValueError(data_type)


def ser_for(data_type: str) -> str:
    return "str" if data_type == "str" else (
        "bytes" if data_type == "bytes" else data_type.replace("df-", "")
    )


def main() -> None:
    print("=== FrameCodec 16 组合 roundtrip ===")
    failed = []
    for data_type in ["str", "bytes", "df-msgpack", "df-pyarrow"]:
        for comp in ["none", "snappy", "lz4", "zstd"]:
            ser = ser_for(data_type)
            payload = build_payload(data_type)
            try:
                enc = FrameCodec.encode_payload(payload, ser, comp)
                dec = FrameCodec.decode_payload(enc, ser, comp)
                if isinstance(payload, pd.DataFrame):
                    assert isinstance(dec, (list, pd.DataFrame)) or hasattr(dec, "to_pandas"), \\
                        f"decode type wrong: {type(dec)}"
                elif isinstance(payload, bytes):
                    assert dec == payload, f"bytes mismatch"
                else:
                    assert dec == payload, f"str mismatch"
                print(f"  OK   {data_type:12s} {ser:8s} {comp:6s} -> {len(enc):6d} B")
            except Exception as e:
                failed.append((data_type, ser, comp, e))
                print(f"  FAIL {data_type:12s} {ser:8s} {comp:6s} -> {e}")

    if failed:
        print(f"\\n{len(failed)} FAILED")
        sys.exit(1)
    print("\\n16/16 codec roundtrip OK")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: 运行探测**

Run: `cd D:/workflow/pulse-mq && uv run python scripts/_probe_codec.py`

Expected: 看到 16 行 `OK`，最后一行 `16/16 codec roundtrip OK`。

- [ ] **Step 3: 删掉临时探测脚本（不提交）**

```bash
rm scripts/_probe_codec.py
```

> 注：这一步**不提交**。目的是确认理解，不留临时文件。

---

## Task 3: 写 test_e2e_all.py 主测试

**Files:**
- Create: `scripts/test_e2e_all.py`

- [ ] **Step 1: 写完整文件**

创建 `scripts/test_e2e_all.py`：

```python
#!/usr/bin/env python3
"""PulseMQ 端到端全格式/全压缩集成测试。

启动一个 server + 两个 client（publisher / subscriber），
覆盖 4 数据类型 × 4 压缩 = 16 组合，验证 payload 往返一致。

依赖 Task 0 修复过的 PulseClient（_decode_message 读 FrameFlags），
本脚本直接用 msg.payload 验证。

用法:
    python scripts/test_e2e_all.py
    python scripts/test_e2e_all.py --port 15555 --timeout 30
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import subprocess
import sys
import time
from dataclasses import dataclass, field
from typing import Any

import pandas as pd

# 让脚本能找到 pulsemq
sys.path.insert(0, "src")

from pulsemq.client.async_client import PulseClient


# ---------------------------------------------------------------------------
# 测试用例定义
# ---------------------------------------------------------------------------

DATA_TYPES = ["str", "bytes", "df-msgpack", "df-pyarrow"]
COMPRESSIONS = ["none", "snappy", "lz4", "zstd"]


@dataclass
class Case:
    id: int
    data_type: str        # str | bytes | df-msgpack | df-pyarrow
    compression: str      # none | snappy | lz4 | zstd
    topic: str            # test.e2e.<id>.<data_type>
    original: Any         # 原始数据（用于 publish + 比较）
    ser: str              # FrameCodec 用的 ser_fmt（与 data_type 对应）


def _ser_for(data_type: str) -> str:
    if data_type == "str":
        return "str"
    if data_type == "bytes":
        return "bytes"
    if data_type.startswith("df-"):
        return data_type[len("df-"):]  # msgpack / pyarrow
    raise ValueError(data_type)


def build_originals() -> list[Case]:
    """构造 16 个用例，每个用独立的原始数据（避免 hash 碰撞）。"""
    rng = random.Random(42)  # 固定 seed，保证可复现
    cases: list[Case] = []
    cid = 0
    for data_type in DATA_TYPES:
        for comp in COMPRESSIONS:
            if data_type == "str":
                original = json.dumps({
                    "case_id": cid,
                    "msg": "hello-世界-🚀",
                    "ts": time.time(),
                    "rand": rng.randint(0, 1_000_000),
                }, ensure_ascii=False)
            elif data_type == "bytes":
                original = bytes(rng.randint(0, 255) for _ in range(64))
            elif data_type in ("df-msgpack", "df-pyarrow"):
                original = pd.DataFrame({
                    "case_id": [cid] * 5,
                    "i": list(range(5)),
                    "f": [round(x * 0.1, 2) for x in range(5)],
                    "s": [f"row{x}" for x in range(5)],
                    "b": [os.urandom(4) for _ in range(5)],
                })
            else:
                raise ValueError(data_type)
            cases.append(Case(
                id=cid,
                data_type=data_type,
                compression=comp,
                topic=f"test.e2e.{cid}.{data_type}",
                original=original,
                ser=_ser_for(data_type),
            ))
            cid += 1
    return cases


# ---------------------------------------------------------------------------
# Payload 断言：依赖 Task 0 修复过的 client，直接用 msg.payload
# ---------------------------------------------------------------------------

def assert_payload_equal(received: Any, case: Case) -> None:
    """比对 received 与 case.original；不一致抛 AssertionError。"""
    orig = case.original
    if case.data_type == "str":
        # 客户端正确解码后是 str
        assert isinstance(received, str), \\
            f"type mismatch: expected str, got {type(received).__name__}"
        assert received == orig, \\
            f"str mismatch:\\n  expected: {orig!r}\\n  got:      {received!r}"
    elif case.data_type == "bytes":
        assert isinstance(received, (bytes, bytearray)), \\
            f"type mismatch: expected bytes, got {type(received).__name__}"
        assert bytes(received) == orig, \\
            f"bytes mismatch (len expected={len(orig)}, got={len(received)})"
    elif case.data_type == "df-msgpack":
        # msgpack 路径：received 是 list[dict]（msgpack 反序列化的结果）
        assert isinstance(received, list), \\
            f"df-msgpack: expected list, got {type(received).__name__}"
        received_df = pd.DataFrame(received)
        pd.testing.assert_frame_equal(received_df, orig)
    elif case.data_type == "df-pyarrow":
        # pyarrow 路径：received 是 pa.Table
        import pyarrow as pa
        assert isinstance(received, pa.Table), \\
            f"df-pyarrow: expected pa.Table, got {type(received).__name__}"
        pd.testing.assert_frame_equal(received.to_pandas(), orig)
    else:
        raise ValueError(case.data_type)


# ---------------------------------------------------------------------------
# Subprocess 启 server
# ---------------------------------------------------------------------------

def start_server(port: int, ready_timeout: float = 10.0) -> subprocess.Popen:
    """启动 server_runner 子进程，等待 READY 后返回。"""
    proc = subprocess.Popen(
        [sys.executable, "scripts/test_server_runner.py", "--port", str(port)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,  # 行缓冲，让 READY 能立即刷出
        env={**os.environ, "PYTHONUNBUFFERED": "1"},
    )
    deadline = time.time() + ready_timeout
    while time.time() < deadline:
        line = proc.stdout.readline()
        if not line:
            time.sleep(0.05)
            if proc.poll() is not None:
                stderr = proc.stderr.read() if proc.stderr else ""
                raise RuntimeError(
                    f"server_runner 提前退出 (rc={proc.returncode})\\n"
                    f"stderr: {stderr}"
                )
            continue
        if line.strip() == "READY":
            return proc
    proc.kill()
    raise TimeoutError(
        f"等待 READY 超时（{ready_timeout}s）"
    )


def stop_server(proc: subprocess.Popen) -> None:
    if proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=2)


# ---------------------------------------------------------------------------
# Publisher / Subscriber 协程
# ---------------------------------------------------------------------------

async def run_publisher(
    port: int,
    cases: list[Case],
    done: asyncio.Event,
) -> None:
    """顺序发布所有用例。"""
    address = f"tcp://localhost:{port}"
    xpub_address = f"tcp://localhost:{port + 1}"
    async with PulseClient(
        address=address,
        xpub_address=xpub_address,
        auto_reconnect=False,
    ) as client:
        for case in cases:
            if case.data_type in ("df-msgpack", "df-pyarrow"):
                await client.publish(
                    case.topic, case.original,
                    format=case.ser, compression=case.compression,
                )
            else:
                await client.publish(
                    case.topic, case.original,
                    compression=case.compression,
                )
            await asyncio.sleep(0.05)  # 给 SUB 缓冲时间
    done.set()


@dataclass
class SubscriberResult:
    received: dict[int, None] = field(default_factory=dict)
    errors: list[tuple[int, str]] = field(default_factory=list)


async def run_subscriber(
    port: int,
    cases: list[Case],
    publisher_done: asyncio.Event,
    timeout_after_publisher: float = 5.0,
) -> SubscriberResult:
    """订阅通配 topic，验证收到的每条消息。"""
    address = f"tcp://localhost:{port}"
    xpub_address = f"tcp://localhost:{port + 1}"
    cases_by_id = {c.id: c for c in cases}
    expected_ids = set(cases_by_id.keys())
    result = SubscriberResult()

    async with PulseClient(
        address=address,
        xpub_address=xpub_address,
        auto_reconnect=False,
    ) as client:
        async for msg in client.subscribe("test.e2e.>"):
            cid = _extract_case_id(msg.topic)
            if cid in result.received:
                result.errors.append((cid, "duplicate"))
                continue
            result.received[cid] = None
            try:
                # 依赖 Task 0 修复：msg.payload 已是正确解码后的对象
                assert_payload_equal(msg.payload, cases_by_id[cid])
            except AssertionError as e:
                result.errors.append((cid, str(e)))

            if expected_ids.issubset(result.received.keys()):
                break

        # publisher 已发完 + 等收尾
        if publisher_done.is_set():
            try:
                await asyncio.wait_for(_drain(client), timeout=timeout_after_publisher)
            except asyncio.TimeoutError:
                pass

    return result


async def _drain(client: PulseClient) -> None:
    """publisher 完成后短暂 drain 残余消息。"""
    async for _msg in client.subscribe("test.e2e.>"):
        await asyncio.sleep(0.01)
        break


def _extract_case_id(topic: str) -> int:
    """从 'test.e2e.<id>.<type>' 解析出 <id>。"""
    parts = topic.split(".")
    return int(parts[2])


# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------

async def run_all(port: int, timeout: float) -> int:
    cases = build_originals()
    print(f"=== PulseMQ 端到端测试 ===")
    print(f"  端口:   {port} / {port + 1}")
    print(f"  用例:   {len(cases)} ({' × '.join([str(len(DATA_TYPES)), str(len(COMPRESSIONS))])})")
    print(f"  超时:   {timeout}s")
    print()

    proc = start_server(port)
    publisher_done = asyncio.Event()
    try:
        pub_task = asyncio.create_task(run_publisher(port, cases, publisher_done))
        sub_task = asyncio.create_task(run_subscriber(port, cases, publisher_done))
        try:
            results: tuple[SubscriberResult, None] = await asyncio.wait_for(
                asyncio.gather(sub_task, pub_task, return_exceptions=True),
                timeout=timeout,
            )
        except asyncio.TimeoutError:
            pub_task.cancel()
            sub_task.cancel()
            await asyncio.gather(pub_task, sub_task, return_exceptions=True)
            print(f"❌ TIMEOUT after {timeout}s")
            return 1

        sub_result = results[0]
        if isinstance(sub_result, Exception):
            print(f"❌ Subscriber crashed: {sub_result!r}")
            return 1

        # 汇总
        received_ids = set(sub_result.received.keys())
        missing = sorted(set(c.id for c in cases) - received_ids)
        print(f"\\n=== 结果 ===")
        print(f"  已收: {len(received_ids)}/{len(cases)}")
        if sub_result.errors:
            for cid, err in sub_result.errors:
                print(f"  ❌ case {cid} ({cases[cid].data_type}, {cases[cid].compression}): {err}")
        if missing:
            print(f"  ❌ 缺失: {missing}")
        if not sub_result.errors and not missing:
            print(f"\\n✅ {len(cases)}/{len(cases)} cases passed")
            return 0
        return 1
    finally:
        stop_server(proc)


def main() -> int:
    parser = argparse.ArgumentParser(description="PulseMQ 端到端测试")
    parser.add_argument("--port", type=int, default=15555, help="server 端口 (默认 15555)")
    parser.add_argument("--timeout", type=float, default=30.0, help="整体超时秒数 (默认 30)")
    args = parser.parse_args()
    return asyncio.run(run_all(args.port, args.timeout))


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 2: 端到端运行测试**

Run: `cd D:/workflow/pulse-mq && uv run python scripts/test_e2e_all.py --port 15555 --timeout 30`

Expected: 输出形如：

```
=== PulseMQ 端到端测试 ===
  端口:   15555 / 15556
  用例:   16 (4 × 4)
  超时:   30s

=== 结果 ===
  已收: 16/16

✅ 16/16 cases passed
```

退出码 0。

- [ ] **Step 3: 验证错误检测（反向冒烟）**

为确认失败检测确实生效，临时把 `assert_payload_equal` 的 `str` 分支加一行 `assert False`：

```python
if case.data_type == "str":
    assert False, "故意失败以验证错误检测"  # ← 加这一行（跑完后删）
    assert isinstance(received, str), ...
```

Run: `cd D:/workflow/pulse-mq && uv run python scripts/test_e2e_all.py --port 15555 --timeout 30 ; echo "exit=$?"`

Expected: 退出码 1，stdout 中 `已收: 0/16`（因为所有 str 用例都会失败被记录到 errors），并打印 `case 0 (str, none): 故意失败...` 等 4 行。

确认后**立即删除**那行 `assert False`，重新跑一遍 `test_e2e_all.py`，确认恢复 16/16 通过、退出码 0。

- [ ] **Step 4: 提交**

```bash
git add scripts/test_e2e_all.py
git commit -m "test: 添加端到端全格式/全压缩集成测试 (16/16 roundtrip)"
```

---

## 验证清单

- [ ] `python scripts/test_e2e_all.py` 直接跑通
- [ ] 16/16 通过，退出码 0
- [ ] 杀掉 server 进程（手动 Ctrl+C server_runner）后，子进程清理无残留
- [ ] 重复跑 3 次都稳定通过
