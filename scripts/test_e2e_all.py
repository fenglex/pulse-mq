#!/usr/bin/env python3
"""PulseMQ 端到端全格式/全压缩集成测试。

启动一个 server + 两个 client（publisher / subscriber），
覆盖 4 数据类型 × 4 压缩 = 16 组合，验证 payload 往返一致。

依赖：
  - scripts/test_server_runner.py（关闭认证、关闭指标，输出 READY）
  - 修复过的 PulseClient（_decode_message 读 FrameFlags，df+msgpack 路径先转 list[dict]）

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

# 让脚本能找到 pulsemq（从仓库根目录运行）
sys.path.insert(0, "src")

# Windows 上 pyzmq 不兼容 ProactorEventLoop，必须在 asyncio.run 之前切换
from pulsemq.event_loop import install_event_loop

if sys.platform == "win32":
    install_event_loop(use_uvloop=False)

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
# Payload 断言
# ---------------------------------------------------------------------------

def assert_payload_equal(received: Any, case: Case) -> None:
    """比对 received 与 case.original；不一致抛 AssertionError。"""
    orig = case.original
    if case.data_type == "str":
        assert isinstance(received, str), \
            f"type mismatch: expected str, got {type(received).__name__}"
        assert received == orig, \
            f"str mismatch:\n  expected: {orig!r}\n  got:      {received!r}"
    elif case.data_type == "bytes":
        assert isinstance(received, (bytes, bytearray)), \
            f"type mismatch: expected bytes, got {type(received).__name__}"
        assert bytes(received) == orig, \
            f"bytes mismatch (len expected={len(orig)}, got={len(received)})"
    elif case.data_type == "df-msgpack":
        # msgpack 路径：received 是 list[dict]（msgpack 反序列化的结果）
        assert isinstance(received, list), \
            f"df-msgpack: expected list, got {type(received).__name__}"
        received_df = pd.DataFrame(received)
        # bytes 列在 dict 里是 bytes，DataFrame 还原后是 object 列；orig 已经是 DataFrame
        # pd.testing.assert_frame_equal 对 bytes 列也能比
        pd.testing.assert_frame_equal(received_df, orig)
    elif case.data_type == "df-pyarrow":
        # pyarrow 路径：received 是 pa.Table
        import pyarrow as pa
        assert isinstance(received, pa.Table), \
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
                    f"server_runner 提前退出 (rc={proc.returncode})\n"
                    f"stderr: {stderr}"
                )
            continue
        if line.strip() == "READY":
            return proc
    proc.kill()
    raise TimeoutError(f"等待 READY 超时（{ready_timeout}s）")


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
        # 等订阅就绪（subscriber 完成 setsockopt + DEALER SUB 注册）
        await asyncio.sleep(0.3)
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
    print("=== PulseMQ 端到端测试 ===")
    print(f"  端口:   {port} / {port + 1}")
    print(f"  用例:   {len(cases)} ({len(DATA_TYPES)} × {len(COMPRESSIONS)})")
    print(f"  超时:   {timeout}s")
    print()

    proc = start_server(port)
    publisher_done = asyncio.Event()
    try:
        pub_task = asyncio.create_task(run_publisher(port, cases, publisher_done))
        sub_task = asyncio.create_task(run_subscriber(port, cases, publisher_done))
        try:
            results = await asyncio.wait_for(
                asyncio.gather(sub_task, pub_task, return_exceptions=True),
                timeout=timeout,
            )
        except asyncio.TimeoutError:
            pub_task.cancel()
            sub_task.cancel()
            await asyncio.gather(pub_task, sub_task, return_exceptions=True)
            print(f"[FAIL] TIMEOUT after {timeout}s")
            return 1

        sub_result = results[0]
        if isinstance(sub_result, Exception):
            print(f"[FAIL] Subscriber crashed: {sub_result!r}")
            return 1

        # 汇总
        received_ids = set(sub_result.received.keys())
        missing = sorted(set(c.id for c in cases) - received_ids)
        print("\n=== 结果 ===")
        print(f"  已收: {len(received_ids)}/{len(cases)}")
        if sub_result.errors:
            for cid, err in sub_result.errors:
                print(f"  [FAIL] case {cid} ({cases[cid].data_type}, {cases[cid].compression}): {err}")
        if missing:
            print(f"  [FAIL] 缺失: {missing}")
        if not sub_result.errors and not missing:
            print(f"\n[PASS] {len(cases)}/{len(cases)} cases passed")
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
