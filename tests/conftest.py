"""PulseMQ 端到端测试共享 fixtures。

提供:
- 端口隔离、临时 SQLite
- Publisher 后台启动 / 优雅关闭
- 数据形态枚举 + 期望值生成
- 公共断言 helper
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import random
import sys
import tempfile
from typing import Any, AsyncIterator, Callable

import pandas as pd
import pytest

# Windows: 强制 Selector 事件循环。pyzmq 的 asyncio 集成不支持 Proactor。
if sys.platform == "win32" and hasattr(asyncio, "WindowsSelectorEventLoopPolicy"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

from pulsemq.config import PublisherConfig
from pulsemq.publisher import PulsePublisher
from pulsemq.protocol.frames import PulseMessage
from pulsemq.subscriber import PulseSubscriber


# ---------------------------------------------------------------------------
# 维度常量
# ---------------------------------------------------------------------------

SERIALIZERS: list[str] = ["msgpack", "json", "str", "bytes", "pyarrow"]
COMPRESSIONS: list[str] = ["none", "snappy", "lz4", "zstd"]

# 数据形态: (name, generator, kind)
#   kind 用于判断与序列化是否兼容
DATA_SHAPES: list[tuple[str, str]] = [
    "scalar_str",     # str
    "scalar_bytes",   # bytes
    "list_dict",      # list[dict]
    "dataframe",      # pd.DataFrame
    "large_dict",     # dict 1.1MB
]


# ---------------------------------------------------------------------------
# 端口与临时文件
# ---------------------------------------------------------------------------


def _rand_port() -> int:
    return random.randint(25000, 35000)


@pytest.fixture
def random_port_pair() -> tuple[int, int]:
    """返回 (pub_port, admin_port)，两端口互不相同。"""
    p = _rand_port()
    a = _rand_port()
    while a == p:
        a = _rand_port()
    return p, a


@pytest.fixture
def tmp_sqlite_url() -> str:
    """返回临时 SQLite URL（yield 后清理）。"""
    fd, path = tempfile.mkstemp(suffix=".sqlite")
    os.close(fd)
    try:
        yield f"sqlite://{path}"
    finally:
        for ext in ("", "-shm", "-wal"):
            try:
                os.unlink(path + ext)
            except OSError:
                pass


# ---------------------------------------------------------------------------
# Publisher 启动辅助
# ---------------------------------------------------------------------------


@contextlib.asynccontextmanager
async def running_publisher(
    pub: PulsePublisher, *, warmup: float = 0.5
) -> AsyncIterator[PulsePublisher]:
    """后台运行 pub._run()，yield 后优雅关闭。

    关闭流程:
    1. pub._running = False  → producer 任务 drain
    2. sleep 0.3             → minute_roll 跑最后一次
    3. task.cancel()         → 主循环退出
    """
    task = asyncio.create_task(pub._run())
    try:
        await asyncio.sleep(warmup)
        yield pub
    finally:
        pub._running = False
        await asyncio.sleep(0.3)
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await task


def make_publisher(
    *,
    pub_port: int,
    admin_port: int,
    tmp_db: str,
    api_keys: dict[str, str] | None = None,
) -> PulsePublisher:
    """构造 PulsePublisher，零业务 producer。"""
    return PulsePublisher(
        config=PublisherConfig(
            bind=f"tcp://127.0.0.1:{pub_port}",
            admin_bind=f"127.0.0.1:{admin_port}",
            stats_db=tmp_db,
        ),
        api_keys=api_keys,
    )


# ---------------------------------------------------------------------------
# 数据形态
# ---------------------------------------------------------------------------


def make_value(shape: str, seq: int = 0) -> Any:
    """根据形态生成测试值。"""
    if shape == "scalar_str":
        return f"hello-{seq}"
    if shape == "scalar_bytes":
        return seq.to_bytes(4, "big") + b"\x00\x01\x02\x03"
    if shape == "list_dict":
        return [{"seq": seq * 10 + i, "v": float(i)} for i in range(3)]
    if shape == "dataframe":
        return pd.DataFrame(
            {
                "seq": [seq * 10 + i for i in range(3)],
                "price": [10.0 + i * 0.1 for i in range(3)],
                "volume": [100 + i for i in range(3)],
            }
        )
    if shape == "large_dict":
        return {"seq": seq, "payload": "x" * 1_100_000}
    raise ValueError(f"未知 data shape: {shape}")


def expected_record_count(value: Any) -> int:
    """与 publisher._infer_record_count 保持一致的 record_count 推断。"""
    if isinstance(value, pd.DataFrame):
        return len(value)
    if isinstance(value, list):
        # list[DataFrame] 求和；list[dict] / list[str] / list[bytes] 取 len
        total = 0
        for item in value:
            if isinstance(item, pd.DataFrame):
                total += len(item)
            else:
                total += 1
        return total
    return 1


def is_compatible(ser: str, shape: str) -> bool:
    """判断 (serializer, shape) 是否为合法组合。

    兼容性基于实际协议层的语义:
    - str: 只接受 UTF-8 字符串
    - bytes: 只接受原始字节
    - json: msgspec.json 不支持 bytes（会抛 TypeError）
    - msgpack: 接受几乎所有可序列化结构
    - pyarrow: 订阅端按 pa.Table 反序列化，仅 DataFrame 兼容
    """
    if ser == "str" and shape != "scalar_str":
        return False
    if ser == "bytes" and shape != "scalar_bytes":
        return False
    if ser == "json" and shape == "scalar_bytes":
        return False
    if ser == "pyarrow" and shape != "dataframe":
        return False
    return True


# ---------------------------------------------------------------------------
# 公共断言
# ---------------------------------------------------------------------------


def assert_message_roundtrip(
    msg: PulseMessage,
    expected: Any,
    *,
    ser: str,
    comp: str,
    record_count: int,
) -> None:
    """端到端消息一致性核心断言。"""
    assert msg.serializer == ser, f"serializer: got {msg.serializer}, want {ser}"
    assert msg.compression == comp, f"compression: got {msg.compression}, want {comp}"
    assert msg.record_count == record_count, (
        f"record_count: got {msg.record_count}, want {record_count}"
    )
    assert msg.timestamp_ns > 0, "timestamp_ns 应为正"
    # payload 等值比较
    if isinstance(expected, pd.DataFrame):
        # publisher 会把 DataFrame 转为 list[dict]（msgpack/json/pyarrow 都做）
        # pyarrow 路径下 deserialize 返回 pa.Table，这里统一以 dict 列表比较
        got = msg.payload
        if hasattr(got, "to_pylist"):
            got = got.to_pylist()
        elif hasattr(got, "to_dict"):
            got = got.to_dict(orient="records")
        assert got == expected.to_dict(orient="records"), (
            f"DataFrame payload 不一致: got {got[:2]}..., want {expected.to_dict(orient='records')[:2]}..."
        )
    else:
        assert msg.payload == expected, (
            f"payload 不一致: got {msg.payload!r}, want {expected!r}"
        )


# ---------------------------------------------------------------------------
# 协程 fixture
# ---------------------------------------------------------------------------


@pytest.fixture
async def connected_subscriber() -> AsyncIterator[Callable]:
    """工厂: 构造并连接 PulseSubscriber，teardown 时关闭。"""
    subs: list[PulseSubscriber] = []

    async def _factory(address: str, *, username: str = "", password: str = "") -> PulseSubscriber:
        sub = PulseSubscriber(address, username=username, password=password)
        await sub.connect()
        subs.append(sub)
        return sub

    try:
        yield _factory
    finally:
        for s in subs:
            with contextlib.suppress(Exception):
                await s.close()
