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

# 数据形态: 覆盖 7 种白名单类型
DATA_SHAPES: list[tuple[str, str]] = [
    "scalar_str",      # str
    "scalar_bytes",    # bytes
    "list_dict",       # list[dict]
    "list_str",        # list[str]
    "dataframe",       # pd.DataFrame
    "list_dataframe",  # list[pd.DataFrame]
    "large_dict",      # dict 1.1MB
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
    if shape == "list_str":
        return [f"msg-{seq}-{i}" for i in range(3)]
    if shape == "dataframe":
        return pd.DataFrame(
            {
                "seq": [seq * 10 + i for i in range(3)],
                "price": [10.0 + i * 0.1 for i in range(3)],
                "volume": [100 + i for i in range(3)],
            }
        )
    if shape == "list_dataframe":
        # 两个 DataFrame：2 行 + 3 行 = 行数和 5
        return [
            pd.DataFrame({"seq": [seq * 10, seq * 10 + 1], "v": [1.0, 2.0]}),
            pd.DataFrame({"seq": [seq * 10 + 2, seq * 10 + 3, seq * 10 + 4], "v": [3.0, 4.0, 5.0]}),
        ]
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
    """判断 (serializer, shape) 是否为合法组合（方案 A：强类型绑定）。

    收紧后的规则：
    - str 数据（scalar_str）只允许 'str' 序列化器
    - bytes 数据（scalar_bytes）只允许 'bytes' 序列化器
    - 结构化数据（dataframe/list_dict/large_dict）允许 msgpack/json/pyarrow
    - list[str] 允许 msgpack/json（pyarrow 不支持）
    """
    if shape == "scalar_str":
        # str 数据强制用 str 序列化器
        return ser == "str"
    if shape == "scalar_bytes":
        # bytes 数据强制用 bytes 序列化器
        return ser == "bytes"
    # 结构化数据（dataframe / list_dict / large_dict / list_str）
    if shape in ("dataframe", "list_dict", "large_dict", "list_dataframe"):
        return ser in ("msgpack", "json", "pyarrow")
    if shape == "list_str":
        return ser in ("msgpack", "json")
    return False


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

    # 把 expected 统一规整为"可比形态"：
    # - DataFrame → list[dict]
    # - list[DataFrame] → 展平为 list[dict]（与 publisher._prepare_payload 一致）
    expected_normalized: Any = expected
    if isinstance(expected, pd.DataFrame):
        expected_normalized = expected.to_dict(orient="records")
    elif isinstance(expected, list) and expected and isinstance(expected[0], pd.DataFrame):
        expected_normalized = []
        for df in expected:
            expected_normalized.extend(df.to_dict(orient="records"))

    # payload 等值比较
    got = msg.payload
    # pyarrow 反序列化返回 pa.Table，转成 list[dict] 比较
    if hasattr(got, "to_pylist") and not isinstance(got, list):
        got = got.to_pylist()
    elif hasattr(got, "to_dict") and not isinstance(got, (list, dict)):
        got = got.to_dict(orient="records")

    # 形态对齐：pyarrow 对单个 dict 会返回 1 行 Table → list[dict]，
    # 而原 expected 是 dict。两侧统一：单元素 list[dict] ↔ dict 互转。
    if isinstance(got, list) and len(got) == 1 and isinstance(got[0], dict) \
            and isinstance(expected_normalized, dict):
        got = got[0]
    elif isinstance(expected_normalized, list) and len(expected_normalized) == 1 \
            and isinstance(expected_normalized[0], dict) and isinstance(got, dict):
        expected_normalized = expected_normalized[0]

    assert got == expected_normalized, (
        f"payload 不一致: got {str(got)[:120]}, want {str(expected_normalized)[:120]}"
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
