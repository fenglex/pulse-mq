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

# 数据形态: 覆盖 4 种白名单类型
DATA_SHAPES: list[tuple[str, str]] = [
    "scalar_str",      # str
    "scalar_bytes",    # bytes
    "dataframe",       # pd.DataFrame
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
    return 1


def is_compatible(ser: str, shape: str) -> bool:
    """判断 (serializer, shape) 是否为合法组合（方案 A：强类型绑定）。

    收紧后的规则：
    - str 数据（scalar_str）只允许 'str' 序列化器
    - bytes 数据（scalar_bytes）只允许 'bytes' 序列化器
    - 结构化数据（dataframe/large_dict）允许 msgpack/json/pyarrow
    """
    if shape == "scalar_str":
        # str 数据强制用 str 序列化器
        return ser == "str"
    if shape == "scalar_bytes":
        # bytes 数据强制用 bytes 序列化器
        return ser == "bytes"
    # 结构化数据（dataframe / large_dict）
    if shape in ("dataframe", "large_dict"):
        return ser in ("msgpack", "json", "pyarrow")
    return False


# ---------------------------------------------------------------------------
# 公共断言
# ---------------------------------------------------------------------------


def _type_tag(obj: Any) -> str:
    """返回用于类型保真比较的简短类型标签。

    统一处理 DataFrame 等类型，让 pub 端原始类型与 sub 端还原后的类型可比。
    """
    import pandas as _pd
    if isinstance(obj, _pd.DataFrame):
        return "DataFrame"
    return type(obj).__name__


def assert_message_roundtrip(
    msg: PulseMessage,
    expected: Any,
    *,
    ser: str,
    comp: str,
    record_count: int,
) -> None:
    """端到端消息一致性核心断言（v3：类型保真）。

    v3 起 sub 端 payload 应还原为 pub 端原始类型，本断言：
    1. 类型保真：sub 端 payload 类型标签 == 原始 expected 类型标签
    2. 值相等：DataFrame 用 assert_frame_equal，其余直接 ==
    """
    assert msg.serializer == ser, f"serializer: got {msg.serializer}, want {ser}"
    assert msg.compression == comp, f"compression: got {msg.compression}, want {comp}"
    assert msg.record_count == record_count, (
        f"record_count: got {msg.record_count}, want {record_count}"
    )
    assert msg.timestamp_ns > 0, "timestamp_ns 应为正"

    got = msg.payload

    # 1. 类型保真断言
    got_tag = _type_tag(got)
    expected_tag = _type_tag(expected)
    assert got_tag == expected_tag, (
        f"类型不保真: pub={expected_tag}, sub={got_tag} "
        f"(data_type={msg.data_type}, serializer={ser})"
    )

    # 2. 值相等断言（按类型分别处理）
    import pandas as pd

    if isinstance(expected, pd.DataFrame):
        # DataFrame：列顺序/类型可能因序列化路径略有差异，用 assert_frame_equal 宽松比较
        pd.testing.assert_frame_equal(
            got.reset_index(drop=True), expected.reset_index(drop=True),
            check_dtype=False, check_like=True,  # 忽略列顺序与 dtype 差异
        )
    else:
        # dict / str / bytes：直接 ==（pyarrow 路径已还原）
        assert got == expected, (
            f"payload 不一致: got {str(got)[:120]}, want {str(expected)[:120]}"
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
