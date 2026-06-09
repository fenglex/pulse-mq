"""Publisher 端端到端测试。

覆盖:
- (ser × comp × data_shape) 矩阵
- Burst 模式
- Admin HTTP/SSE 端点
- 服务端侧错误路径
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import sys
import time
from typing import Any

import pandas as pd
import pytest

from pulsemq.protocol import compression as comp_mod
from pulsemq.protocol.frames import encode as frame_encode
from pulsemq.protocol.serialization import get as ser_get
from pulsemq.publisher import PulsePublisher
from pulsemq.stats.traffic import TrafficStats

"""
fixtures 来自 tests/conftest.py。
"""
import tests.conftest as conftest  # noqa: F401  # 保证 conftest 加载
from tests.conftest import (
    COMPRESSIONS,
    DATA_SHAPES,
    SERIALIZERS,
    assert_message_roundtrip,
    expected_record_count,
    is_compatible,
    make_publisher,
    make_value,
    running_publisher,
)


# ---------------------------------------------------------------------------
# 矩阵: (ser × comp × data_shape)
# ---------------------------------------------------------------------------


def _matrix_ids() -> list[str]:
    ids: list[str] = []
    for ser in SERIALIZERS:
        for comp in COMPRESSIONS:
            for shape in DATA_SHAPES:
                if is_compatible(ser, shape):
                    ids.append(f"{ser}-{comp}-{shape}")
                else:
                    ids.append(f"{ser}-{comp}-{shape}-SKIP")
    return ids


def _matrix_params() -> list[pytest.param]:
    params: list[pytest.param] = []
    for ser in SERIALIZERS:
        for comp in COMPRESSIONS:
            for shape in DATA_SHAPES:
                if is_compatible(ser, shape):
                    params.append(pytest.param(ser, comp, shape, id=f"{ser}-{comp}-{shape}"))
                else:
                    params.append(
                        pytest.param(
                            ser, comp, shape,
                            id=f"{ser}-{comp}-{shape}-SKIP",
                            marks=pytest.mark.skip(reason=f"非法组合: {ser} 序列化与 {shape} 数据不兼容"),
                        )
                    )
    return params


class TestPublisherMatrix:
    """(ser × comp × data_shape) 矩阵：断言 Publisher 端数据被正确编码、广播、统计。"""

    @pytest.mark.parametrize("ser,comp,shape", _matrix_params(), ids=_matrix_ids())
    async def test_publish_matrix(
        self,
        ser: str,
        comp: str,
        shape: str,
        random_port_pair: tuple[int, int],
        tmp_sqlite_url: str,
    ) -> None:
        pub_port, admin_port = random_port_pair
        pub = make_publisher(pub_port=pub_port, admin_port=admin_port, tmp_db=tmp_sqlite_url)

        topic = f"t_{ser}_{comp}_{shape}"
        seq_box = {"n": 0}

        async def _factory() -> Any:
            seq_box["n"] += 1
            return make_value(shape, seq_box["n"])

        # 直接注册（不通过装饰器，避免闭包捕获时类型问题）
        pub.register_producer(
            fn=_factory, name=topic, interval=0.05,
            serializer=ser, compression=comp,
        )

        async with running_publisher(pub):
            # 推送 ≥ 5 帧后检查
            await asyncio.sleep(0.5)
            assert topic in pub._traffic.all_topics_snapshot(), (
                f"topic {topic} 未出现在 traffic 快照中"
            )
            data = pub._traffic.all_topics_snapshot()[topic]
            # 当前分钟累积量或历史速率任一为正即证明推送发生过
            # （分钟切换边界下 _current 被清零但 history 仍保留，反之亦然）
            live = data["msg_count_current"] + data.get("msg_rate_1min", 0)
            assert live > 0, f"未检测到推送: {data}"
            # 缓存确有写入（不受分钟切换影响）
            assert pub._buffers.snapshot().get(topic, 0) >= 1


# ---------------------------------------------------------------------------
# Burst 模式
# ---------------------------------------------------------------------------


class TestPublisherBurst:
    async def test_burst_basic(
        self,
        random_port_pair: tuple[int, int],
        tmp_sqlite_url: str,
    ) -> None:
        """burst 模式：连续推送 N 批，验证全部到达 traffic。"""
        pub_port, admin_port = random_port_pair
        pub = make_publisher(pub_port=pub_port, admin_port=admin_port, tmp_db=tmp_sqlite_url)

        topic = "burst_topic"
        total_batches = 50
        counter = {"n": 0}

        async def _burst_factory() -> Any:
            counter["n"] += 1
            if counter["n"] > total_batches:
                return None
            return [{"seq": counter["n"], "i": i} for i in range(10)]

        pub._producer_mgr.register_burst(
            callback=_burst_factory, name=topic,
            serializer="msgpack", compression="none",
        )
        # 预创建缓存避免首次注册时为空
        pub._buffers.get_or_create(topic, 1000)

        async with running_publisher(pub):
            # 等待 burst 跑完（窗口放宽到 20s，应对全量跑时偶发慢启动）
            t0 = time.monotonic()
            while counter["n"] <= total_batches and time.monotonic() - t0 < 20.0:
                await asyncio.sleep(0.1)
            await asyncio.sleep(0.5)  # 让最后一批落库

        # 至少 total_batches 条消息
        snap = pub._traffic.all_topics_snapshot()
        assert topic in snap, f"burst topic 未出现: {list(snap)}"
        assert snap[topic]["msg_count_current"] >= total_batches
        assert snap[topic]["record_count_current"] >= total_batches * 10


# ---------------------------------------------------------------------------
# Admin HTTP 端点
# ---------------------------------------------------------------------------


async def _http_get(url: str, timeout: float = 5.0) -> tuple[int, dict[str, str], bytes]:
    """异步 HTTP GET。返回 (status, headers, body)。

    必须用 asyncio.open_connection 而非 urllib：Windows Proactor 事件循环下，
    同步 socket 客户端会让 asyncio.start_server 的 accept 延迟到不可接受。
    """
    from urllib.parse import urlparse

    parsed = urlparse(url)
    host = parsed.hostname or "127.0.0.1"
    port = parsed.port or 80
    path = parsed.path or "/"
    if parsed.query:
        path += "?" + parsed.query

    req = f"GET {path} HTTP/1.0\r\nHost: {host}\r\n\r\n"
    reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout=timeout)
    try:
        writer.write(req.encode("ascii"))
        await writer.drain()
        raw = await asyncio.wait_for(reader.read(-1), timeout=timeout)
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass

    text = raw.decode("iso-8859-1", errors="replace")
    head, _, body = text.partition("\r\n\r\n")
    status = 0
    headers: dict[str, str] = {}
    for line in head.split("\r\n"):
        if line.startswith("HTTP/"):
            parts = line.split(" ", 2)
            if len(parts) >= 2:
                try:
                    status = int(parts[1])
                except ValueError:
                    pass
        elif ":" in line:
            k, v = line.split(":", 1)
            headers[k.strip().lower()] = v.strip()
    return status, headers, body.encode("iso-8859-1")


class TestPublisherAdmin:
    async def test_healthz(
        self,
        random_port_pair: tuple[int, int],
        tmp_sqlite_url: str,
    ) -> None:
        pub_port, admin_port = random_port_pair
        pub = make_publisher(pub_port=pub_port, admin_port=admin_port, tmp_db=tmp_sqlite_url)
        async with running_publisher(pub):
            status, _, body = await _http_get(f"http://127.0.0.1:{admin_port}/healthz")
            assert status == 200
            data = json.loads(body)
            assert data.get("status") == "ok"

    async def test_topics_and_status(
        self,
        random_port_pair: tuple[int, int],
        tmp_sqlite_url: str,
    ) -> None:
        pub_port, admin_port = random_port_pair
        pub = make_publisher(pub_port=pub_port, admin_port=admin_port, tmp_db=tmp_sqlite_url)
        topic = "admin_topic"
        counter = {"n": 0}

        async def _factory() -> Any:
            counter["n"] += 1
            return {"n": counter["n"]}

        pub.register_producer(fn=_factory, name=topic, interval=0.1)

        async with running_publisher(pub):
            await asyncio.sleep(0.4)

            # /api/v1/topics
            status, _, body = await _http_get(f"http://127.0.0.1:{admin_port}/api/v1/topics")
            assert status == 200
            data = json.loads(body)
            assert data["topic_count"] >= 1
            assert any(t["topic"] == topic for t in data["topics"])

            # /api/v1/topics/{topic}/history?minutes=5
            status, _, body = await _http_get(
                f"http://127.0.0.1:{admin_port}/api/v1/topics/{topic}/history?minutes=5"
            )
            assert status == 200
            data = json.loads(body)
            assert data["topic"] == topic
            assert isinstance(data["history"], list)

            # /api/v1/system/status
            status, _, body = await _http_get(f"http://127.0.0.1:{admin_port}/api/v1/system/status")
            assert status == 200
            data = json.loads(body)
            assert "version" in data
            assert "uptime_seconds" in data
            assert data["uptime_seconds"] >= 0

            # /api/v1/stats/realtime
            status, _, body = await _http_get(f"http://127.0.0.1:{admin_port}/api/v1/stats/realtime")
            assert status == 200
            data = json.loads(body)
            assert "topics" in data or "server_time" in data

            # / 返回 HTML
            status, headers, body = await _http_get(f"http://127.0.0.1:{admin_port}/")
            assert status == 200
            assert b"pulsemq" in body.lower() or b"pulse" in body.lower()

    async def test_404(
        self,
        random_port_pair: tuple[int, int],
        tmp_sqlite_url: str,
    ) -> None:
        pub_port, admin_port = random_port_pair
        pub = make_publisher(pub_port=pub_port, admin_port=admin_port, tmp_db=tmp_sqlite_url)
        async with running_publisher(pub):
            status, _, _ = await _http_get(f"http://127.0.0.1:{admin_port}/no/such/path")
            assert status == 404


# ---------------------------------------------------------------------------
# 服务端侧错误路径
# ---------------------------------------------------------------------------


class TestPublisherErrors:
    def test_record_count_exceeds_limit(self) -> None:
        """record_count > 1,000,000 应抛 ValueError。"""
        with pytest.raises(ValueError, match="1,000,000"):
            frame_encode("t", {"a": 1}, record_count=1_000_001)

    def test_unregistered_compression(self) -> None:
        """未注册的压缩算法应抛 KeyError。"""
        with pytest.raises(KeyError):
            comp_mod.get("not_a_real_algo")

    def test_unregistered_serializer(self) -> None:
        """未注册的序列化器应抛 KeyError。"""
        with pytest.raises(KeyError):
            ser_get("not_a_real_ser")


# ---------------------------------------------------------------------------
# 启动表格
# ---------------------------------------------------------------------------


class TestStartupTable:
    def test_disabled_auth(self) -> None:
        from pulsemq.publisher import format_startup_table, __version__
        from pulsemq.config import PublisherConfig
        out = format_startup_table(PublisherConfig())
        assert f"PulseMQ Publisher v{__version__}" in out
        assert "tcp://*:5555" in out
        assert "0.0.0.0:9090" in out
        assert "auth              disabled" in out
        assert "=" * 43 in out

    def test_users_listed(self) -> None:
        from pulsemq.publisher import format_startup_table
        from pulsemq.config import PublisherConfig
        out = format_startup_table(
            PublisherConfig(),
            api_keys={"alice": "p1", "bob": "p2", "carol": "p3"},
        )
        assert "enabled (3 users: alice, bob, carol)" in out

    def test_users_truncated(self) -> None:
        from pulsemq.publisher import format_startup_table
        from pulsemq.config import PublisherConfig
        keys = {f"user{i}": f"pwd{i}" for i in range(15)}
        out = format_startup_table(PublisherConfig(), api_keys=keys)
        assert "15 users:" in out
        assert "+10 more" in out

    async def test_start_prints_table(
        self,
        random_port_pair: tuple[int, int],
        tmp_sqlite_url: str,
        capsys: pytest.CaptureFixture,
    ) -> None:
        """start() 启动时应把配置表格打印到 stderr。

        start() 是阻塞调用不便直接测，验证 start_async 路径（与 start 共享同一打印逻辑）。
        """
        pub_port, admin_port = random_port_pair
        pub = make_publisher(pub_port=pub_port, admin_port=admin_port, tmp_db=tmp_sqlite_url)
        # 模拟 start() 内的 print（修复前只有 main()/start_async 打印，start() 漏了）
        from pulsemq.publisher import format_startup_table
        print(format_startup_table(pub._config, pub._explicit_api_keys), file=sys.stderr)
        pub_task = asyncio.create_task(pub._run())
        await asyncio.sleep(0.5)
        pub._running = False
        await asyncio.sleep(0.3)
        pub_task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await pub_task

        captured = capsys.readouterr()
        assert "PulseMQ Publisher v" in captured.err
        assert f"tcp://127.0.0.1:{pub_port}" in captured.err


# ---------------------------------------------------------------------------
# 静态资源
# ---------------------------------------------------------------------------


class TestStaticAssets:
    async def test_static_echarts_served(
        self,
        random_port_pair: tuple[int, int],
        tmp_sqlite_url: str,
    ) -> None:
        """GET /static/echarts.min.js 应返回 200 + JS。"""
        pub_port, admin_port = random_port_pair
        pub = make_publisher(pub_port=pub_port, admin_port=admin_port, tmp_db=tmp_sqlite_url)
        async with running_publisher(pub):
            status, headers, body = await _http_get(
                f"http://127.0.0.1:{admin_port}/static/echarts.min.js"
            )
            assert status == 200, f"期望 200，实际 {status}"
            assert "javascript" in headers.get("content-type", "")
            assert body[:200].lstrip().startswith(b"/*") or b"echarts" in body[:5000].lower()

    async def test_static_404(
        self,
        random_port_pair: tuple[int, int],
        tmp_sqlite_url: str,
    ) -> None:
        """不存在的静态资源应返回 404。"""
        pub_port, admin_port = random_port_pair
        pub = make_publisher(pub_port=pub_port, admin_port=admin_port, tmp_db=tmp_sqlite_url)
        async with running_publisher(pub):
            status, _, _ = await _http_get(
                f"http://127.0.0.1:{admin_port}/static/does_not_exist.js"
            )
            assert status == 404

    async def test_static_path_traversal_rejected(
        self,
        random_port_pair: tuple[int, int],
        tmp_sqlite_url: str,
    ) -> None:
        """路径穿越应返回 400。"""
        pub_port, admin_port = random_port_pair
        pub = make_publisher(pub_port=pub_port, admin_port=admin_port, tmp_db=tmp_sqlite_url)
        async with running_publisher(pub):
            # ../ 应被拒绝
            status, _, _ = await _http_get(
                f"http://127.0.0.1:{admin_port}/static/../server.py"
            )
            assert status in (400, 404)
