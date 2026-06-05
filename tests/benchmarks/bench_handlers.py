"""纯 Python 层消息处理器吞吐基准测试。

不启动真实 ZMQ socket，直接测 handlers.dispatch() 的逻辑吞吐。
测试所有消息类型：PUB / SUB / UNSUB / PING / QUERY / HISTORY_REPLAY。
"""

import asyncio
import pytest
import time

from pulsemq.protocol.frames import FrameCodec
from pulsemq.protocol.msg_type import MsgType
from tests.benchmarks.data_generators import get_preset_single, get_preset_batch
from tests.benchmarks.conftest import (
    BenchResult,
    make_pub_frames,
    make_sub_frames,
    make_unsub_frames,
    make_ping_frames,
    make_query_frames,
    make_replay_frames,
)


# ---- PUB 吞吐 ----

class TestPubThroughput:
    """PUB 消息处理吞吐测试。"""

    async def test_pub_no_subscriber(self, full_handlers):
        """PUB 无订阅者：仅 topic 注册 + 消息缓存。"""
        handlers, _, _ = full_handlers
        data = get_preset_single()
        n = 10_000

        with BenchResult("PUB (无订阅者)") as br:
            for _ in range(n):
                frames = make_pub_frames("bench.topic", data)
                await handlers.dispatch(frames)
            br.set_ops(n)
        print(br.report())

    async def test_pub_with_subscriber(self, pub_sub_setup):
        """PUB 有订阅者：含广播。"""
        handlers, _, broadcast = pub_sub_setup
        data = get_preset_single()

        # 先订阅
        sub_frames = make_sub_frames("bench.topic")
        await handlers.dispatch(sub_frames)
        broadcast.clear()

        n = 10_000
        with BenchResult("PUB (有订阅者)") as br:
            for _ in range(n):
                frames = make_pub_frames("bench.topic", data)
                await handlers.dispatch(frames)
            br.set_ops(n)
        print(br.report())
        assert len(broadcast) == n

    async def test_pub_multi_topic(self, pub_sub_setup):
        """PUB 多 topic 吞吐。"""
        handlers, _, broadcast = pub_sub_setup
        data = get_preset_single()

        # 订阅 10 个 topic
        topics = [f"bench.topic_{i}" for i in range(10)]
        for t in topics:
            await handlers.dispatch(make_sub_frames(t))
        broadcast.clear()

        n = 10_000
        with BenchResult("PUB (10 topics)") as br:
            for i in range(n):
                t = topics[i % len(topics)]
                frames = make_pub_frames(t, data)
                await handlers.dispatch(frames)
            br.set_ops(n)
        print(br.report())

    @pytest.mark.parametrize("comp", ["none", "snappy", "lz4", "zstd"])
    async def test_pub_with_compression(self, pub_sub_setup, comp):
        """PUB 各压缩格式吞吐。"""
        handlers, _, broadcast = pub_sub_setup
        data = get_preset_single()

        await handlers.dispatch(make_sub_frames("bench.topic"))
        broadcast.clear()

        n = 5_000
        with BenchResult(f"PUB (msgpack+{comp})") as br:
            for _ in range(n):
                frames = make_pub_frames("bench.topic", data, "msgpack", comp)
                await handlers.dispatch(frames)
            br.set_ops(n)
        print(br.report())

    async def test_pub_batch_data(self, pub_sub_setup):
        """PUB 发送批量行情数据（单条消息 100 条行情）。"""
        handlers, _, broadcast = pub_sub_setup
        batch = get_preset_batch(100)

        await handlers.dispatch(make_sub_frames("bench.batch"))
        broadcast.clear()

        n = 2_000
        with BenchResult("PUB (100条行情/消息)") as br:
            for _ in range(n):
                frames = make_pub_frames("bench.batch", batch)
                await handlers.dispatch(frames)
            br.set_ops(n)
        print(br.report())
        # 实际吞吐 = n * 100 条行情
        print(f"  → 行情吞吐: {n * 100 / br.elapsed_s:,.0f} snapshots/s")


# ---- SUB / UNSUB 吞吐 ----

class TestSubUnsubThroughput:
    """SUB / UNSUB 消息处理吞吐测试。"""

    async def test_subscribe_throughput(self, full_handlers):
        handlers, sent, _ = full_handlers
        n = 10_000

        with BenchResult("SUB") as br:
            for i in range(n):
                frames = make_sub_frames(f"bench.sub_{i}")
                await handlers.dispatch(frames)
            br.set_ops(n)
        print(br.report())

    async def test_unsubscribe_throughput(self, full_handlers):
        handlers, sent, _ = full_handlers
        n = 5_000

        # 先订阅
        for i in range(n):
            await handlers.dispatch(make_sub_frames(f"bench.unsub_{i}"))
        sent.clear()

        with BenchResult("UNSUB") as br:
            for i in range(n):
                frames = make_unsub_frames(f"bench.unsub_{i}")
                await handlers.dispatch(frames)
            br.set_ops(n)
        print(br.report())

    async def test_subscribe_wildcard_throughput(self, full_handlers):
        """通配符订阅吞吐。"""
        handlers, _, _ = full_handlers
        n = 5_000

        with BenchResult("SUB (wildcard)") as br:
            for i in range(n):
                frames = make_sub_frames(f"team-{i % 10}.mkt.*")
                await handlers.dispatch(frames)
            br.set_ops(n)
        print(br.report())


# ---- PING 吞吐 ----

class TestPingThroughput:

    async def test_ping_throughput(self, full_handlers):
        handlers, sent, _ = full_handlers
        n = 10_000

        with BenchResult("PING") as br:
            for _ in range(n):
                frames = make_ping_frames()
                await handlers.dispatch(frames)
            br.set_ops(n)
        print(br.report())
        assert len(sent) == n


# ---- QUERY 吞吐 ----

class TestQueryThroughput:

    async def test_query_system_status_throughput(self, full_handlers):
        handlers, sent, _ = full_handlers
        n = 10_000

        with BenchResult("QUERY (system_status)") as br:
            for _ in range(n):
                frames = make_query_frames("system_status")
                await handlers.dispatch(frames)
            br.set_ops(n)
        print(br.report())


# ---- HISTORY_REPLAY 吞吐 ----

class TestReplayThroughput:

    async def test_history_replay_throughput(self, full_handlers):
        """先写入消息再回放，测试回放吞吐。"""
        handlers, sent, _ = full_handlers
        data = get_preset_single()

        # 先写入 1000 条消息
        for i in range(1000):
            frames = make_pub_frames("bench.replay", data)
            await handlers.dispatch(frames)

        sent.clear()
        n = 1000

        with BenchResult("HISTORY_REPLAY (1000条回放)") as br:
            for _ in range(n):
                frames = make_replay_frames("bench.replay", from_seq=0, limit=500)
                await handlers.dispatch(frames)
            br.set_ops(n)
        print(br.report())
        # 每次回放返回最多 500 条消息
        total_msgs = sum(len(s[1]) if isinstance(s[1], list) else 0 for s in sent)
        print(f"  → 总回放消息数: {total_msgs}")


# ---- 混合消息类型吞吐 ----

class TestMixedWorkload:
    """模拟真实场景的混合消息负载。"""

    async def test_mixed_pub_sub_ping(self, pub_sub_setup):
        """混合 PUB(70%) / SUB(10%) / UNSUB(5%) / PING(15%) 吞吐。"""
        handlers, _, broadcast = pub_sub_setup
        data = get_preset_single()

        # 预订阅
        for i in range(20):
            await handlers.dispatch(make_sub_frames(f"bench.mix_{i}"))
        broadcast.clear()

        n = 10_000
        topic_idx = 0

        with BenchResult("混合负载 (PUB 70% / SUB 10% / UNSUB 5% / PING 15%)") as br:
            for i in range(n):
                r = i % 100
                if r < 70:
                    frames = make_pub_frames(f"bench.mix_{topic_idx % 20}", data)
                    topic_idx += 1
                elif r < 80:
                    frames = make_sub_frames(f"bench.mix_new_{i}")
                elif r < 85:
                    frames = make_unsub_frames(f"bench.mix_{i % 20}")
                else:
                    frames = make_ping_frames()
                await handlers.dispatch(frames)
            br.set_ops(n)
        print(br.report())
