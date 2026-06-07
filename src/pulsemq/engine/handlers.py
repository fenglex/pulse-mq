"""消息类型分发和处理器。

集成拦截器链：Auth → Permission → Monitor → Handler
"""

from __future__ import annotations

import asyncio
import logging
import struct
import time
from collections.abc import Awaitable, Callable

from pulsemq.engine.pipeline import (
    AuthError,
    InterceptorChain,
    MonitorInterceptor,
    PipelineContext,
    PermissionError,
)
from pulsemq.engine.pool import PipelineContextPool
from pulsemq.engine.router import MessageRouter
from pulsemq.models import TopicInfo
from pulsemq.monitoring.client_tracker import ClientTracker
from pulsemq.monitoring.realtime import TopicMetricsRegistry
from pulsemq.protocol.flags import FrameFlags
from pulsemq.protocol.frames import FrameCodec, _RECORD_COUNT_STRUCT
from pulsemq.protocol.msg_type import MsgType

logger = logging.getLogger(__name__)


class MessageHandlers:
    """消息处理器集合。"""

    def __init__(
        self,
        router: MessageRouter,
        send_fn: Callable[[bytes, list[bytes]], Awaitable[None] | None],
        broadcast_fn: Callable[[list[bytes]], Awaitable[None] | None],
        pipeline: InterceptorChain | None = None,
        default_ser: str = "msgpack",
        default_comp: str = "none",
        client_tracker: ClientTracker | None = None,
        topic_metrics: TopicMetricsRegistry | None = None,
    ):
        self.router = router
        self._send = send_fn
        self._broadcast = broadcast_fn
        self._pipeline = pipeline
        self._default_ser = default_ser
        self._default_comp = default_comp
        self._ctx_pool = PipelineContextPool(size=4096)
        # Phase 4: 客户端追踪器 (可选注入, 兼容旧调用方)
        self._client_tracker = client_tracker
        # Phase 5: topic 维度 1-min 监控 (可选注入)
        self._topic_metrics = topic_metrics

        # 预计算 broadcast 帧的固定部分（#1 优化）
        _flags = FrameFlags(ser_fmt=default_ser, comp=default_comp, has_topic=True)
        self._broadcast_meta = bytes([MsgType.BROADCAST, _flags.encode()])
        self._broadcast_meta_no_topic = bytes(
            [MsgType.BROADCAST,
             FrameFlags(ser_fmt=default_ser, comp=default_comp, has_topic=False).encode()]
        )
        # topic_bytes 缓存（topic → bytes）
        self._topic_bytes_cache: dict[str, bytes] = {}

        # #7 优化：broadcast 解耦队列（由 Engine.run() 注入）
        self._broadcast_queue: asyncio.Queue | None = None

    def set_broadcast_queue(self, queue: asyncio.Queue) -> None:
        """注入 broadcast 解耦队列（#7 优化）。"""
        self._broadcast_queue = queue

    async def dispatch(self, server_frames: list[bytes]) -> None:
        """根据 msg_type 分发到对应处理器（含拦截器链）。"""
        # 解码必须在 try 内, 否则非法帧会逃逸异常, 引擎主循环 catch 不到
        try:
            decoded = FrameCodec.decode_server(server_frames)
            msg_type = decoded.msg_type
        except Exception as e:
            logger.warning("帧解码失败: %s (frames=%d)", e, len(server_frames))
            return

        # 从对象池获取上下文
        ctx = self._ctx_pool.acquire(
            identity=decoded.identity,
            msg_type=msg_type,
            topic=decoded.topic,
            meta=bytes([msg_type, decoded.flags.encode()]),
            payload=decoded.payload,
            record_count=decoded.record_count,
        )

        try:
            # 执行拦截器链，handler 作为末端执行器
            if self._pipeline is not None:
                async def _handle():
                    await self._dispatch_internal(ctx, server_frames)
                await self._pipeline.execute(ctx, terminal_handler=_handle)
            else:
                await self._dispatch_internal(ctx, server_frames)

        except AuthError as e:
            await self._send_error(decoded.identity, 1001, str(e))
        except PermissionError as e:
            await self._send_error(decoded.identity, 2001, str(e), decoded.topic)
        except Exception as e:
            logger.exception("消息处理异常")
            await self._send_error(decoded.identity, 9001, "内部错误")
        finally:
            # 归还上下文到对象池
            self._ctx_pool.release(ctx)

    async def dispatch_pub_fast(self, frames: list[bytes]) -> None:
        """PUB 快速路径：跳过 pipeline + ctx_pool，直接处理。

        适用于 auth 关闭或无需鉴权的场景。
        frames 为 ROUTER 收到的原始帧（5-6 帧）。
        """
        # 内联解码，避免 DecodedFrame dataclass 开销
        if len(frames) == 6:
            identity = frames[0]
            topic = frames[2].decode("utf-8")
            wire_meta = frames[3]      # 原始 2 字节 meta（msg_type + flags）
            payload = frames[5]
            record_count = _RECORD_COUNT_STRUCT.unpack(frames[4])[0]
        else:  # 5 帧
            identity = frames[0]
            topic = frames[1].decode("utf-8")
            wire_meta = frames[2]
            payload = frames[4]
            record_count = _RECORD_COUNT_STRUCT.unpack(frames[3])[0]

        # Phase 4: 客户端追踪 - 发布计数
        if self._client_tracker is not None:
            self._client_tracker.on_pub(identity, len(payload))

        # Phase 5: topic 1-min 监控 (快速路径没有 ctx.timestamp,
        # 直接用 0 延迟占位, 不影响分位数)
        if self._topic_metrics is not None:
            self._topic_metrics.record(topic, 0.0)

        # 构造 broadcast meta：保留原始 ser/comp，仅替换 msg_type=BROADCAST
        broadcast_meta = self._build_broadcast_meta(wire_meta)

        # 注册 topic（幂等）
        self.router.register_topic(topic)

        # 轻量级订阅者检查（#2 优化：不拷贝 set）
        if self.router.has_subscribers(topic):
            topic_bytes = self._get_topic_bytes(topic)
            rc_bytes = _RECORD_COUNT_STRUCT.pack(record_count)
            broadcast_frames = [topic_bytes, broadcast_meta, rc_bytes, payload]
            # #7 优化：通过解耦队列发送，不阻塞主循环
            if self._broadcast_queue is not None:
                self._broadcast_queue.put_nowait(broadcast_frames)
            else:
                result = self._broadcast(broadcast_frames)
                if result is not None:
                    await result

        # 条件化缓存（#4 优化）
        if self.router.buffer_enabled:
            meta = bytes([MsgType.PUB, 0])
            self.router.append_message(topic, meta, record_count, payload)

    def _get_topic_bytes(self, topic: str) -> bytes:
        """获取 topic 的 UTF-8 编码（带缓存）。"""
        cached = self._topic_bytes_cache.get(topic)
        if cached is not None:
            return cached
        b = topic.encode("utf-8")
        self._topic_bytes_cache[topic] = b
        return b

    @staticmethod
    def _build_broadcast_meta(wire_meta: bytes) -> bytes:
        """从原始 PUB 帧的 meta 构造 BROADCAST meta。

        保留原始 ser_fmt/comp/has_topic 标志，仅把 msg_type 替换为 BROADCAST，
        让 subscriber 端能正确反序列化（之前用 default_ser/default_comp 预计算的
        _broadcast_meta 会丢失每条消息的 ser/comp 信息）。
        """
        flags_byte = wire_meta[1] if len(wire_meta) > 1 else 0
        return bytes([MsgType.BROADCAST, flags_byte])

    async def _dispatch_internal(self, ctx: PipelineContext, server_frames: list[bytes]) -> None:
        """内部分发。"""
        if ctx.msg_type == MsgType.PUB:
            await self._handle_pub(ctx)
        elif ctx.msg_type == MsgType.SUB:
            await self._handle_sub(ctx)
        elif ctx.msg_type == MsgType.UNSUB:
            await self._handle_unsub(ctx)
        elif ctx.msg_type == MsgType.PING:
            await self._handle_ping(ctx)
        elif ctx.msg_type == MsgType.QUERY:
            await self._handle_query(ctx)
        elif ctx.msg_type == MsgType.HISTORY_REPLAY:
            await self._handle_history_replay(ctx)
        elif ctx.msg_type == MsgType.BATCH:
            await self._handle_batch(ctx)
        # 其他类型暂忽略

    async def _handle_pub(self, ctx: PipelineContext) -> None:
        """处理 PUB：注册 topic → 广播 → 缓存。"""
        topic = ctx.topic
        record_count = ctx.record_count

        # 注册 topic（幂等）
        self.router.register_topic(topic)

        # Phase 4: 客户端追踪 - 发布者 PUB 计数 (msg_out)
        if self._client_tracker is not None:
            self._client_tracker.on_pub(ctx.identity, len(ctx.payload))

        # Phase 5: topic 1-min 监控 - 记录延迟
        if self._topic_metrics is not None:
            latency_ms = max(0.0, (time.time() - ctx.timestamp) * 1000.0)
            self._topic_metrics.record(topic, latency_ms)

        # 轻量级订阅者检查（#2 优化）
        if self.router.has_subscribers(topic):
            topic_bytes = self._get_topic_bytes(topic)
            rc_bytes = _RECORD_COUNT_STRUCT.pack(record_count)
            broadcast_meta = self._build_broadcast_meta(ctx.meta)
            broadcast_frames = [topic_bytes, broadcast_meta, rc_bytes, ctx.payload]
            result = self._broadcast(broadcast_frames)
            if result is not None:
                await result

        # 条件化缓存（#4 优化）
        if self.router.buffer_enabled:
            self.router.append_message(topic, ctx.meta, record_count, ctx.payload)

    async def _handle_batch(self, ctx: PipelineContext) -> None:
        """处理 BATCH PUB：拆 N 条, 每条走完整 PUB 路径。

        协议: ctx.payload 是 msgpack 编码的 list[(ser_fmt, payload_bytes)]，
        外层已按 ctx meta 的 comp 压缩。这里先解压+msgpack 解码得到原始 payload list,
        然后对每条用其原始 ser_fmt 构造 broadcast (避免 client 端 ser_fmt 信息丢失).

        鉴权已在拦截器链入口处对 BATCH 整体完成, 这里不再走 pipeline。
        """
        topic = ctx.topic

        # 从 meta[1] flags 字节解码 comp（PipelineContext 没有 comp 字段）
        flags_byte = ctx.meta[1] if len(ctx.meta) > 1 else 0
        flags = FrameFlags.decode(flags_byte)
        comp = flags.comp

        # 1. 拆解 BATCH payload → list[(ser_fmt, payload_bytes)]
        items = FrameCodec.decode_batch_payload(ctx.payload, comp)
        n = len(items)
        if n == 0:
            return

        # 2. 注册 topic（幂等）
        self.router.register_topic(topic)

        # 3. 对每条 payload 执行 PUB 路径
        has_subs = self.router.has_subscribers(topic)
        buffered = self.router.buffer_enabled
        topic_bytes = self._get_topic_bytes(topic) if (has_subs or buffered) else None
        rc_bytes = _RECORD_COUNT_STRUCT.pack(1)

        for ser_fmt, raw_payload in items:
            # 构造该条 payload 的 PUB meta: msg_type=PUB, flags 来自 ser_fmt
            # 沿用 BATCH 帧的 comp (BATCH 内 payload 压缩方式与外层一致)
            item_flags = FrameFlags(ser_fmt=ser_fmt, comp=comp, has_topic=True)
            pub_meta = bytes([MsgType.PUB, item_flags.encode()])
            broadcast_meta = self._build_broadcast_meta(pub_meta) if has_subs else None

            # 广播
            if has_subs:
                broadcast_frames = [topic_bytes, broadcast_meta, rc_bytes, raw_payload]
                result = self._broadcast(broadcast_frames)
                if result is not None:
                    await result

            # 缓存
            if buffered:
                self.router.append_message(topic, pub_meta, 1, raw_payload)

    async def _handle_sub(self, ctx: PipelineContext) -> None:
        """处理 SUB：建立订阅（支持通配符）→ 发送确认。"""
        identity = ctx.identity
        topic = ctx.topic

        # 判断是否为通配符
        info = TopicInfo.from_name(topic)
        expanded: list[str] = []

        if info.is_wildcard:
            # 通配符订阅：展开匹配
            expanded = self.router.subscribe_wildcard(identity, topic)
            # 也注册通配符本身
            self.router.register_topic(topic)
        else:
            # 精确订阅
            self.router.register_topic(topic)
            self.router.subscribe(identity, topic)
            expanded = [topic]

        # Phase 4: 客户端追踪 - 记录订阅
        if self._client_tracker is not None:
            self._client_tracker.on_sub(identity, topic)

        # 发送 SUB 确认
        reply_payload = FrameCodec.encode_payload(
            {"status": "ok", "expanded_topics": expanded},
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

    async def _handle_unsub(self, ctx: PipelineContext) -> None:
        """处理 UNSUB：取消订阅 → 发送确认。"""
        identity = ctx.identity
        topic = ctx.topic

        self.router.unsubscribe(identity, topic)

        # Phase 4: 客户端追踪 - 删除订阅
        if self._client_tracker is not None:
            self._client_tracker.on_unsub(identity, topic)

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

    async def _handle_ping(self, ctx: PipelineContext) -> None:
        """处理 PING：回复 PONG。"""
        identity = ctx.identity

        # Phase 4: 客户端追踪 - 更新心跳
        if self._client_tracker is not None:
            self._client_tracker.on_heartbeat(identity)

        try:
            client_data = FrameCodec.decode_payload(
                ctx.payload, self._default_ser, self._default_comp
            )
            client_ts = client_data.get("client_ts", 0)
        except Exception:
            client_ts = 0

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

    async def _handle_query(self, ctx: PipelineContext) -> None:
        """处理 QUERY 消息（V1 仅 system_status）。"""
        identity = ctx.identity
        try:
            query = FrameCodec.decode_payload(
                ctx.payload, self._default_ser, self._default_comp
            )
        except Exception:
            await self._send_error(identity, 4007, "payload 反序列化失败")
            return

        action = query.get("action", "")
        if action == "system_status":
            await self._query_system_status(identity)
        else:
            await self._send_error(identity, 3004, f"未知 action: {action}")

    async def _query_system_status(self, identity: bytes) -> None:
        """返回系统状态。"""
        status = {
            "status": "ok",
            "timestamp": time.time(),
            "topic_count": self.router.topic_count(),
            "subscription_count": self.router.subscription_count(),
            "connection_count": self.router.connection_count(),
        }
        payload = FrameCodec.encode_payload(
            status, self._default_ser, self._default_comp
        )
        frames = FrameCodec.encode(
            MsgType.QUERY, "", 0, payload,
            self._default_ser, self._default_comp,
        )
        result = self._send(identity, frames)
        if result is not None:
            await result

    async def _handle_history_replay(self, ctx: PipelineContext) -> None:
        """处理 HISTORY_REPLAY：从 MessageBuffer 回放历史消息。"""
        identity = ctx.identity
        topic = ctx.topic

        try:
            req = FrameCodec.decode_payload(
                ctx.payload, self._default_ser, self._default_comp
            )
        except Exception:
            await self._send_error(identity, 4007, "payload 反序列化失败", topic)
            return

        from_seq = req.get("from_seq", 0)
        limit = min(req.get("limit", 100), 500)

        # 回放消息
        messages = self.router.replay_messages(topic, from_seq, limit)

        # 逐条发送 BROADCAST
        for msg in messages:
            replay_payload = FrameCodec.encode_payload(
                {
                    "_seq": msg.seq,
                    "_ts": msg.timestamp,
                    "_replayed": True,
                },
                self._default_ser,
                self._default_comp,
            )
            broadcast_frames = FrameCodec.encode(
                MsgType.BROADCAST, topic, msg.record_count, msg.payload,
                self._default_ser, self._default_comp,
            )
            result = self._send(identity, broadcast_frames)
            if result is not None:
                await result

        # 发送回放结束标记
        latest = self.router.latest_seq(topic)
        done_payload = FrameCodec.encode_payload(
            {
                "status": "ok",
                "from_seq": from_seq,
                "to_seq": messages[-1].seq if messages else from_seq,
                "count": len(messages),
                "latest_seq": latest,
            },
            self._default_ser,
            self._default_comp,
        )
        done_frames = FrameCodec.encode(
            MsgType.HISTORY_REPLAY, topic, 0, done_payload,
            self._default_ser, self._default_comp,
        )
        result = self._send(identity, done_frames)
        if result is not None:
            await result

    async def _send_error(
        self, identity: bytes, code: int, message: str, topic: str = ""
    ) -> None:
        """发送 ERROR 消息。"""
        error_payload = FrameCodec.encode_payload(
            {"code": code, "message": message},
            self._default_ser,
            self._default_comp,
        )
        error_frames = FrameCodec.encode(
            MsgType.ERROR, topic, 0, error_payload,
            self._default_ser, self._default_comp,
        )
        result = self._send(identity, error_frames)
        if result is not None:
            await result
