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
    ):
        self.router = router
        self._send = send_fn
        self._broadcast = broadcast_fn
        self._pipeline = pipeline
        self._default_ser = default_ser
        self._default_comp = default_comp
        self._ctx_pool = PipelineContextPool(size=4096)

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
        decoded = FrameCodec.decode_server(server_frames)
        msg_type = decoded.msg_type

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
            topic = frames[2].decode("utf-8")
            payload = frames[5]
            record_count = _RECORD_COUNT_STRUCT.unpack(frames[4])[0]
        else:  # 5 帧
            topic = frames[1].decode("utf-8")
            payload = frames[4]
            record_count = _RECORD_COUNT_STRUCT.unpack(frames[3])[0]

        # 注册 topic（幂等）
        self.router.register_topic(topic)

        # 轻量级订阅者检查（#2 优化：不拷贝 set）
        if self.router.has_subscribers(topic):
            topic_bytes = self._get_topic_bytes(topic)
            rc_bytes = _RECORD_COUNT_STRUCT.pack(record_count)
            broadcast_frames = [topic_bytes, self._broadcast_meta, rc_bytes, payload]
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
        # 其他类型暂忽略

    async def _handle_pub(self, ctx: PipelineContext) -> None:
        """处理 PUB：注册 topic → 广播 → 缓存。"""
        topic = ctx.topic
        record_count = ctx.record_count

        # 注册 topic（幂等）
        self.router.register_topic(topic)

        # 轻量级订阅者检查（#2 优化）
        if self.router.has_subscribers(topic):
            topic_bytes = self._get_topic_bytes(topic)
            rc_bytes = _RECORD_COUNT_STRUCT.pack(record_count)
            broadcast_frames = [topic_bytes, self._broadcast_meta, rc_bytes, ctx.payload]
            result = self._broadcast(broadcast_frames)
            if result is not None:
                await result

        # 条件化缓存（#4 优化）
        if self.router.buffer_enabled:
            self.router.append_message(topic, ctx.meta, record_count, ctx.payload)

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
