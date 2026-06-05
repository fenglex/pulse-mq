"""消息类型分发和处理器。

集成拦截器链：Auth → Permission → Monitor → Handler
"""

from __future__ import annotations

import logging
import time
from collections.abc import Awaitable, Callable

from pulsemq.engine.pipeline import (
    AuthError,
    InterceptorChain,
    MonitorInterceptor,
    PipelineContext,
    PermissionError,
)
from pulsemq.engine.router import MessageRouter
from pulsemq.models import TopicInfo
from pulsemq.protocol.frames import FrameCodec
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

    async def dispatch(self, server_frames: list[bytes]) -> None:
        """根据 msg_type 分发到对应处理器（含拦截器链）。"""
        decoded = FrameCodec.decode_server(server_frames)
        msg_type = decoded.msg_type

        # 构建上下文
        ctx = PipelineContext(
            identity=decoded.identity,
            msg_type=msg_type,
            topic=decoded.topic,
            meta=bytes([msg_type, decoded.flags.encode()]),
            payload=decoded.payload,
            record_count=decoded.record_count,
        )

        try:
            # 执行拦截器链
            if self._pipeline is not None:
                async def _handle():
                    await self._dispatch_internal(ctx, server_frames)
                await self._pipeline.execute(ctx)
                await _handle()
            else:
                await self._dispatch_internal(ctx, server_frames)

        except AuthError as e:
            await self._send_error(decoded.identity, 1001, str(e))
        except PermissionError as e:
            await self._send_error(decoded.identity, 2001, str(e), decoded.topic)
        except Exception as e:
            logger.exception("消息处理异常")
            await self._send_error(decoded.identity, 9001, "内部错误")

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
        # 其他类型暂忽略

    async def _handle_pub(self, ctx: PipelineContext) -> None:
        """处理 PUB：注册 topic → 广播 → 缓存。"""
        topic = ctx.topic
        record_count = ctx.record_count

        # 注册 topic（幂等）
        self.router.register_topic(topic)

        # 获取订阅者（含通配符匹配）
        subscribers = self.router.get_subscribers(topic)

        # 广播
        if subscribers:
            from pulsemq.protocol.flags import FrameFlags
            flags = FrameFlags(
                ser_fmt=self._default_ser,
                comp=self._default_comp,
                has_topic=bool(topic),
            )
            broadcast_meta = bytes([MsgType.BROADCAST, flags.encode()])
            broadcast_frames = [
                topic.encode("utf-8"),
                broadcast_meta,
                ctx.meta,           # 复用 meta 中的 record_count 不对，用编码后的
                ctx.payload,
            ]
            # 重新编码 record_count
            import struct
            broadcast_frames[2] = struct.pack(">I", record_count)
            result = self._broadcast(broadcast_frames)
            if result is not None:
                await result

        # 缓存消息
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
