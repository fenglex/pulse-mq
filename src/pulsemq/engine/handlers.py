"""消息类型分发和处理器。

Phase 1 处理: PUB, SUB, UNSUB, PING, PONG
不包含: AUTH（Phase 2 ZAP）, QUERY, STATUS, HISTORY_REPLAY
"""

from __future__ import annotations

import time
from collections.abc import Callable, Awaitable

from pulsemq.engine.router import MessageRouter
from pulsemq.protocol.frames import FrameCodec
from pulsemq.protocol.msg_type import MsgType


class MessageHandlers:
    """消息处理器集合。"""

    def __init__(
        self,
        router: MessageRouter,
        send_fn: Callable[[bytes, list[bytes]], Awaitable[None] | None],
        broadcast_fn: Callable[[list[bytes]], Awaitable[None] | None],
        default_ser: str = "msgpack",
        default_comp: str = "none",
    ):
        self.router = router
        self._send = send_fn
        self._broadcast = broadcast_fn
        self._default_ser = default_ser
        self._default_comp = default_comp

    async def dispatch(self, server_frames: list[bytes]) -> None:
        """根据 msg_type 分发到对应处理器。"""
        decoded = FrameCodec.decode_server(server_frames)
        msg_type = decoded.msg_type

        if msg_type == MsgType.PUB:
            await self.handle_pub(server_frames)
        elif msg_type == MsgType.SUB:
            await self.handle_sub(server_frames)
        elif msg_type == MsgType.UNSUB:
            await self.handle_unsub(server_frames)
        elif msg_type == MsgType.PING:
            await self.handle_ping(server_frames)
        else:
            pass  # Phase 1 忽略不认识的消息类型

    async def handle_pub(self, server_frames: list[bytes]) -> None:
        """处理 PUB 消息：注册 topic → 广播给订阅者 → 缓存。"""
        decoded = FrameCodec.decode_server(server_frames)
        topic = decoded.topic
        record_count = decoded.record_count

        # 注册 topic（幂等）
        self.router.register_topic(topic)

        # 获取订阅者
        subscribers = self.router.get_subscribers(topic)

        # 零拷贝广播：替换 msg_type 为 BROADCAST
        if subscribers:
            from pulsemq.protocol.flags import FrameFlags
            broadcast_meta = bytes([MsgType.BROADCAST, decoded.flags.encode()])
            broadcast_frames = [
                server_frames[2],               # topic
                broadcast_meta,                 # meta (BROADCAST + original flags)
                server_frames[4],               # record_count
                server_frames[5],               # payload
            ]
            result = self._broadcast(broadcast_frames)
            if result is not None:
                await result

        # 缓存消息
        self.router.append_message(
            topic, server_frames[3], record_count, server_frames[5]
        )

    async def handle_sub(self, server_frames: list[bytes]) -> None:
        """处理 SUB 消息：建立订阅 → 发送确认。"""
        decoded = FrameCodec.decode_server(server_frames)
        identity = decoded.identity
        topic = decoded.topic

        # 自动注册 topic
        self.router.register_topic(topic)

        # 建立订阅
        self.router.subscribe(identity, topic)

        # 发送 SUB 确认
        reply_payload = FrameCodec.encode_payload(
            {"status": "ok", "expanded_topics": [topic]},
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

    async def handle_unsub(self, server_frames: list[bytes]) -> None:
        """处理 UNSUB 消息：取消订阅 → 发送确认。"""
        decoded = FrameCodec.decode_server(server_frames)
        identity = decoded.identity
        topic = decoded.topic

        # 取消订阅
        self.router.unsubscribe(identity, topic)

        # 发送 UNSUB 确认
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

    async def handle_ping(self, server_frames: list[bytes]) -> None:
        """处理 PING：回复 PONG。"""
        decoded = FrameCodec.decode_server(server_frames)
        identity = decoded.identity

        # 解析客户端时间戳
        try:
            client_data = FrameCodec.decode_payload(
                decoded.payload, decoded.ser_fmt, decoded.comp
            )
            client_ts = client_data.get("client_ts", 0)
        except Exception:
            client_ts = 0

        # 回复 PONG
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
