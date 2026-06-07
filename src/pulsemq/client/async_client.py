"""PulseClient 异步客户端。

支持：connect/auth/publish/subscribe/unsubscribe/query/ping
      context manager、自动重连、带重试的 publish
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Any, AsyncIterator

import msgpack
import pandas as pd
import zmq
import zmq.asyncio

from pulsemq.protocol.flags import FrameFlags
from pulsemq.protocol.frames import FrameCodec
from pulsemq.protocol.msg_type import MsgType
from pulsemq.auth.permission import topic_match

logger = logging.getLogger(__name__)


# ---- 错误类型 ----


class PulseError(Exception):
    pass


class PulseConnectionError(PulseError):
    pass


class PulseAuthError(PulseError):
    pass


class PulsePermissionError(PulseError):
    pass


class PulseTimeoutError(PulseError):
    pass


class PulseServerError(PulseError):
    def __init__(self, code: int, message: str):
        self.code = code
        self.message = message
        super().__init__(f"[{code}] {message}")


# ---- 消息对象 ----


@dataclass
class PulseMessage:
    """客户端收到的消息。"""

    topic: str
    msg_type: int
    payload: Any
    raw_payload: bytes
    meta_flags: int
    timestamp: float


# ---- 客户端实现 ----


class PulseClient:
    """PulseMQ 异步客户端。

    使用方式:
        async with PulseClient("tcp://localhost:5555", api_key="pulse_sk_xxx") as client:
            await client.publish("topic", "hello")
            await client.publish("topic", df, format="pyarrow")
            async for msg in client.subscribe("topic"):
                print(msg)
    """

    # 大小限制（压缩前）
    _MAX_STR_SIZE = 16 * 1024 * 1024     # 16MB
    _MAX_BYTES_SIZE = 128 * 1024 * 1024  # 128MB

    # 内部默认序列化/压缩（subscribe/query/ping 使用）
    _DEFAULT_SER = "msgpack"
    _DEFAULT_COMP = "none"

    def __init__(
        self,
        address: str,
        api_key: str | None = None,
        xpub_address: str | None = None,
        auto_reconnect: bool = True,
        reconnect_initial_delay: float = 1.0,
        reconnect_max_delay: float = 30.0,
        reconnect_backoff: float = 2.0,
        heartbeat_interval: float = 10.0,
        recv_timeout: float = 5.0,
        connect_timeout: float = 5.0,
        identity: bytes | None = None,
    ):
        self._address = address
        self._xpub_address = xpub_address or address.replace("5555", "5556")
        self._api_key = api_key
        self._auto_reconnect = auto_reconnect
        self._reconnect_initial_delay = reconnect_initial_delay
        self._reconnect_max_delay = reconnect_max_delay
        self._reconnect_backoff = reconnect_backoff
        self._heartbeat_interval = heartbeat_interval
        self._recv_timeout = recv_timeout
        self._connect_timeout = connect_timeout
        self._identity = identity or f"client_{id(self)}".encode()

        self._ctx: zmq.asyncio.Context | None = None
        self._dealer: zmq.asyncio.Socket | None = None
        self._sub: zmq.asyncio.Socket | None = None
        self._connected = False
        self._reconnect_count = 0
        self._user_info: dict | None = None

    async def __aenter__(self) -> PulseClient:
        await self.connect()
        return self

    async def __aexit__(self, *args) -> None:
        await self.disconnect()

    # ---- 连接管理 ----

    async def connect(self) -> None:
        """连接到服务端。"""
        self._ctx = zmq.asyncio.Context()

        # DEALER socket 用于发送消息
        self._dealer = self._ctx.socket(zmq.DEALER)
        self._dealer.setsockopt(zmq.IDENTITY, self._identity)
        self._dealer.setsockopt(zmq.HEARTBEAT_IVL, 2000)
        self._dealer.setsockopt(zmq.HEARTBEAT_TIMEOUT, 5000)
        self._dealer.connect(self._address)

        # SUB socket 用于接收广播
        self._sub = self._ctx.socket(zmq.SUB)
        self._sub.connect(self._xpub_address)

        self._connected = True
        self._reconnect_count = 0
        logger.info("已连接到 %s", self._address)

    async def disconnect(self) -> None:
        """断开连接。"""
        self._connected = False
        if self._sub:
            self._sub.close(linger=0)
            self._sub = None
        if self._dealer:
            self._dealer.close(linger=0)
            self._dealer = None
        if self._ctx:
            self._ctx.term()
            self._ctx = None
        logger.info("已断开连接")

    async def _reconnect(self) -> None:
        """自动重连（指数退避）。"""
        if not self._auto_reconnect:
            raise PulseConnectionError("连接断开")

        delay = min(
            self._reconnect_initial_delay * (self._reconnect_backoff ** self._reconnect_count),
            self._reconnect_max_delay,
        )
        self._reconnect_count += 1
        logger.info("重连中... (%d 次，等待 %.1fs)", self._reconnect_count, delay)
        await asyncio.sleep(delay)

        await self.disconnect()
        await self.connect()

    # ---- 发布 ----

    async def publish(
        self,
        topic: str,
        data: str | bytes | pd.DataFrame,
        format: str | None = None,
        compression: str = "none",
        retry: int = 0,
        retry_delay: float = 0.1,
    ) -> None:
        """发布消息。

        三种数据类型，自动推断序列化方式：
          - str       → StringSerializer (UTF-8)，限制 ≤ 16MB
          - bytes     → BytesSerializer (透传)，限制 ≤ 128MB
          - DataFrame → msgpack 或 pyarrow（通过 format 指定，默认 msgpack）

        Args:
            topic: topic 路径（必填）
            data: 消息数据，支持 str / bytes / pandas.DataFrame
            format: 仅 DataFrame 时生效，"msgpack"（默认）/"pyarrow"；
                    传 str 或 bytes 时必须为 None
            compression: 压缩算法，none/snappy/lz4/zstd
            retry: 重试次数，默认 0
            retry_delay: 重试间隔（秒），默认 0.1
        """
        ser_fmt = self._resolve_format(data, format)
        self._validate_data(data, ser_fmt)
        record_count = self._infer_record_count(data)
        # DataFrame 走 msgpack 时先转成 list[dict]（msgpack 无法直接序列化 DataFrame）
        payload_obj = (
            data.to_dict(orient="records")
            if ser_fmt == "msgpack" and isinstance(data, pd.DataFrame)
            else data
        )
        payload = FrameCodec.encode_payload(payload_obj, ser_fmt, compression)
        frames = FrameCodec.encode(
            MsgType.PUB, topic, record_count, payload, ser_fmt, compression
        )
        await self._send_with_retry(frames, retry, retry_delay)

    # ---- 订阅 ----

    @staticmethod
    def _is_wildcard(topic: str) -> bool:
        """判断 topic 是否含通配符（* 或 >）。"""
        return "*" in topic or ">" in topic

    async def subscribe(self, *topics: str) -> AsyncIterator[PulseMessage]:
        """订阅一个或多个 topic，返回异步迭代器。

        用法:
            async for msg in client.subscribe("team-a.mkt.*"):
                print(msg.topic, msg.payload)

            # 多 topic 订阅
            async for msg in client.subscribe("topic-a", "topic-b", "team-a.>"):
                print(msg.topic, msg.payload)

        通配符实现: ZMQ SUB 是前缀匹配，不支持 *, >。
        对含通配符的 pattern, 在 SUB socket 上订阅空 (匹配所有),
        在客户端用 topic_match() 过滤; 同时把 pattern 发给 server
        让 router.has_subscribers() 知道有订阅。
        """
        if self._sub is None:
            raise PulseConnectionError("未连接")

        # 区分精确与通配符
        exact_topics = [t for t in topics if not self._is_wildcard(t)]
        wildcard_topics = [t for t in topics if self._is_wildcard(t)]

        # 精确 topic: 走 ZMQ 前缀过滤
        for topic in exact_topics:
            self._sub.setsockopt(zmq.SUBSCRIBE, topic.encode("utf-8"))

        # 通配符 topic: 在 ZMQ 层订阅空 (匹配所有), 客户端过滤
        for topic in wildcard_topics:
            self._sub.setsockopt(zmq.SUBSCRIBE, b"")

        # 通知 server 注册订阅 (精确 + 通配符都要发, server 端会识别)
        for topic in topics:
            sub_frames = FrameCodec.encode(MsgType.SUB, topic, 0, b"")
            await self._dealer.send_multipart(sub_frames)

        # 等待所有 SUB 确认
        for _ in topics:
            try:
                await asyncio.wait_for(
                    self._dealer.recv_multipart(), timeout=self._recv_timeout
                )
            except asyncio.TimeoutError:
                pass  # 超时不阻塞

        # 消息循环
        while self._connected:
            try:
                msg_frames = await asyncio.wait_for(
                    self._sub.recv_multipart(), timeout=self._heartbeat_interval
                )
                if len(msg_frames) >= 4:
                    msg = self._decode_message(msg_frames)
                    if msg and (not wildcard_topics or
                                any(topic_match(p, msg.topic) for p in wildcard_topics)):
                        yield msg
            except asyncio.TimeoutError:
                # 发送心跳
                await self.ping()
            except zmq.ZMQError:
                if self._auto_reconnect:
                    await self._reconnect()
                    for topic in exact_topics:
                        self._sub.setsockopt(zmq.SUBSCRIBE, topic.encode("utf-8"))
                    for topic in wildcard_topics:
                        self._sub.setsockopt(zmq.SUBSCRIBE, b"")
                else:
                    break

    async def unsubscribe(self, topic: str) -> None:
        """取消订阅。"""
        frames = FrameCodec.encode(MsgType.UNSUB, topic, 0, b"")
        await self._dealer.send_multipart(frames)
        # ZMQ 层: 通配符订阅时取消的是空过滤
        if self._is_wildcard(topic):
            self._sub.setsockopt(zmq.UNSUBSCRIBE, b"")
        else:
            self._sub.setsockopt(zmq.UNSUBSCRIBE, topic.encode("utf-8"))

    # ---- 查询 ----

    async def query(self, params: dict) -> dict:
        """发送管理查询。"""
        payload = FrameCodec.encode_payload(params, self._DEFAULT_SER, self._DEFAULT_COMP)
        frames = FrameCodec.encode(
            MsgType.QUERY, "", 0, payload, self._DEFAULT_SER, self._DEFAULT_COMP
        )
        await self._dealer.send_multipart(frames)

        reply = await asyncio.wait_for(
            self._dealer.recv_multipart(), timeout=self._recv_timeout
        )
        # ROUTER 发送 [identity, b"", ...client_frames] (6 帧);
        # DEALER 剥掉 identity, 剩 5 帧: [b"", topic, meta, rc, payload]
        if len(reply) >= 5:
            return FrameCodec.decode_payload(reply[4], self._DEFAULT_SER, self._DEFAULT_COMP)
        return {}

    # ---- 心跳 ----

    async def ping(self) -> dict:
        """发送 PING，返回延迟信息。"""
        ts = time.time()
        payload = FrameCodec.encode_payload(
            {"client_ts": ts}, self._DEFAULT_SER, self._DEFAULT_COMP
        )
        frames = FrameCodec.encode(
            MsgType.PING, "", 0, payload, self._DEFAULT_SER, self._DEFAULT_COMP
        )
        await self._dealer.send_multipart(frames)

        reply = await asyncio.wait_for(
            self._dealer.recv_multipart(), timeout=self._recv_timeout
        )
        # ROUTER 发送 [identity, b"", ...client_frames] (6 帧);
        # DEALER 剥掉 identity, 剩 5 帧: [b"", topic, meta, rc, payload]
        if len(reply) >= 5:
            return FrameCodec.decode_payload(reply[4], self._DEFAULT_SER, self._DEFAULT_COMP)
        return {}

    # ---- 内部方法 ----

    @staticmethod
    def _resolve_format(data: Any, format: str | None) -> str:
        """根据 data 类型自动推断序列化格式。

        str → "str"，bytes → "bytes"，DataFrame → format 参数（默认 msgpack）。
        """
        if isinstance(data, str):
            if format is not None:
                raise ValueError("data 为 str 时 format 必须为 None")
            return "str"
        if isinstance(data, bytes):
            if format is not None:
                raise ValueError("data 为 bytes 时 format 必须为 None")
            return "bytes"
        if isinstance(data, pd.DataFrame):
            _fmt = format or "msgpack"
            if _fmt not in ("msgpack", "pyarrow"):
                raise ValueError(
                    f"DataFrame 的 format 只支持 msgpack/pyarrow，收到 '{_fmt}'"
                )
            return _fmt
        raise TypeError(
            f"data 类型不支持: {type(data).__name__}，仅支持 str/bytes/DataFrame"
        )

    @staticmethod
    def _infer_record_count(data: Any) -> int:
        """根据 data 类型推断 record_count。

        DataFrame 按实际行数，其余都算 1 条。
        """
        if isinstance(data, pd.DataFrame):
            return len(data)
        return 1

    @classmethod
    def _validate_data(cls, data: Any, ser_fmt: str) -> None:
        """校验数据大小限制（压缩前）。"""
        if ser_fmt == "str":
            if isinstance(data, str) and len(data.encode("utf-8")) > cls._MAX_STR_SIZE:
                raise ValueError(
                    f"str 消息超过大小限制: {len(data.encode('utf-8')):,} > {cls._MAX_STR_SIZE:,} bytes"
                )
        elif ser_fmt == "bytes":
            if isinstance(data, bytes) and len(data) > cls._MAX_BYTES_SIZE:
                raise ValueError(
                    f"bytes 消息超过大小限制: {len(data):,} > {cls._MAX_BYTES_SIZE:,} bytes"
                )

    async def _send_with_retry(
        self, frames: list[bytes], retry: int, retry_delay: float
    ) -> None:
        """带重试的发送。"""
        for attempt in range(retry + 1):
            try:
                await self._dealer.send_multipart(frames)
                return
            except zmq.ZMQError:
                if attempt < retry:
                    delay = retry_delay * (2 ** attempt)
                    await asyncio.sleep(delay)
                    if self._auto_reconnect:
                        await self._reconnect()
                else:
                    raise PulseConnectionError("发送失败，重试耗尽")

    def _decode_message(self, frames: list[bytes]) -> PulseMessage | None:
        """解码 SUB 收到的广播消息。

        从 frame[1] (meta 字节) 解析出 ser_fmt / comp，再调 FrameCodec 解码。
        """
        try:
            topic = frames[0].decode("utf-8")
            meta = frames[1]
            msg_type = meta[0]
            flags = (
                FrameFlags.decode(meta[1])
                if len(meta) > 1
                else FrameFlags(ser_fmt="msgpack", comp="none", has_topic=False)
            )
            payload_bytes = frames[3] if len(frames) > 3 else b""

            # 用 wire 上的真实 ser_fmt / comp 解码（不再硬编码 msgpack/none）
            try:
                payload = FrameCodec.decode_payload(
                    payload_bytes, flags.ser_fmt, flags.comp
                )
            except Exception:
                payload = None

            return PulseMessage(
                topic=topic,
                msg_type=msg_type,
                payload=payload,
                raw_payload=payload_bytes,
                meta_flags=meta[1] if len(meta) > 1 else 0,
                timestamp=time.time(),
            )
        except Exception as e:
            logger.debug("消息解码失败: %s", e)
            return None
