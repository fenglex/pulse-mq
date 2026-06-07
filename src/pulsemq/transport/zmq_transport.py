"""ZMQ ROUTER + XPUB 传输适配器 — IO 线程化。

架构 (v1):
- ROUTER socket: ZmqRecvThread 持有 (Python threading), 阻塞 recv
- XPUB socket:   ZmqBroadcastThread 持有, 阻塞 send
- asyncio 主循环通过 queue.Queue + asyncio.to_thread 与两个线程通信

外部 API 与原 asyncio 版本保持一致:
- async start() / stop(linger_ms)
- async recv() -> list[bytes]
- async send(identity, frames) -> None
- async broadcast(frames) -> None
- 属性 _router / _xpub 仍可访问 (供 server.py 设置 ROUTER_NOTIFY 等)
"""

from __future__ import annotations

import asyncio
import logging
import queue
import threading
from typing import Any

import zmq

from pulsemq.config import ServerConfig

logger = logging.getLogger(__name__)


class _ZmqRecvThread(threading.Thread):
    """持有 ROUTER socket 的独立 IO 线程。

    - 死循环阻塞 recv_multipart(), 收到帧就 put 到 recv_queue
    - 同时处理 send_queue (server.py 主动 send, 如 AUTH/ERROR 响应)
    - stop_event 触发后干净退出
    """

    POLL_TIMEOUT_MS = 50  # 短轮询, 让 send_queue 也能及时消费

    def __init__(
        self,
        socket: zmq.Socket,
        recv_queue: queue.Queue,
        send_queue: queue.Queue,
        stop_event: threading.Event,
    ):
        super().__init__(daemon=True, name="ZmqRecvThread")
        self._socket = socket
        self._recv_queue = recv_queue
        self._send_queue = send_queue
        self._stop_event = stop_event
        self._poller = zmq.Poller()
        self._poller.register(socket, zmq.POLLIN)

    def run(self) -> None:
        """IO 线程主循环: 同时处理 recv 和 outbound send。"""
        while not self._stop_event.is_set():
            try:
                socks = dict(self._poller.poll(self.POLL_TIMEOUT_MS))
            except zmq.ZMQError as e:
                logger.debug("recv poller 异常: %s", e)
                break

            if self._socket in socks and socks[self._socket] & zmq.POLLIN:
                # 有数据帧到达
                try:
                    frames = self._socket.recv_multipart()
                except zmq.ZMQError as e:
                    logger.debug("recv 异常: %s", e)
                    break
                self._recv_queue.put(frames)

            # 处理 outbound send (server 主动推送, 不需要 await)
            if self._socket in socks and socks[self._socket] & zmq.POLLOUT:
                # SNDHWM 满时 socket 不可写, 这里不专门处理 (SNDHWM=0 = 无限)
                pass

            # 排空 send_queue (server 推 AUTH/ERROR)
            while not self._send_queue.empty():
                try:
                    identity, out_frames = self._send_queue.get_nowait()
                except queue.Empty:
                    break
                try:
                    self._socket.send_multipart([identity, b""] + out_frames, zmq.DONTWAIT)
                except zmq.Again:
                    # 缓冲区满, 放回队首
                    self._send_queue.put((identity, out_frames))
                    break
                except zmq.ZMQError as e:
                    logger.debug("send 异常: %s", e)
                    break

        # 退出时放哨兵
        self._recv_queue.put(None)


class _ZmqBroadcastThread(threading.Thread):
    """持有 XPUB socket 的独立 IO 线程。

    - 死循环从 broadcast_queue 取帧, send_multipart 到 XPUB
    - 短轮询 + poll XPUB 接收 SUBSCRIBE 确认
    - stop_event 触发后干净退出
    """

    POLL_TIMEOUT_S = 0.05

    def __init__(
        self,
        socket: zmq.Socket,
        broadcast_queue: queue.Queue,
        stop_event: threading.Event,
    ):
        super().__init__(daemon=True, name="ZmqBroadcastThread")
        self._socket = socket
        self._broadcast_queue = broadcast_queue
        self._stop_event = stop_event
        self._poller = zmq.Poller()
        self._poller.register(socket, zmq.POLLIN)

    def run(self) -> None:
        """IO 线程主循环: 消费 broadcast_queue, 顺便 poll XPUB ack。"""
        while not self._stop_event.is_set():
            # 排空 XPUB 的 SUBSCRIBE/UNSUBSCRIBE 确认 (避免接收缓冲区满)
            try:
                while self._socket in dict(self._poller.poll(0)) and \
                        dict(self._poller.poll(0))[self._socket] & zmq.POLLIN:
                    self._socket.recv_multipart()
            except zmq.ZMQError:
                pass

            # 阻塞取 1 条 (短超时, 让 stop_event 能被检测)
            try:
                frames = self._broadcast_queue.get(timeout=self.POLL_TIMEOUT_S)
            except queue.Empty:
                continue
            if frames is None:
                # 哨兵, 退出
                break

            # 发送
            try:
                self._socket.send_multipart(frames, zmq.DONTWAIT)
            except zmq.Again:
                # 缓冲区满 (SNDHWM != 0), 退回重试一次
                try:
                    self._socket.send_multipart(frames)
                except zmq.ZMQError as e:
                    logger.debug("broadcast 异常: %s", e)
            except zmq.ZMQError as e:
                logger.debug("broadcast 异常: %s", e)


class ZmqTransport:
    """ZMQ 传输层, 持有 ROUTER + XPUB 两个 socket, 通过两个 IO 线程驱动。

    asyncio 主循环调用:
    - await recv()       → to_thread(recv_queue.get)
    - await broadcast()  → broadcast_queue.put (无 await)
    - await send()       → send_queue.put (无 await)
    """

    def __init__(self, config: ServerConfig):
        self._config = config
        self._ctx: zmq.Context | None = None
        self._router: zmq.Socket | None = None
        self._xpub: zmq.Socket | None = None
        # 线程安全队列 (thread ↔ asyncio 通信)
        self._recv_queue: queue.Queue[list[bytes] | None] = queue.Queue()
        self._broadcast_queue: queue.Queue[list[bytes] | None] = queue.Queue()
        self._send_queue: queue.Queue[tuple[bytes, list[bytes]]] = queue.Queue()
        # 线程
        self._recv_thread: _ZmqRecvThread | None = None
        self._broadcast_thread: _ZmqBroadcastThread | None = None
        self._stop_event = threading.Event()

    async def start(self) -> None:
        """初始化 ZMQ socket + bind + 启动两个 IO 线程。"""
        self._ctx = zmq.Context()

        # ROUTER socket
        self._router = self._ctx.socket(zmq.ROUTER)
        self._router.setsockopt(zmq.RCVHWM, self._config.zmq_rcvhwm)
        self._router.setsockopt(zmq.SNDHWM, self._config.zmq_sndhwm)
        self._router.setsockopt(zmq.IMMEDIATE, 1)
        self._router.setsockopt(zmq.HEARTBEAT_IVL, self._config.zmq_heartbeat_ivl)
        self._router.setsockopt(zmq.HEARTBEAT_TIMEOUT, self._config.zmq_heartbeat_timeout)
        self._router.setsockopt(zmq.HEARTBEAT_TTL, self._config.zmq_heartbeat_ttl)
        self._router.setsockopt(zmq.ROUTER_MANDATORY, 0)
        self._router.bind(self._config.bind)
        logger.info("ROUTER 绑定到 %s", self._config.bind)

        # XPUB socket
        self._xpub = self._ctx.socket(zmq.XPUB)
        self._xpub.setsockopt(zmq.SNDHWM, self._config.zmq_sndhwm)
        self._xpub.setsockopt(zmq.IMMEDIATE, 1)
        if self._config.zmq_sndhwm != 0:
            self._xpub.setsockopt(zmq.XPUB_NODROP, 1)
        self._xpub.bind(self._config.xpub_bind)
        logger.info("XPUB 绑定到 %s", self._config.xpub_bind)

        # 重置队列 (start 可能被调多次)
        self._recv_queue = queue.Queue()
        self._broadcast_queue = queue.Queue()
        self._send_queue = queue.Queue()
        self._stop_event.clear()

        # 启动 IO 线程
        self._recv_thread = _ZmqRecvThread(
            self._router, self._recv_queue, self._send_queue, self._stop_event,
        )
        self._recv_thread.start()
        self._broadcast_thread = _ZmqBroadcastThread(
            self._xpub, self._broadcast_queue, self._stop_event,
        )
        self._broadcast_thread.start()

    async def stop(self, linger_ms: int = 2000) -> None:
        """干净停止: 放哨兵 + join 线程 + 关闭 socket。"""
        # 1. 让线程退出
        self._stop_event.set()
        # 2. 放哨兵
        self._recv_queue.put(None)
        self._broadcast_queue.put(None)

        # 3. 等线程结束
        if self._recv_thread is not None and self._recv_thread.is_alive():
            self._recv_thread.join(timeout=2.0)
        self._recv_thread = None
        if self._broadcast_thread is not None and self._broadcast_thread.is_alive():
            self._broadcast_thread.join(timeout=2.0)
        self._broadcast_thread = None

        # 4. 关 socket
        if self._router is not None:
            self._router.close(linger=linger_ms)
            self._router = None
        if self._xpub is not None:
            self._xpub.close(linger=linger_ms)
            self._xpub = None

        # 5. term context
        if self._ctx is not None:
            self._ctx.term()
            self._ctx = None

        logger.info("ZMQ Transport 已停止")

    async def recv(self) -> list[bytes]:
        """接收一条 ROUTER 消息。"""
        return await asyncio.to_thread(self._recv_queue.get)

    async def send(self, identity: bytes, frames: list[bytes]) -> None:
        """通过 ROUTER 发送消息给特定客户端 (入 send_queue)。"""
        self._send_queue.put((identity, frames))

    async def broadcast(self, frames: list[bytes]) -> None:
        """通过 XPUB 广播 (入 broadcast_queue)。"""
        self._broadcast_queue.put(frames)
