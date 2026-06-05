"""Benchmark 测试共享 fixtures。"""

import pytest
import time

from pulsemq.auth.memory_store import AuthMemoryStore
from pulsemq.engine.handlers import MessageHandlers
from pulsemq.engine.pipeline import (
    AuthInterceptor,
    InterceptorChain,
    MonitorInterceptor,
    PermissionInterceptor,
)
from pulsemq.auth.permission import PermissionService
from pulsemq.engine.router import MessageRouter
from pulsemq.models import AuthUser
from pulsemq.monitoring.realtime import RealtimeMetrics
from pulsemq.protocol.frames import FrameCodec
from pulsemq.protocol.msg_type import MsgType


@pytest.fixture
def router() -> MessageRouter:
    return MessageRouter()


@pytest.fixture
def auth_store() -> AuthMemoryStore:
    store = AuthMemoryStore()
    admin = AuthUser(user_id=1, role="admin", groups=[], api_key="bench_key", namespace="")
    store.register(b"bench_id", admin)
    return store


@pytest.fixture
def realtime_metrics() -> RealtimeMetrics:
    return RealtimeMetrics()


@pytest.fixture
def admin_user() -> AuthUser:
    return AuthUser(user_id=1, role="admin", groups=[], api_key="bench_key", namespace="")


@pytest.fixture
def full_handlers(router, auth_store, realtime_metrics):
    """带完整拦截器链的 MessageHandlers。"""
    sent: list = []
    broadcast_frames: list = []

    pipeline = InterceptorChain([
        MonitorInterceptor(realtime_metrics=realtime_metrics),
        AuthInterceptor(auth_store),
        PermissionInterceptor(PermissionService(perm_repo=None)),
    ])

    handlers = MessageHandlers(
        router=router,
        send_fn=lambda identity, frames: sent.append((identity, frames)),
        broadcast_fn=lambda frames: broadcast_frames.append(frames),
        pipeline=pipeline,
    )
    return handlers, sent, broadcast_frames


@pytest.fixture
def pub_sub_setup(full_handlers, router, auth_store, admin_user):
    """预建好 PUB/SUB 订阅关系的测试环境。"""
    handlers, sent, broadcast = full_handlers

    # 注册 pub 和 sub 的连接
    auth_store.register(b"pub_id", admin_user)
    auth_store.register(b"sub_id", admin_user)
    router.register_connection(b"pub_id", admin_user)
    router.register_connection(b"sub_id", admin_user)

    return handlers, sent, broadcast


def make_pub_frames(topic: str, data, ser: str = "msgpack", comp: str = "none"):
    """快速构建 PUB 6 帧。"""
    payload = FrameCodec.encode_payload(data, ser, comp)
    frames = FrameCodec.encode(MsgType.PUB, topic, 1, payload, ser, comp)
    return [b"pub_id", b""] + frames


def make_sub_frames(topic: str, identity: bytes = b"sub_id"):
    """快速构建 SUB 6 帧。"""
    frames = FrameCodec.encode(MsgType.SUB, topic, 0, b"")
    return [identity, b""] + frames


def make_unsub_frames(topic: str, identity: bytes = b"sub_id"):
    """快速构建 UNSUB 6 帧。"""
    frames = FrameCodec.encode(MsgType.UNSUB, topic, 0, b"")
    return [identity, b""] + frames


def make_ping_frames(identity: bytes = b"bench_id"):
    """快速构建 PING 6 帧。"""
    payload = FrameCodec.encode_payload({"client_ts": time.time()}, "msgpack", "none")
    frames = FrameCodec.encode(MsgType.PING, "", 0, payload)
    return [identity, b""] + frames


def make_query_frames(action: str = "system_status", identity: bytes = b"bench_id"):
    """快速构建 QUERY 6 帧。"""
    payload = FrameCodec.encode_payload({"action": action}, "msgpack", "none")
    frames = FrameCodec.encode(MsgType.QUERY, "", 0, payload)
    return [identity, b""] + frames


def make_replay_frames(topic: str, from_seq: int = 0, limit: int = 100,
                       identity: bytes = b"bench_id"):
    """快速构建 HISTORY_REPLAY 6 帧。"""
    payload = FrameCodec.encode_payload(
        {"from_seq": from_seq, "limit": limit}, "msgpack", "none"
    )
    frames = FrameCodec.encode(MsgType.HISTORY_REPLAY, topic, 0, payload)
    return [identity, b""] + frames


class BenchResult:
    """简单的基准测试结果收集器。"""

    def __init__(self, name: str):
        self.name = name
        self.total_ops: int = 0
        self.elapsed_s: float = 0.0

    def __enter__(self):
        self._start = time.perf_counter()
        return self

    def __exit__(self, *args):
        self.elapsed_s = time.perf_counter() - self._start

    def set_ops(self, n: int):
        self.total_ops = n

    @property
    def ops_per_sec(self) -> float:
        return self.total_ops / self.elapsed_s if self.elapsed_s > 0 else 0

    @property
    def us_per_op(self) -> float:
        return (self.elapsed_s / self.total_ops * 1_000_000) if self.total_ops > 0 else 0

    def report(self) -> str:
        return (
            f"  {self.name}: "
            f"{self.ops_per_sec:,.0f} ops/s, "
            f"{self.us_per_op:.1f} us/op "
            f"({self.total_ops:,} ops in {self.elapsed_s:.3f}s)"
        )
