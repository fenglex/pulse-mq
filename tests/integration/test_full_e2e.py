"""全链路集成测试：Server 完整组装 + 认证 + 监控 + AUTH 推送 + 断线清理。"""

import asyncio

import pytest
import zmq
import zmq.asyncio

from pulsemq.auth.memory_store import AuthMemoryStore
from pulsemq.auth.permission import PermissionService
from pulsemq.config import BrokerConfig
from pulsemq.engine.engine import Engine
from pulsemq.engine.handlers import MessageHandlers
from pulsemq.engine.pipeline import (
    AuthInterceptor,
    InterceptorChain,
    MonitorInterceptor,
    PermissionInterceptor,
)
from pulsemq.engine.router import MessageRouter
from pulsemq.models import AuthUser
from pulsemq.monitoring.realtime import RealtimeMetrics
from pulsemq.protocol.frames import FrameCodec
from pulsemq.protocol.msg_type import MsgType
from pulsemq.storage.database import init_db
from pulsemq.storage.interfaces import User
from pulsemq.storage.sqlite_perm import SqlitePermGroupRepo
from pulsemq.storage.sqlite_user import SqliteUserRepo
from pulsemq.transport.zmq_transport import ZmqTransport

import random


class TestFullIntegration:
    """完整 Server 组装的集成测试。"""

    async def test_server_with_monitoring(self, tmp_path):
        """Server 启动 → 发布 → 订阅 → 监控指标有数据。"""
        port_router = random.randint(20000, 25000)
        port_xpub = port_router + 1

        config = BrokerConfig(
            bind=f"tcp://*:{port_router}",
            xpub_bind=f"tcp://*:{port_xpub}",
            db_url=f"sqlite://{tmp_path / 'test.db'}",
            auth_enabled=False,  # 简化：禁用认证
        )

        # 组装完整 Server（与 PulseServer 相同逻辑）
        router = MessageRouter()
        router.buffer_enabled = True  # 测试场景下开启缓冲
        realtime = RealtimeMetrics()
        monitor = MonitorInterceptor(realtime_metrics=realtime)
        auth_store = AuthMemoryStore()

        # 注入 admin（auth_disabled 模式）
        admin = AuthUser(user_id=1, role="admin", groups=[], api_key="default", namespace="")
        auth_store.register(b"pub_id", admin)
        auth_store.register(b"sub_id", admin)
        router.register_connection(b"pub_id", admin)
        router.register_connection(b"sub_id", admin)

        pipeline = InterceptorChain([
            monitor,                                           # 外层：记录延迟和错误
            AuthInterceptor(auth_store),                       # 认证
            PermissionInterceptor(PermissionService(perm_repo=None)),  # 权限
        ])

        transport = ZmqTransport(config)

        handlers = MessageHandlers(
            router=router,
            send_fn=transport.send,
            broadcast_fn=transport.broadcast,
            pipeline=pipeline,
        )

        engine = Engine(transport=transport, handlers=handlers, config=config)
        await transport.start()

        try:
            # SUB
            sub_frames = FrameCodec.encode(MsgType.SUB, "mkt.sh.600000", 0, b"")
            await handlers.dispatch([b"sub_id", b""] + sub_frames)

            # PUB
            data = {"price": 15.8, "symbol": "sh.600000"}
            payload = FrameCodec.encode_payload(data, "msgpack", "none")
            pub_frames = FrameCodec.encode(MsgType.PUB, "mkt.sh.600000", 1, payload)
            await handlers.dispatch([b"pub_id", b""] + pub_frames)

            # 验证广播
            subscribers = router.get_subscribers("mkt.sh.600000")
            assert b"sub_id" in subscribers

            # 验证监控指标有数据
            snap = realtime.snapshot()
            assert snap["msg_rate"] > 0
            assert snap["active_connections"] == 2

            # 验证路由统计
            assert router.topic_count() >= 1
            assert router.latest_seq("mkt.sh.600000") == 1

        finally:
            await engine.stop()
            await transport.stop()

    async def test_permission_denied_in_full_pipeline(self, tmp_path):
        """完整流水线：普通用户无 pub 权限被拒绝。"""
        port = random.randint(20000, 25000)
        config = BrokerConfig(
            bind=f"tcp://*:{port}",
            xpub_bind=f"tcp://*:{port+1}",
            db_url=f"sqlite://{tmp_path / 'test.db'}",
            auth_enabled=False,
        )

        conn = init_db(str(tmp_path / "test.db"))
        user_repo = SqliteUserRepo(conn)
        perm_repo = SqlitePermGroupRepo(conn)

        # 创建只读用户
        u = await user_repo.create(User(username="readonly", api_key="k_ro", role="user"))
        g = await perm_repo.create_group("只读")
        await perm_repo.add_permission(g.id, "*.mkt.*", "sub")
        await perm_repo.add_member(g.id, u.id)

        router = MessageRouter()
        auth_store = AuthMemoryStore()
        realtime = RealtimeMetrics()
        perm_svc = PermissionService(perm_repo)

        user = AuthUser(user_id=u.id, role="user", groups=[], api_key="k_ro", namespace="")
        auth_store.register(b"ro_id", user)
        router.register_connection(b"ro_id", user)

        sent: list = []
        pipeline = InterceptorChain([
            MonitorInterceptor(realtime),                       # 外层：记录延迟和错误
            AuthInterceptor(auth_store),                        # 认证
            PermissionInterceptor(perm_svc),                   # 权限
        ])

        transport = ZmqTransport(config)
        handlers = MessageHandlers(
            router=router,
            send_fn=lambda identity, frames: sent.append((identity, frames)),
            broadcast_fn=lambda frames: None,
            pipeline=pipeline,
        )

        engine = Engine(transport=transport, handlers=handlers, config=config)
        await transport.start()

        try:
            # SUB 应成功
            sub_frames = FrameCodec.encode(MsgType.SUB, "team-a.mkt.sh.600000", 0, b"")
            await handlers.dispatch([b"ro_id", b""] + sub_frames)

            # PUB 应被拒绝
            payload = FrameCodec.encode_payload({"data": 1}, "msgpack", "none")
            pub_frames = FrameCodec.encode(MsgType.PUB, "team-a.mkt.sh.600000", 1, payload)
            sent.clear()
            await handlers.dispatch([b"ro_id", b""] + pub_frames)

            # 应收到 ERROR 2001
            assert len(sent) == 1
            error_data = FrameCodec.decode_payload(sent[0][1][3], "msgpack", "none")
            assert error_data["code"] == 2001

            # 监控应有错误记录
            assert realtime.snapshot()["error_rate"] > 0

        finally:
            await engine.stop()
            await transport.stop()
            conn.close()

    async def test_auth_push_on_connect(self, tmp_path):
        """AUTH 元信息推送：连接建立后自动推送用户元信息。"""
        auth_store = AuthMemoryStore()
        admin = AuthUser(user_id=1, role="admin", groups=["行情全订阅"], api_key="k", namespace="team-a")
        auth_store.register(b"test_id", admin)

        sent: list = []

        # 模拟 AUTH 推送
        auth_info = {
            "user_id": admin.user_id,
            "role": admin.role,
            "namespace": admin.namespace,
            "groups": admin.groups,
            "server_time": asyncio.get_event_loop().time(),
        }
        payload = FrameCodec.encode_payload(auth_info, "msgpack", "none")
        frames = FrameCodec.encode(MsgType.AUTH, "", 0, payload, "msgpack", "none")

        # 直接调用 transport.send 会需要真实 socket，这里只验证帧格式
        assert len(frames) == 4
        assert frames[0] == b""  # 空 topic
        assert frames[1][0] == MsgType.AUTH
        decoded = FrameCodec.decode_payload(frames[3], "msgpack", "none")
        assert decoded["user_id"] == 1
        assert decoded["role"] == "admin"
        assert "行情全订阅" in decoded["groups"]

    async def test_disconnect_cleanup(self, tmp_path):
        """断线清理：移除认证 + 订阅 + 连接映射。"""
        auth_store = AuthMemoryStore()
        router = MessageRouter()

        user = AuthUser(user_id=1, role="user", groups=[], api_key="k", namespace="")
        identity = b"client_1"

        # 模拟连接
        auth_store.register(identity, user)
        router.register_connection(identity, user)
        router.subscribe(identity, "topic_a")
        router.subscribe(identity, "topic_b")

        assert auth_store.get_user(identity) is not None
        assert len(router.get_subscriptions(identity)) == 2

        # 模拟断线清理
        auth_store.unregister(identity)
        router.unregister_connection(identity)
        router.remove_identity(identity)

        assert auth_store.get_user(identity) is None
        assert len(router.get_subscriptions(identity)) == 0
        assert b"client_1" not in router.get_subscribers("topic_a")
        assert b"client_1" not in router.get_subscribers("topic_b")

    async def test_real_zmq_with_monitoring(self, tmp_path):
        """真实 ZMQ 链路 + 监控指标采集。"""
        port = random.randint(20000, 25000)
        config = BrokerConfig(
            bind=f"tcp://*:{port}",
            xpub_bind=f"tcp://*:{port+1}",
            db_url=f"sqlite://{tmp_path / 'test.db'}",
            auth_enabled=False,
        )

        router = MessageRouter()
        realtime = RealtimeMetrics()
        auth_store = AuthMemoryStore()

        # 注入 admin
        admin = AuthUser(user_id=1, role="admin", groups=[], api_key="default", namespace="")
        auth_store.register(b"dealer_1", admin)
        router.register_connection(b"dealer_1", admin)

        pipeline = InterceptorChain([
            AuthInterceptor(auth_store),
            PermissionInterceptor(PermissionService(perm_repo=None)),
            MonitorInterceptor(realtime_metrics=realtime),
        ])

        transport = ZmqTransport(config)
        handlers = MessageHandlers(
            router=router,
            send_fn=transport.send,
            broadcast_fn=transport.broadcast,
            pipeline=pipeline,
        )
        engine = Engine(transport=transport, handlers=handlers, config=config)
        await transport.start()

        ctx = zmq.asyncio.Context()
        try:
            # SUB 客户端
            sub_sock = ctx.socket(zmq.SUB)
            sub_sock.connect(f"tcp://127.0.0.1:{port+1}")
            sub_sock.setsockopt(zmq.SUBSCRIBE, b"test.topic")
            await asyncio.sleep(0.2)

            # DEALER 客户端
            dealer = ctx.socket(zmq.DEALER)
            dealer.setsockopt(zmq.IDENTITY, b"dealer_1")
            dealer.connect(f"tcp://127.0.0.1:{port}")
            await asyncio.sleep(0.1)

            # SUB
            sub_frames = FrameCodec.encode(MsgType.SUB, "test.topic", 0, b"")
            await dealer.send_multipart(sub_frames)
            frames = await asyncio.wait_for(transport.recv(), timeout=2.0)
            await handlers.dispatch(frames)

            # PUB
            data = {"price": 42.0}
            payload = FrameCodec.encode_payload(data, "msgpack", "none")
            pub_frames = FrameCodec.encode(MsgType.PUB, "test.topic", 1, payload)
            await dealer.send_multipart(pub_frames)
            frames = await asyncio.wait_for(transport.recv(), timeout=2.0)
            await handlers.dispatch(frames)

            # 验证监控指标
            snap = realtime.snapshot()
            assert snap["msg_rate"] > 0
            assert snap["latency_p50_ms"] >= 0

        finally:
            sub_sock.close(linger=0)
            dealer.close(linger=0)
            ctx.term()
            await engine.stop()
            await transport.stop()
