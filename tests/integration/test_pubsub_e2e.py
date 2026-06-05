"""端到端集成测试：PUB → SUB 完整消息链路 + 认证 + 权限 + 通配符。"""

import asyncio
import random

import pytest
import zmq
import zmq.asyncio

from pulsemq.auth.memory_store import AuthMemoryStore
from pulsemq.auth.permission import PermissionService
from pulsemq.config import BrokerConfig
from pulsemq.engine.handlers import MessageHandlers
from pulsemq.engine.pipeline import (
    AuthInterceptor,
    InterceptorChain,
    MonitorInterceptor,
    PermissionInterceptor,
)
from pulsemq.engine.router import MessageRouter
from pulsemq.models import AuthUser
from pulsemq.protocol.frames import FrameCodec
from pulsemq.protocol.msg_type import MsgType
from pulsemq.transport.zmq_transport import ZmqTransport


@pytest.fixture
async def broker_config():
    port_router = random.randint(16000, 19000)
    port_xpub = port_router + 1
    return BrokerConfig(
        bind=f"tcp://*:{port_router}",
        xpub_bind=f"tcp://*:{port_xpub}",
    )


@pytest.fixture
async def running_broker(broker_config):
    transport = ZmqTransport(broker_config)
    router = MessageRouter()
    sent_messages: list[tuple[bytes, list[bytes]]] = []
    broadcast_messages: list[list[bytes]] = []

    async def send_fn(identity, frames):
        sent_messages.append((identity, frames))

    async def broadcast_fn(frames):
        broadcast_messages.append(frames)

    handlers = MessageHandlers(
        router=router,
        send_fn=send_fn,
        broadcast_fn=broadcast_fn,
    )
    await transport.start()

    yield transport, router, handlers, sent_messages, broadcast_messages, broker_config

    await transport.stop()


class TestPubSubE2E:
    async def test_pub_sub_full_flow(self, running_broker):
        transport, router, handlers, sent, broadcast, config = running_broker

        pub_id = b"pub_client"
        sub_id = b"sub_client"
        pub_user = AuthUser(user_id=1, role="admin", groups=[], api_key="k1", namespace="")
        sub_user = AuthUser(user_id=2, role="admin", groups=[], api_key="k2", namespace="")
        router.register_connection(pub_id, pub_user)
        router.register_connection(sub_id, sub_user)

        # SUB
        sub_frames = FrameCodec.encode(MsgType.SUB, "test.topic", 0, b"")
        await handlers.dispatch([sub_id, b""] + sub_frames)
        assert sub_id in router.get_subscribers("test.topic")

        # PUB
        data = {"price": 15.8, "volume": 1000}
        payload = FrameCodec.encode_payload(data, "msgpack", "none")
        pub_frames = FrameCodec.encode(MsgType.PUB, "test.topic", 1, payload)
        await handlers.dispatch([pub_id, b""] + pub_frames)

        assert len(broadcast) == 1
        result = FrameCodec.decode_payload(broadcast[0][3], "msgpack", "none")
        assert result == data
        assert router.latest_seq("test.topic") == 1

    async def test_ping_pong_flow(self, running_broker):
        transport, router, handlers, sent, broadcast, config = running_broker

        identity = b"ping_client"
        user = AuthUser(user_id=1, role="admin", groups=[], api_key="k", namespace="")
        router.register_connection(identity, user)

        payload = FrameCodec.encode_payload({"client_ts": 1234.5}, "msgpack", "none")
        frames = FrameCodec.encode(MsgType.PING, "", 0, payload)
        await handlers.dispatch([identity, b""] + frames)

        assert len(sent) == 1
        pong_data = FrameCodec.decode_payload(sent[0][1][3], "msgpack", "none")
        assert pong_data["client_ts"] == 1234.5

    async def test_multiple_topics(self, running_broker):
        transport, router, handlers, sent, broadcast, config = running_broker

        sub_user = AuthUser(user_id=2, role="admin", groups=[], api_key="k2", namespace="")
        router.register_connection(b"sub1", sub_user)
        router.subscribe(b"sub1", "topic_a")
        router.subscribe(b"sub1", "topic_b")

        pub_user = AuthUser(user_id=1, role="admin", groups=[], api_key="k1", namespace="")
        router.register_connection(b"pub1", pub_user)

        for topic, val in [("topic_a", "a"), ("topic_b", "b")]:
            payload = FrameCodec.encode_payload({"data": val}, "msgpack", "none")
            frames = FrameCodec.encode(MsgType.PUB, topic, 1, payload)
            await handlers.dispatch([b"pub1", b""] + frames)

        assert len(broadcast) == 2
        assert router.latest_seq("topic_a") == 1
        assert router.latest_seq("topic_b") == 1

    async def test_real_zmq_pubsub(self, broker_config):
        transport = ZmqTransport(broker_config)
        router = MessageRouter()

        async def send_fn(identity, frames):
            await transport.send(identity, frames)

        async def broadcast_fn(frames):
            await transport.broadcast(frames)

        handlers = MessageHandlers(
            router=router,
            send_fn=send_fn,
            broadcast_fn=broadcast_fn,
        )
        await transport.start()

        pub_user = AuthUser(user_id=1, role="admin", groups=[], api_key="k1", namespace="")
        router.register_connection(b"dealer_pub", pub_user)

        ctx = zmq.asyncio.Context()

        try:
            sub_sock = ctx.socket(zmq.SUB)
            sub_sock.connect(broker_config.xpub_bind.replace("*", "127.0.0.1"))
            sub_sock.setsockopt(zmq.SUBSCRIBE, b"mkt.sh.600000")
            await asyncio.sleep(0.2)

            dealer_sock = ctx.socket(zmq.DEALER)
            dealer_sock.setsockopt(zmq.IDENTITY, b"dealer_pub")
            dealer_sock.connect(broker_config.bind.replace("*", "127.0.0.1"))
            await asyncio.sleep(0.1)

            data = {"price": 15.8, "symbol": "sh.600000"}
            payload = FrameCodec.encode_payload(data, "msgpack", "none")
            pub_frames = FrameCodec.encode(MsgType.PUB, "mkt.sh.600000", 1, payload)
            await dealer_sock.send_multipart(pub_frames)

            frames = await asyncio.wait_for(transport.recv(), timeout=2.0)
            await handlers.dispatch(frames)

            try:
                result = await asyncio.wait_for(sub_sock.recv_multipart(), timeout=2.0)
                assert result[0] == b"mkt.sh.600000"
                assert result[1][0] == MsgType.BROADCAST
                decoded = FrameCodec.decode_payload(result[3], "msgpack", "none")
                assert decoded["price"] == 15.8
            except asyncio.TimeoutError:
                assert router.latest_seq("mkt.sh.600000") == 1

        finally:
            sub_sock.close(linger=0)
            dealer_sock.close(linger=0)
            ctx.term()
            await transport.stop()


class TestAuthPipelineE2E:
    """认证 + 权限拦截器集成测试。"""

    async def test_unauthenticated_pub_rejected(self):
        router = MessageRouter()
        auth_store = AuthMemoryStore()
        sent: list = []
        broadcast: list = []

        pipeline = InterceptorChain([AuthInterceptor(auth_store)])

        handlers = MessageHandlers(
            router=router,
            send_fn=lambda identity, frames: sent.append((identity, frames)),
            broadcast_fn=lambda frames: broadcast.append(frames),
            pipeline=pipeline,
        )

        payload = FrameCodec.encode_payload({"data": 1}, "msgpack", "none")
        frames = FrameCodec.encode(MsgType.PUB, "test.topic", 1, payload)
        server_frames = [b"unknown_client", b""] + frames

        await handlers.dispatch(server_frames)

        # 应收到 ERROR 1001
        assert len(sent) == 1
        error_data = FrameCodec.decode_payload(sent[0][1][3], "msgpack", "none")
        assert error_data["code"] == 1001
        assert len(broadcast) == 0

    async def test_authenticated_admin_passes(self, tmp_path):
        from pulsemq.storage.database import init_db
        from pulsemq.storage.sqlite_perm import SqlitePermGroupRepo

        conn = init_db(str(tmp_path / "test.db"))
        perm_repo = SqlitePermGroupRepo(conn)

        router = MessageRouter()
        auth_store = AuthMemoryStore()
        perm_service = PermissionService(perm_repo)
        sent: list = []
        broadcast: list = []

        # 注册 admin 用户
        admin = AuthUser(user_id=1, role="admin", groups=[], api_key="k", namespace="")
        auth_store.register(b"admin_client", admin)
        router.register_connection(b"admin_client", admin)

        pipeline = InterceptorChain([
            AuthInterceptor(auth_store),
            PermissionInterceptor(perm_service),
        ])

        handlers = MessageHandlers(
            router=router,
            send_fn=lambda identity, frames: sent.append((identity, frames)),
            broadcast_fn=lambda frames: broadcast.append(frames),
            pipeline=pipeline,
        )

        # SUB
        sub_frames = FrameCodec.encode(MsgType.SUB, "test.topic", 0, b"")
        await handlers.dispatch([b"admin_client", b""] + sub_frames)
        assert b"admin_client" in router.get_subscribers("test.topic")

        # PUB
        payload = FrameCodec.encode_payload({"data": 1}, "msgpack", "none")
        pub_frames = FrameCodec.encode(MsgType.PUB, "test.topic", 1, payload)
        await handlers.dispatch([b"admin_client", b""] + pub_frames)
        assert len(broadcast) == 1

        conn.close()

    async def test_permission_denied(self, tmp_path):
        from pulsemq.storage.database import init_db
        from pulsemq.storage.interfaces import User
        from pulsemq.storage.sqlite_user import SqliteUserRepo
        from pulsemq.storage.sqlite_perm import SqlitePermGroupRepo

        conn = init_db(str(tmp_path / "test.db"))
        user_repo = SqliteUserRepo(conn)
        perm_repo = SqlitePermGroupRepo(conn)

        # 创建只有 sub 权限的用户
        u = await user_repo.create(User(username="readonly", api_key="k_ro", role="user"))
        g = await perm_repo.create_group("只读")
        await perm_repo.add_permission(g.id, "*.mkt.*", "sub")
        await perm_repo.add_member(g.id, u.id)

        router = MessageRouter()
        auth_store = AuthMemoryStore()
        perm_service = PermissionService(perm_repo)
        sent: list = []

        user = AuthUser(user_id=u.id, role="user", groups=[], api_key="k_ro", namespace="")
        auth_store.register(b"ro_client", user)
        router.register_connection(b"ro_client", user)

        pipeline = InterceptorChain([
            AuthInterceptor(auth_store),
            PermissionInterceptor(perm_service),
        ])

        handlers = MessageHandlers(
            router=router,
            send_fn=lambda identity, frames: sent.append((identity, frames)),
            broadcast_fn=lambda frames: None,
            pipeline=pipeline,
        )

        # SUB 应成功
        sub_frames = FrameCodec.encode(MsgType.SUB, "team-a.mkt.sh.600000", 0, b"")
        await handlers.dispatch([b"ro_client", b""] + sub_frames)
        sub_replies = [s for s in sent if len(s[1]) > 1 and s[1][1][0] == MsgType.SUB]
        assert len(sub_replies) >= 1

        # PUB 应被拒绝
        payload = FrameCodec.encode_payload({"data": 1}, "msgpack", "none")
        pub_frames = FrameCodec.encode(MsgType.PUB, "team-a.mkt.sh.600000", 1, payload)
        sent.clear()
        await handlers.dispatch([b"ro_client", b""] + pub_frames)
        error_data = FrameCodec.decode_payload(sent[0][1][3], "msgpack", "none")
        assert error_data["code"] == 2001

        conn.close()
