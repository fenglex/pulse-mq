"""消息处理器测试（通过 dispatch 调用，验证拦截器链 + 处理逻辑）。"""

import pytest
from pulsemq.auth.memory_store import AuthMemoryStore
from pulsemq.auth.permission import PermissionService
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


@pytest.fixture
def setup():
    """创建无拦截器链的 handlers（兼容旧测试）。"""
    router = MessageRouter()
    sent: list[tuple[bytes, list[bytes]]] = []
    broadcast_frames: list[list[bytes]] = []
    handlers = MessageHandlers(
        router=router,
        send_fn=lambda identity, frames: sent.append((identity, frames)),
        broadcast_fn=lambda frames: broadcast_frames.append(frames),
        pipeline=None,
        default_ser="msgpack",
        default_comp="none",
    )
    # 默认注册一个 admin 用户
    user = AuthUser(user_id=1, role="admin", groups=[], api_key="key", namespace="")
    router.register_connection(b"pub_client", user)
    router.register_connection(b"sub_client", user)
    router.register_connection(b"client", user)
    return handlers, sent, broadcast_frames


class TestPubHandler:
    async def test_pub_with_subscribers(self, setup):
        h, sent, broadcast_frames = setup
        h.router.subscribe(b"sub_client", "team-a.mkt.sh.600000")

        payload = FrameCodec.encode_payload({"price": 15.8}, "msgpack", "none")
        frames = FrameCodec.encode(MsgType.PUB, "team-a.mkt.sh.600000", 1, payload)
        server_frames = [b"pub_client", b""] + frames

        await h.dispatch(server_frames)

        assert len(broadcast_frames) == 1
        assert broadcast_frames[0][0] == b"team-a.mkt.sh.600000"
        assert broadcast_frames[0][1][0] == MsgType.BROADCAST
        assert h.router.latest_seq("team-a.mkt.sh.600000") == 1

    async def test_pub_no_subscribers(self, setup):
        h, sent, broadcast_frames = setup

        payload = FrameCodec.encode_payload({"price": 15.8}, "msgpack", "none")
        frames = FrameCodec.encode(MsgType.PUB, "team-a.mkt.sh.600000", 1, payload)
        server_frames = [b"pub_client", b""] + frames

        await h.dispatch(server_frames)

        assert len(broadcast_frames) == 0
        assert h.router.latest_seq("team-a.mkt.sh.600000") == 1


class TestSubHandler:
    async def test_subscribe_success(self, setup):
        h, sent, broadcast_frames = setup
        h.router.register_topic("team-a.mkt.sh.600000")

        frames = FrameCodec.encode(MsgType.SUB, "team-a.mkt.sh.600000", 0, b"")
        server_frames = [b"sub_client", b""] + frames

        await h.dispatch(server_frames)

        subs = h.router.get_subscribers("team-a.mkt.sh.600000")
        assert b"sub_client" in subs
        assert len(sent) == 1
        assert sent[0][1][1][0] == MsgType.SUB  # meta byte 0 = msg_type

    async def test_unsubscribe(self, setup):
        h, sent, _ = setup
        h.router.subscribe(b"sub_client", "team-a.mkt.sh.600000")

        frames = FrameCodec.encode(MsgType.UNSUB, "team-a.mkt.sh.600000", 0, b"")
        server_frames = [b"sub_client", b""] + frames

        await h.dispatch(server_frames)

        assert b"sub_client" not in h.router.get_subscribers("team-a.mkt.sh.600000")


class TestPingPong:
    async def test_ping_pong(self, setup):
        h, sent, _ = setup

        payload = FrameCodec.encode_payload({"client_ts": 1234.5}, "msgpack", "none")
        frames = FrameCodec.encode(MsgType.PING, "", 0, payload)
        server_frames = [b"client", b""] + frames

        await h.dispatch(server_frames)

        assert len(sent) == 1
        assert sent[0][0] == b"client"
        assert sent[0][1][1][0] == MsgType.PONG  # meta byte 0 = msg_type


class TestWildcardSub:
    async def test_wildcard_subscribe_expands(self, setup):
        h, sent, _ = setup
        # 预先注册精确 topic
        h.router.register_topic("team-a.mkt.sh.600000")
        h.router.register_topic("team-a.mkt.sz.000333")

        frames = FrameCodec.encode(MsgType.SUB, "team-a.mkt.*", 0, b"")
        server_frames = [b"sub_client", b""] + frames

        await h.dispatch(server_frames)

        # 通配符订阅应展开到已有精确 topic
        subs_600000 = h.router.get_subscribers("team-a.mkt.sh.600000")
        subs_000333 = h.router.get_subscribers("team-a.mkt.sz.000333")
        assert b"sub_client" in subs_600000
        assert b"sub_client" in subs_000333

        # 新 topic 也应匹配通配符
        subs_new = h.router.get_subscribers("team-a.mkt.bj.000001")
        assert b"sub_client" in subs_new

    async def test_wildcard_reply_with_expanded(self, setup):
        h, sent, _ = setup
        h.router.register_topic("team-a.mkt.sh.600000")

        frames = FrameCodec.encode(MsgType.SUB, "team-a.mkt.*", 0, b"")
        server_frames = [b"sub_client", b""] + frames

        await h.dispatch(server_frames)

        # SUB 确认应包含 expanded_topics
        assert len(sent) == 1
        reply_data = FrameCodec.decode_payload(sent[0][1][3], "msgpack", "none")
        assert "team-a.mkt.sh.600000" in reply_data["expanded_topics"]


class TestAuthPipeline:
    async def test_unauthenticated_rejected(self):
        """未认证连接被拦截器拒绝。"""
        router = MessageRouter()
        auth_store = AuthMemoryStore()
        sent: list = []

        pipeline = InterceptorChain([
            AuthInterceptor(auth_store),
        ])
        handlers = MessageHandlers(
            router=router,
            send_fn=lambda identity, frames: sent.append((identity, frames)),
            broadcast_fn=lambda frames: None,
            pipeline=pipeline,
        )

        payload = FrameCodec.encode_payload({"data": 1}, "msgpack", "none")
        frames = FrameCodec.encode(MsgType.PUB, "test.topic", 1, payload)
        server_frames = [b"unknown", b""] + frames

        await handlers.dispatch(server_frames)

        # 应收到 ERROR 1001
        assert len(sent) == 1
        error_data = FrameCodec.decode_payload(sent[0][1][3], "msgpack", "none")
        assert error_data["code"] == 1001
