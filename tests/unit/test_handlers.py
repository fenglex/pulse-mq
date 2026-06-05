import pytest
from pulsemq.engine.handlers import MessageHandlers
from pulsemq.engine.router import MessageRouter
from pulsemq.models import AuthUser
from pulsemq.protocol.frames import FrameCodec
from pulsemq.protocol.msg_type import MsgType


@pytest.fixture
def handlers():
    router = MessageRouter()
    sent: list[tuple[bytes, list[bytes]]] = []
    broadcast_frames: list[list[bytes]] = []
    return MessageHandlers(
        router=router,
        send_fn=lambda identity, frames: sent.append((identity, frames)),
        broadcast_fn=lambda frames: broadcast_frames.append(frames),
        default_ser="msgpack",
        default_comp="none",
    ), sent, broadcast_frames


class TestPubHandler:
    async def test_pub_with_subscribers(self, handlers):
        h, sent, broadcast_frames = handlers
        identity = b"pub_client"
        user = AuthUser(user_id=1, role="user", groups=[], api_key="key", namespace="")
        h.router.register_connection(identity, user)
        h.router.subscribe(b"sub_client", "team-a.mkt.sh.600000")

        payload = FrameCodec.encode_payload({"price": 15.8}, "msgpack", "none")
        frames = FrameCodec.encode(MsgType.PUB, "team-a.mkt.sh.600000", 1, payload)
        server_frames = [identity, b""] + frames

        await h.handle_pub(server_frames)

        # 验证广播帧被发送
        assert len(broadcast_frames) == 1
        assert broadcast_frames[0][0] == b"team-a.mkt.sh.600000"
        assert broadcast_frames[0][1][0] == MsgType.BROADCAST

        # 验证消息被缓存
        assert h.router.latest_seq("team-a.mkt.sh.600000") == 1

    async def test_pub_no_subscribers(self, handlers):
        h, sent, broadcast_frames = handlers
        identity = b"pub_client"
        user = AuthUser(user_id=1, role="user", groups=[], api_key="key", namespace="")
        h.router.register_connection(identity, user)

        payload = FrameCodec.encode_payload({"price": 15.8}, "msgpack", "none")
        frames = FrameCodec.encode(MsgType.PUB, "team-a.mkt.sh.600000", 1, payload)
        server_frames = [identity, b""] + frames

        await h.handle_pub(server_frames)

        # 无订阅者，不广播但仍然缓存
        assert len(broadcast_frames) == 0
        assert h.router.latest_seq("team-a.mkt.sh.600000") == 1


class TestSubHandler:
    async def test_subscribe_success(self, handlers):
        h, sent, broadcast_frames = handlers
        identity = b"sub_client"
        user = AuthUser(user_id=1, role="user", groups=[], api_key="key", namespace="")
        h.router.register_connection(identity, user)
        h.router.register_topic("team-a.mkt.sh.600000")

        frames = FrameCodec.encode(MsgType.SUB, "team-a.mkt.sh.600000", 0, b"")
        server_frames = [identity, b""] + frames

        await h.handle_sub(server_frames)

        # 验证订阅关系建立
        subs = h.router.get_subscribers("team-a.mkt.sh.600000")
        assert identity in subs

        # 验证 SUB 确认被发送
        assert len(sent) == 1
        reply_identity, reply_frames = sent[0]
        assert reply_identity == identity
        assert reply_frames[1][0] == MsgType.SUB

    async def test_unsubscribe(self, handlers):
        h, sent, _ = handlers
        identity = b"sub_client"
        user = AuthUser(user_id=1, role="user", groups=[], api_key="key", namespace="")
        h.router.register_connection(identity, user)
        h.router.subscribe(identity, "team-a.mkt.sh.600000")

        frames = FrameCodec.encode(MsgType.UNSUB, "team-a.mkt.sh.600000", 0, b"")
        server_frames = [identity, b""] + frames

        await h.handle_unsub(server_frames)

        assert identity not in h.router.get_subscribers("team-a.mkt.sh.600000")


class TestPingPong:
    async def test_ping_pong(self, handlers):
        h, sent, _ = handlers
        identity = b"client"
        user = AuthUser(user_id=1, role="user", groups=[], api_key="key", namespace="")
        h.router.register_connection(identity, user)

        payload = FrameCodec.encode_payload({"client_ts": 1234.5}, "msgpack", "none")
        frames = FrameCodec.encode(MsgType.PING, "", 0, payload)
        server_frames = [identity, b""] + frames

        await h.handle_ping(server_frames)

        assert len(sent) == 1
        reply_identity, reply_frames = sent[0]
        assert reply_identity == identity
        assert reply_frames[1][0] == MsgType.PONG


class TestDispatch:
    async def test_dispatch_pub(self, handlers):
        h, sent, broadcast_frames = handlers
        identity = b"pub_client"
        user = AuthUser(user_id=1, role="user", groups=[], api_key="key", namespace="")
        h.router.register_connection(identity, user)
        h.router.subscribe(b"sub_client", "test.topic")

        payload = FrameCodec.encode_payload({"data": 1}, "msgpack", "none")
        frames = FrameCodec.encode(MsgType.PUB, "test.topic", 1, payload)
        server_frames = [identity, b""] + frames

        await h.dispatch(server_frames)
        assert len(broadcast_frames) == 1

    async def test_dispatch_ping(self, handlers):
        h, sent, _ = handlers
        identity = b"client"
        user = AuthUser(user_id=1, role="user", groups=[], api_key="key", namespace="")
        h.router.register_connection(identity, user)

        payload = FrameCodec.encode_payload({"client_ts": 1234.5}, "msgpack", "none")
        frames = FrameCodec.encode(MsgType.PING, "", 0, payload)
        server_frames = [identity, b""] + frames

        await h.dispatch(server_frames)
        assert len(sent) == 1
