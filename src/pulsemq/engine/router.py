"""纯内存消息路由器。

包含四个子组件:
- TopicRegistry: 精确 topic 注册（Phase 1 不含通配符展开）
- SubscriptionManager: 订阅关系双向索引
- ConnectionManager: identity ↔ user 映射
- MessageBuffer: 每个 topic 的环形缓冲区
"""

from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field

from pulsemq.models import AuthUser, BufferedMessage, TopicInfo


@dataclass
class MessageRouter:
    """消息路由器（纯内存，单线程安全）。"""

    # Topic 注册表
    _topics: dict[str, TopicInfo] = field(default_factory=dict)

    # 订阅关系双向索引
    _topic_subscribers: dict[str, set[bytes]] = field(default_factory=dict)
    _identity_subscriptions: dict[bytes, set[str]] = field(default_factory=dict)

    # 连接管理
    _identity_user: dict[bytes, AuthUser] = field(default_factory=dict)
    _user_identities: dict[int, set[bytes]] = field(default_factory=dict)

    # 消息缓冲区
    _buffers: dict[str, deque] = field(default_factory=dict)
    _seq_counter: dict[str, int] = field(default_factory=dict)
    max_buffer_size: int = 1000

    # ---- Topic 管理 ----

    def register_topic(self, full_name: str) -> TopicInfo:
        """注册 topic，幂等操作。"""
        if full_name in self._topics:
            return self._topics[full_name]
        info = TopicInfo.from_name(full_name)
        self._topics[full_name] = info
        return info

    def get_topic(self, full_name: str) -> TopicInfo | None:
        return self._topics.get(full_name)

    def remove_topic_if_empty(self, full_name: str) -> None:
        """topic 无订阅者时移除。"""
        subs = self._topic_subscribers.get(full_name, set())
        if not subs:
            self._topics.pop(full_name, None)

    # ---- 订阅管理 ----

    def subscribe(self, identity: bytes, topic: str) -> None:
        """建立订阅关系。"""
        if topic not in self._topic_subscribers:
            self._topic_subscribers[topic] = set()
        self._topic_subscribers[topic].add(identity)

        if identity not in self._identity_subscriptions:
            self._identity_subscriptions[identity] = set()
        self._identity_subscriptions[identity].add(topic)

        # 更新订阅计数
        info = self._topics.get(topic)
        if info is not None:
            info.subscriber_count = len(self._topic_subscribers[topic])

    def unsubscribe(self, identity: bytes, topic: str) -> None:
        """取消订阅。"""
        subs = self._topic_subscribers.get(topic)
        if subs is not None:
            subs.discard(identity)
            info = self._topics.get(topic)
            if info is not None:
                info.subscriber_count = len(subs)

        id_subs = self._identity_subscriptions.get(identity)
        if id_subs is not None:
            id_subs.discard(topic)

    def get_subscribers(self, topic: str) -> set[bytes]:
        """获取 topic 的所有订阅者。"""
        return self._topic_subscribers.get(topic, set())

    def get_subscriptions(self, identity: bytes) -> set[str]:
        """获取 identity 的所有订阅。"""
        return self._identity_subscriptions.get(identity, set())

    def remove_identity(self, identity: bytes) -> None:
        """移除 identity 的所有订阅关系。"""
        id_subs = self._identity_subscriptions.pop(identity, set())
        for topic in id_subs:
            subs = self._topic_subscribers.get(topic)
            if subs is not None:
                subs.discard(identity)

    # ---- 连接管理 ----

    def register_connection(self, identity: bytes, user: AuthUser) -> None:
        """注册 identity ↔ user 映射。"""
        self._identity_user[identity] = user
        if user.user_id not in self._user_identities:
            self._user_identities[user.user_id] = set()
        self._user_identities[user.user_id].add(identity)

    def unregister_connection(self, identity: bytes) -> AuthUser | None:
        """移除映射，返回 user 或 None。"""
        user = self._identity_user.pop(identity, None)
        if user is not None:
            idents = self._user_identities.get(user.user_id)
            if idents is not None:
                idents.discard(identity)
        return user

    def get_user(self, identity: bytes) -> AuthUser | None:
        return self._identity_user.get(identity)

    def get_connections(self, user_id: int) -> set[bytes]:
        return self._user_identities.get(user_id, set())

    # ---- 消息缓冲 ----

    def append_message(
        self, topic: str, meta: bytes, record_count: int, payload: bytes
    ) -> BufferedMessage:
        """追加消息到环形缓冲区。"""
        seq = self._seq_counter.get(topic, 0) + 1
        self._seq_counter[topic] = seq

        msg = BufferedMessage(
            topic=topic,
            seq=seq,
            record_count=record_count,
            meta=meta,
            payload=payload,
            timestamp=time.time(),
        )

        if topic not in self._buffers:
            self._buffers[topic] = deque(maxlen=self.max_buffer_size)
        self._buffers[topic].append(msg)
        return msg

    def replay_messages(
        self, topic: str, from_seq: int = 0, limit: int = 100
    ) -> list[BufferedMessage]:
        """从指定序列号开始回放消息。"""
        buf = self._buffers.get(topic)
        if buf is None:
            return []
        msgs = [m for m in buf if m.seq >= from_seq]
        return msgs[:limit]

    def latest_seq(self, topic: str) -> int:
        return self._seq_counter.get(topic, 0)

    def remove_topic_buffer(self, topic: str) -> None:
        self._buffers.pop(topic, None)
        self._seq_counter.pop(topic, None)

    # ---- 统计 ----

    def topic_count(self) -> int:
        return len(self._topics)

    def subscription_count(self) -> int:
        return sum(len(s) for s in self._topic_subscribers.values())

    def connection_count(self) -> int:
        return len(self._identity_user)
