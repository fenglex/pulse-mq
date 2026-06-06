"""纯内存消息路由器。

包含四个子组件:
- TopicRegistry: 精确 topic 注册 + 通配符订阅展开
- SubscriptionManager: 订阅关系双向索引
- ConnectionManager: identity ↔ user 映射
- MessageBuffer: 每个 topic 的环形缓冲区
"""

from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field

from pulsemq.auth.permission import topic_match

from pulsemq.models import AuthUser, BufferedMessage, TopicInfo


@dataclass
class MessageRouter:
    """消息路由器（纯内存，单线程安全）。"""

    # Topic 注册表
    _topics: dict[str, TopicInfo] = field(default_factory=dict)

    # 通配符订阅列表（pattern → set of identity）
    _wildcard_subscriptions: dict[str, set[bytes]] = field(default_factory=dict)
    # identity → set of wildcard patterns
    _identity_wildcards: dict[bytes, set[str]] = field(default_factory=dict)

    # 通配符匹配缓存（topic → set of identity），订阅变更时失效
    _wildcard_cache: dict[str, set[bytes]] = field(default_factory=dict)
    _wildcard_cache_valid: bool = field(default=False)

    # 订阅关系双向索引
    _topic_subscribers: dict[str, set[bytes]] = field(default_factory=dict)
    _identity_subscriptions: dict[bytes, set[str]] = field(default_factory=dict)

    # 消息缓冲区
    _buffers: dict[str, deque] = field(default_factory=dict)
    _seq_counter: dict[str, int] = field(default_factory=dict)
    max_buffer_size: int = 1000
    buffer_enabled: bool = False  # #4 优化：默认关闭缓冲

    # 认证存储引用（用于连接计数）
    _auth_store: object = field(default=None, repr=False)

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

        # 通配符缓存失效
        self._wildcard_cache_valid = False

    def unsubscribe(self, identity: bytes, topic: str) -> None:
        """取消订阅（精确或通配符）。"""
        # 尝试精确取消
        subs = self._topic_subscriptions_remove(identity, topic)
        # 尝试通配符取消
        self._wildcard_unsubscribe(identity, topic)

    def subscribe_wildcard(self, identity: bytes, pattern: str) -> list[str]:
        """通配符订阅：匹配已有精确 topic 并展开。

        Returns:
            展开后匹配到的精确 topic 列表。
        """
        # 注册通配符
        if pattern not in self._wildcard_subscriptions:
            self._wildcard_subscriptions[pattern] = set()
        self._wildcard_subscriptions[pattern].add(identity)

        if identity not in self._identity_wildcards:
            self._identity_wildcards[identity] = set()
        self._identity_wildcards[identity].add(pattern)

        # 展开匹配已有精确 topic
        matched = []
        for name, info in self._topics.items():
            if not info.is_wildcard and topic_match(pattern, name):
                self.subscribe(identity, name)
                matched.append(name)
        return matched

    def _topic_subscriptions_remove(self, identity: bytes, topic: str) -> None:
        """精确订阅移除。"""
        subs = self._topic_subscribers.get(topic)
        if subs is not None:
            subs.discard(identity)
            info = self._topics.get(topic)
            if info is not None:
                info.subscriber_count = len(subs)

        id_subs = self._identity_subscriptions.get(identity)
        if id_subs is not None:
            id_subs.discard(topic)

        # 通配符缓存失效
        self._wildcard_cache_valid = False

    def _wildcard_unsubscribe(self, identity: bytes, pattern: str) -> None:
        """通配符订阅移除。"""
        wc_subs = self._wildcard_subscriptions.get(pattern)
        if wc_subs is not None:
            wc_subs.discard(identity)
            if not wc_subs:
                del self._wildcard_subscriptions[pattern]

        id_wc = self._identity_wildcards.get(identity)
        if id_wc is not None:
            id_wc.discard(pattern)

    def get_subscribers(self, topic: str) -> set[bytes]:
        """获取 topic 的所有订阅者（含通配符匹配，带缓存）。"""
        # 精确订阅始终返回
        result = set(self._topic_subscribers.get(topic, set()))

        # 通配符匹配使用缓存
        if self._wildcard_subscriptions:
            cached = self._wildcard_cache.get(topic)
            if cached is not None and self._wildcard_cache_valid:
                result.update(cached)
            else:
                # 重新计算并缓存
                wc_result: set[bytes] = set()
                for pattern, identities in self._wildcard_subscriptions.items():
                    if topic_match(pattern, topic):
                        wc_result.update(identities)
                self._wildcard_cache[topic] = wc_result
                result.update(wc_result)
        return result

    def has_subscribers(self, topic: str) -> bool:
        """轻量级检查 topic 是否有订阅者（不拷贝 set）。

        用于 PUB 热路径，避免 get_subscribers() 的 set 拷贝开销。
        """
        # 精确订阅快速检查
        subs = self._topic_subscribers.get(topic)
        if subs:
            return True
        # 通配符匹配
        if self._wildcard_subscriptions:
            cached = self._wildcard_cache.get(topic)
            if cached is not None and self._wildcard_cache_valid:
                return bool(cached)
            # 重新计算
            for pattern, identities in self._wildcard_subscriptions.items():
                if identities and topic_match(pattern, topic):
                    return True
        return False

    def get_subscriptions(self, identity: bytes) -> set[str]:
        """获取 identity 的所有订阅（精确 + 通配符）。"""
        result = set(self._identity_subscriptions.get(identity, set()))
        result.update(self._identity_wildcards.get(identity, set()))
        return result

    def remove_identity(self, identity: bytes) -> None:
        """移除 identity 的所有订阅关系（精确 + 通配符）。"""
        # 清理精确订阅
        id_subs = self._identity_subscriptions.pop(identity, set())
        for topic in id_subs:
            subs = self._topic_subscribers.get(topic)
            if subs is not None:
                subs.discard(identity)

        # 清理通配符订阅
        id_wc = self._identity_wildcards.pop(identity, set())
        for pattern in id_wc:
            wc_subs = self._wildcard_subscriptions.get(pattern)
            if wc_subs is not None:
                wc_subs.discard(identity)
                if not wc_subs:
                    del self._wildcard_subscriptions[pattern]

        # 通配符缓存失效
        if id_wc:
            self._wildcard_cache_valid = False
            self._wildcard_cache.clear()

    # ---- 连接管理（已委托给 AuthMemoryStore）----

    def register_connection(self, identity: bytes, user) -> None:
        """注册连接（委托给 auth_store 或仅维护计数）。"""
        if self._auth_store is not None:
            self._auth_store.register(identity, user)
        else:
            # 测试用兼容模式
            if not hasattr(self, '_test_connections'):
                self._test_connections = {}
                self._test_user_identities = {}
            self._test_connections[identity] = user
            uid = getattr(user, 'user_id', None)
            if uid is not None:
                if uid not in self._test_user_identities:
                    self._test_user_identities[uid] = set()
                self._test_user_identities[uid].add(identity)

    def unregister_connection(self, identity: bytes):
        """取消注册连接。"""
        if self._auth_store is not None:
            return self._auth_store.unregister(identity)
        # 测试用兼容模式
        if hasattr(self, '_test_connections'):
            user = self._test_connections.pop(identity, None)
            if user is not None:
                uid = getattr(user, 'user_id', None)
                if uid is not None and uid in self._test_user_identities:
                    self._test_user_identities[uid].discard(identity)
            return user
        return None

    def get_user(self, identity: bytes):
        """获取连接对应的用户。"""
        if self._auth_store is not None:
            return self._auth_store.get_user(identity)
        if hasattr(self, '_test_connections'):
            return self._test_connections.get(identity)
        return None

    def get_connections(self, user_id: int) -> set:
        """获取用户的所有连接。"""
        if self._auth_store is not None:
            return self._auth_store._user_identities.get(user_id, set())
        if hasattr(self, '_test_user_identities'):
            return self._test_user_identities.get(user_id, set())
        return set()

    def connection_count(self, auth_store=None) -> int:
        """获取连接数。"""
        store = auth_store or self._auth_store
        if store is not None:
            return len(store._identity_user)
        if hasattr(self, '_test_connections'):
            return len(self._test_connections)
        return 0

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
