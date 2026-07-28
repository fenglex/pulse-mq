"""topic→订阅表，前缀匹配。只由 control 面驱动，数据面只读 match()。"""
from __future__ import annotations

import threading


class SubscriptionTable:
    def __init__(self) -> None:
        # identity -> set[pattern]（保留用于管理/快照）
        self._by_identity: dict[str, set[str]] = {}
        # 精确匹配索引：pattern -> set[identity]
        self._exact: dict[str, set[str]] = {}
        # 通配匹配索引：prefix -> set[identity]（pattern "foo.*" 的 prefix 为 "foo"）
        self._wild: dict[str, set[str]] = {}
        # 线程安全锁：数据面线程读 match()，控制面协程写 subscribe/unsubscribe/remove
        self._lock = threading.RLock()

    def subscribe(self, identity: str, topic_pattern: str) -> None:
        with self._lock:
            self._by_identity.setdefault(identity, set()).add(topic_pattern)
            if topic_pattern.endswith(".*"):
                prefix = topic_pattern[:-2]
                self._wild.setdefault(prefix, set()).add(identity)
            else:
                self._exact.setdefault(topic_pattern, set()).add(identity)

    def unsubscribe(self, identity: str, topic_pattern: str) -> None:
        with self._lock:
            pats = self._by_identity.get(identity)
            if pats:
                pats.discard(topic_pattern)
                if not pats:
                    self._by_identity.pop(identity, None)
            self._remove_from_index(identity, topic_pattern)

    def remove(self, identity: str) -> None:
        with self._lock:
            pats = self._by_identity.pop(identity, None)
            if pats:
                for pattern in pats:
                    self._remove_from_index(identity, pattern)

    def _remove_from_index(self, identity: str, pattern: str) -> None:
        """从精确/通配索引中移除 identity。"""
        if pattern.endswith(".*"):
            prefix = pattern[:-2]
            s = self._wild.get(prefix)
            if s:
                s.discard(identity)
                if not s:
                    self._wild.pop(prefix, None)
        else:
            s = self._exact.get(pattern)
            if s:
                s.discard(identity)
                if not s:
                    self._exact.pop(pattern, None)

    def match(self, topic: str) -> set[str]:
        with self._lock:
            # 精确匹配 O(1)
            matched: set[str] = set()
            matched.update(self._exact.get(topic, set()))
            # 通配匹配：检查 topic 本身及各级父前缀
            # "foo.*" 匹配 "foo" 和 "foo.<anything>"
            matched.update(self._wild.get(topic, set()))
            parts = topic.split(".")
            for i in range(len(parts) - 1, 0, -1):
                prefix = ".".join(parts[:i])
                matched.update(self._wild.get(prefix, set()))
            return matched

    @staticmethod
    def _matches(pattern: str, topic: str) -> bool:
        if pattern.endswith(".*"):
            prefix = pattern[:-2]
            return topic == prefix or topic.startswith(prefix + ".")
        return pattern == topic

    def subscribers_of(self, identity: str) -> set[str]:
        with self._lock:
            return set(self._by_identity.get(identity, set()))

    def snapshot(self) -> dict:
        with self._lock:
            # routing key 是 ROUTER bytes identity（server.py 用 ident 作为 key），
            # JSON 序列化需要 str 键。client_id 是 uuid-hex ASCII，decode 无损。
            return {
                (k.decode("utf-8", "replace") if isinstance(k, (bytes, bytearray)) else k):
                    sorted(v)
                for k, v in self._by_identity.items()
            }
