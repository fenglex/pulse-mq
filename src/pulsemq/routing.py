"""topic→订阅表，前缀匹配。只由 control 面驱动，数据面只读 match()。"""
from __future__ import annotations


class SubscriptionTable:
    def __init__(self) -> None:
        # identity -> set[pattern]
        self._by_identity: dict[str, set[str]] = {}

    def subscribe(self, identity: str, topic_pattern: str) -> None:
        self._by_identity.setdefault(identity, set()).add(topic_pattern)

    def unsubscribe(self, identity: str, topic_pattern: str) -> None:
        pats = self._by_identity.get(identity)
        if pats:
            pats.discard(topic_pattern)
            if not pats:
                self._by_identity.pop(identity, None)

    def remove(self, identity: str) -> None:
        self._by_identity.pop(identity, None)

    def match(self, topic: str) -> set[str]:
        matched: set[str] = set()
        for identity, patterns in self._by_identity.items():
            for p in patterns:
                if self._matches(p, topic):
                    matched.add(identity)
                    break
        return matched

    @staticmethod
    def _matches(pattern: str, topic: str) -> bool:
        if pattern.endswith(".*"):
            prefix = pattern[:-2]
            return topic == prefix or topic.startswith(prefix + ".")
        return pattern == topic

    def subscribers_of(self, identity: str) -> set[str]:
        return set(self._by_identity.get(identity, set()))

    def snapshot(self) -> dict:
        return {k: sorted(v) for k, v in self._by_identity.items()}
