"""topic->订阅表，前缀匹配。只由 control 面驱动，数据面只读 match()。

COW（copy-on-write）无锁读：写时在 ``_write_lock`` 内构建不可变 ``_Index``
快照并原子替换 ``_read_index`` 引用；数据面 ``match()`` 直接读引用，无锁。
GIL 保证引用赋值原子，数据面见到的快照永远一致。写频率极低（仅订阅变更），
拷贝成本可忽略。
"""
from __future__ import annotations

import threading
from dataclasses import dataclass


@dataclass(frozen=True)
class _Index:
    """不可变路由快照（COW 读路径直接持有引用）。"""
    exact: dict[str, frozenset[bytes]]
    wild: dict[str, frozenset[bytes]]
    by_identity: dict[bytes, frozenset[str]]


_EMPTY = frozenset()


class SubscriptionTable:
    """topic 前缀匹配路由表（COW 无锁读）。"""

    def __init__(self) -> None:
        self._read_index = _Index(exact={}, wild={}, by_identity={})
        self._write_lock = threading.Lock()
        # match 结果缓存：{topic: (version, frozenset[bytes])}。
        # 写操作（subscribe/unsubscribe/remove）递增 _version 并清空缓存；
        # match() 命中时校验 version 一致才返回，避免写后读到陈旧结果。
        # 读路径（数据面）无锁：GIL 保证 dict.get / int 读原子，version
        # 与 _read_index 在 _write_lock 内一起更新，读侧最差多算一次（无错误结果）。
        self._version: int = 0
        self._match_cache: dict[str, tuple[int, frozenset[bytes]]] = {}

    def match(self, topic: str) -> set[bytes]:
        """前缀匹配 -> identity 集合。无锁读（COW + 结果缓存）。

        热路径优化：同一 topic 反复 match 时（典型发布场景），跳过 split/join
        分配与多次 dict 查找，直接返回缓存的 frozenset。写操作通过 version
        验证使缓存失效，确保写后不会返回陈旧结果。
        """
        ver = self._version  # 原子读
        entry = self._match_cache.get(topic)
        if entry is not None and entry[0] == ver:
            return entry[1]  # 缓存命中
        idx = self._read_index  # 原子读引用，与 ver 同版本
        matched: set[bytes] = set(idx.exact.get(topic, _EMPTY))
        matched |= idx.wild.get(topic, _EMPTY)
        parts = topic.split(".")
        for i in range(len(parts) - 1, 0, -1):
            matched |= idx.wild.get(".".join(parts[:i]), _EMPTY)
        result = frozenset(matched)
        self._match_cache[topic] = (ver, result)
        return result

    def subscribe(self, identity: bytes, topic_pattern: str) -> None:
        with self._write_lock:
            base = self._read_index
            # 拷贝并更新（写频率低，拷贝成本可忽略）
            by_id = dict(base.by_identity)
            pats = set(by_id.get(identity, _EMPTY))
            pats.add(topic_pattern)
            by_id[identity] = frozenset(pats)
            exact = dict(base.exact)
            wild = dict(base.wild)
            if topic_pattern.endswith(".*"):
                prefix = topic_pattern[:-2]
                s = set(wild.get(prefix, _EMPTY)); s.add(identity)
                wild[prefix] = frozenset(s)
            else:
                s = set(exact.get(topic_pattern, _EMPTY)); s.add(identity)
                exact[topic_pattern] = frozenset(s)
            self._read_index = _Index(exact, wild, by_id)
            self._invalidate_cache()

    def unsubscribe(self, identity: bytes, topic_pattern: str) -> None:
        with self._write_lock:
            base = self._read_index
            by_id = dict(base.by_identity)
            pats = set(by_id.get(identity, _EMPTY))
            pats.discard(topic_pattern)
            if pats:
                by_id[identity] = frozenset(pats)
            else:
                by_id.pop(identity, None)
            exact = dict(base.exact); wild = dict(base.wild)
            if topic_pattern.endswith(".*"):
                prefix = topic_pattern[:-2]
                s = set(wild.get(prefix, _EMPTY)); s.discard(identity)
                if s:
                    wild[prefix] = frozenset(s)
                else:
                    wild.pop(prefix, None)
            else:
                s = set(exact.get(topic_pattern, _EMPTY)); s.discard(identity)
                if s:
                    exact[topic_pattern] = frozenset(s)
                else:
                    exact.pop(topic_pattern, None)
            self._read_index = _Index(exact, wild, by_id)
            self._invalidate_cache()

    def remove(self, identity: bytes) -> None:
        with self._write_lock:
            base = self._read_index
            by_id = dict(base.by_identity)
            pats = by_id.pop(identity, _EMPTY)
            exact = dict(base.exact); wild = dict(base.wild)
            for pattern in pats:
                if pattern.endswith(".*"):
                    prefix = pattern[:-2]
                    s = set(wild.get(prefix, _EMPTY)); s.discard(identity)
                    if s:
                        wild[prefix] = frozenset(s)
                    else:
                        wild.pop(prefix, None)
                else:
                    s = set(exact.get(pattern, _EMPTY)); s.discard(identity)
                    if s:
                        exact[pattern] = frozenset(s)
                    else:
                        exact.pop(pattern, None)
            self._read_index = _Index(exact, wild, by_id)
            self._invalidate_cache()

    def _invalidate_cache(self) -> None:
        """写后使 match 缓存失效（在 _write_lock 内调用）。"""
        self._version += 1
        self._match_cache.clear()

    def subscribers_of(self, identity: bytes) -> set[str]:
        """查某 identity 的订阅模式集合。"""
        return set(self._read_index.by_identity.get(identity, _EMPTY))

    @staticmethod
    def _matches(pattern: str, topic: str) -> bool:
        """单 pattern 匹配判定（供客户端等复用）。"""
        if pattern.endswith(".*"):
            prefix = pattern[:-2]
            return topic == prefix or topic.startswith(prefix + ".")
        return pattern == topic

    def snapshot(self) -> dict:
        """快照（bytes key decode 为 str，供 JSON 序列化）。"""
        idx = self._read_index
        return {
            (k.decode("utf-8", "replace") if isinstance(k, (bytes, bytearray)) else k):
                sorted(v) for k, v in idx.by_identity.items()
        }
