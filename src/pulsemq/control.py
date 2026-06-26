"""控制面：命令集 + 在线用户表。不依赖 transport。"""
from __future__ import annotations

import time
from dataclasses import dataclass, field


class ControlCmd:
    REGISTER = "REGISTER"
    HEARTBEAT = "HEARTBEAT"
    SUBSCRIBE = "SUBSCRIBE"
    UNSUBSCRIBE = "UNSUBSCRIBE"
    DISCONNECT = "DISCONNECT"
    KICK = "KICK"


@dataclass
class ControlMessage:
    cmd: str
    payload: dict = field(default_factory=dict)


class RegisterResult:
    OK = "OK"
    ALREADY_ONLINE = "ALREADY_ONLINE"
    REJECTED = "REJECTED"


@dataclass
class ClientInfo:
    client_id: str
    username: str
    endpoint: str
    roles: list[str]
    topics: list[str]
    connected_at: float
    last_seen: float = 0.0


class OnlineRegistry:
    """在线用户表，key=username（单用户单在线）。"""

    def __init__(self, heartbeat_timeout: float = 6.0) -> None:
        self.heartbeat_timeout = heartbeat_timeout
        self._by_client: dict[str, ClientInfo] = {}
        self._by_user: dict[str, str] = {}  # username -> client_id

    def register(self, info: ClientInfo) -> str:
        if info.username in self._by_user:
            return RegisterResult.ALREADY_ONLINE
        if not info.last_seen:
            info.last_seen = time.time()
        self._by_client[info.client_id] = info
        self._by_user[info.username] = info.client_id
        return RegisterResult.OK

    def heartbeat(self, client_id: str) -> None:
        info = self._by_client.get(client_id)
        if info:
            info.last_seen = time.time()

    def unregister(self, client_id: str) -> None:
        info = self._by_client.pop(client_id, None)
        if info:
            self._by_user.pop(info.username, None)

    def sweep_timeout(self) -> list[ClientInfo]:
        now = time.time()
        offline = [c for c in self._by_client.values()
                   if now - c.last_seen > self.heartbeat_timeout]
        for c in offline:
            self.unregister(c.client_id)
        return offline

    def snapshot(self) -> dict:
        return {
            "clients": [
                {
                    "client_id": c.client_id, "username": c.username,
                    "endpoint": c.endpoint, "roles": list(c.roles),
                    "topics": list(c.topics), "connected_at": c.connected_at,
                    "last_seen": c.last_seen,
                }
                for c in self._by_client.values()
            ]
        }
