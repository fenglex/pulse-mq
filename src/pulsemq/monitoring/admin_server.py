"""后台管理 API + SSE 实时推送。

与 monitoring/api.py 中极简的 MetricsHTTPServer 不同, AdminServer 提供:
- 完整 REST 端点 (Topics / Clients / Users / Permissions / BatchConfig)
- SSE 实时指标推送
- 嵌入的 Web UI (单页应用)

实现:
- 复用 MetricsHTTPServer 的 stdlib asyncio HTTP 模式, 手写请求行/Header/Body 解析
- SSE 用每客户端一个 Queue + writer 协程, 客户端断开时清理
- 路由: 集中式 _route() 分发, 避免 8 段 if/elif 嵌套
- 静态资源: 全部从 web_ui.py 字符串常量加载
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections import deque
from typing import Any, Callable
from urllib.parse import parse_qs, urlparse

from pulsemq.auth.permission import PermissionService
from pulsemq.monitoring.client_tracker import ClientTracker
from pulsemq.monitoring.realtime import RealtimeMetrics, TopicMetricsRegistry
from pulsemq.monitoring.web_ui import INDEX_HTML
from pulsemq.storage.sqlite_stats import SQLiteStatsRepo
from pulsemq.storage.sqlite_user import SqliteUserRepo

logger = logging.getLogger(__name__)


# 启动时间: 进程级别常量, 由 PulseServer.start() 注入
SERVER_START_TIME: float = time.time()
SERVER_VERSION: str = "1.0.0"


def _user_to_dict(user) -> dict:
    """User dataclass -> JSON 友好的 dict。"""
    return {
        "id": user.id,
        "username": user.username,
        "api_key": user.api_key,
        "role": user.role,
        "namespace": user.namespace,
        "disabled": user.disabled,
        "max_connections": user.max_connections,
        "batch_size": user.batch_size,
        "batch_interval_ms": user.batch_interval_ms,
        "batch_max_wait_ms": user.batch_max_wait_ms,
        "created_at": user.created_at,
        "updated_at": user.updated_at,
    }


class AdminServer:
    """后台管理 HTTP 服务: REST + SSE + 静态 Web UI。

    端点分组:
      - GET  /                                  静态 HTML
      - GET  /api/v1/metrics/realtime           JSON
      - GET  /api/v1/metrics/snapshot           JSON
      - GET  /api/v1/metrics/stream             SSE (1s 一帧)
      - GET  /api/v1/topics                     JSON
      - GET  /api/v1/topics/{topic}             JSON
      - GET  /api/v1/topics/{topic}/history     JSON (SQLite)
      - GET  /api/v1/clients                    JSON
      - GET  /api/v1/clients/{identity}         JSON
      - GET  /api/v1/users                      JSON
      - POST /api/v1/users                      JSON 创建
      - GET  /api/v1/users/{user_id}            JSON
      - PUT  /api/v1/users/{user_id}            JSON 更新
      - DELETE /api/v1/users/{user_id}          JSON 删除
      - POST /api/v1/users/{user_id}/api_keys   JSON 重新生成
      - GET  /api/v1/permissions                JSON
      - POST /api/v1/permissions                JSON 授予
      - DELETE /api/v1/permissions              JSON 撤销 (query: group_id, topic_pattern, action)
      - GET  /api/v1/users/{user_id}/batch_config   JSON
      - PUT  /api/v1/users/{user_id}/batch_config   JSON
      - GET  /api/v1/system/status              JSON
    """

    def __init__(
        self,
        bind: str = "0.0.0.0:9090",
        client_tracker: ClientTracker | None = None,
        topic_metrics: TopicMetricsRegistry | None = None,
        realtime_metrics: RealtimeMetrics | None = None,
        stats_repo: SQLiteStatsRepo | None = None,
        user_repo: SqliteUserRepo | None = None,
        perm_service: PermissionService | None = None,
        perm_repo=None,
        snapshot_fn: Callable[[], dict] | None = None,
        start_time: float | None = None,
    ):
        host, port = bind.split(":")
        self._host = host
        self._port = int(port)
        self._client_tracker = client_tracker
        self._topic_metrics = topic_metrics
        self._realtime_metrics = realtime_metrics
        self._stats_repo = stats_repo
        self._user_repo = user_repo
        self._perm_service = perm_service
        self._perm_repo = perm_repo
        self._snapshot_fn = snapshot_fn
        self._start_time = start_time or SERVER_START_TIME
        self._server: asyncio.AbstractServer | None = None
        # SSE 客户端管理: 每个客户端一个 writer task + queue
        self._sse_clients: dict[int, tuple[asyncio.Queue, asyncio.Task]] = {}
        self._sse_clients_lock = asyncio.Lock()
        self._next_sse_id = 0
        # SSE 广播任务
        self._sse_broadcast_task: asyncio.Task | None = None

    # ---- 生命周期 ----

    async def start(self) -> None:
        self._server = await asyncio.start_server(
            self._handle_request, self._host, self._port
        )
        # 启动 SSE 广播
        self._sse_broadcast_task = asyncio.create_task(self._sse_broadcast_loop())
        logger.info("AdminServer 启动: http://%s:%d", self._host, self._port)

    async def stop(self) -> None:
        # 关闭所有 SSE 客户端
        async with self._sse_clients_lock:
            for _qid, (_q, task) in list(self._sse_clients.items()):
                task.cancel()
            self._sse_clients.clear()
        if self._sse_broadcast_task is not None:
            self._sse_broadcast_task.cancel()
            try:
                await self._sse_broadcast_task
            except (asyncio.CancelledError, Exception):
                pass
            self._sse_broadcast_task = None
        if self._server:
            self._server.close()
            await self._server.wait_closed()
            logger.info("AdminServer 已关闭")

    # ---- HTTP 解析 ----

    async def _handle_request(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            # 请求行
            request_line = await asyncio.wait_for(reader.readline(), timeout=5.0)
            if not request_line:
                return
            line = request_line.decode("utf-8", errors="ignore").strip()
            parts = line.split()
            if len(parts) < 2:
                await self._respond_json(writer, 400, {"error": "bad request"})
                return
            method = parts[0].upper()
            full_path = parts[1]

            # Headers
            headers: dict[str, str] = {}
            while True:
                hdr = await asyncio.wait_for(reader.readline(), timeout=5.0)
                if not hdr or hdr == b"\r\n" or hdr == b"\n":
                    break
                hdr_str = hdr.decode("utf-8", errors="ignore").strip()
                if ":" in hdr_str:
                    k, v = hdr_str.split(":", 1)
                    headers[k.strip().lower()] = v.strip()

            # Body (仅在 Content-Length 存在时读)
            body = b""
            cl = headers.get("content-length")
            if cl:
                try:
                    body = await asyncio.wait_for(
                        reader.readexactly(int(cl)), timeout=10.0
                    )
                except (asyncio.TimeoutError, asyncio.IncompleteReadError):
                    await self._respond_json(writer, 400, {"error": "body read failed"})
                    return

            # 解析 path + query
            parsed = urlparse(full_path)
            path = parsed.path
            query = parse_qs(parsed.query)

            # 路由
            await self._route(writer, method, path, query, body, headers)
        except asyncio.TimeoutError:
            pass
        except (ConnectionResetError, BrokenPipeError):
            pass
        except Exception as e:
            logger.debug("AdminServer 请求处理异常: %s", e)
            try:
                await self._respond_json(writer, 500, {"error": "internal error"})
            except Exception:
                pass
        finally:
            # SSE handler 接管 writer (置 _sse_takeover=True), 不会在此关闭
            if not getattr(writer, "_sse_takeover", False):
                try:
                    writer.close()
                    await writer.wait_closed()
                except Exception:
                    pass

    # ---- 路由分发 ----

    async def _route(
        self,
        writer: asyncio.StreamWriter,
        method: str,
        path: str,
        query: dict[str, list[str]],
        body: bytes,
        headers: dict[str, str],
    ) -> None:
        # 静态首页
        if method == "GET" and (path == "/" or path == "/index.html"):
            await self._respond_html(writer, 200, INDEX_HTML)
            return

        # 实时指标
        if method == "GET" and path == "/api/v1/metrics/realtime":
            await self._respond_json(writer, 200, self._realtime_snapshot())
            return

        if method == "GET" and path == "/api/v1/metrics/snapshot":
            await self._respond_json(writer, 200, self._full_snapshot())
            return

        if method == "GET" and path == "/api/v1/metrics/stream":
            # SSE: 不在 finally 关闭 writer, 由 SSE writer task 持有
            await self._handle_sse(writer, headers)
            return

        # Topics
        if method == "GET" and path == "/api/v1/topics":
            await self._respond_json(writer, 200, self._list_topics())
            return

        topic_match = self._match_topic_path(path)
        if topic_match is not None:
            sub_path, topic_name = topic_match
            await self._handle_topic(writer, method, sub_path, topic_name)
            return

        # Clients
        if method == "GET" and path == "/api/v1/clients":
            await self._respond_json(writer, 200, self._list_clients())
            return

        client_match = self._match_client_path(path)
        if client_match is not None:
            await self._handle_client(writer, method, client_match)
            return

        # Users
        if method == "GET" and path == "/api/v1/users":
            await self._respond_json(writer, 200, await self._list_users())
            return
        if method == "POST" and path == "/api/v1/users":
            await self._create_user(writer, body)
            return

        user_match = self._match_user_path(path, method, body)
        if user_match is not None:
            await self._handle_user(writer, method, user_match, body, query)
            return

        # Permissions
        if method == "GET" and path == "/api/v1/permissions":
            await self._respond_json(writer, 200, await self._list_permissions(query))
            return
        if method == "POST" and path == "/api/v1/permissions":
            await self._grant_permission(writer, body)
            return
        if method == "DELETE" and path == "/api/v1/permissions":
            await self._revoke_permission(writer, query)
            return

        # System status
        if method == "GET" and path == "/api/v1/system/status":
            await self._respond_json(writer, 200, self._system_status())
            return

        # healthz (复用)
        if method == "GET" and path == "/healthz":
            await self._respond_json(writer, 200, {"status": "ok"})
            return

        await self._respond_json(writer, 404, {"error": "not found", "path": path})

    # ---- 路径匹配辅助 ----

    @staticmethod
    def _match_topic_path(path: str) -> tuple[str, str] | None:
        """匹配 /api/v1/topics/{topic} 或 /api/v1/topics/{topic}/history"""
        prefix = "/api/v1/topics/"
        if not path.startswith(prefix):
            return None
        rest = path[len(prefix):]
        if not rest:
            return None
        parts = rest.split("/", 1)
        topic_name = parts[0]
        if not topic_name:
            return None
        if len(parts) == 1:
            return ("", topic_name)
        # /api/v1/topics/{topic}/history
        if parts[1] == "history":
            return ("history", topic_name)
        return None

    @staticmethod
    def _match_client_path(path: str) -> str | None:
        """匹配 /api/v1/clients/{identity_hex}"""
        prefix = "/api/v1/clients/"
        if not path.startswith(prefix):
            return None
        rest = path[len(prefix):]
        if not rest or "/" in rest:
            return None
        return rest

    @staticmethod
    def _match_user_path(path: str, method: str, body: bytes) -> str | None:
        """匹配 /api/v1/users/{user_id}, /api/v1/users/{user_id}/api_keys,
        /api/v1/users/{user_id}/batch_config"""
        prefix = "/api/v1/users/"
        if not path.startswith(prefix):
            return None
        rest = path[len(prefix):]
        if not rest:
            return None
        # /api/v1/users/{user_id}/api_keys -> "API_KEYS:{user_id}"
        # /api/v1/users/{user_id}/batch_config -> "BATCH:{user_id}"
        # /api/v1/users/{user_id} -> "USER:{user_id}"
        parts = rest.split("/")
        if len(parts) == 1:
            return f"USER:{parts[0]}"
        if len(parts) == 2:
            if parts[1] == "api_keys":
                return f"API_KEYS:{parts[0]}"
            if parts[1] == "batch_config":
                return f"BATCH:{parts[0]}"
        return None

    # ---- Handler: Topics ----

    def _list_topics(self) -> dict:
        if self._topic_metrics is None:
            return {"topic_count": 0, "topics": []}
        return self._topic_metrics.snapshot()

    def _realtime_snapshot(self) -> dict:
        """实时指标: RealtimeMetrics + TopicMetrics 合并."""
        snap: dict[str, Any] = {}
        if self._realtime_metrics is not None:
            snap = self._realtime_metrics.snapshot()
        if self._topic_metrics is not None:
            snap["topics"] = self._topic_metrics.snapshot()
        if self._client_tracker is not None:
            snap["clients_online"] = self._client_tracker.snapshot()["online_count"]
        snap["server_time"] = time.time()
        return snap

    def _full_snapshot(self) -> dict:
        """全量快照: metrics + clients + topics + system."""
        snap = self._realtime_snapshot()
        if self._client_tracker is not None:
            snap["clients"] = self._client_tracker.snapshot()
        snap["system"] = self._system_status()
        return snap

    async def _handle_topic(
        self, writer: asyncio.StreamWriter, method: str, sub_path: str, topic: str
    ) -> None:
        if method != "GET":
            await self._respond_json(writer, 405, {"error": "method not allowed"})
            return
        if sub_path == "":
            if self._topic_metrics is None:
                await self._respond_json(writer, 200, {"topic": topic, "exists": False})
                return
            m = self._topic_metrics.get(topic)
            await self._respond_json(writer, 200, {
                "topic": m.topic,
                "msg_count_1min": m.msg_count_1min,
                "msg_rate_1min": round(m.msg_rate_1min, 2),
                "latency_p50_1min": round(m.latency_p50_1min, 3),
                "latency_p99_1min": round(m.latency_p99_1min, 3),
                "latency_p999_1min": round(m.latency_p999_1min, 3),
                "latency_max_1min": round(m.latency_max_1min, 3),
                "in_flight": m.in_flight,
                "backpressure": m.backpressure,
                "last_msg_ts": m.last_msg_ts,
            })
            return
        if sub_path == "history":
            if self._stats_repo is None:
                await self._respond_json(writer, 200, {"topic": topic, "history": []})
                return
            # 默认查最近 60 分钟
            try:
                minutes = 60
            except (KeyError, ValueError):
                minutes = 60
            since_ts = int(time.time()) - minutes * 60
            rows = await self._stats_repo.get_topic_history(topic, since_ts)
            await self._respond_json(writer, 200, {
                "topic": topic,
                "minutes": minutes,
                "history": rows,
            })
            return
        await self._respond_json(writer, 404, {"error": "unknown sub-path"})

    # ---- Handler: Clients ----

    def _list_clients(self) -> dict:
        if self._client_tracker is None:
            return {"online_count": 0, "clients": []}
        return self._client_tracker.snapshot()

    async def _handle_client(
        self, writer: asyncio.StreamWriter, method: str, identity_hex: str
    ) -> None:
        if method != "GET":
            await self._respond_json(writer, 405, {"error": "method not allowed"})
            return
        if self._client_tracker is None:
            await self._respond_json(writer, 404, {"error": "client tracker not available"})
            return
        try:
            identity = bytes.fromhex(identity_hex)
        except ValueError:
            await self._respond_json(writer, 400, {"error": "identity must be hex"})
            return
        info = self._client_tracker.get(identity)
        if info is None:
            await self._respond_json(writer, 404, {"error": "client not found"})
            return
        await self._respond_json(writer, 200, {
            "identity": identity_hex,
            "user_id": info.user_id,
            "connected_at": info.connected_at,
            "last_heartbeat": info.last_heartbeat,
            "subscribed_topics": sorted(info.subscribed_topics),
            "msg_in_count": info.msg_in_count,
            "msg_out_count": info.msg_out_count,
            "msg_in_rate_1min": round(info.msg_in_rate_1min.value, 2),
            "msg_out_rate_1min": round(info.msg_out_rate_1min.value, 2),
        })

    # ---- Handler: Users ----

    async def _list_users(self) -> dict:
        if self._user_repo is None:
            return {"count": 0, "users": []}
        users = await self._user_repo.list_all()
        return {
            "count": len(users),
            "users": [_user_to_dict(u) for u in users],
        }

    async def _create_user(self, writer: asyncio.StreamWriter, body: bytes) -> None:
        if self._user_repo is None:
            await self._respond_json(writer, 503, {"error": "user repo not available"})
            return
        try:
            data = json.loads(body) if body else {}
        except json.JSONDecodeError:
            await self._respond_json(writer, 400, {"error": "invalid json"})
            return
        username = (data.get("username") or "").strip()
        if not username:
            await self._respond_json(writer, 400, {"error": "username is required"})
            return
        from pulsemq.storage.interfaces import User
        new_user = User(
            username=username,
            api_key=data.get("api_key") or f"pulse_sk_{username}_{int(time.time())}",
            role=data.get("role", "user"),
            namespace=data.get("namespace", ""),
            disabled=bool(data.get("disabled", False)),
            max_connections=int(data.get("max_connections", 10)),
            batch_size=int(data.get("batch_size", 100)),
            batch_interval_ms=int(data.get("batch_interval_ms", 50)),
            batch_max_wait_ms=int(data.get("batch_max_wait_ms", 200)),
        )
        try:
            created = await self._user_repo.create(new_user)
        except Exception as e:
            logger.debug("create_user 失败: %s", e)
            await self._respond_json(writer, 409, {"error": f"create failed: {e}"})
            return
        await self._respond_json(writer, 201, _user_to_dict(created))

    async def _handle_user(
        self,
        writer: asyncio.StreamWriter,
        method: str,
        route_key: str,
        body: bytes,
        query: dict[str, list[str]],
    ) -> None:
        kind, user_id_str = route_key.split(":", 1)
        try:
            user_id = int(user_id_str)
        except ValueError:
            await self._respond_json(writer, 400, {"error": "user_id must be int"})
            return

        if kind == "USER":
            await self._handle_user_crud(writer, method, user_id, body)
            return
        if kind == "API_KEYS":
            await self._handle_user_api_keys(writer, method, user_id)
            return
        if kind == "BATCH":
            await self._handle_user_batch(writer, method, user_id, body)
            return
        await self._respond_json(writer, 404, {"error": "unknown sub-route"})

    async def _handle_user_crud(
        self, writer: asyncio.StreamWriter, method: str, user_id: int, body: bytes
    ) -> None:
        if self._user_repo is None:
            await self._respond_json(writer, 503, {"error": "user repo not available"})
            return
        if method == "GET":
            user = await self._user_repo.get_by_id(user_id)
            if user is None:
                await self._respond_json(writer, 404, {"error": "user not found"})
                return
            await self._respond_json(writer, 200, _user_to_dict(user))
            return
        if method == "PUT":
            user = await self._user_repo.get_by_id(user_id)
            if user is None:
                await self._respond_json(writer, 404, {"error": "user not found"})
                return
            try:
                data = json.loads(body) if body else {}
            except json.JSONDecodeError:
                await self._respond_json(writer, 400, {"error": "invalid json"})
                return
            # 部分更新: 字段缺省保留原值
            if "username" in data:
                user.username = data["username"]
            if "api_key" in data:
                user.api_key = data["api_key"]
            if "role" in data:
                user.role = data["role"]
            if "namespace" in data:
                user.namespace = data["namespace"]
            if "disabled" in data:
                user.disabled = bool(data["disabled"])
            if "max_connections" in data:
                user.max_connections = int(data["max_connections"])
            if "batch_size" in data:
                user.batch_size = int(data["batch_size"])
            if "batch_interval_ms" in data:
                user.batch_interval_ms = int(data["batch_interval_ms"])
            if "batch_max_wait_ms" in data:
                user.batch_max_wait_ms = int(data["batch_max_wait_ms"])
            updated = await self._user_repo.update(user)
            await self._respond_json(writer, 200, _user_to_dict(updated))
            return
        if method == "DELETE":
            try:
                await self._user_repo.delete(user_id)
            except Exception as e:
                logger.debug("delete_user 失败: %s", e)
                await self._respond_json(writer, 404, {"error": f"delete failed: {e}"})
                return
            await self._respond_json(writer, 200, {"deleted": user_id})
            return
        await self._respond_json(writer, 405, {"error": "method not allowed"})

    async def _handle_user_api_keys(
        self, writer: asyncio.StreamWriter, method: str, user_id: int
    ) -> None:
        if method != "POST":
            await self._respond_json(writer, 405, {"error": "method not allowed"})
            return
        if self._user_repo is None:
            await self._respond_json(writer, 503, {"error": "user repo not available"})
            return
        user = await self._user_repo.get_by_id(user_id)
        if user is None:
            await self._respond_json(writer, 404, {"error": "user not found"})
            return
        new_key = f"pulse_sk_{user.username}_{int(time.time() * 1000)}"
        user.api_key = new_key
        updated = await self._user_repo.update(user)
        await self._respond_json(writer, 200, {
            "user_id": updated.id,
            "api_key": updated.api_key,
        })

    async def _handle_user_batch(
        self,
        writer: asyncio.StreamWriter,
        method: str,
        user_id: int,
        body: bytes,
    ) -> None:
        if self._perm_service is None:
            await self._respond_json(writer, 503, {"error": "perm service not available"})
            return
        if method == "GET":
            try:
                cfg = await self._perm_service.get_batch_config(user_id)
            except LookupError as e:
                await self._respond_json(writer, 404, {"error": str(e)})
                return
            except Exception as e:
                await self._respond_json(writer, 500, {"error": str(e)})
                return
            await self._respond_json(writer, 200, {"user_id": user_id, **cfg})
            return
        if method == "PUT":
            try:
                data = json.loads(body) if body else {}
            except json.JSONDecodeError:
                await self._respond_json(writer, 400, {"error": "invalid json"})
                return
            try:
                await self._perm_service.set_batch_config(
                    user_id=user_id,
                    batch_size=int(data.get("batch_size", 100)),
                    batch_interval_ms=int(data.get("batch_interval_ms", 50)),
                    batch_max_wait_ms=int(data.get("batch_max_wait_ms", 200)),
                )
            except LookupError as e:
                await self._respond_json(writer, 404, {"error": str(e)})
                return
            except ValueError as e:
                await self._respond_json(writer, 400, {"error": str(e)})
                return
            await self._respond_json(writer, 200, {"updated": user_id})
            return
        await self._respond_json(writer, 405, {"error": "method not allowed"})

    # ---- Handler: Permissions ----

    async def _list_permissions(self, query: dict[str, list[str]]) -> dict:
        """支持 query: user_id 过滤单个用户."""
        if self._perm_service is None or self._perm_repo is None:
            return {"count": 0, "permissions": []}
        if "user_id" in query:
            try:
                uid = int(query["user_id"][0])
            except (ValueError, IndexError):
                return {"count": 0, "permissions": []}
            perms = await self._perm_service.list_user_permissions(uid)
            return {"count": len(perms), "user_id": uid, "permissions": perms}
        # 全量: 遍历所有用户 (简陋, 用户量小时可接受)
        if self._user_repo is None:
            return {"count": 0, "permissions": []}
        users = await self._user_repo.list_all()
        all_perms: list[dict] = []
        for u in users:
            if u.id is None:
                continue
            perms = await self._perm_service.list_user_permissions(u.id)
            for p in perms:
                all_perms.append({"user_id": u.id, "username": u.username, **p})
        return {"count": len(all_perms), "permissions": all_perms}

    async def _grant_permission(
        self, writer: asyncio.StreamWriter, body: bytes
    ) -> None:
        if self._perm_service is None or self._perm_repo is None:
            await self._respond_json(writer, 503, {"error": "perm service not available"})
            return
        try:
            data = json.loads(body) if body else {}
        except json.JSONDecodeError:
            await self._respond_json(writer, 400, {"error": "invalid json"})
            return
        user_id = data.get("user_id")
        topic_pattern = (data.get("topic_pattern") or "").strip()
        action = (data.get("action") or "").strip()
        if user_id is None or not topic_pattern or action not in ("pub", "sub", "query"):
            await self._respond_json(
                writer, 400,
                {"error": "user_id, topic_pattern, action (pub|sub|query) required"},
            )
            return
        try:
            uid = int(user_id)
        except (ValueError, TypeError):
            await self._respond_json(writer, 400, {"error": "user_id must be int"})
            return
        try:
            if action == "pub":
                await self._perm_service.grant_pub(uid, topic_pattern)
            elif action == "sub":
                await self._perm_service.grant_sub(uid, topic_pattern)
            else:
                # query: 直接写到用户的第一个 group (保持兼容)
                groups = await self._perm_repo.get_user_groups(uid)
                if not groups or groups[0].id is None:
                    await self._respond_json(writer, 400, {"error": "user has no group"})
                    return
                await self._perm_repo.add_permission(groups[0].id, topic_pattern, action)
                self._perm_service.invalidate_user(uid)
        except Exception as e:
            logger.debug("grant_permission 失败: %s", e)
            await self._respond_json(writer, 500, {"error": str(e)})
            return
        await self._respond_json(writer, 201, {
            "granted": {"user_id": uid, "topic_pattern": topic_pattern, "action": action},
        })

    async def _revoke_permission(
        self, writer: asyncio.StreamWriter, query: dict[str, list[str]]
    ) -> None:
        if self._perm_service is None or self._perm_repo is None:
            await self._respond_json(writer, 503, {"error": "perm service not available"})
            return
        try:
            uid = int(query.get("user_id", [""])[0])
            topic_pattern = query.get("topic_pattern", [""])[0]
            action = query.get("action", [""])[0]
        except (ValueError, IndexError):
            await self._respond_json(
                writer, 400,
                {"error": "user_id, topic_pattern, action required"},
            )
            return
        if not uid or not topic_pattern or action not in ("pub", "sub", "query"):
            await self._respond_json(
                writer, 400,
                {"error": "user_id, topic_pattern, action (pub|sub|query) required"},
            )
            return
        try:
            if action == "pub":
                await self._perm_service.revoke_pub(uid, topic_pattern)
            elif action == "sub":
                await self._perm_service.revoke_sub(uid, topic_pattern)
            else:
                groups = await self._perm_repo.get_user_groups(uid)
                for g in groups:
                    if g.id is None:
                        continue
                    await self._perm_repo.remove_permission(g.id, topic_pattern, action)
                self._perm_service.invalidate_user(uid)
        except Exception as e:
            logger.debug("revoke_permission 失败: %s", e)
            await self._respond_json(writer, 500, {"error": str(e)})
            return
        await self._respond_json(writer, 200, {
            "revoked": {"user_id": uid, "topic_pattern": topic_pattern, "action": action},
        })

    # ---- Handler: System ----

    def _system_status(self) -> dict:
        return {
            "version": SERVER_VERSION,
            "start_time": self._start_time,
            "uptime_seconds": round(time.time() - self._start_time, 2),
            "server_time": time.time(),
        }

    # ---- SSE ----

    async def _handle_sse(
        self, writer: asyncio.StreamWriter, headers: dict[str, str]
    ) -> None:
        """SSE: 注册客户端, 启动 writer 协程, 由协程负责写数据 + 关连接."""
        # 标记 writer 已被 SSE 接管, _handle_request 的 finally 不应再关闭
        writer._sse_takeover = True  # type: ignore[attr-defined]
        # 设置响应头 (Connection: keep-alive, 由 writer 协程关闭)
        header = (
            "HTTP/1.1 200 OK\r\n"
            "Content-Type: text/event-stream; charset=utf-8\r\n"
            "Cache-Control: no-cache\r\n"
            "Connection: keep-alive\r\n"
            "X-Accel-Buffering: no\r\n"
            "\r\n"
        )
        try:
            writer.write(header.encode("utf-8"))
            await writer.drain()
        except (ConnectionResetError, BrokenPipeError):
            return

        # 发送注释行触发浏览器 EventSource onopen
        try:
            writer.write(b": connected\n\n")
            await writer.drain()
        except (ConnectionResetError, BrokenPipeError):
            return

        # 注册
        async with self._sse_clients_lock:
            cid = self._next_sse_id
            self._next_sse_id += 1
        queue: asyncio.Queue = asyncio.Queue(maxsize=64)
        task = asyncio.create_task(self._sse_writer(writer, cid, queue))
        async with self._sse_clients_lock:
            self._sse_clients[cid] = (queue, task)
        logger.debug("SSE 客户端连接: id=%d", cid)

    async def _sse_writer(
        self, writer: asyncio.StreamWriter, cid: int, queue: asyncio.Queue
    ) -> None:
        """单客户端 writer: 阻塞等队列, 写数据, 失败/结束时清理."""
        try:
            while True:
                try:
                    payload = await queue.get()
                except asyncio.CancelledError:
                    break
                if payload is None:
                    break
                try:
                    writer.write(payload)
                    await writer.drain()
                except (ConnectionResetError, BrokenPipeError, ConnectionAbortedError):
                    break
        except Exception as e:
            logger.debug("SSE writer id=%d 异常: %s", cid, e)
        finally:
            async with self._sse_clients_lock:
                self._sse_clients.pop(cid, None)
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
            logger.debug("SSE 客户端断开: id=%d", cid)

    async def _sse_broadcast_loop(self) -> None:
        """每 1s 拉一次快照, 推给所有在线 SSE 客户端."""
        while True:
            try:
                payload = self._realtime_snapshot()
                data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
                frame = f"data: {data.decode('utf-8')}\n\n".encode("utf-8")
                async with self._sse_clients_lock:
                    clients = list(self._sse_clients.items())
                for _cid, (q, _task) in clients:
                    # 非阻塞: 队列满则丢弃
                    try:
                        q.put_nowait(frame)
                    except asyncio.QueueFull:
                        pass
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.debug("SSE 广播异常: %s", e)
            await asyncio.sleep(1.0)

    # ---- 响应辅助 ----

    async def _respond_json(
        self, writer: asyncio.StreamWriter, status: int, data: dict
    ) -> None:
        try:
            body = json.dumps(data, ensure_ascii=False, indent=2)
        except (TypeError, ValueError):
            body = json.dumps({"error": "encode failed"})
            status = 500
        status_text = {
            200: "OK", 201: "Created", 400: "Bad Request",
            404: "Not Found", 405: "Method Not Allowed",
            409: "Conflict", 500: "Internal Server Error", 503: "Service Unavailable",
        }.get(status, "OK")
        response = (
            f"HTTP/1.1 {status} {status_text}\r\n"
            f"Content-Type: application/json; charset=utf-8\r\n"
            f"Content-Length: {len(body.encode('utf-8'))}\r\n"
            f"Connection: close\r\n"
            f"\r\n"
            f"{body}"
        )
        try:
            writer.write(response.encode("utf-8"))
            await writer.drain()
        except (ConnectionResetError, BrokenPipeError):
            pass

    async def _respond_html(
        self, writer: asyncio.StreamWriter, status: int, html: str
    ) -> None:
        body = html.encode("utf-8")
        status_text = "OK" if status == 200 else "Not Found"
        response = (
            f"HTTP/1.1 {status} {status_text}\r\n"
            f"Content-Type: text/html; charset=utf-8\r\n"
            f"Content-Length: {len(body)}\r\n"
            f"Connection: close\r\n"
            f"\r\n"
        ).encode("utf-8") + body
        try:
            writer.write(response)
            await writer.drain()
        except (ConnectionResetError, BrokenPipeError):
            pass
