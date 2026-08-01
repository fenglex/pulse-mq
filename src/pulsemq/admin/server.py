"""AdminServer: HTTP + SSE + REST API。

stdlib asyncio HTTP，手写请求解析，不引入框架。
"""

from __future__ import annotations

import asyncio
import json
import mimetypes
import threading
import time
from pathlib import Path
from typing import Any, Callable
from urllib.parse import parse_qs, urlparse

from loguru import logger

from pulsemq._version import __version__ as _PKG_VERSION
from pulsemq.admin.auth import TokenAuth
from pulsemq.admin.web_ui import INDEX_HTML
from pulsemq.stats.storage import StatsStorage
from pulsemq.stats.traffic import TrafficStats

# 版本号：从 pulsemq._version 统一读取，避免与包版本脱节
SERVER_VERSION: str = _PKG_VERSION

# HTTP 状态码 → 状态文本
_STATUS_TEXT: dict[int, str] = {
    200: "OK",
    400: "Bad Request",
    401: "Unauthorized",
    404: "Not Found",
    500: "Internal Server Error",
}

# 静态资源根目录（与本文件同级的 static/）
STATIC_ROOT: Path = Path(__file__).resolve().parent / "static"


def _iso(ts: float) -> str:
    """Unix 时间戳 → ISO8601 UTC 字符串（空/0 → 空串）。"""
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts)) if ts else ""


class AdminServer:
    """后台管理 HTTP 服务: REST + SSE + Web UI。

    端点:
      GET  /                              深色 Web UI 首页
      GET  /static/{path}                 静态资源（ECharts 等）
      GET  /api/v1/stats/realtime         实时指标 JSON
      GET  /api/v1/stats/stream           SSE 实时推送（1s 一帧）
      GET  /api/v1/topics                 所有 topic 列表 + 当前指标
      GET  /api/v1/topics/{topic}/history 分钟级历史（最近 N 分钟）
      GET  /api/v1/system/status          系统状态（uptime, version）
      GET  /healthz                       健康检查
    """

    def __init__(
        self,
        bind: str = "0.0.0.0:9090",
        traffic_stats: TrafficStats | None = None,
        stats_storage: StatsStorage | None = None,
        snapshot_fn: Callable[[], dict] | None = None,
        start_time: float | None = None,
        token_auth: TokenAuth | None = None,
        *,
        connection_stats=None,
        latency_stats=None,
        latency_e2e_stats=None,
        drop_stats=None,
        admin_thread: bool = True,
    ) -> None:
        host, port = bind.split(":")
        self._host = host
        self._port = int(port)
        self._traffic = traffic_stats
        self._storage = stats_storage
        self._snapshot_fn = snapshot_fn
        self._start_time = start_time or time.time()
        self._token_auth = token_auth
        # Spec 3 监控扩展：连接/延迟统计 + 独立线程模式
        self._connections = connection_stats
        self._latency = latency_stats
        self._latency_e2e = latency_e2e_stats
        self._drop_stats = drop_stats
        self._admin_thread = admin_thread
        self._server: asyncio.AbstractServer | None = None
        # SSE 客户端
        self._sse_clients: dict[int, tuple[asyncio.Queue, asyncio.Task]] = {}
        self._sse_id = 0
        self._sse_task: asyncio.Task | None = None
        # 独立线程生命周期（admin_thread=True 模式）
        self._thread: threading.Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread_started = threading.Event()

    # ---- 生命周期 ----

    async def start(self) -> None:
        """启动 AdminServer。

        - admin_thread=True（默认）：在独立 daemon 线程 + 独立 asyncio loop 上
          运行 HTTP/SSE 服务，使 HTTP 请求不会阻塞 ZMQ 数据线程。`start()` 阻塞
          至线程内 server 就绪（`_thread_started` 被置位）后返回。
        - admin_thread=False：直接在调用方 loop 上 `await self._serve()`。
        """
        if self._admin_thread:
            self._thread = threading.Thread(
                target=self._run_thread, daemon=True, name="pulsemq-admin"
            )
            self._thread.start()
            # 等待线程内 server 就绪，使调用方可立即访问端口
            self._thread_started.wait(timeout=5.0)
        else:
            await self._serve()

    def _run_thread(self) -> None:
        """独立线程入口：自建 asyncio loop 并运行 _serve()。"""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._loop = loop
        try:
            loop.run_until_complete(self._serve())
        except (asyncio.CancelledError, Exception):
            # stop() 取消 serve_forever 会抛 CancelledError，属正常关闭路径，吞掉。
            logger.debug("AdminServer 线程退出")
        finally:
            # 取消所有未完成任务，避免 "coroutine was never awaited" RuntimeWarning。
            # _stop_serve() 可能已通过 run_coroutine_threadsafe 提交但 loop 在
            # serve_forever() 返回后立即关闭，导致协程未被执行。
            try:
                pending = asyncio.all_tasks(loop)
                for task in pending:
                    task.cancel()
                if pending:
                    loop.run_until_complete(
                        asyncio.gather(*pending, return_exceptions=True))
            except Exception:
                pass
            try:
                loop.close()
            except Exception:
                pass

    async def _serve(self) -> None:
        """实际建立 HTTP server + SSE 广播任务。

        - admin_thread=True（独立线程）：建立后 `serve_forever()` 阻塞，使线程 loop
          持续运行直至 `_stop_serve()` 关闭 server。
        - admin_thread=False（内联）：仅建立 server + SSE 任务后返回（沿用 Spec 1 行为，
          由调用方 loop 在后台驱动连接处理）。
        """
        self._server = await asyncio.start_server(
            self._handle_request, self._host, self._port
        )
        self._sse_task = asyncio.create_task(self._sse_broadcast_loop())
        # 通知等待方 server 已就绪（独立线程模式下尤为关键）
        self._thread_started.set()
        logger.info("AdminServer 启动: http://{}:{}", self._host, self._port)
        # token 启用时，额外打一条带 token 的可点击 URL，方便直接进监控面板。
        # host 为 0.0.0.0（监听所有网卡）时显示 localhost 以便浏览器访问。
        if self._token_auth is not None and self._token_auth.enabled:
            display_host = "localhost" if self._host in ("0.0.0.0", "::") else self._host
            logger.info("AdminServer 监控面板: http://{}:{}/?token={}",
                        display_host, self._port, self._token_auth.token)
        if self._admin_thread:
            async with self._server:
                await self._server.serve_forever()

    async def stop(self) -> None:
        """停止 AdminServer。

        独立线程模式下：通过 ``call_soon_threadsafe`` 在 admin loop 上直接关闭
        server（同步操作，不创建协程），再 join 线程。避免 ``run_coroutine_threadsafe``
        提交的协程在 loop 关闭前未被调度执行，导致 "coroutine was never awaited"
        RuntimeWarning。
        """
        if self._admin_thread and self._loop is not None:
            # 同步回调：关闭 HTTP server + 取消 SSE 任务。
            # 不创建协程，避免 loop 关闭前协程未被执行的 RuntimeWarning。
            def _do_stop() -> None:
                if self._sse_task is not None:
                    self._sse_task.cancel()
                for _qid, (_q, task) in list(self._sse_clients.items()):
                    task.cancel()
                self._sse_clients.clear()
                if self._server:
                    self._server.close()
            self._loop.call_soon_threadsafe(_do_stop)
            if self._thread:
                self._thread.join(timeout=5.0)
        else:
            await self._stop_serve()

    async def _stop_serve(self) -> None:
        """关闭 SSE 客户端 + 广播任务 + HTTP server（原 stop 逻辑）。"""
        for _qid, (_q, task) in list(self._sse_clients.items()):
            task.cancel()
        self._sse_clients.clear()
        if self._sse_task is not None:
            self._sse_task.cancel()
            try:
                await self._sse_task
            except (asyncio.CancelledError, Exception):
                pass
        if self._server:
            self._server.close()
            await self._server.wait_closed()

    # ---- HTTP 解析 ----

    async def _handle_request(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            request_line = await asyncio.wait_for(reader.readline(), timeout=5.0)
            if not request_line:
                return
            parts = request_line.decode("utf-8", errors="ignore").strip().split()
            if len(parts) < 2:
                await self._respond_json(writer, 400, {"error": "bad request"})
                return
            method = parts[0].upper()
            full_path = parts[1]

            headers: dict[str, str] = {}
            while True:
                hdr = await asyncio.wait_for(reader.readline(), timeout=5.0)
                if not hdr or hdr == b"\r\n" or hdr == b"\n":
                    break
                hdr_str = hdr.decode("utf-8", errors="ignore").strip()
                if ":" in hdr_str:
                    k, v = hdr_str.split(":", 1)
                    headers[k.strip().lower()] = v.strip()

            body = b""
            cl = headers.get("content-length")
            if cl:
                try:
                    body = await asyncio.wait_for(reader.readexactly(int(cl)), timeout=10.0)
                except (asyncio.TimeoutError, asyncio.IncompleteReadError):
                    await self._respond_json(writer, 400, {"error": "body read failed"})
                    return

            parsed = urlparse(full_path)
            path = parsed.path
            query = parse_qs(parsed.query)
            # token 认证（除 /healthz）
            if self._token_auth is not None and self._token_auth.enabled and path != "/healthz":
                if not self._token_auth.validate(headers, query):
                    await self._respond_json(writer, 401, {"error": "unauthorized"})
                    return
            await self._route(writer, method, path, query)
        except asyncio.TimeoutError:
            pass
        except (ConnectionResetError, BrokenPipeError):
            pass
        except Exception:
            logger.debug("请求处理异常 path={} locals={}", locals().get("path", "?"), exc_info=True)
        finally:
            if not getattr(writer, "_sse_takeover", False):
                try:
                    writer.close()
                    await writer.wait_closed()
                except Exception:
                    pass

    # ---- 路由 ----

    async def _route(
        self,
        writer: asyncio.StreamWriter,
        method: str,
        path: str,
        query: dict[str, list[str]],
    ) -> None:
        if method == "GET" and path in ("/", "/index.html"):
            await self._respond_html(writer, 200, INDEX_HTML)
            return

        if method == "GET" and path == "/api/v1/stats/realtime":
            await self._respond_json(writer, 200, self._realtime_snapshot())
            return

        if method == "GET" and path == "/api/v1/stats/stream":
            await self._handle_sse(writer)
            return

        if method == "GET" and path == "/api/v1/clients":
            await self._respond_json(writer, 200, self._clients_snapshot())
            return

        if method == "GET" and path == "/api/v1/events":
            limit = 50
            try:
                limit = int(query.get("limit", ["50"])[0])
            except (ValueError, IndexError):
                pass
            await self._respond_json(writer, 200, self._events_snapshot(limit))
            return

        if method == "GET" and path == "/api/v1/topics":
            await self._respond_json(writer, 200, self._list_topics())
            return

        # /api/v1/topics/{topic}/history
        prefix = "/api/v1/topics/"
        if method == "GET" and path.startswith(prefix):
            rest = path[len(prefix):]
            if rest:
                parts = rest.split("/", 1)
                topic = parts[0]
                if len(parts) == 2 and parts[1] == "history":
                    minutes = 60
                    try:
                        minutes = int(query.get("minutes", ["60"])[0])
                    except (ValueError, IndexError):
                        pass
                    await self._respond_json(writer, 200, self._topic_history(topic, minutes))
                    return

        # /api/v1/latency/topics/{topic}/history?minutes=60&kind=half|e2e
        lat_prefix = "/api/v1/latency/topics/"
        if method == "GET" and path.startswith(lat_prefix):
            rest = path[len(lat_prefix):]
            if rest:
                parts = rest.split("/", 1)
                topic = parts[0]
                if len(parts) == 2 and parts[1] == "history":
                    minutes = 60
                    try:
                        minutes = int(query.get("minutes", ["60"])[0])
                    except (ValueError, IndexError):
                        pass
                    kind = query.get("kind", ["half"])[0]
                    reg = self._latency_e2e if kind == "e2e" else self._latency
                    if reg is not None:
                        await self._respond_json(writer, 200, reg.get_history(topic, minutes))
                    else:
                        await self._respond_json(writer, 200, [])
                    return

        if method == "GET" and path == "/api/v1/system/status":
            await self._respond_json(writer, 200, self._system_status())
            return

        if method == "GET" and path == "/healthz":
            await self._respond_json(writer, 200, {"status": "ok"})
            return

        if method == "GET" and path.startswith("/static/"):
            await self._route_static(writer, path)
            return

        await self._respond_json(writer, 404, {"error": "not found"})

    # ---- 数据方法 ----

    def _realtime_snapshot(self) -> dict:
        """实时指标快照。"""
        snap: dict[str, Any] = {}
        if self._traffic is not None:
            snap["topics"] = self._traffic.all_topics_snapshot()
        if self._snapshot_fn is not None:
            snap.update(self._snapshot_fn())
        # 延迟快照（按 topic，LatencyStatsRegistry.snapshot()）
        if self._latency is not None:
            snap["latency_half"] = self._latency.snapshot()
        if self._latency_e2e is not None:
            snap["latency_e2e"] = self._latency_e2e.snapshot()
        # 消费端丢弃统计（来自心跳聚合）
        if self._drop_stats is not None:
            snap["drops"] = self._drop_stats.snapshot()
        # Spec 3 监控扩展：在线 client 计数（online_users/producers/consumers/...）
        if self._connections is not None:
            snap.update(self._connections.counters())
            # 最近 10 条生命周期事件（SSE 推送，JS 每帧替换 state 而非增量追加）。
            snap["sse_events"] = [
                {"ts": e.ts, "type": e.type, "level": e.level, "message": e.message}
                for e in self._connections.recent_events(10)
            ]
        snap["server_time"] = time.time()
        # start_time：供前端 SSE 实时计算 uptime = server_time - start_time。
        # 若缺失，前端 uptime 卡片只能靠页面加载时一次性 fetch /system/status，
        # 之后不再增长（会冻结在一个数）。
        if self._start_time:
            snap["start_time"] = self._start_time
        return snap

    def _clients_snapshot(self) -> dict:
        """在线 client 明细（跨线程只读快照；connection_stats 为 None 时返回空）。"""
        if self._connections is None:
            return {"clients": []}
        clients = []
        for c in self._connections.online_clients():
            clients.append({
                "client_id": c.client_id,
                "username": c.username,
                "role": c.role,
                "endpoint": c.endpoint,
                "topics": list(c.topics),
                "connected_at_iso": _iso(c.connected_at),
                "duration_seconds": round(c.duration_seconds, 1),
            })
        return {"clients": clients}

    def _events_snapshot(self, limit: int) -> dict:
        """生命周期事件（最近 N 条；connection_stats 为 None 时返回空）。"""
        if self._connections is None:
            return {"events": []}
        events = []
        for e in self._connections.recent_events(limit):
            events.append({
                "ts_iso": _iso(e.ts),
                "level": e.level,
                "type": e.type,
                "message": e.message,
            })
        return {"events": events}

    def _list_topics(self) -> dict:
        """所有 topic 列表 + 指标。"""
        if self._traffic is None:
            return {"topic_count": 0, "topics": []}
        all_data = self._traffic.all_topics_snapshot()
        topics = []
        for topic, data in all_data.items():
            topics.append({
                "topic": topic,
                "msg_rate_1min": data["msg_rate_1min"],
                "msg_count_current": data["msg_count_current"],
                "record_count_current": data["record_count_current"],
                "bytes_total_current": data["bytes_total_current"],
            })
        return {"topic_count": len(topics), "topics": topics}

    def _topic_history(self, topic: str, minutes: int) -> dict:
        """分钟级历史（内存 + SQLite 合并，timestamp 去重）。"""
        # 内存数据优先
        mem_history: list[dict] = []
        if self._traffic is not None:
            mem_history = self._traffic.get_history(topic, minutes)

        if mem_history and len(mem_history) >= minutes - 1:
            # 内存数据已覆盖请求范围（当前正在累积的分钟尚未归档进 slots，
            # 故 slots 内最多 minutes-1 条），直接返回
            return {"topic": topic, "minutes": minutes, "history": mem_history}

        # SQLite 补充更早的数据
        db_history: list[dict] = []
        if self._storage is not None:
            since_ts = int(time.time()) - minutes * 60
            db_history = self._storage.load_history(topic, since_ts)

        if not mem_history and not db_history:
            return {"topic": topic, "minutes": minutes, "history": []}

        # 合并去重：内存优先（更准确），SQLite 按时间戳去重
        seen: set[int] = set()
        merged: list[dict] = []

        for item in mem_history:
            ts = item.get("timestamp", 0)
            if ts not in seen:
                seen.add(ts)
                merged.append(item)

        for item in db_history:
            ts = item.get("timestamp", 0)
            if ts not in seen:
                seen.add(ts)
                merged.append(item)

        # 按 timestamp 排序
        merged.sort(key=lambda x: x.get("timestamp", 0))

        return {"topic": topic, "minutes": minutes, "history": merged}

    def _system_status(self) -> dict:
        return {
            "version": SERVER_VERSION,
            "start_time": self._start_time,
            "uptime_seconds": round(time.time() - self._start_time, 2),
        }

    # ---- SSE ----

    async def _handle_sse(self, writer: asyncio.StreamWriter) -> None:
        writer._sse_takeover = True  # type: ignore[attr-defined]
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
            writer.write(b": connected\n\n")
            await writer.drain()
        except (ConnectionResetError, BrokenPipeError):
            return

        self._sse_id += 1
        cid = self._sse_id
        queue: asyncio.Queue = asyncio.Queue(maxsize=64)
        task = asyncio.create_task(self._sse_writer(writer, cid, queue))
        self._sse_clients[cid] = (queue, task)

    async def _sse_writer(
        self, writer: asyncio.StreamWriter, cid: int, queue: asyncio.Queue
    ) -> None:
        try:
            while True:
                payload = await queue.get()
                if payload is None:
                    break
                writer.write(payload)
                await writer.drain()
        except (ConnectionResetError, BrokenPipeError, asyncio.CancelledError):
            pass
        finally:
            self._sse_clients.pop(cid, None)
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    async def _sse_broadcast_loop(self) -> None:
        while True:
            try:
                data = self._realtime_snapshot()
                frame = f"data: {json.dumps(data, ensure_ascii=False)}\n\n".encode("utf-8")
                for cid, (q, task) in list(self._sse_clients.items()):
                    try:
                        q.put_nowait(frame)
                    except asyncio.QueueFull:
                        # 队列堆积（客户端断开或消费过慢）：主动取消该连接，
                        # 避免死客户端在字典中残留造成内存泄漏。
                        task.cancel()
                        self._sse_clients.pop(cid, None)
                        logger.debug("SSE 客户端 {} 队列满，已断开", cid)
            except asyncio.CancelledError:
                break
            except Exception:
                pass
            await asyncio.sleep(1.0)

    # ---- 响应辅助 ----

    async def _respond_json(self, writer: asyncio.StreamWriter, status: int, data: dict) -> None:
        body = json.dumps(data, ensure_ascii=False, indent=2)
        status_text = _STATUS_TEXT.get(status, "OK")
        response = (
            f"HTTP/1.1 {status} {status_text}\r\n"
            f"Content-Type: application/json; charset=utf-8\r\n"
            f"Content-Length: {len(body.encode('utf-8'))}\r\n"
            f"Connection: close\r\n\r\n{body}"
        )
        try:
            writer.write(response.encode("utf-8"))
            await writer.drain()
        except (ConnectionResetError, BrokenPipeError):
            pass

    async def _respond_html(self, writer: asyncio.StreamWriter, status: int, html: str) -> None:
        body = html.encode("utf-8")
        status_text = _STATUS_TEXT.get(status, "OK")
        response = (
            f"HTTP/1.1 {status} {status_text}\r\n"
            f"Content-Type: text/html; charset=utf-8\r\n"
            f"Content-Length: {len(body)}\r\n"
            f"Connection: close\r\n\r\n"
        ).encode("utf-8") + body
        try:
            writer.write(response)
            await writer.drain()
        except (ConnectionResetError, BrokenPipeError):
            pass

    async def _route_static(self, writer: asyncio.StreamWriter, path: str) -> None:
        """GET /static/{path} — 静态资源（JS/CSS 等）。

        安全: 拒绝包含 .. 或绝对路径的资源。
        """
        rel = path[len("/static/"):]
        if ".." in rel.split("/") or rel.startswith("/") or "\\" in rel:
            await self._respond_json(writer, 400, {"error": "bad path"})
            return

        full = STATIC_ROOT / rel
        if not full.is_file() or not full.resolve().is_relative_to(STATIC_ROOT):
            await self._respond_json(writer, 404, {"error": "not found"})
            return

        body = full.read_bytes()
        ctype, _ = mimetypes.guess_type(rel)
        ctype = ctype or "application/octet-stream"
        header = (
            f"HTTP/1.1 200 OK\r\n"
            f"Content-Type: {ctype}\r\n"
            f"Content-Length: {len(body)}\r\n"
            f"Cache-Control: public, max-age=3600\r\n"
            f"Connection: close\r\n\r\n"
        ).encode("utf-8")
        try:
            writer.write(header + body)
            await writer.drain()
        except (ConnectionResetError, BrokenPipeError):
            pass
