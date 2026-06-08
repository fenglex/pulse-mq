"""AdminServer: HTTP + SSE + REST API。

stdlib asyncio HTTP，手写请求解析，不引入框架。
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any, Callable
from urllib.parse import parse_qs, urlparse

from pulsemq.admin.web_ui import INDEX_HTML
from pulsemq.cache.topic_buffer import TopicBufferRegistry
from pulsemq.stats.storage import StatsStorage
from pulsemq.stats.traffic import TrafficStats

logger = logging.getLogger(__name__)

SERVER_VERSION: str = "2.0.0"


class AdminServer:
    """后台管理 HTTP 服务: REST + SSE + Web UI。

    端点:
      GET  /                              深色 Web UI 首页
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
        topic_buffers: TopicBufferRegistry | None = None,
        stats_storage: StatsStorage | None = None,
        snapshot_fn: Callable[[], dict] | None = None,
        start_time: float | None = None,
    ) -> None:
        host, port = bind.split(":")
        self._host = host
        self._port = int(port)
        self._traffic = traffic_stats
        self._buffers = topic_buffers
        self._storage = stats_storage
        self._snapshot_fn = snapshot_fn
        self._start_time = start_time or time.time()
        self._server: asyncio.AbstractServer | None = None
        # SSE 客户端
        self._sse_clients: dict[int, tuple[asyncio.Queue, asyncio.Task]] = {}
        self._sse_id = 0
        self._sse_task: asyncio.Task | None = None

    # ---- 生命周期 ----

    async def start(self) -> None:
        self._server = await asyncio.start_server(
            self._handle_request, self._host, self._port
        )
        self._sse_task = asyncio.create_task(self._sse_broadcast_loop())
        logger.info("AdminServer 启动: http://%s:%d", self._host, self._port)

    async def stop(self) -> None:
        # 关闭 SSE 客户端
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
            await self._route(writer, method, path, query)
        except asyncio.TimeoutError:
            pass
        except (ConnectionResetError, BrokenPipeError):
            pass
        except Exception:
            pass
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

        if method == "GET" and path == "/api/v1/system/status":
            await self._respond_json(writer, 200, self._system_status())
            return

        if method == "GET" and path == "/healthz":
            await self._respond_json(writer, 200, {"status": "ok"})
            return

        await self._respond_json(writer, 404, {"error": "not found"})

    # ---- 数据方法 ----

    def _realtime_snapshot(self) -> dict:
        """实时指标快照。"""
        snap: dict[str, Any] = {}
        if self._traffic is not None:
            snap["topics"] = self._traffic.all_topics_snapshot()
        if self._buffers is not None:
            snap["cache_sizes"] = self._buffers.snapshot()
        if self._snapshot_fn is not None:
            snap.update(self._snapshot_fn())
        snap["server_time"] = time.time()
        return snap

    def _list_topics(self) -> dict:
        """所有 topic 列表 + 指标。"""
        if self._traffic is None:
            return {"topic_count": 0, "topics": []}
        all_data = self._traffic.all_topics_snapshot()
        cache_sizes = self._buffers.snapshot() if self._buffers else {}
        topics = []
        for topic, data in all_data.items():
            topics.append({
                "topic": topic,
                "msg_rate_1min": data["msg_rate_1min"],
                "msg_count_current": data["msg_count_current"],
                "record_count_current": data["record_count_current"],
                "bytes_total_current": data["bytes_total_current"],
                "cache_size": cache_sizes.get(topic, 0),
            })
        return {"topic_count": len(topics), "topics": topics}

    def _topic_history(self, topic: str, minutes: int) -> dict:
        """分钟级历史。"""
        # 优先从内存
        if self._traffic is not None:
            history = self._traffic.get_history(topic, minutes)
            if history:
                return {"topic": topic, "minutes": minutes, "history": history}
        # 从 SQLite
        if self._storage is not None:
            since_ts = int(time.time()) - minutes * 60
            history = self._storage.load_history(topic, since_ts)
            return {"topic": topic, "minutes": minutes, "history": history}
        return {"topic": topic, "minutes": minutes, "history": []}

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
                for _cid, (q, _task) in list(self._sse_clients.items()):
                    try:
                        q.put_nowait(frame)
                    except asyncio.QueueFull:
                        pass
            except asyncio.CancelledError:
                break
            except Exception:
                pass
            await asyncio.sleep(1.0)

    # ---- 响应辅助 ----

    async def _respond_json(self, writer: asyncio.StreamWriter, status: int, data: dict) -> None:
        body = json.dumps(data, ensure_ascii=False, indent=2)
        status_text = {
            200: "OK", 400: "Bad Request", 404: "Not Found", 500: "Internal Server Error",
        }.get(status, "OK")
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
        response = (
            f"HTTP/1.1 {status} OK\r\n"
            f"Content-Type: text/html; charset=utf-8\r\n"
            f"Content-Length: {len(body)}\r\n"
            f"Connection: close\r\n\r\n"
        ).encode("utf-8") + body
        try:
            writer.write(response)
            await writer.drain()
        except (ConnectionResetError, BrokenPipeError):
            pass
