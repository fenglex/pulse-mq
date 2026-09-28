"""AdminServer: HTTP + SSE + REST API。

stdlib asyncio HTTP，手写请求解析，不引入框架。
"""

from __future__ import annotations

import asyncio
import json
import mimetypes
import os
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
    """后台管理 HTTP 服务: REST + SSE + Web UI + Prometheus。

    端点:
      GET  /                              深色 Web UI 首页
      GET  /static/{path}                 静态资源（ECharts 等）
      GET  /api/v1/stats/realtime         实时指标 JSON
      GET  /api/v1/stats/stream           SSE 实时推送（1s 一帧）
      GET  /api/v1/stats/buffers          订阅者缓冲（含 credit 余量，9.2.8）
      GET  /api/v1/stats/drops/history    丢弃分钟历史（内存 + SQLite，9.2.8）
      GET  /api/v1/stats/gaps/history     缺口分钟历史（同上）
      GET  /api/v1/topics                 所有 topic 列表 + 当前指标
      GET  /api/v1/topics/{topic}/history 分钟级历史（最近 N 分钟）
      GET  /api/v1/system/status          系统状态（uptime/version/RSS，9.2.8 扩展）
      GET  /metrics                       Prometheus 文本 exposition（9.2.8）
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
        proc_stats=None,
        buffer_stats=None,
        gap_stats=None,
        dataplane_stats=None,
        hb_monitor=None,
        credit_fn=None,
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
        self._proc_stats = proc_stats
        self._buffer_stats = buffer_stats
        self._gap_stats = gap_stats
        # 9.2.8 扩展：数据面健康 / 心跳质量 / per-client 信用视图
        self._dataplane = dataplane_stats
        self._hb_monitor = hb_monitor
        self._credit_fn = credit_fn
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

        if method == "GET" and path == "/api/v1/stats/buffers":
            # 订阅者缓冲指标：深度/字节/最老帧年龄/淘汰/过期/合并计数
            if self._buffer_stats is not None:
                await self._respond_json(writer, 200, self._buffer_stats.snapshot())
            else:
                await self._respond_json(writer, 200,
                                         {"defaults": None, "subscribers": {}})
            return

        if method == "GET" and path == "/api/v1/stats/stream":
            await self._handle_sse(writer)
            return

        # 丢弃/缺口分钟历史（9.2.8）：?topic= 指定单 topic 曲线，缺省为全量合计
        if method == "GET" and path in ("/api/v1/stats/drops/history",
                                        "/api/v1/stats/gaps/history"):
            minutes = 60
            try:
                minutes = int(query.get("minutes", ["60"])[0])
            except (ValueError, IndexError):
                pass
            topic = query.get("topic", [None])[0]
            is_drops = path.endswith("drops/history")
            await self._respond_json(
                writer, 200,
                self._drop_or_gap_history(is_drops, topic, minutes))
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

        if method == "GET" and path == "/metrics":
            body = self._render_prometheus().encode("utf-8")
            header = (
                "HTTP/1.1 200 OK\r\n"
                "Content-Type: text/plain; version=0.0.4; charset=utf-8\r\n"
                f"Content-Length: {len(body)}\r\n"
                "Connection: close\r\n\r\n"
            ).encode("utf-8")
            try:
                writer.write(header + body)
                await writer.drain()
            except (ConnectionResetError, BrokenPipeError):
                pass
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
        # 消费端缺口统计（9.2.1：帧头 seq 检测，心跳 gaps 字段聚合）
        if self._gap_stats is not None:
            snap["gaps"] = self._gap_stats.snapshot()
        # 订阅者缓冲指标（深度/最老帧年龄/淘汰/过期/合并 + credit 余量）
        if self._buffer_stats is not None:
            snap["buffers"] = self._buffer_stats.snapshot()
        # 每客户端上报的 per-topic 处理速率/延迟聚合（心跳 proc 字段）
        proc_by_topic: dict[str, dict] = {}
        if self._proc_stats is not None:
            proc_by_topic = self._proc_stats.topics()
            snap["processing_by_topic"] = proc_by_topic
        # 数据面健康（9.2.8）：循环耗时/收发速率/EAGAIN/信用拦截
        if self._dataplane is not None:
            snap["dataplane"] = self._dataplane.snapshot()
        # 心跳质量（9.2.8）：per-client 到达间隔 + 踢线计数
        if self._hb_monitor is not None:
            snap["heartbeat"] = self._hb_monitor.snapshot()
        # 对账视图（9.2.8）：per-topic 发布累计 vs 消费端处理/缺失/丢弃累计，
        # 在途 = published - processed - missing - dropped（瞬时可为负/正，
        # 客户端重启会重置 processed_cum，作趋势参考而非精确恒等式）
        snap["reconciliation"] = self._reconciliation_snapshot(proc_by_topic)
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

    def _reconciliation_snapshot(self, proc_by_topic: dict) -> dict[str, dict]:
        """per-topic 三方账本合并（发布/处理/缺失/丢弃累计 + 在途估算）。"""
        topics: dict[str, dict] = {}
        if self._traffic is not None:
            for t, d in self._traffic.all_topics_snapshot().items():
                cum = d.get("msg_count_cum")
                if cum:
                    topics.setdefault(t, {})["published_cum"] = cum
        for t, d in proc_by_topic.items():
            e = topics.setdefault(t, {})
            if d.get("processed_cum"):
                e["processed_cum"] = d["processed_cum"]
            if d.get("rate_per_sec") is not None:
                e["processing_rate"] = d["rate_per_sec"]
        drops = self._drop_stats.snapshot() if self._drop_stats else {}
        for t, d in drops.items():
            if d.get("drops_cum"):
                topics.setdefault(t, {})["dropped_cum"] = d["drops_cum"]
        gaps = self._gap_stats.snapshot() if self._gap_stats else {}
        for t, g in gaps.items():
            if g:
                topics.setdefault(t, {})["missing_cum"] = g
        for t, e in topics.items():
            pub = e.get("published_cum", 0)
            if pub:
                e["in_flight_est"] = pub - (e.get("processed_cum", 0)
                                            + e.get("missing_cum", 0)
                                            + e.get("dropped_cum", 0))
        return topics

    def _clients_snapshot(self) -> dict:
        """在线 client 明细（跨线程只读快照；connection_stats 为 None 时返回空）。"""
        if self._connections is None:
            return {"clients": []}
        clients = []
        for c in self._connections.online_clients():
            entry = {
                "client_id": c.client_id,
                "username": c.username,
                "role": c.role,
                "endpoint": c.endpoint,
                "topics": list(c.topics),
                "connected_at_iso": _iso(c.connected_at),
                "duration_seconds": round(c.duration_seconds, 1),
                # Web UI 客户端表格实际读取的字段名（与上兼容并存）：
                # connected_at（数值秒）、remote、subscriptions（订阅数）。
                "connected_at": c.connected_at,
                "remote": c.endpoint,
                "subscriptions": len(c.topics),
            }
            # 处理速率/延迟（心跳 proc 上报；老客户端无数据则缺省）。
            if self._proc_stats is not None:
                proc = self._proc_stats.client_entry(c.client_id)
                if proc is not None:
                    entry["processing"] = self._proc_stats.summarize(proc)
                    entry["processing_topics"] = proc["topics"]
                    if "confirm" in proc:
                        entry["confirm"] = proc["confirm"]
                    if "workers" in proc:
                        entry["workers"] = proc["workers"]
                    # key 拆分回退累计（9.2.9）：>0 说明该消费端 key 路由
                    # 遇到 DataFrame 等不支持载荷，已回退 topic 路由
                    if proc.get("key_fallback_cum"):
                        entry["key_fallback"] = proc["key_fallback_cum"]
            # 信用窗口视图（9.2.8）：最近上报 credit / 窗口剩余 / 拦截轮次
            if self._credit_fn is not None:
                credit = self._credit_fn(c.client_id)
                if credit is not None:
                    entry["credit"] = credit
            clients.append(entry)
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
        out = {
            "version": SERVER_VERSION,
            "start_time": self._start_time,
            "uptime_seconds": round(time.time() - self._start_time, 2),
            "pid": os.getpid(),
            "threads": threading.active_count(),
        }
        # RSS（MB）：Linux 读 /proc；Windows/其他平台缺省（不引 psutil 依赖）
        try:
            with open("/proc/self/status", encoding="ascii") as f:
                for line in f:
                    if line.startswith("VmRSS:"):
                        out["rss_mb"] = round(int(line.split()[1]) / 1024, 1)
                        break
        except OSError:
            pass
        return out

    # ---- 丢弃/缺口分钟历史（内存 + SQLite 合并，9.2.8）----

    def _drop_or_gap_history(self, is_drops: bool, topic: str | None,
                             minutes: int) -> dict:
        """单 topic（或全量合计）的分钟曲线 [{timestamp, count}]。"""
        if is_drops:
            mem = self._drop_stats.history(minutes) if self._drop_stats else []
        else:
            mem = self._gap_stats.history(minutes) if self._gap_stats else []
        series: dict[int, int] = {}
        for slot in mem:
            topics = slot.get("topics", {})
            if topic is not None:
                series[slot["timestamp"]] = topics.get(topic, 0)
            else:
                series[slot["timestamp"]] = series.get(slot["timestamp"], 0) \
                    + sum(topics.values())
        since = int(time.time()) - minutes * 60
        if self._storage is not None and topic is not None:
            if is_drops:
                rows = self._storage.load_drop_history(topic, since)
            else:
                rows = self._storage.load_gap_history(topic, since)
            for r in rows:
                if r["timestamp"] not in series:
                    series[r["timestamp"]] = r["count"]
        history = [{"timestamp": ts, "count": c}
                   for ts, c in sorted(series.items())]
        kind = "drops" if is_drops else "gaps"
        return {"kind": kind, "topic": topic, "minutes": minutes,
                "history": history}

    # ---- Prometheus 文本 exposition（9.2.8）----

    @staticmethod
    def _esc_label(v: str) -> str:
        return (v.replace("\\", "\\\\").replace('"', '\\"')
                .replace("\n", "\\n"))

    def _render_prometheus(self) -> str:
        lines: list[str] = []

        def metric(name: str, mtype: str, help_text: str) -> None:
            lines.append(f"# HELP {name} {help_text}")
            lines.append(f"# TYPE {name} {mtype}")

        def emit(name: str, value, labels: str = "") -> None:
            if labels:
                lines.append(f'{name}{{{labels}}} {value}')
            else:
                lines.append(f"{name} {value}")

        snap = self._realtime_snapshot()
        metric("pulsemq_up", "gauge", "Server is up")
        emit("pulsemq_up", 1)
        metric("pulsemq_uptime_seconds", "gauge", "Uptime in seconds")
        emit("pulsemq_uptime_seconds",
             round(time.time() - self._start_time, 1))
        metric("pulsemq_build_info", "gauge", "Version info")
        emit("pulsemq_build_info", 1, f'version="{SERVER_VERSION}"')

        for k in ("online_users", "online_producers", "online_consumers",
                  "total_subscriptions"):
            if k in snap:
                metric(f"pulsemq_{k}", "gauge", k)
                emit(f"pulsemq_{k}", snap[k])

        metric("pulsemq_topic_msg_rate_1min", "gauge",
               "Published frames/s (1min est) per topic")
        metric("pulsemq_topic_msg_count_cum", "counter",
               "Published frames since server start per topic")
        for t, d in snap.get("topics", {}).items():
            lb = f'topic="{self._esc_label(t)}"'
            emit("pulsemq_topic_msg_rate_1min", d.get("msg_rate_1min", 0), lb)
            emit("pulsemq_topic_msg_count_cum",
                 d.get("msg_count_cum", 0), lb)

        metric("pulsemq_drops_last_min", "gauge", "Drops in last minute")
        metric("pulsemq_drops_cum", "counter", "Drops since server start")
        for t, d in snap.get("drops", {}).items():
            lb = f'topic="{self._esc_label(t)}"'
            emit("pulsemq_drops_last_min", d.get("drops_last_min", 0), lb)
            emit("pulsemq_drops_cum", d.get("drops_cum", 0), lb)

        metric("pulsemq_gaps_cum", "counter", "Missing frames per topic")
        for t, g in snap.get("gaps", {}).items():
            emit("pulsemq_gaps_cum", g, f'topic="{self._esc_label(t)}"')

        metric("pulsemq_buffer_depth", "gauge", "Subscriber buffer depth")
        metric("pulsemq_buffer_bytes", "gauge", "Subscriber buffer bytes")
        metric("pulsemq_buffer_oldest_age_ms", "gauge",
               "Oldest buffered frame age ms")
        for s, b in snap.get("buffers", {}).get("subscribers", {}).items():
            lb = f'subscriber="{self._esc_label(s)}"'
            emit("pulsemq_buffer_depth", b.get("depth", 0), lb)
            emit("pulsemq_buffer_bytes", b.get("bytes", 0), lb)
            emit("pulsemq_buffer_oldest_age_ms",
                 b.get("oldest_age_ms", 0), lb)

        for kind in ("latency_half", "latency_e2e"):
            reg = snap.get(kind, {})
            if not reg:
                continue
            tag = "half" if kind == "latency_half" else "e2e"
            metric(f"pulsemq_latency_ms", "gauge", f"Latency ms ({tag})")
            for t, d in reg.items():
                lb = (f'topic="{self._esc_label(t)}",kind="{tag}"')
                for p in ("p50", "p95", "p99"):
                    if f"{p}_ms" in d:
                        emit("pulsemq_latency_ms", d[f"{p}_ms"], lb)

        proc = snap.get("processing_by_topic", {})
        if proc:
            metric("pulsemq_processing_rate", "gauge",
                   "Consumer processing frames/s per topic")
            metric("pulsemq_processing_cum", "counter",
                   "Consumer processed frames since client start")
            metric("pulsemq_processing_e2e_avg_ms", "gauge",
                   "Processing-time e2e latency avg ms")
            for t, d in proc.items():
                lb = f'topic="{self._esc_label(t)}"'
                emit("pulsemq_processing_rate", d.get("rate_per_sec", 0), lb)
                emit("pulsemq_processing_cum",
                     d.get("processed_cum", 0), lb)
                if "e2e_avg_ms" in d:
                    emit("pulsemq_processing_e2e_avg_ms",
                         d["e2e_avg_ms"], lb)

        dp = snap.get("dataplane", {})
        if dp:
            metric("pulsemq_dataplane_loop_ms", "gauge",
                   "Data-plane loop avg/max ms in window")
            emit("pulsemq_dataplane_loop_ms", dp.get("loop_avg_ms", 0),
                 'kind="avg"')
            emit("pulsemq_dataplane_loop_ms", dp.get("loop_max_ms", 0),
                 'kind="max"')
            for k in ("in_per_s", "out_per_s", "eagain_per_s",
                      "starved_per_s"):
                metric(f"pulsemq_dataplane_{k}", "gauge", k)
                emit(f"pulsemq_dataplane_{k}", dp.get(k, 0))
            for k in ("msgs_in", "msgs_out", "eagain", "credit_starved"):
                metric(f"pulsemq_dataplane_{k}_total", "counter", k)
                emit(f"pulsemq_dataplane_{k}_total", dp.get(k, 0))

        hb = snap.get("heartbeat", {})
        if hb:
            metric("pulsemq_heartbeat_interval_s", "gauge",
                   "Client heartbeat interval avg/max (recent window)")
            for cid, d in hb.get("clients", {}).items():
                lb = f'client_id="{self._esc_label(cid)}"'
                emit("pulsemq_heartbeat_interval_s",
                     d.get("interval_avg_s", 0), lb + ',kind="avg"')
                emit("pulsemq_heartbeat_interval_s",
                     d.get("interval_max_s", 0), lb + ',kind="max"')
            metric("pulsemq_heartbeat_kicks_total", "counter",
                   "Clients kicked by heartbeat timeout")
            emit("pulsemq_heartbeat_kicks_total", hb.get("kick_total", 0))

        lines.append("")
        return "\n".join(lines)

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
