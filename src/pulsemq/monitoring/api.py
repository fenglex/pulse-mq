"""HTTP 监控 API。

简单的 asyncio HTTP server，提供实时指标查询。
不依赖外部 HTTP 框架（如 aiohttp/fastapi），使用标准库实现。
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Callable

logger = logging.getLogger(__name__)


class MetricsHTTPServer:
    """轻量级监控 HTTP 服务器。"""

    def __init__(
        self,
        bind: str = "0.0.0.0:9090",
        snapshot_fn: Callable[[], dict] | None = None,
    ):
        host, port = bind.split(":")
        self._host = host
        self._port = int(port)
        self._snapshot_fn = snapshot_fn
        self._server: asyncio.AbstractServer | None = None

    async def start(self) -> None:
        self._server = await asyncio.start_server(
            self._handle_request, self._host, self._port
        )
        logger.info("监控 API 启动: http://%s:%d", self._host, self._port)

    async def stop(self) -> None:
        if self._server:
            self._server.close()
            await self._server.wait_closed()
            logger.info("监控 API 已关闭")

    async def _handle_request(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            # 读取请求行
            request_line = await asyncio.wait_for(reader.readline(), timeout=5.0)
            if not request_line:
                # 空请求行: 直接返回, 由 finally 统一 close
                return

            line = request_line.decode("utf-8", errors="ignore").strip()
            parts = line.split()
            path = parts[1] if len(parts) > 1 else "/"

            # 路由
            if path == "/api/v1/metrics/realtime":
                await self._respond_json(writer, 200, self._get_realtime())
            elif path == "/healthz":
                await self._respond_json(writer, 200, {"status": "ok"})
            else:
                await self._respond_json(writer, 404, {"error": "not found"})

        except asyncio.TimeoutError:
            pass
        except Exception as e:
            logger.debug("HTTP 请求处理异常: %s", e)
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    def _get_realtime(self) -> dict:
        if self._snapshot_fn:
            return self._snapshot_fn()
        return {"error": "metrics not available"}

    async def _respond_json(
        self, writer: asyncio.StreamWriter, status: int, data: dict
    ) -> None:
        body = json.dumps(data, ensure_ascii=False, indent=2)
        status_text = {200: "OK", 404: "Not Found"}.get(status, "OK")
        response = (
            f"HTTP/1.1 {status} {status_text}\r\n"
            f"Content-Type: application/json; charset=utf-8\r\n"
            f"Content-Length: {len(body.encode('utf-8'))}\r\n"
            f"Connection: close\r\n"
            f"\r\n"
            f"{body}"
        )
        writer.write(response.encode("utf-8"))
        await writer.drain()
