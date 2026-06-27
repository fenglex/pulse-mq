"""Server：组装 transport+routing+control+stats。内存转发，不持久化消息。

设计要点（与 brief 修正后一致）：
- 数据面/控制面分离：两个 ROUTER socket（bind 数据端口/控制端口）+ ZAP PLAIN。
- 路由键（routing key）= ROUTER 的 **bytes identity**，不是 client_id 字符串。
  Transport.send 调 send_multipart([identity_bytes, frame])，必须传 bytes。
- Server 持有 client_id→ident 映射，用于心跳超时清理 routing（client_id 与
  ROUTER identity 不是同一个东西）。
- 不维护 topic 缓存（Spec 1 §9.2 admin 服务沿用现有 AdminServer；topic_buffers
  传 None，AdminServer 已对 None 做了容忍）。
"""
from __future__ import annotations

import asyncio
import base64
import os
import secrets
import stat
import sys
import time

from pulsemq.admin.auth import TokenAuth
from pulsemq.admin.server import AdminServer
from pulsemq.auth import PlainAuth
from pulsemq.config import ServerConfig, load_server_config
from pulsemq.control import (ClientInfo, ControlCmd, ControlMessage, OnlineRegistry,
                             RegisterResult)
from pulsemq.logging_setup import log_event, logger
from pulsemq.protocol import frames
from pulsemq.routing import SubscriptionTable
from pulsemq.security import CredentialStore
from pulsemq.stats.connections import ConnectionStats, _role_of
from pulsemq.stats.latency import LatencyStats
from pulsemq.stats.storage import AsyncArchiveWriter, StatsStorage
from pulsemq.stats.traffic import TrafficStats
from pulsemq.transport.router import Transport


class Server:
    """组装 transport + routing + control + stats，运行四个后台任务。"""

    def __init__(
        self,
        data_endpoint: str = "tcp://0.0.0.0:5555",
        control_endpoint: str = "tcp://0.0.0.0:5556",
        admin_endpoint: str = "0.0.0.0:9090",
        credentials: dict[str, str] | None = None,
        credentials_file: str | None = None,
        allow_auto_generated: bool | None = None,
        config: ServerConfig | None = None,
        admin_token: str | None = None,
        admin_token_file: str | None = None,
        latency_sample_rate: float | None = None,
    ) -> None:
        self._cfg = config or load_server_config(None)
        # 显式传值优先于 config；空串视为未传（回退到 config）
        self._data_endpoint = data_endpoint or self._cfg.data_endpoint
        self._control_endpoint = control_endpoint or self._cfg.control_endpoint
        self._admin_endpoint = admin_endpoint or self._cfg.admin_endpoint
        # 凭据源（Spec 2）：CredentialStore + PlainAuth
        self.generated_admin_password: str | None = None
        if credentials is not None:
            # 显式明文 dict（测试/兼容）：内存态 store，哈希落值
            store = CredentialStore.from_dict(credentials)
        else:
            cred_file = credentials_file or self._cfg.credentials_file
            allow = (self._cfg.allow_auto_generated_credentials
                     if allow_auto_generated is None else allow_auto_generated)
            store = CredentialStore(
                cred_file,
                allow_auto_generated=allow,
                hash_algo=self._cfg.password_hash_algo,
                bcrypt_cost=self._cfg.bcrypt_cost,
            )
            plaintext = store.load()
            if plaintext is not None:
                # 自动生成的默认 admin：明文仅此一次输出
                self.generated_admin_password = plaintext
                print(f"[SECURITY] 未检测到 {cred_file}，已生成默认用户", file=sys.stderr)
                print(f"[SECURITY] username=admin, password={plaintext}", file=sys.stderr)
                print(
                    "[SECURITY] 提示：默认凭据仅用于首次启动，"
                    "请使用 pulsemq.users CLI 创建正式用户",
                    file=sys.stderr,
                )
                log_event("WARNING", "SECURITY", action="default_credentials_generated")
            else:
                log_event(
                    "INFO", "SECURITY",
                    action="credentials_file_loaded",
                    path=cred_file,
                    users=len(store.list_users()),
                )
        self._credential_store = store
        self._auth = PlainAuth(store)
        # admin token（Spec 2 §5）：优先级 explicit > config > env > 随机生成（写文件 0600）。
        # 显式传 admin_token="" 视为「禁用 token 校验」（向后兼容 Spec 1 测试）。
        self.admin_token = self._resolve_admin_token(admin_token, admin_token_file)
        self._token_auth = TokenAuth(self.admin_token)
        self._transport = Transport()
        self._routing = SubscriptionTable()
        self._registry = OnlineRegistry(heartbeat_timeout=self._cfg.heartbeat_timeout)
        self._stats = TrafficStats(retention_minutes=self._cfg.stats_retention_minutes)
        self._storage = StatsStorage(self._cfg.stats_db)
        # Spec 3 监控依赖：延迟统计 + 连接/事件统计 + 异步归档 writer。
        # 须在 registry/storage 之后构造（ConnectionStats 持有 registry.snapshot 引用，
        # AsyncArchiveWriter 持有 storage 引用）。
        # 显式 latency_sample_rate 覆盖 config；None 时回退到 config。
        rate = (latency_sample_rate if latency_sample_rate is not None
                else self._cfg.latency_sample_rate)
        self._latency = LatencyStats(sample_rate=rate)
        self._connections = ConnectionStats(
            registry_snapshot_fn=self._registry.snapshot,
            ring_size=self._cfg.event_ring_size,
        )
        self._archive_writer = AsyncArchiveWriter(
            self._storage, batch_size=self._cfg.stats_archive_batch_size
        )
        # ZAP on_auth 回调（认证事件）。ZAP 是 ctx 单例：on_auth 必须在
        # 首次（数据面）auth bind 时提供，控制面 bind 复用同一 ZAP，不传 on_auth。
        self._auth_on_auth = self._on_auth_event
        self._tasks: list[asyncio.Task] = []
        self._running = False
        self._stop = asyncio.Event()
        # client_id (REGISTER payload) -> ROUTER bytes identity
        # 心跳超时清理 routing 时必须用它把 client_id 映射回 bytes ident。
        self._ident_by_client_id: dict[str, bytes] = {}
        # admin HTTP 服务（Spec 1 §9.2：常驻，沿用现有 AdminServer）
        self._admin: AdminServer | None = None
        self._start_time: float | None = None

    async def start(self) -> None:
        self._storage.connect()
        # 数据面 bind 传 on_auth：ZAP 是 ctx 单例，仅首次 auth bind 创建 handler，
        # 故 on_auth 必须在数据面 bind 提供；控制面 bind 复用同一 ZAP，不传。
        await self._transport.bind(
            self._data_endpoint, "server_ingress",
            auth=self._auth, on_auth=self._auth_on_auth,
        )
        await self._transport.bind(self._control_endpoint, "control", auth=self._auth)
        # 异步归档 writer：在 minute_roll_loop 启动前 start。
        await self._archive_writer.start()
        # 接入 AdminServer（:9090 监控端口）。
        self._start_time = time.time()
        self._admin = AdminServer(
            bind=self._admin_endpoint,
            traffic_stats=self._stats,
            topic_buffers=None,  # Spec 1 不维护 topic 缓存
            stats_storage=self._storage,
            snapshot_fn=lambda: {
                "online_clients": self._registry.snapshot(),
                "subscriptions": self._routing.snapshot(),
            },
            start_time=self._start_time,
            token_auth=self._token_auth,
            connection_stats=self._connections,
            latency_stats=self._latency,
            admin_thread=self._cfg.admin_thread,
        )
        await self._admin.start()
        self._running = True
        self._tasks = [
            asyncio.create_task(self._data_loop()),
            asyncio.create_task(self._control_loop()),
            asyncio.create_task(self._heartbeat_sweep_loop()),
            asyncio.create_task(self._minute_roll_loop()),
        ]
        logger.info(
            "Server 启动完成 data={} control={} admin={}",
            self._data_endpoint,
            self._control_endpoint,
            self._admin_endpoint,
        )
        self._install_sighup_reload()

    def _resolve_admin_token(self, explicit: str | None,
                             token_file: str | None) -> str:
        """确定 admin HTTP token（Spec 2 §5）。

        优先级：
        1. 显式 ``admin_token=`` 参数（含空串 → 禁用 token，向后兼容 Spec 1）
        2. config ``monitoring.admin_token``（含被环境变量 ``PULSEMQ_ADMIN_TOKEN``
           覆盖后的值，见 ``load_server_config``）
        3. 环境变量 ``PULSEMQ_ADMIN_TOKEN``（双保险；正常路径已被 step 2 吸收）
        4. 随机生成 32 字节 base64url，写 ``admin_token_file``（0600）+ stderr 输出一次

        返回最终 token 字符串（空串表示禁用）。
        """
        if explicit is not None:
            return explicit
        if self._cfg.admin_token:
            return self._cfg.admin_token
        env_tok = os.environ.get("PULSEMQ_ADMIN_TOKEN")
        if env_tok:
            return env_tok
        tok = base64.urlsafe_b64encode(secrets.token_bytes(32)).decode("ascii").rstrip("=")
        path = token_file or self._cfg.admin_token_file
        try:
            with open(path, "w", encoding="utf-8") as f:
                f.write(tok)
            try:
                os.chmod(path, 0o600)
            except OSError:
                # Windows/非 POSIX：chmod 失败可接受，文件已写入。
                pass
            # Spec §11.4：token 文件必须 0600。检查实际权限位并告警。
            if os.name == "posix":
                try:
                    mode = stat.S_IMODE(os.stat(path).st_mode)
                except OSError:
                    mode = None
                if mode is not None and (mode & 0o077):
                    print(
                        f"[ADMIN] 警告：token 文件权限过宽 (mode={oct(mode)}), "
                        "建议 chmod 600",
                        file=sys.stderr,
                    )
                    log_event(
                        "WARNING", "ADMIN",
                        action="insecure_token_file", path=path, mode=oct(mode),
                    )
            else:
                # Windows：chmod 无效，提示目录 ACL 受控。
                print(
                    "[ADMIN] 警告：Windows 未限制 token 文件 ACL，请确保目录访问受控",
                    file=sys.stderr,
                )
                log_event(
                    "WARNING", "ADMIN",
                    action="insecure_token_file_windows", path=path,
                )
            print(f"[ADMIN] 管理接口 token: {tok}", file=sys.stderr)
            log_event("WARNING", "ADMIN", action="admin_token_generated", path=path)
        except OSError:
            log_event("WARNING", "ADMIN", action="admin_token_write_failed", path=path)
        return tok

    def reload_credentials(self) -> None:
        """热更新凭据（CLI 改文件后调用，或 SIGHUP 触发）。"""
        self._credential_store.reload()
        log_event("INFO", "SECURITY", action="credentials_reloaded")

    def _install_sighup_reload(self) -> None:
        """Linux：注册 SIGHUP→reload_credentials。Windows/非主线程静默跳过。"""
        import signal
        loop = asyncio.get_running_loop()
        try:
            loop.add_signal_handler(signal.SIGHUP, self.reload_credentials)
        except (AttributeError, NotImplementedError, ValueError, RuntimeError):
            # Windows 无 SIGHUP；非主线程无信号。留接口，Spec 3 admin 接口接入。
            pass

    async def _data_loop(self) -> None:
        while self._running:
            try:
                ident, frame_bytes = await self._transport.recv("server_ingress")
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("数据面 recv 异常")
                continue
            # 轻量头部解码（不解压/不反序列化），消除完整 frames.decode 在服务端的开销。
            try:
                hdr = frames.decode_header(frame_bytes)
            except Exception:
                logger.debug("数据面帧头部解码失败，丢弃")
                continue
            self._stats.record(hdr.topic, hdr.record_count, len(hdr.raw_payload))
            # 延迟采样：帧已携带 timestamp_ns（生产者发送时刻），差值为端到端延迟。
            if self._latency.should_sample():
                self._latency.record(time.time_ns() - hdr.timestamp_ns)
            # match() 返回 bytes identity，直接给 send_multipart。
            for target in self._routing.match(hdr.topic):
                await self._transport.send(target, frame_bytes, role="server_ingress")

    async def _control_loop(self) -> None:
        while self._running:
            try:
                ident, frame_bytes = await self._transport.recv("control")
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("控制面 recv 异常")
                continue
            try:
                cmd_msg = frames.decode_control(frame_bytes)
            except Exception:
                logger.debug("控制面帧解码失败，丢弃")
                continue
            try:
                await self._dispatch_control(ident, cmd_msg)
            except Exception:
                logger.exception("控制命令处理异常（可能 DEALER 已断开导致 ROUTER_MANDATORY 发送失败）")

    async def _dispatch_control(self, ident: bytes, cmd_msg: ControlMessage) -> None:
        """分发控制命令。

        ident 永远是 ROUTER 的 bytes identity，直接作为 routing key / send 目标。
        client_id 是 REGISTER/SUBSCRIBE payload 里的 app 字符串，与 ident 不是同一个东西。
        """
        cid = cmd_msg.payload.get("client_id", "")

        if cmd_msg.cmd == ControlCmd.REGISTER:
            topics = list(cmd_msg.payload.get("topics", []))
            info = ClientInfo(
                client_id=cid,
                username=cmd_msg.payload.get("username", ""),
                endpoint=cmd_msg.payload.get("endpoint", ""),
                roles=list(cmd_msg.payload.get("roles", [])),
                topics=topics,
                connected_at=time.time(),
            )
            result = self._registry.register(info)
            if result == RegisterResult.OK:
                # 用 bytes ident 作为 routing key 写入；不要 decode 成字符串。
                self._ident_by_client_id[cid] = ident
                for pattern in topics:
                    self._routing.subscribe(ident, pattern)
                # 连接事件（Spec 3）：REGISTER 成功 → on_connect
                self._connections.on_connect(
                    cid, info.username, info.endpoint, _role_of(info.roles)
                )
            reply = frames.encode_control(cmd_msg.cmd, {"result": result})
            await self._transport.send(ident, reply, role="control")
            log_event(
                "INFO", "CLIENT",
                username=info.username, action="register", result=result,
            )

        elif cmd_msg.cmd == ControlCmd.HEARTBEAT:
            self._registry.heartbeat(cid)
            await self._transport.send(
                ident,
                frames.encode_control(cmd_msg.cmd, {"result": "OK"}),
                role="control",
            )

        elif cmd_msg.cmd == ControlCmd.SUBSCRIBE:
            pattern = cmd_msg.payload.get("topic", "")
            self._routing.subscribe(ident, pattern)
            await self._transport.send(
                ident,
                frames.encode_control(cmd_msg.cmd, {"result": "OK"}),
                role="control",
            )

        elif cmd_msg.cmd == ControlCmd.UNSUBSCRIBE:
            pattern = cmd_msg.payload.get("topic", "")
            self._routing.unsubscribe(ident, pattern)
            await self._transport.send(
                ident,
                frames.encode_control(cmd_msg.cmd, {"result": "OK"}),
                role="control",
            )

        elif cmd_msg.cmd == ControlCmd.DISCONNECT:
            self._routing.remove(ident)
            self._ident_by_client_id.pop(cid, None)
            self._registry.unregister(cid)
            # 断开事件（Spec 3）：DISCONNECT → on_disconnect
            self._connections.on_disconnect(cid, "disconnect")
            await self._transport.send(
                ident,
                frames.encode_control(cmd_msg.cmd, {"result": "OK"}),
                role="control",
            )

        else:
            logger.debug("未知控制命令: {}", cmd_msg.cmd)

    async def _heartbeat_sweep_loop(self) -> None:
        while self._running:
            await asyncio.sleep(1.0)
            try:
                offline = self._registry.sweep_timeout()
                for c in offline:
                    # routing key 是 bytes ident，需要从 client_id 反查；
                    # registry 只知道 client_id 字符串，不能直接拿它清 routing。
                    ident = self._ident_by_client_id.pop(c.client_id, None)
                    if ident is not None:
                        self._routing.remove(ident)
                    # 断开事件（Spec 3）：心跳超时下线 → on_disconnect
                    self._connections.on_disconnect(c.client_id, "heartbeat_timeout")
                    log_event(
                        "WARNING", "CLIENT",
                        username=c.username, reason="heartbeat_timeout",
                    )
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("心跳扫描异常")

    async def _minute_roll_loop(self) -> None:
        while self._running:
            await asyncio.sleep(60.0)
            try:
                archived = self._stats.roll_minute()
                if archived:
                    # Spec 3：异步归档，不阻塞数据接收循环。
                    await self._archive_writer.enqueue(archived)
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("分钟归档异常")

    async def _on_auth_event(self, username: str, address: str, ok: bool,
                             reason: str | None = None) -> None:
        """ZAP 认证回调（async）：发认证事件到连接统计环。

        ZAP handler 在 reply 后调用，签名为 ``(username, address, ok, reason)``。
        reason 由 PlainAuthDict.verify 返回（如 user_not_found / invalid_password）。
        address 当前由 AsyncZAPHandler 传空串（ZAP 帧内的 address 字段），保留接口。
        """
        self._connections.on_auth(username, address, success=ok, reason=reason)

    async def wait_for_shutdown(self) -> None:
        await self._stop.wait()

    def is_shutting_down(self) -> bool:
        """是否已进入关闭流程（_stop 已 set）。"""
        return self._stop.is_set()

    async def stop(self) -> None:
        self._running = False
        self._stop.set()
        for t in self._tasks:
            t.cancel()
        for t in self._tasks:
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass
        self._tasks.clear()
        # 停机前最后归档一次当前分钟，避免丢统计：入队 archive_writer，
        # 由其 stop() drain 落库。
        try:
            archived = self._stats.roll_minute()
            if archived:
                await self._archive_writer.enqueue(archived)
        except Exception:
            logger.debug("stop 归档入队失败", exc_info=True)
        # 关闭顺序（Spec 3）：停 admin → 停 archive_writer（drain 剩余）→
        # 停 transport → 关 storage。storage.close 必须在 archive_writer.stop 之后，
        # 否则 drain 写入已关闭的 SQLite 连接。
        if self._admin is not None:
            await self._admin.stop()
            self._admin = None
        await self._archive_writer.stop()
        await self._transport.close()
        self._storage.close()
