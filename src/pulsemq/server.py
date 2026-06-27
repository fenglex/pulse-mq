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
import sys
import time

from pulsemq.admin.server import AdminServer
from pulsemq.auth import PlainAuth
from pulsemq.config import ServerConfig, load_server_config
from pulsemq.control import (ClientInfo, ControlCmd, ControlMessage, OnlineRegistry,
                             RegisterResult)
from pulsemq.logging_setup import log_event, logger
from pulsemq.protocol import frames
from pulsemq.routing import SubscriptionTable
from pulsemq.security import CredentialStore
from pulsemq.stats.storage import StatsStorage
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
        self._transport = Transport()
        self._routing = SubscriptionTable()
        self._registry = OnlineRegistry(heartbeat_timeout=self._cfg.heartbeat_timeout)
        self._stats = TrafficStats(retention_minutes=self._cfg.stats_retention_minutes)
        self._storage = StatsStorage(self._cfg.stats_db)
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
        await self._transport.bind(self._data_endpoint, "server_ingress", auth=self._auth)
        await self._transport.bind(self._control_endpoint, "control", auth=self._auth)
        # 接入 AdminServer（:9090 监控端口）。Spec 1 在同一 event loop 上运行，
        # Spec 3 后续再迁移到独立线程。
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
            try:
                msg = frames.decode(frame_bytes)
            except Exception:
                logger.debug("数据面帧解码失败，丢弃")
                continue
            self._stats.record(msg.topic, msg.record_count, len(msg.raw_payload))
            # match() 返回的是 routing key 集合，本实现里 routing key=bytes ident，
            # 直接交给 Transport.send（send_multipart([target, frame])）。
            for target in self._routing.match(msg.topic):
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
            await self._dispatch_control(ident, cmd_msg)

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
                self._storage.save_minutes_batch(archived)
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("分钟归档异常")

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
        # 停机前最后归档一次当前分钟，避免丢统计。
        try:
            archived = self._stats.roll_minute()
            self._storage.save_minutes_batch(archived)
        except Exception:
            logger.debug("stop 归档失败", exc_info=True)
        # 关闭顺序（Spec 1 §10/§11.8）：停 admin → 停 transport → 关 storage。
        if self._admin is not None:
            await self._admin.stop()
            self._admin = None
        await self._transport.close()
        self._storage.close()
