"""PulsePublisher: v2 入口类（编排各层）。

纯 pub → sub 架构，单进程 publisher：
- ZMQ PUB socket 广播
- Producer 回调注册 + 并发调度
- 流量统计 + Admin 后台
- Topic 缓存

用法:
    pub = PulsePublisher()

    @pub.producer(name="sh_market", interval=5.0)
    async def sh_market():
        return fetch_data()

    pub.start()  # 阻塞运行
"""

from __future__ import annotations

import asyncio
import logging
import time
from functools import wraps
from typing import Any, Callable, Awaitable

from pulsemq._version import __version__
from pulsemq.admin.server import AdminServer
from pulsemq.cache.topic_buffer import TopicBufferRegistry
from pulsemq.config import PublisherConfig, load_config
from pulsemq.producers.manager import ProducerManager
from pulsemq.protocol import frames as frame_codec
from pulsemq.stats.storage import StatsStorage
from pulsemq.stats.traffic import TrafficStats
from pulsemq.transport.zmq_pub import AuthCallback, ZmqPubTransport

logger = logging.getLogger(__name__)

# 包版本：从 pulsemq._version 统一读取，保持向后兼容的导入路径
__all__ = ["__version__"]


class PublisherSender:
    """注入 producer 回调的手动发送端。"""

    def __init__(self, publisher: "PulsePublisher", spec: Any) -> None:
        self._publisher = publisher
        self._spec = spec

    async def send(
        self,
        data: Any,
        *,
        topic: str | None = None,
        serializer: str | None = None,
        compression: str | None = None,
    ) -> None:
        """手动发送一条消息，默认沿用当前 producer 配置。"""
        await self._publisher._publish_data(
            topic=topic or self._spec.name,
            data=data,
            cache_size=self._spec.cache_size,
            serializer=serializer or self._spec.serializer,
            compression=compression or self._spec.compression,
        )


class PulsePublisher:
    """PulseMQ v2 Publisher 入口类。"""

    def __init__(
        self,
        config: PublisherConfig | None = None,
        *,
        bind: str | None = None,
        admin_bind: str | None = None,
        api_keys: dict[str, str] | None = None,
        on_auth: AuthCallback | None = None,
    ) -> None:
        self._config = config or load_config()
        # 参数覆盖
        if bind:
            self._config.bind = bind
        if admin_bind:
            self._config.admin_bind = admin_bind
        self._explicit_api_keys = api_keys
        self._on_auth = on_auth

        # 内部组件
        self._transport: ZmqPubTransport | None = None
        self._producer_mgr = ProducerManager()
        self._buffers = TopicBufferRegistry()
        self._traffic = TrafficStats(self._config.stats_retention_minutes)
        self._storage: StatsStorage | None = None
        self._admin: AdminServer | None = None

        self._start_time: float = 0
        self._running = False

    # ---- Producer 注册 ----

    def producer(
        self,
        name: str,
        *,
        interval: float = 5.0,
        cache_size: int = 100_000,
        serializer: str = "msgpack",
        compression: str = "none",
        inject_sender: bool = False,
    ) -> Callable:
        """装饰器：注册 async producer。"""
        def decorator(fn: Callable[[], Awaitable[Any]]) -> Callable[[], Awaitable[Any]]:
            self._producer_mgr.register(
                callback=fn,
                name=name,
                interval=interval,
                cache_size=cache_size,
                serializer=serializer,
                compression=compression,
                inject_sender=inject_sender,
            )
            return fn
        return decorator

    def burst_producer(
        self,
        name: str,
        *,
        cache_size: int = 100_000,
        serializer: str = "msgpack",
        compression: str = "none",
        inject_sender: bool = False,
    ) -> Callable:
        """装饰器：注册 burst producer（无间隔连续发送，用于极限性能测试）。"""
        def decorator(fn: Callable[[], Awaitable[Any]]) -> Callable[[], Awaitable[Any]]:
            self._producer_mgr.register_burst(
                callback=fn,
                name=name,
                cache_size=cache_size,
                serializer=serializer,
                compression=compression,
                inject_sender=inject_sender,
            )
            return fn
        return decorator

    def register_producer(
        self,
        fn: Callable[[], Awaitable[Any]],
        *,
        name: str,
        interval: float = 5.0,
        cache_size: int = 100_000,
        serializer: str = "msgpack",
        compression: str = "none",
        inject_sender: bool = False,
    ) -> None:
        """直接注册 async producer。"""
        self._producer_mgr.register(
            callback=fn,
            name=name,
            interval=interval,
            cache_size=cache_size,
            serializer=serializer,
            compression=compression,
            inject_sender=inject_sender,
        )

    def add_api_key(self, username: str, password: str) -> None:
        """编程式添加 API Key。需要在 start() 前调用。"""
        if self._explicit_api_keys is None:
            self._explicit_api_keys = {}
        self._explicit_api_keys[username] = password

    def set_auth_callback(self, callback: AuthCallback | None) -> None:
        """设置认证事件回调（可运行时动态替换）。

        回调签名为 async def(username, client_address, success)。
        需在 start() 前调用，或在 start() 后通过 transport 自动传递。
        """
        self._on_auth = callback
        if self._transport is not None:
            self._transport.set_auth_callback(callback)

    # ---- 启动 ----

    def start(self) -> None:
        """阻塞启动 publisher。"""
        print(format_startup_table(self._config, self._explicit_api_keys), file=__import__("sys").stderr)
        asyncio.run(self._run())

    async def start_async(self) -> None:
        """异步启动 publisher（方便嵌入其他 asyncio 程序）。"""
        print(format_startup_table(self._config, self._explicit_api_keys), file=__import__("sys").stderr)
        await self._run()

    async def _run(self) -> None:
        """主运行循环。"""
        self._running = True
        self._start_time = time.time()

        # 确定最终 api_keys
        api_keys = self._explicit_api_keys or self._config.api_keys

        # 初始化传输层
        self._transport = ZmqPubTransport(
            bind=self._config.bind,
            api_keys=api_keys,
            on_auth=self._on_auth,
        )
        await self._transport.start()

        # 初始化统计存储
        self._storage = StatsStorage(self._config.stats_db)
        self._storage.connect()

        # 为所有 producer 创建 topic 缓存
        for name, spec in self._producer_mgr.specs.items():
            self._buffers.get_or_create(name, spec.cache_size)

        # 初始化 Admin 后台
        self._admin = AdminServer(
            bind=self._config.admin_bind,
            traffic_stats=self._traffic,
            topic_buffers=self._buffers,
            stats_storage=self._storage,
            snapshot_fn=self._system_snapshot,
            start_time=self._start_time,
        )
        await self._admin.start()

        # 启动分钟滚动任务
        roll_task = asyncio.create_task(self._minute_roll_loop())

        # 启动心跳循环（默认 30s 间隔）
        hb_task: asyncio.Task | None = None
        if self._config.heartbeat_interval > 0:
            hb_task = asyncio.create_task(self._heartbeat_loop(), name="heartbeat")

        # 启动所有 producer
        await self._producer_mgr.start_all(self._on_produce, self._make_sender)

        logger.info("PulsePublisher 运行中 (bind=%s, admin=%s)", self._config.bind, self._config.admin_bind)

        try:
            # 等待运行结束
            while self._running:
                await asyncio.sleep(1)
        except (KeyboardInterrupt, asyncio.CancelledError):
            pass
        finally:
            if hb_task is not None:
                hb_task.cancel()
                try:
                    await hb_task
                except asyncio.CancelledError:
                    pass
            await self._shutdown(roll_task)

    async def _shutdown(self, roll_task: asyncio.Task) -> None:
        """优雅关闭。"""
        self._running = False
        await self._producer_mgr.stop_all()
        roll_task.cancel()
        try:
            await roll_task
        except asyncio.CancelledError:
            pass

        # 最后一次分钟滚动 + 落库
        archived = self._traffic.roll_minute()
        if self._storage and archived:
            self._storage.save_minutes_batch(archived)

        if self._admin:
            await self._admin.stop()
        if self._transport:
            await self._transport.stop()
        if self._storage:
            self._storage.close()
        logger.info("PulsePublisher 已关闭")

    def _make_sender(self, spec: Any) -> PublisherSender:
        """为 inject_sender producer 构造手动发送端。"""
        return PublisherSender(self, spec)

    async def _on_produce(self, spec: Any, data: Any) -> None:
        """Producer 回调返回数据后的处理流程。"""
        try:
            await self._publish_data(
                topic=spec.name,
                data=data,
                cache_size=spec.cache_size,
                serializer=spec.serializer,
                compression=spec.compression,
            )
        except Exception:
            logger.warning("Producer %s 消息处理异常", spec.name, exc_info=True)

    async def _publish_data(
        self,
        *,
        topic: str,
        data: Any,
        cache_size: int,
        serializer: str,
        compression: str,
    ) -> None:
        """校验、编码并发送一条消息。"""
        # 1. 类型白名单校验 + record_count 推断
        record_count = self._infer_record_count(data)
        # 2. 数据类型 ↔ 序列化器强绑定校验（str→str, bytes→bytes 等）
        self._validate_serializer(data, serializer)
        # 3. 推断原始数据类型标记（v3：供 sub 端还原原始类型）
        data_type = self._infer_data_type(data)
        payload_obj = self._prepare_payload(data)

        # 4. 序列化 + 压缩 + 编码帧
        encoded_frames = frame_codec.encode(
            topic=topic,
            data=payload_obj,
            serializer=serializer,
            compression=compression,
            record_count=record_count,
            data_type=data_type,
        )

        # 5. 并行分发
        if self._transport is None:
            raise RuntimeError("Publisher transport 未启动")
        await self._transport.send(encoded_frames)

        # 6. 同步操作：缓存 + 统计
        ts_ns = frame_codec._TS_STRUCT.unpack(encoded_frames[2])[0]
        self._buffers.get_or_create(topic, cache_size).append(
            ts_ns, encoded_frames, record_count
        )
        self._traffic.record(topic, record_count, len(encoded_frames[3]))

    @staticmethod
    def _infer_record_count(data: Any) -> int:
        """推断记录数。

        仅支持 4 种白名单类型，其余抛 TypeError：
        - pd.DataFrame → 行数 len(df)
        - dict / str / bytes → 1

        list 不再作为 producer 返回类型支持。
        """
        try:
            import pandas as pd
        except ImportError:
            pd = None  # type: ignore[assignment]

        # 单个 DataFrame
        if pd is not None and isinstance(data, pd.DataFrame):
            return len(data)

        if isinstance(data, list):
            raise TypeError("不支持的返回类型: list。仅支持 pd.DataFrame / dict / str / bytes。")

        # dict / str / bytes → 1
        if isinstance(data, (dict, str, bytes)):
            return 1

        # 白名单外（标量 int/float/bool、pa.Table、set、tuple 等）
        raise TypeError(
            f"不支持的返回类型: {type(data).__name__}。"
            f"仅支持 pd.DataFrame / dict / str / bytes。"
        )

    @staticmethod
    def _infer_data_type(data: Any) -> int:
        """推断原始数据类型，返回 DataType 常量（v3 新增）。

        用于在 meta 帧记录原始 Python 类型，让 sub 端能还原原始类型
        （如 DataFrame → DataFrame，而非降级为 list[dict]）。
        """
        from pulsemq.protocol.msg_type import DataType

        try:
            import pandas as pd
        except ImportError:
            pd = None  # type: ignore[assignment]

        if pd is not None and isinstance(data, pd.DataFrame):
            return DataType.DATAFRAME
        if isinstance(data, dict):
            return DataType.DICT
        if isinstance(data, str):
            return DataType.STR
        if isinstance(data, bytes):
            return DataType.BYTES
        return DataType.UNKNOWN

    @staticmethod
    def _validate_serializer(data: Any, serializer: str) -> None:
        """校验数据类型与序列化器的匹配（方案 A：强类型绑定）。

        规则（收紧后）：
        - str 数据 → 只允许 'str' 序列化器
        - bytes 数据 → 只允许 'bytes' 序列化器
        - pd.DataFrame / dict → 允许 msgpack/json/pyarrow

        不匹配抛 TypeError，提示正确的序列化器。
        """
        try:
            import pandas as pd
        except ImportError:
            pd = None  # type: ignore[assignment]

        if isinstance(data, list):
            raise TypeError("list 数据不再支持，请返回 pd.DataFrame / dict / str / bytes。")

        # str / bytes：各自唯一序列化器
        if isinstance(data, str) and serializer != "str":
            raise TypeError(
                f"str 数据必须用 serializer='str'，当前为 '{serializer}'。"
            )
        if isinstance(data, bytes) and serializer != "bytes":
            raise TypeError(
                f"bytes 数据必须用 serializer='bytes'，当前为 '{serializer}'。"
            )
        if isinstance(data, str) or isinstance(data, bytes):
            return  # str/bytes 已匹配，通过

        # 结构化数据：判断数据族系
        is_dataframe_family = (
            (pd is not None and isinstance(data, pd.DataFrame))
            or isinstance(data, dict)
        )

        if is_dataframe_family:
            allowed = {"msgpack", "json", "pyarrow"}
            if serializer not in allowed:
                raise TypeError(
                    f"结构化数据（DataFrame/dict）应使用 msgpack/json/pyarrow，"
                    f"当前为 '{serializer}'。"
                )

    @staticmethod
    def _prepare_payload(data: Any) -> Any:
        """预处理数据为可序列化格式。"""
        try:
            import pandas as pd
            if isinstance(data, pd.DataFrame):
                return data.to_dict(orient="records")
        except ImportError:
            pass
        return data

    def _system_snapshot(self) -> dict:
        """系统快照（给 admin SSE 用）。"""
        return {
            "start_time": self._start_time,
            "producer_count": len(self._producer_mgr.specs),
        }

    # ---- 心跳 ----

    async def _heartbeat_loop(self) -> None:
        """心跳发送循环：每隔 heartbeat_interval 秒发送一条 PING 帧。"""
        from pulsemq.protocol.frames import encode_heartbeat

        while self._running:
            await asyncio.sleep(self._config.heartbeat_interval)
            if not self._running:
                break
            try:
                frames = encode_heartbeat()
                if self._transport is not None:
                    await self._transport.send(frames)
            except asyncio.CancelledError:
                break
            except Exception:
                logger.warning("心跳发送异常", exc_info=True)

    # ---- 分钟滚动 ----

    async def _minute_roll_loop(self) -> None:
        """每分钟执行一次：归档统计 → SQLite 落库。"""
        while self._running:
            now = time.time()
            next_minute = (int(now) // 60 + 1) * 60
            await asyncio.sleep(next_minute - now)
            if not self._running:
                break

            archived = self._traffic.roll_minute()
            if self._storage and archived:
                self._storage.save_minutes_batch(archived)

            # 每小时清理过期数据
            if int(next_minute) % 3600 < 70:
                if self._storage:
                    self._storage.cleanup()


def main() -> None:
    """CLI 入口点。提供最小示例 publisher。"""
    import sys
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        stream=sys.stderr,
    )
    pub = PulsePublisher()
    print(format_startup_table(pub._config, pub._explicit_api_keys), file=sys.stderr)
    print("用法: 参考 PulsePublisher 文档注册 producer", file=sys.stderr)
    pub.start()


def format_startup_table(
    cfg: PublisherConfig,
    api_keys: dict[str, str] | None = None,
    version: str = __version__,
) -> str:
    """生成启动配置表格字符串（不打印，便于测试）。

    字段: bind、admin_bind、auth 状态（用户列表脱敏，最多展示 10 个）。
    """
    keys = api_keys or cfg.api_keys
    if keys:
        names = sorted(keys)
        if len(names) > 10:
            shown = ", ".join(names[:5])
            user_str = f"{len(names)} users: {shown}, ... +{len(names) - 5} more"
        else:
            user_str = f"enabled ({len(names)} users: {', '.join(names)})"
    else:
        user_str = "disabled"

    bar = "=" * 43
    return "\n".join(
        [
            bar,
            f"  PulseMQ Publisher v{version}",
            bar,
            f"  bind              {cfg.bind}",
            f"  admin             {cfg.admin_bind}",
            f"  auth              {user_str}",
            bar,
        ]
    )


if __name__ == "__main__":
    main()
