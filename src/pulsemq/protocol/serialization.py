"""序列化注册表 + 内置实现。

支持: str, bytes, msgpack, json, pyarrow
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from io import BytesIO
from typing import Any

# 模块级缓存后端 import，避免热路径每次 serialize/deserialize 重复 import 查找
# （与 frames.py 的 _pd 模式对齐）
try:
    import msgspec as _msgspec
except ImportError:
    _msgspec = None

try:
    import pyarrow as _pa
except ImportError:
    _pa = None

try:
    import pandas as _pd
except ImportError:
    _pd = None


# ---------------------------------------------------------------------------
# 抽象接口
# ---------------------------------------------------------------------------


class Serializer(ABC):
    """序列化器抽象接口。"""

    @abstractmethod
    def serialize(self, obj: Any) -> bytes: ...

    @abstractmethod
    def deserialize(self, data: bytes) -> Any: ...


# ---------------------------------------------------------------------------
# 序列化器实现
# ---------------------------------------------------------------------------


class StringSerializer(Serializer):
    """字符串序列化：str ↔ UTF-8 bytes。"""

    def serialize(self, obj: Any) -> bytes:
        if isinstance(obj, str):
            return obj.encode("utf-8")
        if isinstance(obj, bytes):
            return obj
        raise TypeError(f"str 序列化只接受 str 或 bytes，收到 {type(obj).__name__}")

    def deserialize(self, data: bytes) -> str:
        return data.decode("utf-8")


class MsgpackSerializer(Serializer):
    """msgpack 二进制序列化（msgspec 后端）。"""

    def serialize(self, obj: Any) -> bytes:
        if _msgspec is None:
            raise ImportError("msgspec 未安装")
        return _msgspec.msgpack.encode(obj)

    def deserialize(self, data: bytes) -> Any:
        if _msgspec is None:
            raise ImportError("msgspec 未安装")
        return _msgspec.msgpack.decode(data)


class JsonSerializer(Serializer):
    """JSON 文本序列化 (msgspec.json)。"""

    def serialize(self, obj: Any) -> bytes:
        if _msgspec is None:
            raise ImportError("msgspec 未安装")
        if isinstance(obj, (bytes, bytearray)):
            # msgspec.json 会把 bytes 编码为 base64 字符串，但解码后变成 str，
            # 类型不一致。明确拒绝，提示改用 bytes 序列化器或 msgpack。
            raise TypeError(
                "json 序列化不支持 bytes（解码后类型会变形为 str）。"
                "请改用 serializer='bytes'（二进制透传）或 'msgpack'（通用）。"
            )
        return _msgspec.json.encode(obj)

    def deserialize(self, data: bytes) -> Any:
        if _msgspec is None:
            raise ImportError("msgspec 未安装")
        return _msgspec.json.decode(data)


class PyArrowSerializer(Serializer):
    """PyArrow IPC 流式序列化。

    支持 pa.Table / pd.DataFrame / dict / list[dict]。
    """

    def serialize(self, obj: Any) -> bytes:
        if _pa is None:
            raise ImportError("pyarrow 未安装")
        if _pd is None:
            raise ImportError("pandas 未安装")

        table = None
        if isinstance(obj, _pa.Table):
            table = obj
        else:
            if isinstance(obj, _pd.DataFrame):
                table = _pa.Table.from_pandas(obj, preserve_index=False)
            elif isinstance(obj, list) and obj and isinstance(obj[0], dict):
                df = _pd.DataFrame(obj)
                table = _pa.Table.from_pandas(df, preserve_index=False)
            elif isinstance(obj, dict):
                df = _pd.DataFrame([obj])
                table = _pa.Table.from_pandas(df, preserve_index=False)

        if table is None:
            # 不支持的类型：避免静默退回 msgpack 导致 flags 标记与实际编码不一致
            # （订阅端按 pyarrow 解码会失败）。明确报错，提示用户换序列化器。
            raise TypeError(
                f"pyarrow 序列化只支持 pa.Table / pd.DataFrame / dict / list[dict]，"
                f"收到 {type(obj).__name__}。"
                f"对于 list[基础类型] 或标量，请改用 serializer='msgpack'（通用）/"
                f"'json'（可读）/'str'（纯文本）/'bytes'（二进制透传）。"
            )

        sink = BytesIO()
        writer = _pa.ipc.new_stream(sink, table.schema)
        writer.write_table(table)
        writer.close()
        return sink.getvalue()

    def deserialize(self, data: bytes) -> Any:
        if _pa is None:
            raise ImportError("pyarrow 未安装")

        reader = _pa.ipc.open_stream(BytesIO(data))
        return reader.read_all()


class BytesSerializer(Serializer):
    """纯字节透传。"""

    def serialize(self, obj: Any) -> bytes:
        if not isinstance(obj, bytes):
            raise TypeError(f"bytes 序列化只接受 bytes，收到 {type(obj).__name__}")
        return obj

    def deserialize(self, data: bytes) -> bytes:
        return data


# ---------------------------------------------------------------------------
# 注册表
# ---------------------------------------------------------------------------

_REGISTRY: dict[str, Serializer] = {}


def register(name: str, serializer: Serializer) -> None:
    _REGISTRY[name] = serializer


def get(name: str) -> Serializer:
    if name not in _REGISTRY:
        raise KeyError(f"未注册的序列化格式: {name}")
    return _REGISTRY[name]


def available() -> list[str]:
    return list(_REGISTRY.keys())


# ---------------------------------------------------------------------------
# 自动注册内置实现
# ---------------------------------------------------------------------------


def _init_builtins() -> None:
    register("str", StringSerializer())
    register("msgpack", MsgpackSerializer())
    register("json", JsonSerializer())
    register("bytes", BytesSerializer())
    try:
        register("pyarrow", PyArrowSerializer())
    except ImportError:
        pass


_init_builtins()
