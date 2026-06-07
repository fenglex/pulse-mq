"""延迟加载 Cython 扩展, fallback 到纯 Python 实现。

外部代码统一通过 ``from pulsemq.serialization._df_msgpack_loader
import encode_dataframe_to_msgpack`` 调用, 不需要关心后端是 Cython 还是纯 Python.
"""
from __future__ import annotations

__all__ = ["encode_dataframe_to_msgpack", "is_using_cython"]


try:
    from pulsemq.serialization._df_msgpack import encode_dataframe_to_msgpack
    _USING_CYTHON = True
except ImportError:
    from pulsemq.serialization._df_msgpack_py import encode_dataframe_to_msgpack
    _USING_CYTHON = False


def is_using_cython() -> bool:
    """返回当前是否使用 Cython 扩展 (False 表示 fallback 到纯 Python)."""
    return _USING_CYTHON
