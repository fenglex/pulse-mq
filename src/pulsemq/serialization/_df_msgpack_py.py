"""纯 Python fallback: DataFrame → msgpack bytes.

与 Cython 版本输出等价, 用 ``df.to_dict(orient='records') + msgspec.msgpack.encode``.
"""
from __future__ import annotations

import msgspec

__all__ = ["encode_dataframe_to_msgpack"]


def encode_dataframe_to_msgpack(df, use_bin_type: bool = True) -> bytes:
    """把 DataFrame 编码为 msgpack bytes, 纯 Python fallback.

    Args:
        df: pandas DataFrame.
        use_bin_type: 仅用于 API 兼容, msgspec 默认 str→str/bytes→bin, 等价 use_bin_type=True.

    Returns:
        msgpack 编码后的 bytes.
    """
    return msgspec.msgpack.encode(df.to_dict(orient="records"))
