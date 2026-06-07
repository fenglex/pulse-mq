"""纯 Python fallback: DataFrame → msgpack bytes.

与 Cython 版本输出等价, 用 ``df.to_dict(orient='records') + msgpack.packb``.
"""
from __future__ import annotations

import msgpack

__all__ = ["encode_dataframe_to_msgpack"]


def encode_dataframe_to_msgpack(df, use_bin_type: bool = True) -> bytes:
    """把 DataFrame 编码为 msgpack bytes, 纯 Python fallback.

    Args:
        df: pandas DataFrame.
        use_bin_type: 透传给 msgpack.packb.

    Returns:
        msgpack 编码后的 bytes.
    """
    return msgpack.packb(
        df.to_dict(orient="records"),
        use_bin_type=use_bin_type,
    )
