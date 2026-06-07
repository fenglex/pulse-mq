# distutils: language = c
# cython: language_level=3, boundscheck=False, wraparound=False, cdivision=True
"""Cython 加速的 DataFrame → msgpack bytes 编码。

策略: 用 typed numpy 缓冲区直接读列, 避免 pandas.to_dict 的 Python-level 包装.
保持与 ``df.to_dict(orient="records") + msgspec.msgpack.encode`` 输出完全一致.
"""
from __future__ import annotations

cimport cython
cimport numpy as cnp
import numpy as np
import msgspec
import msgspec.msgpack

__all__ = ["encode_dataframe_to_msgpack"]


cdef inline object _scalar_to_py(object arr, Py_ssize_t i):
    """读 numpy 数组第 i 个元素, 转原生 Python 标量.

    优先用 C-level PyArray_GETITEM, 跳过 Python __getitem__ wrapper.
    """
    if cnp.PyArray_CheckExact(arr):
        return cnp.PyArray_GETITEM(arr, <char *>cnp.PyArray_GETPTR1(arr, i))
    # 非 numpy 数组 (例如 ArrowStringArray): 走 Python 索引
    return arr[i]


cpdef bytes encode_dataframe_to_msgpack(object df, bint use_bin_type=True):
    """把 DataFrame 编码为 msgpack bytes, Cython 加速版本."""
    cdef:
        Py_ssize_t n_rows = len(df)
        Py_ssize_t n_cols = len(df.columns)
        list columns = df.columns.tolist()
        list records
        dict record
        Py_ssize_t i, j
        object col
        list arrays
    if n_rows == 0:
        # msgspec 默认 str→str, bytes→bin, 等价于 msgpack.packb(use_bin_type=True)
        return msgspec.msgpack.encode([])
    if n_cols == 0:
        records = []
        for i in range(n_rows):
            records.append({})
        return msgspec.msgpack.encode(records)

    # 缓存每列的底层数组 (.values 优先, 退化为 to_numpy())
    arrays = []
    for col in columns:
        col_obj = df[col]
        col_arr = getattr(col_obj, "values", None)
        if col_arr is None:
            col_arr = col_obj.to_numpy()
        arrays.append(col_arr)
    records = []
    cdef list records_view = records
    for i in range(n_rows):
        record = {}
        for j in range(n_cols):
            col = arrays[j]
            record[columns[j]] = _scalar_to_py(col, i)
        records_view.append(record)
    return msgspec.msgpack.encode(records_view)
