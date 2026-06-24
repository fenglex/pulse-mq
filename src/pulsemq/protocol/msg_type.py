"""消息类型常量。

v2 简化：无 broker 不需要 AUTH/SUB/UNSUB/QUERY 等控制消息，
仅保留 DATA 和 PING。

v3：meta 帧扩展为 7 字节，新增 Byte 2 = data_type（原始数据类型标记），
让 sub 端能把反序列化结果还原为 pub 端的原始 Python 类型
（如 DataFrame → DataFrame）。
"""

from __future__ import annotations


class MsgType:
    """消息类型常量，对应 meta 帧 Byte 0。"""

    DATA = 0x01
    PING = 0x02


class DataType:
    """原始数据类型标记，对应 meta 帧 Byte 2（v3 新增）。

    pub 端在 encode 时记录原始 Python 类型，sub 端 decode 后据此
    把反序列化结果还原为原始类型，实现全链路类型保真。
    """

    UNKNOWN = 0x00    # 兜底（未知/不可还原）
    DICT = 0x01       # dict
    DATAFRAME = 0x02  # pd.DataFrame
    STR = 0x03        # str
    BYTES = 0x04      # bytes
