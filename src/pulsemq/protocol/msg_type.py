"""帧类型。Spec 1：DATA/CONTROL/HEARTBEAT/ADMIN。"""


class MsgType:
    DATA = 0x01
    CONTROL = 0x02
    HEARTBEAT = 0x03
    ADMIN = 0x04


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
