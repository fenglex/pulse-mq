"""PyArrow IPC 序列化器 — 支持高效 DataFrame 传输。"""

from __future__ import annotations

from io import BytesIO
from typing import Any

import pyarrow as pa

from pulsemq.serialization.registry import Serializer


class PyArrowSerializer(Serializer):
    """PyArrow IPC 流式序列化。

    支持输入类型:
      - pa.Table / pd.DataFrame → 序列化为 Arrow IPC stream
      - dict (单条) → 自动转为 1 行 pa.Table 再序列化
    """

    def serialize(self, obj: Any) -> bytes:
        if isinstance(obj, pa.Table):
            table = obj
        else:
            # 尝试转为 DataFrame → Table
            import pandas as pd

            if isinstance(obj, pd.DataFrame):
                table = pa.Table.from_pandas(obj, preserve_index=False)
            elif isinstance(obj, dict):
                # 单条 dict → 1 行 DataFrame
                df = pd.DataFrame([obj])
                table = pa.Table.from_pandas(df, preserve_index=False)
            else:
                # fallback: 用 msgpack
                import msgpack
                return msgpack.packb(obj, use_bin_type=True)

        sink = BytesIO()
        writer = pa.ipc.new_stream(sink, table.schema)
        writer.write_table(table)
        writer.close()
        return sink.getvalue()

    def deserialize(self, data: bytes) -> Any:
        reader = pa.ipc.open_stream(BytesIO(data))
        table = reader.read_all()
        return table
