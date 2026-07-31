"""Producer 管线的类型单一来源。

集中定义数据白名单类型与 producer 回调签名别名，
供 ProducerManager 复用，
与运行时 frames.encode 的类型校验语义一一对应。
"""

from __future__ import annotations

from typing import TypeAlias, Callable, Awaitable, Union

import pandas as pd

# —— 数据白名单：producer 回调返回值的合法类型 ——
# 与运行时白名单（DataFrame/dict/str/bytes）一一对应
PubData: TypeAlias = Union[pd.DataFrame, dict, bytes, str]

# producer 回调签名：async def fn() -> PubData | None
SimpleProducerCallback: TypeAlias = Callable[[], Awaitable[PubData | None]]

# 统一别名（注册入口用）
ProducerCallback: TypeAlias = SimpleProducerCallback
