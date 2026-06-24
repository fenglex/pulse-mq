"""Producer 管线的类型单一来源。

集中定义数据白名单类型与 producer 回调签名别名，
供 ProducerManager / PulsePublisher / PublisherSender 复用，
与运行时 _infer_record_count / _validate_serializer 的白名单语义一一对应。
"""

from __future__ import annotations

from typing import TypeAlias, Callable, Awaitable, Union

import pandas as pd

# —— 数据白名单：sender.send() 和 producer 回调返回值的合法类型 ——
# 与运行时白名单（DataFrame/dict/str/bytes）一一对应
PubData: TypeAlias = Union[pd.DataFrame, dict, bytes, str]

# —— 两种回调形态 ——
# 无 sender 注入：async def fn() -> PubData | None
SimpleProducerCallback: TypeAlias = Callable[[], Awaitable[PubData | None]]

# 有 sender 注入：async def fn(sender: PublisherSender) -> PubData | None
# PublisherSender 做前向引用（字符串注解），避免循环导入
# （types.py 不 import publisher.py；publisher.py 导入 types.py）
SenderProducerCallback: TypeAlias = Callable[["PublisherSender"], Awaitable[PubData | None]]

# 统一别名（注册入口用）：两种形态的并集
ProducerCallback: TypeAlias = Union[SimpleProducerCallback, SenderProducerCallback]
