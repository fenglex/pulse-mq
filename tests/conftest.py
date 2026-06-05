import asyncio
import sys

import pytest

# Windows 下 ZMQ asyncio 需要 SelectorEventLoop（不支持 ProactorEventLoop）
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
