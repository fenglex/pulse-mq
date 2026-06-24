"""诊断：pub→sub 全链路类型保真度排查。

逐一测试 4 种白名单类型 × 兼容序列化器，对比 pub 端发送类型 vs sub 端接收类型，
找出所有"类型变形"的组合。

运行：
    uv run python scripts/_diag_type_fidelity.py
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys

import pandas as pd

import pulsemq  # noqa: F401
from pulsemq import PulseSubscriber
from pulsemq.config import PublisherConfig
from pulsemq.publisher import PulsePublisher

logging.basicConfig(level=logging.WARNING, stream=sys.stderr)

PUB_BIND = "tcp://127.0.0.1:15558"
ADMIN_BIND = "127.0.0.1:19093"


def make_samples():
    """构造 4 种类型的样本 + 兼容的序列化器。"""
    return [
        ("str", "hello-world", ["str"]),
        ("bytes", b"\x00\x01\x02\x03", ["bytes"]),
        ("dict", {"a": 1, "b": 2.5}, ["msgpack", "json", "pyarrow"]),
        ("DataFrame", pd.DataFrame({"a": [1, 2], "b": [1.5, 2.5]}),
         ["msgpack", "json", "pyarrow"]),
    ]


def type_name(obj) -> str:
    """简短类型名。"""
    if isinstance(obj, list) and obj:
        return f"list[{type_name(obj[0])}]"
    if isinstance(obj, dict):
        return "dict"
    return type(obj).__name__


async def test_one(shape: str, sample, serializer: str, port: int) -> dict:
    """测一个组合，返回 {shape, serializer, pub_type, sub_type, equal, note}。"""
    topic = f"t_{abs(hash(shape + serializer)) % 100000}"
    pub = PulsePublisher(
        config=PublisherConfig(
            bind=f"tcp://127.0.0.1:{port}",
            admin_bind=f"127.0.0.1:{port + 100}",
            stats_db=f"sqlite://./_diag_fid_{port}.sqlite",
        )
    )

    async def _f():
        return sample

    pub.register_producer(fn=_f, name=topic, interval=0.1, serializer=serializer)

    pub_task = asyncio.create_task(pub._run())
    await asyncio.sleep(0.8)

    sub = PulseSubscriber(f"tcp://127.0.0.1:{port}")
    await sub.connect()

    pub_type = type_name(sample)
    result = {"shape": shape, "serializer": serializer,
              "pub_type": pub_type, "sub_type": None,
              "equal": False, "note": ""}

    try:
        async def _consume():
            async for msg in sub.subscribe(topic):
                return msg.payload

        payload = await asyncio.wait_for(_consume(), timeout=3.0)
        result["sub_type"] = type_name(payload)

        # 值相等比较（v3：sub 端已还原原始类型，按类型分别处理）
        try:
            if isinstance(sample, pd.DataFrame):
                import pandas as pd_test
                pd_test.testing.assert_frame_equal(
                    payload.reset_index(drop=True),
                    sample.reset_index(drop=True),
                    check_dtype=False, check_like=True,
                )
                result["equal"] = True
            else:
                result["equal"] = (payload == sample)
        except Exception:
            result["equal"] = False
    except asyncio.TimeoutError:
        result["note"] = "接收超时"
    except Exception as e:
        result["note"] = f"异常: {type(e).__name__}: {str(e)[:80]}"
    finally:
        await sub.close()

    pub._running = False
    await asyncio.sleep(0.2)
    pub_task.cancel()
    with __import__("contextlib").suppress(asyncio.CancelledError, Exception):
        await pub_task

    for ext in ("", "-shm", "-wal"):
        try:
            os.unlink(f"./_diag_fid_{port}.sqlite{ext}")
        except OSError:
            pass

    return result


async def main():
    samples = make_samples()
    port = 16000
    results = []
    for shape, sample, serializers in samples:
        for ser in serializers:
            port += 1
            r = await test_one(shape, sample, ser, port)
            results.append(r)
            mark = "✅" if r["pub_type"] == r["sub_type"] else "❌类型变形"
            eq = "值✓" if r["equal"] else "值✗"
            note = f" [{r['note']}]" if r["note"] else ""
            print(f"  {mark} {r['shape']:<18} + {ser:<8} | "
                  f"pub={r['pub_type']:<16} sub={r['sub_type']:<16} {eq}{note}")

    print("\n" + "=" * 70)
    deformed = [r for r in results if r["pub_type"] != r["sub_type"]]
    print(f"类型保真：{len(results) - len(deformed)}/{len(results)} 保真，"
          f"{len(deformed)} 个变形")
    if deformed:
        print("\n❌ 类型变形的组合：")
        for r in deformed:
            print(f"   {r['shape']} + {r['serializer']}: "
                  f"{r['pub_type']} → {r['sub_type']}")


if __name__ == "__main__":
    asyncio.run(main())
