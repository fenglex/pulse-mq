"""全矩阵 pub → sub benchmark 测试。

遍历所有合法的 (serializer × compression × data_shape) 组合，
对每个组合做完整的 pub→sub 往返测试：
1. 正确性验证：pub 端发送的数据在 sub 端完整还原
2. 性能指标：吞吐量、帧延迟 p50/p90/p99/max、压缩率
3. 兼容性：非法组合自动跳过

用法:
    python scripts/bench_pubsub_matrix.py
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import random
import sys
import time
from dataclasses import dataclass, field
from typing import Any

# Windows: 强制 Selector 事件循环
if sys.platform == "win32" and hasattr(asyncio, "WindowsSelectorEventLoopPolicy"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import pandas as pd

from pulsemq.config import PublisherConfig
from pulsemq.protocol.frames import PulseMessage, encode as frame_encode
from pulsemq.publisher import PulsePublisher
from pulsemq.subscriber import PulseSubscriber

# ---------------------------------------------------------------------------
# 配置
# ---------------------------------------------------------------------------

SERIALIZERS = ["msgpack", "json", "str", "bytes", "pyarrow"]
COMPRESSIONS = ["none", "snappy", "lz4", "zstd"]
DATA_SHAPES = ["scalar_str", "scalar_bytes", "list_dict", "dataframe", "large_dict"]

# 每个 topic 发送多少条消息后停止
MESSAGES_PER_TOPIC = 50
# 每条消息包含多少条记录（对批量类型）
RECORDS_PER_MSG = 100
# 帧间隔（秒），0 表示 burst
PUBLISH_INTERVAL = 0.001
# 单条记录的随机载荷大小
PAYLOAD_BYTES = 64

_RANDOM_STR = os.urandom(PAYLOAD_BYTES).hex()[:PAYLOAD_BYTES]


# ---------------------------------------------------------------------------
# 兼容性矩阵
# ---------------------------------------------------------------------------


def is_compatible(ser: str, shape: str) -> bool:
    """判断 (serializer, data_shape) 是否为合法组合。"""
    if ser == "str" and shape != "scalar_str":
        return False
    if ser == "bytes" and shape != "scalar_bytes":
        return False
    if ser == "json" and shape == "scalar_bytes":
        return False
    if ser == "pyarrow" and shape != "dataframe":
        return False
    return True


# ---------------------------------------------------------------------------
# 数据生成
# ---------------------------------------------------------------------------


def make_value(shape: str, seq: int = 0) -> Any:
    """根据形态生成测试值（面向 benchmark，每批含 RECORDS_PER_MSG 条记录）。"""
    if shape == "scalar_str":
        return f"seq-{seq}-{_RANDOM_STR}"
    if shape == "scalar_bytes":
        return seq.to_bytes(4, "big") + os.urandom(PAYLOAD_BYTES)
    if shape == "list_dict":
        return [{"seq": seq * RECORDS_PER_MSG + i, "v": float(i), "d": _RANDOM_STR}
                for i in range(RECORDS_PER_MSG)]
    if shape == "dataframe":
        return pd.DataFrame({
            "seq": [seq * RECORDS_PER_MSG + i for i in range(RECORDS_PER_MSG)],
            "price": [100.0 + i * 0.01 for i in range(RECORDS_PER_MSG)],
            "volume": [1000 + i for i in range(RECORDS_PER_MSG)],
            "code": [_RANDOM_STR for _ in range(RECORDS_PER_MSG)],
        })
    if shape == "large_dict":
        return {"seq": seq, "payload": "x" * 50_000}
    raise ValueError(f"未知 data shape: {shape}")


def infer_record_count(data: Any) -> int:
    """推断记录数（与 publisher._infer_record_count 保持一致）。"""
    if isinstance(data, list):
        return len(data)
    if hasattr(data, "__len__") and hasattr(data, "columns"):
        return len(data)
    return 1


def prepare_payload(data: Any) -> Any:
    """预处理数据为可序列化格式（与 publisher._prepare_payload 保持一致）。

    Publisher 在序列化前会把 DataFrame 转为 list[dict]。
    """
    try:
        import pandas as pd
        if isinstance(data, pd.DataFrame):
            return data.to_dict(orient="records")
        if isinstance(data, list):
            result = []
            for item in data:
                if isinstance(item, pd.DataFrame):
                    result.extend(item.to_dict(orient="records"))
                else:
                    result.append(item)
            return result
    except ImportError:
        pass
    return data


# ---------------------------------------------------------------------------
# 延迟统计
# ---------------------------------------------------------------------------


@dataclass
class BenchResult:
    """单个 (ser, comp, shape) 组合的 benchmark 结果。"""
    serializer: str
    compression: str
    data_shape: str
    messages_sent: int = 0
    messages_recv: int = 0
    records_sent: int = 0
    records_recv: int = 0
    raw_bytes_total: int = 0      # 序列化后（压缩前）字节总数
    compressed_bytes_total: int = 0  # 压缩后字节总数
    latencies_ms: list[float] = field(default_factory=list)
    elapsed_sec: float = 0.0
    error: str = ""
    correctness_ok: bool = False

    @property
    def compression_ratio(self) -> float:
        if self.raw_bytes_total == 0:
            return 0.0
        return self.compressed_bytes_total / self.raw_bytes_total

    @property
    def throughput_records_per_sec(self) -> float:
        if self.elapsed_sec == 0:
            return 0.0
        return self.records_recv / self.elapsed_sec

    @property
    def throughput_msgs_per_sec(self) -> float:
        if self.elapsed_sec == 0:
            return 0.0
        return self.messages_recv / self.elapsed_sec

    def latency_percentile(self, p: float) -> float:
        if not self.latencies_ms:
            return 0.0
        s = sorted(self.latencies_ms)
        idx = min(int(len(s) * p), len(s) - 1)
        return s[idx]


# ---------------------------------------------------------------------------
# 主 benchmark 函数
# ---------------------------------------------------------------------------


def _rand_port() -> int:
    return random.randint(30000, 50000)


async def bench_one(
    ser: str,
    comp: str,
    shape: str,
) -> BenchResult:
    """对单个 (ser, comp, shape) 组合做 pub→sub benchmark。"""
    result = BenchResult(serializer=ser, compression=comp, data_shape=shape)

    pub_port = _rand_port()
    admin_port = _rand_port()
    while admin_port == pub_port:
        admin_port = _rand_port()

    topic = f"bench_{ser}_{comp}_{shape}"

    pub = PulsePublisher(
        config=PublisherConfig(
            bind=f"tcp://127.0.0.1:{pub_port}",
            admin_bind=f"127.0.0.1:{admin_port}",
            stats_db="sqlite://:memory:",
        ),
    )

    counter = {"n": 0}
    go = asyncio.Event()

    async def producer_fn() -> Any:
        await go.wait()
        counter["n"] += 1
        if counter["n"] > MESSAGES_PER_TOPIC:
            return None
        return make_value(shape, counter["n"])

    pub._producer_mgr.register_burst(
        callback=producer_fn,
        name=topic,
        serializer=ser,
        compression=comp,
    )
    pub._buffers.get_or_create(topic, MESSAGES_PER_TOPIC * 2)

    received: list[PulseMessage] = []

    # 启动 publisher
    pub_task = asyncio.create_task(pub._run())
    await asyncio.sleep(0.3)

    # 启动 subscriber
    sub = PulseSubscriber(f"tcp://127.0.0.1:{pub_port}")
    await sub.connect()

    async def collect() -> None:
        async for msg in sub.subscribe(topic):
            now_ns = time.time_ns()
            lat_ms = (now_ns - msg.timestamp_ns) / 1_000_000
            received.append(msg)
            result.latencies_ms.append(lat_ms)
            if len(received) >= MESSAGES_PER_TOPIC:
                break

    collect_task = asyncio.create_task(collect())
    await asyncio.sleep(0.2)

    # 释放 burst
    t_start = time.monotonic()
    go.set()

    # 等收集完成
    try:
        await asyncio.wait_for(collect_task, timeout=60.0)
    except asyncio.TimeoutError:
        result.error = f"超时：只收到 {len(received)}/{MESSAGES_PER_TOPIC} 条消息"
    except Exception as e:
        result.error = f"异常：{e}"

    t_end = time.monotonic()
    result.elapsed_sec = t_end - t_start

    # 关闭
    pub._running = False
    await asyncio.sleep(0.2)
    pub_task.cancel()
    with contextlib.suppress(asyncio.CancelledError, Exception):
        await pub_task
    await sub.close()

    # 填充结果
    result.messages_sent = min(counter["n"], MESSAGES_PER_TOPIC)
    result.messages_recv = len(received)

    if not result.error and len(received) > 0:
        # 计算吞吐
        total_records = 0
        total_compressed = 0
        for msg in received:
            total_records += msg.record_count
            total_compressed += len(msg.raw_payload)
        result.records_recv = total_records
        result.compressed_bytes_total = total_compressed

        # 计算 raw bytes（序列化后压缩前，匹配 publisher 的 prepare_payload 流程）
        from pulsemq.protocol import serialization as ser_mod
        from pulsemq.protocol import compression as comp_mod
        total_raw = 0
        ser_obj = ser_mod.get(ser)
        for i in range(1, min(counter["n"], MESSAGES_PER_TOPIC) + 1):
            val = make_value(shape, i)
            payload = prepare_payload(val)
            try:
                raw = ser_obj.serialize(payload)
                total_raw += len(raw)
            except Exception:
                pass
        result.raw_bytes_total = total_raw
        result.records_sent = result.messages_sent * (
            RECORDS_PER_MSG if shape in ("list_dict", "dataframe") else 1
        )

        # 正确性验证：对比第一条消息
        try:
            expected_first = make_value(shape, 1)
            msg_first = received[0]
            _assert_payload_match(msg_first, expected_first, ser, shape)
            result.correctness_ok = True
        except AssertionError as e:
            result.error = f"正确性验证失败: {e}"
            result.correctness_ok = False
    elif not result.error:
        result.error = "未收到任何消息"
        result.correctness_ok = False

    return result


def _assert_payload_match(
    msg: PulseMessage,
    expected: Any,
    ser: str,
    shape: str,
) -> None:
    """验证 sub 端收到的 payload 与 pub 端发送的一致。"""
    assert msg.serializer == ser, f"serializer 不匹配: {msg.serializer} != {ser}"
    assert msg.compression != "", "compression 为空"
    assert msg.timestamp_ns > 0, "timestamp_ns 非正"

    if shape == "dataframe":
        # publisher 会把 DataFrame 转为 list[dict]，pyarrow 路径 deserialize 返回 pa.Table
        got = msg.payload
        if hasattr(got, "to_pydict"):
            import pyarrow as pa
            got = got.to_pydict()
            expected_dict = expected.to_dict(orient="list")
            for col in expected_dict:
                assert col in got, f"缺少列: {col}"
        elif hasattr(got, "to_dict"):
            got_records = got.to_dict(orient="records")
            expected_records = expected.to_dict(orient="records")
            assert len(got_records) == len(expected_records), (
                f"行数不匹配: {len(got_records)} != {len(expected_records)}"
            )
        else:
            # msgpack/json 路径下 payload 是 list[dict]
            expected_records = expected.to_dict(orient="records")
            assert isinstance(got, list), f"期望 list，得到 {type(got)}"
    elif shape == "list_dict":
        assert isinstance(msg.payload, list), f"期望 list，得到 {type(msg.payload)}"
        assert len(msg.payload) == RECORDS_PER_MSG, (
            f"记录数不匹配: {len(msg.payload)} != {RECORDS_PER_MSG}"
        )
    elif shape == "scalar_str":
        assert isinstance(msg.payload, str), f"期望 str，得到 {type(msg.payload)}"
        assert msg.payload.startswith("seq-1-"), f"payload 前缀不对: {msg.payload[:20]}"
    elif shape == "scalar_bytes":
        assert isinstance(msg.payload, bytes), f"期望 bytes，得到 {type(msg.payload)}"
    elif shape == "large_dict":
        assert isinstance(msg.payload, dict), f"期望 dict，得到 {type(msg.payload)}"


# ---------------------------------------------------------------------------
# 纯协议层 benchmark（不走 ZMQ 网络层，测序列化+压缩的纯性能）
# ---------------------------------------------------------------------------


@dataclass
class CodecBenchResult:
    """纯编解码 benchmark 结果。"""
    serializer: str
    compression: str
    data_shape: str
    encode_ops_per_sec: float = 0.0
    decode_ops_per_sec: float = 0.0
    raw_bytes: int = 0
    compressed_bytes: int = 0
    compression_ratio: float = 0.0
    encode_us_per_op: float = 0.0   # 微秒/操作
    decode_us_per_op: float = 0.0
    error: str = ""


def bench_codec(ser: str, comp: str, shape: str, iterations: int = 200) -> CodecBenchResult:
    """纯协议层编解码 benchmark（不经过 ZMQ 网络）。

    注意：会对数据做 prepare_payload 预处理（匹配 publisher 实际行为），
    所以 DataFrame 会先转为 list[dict] 再交给 msgpack/json 序列化。
    """
    from pulsemq.protocol import serialization as ser_mod
    from pulsemq.protocol import compression as comp_mod

    result = CodecBenchResult(serializer=ser, compression=comp, data_shape=shape)

    try:
        ser_obj = ser_mod.get(ser)
        comp_obj = comp_mod.get(comp)
    except (KeyError, ImportError) as e:
        result.error = str(e)
        return result

    # 准备数据（匹配 publisher 的预处理流程）
    raw_data = make_value(shape, 1)
    data = prepare_payload(raw_data)

    # 预热 + 验证编解码正确性
    try:
        raw = ser_obj.serialize(data)
        compressed = comp_obj.compress(raw)
        decompressed = comp_obj.decompress(compressed)
        decoded = ser_obj.deserialize(decompressed)
    except Exception as e:
        result.error = f"序列化/压缩失败: {e}"
        return result

    result.raw_bytes = len(raw)

    # 编码 benchmark（序列化 + 压缩）
    encoded_list: list[bytes] = []
    t0 = time.perf_counter()
    for _ in range(iterations):
        raw = ser_obj.serialize(data)
        compressed = comp_obj.compress(raw)
        encoded_list.append(compressed)
    t1 = time.perf_counter()
    encode_elapsed = t1 - t0

    result.compressed_bytes = len(encoded_list[-1])
    result.compression_ratio = result.compressed_bytes / result.raw_bytes if result.raw_bytes else 0

    # 解码 benchmark（解压 + 反序列化）
    sample_compressed = encoded_list[-1]
    t0 = time.perf_counter()
    for _ in range(iterations):
        decompressed = comp_obj.decompress(sample_compressed)
        _ = ser_obj.deserialize(decompressed)
    t1 = time.perf_counter()
    decode_elapsed = t1 - t0

    result.encode_ops_per_sec = iterations / encode_elapsed
    result.decode_ops_per_sec = iterations / decode_elapsed
    result.encode_us_per_op = encode_elapsed / iterations * 1_000_000
    result.decode_us_per_op = decode_elapsed / iterations * 1_000_000

    return result


# ---------------------------------------------------------------------------
# 报告输出
# ---------------------------------------------------------------------------


def print_codec_table(results: list[CodecBenchResult]) -> None:
    """打印纯编解码 benchmark 结果表格。"""
    print("\n" + "=" * 120)
    print("  纯编解码 Benchmark（序列化 + 压缩，不经过 ZMQ 网络）")
    print("=" * 120)
    print(f"  {'组合':<35} {'编码 ops/s':>12} {'解码 ops/s':>12} "
          f"{'编码 us':>10} {'解码 us':>10} {'原始B':>10} {'压缩B':>10} {'压缩率':>8}")
    print("-" * 120)

    for r in results:
        if r.error:
            name = f"{r.serializer}+{r.compression}+{r.data_shape}"
            print(f"  {name:<35} [ERROR] {r.error}")
            continue
        name = f"{r.serializer}+{r.compression}+{r.data_shape}"
        print(f"  {name:<35} {r.encode_ops_per_sec:>12,.0f} {r.decode_ops_per_sec:>12,.0f} "
              f"{r.encode_us_per_op:>10.1f} {r.decode_us_per_op:>10.1f} "
              f"{r.raw_bytes:>10,} {r.compressed_bytes:>10,} {r.compression_ratio:>7.2f}x")


def print_e2e_table(results: list[BenchResult]) -> None:
    """打印端到端 pub→sub benchmark 结果表格。"""
    print("\n" + "=" * 130)
    print("  端到端 pub→sub Benchmark（经过 ZMQ 网络层）")
    print("=" * 130)
    print(f"  {'组合':<35} {'状态':<6} {'记录吞吐/s':>12} {'消息吞吐/s':>10} "
          f"{'延迟p50':>10} {'延迟p90':>10} {'延迟p99':>10} {'延迟max':>10} {'压缩率':>8}")
    print("-" * 130)

    for r in results:
        name = f"{r.serializer}+{r.compression}+{r.data_shape}"
        status = "OK" if r.correctness_ok else "ERR"
        if r.error:
            status = f"ERR {r.error[:30]}"

        p50 = r.latency_percentile(0.50)
        p90 = r.latency_percentile(0.90)
        p99 = r.latency_percentile(0.99)
        max_lat = r.latency_percentile(1.0)

        print(f"  {name:<35} {status:<6} {r.throughput_records_per_sec:>12,.0f} "
              f"{r.throughput_msgs_per_sec:>10,.0f} "
              f"{p50:>9.3f}ms {p90:>9.3f}ms {p99:>9.3f}ms {max_lat:>9.3f}ms "
              f"{r.compression_ratio:>7.2f}x")


def print_summary(results: list[BenchResult], codec_results: list[CodecBenchResult]) -> None:
    """打印汇总统计。"""
    # 找出最佳组合
    ok_results = [r for r in results if r.correctness_ok and not r.error]
    ok_codec = [r for r in codec_results if not r.error]

    print("\n" + "=" * 80)
    print("  汇总")
    print("=" * 80)

    if ok_results:
        fastest = max(ok_results, key=lambda r: r.throughput_records_per_sec)
        lowest_lat = min(ok_results, key=lambda r: r.latency_percentile(0.50))
        print(f"  端到端最高记录吞吐: {fastest.serializer}+{fastest.compression}+{fastest.data_shape} "
              f"= {fastest.throughput_records_per_sec:,.0f} records/s")
        print(f"  端到端最低延迟 p50:  {lowest_lat.serializer}+{lowest_lat.compression}+{lowest_lat.data_shape} "
              f"= {lowest_lat.latency_percentile(0.50):.3f}ms")

    if ok_codec:
        fastest_enc = max(ok_codec, key=lambda r: r.encode_ops_per_sec)
        fastest_dec = max(ok_codec, key=lambda r: r.decode_ops_per_sec)
        best_ratio = min(ok_codec, key=lambda r: r.compression_ratio)
        print(f"  最快编码: {fastest_enc.serializer}+{fastest_enc.compression}+{fastest_enc.data_shape} "
              f"= {fastest_enc.encode_ops_per_sec:,.0f} ops/s")
        print(f"  最快解码: {fastest_dec.serializer}+{fastest_dec.compression}+{fastest_dec.data_shape} "
              f"= {fastest_dec.decode_ops_per_sec:,.0f} ops/s")
        print(f"  最佳压缩比: {best_ratio.serializer}+{best_ratio.compression}+{best_ratio.data_shape} "
              f"= {best_ratio.compression_ratio:.2f}x")

    # 错误汇总
    errors = [(r, r.error) for r in results if r.error]
    if errors:
        print(f"\n  WARN 发现 {len(errors)} 个错误:")
        for r, err in errors:
            print(f"    - {r.serializer}+{r.compression}+{r.data_shape}: {err}")
    else:
        print(f"\n  OK 所有 {len(ok_results)} 个组合全部通过")


# ---------------------------------------------------------------------------
# 主入口
# ---------------------------------------------------------------------------


async def run_all() -> None:
    """运行全矩阵 benchmark。"""
    # 1. 构建合法组合列表
    combos: list[tuple[str, str, str]] = []
    for ser in SERIALIZERS:
        for comp in COMPRESSIONS:
            for shape in DATA_SHAPES:
                if is_compatible(ser, shape):
                    combos.append((ser, comp, shape))

    total = len(combos)
    print("=" * 80)
    print("PulseMQ 全矩阵 pub→sub Benchmark")
    print(f"  序列化格式: {', '.join(SERIALIZERS)}")
    print(f"  压缩格式:   {', '.join(COMPRESSIONS)}")
    print(f"  数据形态:   {', '.join(DATA_SHAPES)}")
    print(f"  合法组合:   {total} 个")
    print(f"  每组合消息数: {MESSAGES_PER_TOPIC}")
    print(f"  批量记录数:   {RECORDS_PER_MSG} (list_dict / dataframe)")
    print("=" * 80)

    # 2. 纯编解码 benchmark（快速，不走网络）
    print("\n[1/2] 纯编解码 benchmark ...")
    codec_results: list[CodecBenchResult] = []
    for i, (ser, comp, shape) in enumerate(combos):
        r = bench_codec(ser, comp, shape)
        codec_results.append(r)
        pct = (i + 1) / total * 100
        status = "OK" if not r.error else f"ERR {r.error[:40]}"
        print(f"  [{i+1}/{total}] {ser}+{comp}+{shape} ... {status} ({pct:.0f}%)")

    print_codec_table(codec_results)

    # 3. 端到端 pub→sub benchmark
    print(f"\n[2/2] 端到端 pub→sub benchmark ...")
    e2e_results: list[BenchResult] = []
    for i, (ser, comp, shape) in enumerate(combos):
        pct = (i + 1) / total * 100
        print(f"  [{i+1}/{total}] {ser}+{comp}+{shape} ... ", end="", flush=True)
        r = await bench_one(ser, comp, shape)
        e2e_results.append(r)
        status = "OK" if r.correctness_ok else f"ERR {r.error[:40]}"
        recv = r.messages_recv
        print(f"{status} (收到 {recv}/{MESSAGES_PER_TOPIC} 条, {pct:.0f}%)")

    print_e2e_table(e2e_results)
    print_summary(e2e_results, codec_results)

    # 4. 返回是否有错误（用于脚本退出码）
    has_errors = any(r.error for r in e2e_results)
    return has_errors


def main() -> None:
    import logging
    logging.basicConfig(level=logging.WARNING)

    has_errors = asyncio.run(run_all())
    if has_errors:
        print("\nWARN 部分测试失败，请检查上方错误信息")
        sys.exit(1)
    else:
        print("\nOK 全部 benchmark 完成")
        sys.exit(0)


if __name__ == "__main__":
    main()
