"""PulseMQ v2 单 bytes 帧格式测试。"""

import struct

import pytest

from pulsemq.errors import FrameError
from pulsemq.protocol import frames
from pulsemq.protocol.frames import (
    MAGIC,
    VERSION,
    PulseMessage,
    decode,
    decode_control,
    encode,
    encode_control,
)
from pulsemq.protocol.msg_type import MsgType


def test_magic_and_version():
    assert MAGIC == b"PM"
    assert VERSION == 0x01


def test_encode_decode_roundtrip_dict():
    data = {"price": 12.3, "sym": "600000"}
    raw = encode("market.stock", data, serializer="msgpack")
    assert raw[:2] == MAGIC
    assert raw[2] == VERSION
    assert raw[3] == MsgType.DATA
    msg = decode(raw)
    assert isinstance(msg, PulseMessage)
    assert msg.topic == "market.stock"
    assert msg.payload == data
    assert msg.msg_type == MsgType.DATA
    assert msg.record_count == 1


def test_encode_infers_record_count_from_dataframe():
    """DataFrame 行数应自动推断为 record_count，而非恒为 1。

    回归 Bug：record_count 默认 1，导致发送 50 行 DataFrame 与 1 条 dict 的
    record_count 相同，Web UI 消息量显示与实际不符。
    （原用 list 直发测试；list 现已禁止直发，改用白名单类型 DataFrame。
    DataFrame 走 msgpack 时内部经 ``to_dict("records")`` 转为 list[dict]，
    仍走同一条 ``_infer_record_count`` 推断路径，测试意图等价。）
    """
    import pandas as pd
    df = pd.DataFrame([{"i": i} for i in range(50)])
    raw = encode("batch.topic", df, serializer="msgpack")
    msg = decode(raw)
    assert msg.record_count == 50, f"DataFrame 行数=50，record_count 应为 50，实际={msg.record_count}"


def test_encode_does_not_infer_for_scalar():
    """单条 dict 应保持 record_count=1（list 以外不做推断）。"""
    raw = encode("t", {"x": 1}, serializer="msgpack")
    assert decode(raw).record_count == 1


def test_encode_explicit_record_count_overrides_inference():
    """显式传 record_count 应覆盖自动推断。"""
    import pandas as pd
    df = pd.DataFrame([{"i": i} for i in range(10)])
    raw = encode("t", df, serializer="msgpack", record_count=3)
    assert decode(raw).record_count == 3


def test_decode_bad_magic():
    bad = b"XX" + b"\x00" * 20
    with pytest.raises(FrameError):
        decode(bad)


def test_decode_bad_version():
    bad = MAGIC + b"\x09" + b"\x00" * 20
    with pytest.raises(FrameError):
        decode(bad)


def test_crc_roundtrip():
    data = {"x": 1}
    raw = encode("t", data, crc=True)
    msg = decode(raw)
    assert msg.payload == data


def test_crc_corruption_detected():
    raw = bytearray(encode("t", {"x": 1}, crc=True))
    raw[-1] ^= 0xFF  # 破坏 CRC
    with pytest.raises(FrameError):
        decode(bytes(raw))


def test_control_roundtrip():
    raw = encode_control("SUBSCRIBE", {"client_id": "c1", "topic": "a.*"})
    msg = decode_control(raw)
    assert msg.cmd == "SUBSCRIBE"
    assert msg.payload["topic"] == "a.*"


def test_timestamp_ns_present():
    raw = encode("t", {"x": 1}, ts_ns=1700000000_000000000)
    msg = decode(raw)
    assert msg.timestamp_ns == 1700000000_000000000


def test_record_count_field():
    raw = encode("t", {"x": 1}, record_count=42)
    msg = decode(raw)
    assert msg.record_count == 42


@pytest.mark.parametrize("bad", [
    [{"i": 1}],   # list（即便 list[dict] 也禁止直发）
    42,           # int
    3.14,         # float
    None,         # None
    {1, 2},       # set
])
def test_encode_rejects_non_whitelist_type(bad):
    """非白名单类型应在 encode 时被拒绝。

    白名单仅允许 DataFrame/dict/str/bytes（PubData 语义）。list/int/None 等
    此前会被打成 data_type=UNKNOWN 静默发送，现强制 raise，避免类型不明数据
    流入管道后订阅端无法还原。显式传 data_type 的路径（如 encode_control）
    不经过自动推断分支，不受影响。
    """
    with pytest.raises(TypeError):
        encode("t", bad)


def test_encode_accepts_whitelist_types():
    """4 种白名单类型应正常编码，不触发 UNKNOWN 校验。"""
    import pandas as pd
    for data in [pd.DataFrame({"a": [1]}), {"k": 1}, "hello", b"bytes"]:
        encode("t", data)  # 不 raise 即通过
