# 02 协议层设计

> 源码：`src/pulsemq/protocol/`（frames.py / serialization.py / compression.py / flags.py / msg_type.py）

## 1. 职责与边界

协议层负责「Python 对象 ↔ 单 bytes 帧」的双向变换，是纯函数库：不依赖 zmq、不依赖服务端/客户端任何状态。服务端热路径只用 `decode_header`，完整 `decode` 只发生在消费端。

```
encode(topic, data, serializer, compression)
   │ ① 推断/校验 data_type      msg_type.py
   │ ② 序列化                    serialization.py（注册表）
   │ ③ 压缩                      compression.py（注册表）
   │ ④ flags 位编码              flags.py
   └ ⑤ struct 打包 + 可选 CRC    frames.py

decode(frame)        → PulseMessage（完整还原 payload 类型）
decode_header(frame) → FrameHeader（仅头部，供路由/统计）
```

## 2. 帧格式（v2，单 bytes 帧）

```
偏移   字段              长度      编码
0      magic            2         ASCII "PM"
2      version          1         0x01
3      msg_type         1         MsgType: DATA=0x01 / CONTROL=0x02
4      flags            1         位域（见 §3）
5      data_type        1         DataType（见 §4）
6      topic_len        2         大端 uint16（topic ≤ 65535 字节）
8      topic            N         UTF-8
8+N    timestamp_ns     8         大端 int64（encode 时 time.time_ns()）
16+N   record_count     4         大端 uint32（≤ 1,000,000）
20+N   payload          变长       序列化 + 压缩后的字节
?      crc32            0/4       flags.bit7 置位时追加，大端 uint32，覆盖前面全部字节
```

struct 定义：`_HEAD_BEFORE_TOPIC = Struct(">2sBBBBH")`（8B），`_HEAD_AFTER_TOPIC = Struct(">qI")`（12B），定长头部共 **20B**。

校验规则（decode 时抛 `FrameError`）：
- 帧长 < 20B → "帧过短"
- magic ≠ `b"PM"` → "魔数不匹配"
- version ≠ 0x01 → "版本不支持"
- CRC 帧长度不足 4B / `zlib.crc32` 不符 → "CRC 缺失/校验失败"

## 3. flags 位域（`flags.py`）

```
bit 0-2  序列化格式：000=msgpack 001=bytes 010=pyarrow 100=str 101=json
bit 3-4  压缩算法：  00=none 01=snappy 10=lz4 11=zstd
bit 5-6  保留
bit 7    CRC 追加标志
```

- 未知编码解码时回退：序列化→`msgpack`，压缩→`none`（向后容错）。
- `encode_flags(ser, comp, crc)` / `decode_flags(byte)` / `has_crc(byte)`。

## 4. DataType 与类型保真（`msg_type.py` + `frames._restore_type`）

| 值 | 常量 | Python 类型 | 允许的序列化器 | 默认序列化器 |
|----|------|-------------|----------------|----------------|
| 0x00 | UNKNOWN | 兜底（控制帧显式使用） | 无规则约束 → msgpack | msgpack |
| 0x01 | DICT | dict | msgpack / json | msgpack |
| 0x02 | DATAFRAME | pandas.DataFrame | msgpack / json / pyarrow | pyarrow |
| 0x03 | STR | str | str | str |
| 0x04 | BYTES | bytes | bytes | bytes |

**类型保真链路**：
- encode 侧按 Python 类型推断 `data_type` 写入帧；推断结果为 UNKNOWN（非白名单类型：list/int/None/set…）且未显式指定时，直接 `TypeError` 拒绝发送——数据白名单 = `DataFrame / dict / str / bytes`（与 `producers.types.PubData` 一一对应）。
- DataFrame × msgpack/json：encode 前先 `to_dict(orient="records")` 转 list[dict]。
- decode 侧 `_restore_type` 按 data_type 还原：
  - DATAFRAME + pyarrow：`pa.Table.to_pandas()`；
  - DATAFRAME + msgpack/json：`pd.DataFrame(list[dict])`；
  - STR：bytes→str；BYTES：str→bytes。

序列化器与 data_type 不匹配时（如 DICT + pyarrow）encode 即 `TypeError`，防止「flags 标记与实际编码不一致导致订阅端解码失败」（`PyArrowSerializer` 对不支持类型同样显式报错而非静默回退，同理）。

## 5. encode 流程细节

```
data_type=None?  → _infer_data_type(data)；UNKNOWN → TypeError
serializer=None? → 按 _SERIALIZER_RULES 默认值
DataFrame+msgpack/json → to_dict("records")
record_count=None? → _infer_record_count：
    list → len；DataFrame → len；标量/dict → 1（下限 1，上限 1_000_000）
compression="auto"? → 序列化后 raw <256B 用 none（压缩不划算），≥256B 用 lz4
payload = compress(serialize(data))
crc=True → body += BE uint32(zlib.crc32(body))
```

## 6. decode 与 decode_header 的分层

| | `decode` | `decode_header` |
|---|---|---|
| 返回 | `PulseMessage`（含还原后的 payload、serializer/compression 名） | `FrameHeader`（topic/record_count/timestamp_ns/msg_type/raw_payload） |
| 代价 | 反序列化 + 解压 + 类型还原（约占完整路径 80%） | 仅 struct.unpack + topic 切片 |
| 调用方 | 消费端（`Client`） | 服务端数据面、客户端 recv 线程、服务端内置 producer 统计 |

`decode_header` 的 topic 经 `_intern_topic` 驻留缓存（`bytes→str`，容量 10000，超限退化为每次 decode），消除同 topic 高频重复解码分配。

## 7. 控制帧

控制命令复用同一帧格式：`msg_type=CONTROL`，**cmd 字符串放在 topic 字段**，payload 为 dict（默认 msgpack、无压缩、record_count=1、data_type=UNKNOWN）。

```python
encode_control(cmd, payload)        # cmd ∈ ControlCmd 常量
decode_control(frame) → ControlMessage(cmd, payload)   # 非 CONTROL 帧 → FrameError
```

命令集（`control.ControlCmd`）：`REGISTER / HEARTBEAT / SUBSCRIBE / UNSUBSCRIBE / DISCONNECT / LATENCY_REPORT`，payload 与回复结构见 [04-server.md](04-server.md) §4。

`decode_control` 在函数体内延迟导入 `ControlMessage`，打破 frames↔control 循环依赖。

## 8. 序列化注册表（`serialization.py`）

`Serializer` 抽象（serialize/deserialize）+ 全局 `_REGISTRY`，`register/get/available`。内置实现模块加载时自动注册：

| 名 | 实现 | 后端 | 特殊约束 |
|----|------|------|----------|
| str | StringSerializer | UTF-8 | 接受 str/bytes |
| msgpack | MsgpackSerializer | msgspec.msgpack | 通用默认 |
| json | JsonSerializer | msgspec.json | **拒绝 bytes**（base64 往返后类型变形为 str，显式 TypeError 引导改用 bytes/msgpack） |
| bytes | BytesSerializer | 透传 | 仅接受 bytes |
| pyarrow | PyArrowSerializer | Arrow IPC stream | 输入 pa.Table / DataFrame / dict / list[dict]；其余 TypeError；反序列化返回 `pa.Table` |

msgspec / pyarrow / pandas 均为模块级 try-import 缓存（`_msgspec/_pa/_pd`），热路径避免重复 import 查找。

## 9. 压缩注册表（`compression.py`）

`Compressor` 抽象 + 同形注册表：

| 名 | 实现 | 说明 |
|----|------|------|
| none | NoneCompressor | 透传 |
| snappy | SnappyCompressor | 极速，import 放构造函数（惰性） |
| lz4 | Lz4Compressor | lz4.frame，`auto` 策略的压缩档 |
| zstd | ZstdCompressor | 高压缩比；cctx/dctx 存 `threading.local`（非线程安全 → 每线程独立 context） |

调用方约定：encode 侧选定的实际压缩算法写回 flags（`auto` 解析后的结果），decode 侧按 flags 还原，两端无需外部协商。
