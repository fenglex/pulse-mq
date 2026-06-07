# 已知问题登记

> 每次代码审读发现的问题, 按模块归档。
> 阻塞 e2e 的会立即修; 非阻塞的留待后续 phase 或 v0.7 修。

## Task 0 审读（tests/conftest.py）

- [日期 2026-06-07] I1: `port_pair` 在 p==65535 时 `bind(p+1)` 越界 — 理论边界, 不阻塞, follow-up
- [日期 2026-06-07] I2: smoke test 仅检查 socket 对象存在, 未验证协议握手 — 建议下个 phase 补 `await c.ping()`
- [日期 2026-06-07] I3: conftest 与 test_server_runner 隐式耦合 (xpub = router+1) — 需在 conftest 顶部加一行注释
- [日期 2026-06-07] I4: server_subprocess 用阻塞 readline, 跨过 polling 周期风险 — 加固, 优先级低
- [日期 2026-06-07] I5: kill() 在 Windows 上可能抛 OSError — 需 try/except 保护

## Task 1 审读（protocol/）

### protocol/flags.py

- [P2][日期 2026-06-07] I6: flags 字节 6 bits 用满 (ser[0:2]=3b + comp[3:4]=2b + has_topic[5]=1b), 0b101~0b111 留空但 docstring 未声明是预留还是废弃 (flags.py:3-7); 建议 docstring 加一句"0b101~0b111 保留给 v0.7+ 扩展"。
- [P0][日期 2026-06-07] I7: `encode()` 对未知 ser_fmt/comp 静默回退到 0b000/0b00 (msgpack/none) — 风险: 调用方拼错格式名 (如 "msgpacK") 时不会报错, 而是把数据当 msgpack+none 发出, 接收端按 msgpack 解码抛异常, 错误信息指向接收端而非真实根因。建议: 严格模式 `KeyError` 抛出。
- [P0][日期 2026-06-07] I8: `decode()` 对未知 ser_bits (如 0b101/0b110/0b111) 静默回退到 "msgpack" — 对未知 comp_bits 回退到 "none" — 风险同 I7, 但更危险: 兼容旧版能容忍的"未分配码"被偷偷映射成 msgpack+none, 行为不可预测。建议: 未知值抛 `ValueError("未知 flags: ser_bits=0b101")`。

### protocol/frames.py

- [P2][日期 2026-06-07] I10: 客户端发 4 帧 / ZMQ 路由信封在服务端变 5 或 6 帧, 模块 docstring 顶部"固定 6 帧格式"措辞与 `decode_server()` 支持 5/6 帧的实情不一致 — 第一句会误导读者 (frames.py:1)。建议: 改 docstring 为"客户端 4 帧, 服务端 ROUTER 收到 5 或 6 帧"。
- [P1][日期 2026-06-07] I11: `decode_server()` 用 `if len(frames) == 6` / `elif len(frames) == 5` 区分, 但 5 帧/6 帧区分依据是"delimiter 是否存在" — 与"DEALER→ROUTER 无 delimiter / ROUTER 路由信封有 delimiter"的注释硬编码绑定, 没有 runtime 检测 (e.g. 检查 frames[1] == b"" 来判 delimiter) (frames.py:73,81)。风险: 如果未来 ZMQ 行为变化 (如 4 帧场景), 代码会按错位解读 topic/meta/payload。建议: 显式 `delimiter = frames[1]; if delimiter == b""` 检测。
- [已确认无问题][日期 2026-06-07] I12: `_RECORD_COUNT_STRUCT = struct.Struct(">I")` 用大端 4 字节 uint32, 与 `encode()` 的 pack 一致 — 正确, 无问题。验证: pack/unpack 配对且字节序一致。
- [已确认无问题][日期 2026-06-07] I13: `encode_payload()` / `decode_payload()` 顺序为 `compress(serialize(obj))` / `deserialize(decompress(data))` — 正确: 先序列化得 bytes, 再压缩 bytes, 接收端先解压得 bytes, 再反序列化。对称无问题, 与 zmq/msgpack 标准实践一致。
- [P1][日期 2026-06-07] I14: `decode_server()` 解析 meta 时假定 `len(meta) >= 2`, 但 `meta = frames[3]` (6帧) / `frames[2]` (5帧) 无长度校验 (frames.py:78,85) — 若 meta 长度 < 2 会抛 `IndexError` 而非 `ValueError`, 错误处理不统一。建议: 加 `if len(meta) < 2: raise ValueError("meta 帧过短")`。
- [P1][日期 2026-06-07] I15: `encode()` 对 msg_type (0-255) 与 record_count (uint32) 缺范围校验, 溢出时 struct.error / 静默截断 (frames.py:56-57); 建议入口统一校验。

### protocol/msg_type.py

- [P2][日期 2026-06-07] I16: `MsgType` 共 11 个枚举值 (AUTH/PUB/SUB/UNSUB/QUERY/PING/PONG/STATUS/ERROR/BROADCAST/HISTORY_REPLAY) (msg_type.py:6-16), `handlers.py` `_dispatch_internal` switch 只处理 6 个: PUB, SUB, UNSUB, PING, QUERY, HISTORY_REPLAY (handlers.py:167-179) — 注释"其他类型暂忽略"。缺失: AUTH (0x01), PONG (0x07), STATUS (0x08), ERROR (0x09), BROADCAST (0x0A)。验证:
    - AUTH 缺失合理 (应被 AuthInterceptor 拦截, 不进 handler switch)
    - PONG 缺失合理 (server 不会收到 PONG, 仅发出)
    - STATUS 缺失 — 无 inbound STATUS 协议, 应移除枚举或标 _DEPRECATED
    - ERROR 缺失合理 (server 仅发出 ERROR)
    - BROADCAST 缺失合理 (server 仅发出 BROADCAST)
  建议: 给未使用枚举加注释标明 "server-only" 或 "client-only" / "reserved"。
- [P2][日期 2026-06-07] I17: `MsgType` 不是 `Enum` 子类, 而是普通 class + 类属性 (msg_type.py:6) — 优点: 常量比较 `msg_type == MsgType.PUB` 仍可用且无 Enum.value 包装; 缺点: 没有类型安全, `MsgType.NONEXISTENT = 99` 可被动态添加。建议: 改 `class MsgType(enum.IntEnum)` 或加 `__slots__` 防篡改。
- [P1][日期 2026-06-07] I18: `from_byte()` 返回 `int | None` (msg_type.py:31), 但 `decode_server()` 中直接用 `msg_type = meta[0]` (frames.py:93) 不走 `from_byte()` 验证 — 也就是说 `from_byte()` 形同虚设, 非法 msg_type 字节值会进入 dispatch switch, 落到 `else: 暂忽略` 分支静默丢弃, 不报错也不日志。建议: `decode_server()` 调 `MsgType.from_byte()` 验证, 非法值抛 `ValueError` 或 `logger.warning`。
- [P1][日期 2026-06-07] I19: control 集合在 msg_type.py:22-24 与 overload.py:53-55 各硬编码一份, 存在单点变更风险; 建议 overload.py 改用 `MsgType.is_control(msg_type)` 单一来源。
- [P2][日期 2026-06-07] I20: 枚举值定义后无单元测试覆盖 `from_byte()` 的合法/非法分支 — 测试已补 (test_protocol_msg_type.py::test_from_byte_known_values / test_from_byte_unknown_returns_none), source 修复 (decode_server 改用 from_byte 验证) 待后续 phase。

## Task 2 审读（tests/unit/test_protocol_*.py）

- [P3][日期 2026-06-07] I21: `FrameFlags.decode()` 对未知 comp_bits 的"静默回退到 none"防御性代码实际上不可达 — comp_bits 占 2 bits, 范围 0-3 全部对应合法压缩算法 (none/snappy/lz4/zstd), 任何单字节输入都不可能产生未知 comp_bits (flags.py:55-58)。_COMP_MAP_REV.get(bits, "none") 的 default 参数实际上是死代码。验证: `(0xFF >> 3) & 0b11` = 0b11 = "zstd", 已知; `(0x00 >> 3) & 0b11` = 0b00 = "none", 已知; 中间值同理。结论: 静默回退路径只对 ser_bits (3 bits) 有效, 对 comp_bits 是死代码。Task 2 测试已文档化此事实 (test_protocol_flags.py::test_decode_unknown_comp_defaults_to_none)。

