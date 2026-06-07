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

## Task 3 审读（serialization/registry.py）

- [P1][日期 2026-06-07] I22: 文档注释说"5 个 serializer" (`StringSerializer/MsgpackSerializer/PyArrowSerializer/BytesSerializer` + 一个 Protobuf), 实际 `_init_builtins()` 只注册 4 个 (str/msgpack/bytes/pyarrow), **未注册 protobuf** (registry.py:1-5, 237-252)。Task 1 审读 flags 时也提到 "未知 ser_fmt 静默回退 msgpack" 风险 (I7), 两者结合: 客户端若错误传 `ser_fmt="protobuf"`, 会被 `FrameFlags.encode()` 静默映射为 msgpack, 数据按 msgpack 编码发出, 接收端按 msgpack 解码 OK (运气), 但用户实际期望 protobuf 行为时毫无反馈。建议: v0.7 添加 ProtobufSerializer + 严格模式 (I7 修复)。
- [P3][日期 2026-06-07] I23: `_init_builtins()` 用 try/except 静默吞 ImportError 注册 pyarrow (registry.py:244-247) — 优点: pyarrow 缺时不强依赖; 缺点: 用户配 `ser_fmt="pyarrow"` 但环境未装, 会在首次 publish 时才抛 KeyError, 错误信息不直观 ("未注册")。建议: 在模块 import 时 `logger.warning("pyarrow 未安装, 跳过注册")` 至少给用户一个 hint。
- [P3][日期 2026-06-07] I24: `SerializationRegistry.register("none", BytesSerializer())` 与 `register("bytes", BytesSerializer())` 创建了**两个独立 BytesSerializer 实例** (registry.py:241-242), 行为等价但实例不同。Task 3 测试 `test_registry_ser_none_is_bytes_alias` 验证语义等价 (产出相同) 而非 `is` 同一对象 — 已通过。备注: 不阻塞。
- [P2][日期 2026-06-07] I25: `StringSerializer.serialize` 接受 str 和 bytes 两种输入 (registry.py:50-54), 文档未明确说明支持 bytes, 用户可能误以为只能传 str。`BytesSerializer.serialize` 只接受 bytes (registry.py:114-116) — 行为不对称: 同样 `b"hello"`, 用 str 序列化器是 noop, 用 bytes 序列化器是 noop, 行为一致; 但用 str 序列化器传 int 抛 TypeError, 文档类型注解 `obj: Any` 太宽松。建议: `StringSerializer.serialize` 改为只接受 str, 或文档显式说明支持 bytes。
- [P3][日期 2026-06-07] I26: `PyArrowSerializer.serialize` 接受非 Table/DataFrame/dict 时静默回退到 msgpack (registry.py:94-95) — 优点: 不让客户端崩; 缺点: 错误路径不透明, 用户期望 Arrow 但收到 msgpack 时无反馈。建议: 抛 `TypeError` 显式拒绝未知类型。
- [P2][日期 2026-06-07] I27: `PyArrowSerializer.deserialize` 返回 `pa.Table` (registry.py:107), 类型签名 `-> Any` 弱类型 — 客户端需 `dec.to_pandas()` 才能用, 强类型用户会撞坑。建议: docstring 加 `# Returns: pa.Table` 明确返回类型 (现已有部分说明但未指明 Table 类型)。
- [P2][日期 2026-06-07] I28: `MsgpackSerializer.serialize` 用 `use_bin_type=True` (registry.py:65), `deserialize` 用 `raw=False` (registry.py:69) — 不对称语义: serialize 把 str 当 bin (bytes), deserialize 把 bin 解为 str (Python 3 默认)。组合下 roundtrip OK, 但若用户单独调用 serialize 拿到的 bytes 再传给第三方解码器, 行为会变 (第三方按 utf-8 解 str)。建议: docstring 注明这一约定。
- [P2][日期 2026-06-07] I29: 模块顶部 docstring 列的序列化器有 4 个 (`StringSerializer/MsgpackSerializer/PyArrowSerializer/BytesSerializer`) 但类名实际是 `StringSerializer` (而非 `StrSerializer`) — 与 plan 中提到 `StrSerializer` 不一致 (registry.py:3-4)。已修正测试导入, 不阻塞。

## Task 4 审读（transport/router/auth/handlers/engine/pipeline/pool/overload）

### auth/permission.py

- [P0][日期 2026-06-07] I30: `topic_match` 中间位置 `*` 错误接受空段 — `topic_match("a.*.c", "a..c")` 返回 True, 但语义上 `*` 应匹配恰好一个非空段 (`permission.py:55-62`)。修复: 已在 `_match_parts` 中间 `*` 分支加 `if not topic[ti]: return False` 守卫。`fnmatch` 对 `"a.*.c"` 匹配 `"a..c"` 也返回 True, 因此是自实现 `_match_parts` 的语义差异 — 与 fnmatch 行为不一致, 但项目自实现应严格定义语义, 选择"非空段"更符合用户预期。
- [P0][日期 2026-06-07] I31: `PermissionService` 和 `PermissionCache` 缺单元测试 — Task 4 未直接覆盖 (权限测试需 mock perm_repo), 留待 Task 8 (auth 审计)。验证: `topic_match` 边界由 test_router.py::test_topic_match_* 系列覆盖, 通过。

### engine/router.py

- [P0][日期 2026-06-07] I32: `unsubscribe(identity, pattern)` 取消通配符订阅时, 只清理了 `_wildcard_subscriptions` 索引, 未清理通配符订阅时由 `subscribe_wildcard` 展开过的精确 topic `_topic_subscribers` 索引 — 导致 `get_subscribers(expanded_topic)` 仍返回该 identity (`router.py:89-94`)。示例: `subscribe_wildcard(c1, "team-a.>")` 后, `c1` 在 `_topic_subscribers["team-a.mkt.sh.600000"]` 中; `unsubscribe(c1, "team-a.>")` 后, 该精确 topic 索引中 `c1` 仍在, 消息会继续推送给断开连接的客户。修复: `unsubscribe` 先检查 `topic` 是否在 `_wildcard_subscriptions` 中, 若是则清理精确 topic 索引 + 失效缓存。

### engine/handlers.py

- [P0][日期 2026-06-07] I33: `dispatch()` 解码 `FrameCodec.decode_server(server_frames)` 在 try 块之外 (`handlers.py:70`), 非法帧数 (如 2 帧) 抛 `ValueError` 会逃逸出 dispatch, 引擎主循环的 `_process_single` 内层 `except Exception` 兜底, 但 contexts 从未 acquire, 无 finally 释放, 整个引擎可能因反复异常而 busy-loop。修复: 解码移到 try 内, 异常时 logger.warning + return。

### engine/engine.py

- [P0][日期 2026-06-07] I34: `_adapt_batch_size` 在 batch_size=1 时 grow/shrink 振荡 (`engine.py:316-331`)。原因: grow 条件 `h >= effective * 0.8` 在 `effective=1, h=1` 时恒成立 → 每个 adapt_window 都触发 grow 到 2; 下一个 window 触发 shrink 回 1; 永久 1↔2 振荡。修复: grow 条件前置 `effective >= 2` 守卫, 避免 floor 退化场景下的虚假 grow。

## Task 6 审读（client/async_client.py）

- [P1][日期 2026-06-07] I35: `unsubscribe()` 调 ZMQ `setsockopt(UNSUBSCRIBE, ...)` 未捕获 `ZMQError` — 取消未订阅过的精确 topic 可能在某些 libzmq 版本抛错逃逸到 `__aexit__` (`async_client.py:307-312`)。修复: try/except `zmq.ZMQError` + `logger.debug`，避免 caller 在清理路径上踩到边缘错误。
- [P2][日期 2026-06-07] I36: `query()` 透传用户 dict, 服务端 `_handle_query` 期望 `query.get("action", "")` 取字符串 ("system_status" 等)。原 docstring 完全未说明 action 字段 (`async_client.py:316`)。修复: docstring 显式列出 V1 支持的 action 与示例。客户端无任何字段映射/校验逻辑；用户传错键名 (如 "type") 会得到 server 端 3004 ERROR 帧, 但本方法只解析 QUERY 响应, 不会主动处理 ERROR → 已知限制，v0.6 不修，留待 v0.7 在 query() 入口校验 + 错误帧处理。
- [P3][日期 2026-06-07] I37: `connect()` 不探测服务端可达性。ZMQ DEALER/SUB `connect()` 立即返回（不等待 TCP 握手）, 客户端 connect 成功 ≠ 服务端在跑。Task 6 验证：测试用 `server_subprocess` fixture 启动真实 server, 所以 connect 后第一次操作成功; 真实部署如果 server 不可达, 客户端 connect 不会报错, 第一次 publish/subscribe 才超时。建议 v0.7 加可选的 `connect_timeout` 探测 (发一个 PING 等 PONG)。
- [P3][日期 2026-06-07] I38: `_reconnect()` 重建 SUB socket 时清空所有 ZMQ 层 filter。`subscribe()` 循环体内会重新 setsockopt SUBSCRIBE — 这是正确的（重连时不会丢订阅）。但 `publish()` 调用前如果 socket 断开, 重连后 SUB socket 上如果有未重发的过滤将丢失, 后续 `subscribe()` 第一次进入循环体时才会重建 — 在 connect→subscribe 期间重连的极端场景下, publish 不受影响 (publish 走 DEALER, 不依赖 SUB 过滤), 不会丢消息。**无 bug**, 仅文档: 当前实现是"lazy re-subscribe on first recv after reconnect"。
- [P3][日期 2026-06-07] I39: `_reconnect()` 用 `await asyncio.sleep(delay)`, 期间 `subscribe()` 循环体阻塞在 sleep, 不处理消息 — 重连过程中到达的消息会在 server 侧 buffer (buffer_enabled=False 时丢弃) 或丢失, client 重连后会因新 SUB socket 错过。重连中 broker 没有 PUB retention 机制, 这是 ZMQ pub/sub 模型固有限制, 不是 client bug。建议 docstring 加一句说明。
- [P3][日期 2026-06-07] I40: `subscribe()` 的 `wildcard_topics` 列表为空时, `if msg and (not wildcard_topics or any(...))` 短路求值返回 True, 所有消息放行。代码 `not wildcard_topics` 看起来可疑, 实际语义正确（精确订阅时 ZMQ 层已过滤）。**无 bug**, 仅记录以便后人理解。
- [P2][日期 2026-06-07] I41: `connect()` 创建 `self._ctx` 在最前面, 如果中途 `self._ctx.socket(...)` 抛异常 (e.g. OOM), `self._ctx` 已分配但未绑定到 `term()`。修复: 用 try/except 包 socket 创建, 失败时 term 已分配的 ctx。当前实现不会 leak (异常向上抛出后对象 GC, `__aenter__` 失败时 `__aexit__` 不会被调) — **不阻塞**, 但实现更稳健的版本可加 try/except 释放。

## Task 10 审读（monitoring/）

### monitoring/realtime.py

- [P2][日期 2026-06-07] I42: `SlidingWindow` docstring 声称采用 "reservoir sampling" (随机替换), 但实际是 `deque(maxlen=...)` 的 FIFO 截断 + 窗口过期清理 (`realtime.py:36-41`)。修复: docstring 改为准确描述"底层 deque(maxlen) + 窗口外 _cleanup"。当前实现不阻塞, 行为符合调用方预期 (FIFO 截断对 P50/P99 延迟计算无偏), 仅文档需修正。
- [P3][日期 2026-06-07] I43: `SlidingWindow.add()` 在 `len(self._data) >= self._max_samples` 时先 `_cleanup(ts)` 再 `append` — 但 `deque(maxlen=max_samples)` 在 append 时已自动丢最旧, `_cleanup` 若是无过期样本则为 wasted call (`realtime.py:48-53`)。修复: 简化为直接 append, 容量裁剪由 deque 自身保证。仅微优化, 不阻塞。

### monitoring/api.py

- [P1][日期 2026-06-07] I44: `MetricsHTTPServer._handle_request` 在 `request_line` 为空时直接 `writer.close()` 后 return (`api.py:49-51`)，但 finally 块又再次 `writer.close()` + `await writer.wait_closed()`。重复 close() 在部分 asyncio 实现会触发 `InvalidStateError` 或 `ConnectionResetError` 警告日志（虽被 `try/except` 吞掉）。修复: 移除提前 close, 让 finally 统一处理, 避免双重关闭。
- [P3][日期 2026-06-07] I45: `MetricsHTTPServer._respond_json` 硬编码 status_text 仅 200/404, 其他状态码默认 "OK" (`api.py:85`)。当前路由只返回 200/404, 不影响功能；但 v0.7 若加 401/503 等状态码会显示错误文案。建议: 扩展 status_text 字典或 fallback 到标准 reason phrase。

### monitoring/minute.py

- [P3][日期 2026-06-07] I46: `MinuteAggregator` 使用 `int(next_minute) // 60` 作为 `ts` (分钟精度) (`minute.py:83`)。这意味着每个跨分钟边界会写入一行; 但当 `time.time()` 返回的 now 已非常接近 next_minute (e.g. 59.999s), `await asyncio.sleep(next_minute - now)` 睡眠 1ms 后立刻切槽, 极端情况下 ts 出现"整分钟边界重复" (e.g. 边界 60s 与 60s 同号)。不阻塞: 实际 `time.time()` 抖动已足够, 但理论边界需注意。

