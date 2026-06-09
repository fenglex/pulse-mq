# PulseMQ 端到端测试覆盖设计

> 日期: 2026-06-09
> 状态: 已批准
> 目标: 全面验证 publisher/subscriber 在各消息类型、各序列化、各压缩组合下的端到端正确性；发现并修复 bug

## 1. 范围

**覆盖对象**：单进程 pub → sub 架构下，发布端编码 + ZeroMQ PUB 广播 + 订阅端解码全链路。

**测试矩阵（笛卡尔积）**：

| 维度 | 值 |
|------|----|
| 序列化 | `msgpack`, `json`, `str`, `bytes`, `pyarrow` |
| 压缩 | `none`, `snappy`, `lz4`, `zstd` |
| 数据形态 | scalar_str, scalar_bytes, list_dict, dataframe, large_dict(1.1MB) |

矩阵参数化三元组，非法组合（如 `bytes` 序列化 × 非 bytes 数据）显式 `pytest.skip`。

**专项场景**：
- Admin HTTP/SSE 端点（`/healthz`、`/api/v1/topics`、`/api/v1/topics/{topic}/history`、`/api/v1/system/status`、`/`）
- Burst 模式端到端
- 错误路径：`record_count > 1,000,000`、ZAP 拒绝、未注册压缩算法
- 多 subscriber 广播一致性

## 2. 文件组织

```
tests/
├── _pubsub_fixtures.py       # 【新增】共享 fixtures + 启动/清理辅助 + 断言 helper
├── test_e2e_publisher.py     # 【新增】Publisher 端矩阵 + Admin + Errors
├── test_e2e_subscriber.py    # 【新增】Subscriber 端矩阵 + Broadcast + Burst + Errors
├── test_integration.py       # 保留
├── test_protocol.py          # 保留
├── test_stats.py             # 保留
```

`test_integration.py` / `test_protocol.py` / `test_stats.py` **不修改、不删除**。

## 3. 共享 fixtures

`_pubsub_fixtures.py` 提供：

- `random_port_pair()` — 独立 (pub_port, admin_port)，范围 25000-35000
- `tmp_sqlite_url()` — `tempfile.mkstemp` 临时 SQLite
- `running_publisher(pub, warmup=0.5)` — async context manager，后台跑 `pub._run()`，yield 后优雅关闭
- 常量：`SERIALIZERS`、`COMPRESSIONS`、`DATA_SHAPES`
- `assert_message_roundtrip(msg, expected, ser, comp, record_count)` — 公共断言
- `make_value(shape, idx)` — 根据 data_shape 生成期望值（DataFrame 用 `to_dict(orient="records")` 比较）

## 4. 生命周期管理

- 复用 `test_integration.py` 的 `pub._running = False` + `task.cancel()` 模式
- `pytest-asyncio` `asyncio_mode = "auto"`，函数级 fixture，新 publisher/subscriber per case
- 端口隔离：不使用 `--reuse-port`

## 5. 断言策略

**Publisher 端**：
- `pub._traffic.all_topics_snapshot()[topic]["record_count_current"] >= 5`
- `pub._buffers.snapshot()[topic] >= 1`
- Admin HTTP 端点返回结构与状态码

**Subscriber 端**：
- 逐条 `assert_message_roundtrip`
- 多 subscriber 各自累计数量与字节级一致

**错误路径**：直接调函数断言抛指定异常类型（`ValueError` / `KeyError`）

**不断言**：具体字节数、p50/p99 延迟（属于 `bench_burst.py` 范畴）

## 6. 修复流程

1. **第一轮**：纯测试，不改源码，跑完收集失败列表
2. **分类**：A 编排层 / B 协议 / C 注册表 / D 文档不一致
3. **第二轮**：按 A→B→C→D 批量修
4. **第三轮**：回归 — 跑全量 `pytest tests/` + `python -m build` + `scripts/bench_burst.py`
5. **提交**：每个修复一个 commit，中文 message

## 7. 不做的事

- 不改 `pyproject.toml` 的 `version`（非破坏性修复）
- 不动 untracked 的 `benchmarks/bench_transport.py` 与 `src/pulsemq/transport/tcp_transport.py`
- 不在测试中引入 `httpx` / `requests` 依赖（用 `urllib.request`）

## 8. 运行方式

```bash
cd D:/workflow/pulse-mq
python -m pytest tests/test_e2e_publisher.py tests/test_e2e_subscriber.py -v --tb=short
```

期望耗时 60-180 秒，规模约 80-100 case。
