# PulseMQ 全面代码审计设计

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task.

## 目标

对 PulseMQ v0.6.0 全部 9 个模块（按路径分共 10 个文件组，约 4300 行）做一次系统化审计，定位并修复逻辑 / 功能 / 性能 / 安全方面的隐患，建立可重复的回归测试网与性能基线。

## 范围与原则

- **广度**：全部 9 个模块，零死角
- **深度**：代码审读 + 单测 + 模块集成 + e2e + 压测 + 安全 fuzz
- **态度**：遇问题修源码（用户偏好），不写 workaround
- **输出**：commit 级别的源码修复 + pytest 单测/集成测试 + e2e/压测脚本 + 2 份基线文档
- **工时单位**：以下"X 天"均为**工程师工日**（实际 8h 集中工作）

## 子系统清单

| 子系统 | 路径 | 关注度 |
|--------|------|--------|
| protocol | `src/pulsemq/protocol/` | 高（基础，所有消息都过） |
| transport | `src/pulsemq/transport/` | 高（网络层） |
| serialization | `src/pulsemq/serialization/` | 高（编解码） |
| engine | `src/pulsemq/engine/` | 高（核心调度） |
| client | `src/pulsemq/client/` | 中（SDK 表面） |
| server | `src/pulsemq/server.py` | 中（编排） |
| auth | `src/pulsemq/auth/` | 高（安全） |
| storage | `src/pulsemq/storage/` | 中（持久化） |
| monitoring | `src/pulsemq/monitoring/` | 中（指标） |
| config | `src/pulsemq/config.py` 等 | 低（胶水） |

## 阶段划分（顺序推进）

| 阶段 | 范围 | 预计工日 |
|------|------|----------|
| **A 核心路径** | protocol/transport/serialization/engine | 3-4 |
| **B 客户端+胶水** | client/server/config/event_loop/models | 2-3 |
| **C 辅助子系统** | auth/storage/monitoring | 2-3 |
| **D 集成+性能+安全** | 整体 | 2-3 |

**为什么顺序推进**：A 阶段的 bug 会污染 B/C/D 的测试结果。基础设施稳定后上层才有可信的回归网。

## 阶段 A — 核心路径

### 审计清单

| 模块 | 关键文件 | 重点审查项 |
|------|----------|-----------|
| protocol | `flags.py` `frames.py` `msg_type.py` | 帧编解码正确性（超长/空 topic、特殊字符）；bits 字段不冲突；`_RECORD_COUNT_STRUCT` 大端序 |
| serialization | `registry.py` | 5 个 serializer × 4 个 compressor 容错；空对象/超大对象 |
| transport | `zmq_transport.py` | XPUB 广播 + ROUTER 路由；XPUB_VERBOSE/NODROP/IMMEDIATE 配置；linger；socket 关闭顺序 |
| engine/router | `router.py` | `topic_match` 性能/正确性；订阅/取消并发；缓存一致性 |
| engine/handlers | `handlers.py` | 拦截器链失败传播；`_build_broadcast_meta` 边界（wire_meta 长度 1/2）；buffer append 锁 |
| engine | `engine.py` | 批处理 + 自适应批大小；`_broadcast_queue` 优雅关闭（哨兵 None）；后台 task 清理 |
| engine/pipeline | `pipeline.py` | 拦截器异常隔离；中间结果传递 |
| engine/pool | `pool.py` | 连接池借还 |
| engine/overload | `overload.py` | 限流/反压触发条件 |

### 产出

- `tests/test_protocol_*.py`：FrameCodec / FrameFlags / msg_type 单测
- `tests/test_serialization_*.py`：16 组合 × 边界单测
- `tests/test_transport_*.py`：transport 模块单测
- `tests/test_engine_*.py`：engine / router / handlers / pipeline / pool / overload 单测
- 修源码中发现的 bug
- 模块集成测试：engine + transport 真实 server 子进程（pytest 启进程）

## 阶段 B — 客户端+胶水

### 审计清单

| 模块 | 关键文件 | 重点审查项 |
|------|----------|-----------|
| client | `async_client.py` | DEALER/SUB 双 socket 生命周期；wildcard 订阅；`unsubscribe` 对应；`auto_reconnect` 竞态；context manager 异常路径 |
| server | `server.py` | 启动/关闭顺序；未捕获异常；指标挂载点 |
| config | `config.py` | 默认值；范围校验；环境变量 |
| event_loop | `event_loop.py` | uvloop 安装时机；idempotent |
| models | `models.py` | 数据类字段不冗余；序列化一致 |

### 已知遗留（e2e 已暴露的同类风险）

- `reply[N]` 帧索引：已修 `ping`/`query`，扫描其他方法是否有同类问题
- 通配符 `unsubscribe` 行为：当前按字面 `b""` 取消，需验证 server 侧 UNSUB handler 行为一致
- 异常传播：`subscribe` 内部对 `TimeoutError`/`ZMQError` 有处理，其他异常会冒泡
- `_reconnect` 是否丢失 SUB 过滤器

### 产出

- `tests/test_client_*.py`：PulseClient 单测
- `tests/test_server_*.py`：server / config / event_loop / models 单测
- `tests/integration/test_client_server.py`：client + 真实 server 集成
- 修源码

## 阶段 C — 辅助子系统

### 审计清单

| 模块 | 关键文件 | 重点审查项 |
|------|----------|-----------|
| auth | `memory_store.py` `permission.py` `zap_handler.py` | ZAP RFC 27 协议合规（4-frame 请求/2-frame 响应）；CURVE/PLAIN；权限通配；凭据存储 |
| storage | `database.py` `interfaces.py` `sqlite_perm.py` `sqlite_user.py` | SQLite 连接管理（线程/异步）；事务；schema 迁移；参数化 SQL |
| monitoring | `api.py` `minute.py` `realtime.py` | 指标聚合；HTTP 鉴权；环形缓冲 |

### 产出

- `tests/test_auth_*.py`：auth 单测 + ZAP 协议单测
- `tests/test_storage_*.py`：storage 单测
- `tests/test_monitoring_*.py`：monitoring 单测
- `tests/security/test_zap_fuzz.py`：ZAP fuzz
- `tests/security/test_sql_injection.py`：SQL 注入
- 修源码

## 阶段 D — 集成 + 性能 + 安全

### 任务清单

| 任务 | 工具/方法 | 验收 |
|------|-----------|------|
| 基线测试 | 扩展 `bench_live.py` | 单 pub × 单 sub × 16 组合 → 写 `docs/bench-baseline.md` |
| 并发压测 | N pub × M sub × 16 组合 | p99 < 10ms、吞吐 > 50k msg/s（待基线调整） |
| Soak | 1 客户端持续 4h 收发 | 内存增长 < 5%，无 fd/cursor 泄漏 |
| ZAP fuzz | 构造坏 ZAP 包 | 全部正确拒绝，server 不崩 |
| SQL 注入 | user/permission 注入 | 全部参数化，无注入 |
| 资源耗尽 | 超大 topic/payload、空 payload、特殊字符 | 优雅处理或失败 |

### 产出

- `scripts/bench_concurrent.py`：并发压测脚本
- `scripts/bench_soak.py`：4h 长时间 soak
- `tests/security/test_zap_fuzz.py`：ZAP fuzz 测试
- `tests/security/test_sql_injection.py`：SQL 注入测试
- `docs/bench-baseline.md`：基线数字
- `docs/bench-thresholds.md`：告警阈值

## 测试基础设施

### 目录结构

```
tests/
├── unit/                    # pytest 单测
│   ├── test_protocol_*.py
│   ├── test_serialization_*.py
│   ├── test_transport_*.py
│   ├── test_engine_*.py
│   ├── test_client_*.py
│   ├── test_server_*.py
│   ├── test_auth_*.py
│   ├── test_storage_*.py
│   └── test_monitoring_*.py
├── integration/             # 多模块集成
│   ├── test_engine_transport.py
│   └── test_client_server.py
├── security/                # 安全 fuzz
│   ├── test_zap_fuzz.py
│   └── test_sql_injection.py
└── conftest.py              # 共享 fixtures (server 子进程、客户端等)
```

### 依赖

- `pytest` + `pytest-asyncio`（已在 pyproject）
- `hypothesis`（property-based，阶段 C/D 视需要加）

## 风险与权衡

| 风险 | 缓解 |
|------|------|
| 单测覆盖旧代码时暴露大量 bug 导致工期失控 | 优先修阻塞 e2e 的；非阻塞 bug 记录到 `docs/known-issues.md` |
| 压测 / soak 在 CI 跑不动 | 压测只本地跑；CI 只跑单测 + 集成 + 安全 |
| ZAP fuzz 误报 | 严格定义"正确行为"，只对偏离该行为报 bug |
| 性能基线数字与硬件相关 | 文档里写明测试环境参数；阈值用倍数（如"基线 × 1.2"）而非绝对值 |

## 验收标准

- [ ] 9 个模块全部走过代码审读，每个发现至少有 1 个回归测试或显式记录"经审读无问题"
- [ ] `uv run pytest` 全绿
- [ ] `scripts/test_e2e_all.py` 16/16 通过
- [ ] 压测、soak、ZAP fuzz、SQL 注入测试全部完成并记录结果
- [ ] `docs/bench-baseline.md` 与 `docs/bench-thresholds.md` 存在
- [ ] 全部 commit 干净（按阶段或按模块分组）

## 不在范围

- 协议扩展（新增 msg_type、新增压缩算法）
- 客户端 SDK 重构（只修 bug）
- 文档站 / 用户手册
- 新功能开发
