# PulseMQ 设计文档（源码反向推导）

> 本套文档由源码逐文件通读后反向生成，不依赖既有设计文档/设计说明。
> 内容以当前代码（v9.0.2）实际行为为准；与代码不一致处以代码为准。

## 文档地图

| 文档 | 范围 | 覆盖源码 |
|------|------|----------|
| [01-architecture.md](01-architecture.md) | 整体架构：拓扑、线程模型、数据流、核心决策 | 全局 |
| [02-protocol.md](02-protocol.md) | 协议层：帧格式、序列化、压缩、标志位 | `protocol/*` |
| [03-transport.md](03-transport.md) | 传输层：ROUTER/DEALER、ZAP、monitor、同步数据面线程 | `transport/router.py` |
| [04-server.md](04-server.md) | 服务端：组装、控制分发、后台循环、流控、生命周期 | `server.py`, `lifecycle.py` |
| [05-client.md](05-client.md) | 客户端：启动/重连状态机、两线程消费模型 | `client.py` |
| [06-routing.md](06-routing.md) | 路由与在线注册：COW 无锁读、订阅前缀匹配 | `routing.py`, `control.py` |
| [07-security.md](07-security.md) | 安全：bcrypt 凭据、ZAP PLAIN、Admin Token、热更新 | `security.py`, `auth.py`, `admin/auth.py` |
| [08-stats.md](08-stats.md) | 统计子系统：流量/延迟/连接/丢弃/持久化 | `stats/*` |
| [09-admin.md](09-admin.md) | Admin 服务：REST、SSE、Web UI | `admin/server.py`, `admin/web_ui.py` |
| [10-producers.md](10-producers.md) | Producer 调度：定时/burst 回调 | `producers/*` |
| [11-foundation.md](11-foundation.md) | 基础设施：配置、异常体系、日志、CLI、打包 | `config.py`, `errors.py`, `logging_setup.py`, `cli/*`, `pyproject.toml` |

## 一句话架构

Client/Server 模型的内存消息中间件：ZeroMQ ROUTER/DEALER 承载**数据面/控制面分离**的双通道，ZAP PLAIN + bcrypt 做接入认证，服务端只解码帧头部（payload 零反序列化透传），按 topic 前缀匹配路由到订阅者；内置分钟粒度的流量/延迟/丢弃统计与 SQLite 归档，通过独立线程上的 HTTP 服务（REST + SSE + 单文件 Web UI）对外暴露监控。

## 术语约定

| 术语 | 含义 | 源码对应 |
|------|------|----------|
| 数据面 | 承载消息收发的 ROUTER(5555)/DEALER 通道 | `Transport.bind_sync_data` / `connect("data")` |
| 控制面 | 承载 REGISTER/HEARTBEAT/SUBSCRIBE 等的 ROUTER(5556)/DEALER 通道 | `Transport.bind("control")` / `connect("control")` |
| identity | ROUTER 侧的 **bytes** 路由键（发送目标） | `send_multipart([identity, frame])` |
| client_id | REGISTER payload 中的应用层字符串标识 | `Client._client_id`（uuid4 hex） |
| 半程延迟 | producer → server 的传输延迟 | `LatencyStatsRegistry(_lat_half)` |
| 端到端延迟 | producer → consumer 回传的全程延迟 | `LatencyStatsRegistry(_lat_e2e)` + `LATENCY_REPORT` |
| 信用（credit） | 消费者心跳上报的解码队列剩余容量，用于服务端流控 | `Server._credits` |
