# 09 Admin 服务与 Web UI 设计

> 源码：`src/pulsemq/admin/server.py`（HTTP/SSE）+ `src/pulsemq/admin/web_ui.py`（单文件 UI）+ `src/pulsemq/admin/static/echarts.min.js`

## 1. 定位与约束

- stdlib `asyncio.start_server` 手写 HTTP/1.1 解析，**不引入 Web 框架**。
- 只读观测面：全部端点为 GET；无写入型管理操作（凭据管理走 CLI + SIGHUP）。
- 运行在独立 daemon 线程 + 独立 event loop（`admin_thread=True` 默认），HTTP 慢客户端不拖累 ZMQ；`start()` 阻塞至 `_thread_started` 置位（端口可用）才返回。

## 2. 端点一览

| 端点 | 功能 | 数据源 |
|------|------|--------|
| `GET /`（或 `/index.html`） | Web UI 首页（内嵌 `INDEX_HTML`） | `web_ui.py` |
| `GET /static/{path}` | 静态资源（本地化 echarts.min.js，Cache-Control 1h） | 文件系统 |
| `GET /api/v1/stats/realtime` | 实时指标 JSON（同 SSE 帧内容） | 全部统计对象快照 |
| `GET /api/v1/stats/stream` | SSE 实时推送（1s 一帧） | 同上 |
| `GET /api/v1/clients` | 在线 client 明细 | `ConnectionStats.online_clients` |
| `GET /api/v1/events?limit=50` | 生命周期事件 | 事件环 |
| `GET /api/v1/topics` | topic 列表 + 当前指标 | `TrafficStats.all_topics_snapshot` |
| `GET /api/v1/topics/{topic}/history?minutes=60` | 分钟级历史（内存 + SQLite 合并） | §5 |
| `GET /api/v1/latency/topics/{topic}/history?minutes=60&kind=half\|e2e` | 延迟趋势序列 | 对应 `LatencyStatsRegistry.get_history` |
| `GET /api/v1/system/status` | version / start_time / uptime_seconds | |
| `GET /healthz` | 健康检查（**唯一免 token 端点**） | |

认证：除 `/healthz` 外均过 `TokenAuth.validate`（Bearer 或 `?token=`，恒时比较），失败 401。

## 3. 请求处理管线（`_handle_request`）

```
readline 请求行（5s 超时）→ 解析 method/path
逐行读 header（5s 超时，小写化 key）
content-length 存在则 readexactly body（10s 超时）
urlparse → path + parse_qs
TokenAuth 校验（/healthz 豁免）
_route 分发 → _respond_json/_respond_html/_route_static
finally：writer.close（SSE 连接打 _sse_takeover 标记跳过）
```

静态资源安全：拒绝路径段含 `..`、以 `/` 开头、含 `\`；`resolve()` 后必须 `is_relative_to(STATIC_ROOT)`；非文件 404。

## 4. 线程模式与关闭

| 模式 | 行为 |
|------|------|
| `admin_thread=True`（默认） | daemon 线程自建 loop；`_serve` 建 server 后 `serve_forever()` 阻塞。`stop()` 用 `call_soon_threadsafe(_do_stop)` **同步回调**关 server/取消 SSE（不用 `run_coroutine_threadsafe`——避免协程在 loop 关闭前未被执行的 "coroutine was never awaited" 警告），再 join 线程（5s）。线程 finally 统一 cancel 未完成任务再关 loop |
| `admin_thread=False` | 在调用方 loop 上只建 server + SSE 任务即返回（内联模式） |

## 5. 历史数据合并算法（`_topic_history`）

分钟级历史 = **内存优先 + SQLite 补洞**：

```
mem = traffic.get_history(topic, minutes)
len(mem) >= minutes-1 → 内存已覆盖（当前进行中分钟未入 slots，故阈值 -1）→ 直接返回
否则 db = storage.load_history(topic, now - minutes*60)
按 timestamp 去重合并（内存优先，同刻覆盖 db）→ 排序返回
```

用途：进程重启后 8h 内存窗口清空，图表由 SQLite 补齐更长历史。

## 6. SSE 设计

```
_sse_broadcast_loop（admin loop，1s 周期）：
    frame = realtime_snapshot() 序列化
    对每个客户端 queue.put_nowait(frame)
      QueueFull（maxsize=64）→ cancel 该客户端 writer 任务 + 移除
      （防死/慢客户端在字典中残留造成内存泄漏）

客户端接入 _handle_sse：
    手写 SSE 响应头（text/event-stream / no-cache / keep-alive / X-Accel-Buffering:no）
    发 ": connected" 注释行 → 注册 (queue, writer_task) → writer._sse_takeover = True
_sse_writer：循环 queue.get → write + drain；断连/取消时清理注册表并关 writer
```

`_realtime_snapshot()` 汇总：topics 全量快照、online_clients/subscriptions（注入的 `snapshot_fn`）、latency_half/latency_e2e、drops、连接 counters、最近 10 条事件（`sse_events`，前端整帧替换而非增量）、server_time 与 start_time（供前端实时算 uptime）。

注意：广播周期在代码中**固定 1s**（`asyncio.sleep(1.0)`）；配置项 `monitoring.sse_interval` 已定义但当前未接线。

## 7. Web UI（`web_ui.py`，单文件内嵌）

- 单 `INDEX_HTML` 字符串常量：HTML + CSS + JS 全内嵌，无外部构建链；唯一外部资源是本地化 `/static/echarts.min.js`（`scripts/fetch_echarts.py` 离线下载）。
- 视觉：深色玻璃态（backdrop-filter、渐变发光卡片）、中文界面。
- 布局自上而下：
  1. 导航栏：logo + SSE 连接状态指示（ok/bad）+ 版本 tag；
  2. 概览卡片 ×4：记录数/s、传输字节率、（近 60s 均值，卡片 title 说明估算口径）等；
  3. 客户端卡片 ×4：在线总数（点击弹窗看明细）/ producers / consumers / 订阅总数；
  4. 流量图表：ECharts 多 topic 叠加曲线（记录数/s，分钟粒度），1H/8H 切换、实时补当前分钟数据点、30s 刷新历史、**最多 5 topic 叠加**（`MAX_SELECTED=5`）；
  5. 延迟区：P50/P95/P99 列表（half/e2e 切换）+ 延迟趋势折线（kind=half|e2e）；
  6. 事件流：最近 50 条生命周期事件，按 type 着色（connect/disconnect/subscribe/unsubscribe/auth/other）；
  7. topic 卡片网格：每 topic 速率 + 丢弃标记（drops_current/1h_total >0 时显示）。
- 数据通道：`EventSource(/api/v1/stats/stream?token=...)` 为主（token 注入 query），历史曲线按需 fetch 对应 history API；`_withToken/_authHeaders` 统一携带 token。
- XSS 防护：渲染前 `esc()` 转义 HTML 实体。
