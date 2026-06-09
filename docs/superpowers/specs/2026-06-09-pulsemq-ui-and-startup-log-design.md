# PulseMQ 监控 UI 与启动日志设计

> 日期: 2026-06-09
> 状态: 已批准
> 目标: 升级监控折线图为 ECharts（拉高 + tooltip + 多 topic 叠加），启动时打印 Publisher 配置表格

## 1. 范围

**前端 ECharts 改造**：
- 用 ECharts 5.5.0 替换当前 SVG 折线图
- 图表高度 120px → 400px
- 折线 tooltip、dataZoom、time xAxis
- 多 topic 叠加（最多 5 个，LRU 淘汰）
- 颜色调色板：浅蓝/琥珀/绿/紫/红

**资源打包**：
- `scripts/fetch_echarts.py` 从 jsDelivr 下载到 `src/pulsemq/admin/static/echarts.min.js`
- hatch 构建配置 `artifacts` 包含 static 目录
- admin server 新增 `/static/{path}` 路由

**启动日志**：
- 启动时打印配置表格到 stderr
- 字段：bind、admin_bind、auth 状态（用户列表脱敏）
- 关闭时不打印

## 2. 文件改动

```
src/pulsemq/admin/
├── web_ui.py                  # 重写 INDEX_HTML
├── server.py                  # 新增 /static/ 路由
├── static/echarts.min.js      # 【新增】下载产物
└── __init__.py                # 不变

src/pulsemq/publisher.py       # main()/start_async() 打印配置
scripts/fetch_echarts.py       # 【新增】下载脚本
pyproject.toml                 # hatch artifacts
tests/test_e2e_publisher.py    # 增加 /static/ + 启动日志断言
README.md                      # 简短说明
```

## 3. 启动日志

```
═══════════════════════════════════════════
  PulseMQ Publisher v2.0.0
═══════════════════════════════════════════
  bind              tcp://*:5555
  admin             0.0.0.0:9090
  auth              enabled (3 users: alice, bob, carol)
═══════════════════════════════════════════
```

- 字段左右对齐
- 关闭认证：`auth              disabled`
- >10 用户：`10 users: a, b, c, ... +5 more`
- 输出到 stderr
- `format_startup_table(cfg, api_keys, version)` 作为纯函数便于测试

## 4. 静态资源

- 路径：`/static/echarts.min.js`
- Content-Type：`application/javascript`
- Cache-Control：`public, max-age=3600`
- 安全：`..` 与绝对路径返回 400

## 5. 资源获取

- `scripts/fetch_echarts.py` 一次性下载
- 默认版本 5.5.0
- `ECHARTS_VERSION` 环境变量可覆盖
- 不进 hatch hook，避免构建时联网

## 6. 测试

- `test_format_startup_table_disabled_auth`
- `test_format_startup_table_users`
- `test_format_startup_table_truncation`
- `test_static_echarts_served` — GET /static/echarts.min.js → 200 + JS content

## 7. 不做的事

- 不在关闭时打印
- 不引入 ECharts 在线 CDN
- 不自动运行时下载
- 不改 `start_async()` 现有行为，仅前置增加一行打印
- 不动协议层

## 8. 运行方式

```bash
# 一次性下载 echarts
python scripts/fetch_echarts.py

# 跑新测试
python -m pytest tests/test_e2e_publisher.py -v -k "startup or static"
```
