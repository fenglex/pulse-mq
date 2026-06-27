"""PulseMQ v2 深色监控 Web UI（中文 · 玻璃态美化版）。

单文件 HTML，内嵌 CSS + JS。
- 顶部：导航栏 + 连接状态 + 版本
- 指标卡片区：4 个带渐变发光的统计卡片（中文标签 + emoji 图标）
- 图表区：ECharts 多 topic 流量曲线（记录数 msg/s → 记录数/s）
  - 分钟粒度，1H / 6H 切换
  - 实时更新当前分钟数据点，30s 自动刷新历史
  - 最多 5 topic 叠加，LRU 淘汰
- 底部：topic 卡片网格
"""

from __future__ import annotations

INDEX_HTML = r"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>PulseMQ 监控面板</title>
<style>
:root {
  --bg-deep: #050a16;
  --bg-primary: #0a1326;
  --bg-card: rgba(17, 28, 50, 0.72);
  --bg-card-hover: rgba(22, 34, 64, 0.85);
  --border: rgba(56, 86, 138, 0.45);
  --border-active: #3b82f6;
  --text-primary: #eef2fa;
  --text-secondary: #8a9ab3;
  --text-muted: #4f5d74;
  --accent-blue: #3b82f6;
  --accent-cyan: #22d3ee;
  --accent-green: #34d399;
  --accent-amber: #fbbf24;
  --accent-purple: #a78bfa;
  --accent-rose: #fb7185;
  --glow-blue: rgba(59,130,246,0.35);
  --glow-cyan: rgba(34,211,238,0.30);
}

*{box-sizing:border-box;margin:0;padding:0}

body{
  font-family:'Inter',-apple-system,BlinkMacSystemFont,"Segoe UI","PingFang SC","Microsoft YaHei",sans-serif;
  background:
    radial-gradient(1200px 600px at 10% -10%, rgba(59,130,246,0.12), transparent 60%),
    radial-gradient(1000px 500px at 110% 10%, rgba(34,211,238,0.10), transparent 55%),
    var(--bg-deep);
  color:var(--text-primary);
  min-height:100vh;
  -webkit-font-smoothing:antialiased;
}

/* ===== 导航栏 ===== */
header{
  background:linear-gradient(135deg, rgba(10,19,38,0.92), rgba(13,26,48,0.92));
  backdrop-filter:blur(14px);
  -webkit-backdrop-filter:blur(14px);
  padding:14px 28px;
  border-bottom:1px solid var(--border);
  display:flex;justify-content:space-between;align-items:center;
  position:sticky;top:0;z-index:100;
  box-shadow:0 4px 24px rgba(0,0,0,0.25);
}
.logo{display:flex;align-items:center;gap:12px}
.logo-icon{
  width:36px;height:36px;border-radius:10px;
  background:linear-gradient(135deg,var(--accent-blue),var(--accent-cyan));
  display:flex;align-items:center;justify-content:center;
  font-weight:800;font-size:18px;color:#fff;
  box-shadow:0 0 18px var(--glow-blue);
}
.logo-text{font-size:19px;font-weight:700;color:var(--text-primary);letter-spacing:-0.3px}
.logo-text span{color:var(--accent-cyan);font-weight:400;font-size:13px;margin-left:8px}
.header-right{display:flex;align-items:center;gap:14px}
#conn-status{
  font-size:12px;padding:5px 14px;border-radius:20px;
  font-weight:500;letter-spacing:0.3px;transition:all .3s;
  display:inline-flex;align-items:center;gap:6px;
}
#conn-status::before{content:'';width:7px;height:7px;border-radius:50%;background:currentColor;box-shadow:0 0 6px currentColor}
#conn-status.ok{background:rgba(52,211,153,0.15);color:var(--accent-green);border:1px solid rgba(52,211,153,0.35)}
#conn-status.bad{background:rgba(251,113,133,0.15);color:var(--accent-rose);border:1px solid rgba(251,113,133,0.35)}
.version-tag{
  font-size:10px;color:var(--text-secondary);
  background:rgba(59,130,246,0.08);
  padding:3px 10px;border-radius:6px;border:1px solid var(--border);
}

/* ===== 主内容 ===== */
main{padding:24px 28px;max-width:1440px;margin:0 auto}

/* ===== 卡片网格 ===== */
.card-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:16px;margin-bottom:24px}
@media(max-width:900px){.card-grid{grid-template-columns:repeat(2,1fr)}}
@media(max-width:500px){.card-grid{grid-template-columns:1fr}}

.card{
  position:relative;overflow:hidden;
  background:var(--bg-card);
  backdrop-filter:blur(10px);
  -webkit-backdrop-filter:blur(10px);
  border:1px solid var(--border);
  border-radius:14px;padding:22px;
  transition:transform .25s, border-color .25s, box-shadow .25s;
}
.card:hover{
  transform:translateY(-2px);
  box-shadow:0 10px 32px rgba(0,0,0,0.4);
}
.card::before{
  content:'';position:absolute;top:0;left:0;right:0;height:2px;border-radius:14px 14px 0 0;
}
.card::after{
  content:'';position:absolute;right:-30px;top:-30px;width:120px;height:120px;border-radius:50%;
  opacity:.10;filter:blur(20px);pointer-events:none;
}
.card.blue::before{background:linear-gradient(90deg,var(--accent-blue),var(--accent-cyan))}
.card.blue::after{background:var(--accent-blue)}
.card.amber::before{background:linear-gradient(90deg,var(--accent-amber),#fcd34d)}
.card.amber::after{background:var(--accent-amber)}
.card.green::before{background:linear-gradient(90deg,var(--accent-green),#6ee7b7)}
.card.green::after{background:var(--accent-green)}
.card.purple::before{background:linear-gradient(90deg,var(--accent-purple),#c4b5fd)}
.card.purple::after{background:var(--accent-purple)}

.card .head{display:flex;align-items:center;justify-content:space-between;margin-bottom:10px}
.card .label{
  font-size:11px;color:var(--text-secondary);
  text-transform:uppercase;letter-spacing:.1em;font-weight:600;
}
.card .icon{font-size:18px;line-height:1;opacity:.9}
.card .value{
  font-size:32px;font-weight:800;color:var(--text-primary);
  letter-spacing:-0.5px;font-variant-numeric:tabular-nums;
  background:linear-gradient(135deg,#fff,#cfe0ff);
  -webkit-background-clip:text;background-clip:text;-webkit-text-fill-color:transparent;
}
.card .sub{font-size:13px;color:var(--text-secondary);margin-top:6px}

/* ===== 图表区 ===== */
.chart-section{
  background:var(--bg-card);
  backdrop-filter:blur(10px);
  -webkit-backdrop-filter:blur(10px);
  border:1px solid var(--border);
  border-radius:14px;padding:22px;margin-bottom:24px;
  transition:border-color .2s;
}
.chart-section:hover{border-color:rgba(59,130,246,0.4)}
.chart-header{display:flex;justify-content:space-between;align-items:center;margin-bottom:16px;flex-wrap:wrap;gap:10px}
.chart-title{font-size:14px;font-weight:600;color:var(--text-primary);display:flex;align-items:center;gap:8px}
.chart-title .dot-indicator{
  width:7px;height:7px;border-radius:50%;background:var(--accent-green);
  box-shadow:0 0 8px var(--accent-green);animation:pulse 2s infinite;
}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.4}}
.chart-controls{display:flex;align-items:center;gap:6px}
.time-btn{
  padding:6px 16px;border:1px solid var(--border);border-radius:8px;
  background:transparent;color:var(--text-secondary);
  cursor:pointer;font-size:12px;font-weight:500;transition:all .2s;
}
.time-btn:hover{border-color:var(--text-secondary);color:var(--text-primary)}
.time-btn.active{
  background:linear-gradient(135deg,var(--accent-blue),var(--accent-cyan));
  color:#fff;border-color:transparent;box-shadow:0 0 14px var(--glow-blue);
}
.chart-hint{font-size:11px;color:var(--text-muted);margin-left:8px}
#chart{width:100%;height:420px}

/* ===== Topic 列表 ===== */
.topic-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(280px,1fr));gap:12px}
.topic-card{
  position:relative;overflow:hidden;cursor:pointer;
  background:var(--bg-card);
  backdrop-filter:blur(8px);
  -webkit-backdrop-filter:blur(8px);
  border:1px solid var(--border);border-radius:12px;padding:16px;
  transition:all .25s;
}
.topic-card:hover{
  border-color:rgba(59,130,246,0.5);
  background:var(--bg-card-hover);
  transform:translateY(-2px);
  box-shadow:0 8px 24px rgba(0,0,0,0.3);
}
.topic-card.selected{border-color:var(--accent-blue);background:rgba(59,130,246,0.10)}
.topic-card.selected::after{
  content:'';position:absolute;top:0;left:0;right:0;height:2px;border-radius:12px 12px 0 0;
}
.topic-card .name{
  color:var(--accent-cyan);font-weight:600;font-size:14px;
  margin-bottom:8px;display:flex;align-items:center;gap:8px;
  word-break:break-all;
}
.topic-card .dot{width:9px;height:9px;border-radius:50%;flex-shrink:0;display:none;box-shadow:0 0 6px currentColor}
.topic-card.selected .dot{display:block}
.topic-card .info{color:var(--text-secondary);font-size:12px;display:flex;gap:14px;flex-wrap:wrap}
.topic-card .info span{display:flex;align-items:center;gap:4px}
.topic-card .rate{color:var(--accent-green);font-weight:600}
.topic-card .rec{color:var(--accent-amber)}
.topic-card .cache{color:var(--text-muted)}
.empty{text-align:center;padding:48px;color:var(--text-muted);font-size:13px}

/* ===== 事件流 ===== */
#event-stream .ev-row{
  padding:9px 14px;border-bottom:1px solid rgba(30,48,84,0.4);
  display:flex;justify-content:space-between;align-items:center;gap:10px;
  font-size:12px;color:var(--text-secondary);
}
#event-stream .ev-row:hover{background:rgba(59,130,246,0.06)}
#event-stream .ev-type{
  font-weight:600;padding:2px 8px;border-radius:6px;font-size:11px;
}
#event-stream .ev-type.connect{background:rgba(52,211,153,0.15);color:var(--accent-green)}
#event-stream .ev-type.disconnect{background:rgba(251,113,133,0.15);color:var(--accent-rose)}
#event-stream .ev-type.subscribe{background:rgba(59,130,246,0.15);color:var(--accent-blue)}
#event-stream .ev-type.unsubscribe{background:rgba(167,139,250,0.15);color:var(--accent-purple)}
#event-stream .ev-type.auth{background:rgba(251,191,36,0.15);color:var(--accent-amber)}
#event-stream .ev-type.other{background:rgba(138,154,179,0.15);color:var(--text-secondary)}
#event-stream .ev-ts{color:var(--text-muted);font-variant-numeric:tabular-nums;font-size:11px}
#event-stream .ev-detail{color:var(--text-primary);font-size:12px;flex:1;text-align:right;word-break:break-all}

/* ===== Modal ===== */
.modal-overlay{
  position:fixed;inset:0;background:rgba(2,6,15,0.72);
  backdrop-filter:blur(6px);-webkit-backdrop-filter:blur(6px);
  display:none;align-items:center;justify-content:center;z-index:200;
}
.modal-overlay.show{display:flex}
.modal{
  background:linear-gradient(135deg,rgba(13,26,48,0.96),rgba(10,19,38,0.96));
  border:1px solid var(--border);border-radius:16px;
  padding:24px;max-width:880px;width:92%;max-height:80vh;overflow:auto;
  box-shadow:0 24px 64px rgba(0,0,0,0.6);
}
.modal-head{display:flex;justify-content:space-between;align-items:center;margin-bottom:18px}
.modal-title{font-size:16px;font-weight:700;color:var(--text-primary)}
.modal-close{
  background:transparent;border:1px solid var(--border);border-radius:8px;
  color:var(--text-secondary);width:30px;height:30px;cursor:pointer;font-size:16px;
  display:flex;align-items:center;justify-content:center;transition:all .2s;
}
.modal-close:hover{color:var(--accent-rose);border-color:var(--accent-rose)}
.client-table{width:100%;border-collapse:collapse;font-size:12px}
.client-table th{
  text-align:left;padding:8px 10px;color:var(--text-secondary);
  font-weight:600;border-bottom:1px solid var(--border);
}
.client-table td{padding:8px 10px;border-bottom:1px solid rgba(30,48,84,0.4);color:var(--text-primary)}
.client-table tr:hover td{background:rgba(59,130,246,0.06)}
.client-role{font-size:11px;padding:2px 8px;border-radius:6px;font-weight:600}
.client-role.pub{background:rgba(251,191,36,0.15);color:var(--accent-amber)}
.client-role.sub{background:rgba(52,211,153,0.15);color:var(--accent-green)}
.client-role.mixed{background:rgba(59,130,246,0.15);color:var(--accent-blue)}

/* ===== 滚动条 ===== */
::-webkit-scrollbar{width:7px;height:7px}
::-webkit-scrollbar-track{background:var(--bg-deep)}
::-webkit-scrollbar-thumb{background:var(--border);border-radius:4px}
::-webkit-scrollbar-thumb:hover{background:var(--text-muted)}
</style>
</head>
<body>
<header>
  <div class="logo">
    <div class="logo-icon">P</div>
    <div class="logo-text">PulseMQ<span>监控面板</span></div>
  </div>
  <div class="header-right">
    <span class="version-tag" id="version-tag">v-</span>
    <span id="conn-status" class="bad">连接中</span>
  </div>
</header>
<main>
  <div class="card-grid" id="overview-cards">
    <div class="card blue">
      <div class="head">
        <div class="label">活跃主题</div>
        <div class="icon">📦</div>
      </div>
      <div class="value" id="v-topics">0</div>
      <div class="sub">近 8 小时内有流量</div>
    </div>
    <div class="card amber" title="近 60 秒平均值（估算）：当前分钟实测 + 上一分钟按比例外推。流量稳定时接近真实滑动窗口；突变时分钟开始处会有偏差。">
      <div class="head">
        <div class="label">消息量 / 秒</div>
        <div class="icon">⚡</div>
      </div>
      <div class="value" id="v-msgs">0.0</div>
      <div class="sub" id="v-msgs-sub">近60秒估算 · 本分钟实测 0 条</div>
    </div>
    <div class="card green" title="近 60 秒平均值（估算）：当前分钟实测 + 上一分钟按比例外推。统计的是压缩后的实际传输字节数（不含帧头开销）。">
      <div class="head">
        <div class="label">流量 / 秒</div>
        <div class="icon">🌐</div>
      </div>
      <div class="value" id="v-bytes">0 B/s</div>
      <div class="sub">近60秒估算（压缩后）</div>
    </div>
    <div class="card purple">
      <div class="head">
        <div class="label">运行时间</div>
        <div class="icon">⏱️</div>
      </div>
      <div class="value" id="v-uptime">0秒</div>
      <div class="sub">自启动以来</div>
    </div>
  </div>

  <div class="card-grid" id="overview-cards-clients">
    <div class="card blue" style="cursor:pointer" onclick="openClientModal()" title="点击查看在线 Client 详情">
      <div class="head">
        <div class="label">在线用户</div>
        <div class="icon">👤</div>
      </div>
      <div class="value" id="v-online-users">0</div>
      <div class="sub">已认证连接</div>
    </div>
    <div class="card amber">
      <div class="head">
        <div class="label">在线生产者</div>
        <div class="icon">📤</div>
      </div>
      <div class="value" id="v-online-producers">0</div>
      <div class="sub">PUB 角色连接</div>
    </div>
    <div class="card green">
      <div class="head">
        <div class="label">在线消费者</div>
        <div class="icon">📥</div>
      </div>
      <div class="value" id="v-online-consumers">0</div>
      <div class="sub">SUB 角色连接</div>
    </div>
    <div class="card purple">
      <div class="head">
        <div class="label">总订阅</div>
        <div class="icon">🔗</div>
      </div>
      <div class="value" id="v-total-subs">0</div>
      <div class="sub">活跃订阅条目</div>
    </div>
  </div>

  <div class="chart-section">
    <div class="chart-header">
      <div class="chart-title">
        <div class="dot-indicator"></div>
        <span>流量趋势（记录数 / 秒）<span class="chart-hint" style="margin-left:6px">分钟级精确值</span></span>
      </div>
      <div class="chart-controls">
        <button class="time-btn active" onclick="setTimeRange(60, this)">1 小时</button>
        <button class="time-btn" onclick="setTimeRange(360, this)">6 小时</button>
        <span class="chart-hint" id="chart-hint">点击下方主题叠加曲线</span>
      </div>
    </div>
    <div id="chart"></div>
  </div>

  <div class="chart-section">
    <div class="chart-header">
      <div class="chart-title">
        <div class="dot-indicator"></div>
        <span>端到端延迟分位（毫秒）<span class="chart-hint" style="margin-left:6px">P50 / P95 / P99 实时</span></span>
      </div>
    </div>
    <div id="latency-chart" style="width:100%;height:320px"></div>
  </div>

  <div class="chart-section">
    <div class="chart-header">
      <div class="chart-title">
        <div class="dot-indicator"></div>
        <span>最近事件流<span class="chart-hint" style="margin-left:6px">最新在上 · 自动滚动</span></span>
      </div>
    </div>
    <div id="event-stream" style="max-height:360px;overflow-y:auto;border-radius:10px;border:1px solid var(--border);background:rgba(10,19,38,0.4)">
      <div class="empty">等待事件…</div>
    </div>
  </div>

  <div class="chart-section">
    <div class="chart-header">
      <div class="chart-title"><span>📋 主题列表</span></div>
    </div>
    <div class="topic-grid" id="topic-list"></div>
  </div>
</main>

<div class="modal-overlay" id="client-modal" onclick="if(event.target===this)closeClientModal()">
  <div class="modal">
    <div class="modal-head">
      <div class="modal-title">👤 在线 Client 列表</div>
      <button class="modal-close" onclick="closeClientModal()">✕</button>
    </div>
    <div id="client-modal-body">
      <div class="empty">加载中…</div>
    </div>
  </div>
</div>

<script>
  window._tok = new URLSearchParams(location.search).get('token') || '';
  document.write('<script src="/static/echarts.min.js' +
    (window._tok ? '?token=' + encodeURIComponent(window._tok) : '') +
    '"><\/script>');
</script>
<script>
const _tok = window._tok || '';
function _authHeaders(extra) {
  const h = extra || {};
  if (_tok) { h['Authorization'] = 'Bearer ' + _tok; }
  return h;
}
function _withToken(url) {
  return _tok ? url + (url.includes('?') ? '&' : '?') + 'token=' + encodeURIComponent(_tok) : url;
}
const $ = id => document.getElementById(id);
const COLORS = ['#3b82f6','#fbbf24','#34d399','#a78bfa','#fb7185'];
const MAX_SELECTED = 5;

let state = {
  topics: {},
  cache_sizes: {},
  history_cache: {},
  selected: [],
  uptime: 0,
  timeRange: 60,
  onlineUsers: 0,
  onlineProducers: 0,
  onlineConsumers: 0,
  totalSubs: 0,
  latency: { p50: 0, p95: 0, p99: 0 },
  events: [],
};

let chart = null;
let latencyChart = null;
let firstSelectDone = false;
const MAX_EVENTS = 50;

/* ---- SSE ---- */
function connectSSE() {
  const es = new EventSource(_withToken('/api/v1/stats/stream'));
  es.onopen = () => { $('conn-status').textContent='实时'; $('conn-status').className='ok'; };
  es.onmessage = ev => {
    try {
      const d = JSON.parse(ev.data);
      state.topics = d.topics || {};
      state.cache_sizes = d.cache_sizes || {};
      if (d.start_time && d.server_time) {
        state.uptime = d.server_time - d.start_time;
      } else if (d.uptime_seconds != null) {
        state.uptime = d.uptime_seconds;
      }
      // Spec3 实时字段
      state.onlineUsers = d.online_users != null ? d.online_users : state.onlineUsers;
      state.onlineProducers = d.online_producers != null ? d.online_producers : state.onlineProducers;
      state.onlineConsumers = d.online_consumers != null ? d.online_consumers : state.onlineConsumers;
      state.totalSubs = d.total_subscriptions != null ? d.total_subscriptions : state.totalSubs;
      if (d.latency_p50_ms != null || d.latency_p95_ms != null || d.latency_p99_ms != null) {
        state.latency = {
          p50: d.latency_p50_ms != null ? d.latency_p50_ms : state.latency.p50,
          p95: d.latency_p95_ms != null ? d.latency_p95_ms : state.latency.p95,
          p99: d.latency_p99_ms != null ? d.latency_p99_ms : state.latency.p99,
        };
      }
      // 若 SSE 内嵌事件流，按事件数组增量入队
      if (Array.isArray(d.events)) {
        for (const e of d.events) pushEvent(e);
      } else if (d.event) {
        pushEvent(d.event);
      }
      render();
      renderOverview();
      renderLatency();
      renderEvents();
      if (!firstSelectDone && Object.keys(d.topics || {}).length > 0) {
        firstSelectDone = true;
        const firstName = Object.keys(d.topics)[0];
        state.selected.push(firstName);
        loadHistory(firstName).then(() => { render(); renderChart(); });
      }
    } catch(e) { console.error('SSE 解析失败', e); }
  };
  es.onerror = () => {
    $('conn-status').textContent='已断开';
    $('conn-status').className='bad';
    es.close();
    setTimeout(connectSSE, 3000);
  };
}

/* ---- 时间范围 ---- */
function setTimeRange(minutes, btn) {
  state.timeRange = minutes;
  document.querySelectorAll('.time-btn').forEach(b => b.classList.remove('active'));
  if (btn) btn.classList.add('active');
  state.history_cache = {};
  loadSelectedHistories().then(() => { render(); renderChart(); });
}

/* ---- 主题选择 ---- */
async function toggleTopic(name) {
  const idx = state.selected.indexOf(name);
  if (idx >= 0) {
    state.selected.splice(idx, 1);
  } else {
    if (state.selected.length >= MAX_SELECTED) state.selected.shift();
    state.selected.push(name);
    await loadHistory(name);
  }
  render();
  renderChart();
}

/* ---- 数据加载 ---- */
async function loadHistory(topic) {
  if (!state.history_cache[topic]) state.history_cache[topic] = {};
  const range = state.timeRange;
  if (state.history_cache[topic][range]) return;
  try {
    const r = await fetch(_withToken('/api/v1/topics/' + encodeURIComponent(topic) + '/history?minutes=' + range), {headers: _authHeaders()});
    const d = await r.json();
    state.history_cache[topic][range] = d.history || [];
  } catch(e) {
    state.history_cache[topic][range] = [];
  }
}

async function loadSelectedHistories() {
  await Promise.all(state.selected.map(n => loadHistory(n)));
}

/* ---- 渲染 ---- */
function render() {
  const topics = Object.entries(state.topics);
  $('v-topics').textContent = topics.length;

  // 消息量改用 record_count（记录数）统计
  let totalRate = 0, totalBytesRate = 0, totalRecordsCurrent = 0;
  for (const [,t] of topics) {
    totalRate += t.record_rate_1min || 0;
    totalBytesRate += t.bytes_rate_1min || 0;
    totalRecordsCurrent += t.record_count_current || 0;
  }
  $('v-msgs').textContent = totalRate.toFixed(1);
  $('v-msgs-sub').textContent = '近60秒估算 · 本分钟实测 ' + totalRecordsCurrent.toLocaleString() + ' 条';
  $('v-bytes').textContent = formatBytesRate(totalBytesRate);
  $('v-uptime').textContent = formatUptime(state.uptime);

  // 主题列表
  const list = $('topic-list');
  if (topics.length === 0) {
    list.innerHTML = '<div class="empty">暂无活跃主题</div>';
    return;
  }

  list.innerHTML = topics.map(([name, t]) => {
    const selIdx = state.selected.indexOf(name);
    const isSel = selIdx >= 0;
    const color = isSel ? COLORS[selIdx % COLORS.length] : 'transparent';
    const topBar = isSel ? `background:${COLORS[selIdx % COLORS.length]}` : '';
    return `<div class="topic-card ${isSel?'selected':''}" onclick="toggleTopic('${esc(name)}')">
      ${isSel ? `<div style="position:absolute;top:0;left:0;right:0;height:2px;border-radius:12px 12px 0 0;${topBar}"></div>` : ''}
      <div class="name"><span class="dot" style="background:${color};color:${color}"></span>${esc(name)}</div>
      <div class="info">
        <span class="rate">⚡ ${(t.record_rate_1min||0).toFixed(1)} 条/秒</span>
        <span class="cache">💾 缓存 ${formatCache(state.cache_sizes[name])}</span>
      </div>
    </div>`;
  }).join('');
}

function renderChart() {
  if (state.selected.length === 0) {
    $('chart-hint').textContent = '点击下方主题叠加曲线';
    if (chart) { chart.clear(); }
    return;
  }
  $('chart-hint').textContent = state.selected.join(' ｜ ');

  if (!chart) {
    chart = echarts.init($('chart'), null, { renderer: 'canvas' });
    window.addEventListener('resize', () => chart && chart.resize());
  }

  const range = state.timeRange;
  const nowMs = Date.now();
  const startTime = nowMs - range * 60 * 1000;

  const series = state.selected.map((name, i) => {
    const cached = (state.history_cache[name] && state.history_cache[name][range]) || [];
    // 时间戳去重
    const seen = new Map();
    for (const h of cached) {
      if (h.timestamp != null) seen.set(h.timestamp, h);
    }
    const deduped = [...seen.values()].sort((a, b) => a.timestamp - b.timestamp);
    // 改用 record_count（每秒记录数）
    const data = deduped.map(h => [h.timestamp * 1000, +((h.record_count || 0) / 60).toFixed(2)]);

    // 实时点
    const liveRate = state.topics[name] ? (state.topics[name].record_rate_1min || 0) : 0;
    const currentMinuteTs = Math.floor(Date.now() / 60000) * 60000;
    const lastTs = data.length > 0 ? data[data.length - 1][0] : 0;
    if (lastTs >= currentMinuteTs) {
      data[data.length - 1] = [currentMinuteTs, +liveRate.toFixed(2)];
    } else if (liveRate > 0) {
      data.push([currentMinuteTs, +liveRate.toFixed(2)]);
    }

    const color = COLORS[i % COLORS.length];
    return {
      name, type: 'line', data, smooth: true, showSymbol: false,
      lineStyle: { width: 2.5, color: color, shadowColor: color, shadowBlur: 6 },
      itemStyle: { color: color },
      areaStyle: {
        opacity: 0.18,
        color: {
          type: 'linear', x: 0, y: 0, x2: 0, y2: 1,
          colorStops: [
            { offset: 0, color: color },
            { offset: 1, color: 'transparent' },
          ],
        },
      },
    };
  });

  chart.setOption({
    backgroundColor: 'transparent',
    animation: true, animationDuration: 300,
    tooltip: {
      trigger: 'axis',
      backgroundColor: 'rgba(10,19,38,0.95)',
      borderColor: '#1e3054',
      textStyle: { color: '#eef2fa', fontSize: 12 },
      valueFormatter: v => v != null ? v.toFixed(2) + ' 条/秒' : '-',
    },
    legend: {
      type: 'scroll', top: 4, right: 4,
      textStyle: { color: '#8a9ab3', fontSize: 11 },
      pageTextStyle: { color: '#8a9ab3' },
      pageIconColor: '#8a9ab3',
      pageIconInactiveColor: '#4f5d74',
    },
    grid: { left: 56, right: 20, top: 36, bottom: 56 },
    xAxis: {
      type: 'time', min: startTime, max: nowMs,
      axisLine: { lineStyle: { color: '#1e3054' } },
      axisTick: { lineStyle: { color: '#1e3054' } },
      axisLabel: { color: '#8a9ab3', fontSize: 11, formatter: '{HH}:{mm}' },
      splitLine: { show: false },
    },
    yAxis: {
      type: 'value', name: '条/秒',
      nameTextStyle: { color: '#8a9ab3', fontSize: 11 },
      axisLine: { show: false },
      axisTick: { show: false },
      axisLabel: { color: '#8a9ab3', fontSize: 11 },
      splitLine: { lineStyle: { color: 'rgba(30,48,84,0.6)', type: 'dashed' } },
    },
    dataZoom: [
      { type: 'inside', xAxisIndex: 0 },
      { type: 'slider', height: 18, bottom: 6,
        backgroundColor: 'transparent', borderColor: '#1e3054',
        fillerColor: 'rgba(59,130,246,0.12)',
        handleStyle: { color: '#3b82f6', borderColor: '#3b82f6' },
        textStyle: { color: '#8a9ab3' },
        dataBackground: { lineStyle: { color: '#1e3054' } },
      },
    ],
    series,
  }, true);
}

/* ---- 工具函数 ---- */
function formatUptime(s) {
  if (s < 60) return Math.floor(s) + ' 秒';
  if (s < 3600) return Math.floor(s/60) + ' 分 ' + Math.floor(s%60) + ' 秒';
  if (s < 86400) return Math.floor(s/3600) + ' 小时 ' + Math.floor((s%3600)/60) + ' 分';
  return Math.floor(s/86400) + ' 天 ' + Math.floor((s%86400)/3600) + ' 小时';
}

function formatBytesRate(bps) {
  if (bps < 1024) return bps.toFixed(0) + ' B/s';
  if (bps < 1048576) return (bps/1024).toFixed(1) + ' KB/s';
  if (bps < 1073741824) return (bps/1048576).toFixed(1) + ' MB/s';
  return (bps/1073741824).toFixed(2) + ' GB/s';
}

function formatCache(c) {
  // c: {current, max} 或旧版数字
  if (c == null) return '0';
  if (typeof c === 'number') return c.toLocaleString();
  const cur = c.current || 0, max = c.max || 0;
  if (max <= 0) return cur.toLocaleString();
  const pct = cur / max;
  const suffix = pct >= 0.99 ? '（满）' : (pct >= 0.9 ? '（接近满）' : '');
  return cur.toLocaleString() + ' / ' + max.toLocaleString() + suffix;
}

function esc(s) { return String(s).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'})[c]); }

/* ---- Spec3 概览卡片 / 延迟 / 事件 / 详情弹窗 ---- */
function renderOverview() {
  $('v-online-users').textContent = state.onlineUsers.toLocaleString();
  $('v-online-producers').textContent = state.onlineProducers.toLocaleString();
  $('v-online-consumers').textContent = state.onlineConsumers.toLocaleString();
  $('v-total-subs').textContent = state.totalSubs.toLocaleString();
}

function renderLatency() {
  if (!latencyChart) {
    latencyChart = echarts.init($('latency-chart'), null, { renderer: 'canvas' });
    window.addEventListener('resize', () => latencyChart && latencyChart.resize());
  }
  const data = [
    { name: 'P50', value: +(state.latency.p50 || 0).toFixed(2) },
    { name: 'P95', value: +(state.latency.p95 || 0).toFixed(2) },
    { name: 'P99', value: +(state.latency.p99 || 0).toFixed(2) },
  ];
  const colors = ['#34d399', '#fbbf24', '#fb7185'];
  latencyChart.setOption({
    backgroundColor: 'transparent',
    animation: true, animationDuration: 300,
    tooltip: {
      trigger: 'axis',
      backgroundColor: 'rgba(10,19,38,0.95)',
      borderColor: '#1e3054',
      textStyle: { color: '#eef2fa', fontSize: 12 },
      valueFormatter: v => v != null ? v.toFixed(2) + ' ms' : '-',
    },
    grid: { left: 56, right: 24, top: 28, bottom: 36 },
    xAxis: {
      type: 'category', data: data.map(d => d.name),
      axisLine: { lineStyle: { color: '#1e3054' } },
      axisTick: { show: false },
      axisLabel: { color: '#8a9ab3', fontSize: 12 },
    },
    yAxis: {
      type: 'value', name: 'ms',
      nameTextStyle: { color: '#8a9ab3', fontSize: 11 },
      axisLine: { show: false },
      axisTick: { show: false },
      axisLabel: { color: '#8a9ab3', fontSize: 11 },
      splitLine: { lineStyle: { color: 'rgba(30,48,84,0.6)', type: 'dashed' } },
    },
    series: [{
      type: 'bar', data: data.map((d, i) => ({ value: d.value, itemStyle: { color: colors[i] } })),
      barWidth: '38%',
      itemStyle: { borderRadius: [6, 6, 0, 0] },
      label: {
        show: true, position: 'top', color: '#eef2fa', fontSize: 12,
        formatter: p => p.value.toFixed(2) + ' ms',
      },
    }],
  }, true);
}

function pushEvent(e) {
  // e: {type, ts/timestamp, detail/user/client_id}
  if (!e) return;
  const ev = {
    type: (e.type || 'other'),
    ts: e.ts != null ? e.ts : (e.timestamp != null ? e.timestamp : Date.now()),
    detail: e.detail != null ? e.detail
      : (e.client_id != null ? e.client_id : (e.user != null ? e.user : '')),
    raw: e,
  };
  state.events.unshift(ev);
  if (state.events.length > MAX_EVENTS) state.events.length = MAX_EVENTS;
}

function renderEvents() {
  const el = $('event-stream');
  if (!state.events || state.events.length === 0) {
    el.innerHTML = '<div class="empty">等待事件…</div>';
    return;
  }
  el.innerHTML = state.events.map(ev => {
    const tCls = ['connect','disconnect','subscribe','unsubscribe','auth'].includes(ev.type) ? ev.type : 'other';
    const ts = new Date(ev.ts).toLocaleTimeString('zh-CN', { hour12: false });
    return `<div class="ev-row">
      <span class="ev-type ${tCls}">${esc(ev.type)}</span>
      <span class="ev-detail">${esc(ev.detail)}</span>
      <span class="ev-ts">${esc(ts)}</span>
    </div>`;
  }).join('');
}

async function openClientModal() {
  const modal = $('client-modal');
  const body = $('client-modal-body');
  body.innerHTML = '<div class="empty">加载中…</div>';
  modal.classList.add('show');
  try {
    const r = await fetch(_withToken('/api/v1/clients'), { headers: _authHeaders() });
    if (!r.ok) {
      body.innerHTML = '<div class="empty">获取失败（HTTP ' + r.status + '）</div>';
      return;
    }
    const d = await r.json();
    const clients = Array.isArray(d) ? d : (d.clients || d.data || []);
    if (!clients.length) {
      body.innerHTML = '<div class="empty">当前无在线 Client</div>';
      return;
    }
    body.innerHTML = `<table class="client-table">
      <thead><tr>
        <th>Client ID</th><th>用户</th><th>角色</th>
        <th>订阅数</th><th>连接时长</th><th>远端</th>
      </tr></thead>
      <tbody>${clients.map(c => {
        const role = c.role || c.roles || '';
        let roleCls = 'mixed';
        let roleText = esc(role || 'unknown');
        if (role === 'pub' || role === 'producer') { roleCls = 'pub'; roleText = 'PUB'; }
        else if (role === 'sub' || role === 'consumer') { roleCls = 'sub'; roleText = 'SUB'; }
        else if (!role) { roleCls = 'mixed'; roleText = '-'; }
        const up = c.connected_at ? Math.max(0, Math.floor(Date.now()/1000 - c.connected_at)) : (c.uptime_seconds || 0);
        return `<tr>
          <td>${esc(c.client_id || c.id || '-')}</td>
          <td>${esc(c.user || c.username || '-')}</td>
          <td><span class="client-role ${roleCls}">${roleText}</span></td>
          <td>${esc((c.subscriptions != null ? c.subscriptions : (c.sub_count != null ? c.sub_count : '-')))}</td>
          <td>${formatUptime(up)}</td>
          <td>${esc(c.remote || c.peer || '-')}</td>
        </tr>`;
      }).join('')}</tbody>
    </table>`;
  } catch(e) {
    body.innerHTML = '<div class="empty">加载失败：' + esc(String(e)) + '</div>';
  }
}
function closeClientModal() { $('client-modal').classList.remove('show'); }

/* ---- 事件流 SSE ---- */
function connectEventStream() {
  try {
    const es = new EventSource(_withToken('/api/v1/events'));
    es.onmessage = ev => {
      try {
        const d = JSON.parse(ev.data);
        if (Array.isArray(d)) { for (const e of d) pushEvent(e); }
        else { pushEvent(d); }
        renderEvents();
      } catch(e) { /* ignore */ }
    };
    es.onerror = () => { es.close(); setTimeout(connectEventStream, 5000); };
  } catch(e) { /* endpoint may be absent; SSE realtime payload already feeds events */ }
}

/* ---- 图表 30s 自动刷新 ---- */
setInterval(() => {
  if (state.selected.length > 0) {
    state.history_cache = {};
    loadSelectedHistories().then(() => renderChart());
  }
}, 30000);

/* ---- 初始化 ---- */
connectSSE();
connectEventStream();
document.addEventListener('keydown', e => { if (e.key === 'Escape') closeClientModal(); });
fetch(_withToken('/api/v1/system/status'), {headers: _authHeaders()}).then(r=>r.json()).then(d => {
  state.uptime = d.uptime_seconds || 0;
  $('version-tag').textContent = 'v' + (d.version || '-');
  render();
}).catch(()=>{});
</script>
</body>
</html>
"""
