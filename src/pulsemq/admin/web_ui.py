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
      <div class="chart-title"><span>📋 主题列表</span></div>
    </div>
    <div class="topic-grid" id="topic-list"></div>
  </div>
</main>

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
};

let chart = null;
let firstSelectDone = false;

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
      render();
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

/* ---- 图表 30s 自动刷新 ---- */
setInterval(() => {
  if (state.selected.length > 0) {
    state.history_cache = {};
    loadSelectedHistories().then(() => renderChart());
  }
}, 30000);

/* ---- 初始化 ---- */
connectSSE();
fetch(_withToken('/api/v1/system/status'), {headers: _authHeaders()}).then(r=>r.json()).then(d => {
  state.uptime = d.uptime_seconds || 0;
  $('version-tag').textContent = 'v' + (d.version || '-');
  render();
}).catch(()=>{});
</script>
</body>
</html>
"""
