"""PulseMQ v2 深色监控 Web UI。

单文件 HTML，内嵌 CSS + JS。
- 顶部：导航栏 + 状态 + 版本
- 指标卡片区：4 个带渐变边框的统计卡片
- 图表区：ECharts 多 topic 流量曲线
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
<title>PulseMQ Monitor</title>
<style>
:root {
  --bg-deep: #060d18;
  --bg-primary: #0b1428;
  --bg-card: #111c32;
  --bg-card-hover: #162240;
  --border: #1e3054;
  --border-active: #3b82f6;
  --text-primary: #e8edf5;
  --text-secondary: #7e8ca3;
  --text-muted: #4a5568;
  --accent-blue: #3b82f6;
  --accent-cyan: #06b6d4;
  --accent-green: #10b981;
  --accent-amber: #f59e0b;
  --accent-purple: #8b5cf6;
  --accent-rose: #f43f5e;
  --glow-blue: rgba(59,130,246,0.15);
  --glow-cyan: rgba(6,182,212,0.12);
}
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:'Inter',-apple-system,BlinkMacSystemFont,"Segoe UI","PingFang SC","Microsoft YaHei",sans-serif;background:var(--bg-deep);color:var(--text-primary);min-height:100vh}

/* 导航栏 */
header{background:linear-gradient(135deg,var(--bg-primary),#0d1a30);padding:14px 28px;border-bottom:1px solid var(--border);display:flex;justify-content:space-between;align-items:center;position:sticky;top:0;z-index:100;backdrop-filter:blur(12px)}
.logo{display:flex;align-items:center;gap:10px}
.logo-icon{width:32px;height:32px;border-radius:8px;background:linear-gradient(135deg,var(--accent-blue),var(--accent-cyan));display:flex;align-items:center;justify-content:center;font-weight:800;font-size:16px;color:#fff;box-shadow:0 0 16px var(--glow-blue)}
.logo-text{font-size:18px;font-weight:700;color:var(--text-primary);letter-spacing:-0.3px}
.logo-text span{color:var(--accent-cyan);font-weight:400;font-size:13px;margin-left:6px}
.header-right{display:flex;align-items:center;gap:16px}
#conn-status{font-size:11px;padding:4px 12px;border-radius:20px;font-weight:500;letter-spacing:0.3px;transition:all .3s}
#conn-status.ok{background:rgba(16,185,129,0.15);color:var(--accent-green);border:1px solid rgba(16,185,129,0.3)}
#conn-status.bad{background:rgba(244,63,94,0.15);color:var(--accent-rose);border:1px solid rgba(244,63,94,0.3)}
.version-tag{font-size:10px;color:var(--text-muted);background:var(--bg-card);padding:2px 8px;border-radius:4px;border:1px solid var(--border)}

/* 主内容 */
main{padding:24px 28px;max-width:1440px;margin:0 auto}

/* 卡片网格 */
.card-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:16px;margin-bottom:24px}
@media(max-width:900px){.card-grid{grid-template-columns:repeat(2,1fr)}}
@media(max-width:500px){.card-grid{grid-template-columns:1fr}}
.card{background:var(--bg-card);border:1px solid var(--border);border-radius:12px;padding:20px;position:relative;overflow:hidden;transition:all .25s}
.card:hover{border-color:rgba(59,130,246,0.4);transform:translateY(-1px);box-shadow:0 4px 24px rgba(0,0,0,0.3)}
.card::before{content:'';position:absolute;top:0;left:0;right:0;height:2px;border-radius:12px 12px 0 0}
.card.blue::before{background:linear-gradient(90deg,var(--accent-blue),var(--accent-cyan))}
.card.amber::before{background:linear-gradient(90deg,var(--accent-amber),#fbbf24)}
.card.green::before{background:linear-gradient(90deg,var(--accent-green),#34d399)}
.card.purple::before{background:linear-gradient(90deg,var(--accent-purple),#a78bfa)}
.card .label{font-size:11px;color:var(--text-secondary);margin-bottom:8px;text-transform:uppercase;letter-spacing:.08em;font-weight:500}
.card .value{font-size:30px;font-weight:700;color:var(--text-primary);letter-spacing:-0.5px;font-variant-numeric:tabular-nums}
.card .sub{font-size:13px;color:var(--text-secondary);margin-top:6px}

/* 图表区 */
.chart-section{background:var(--bg-card);border:1px solid var(--border);border-radius:12px;padding:20px;margin-bottom:24px;transition:border-color .2s}
.chart-section:hover{border-color:rgba(59,130,246,0.3)}
.chart-header{display:flex;justify-content:space-between;align-items:center;margin-bottom:16px;flex-wrap:wrap;gap:10px}
.chart-title{font-size:13px;font-weight:600;color:var(--text-primary);display:flex;align-items:center;gap:8px}
.chart-title .dot-indicator{width:6px;height:6px;border-radius:50%;background:var(--accent-green);animation:pulse 2s infinite}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.4}}
.chart-controls{display:flex;align-items:center;gap:6px}
.time-btn{padding:5px 14px;border:1px solid var(--border);border-radius:6px;background:transparent;color:var(--text-secondary);cursor:pointer;font-size:12px;font-weight:500;transition:all .2s}
.time-btn:hover{border-color:var(--text-secondary);color:var(--text-primary)}
.time-btn.active{background:linear-gradient(135deg,var(--accent-blue),var(--accent-cyan));color:#fff;border-color:transparent;box-shadow:0 0 12px var(--glow-blue)}
.chart-hint{font-size:11px;color:var(--text-muted);margin-left:8px}
#chart{width:100%;height:420px}

/* Topic 列表 */
.topic-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(260px,1fr));gap:12px}
.topic-card{background:var(--bg-card);border:1px solid var(--border);border-radius:10px;padding:16px;cursor:pointer;transition:all .25s;position:relative;overflow:hidden}
.topic-card:hover{border-color:rgba(59,130,246,0.4);background:var(--bg-card-hover);transform:translateY(-1px)}
.topic-card.selected{border-color:var(--accent-blue);background:rgba(59,130,246,0.08)}
.topic-card.selected::before{content:'';position:absolute;top:0;left:0;right:0;height:2px;border-radius:10px 10px 0 0}
.topic-card .name{color:var(--accent-cyan);font-weight:600;font-size:13px;margin-bottom:6px;display:flex;align-items:center;gap:8px}
.topic-card .dot{width:8px;height:8px;border-radius:50%;flex-shrink:0;display:none}
.topic-card.selected .dot{display:block}
.topic-card .info{color:var(--text-secondary);font-size:12px;display:flex;gap:12px}
.topic-card .info span{display:flex;align-items:center;gap:4px}
.topic-card .rate{color:var(--accent-green);font-weight:500}
.topic-card .cache{color:var(--text-muted)}
.empty{text-align:center;padding:48px;color:var(--text-muted);font-size:13px}

/* 滚动条 */
::-webkit-scrollbar{width:6px;height:6px}
::-webkit-scrollbar-track{background:var(--bg-deep)}
::-webkit-scrollbar-thumb{background:var(--border);border-radius:3px}
::-webkit-scrollbar-thumb:hover{background:var(--text-muted)}
</style>
</head>
<body>
<header>
  <div class="logo">
    <div class="logo-icon">P</div>
    <div class="logo-text">PulseMQ<span>Monitor</span></div>
  </div>
  <div class="header-right">
    <span class="version-tag" id="version-tag">v-</span>
    <span id="conn-status" class="bad">Connecting</span>
  </div>
</header>
<main>
  <div class="card-grid" id="overview-cards">
    <div class="card blue">
      <div class="label">Topics</div>
      <div class="value" id="v-topics">0</div>
      <div class="sub">active producers</div>
    </div>
    <div class="card amber">
      <div class="label">Messages / s</div>
      <div class="value" id="v-msgs">0.0</div>
      <div class="sub" id="v-msgs-sub">60s avg</div>
    </div>
    <div class="card green">
      <div class="label">Data / s</div>
      <div class="value" id="v-bytes">0 B</div>
      <div class="sub">60s avg</div>
    </div>
    <div class="card purple">
      <div class="label">Uptime</div>
      <div class="value" id="v-uptime">0s</div>
      <div class="sub" id="v-uptime-sub">since start</div>
    </div>
  </div>

  <div class="chart-section">
    <div class="chart-header">
      <div class="chart-title">
        <div class="dot-indicator"></div>
        <span>Traffic (msg/s)</span>
      </div>
      <div class="chart-controls">
        <button class="time-btn active" onclick="setTimeRange(60, this)">1H</button>
        <button class="time-btn" onclick="setTimeRange(360, this)">6H</button>
        <span class="chart-hint" id="chart-hint">click a topic below to overlay</span>
      </div>
    </div>
    <div id="chart"></div>
  </div>

  <div class="chart-section">
    <div class="chart-header">
      <div class="chart-title"><span>Topic List</span></div>
    </div>
    <div class="topic-grid" id="topic-list"></div>
  </div>
</main>

<script src="/static/echarts.min.js"></script>
<script>
const $ = id => document.getElementById(id);
const COLORS = ['#3b82f6','#f59e0b','#10b981','#8b5cf6','#f43f5e'];
const COLOR_NAMES = ['blue','amber','green','purple','rose'];
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

/* ---- SSE ---- */
function connectSSE() {
  const es = new EventSource('/api/v1/stats/stream');
  es.onopen = () => { $('conn-status').textContent='Live'; $('conn-status').className='ok'; };
  es.onmessage = ev => {
    try {
      const d = JSON.parse(ev.data);
      state.topics = d.topics || {};
      state.cache_sizes = d.cache_sizes || {};
      // 从 SSE 的 start_time + server_time 计算实时 uptime
      if (d.start_time && d.server_time) {
        state.uptime = d.server_time - d.start_time;
      } else if (d.uptime_seconds != null) {
        state.uptime = d.uptime_seconds;
      }
      render();
      // 首次收到数据时自动选中第一个 topic
      if (!firstSelectDone && Object.keys(d.topics || {}).length > 0) {
        firstSelectDone = true;
        const firstName = Object.keys(d.topics)[0];
        state.selected.push(firstName);
        loadHistory(firstName).then(() => { render(); renderChart(); });
      }
    } catch(e) { console.error('SSE parse', e); }
  };
  es.onerror = () => { $('conn-status').textContent='Offline'; $('conn-status').className='bad'; es.close(); setTimeout(connectSSE, 3000); };
}

/* ---- Time range ---- */
function setTimeRange(minutes, btn) {
  state.timeRange = minutes;
  document.querySelectorAll('.time-btn').forEach(b => b.classList.remove('active'));
  if (btn) btn.classList.add('active');
  state.history_cache = {};
  loadSelectedHistories().then(() => { render(); renderChart(); });
}

/* ---- Topic toggle ---- */
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

/* ---- Data loading ---- */
async function loadHistory(topic) {
  if (!state.history_cache[topic]) state.history_cache[topic] = {};
  const range = state.timeRange;
  if (state.history_cache[topic][range]) return;
  try {
    const r = await fetch('/api/v1/topics/' + encodeURIComponent(topic) + '/history?minutes=' + range);
    const d = await r.json();
    state.history_cache[topic][range] = d.history || [];
  } catch(e) {
    state.history_cache[topic][range] = [];
  }
}

async function loadSelectedHistories() {
  await Promise.all(state.selected.map(n => loadHistory(n)));
}

/* ---- Render ---- */
function render() {
  const topics = Object.entries(state.topics);
  $('v-topics').textContent = topics.length;

  let totalRate = 0, totalBytesRate = 0, totalRecords = 0;
  for (const [,t] of topics) {
    totalRate += t.msg_rate_1min || 0;
    totalBytesRate += t.bytes_rate_1min || 0;
    totalRecords += t.record_count_current || 0;
  }
  $('v-msgs').textContent = totalRate.toFixed(1);
  $('v-msgs-sub').textContent = totalRecords.toLocaleString() + ' records total';
  $('v-bytes').textContent = formatBytesRate(totalBytesRate);
  $('v-uptime').textContent = formatUptime(state.uptime);
  $('v-uptime-sub').textContent = 'since start';

  // Topic list
  const list = $('topic-list');
  if (topics.length === 0) { list.innerHTML = '<div class="empty">No active topics</div>'; return; }

  list.innerHTML = topics.map(([name, t]) => {
    const selIdx = state.selected.indexOf(name);
    const isSel = selIdx >= 0;
    const dotColor = isSel ? COLORS[selIdx % COLORS.length] : 'transparent';
    const borderColor = isSel ? `border-color:${COLORS[selIdx % COLORS.length]}` : '';
    return `<div class="topic-card ${isSel?'selected':''}" style="${borderColor}" onclick="toggleTopic('${esc(name)}')">
      ${isSel ? '<div style="position:absolute;top:0;left:0;right:0;height:2px;border-radius:10px 10px 0 0;background:'+COLORS[selIdx%COLORS.length]+'"></div>' : ''}
      <div class="name"><span class="dot" style="background:${dotColor}"></span>${esc(name)}</div>
      <div class="info">
        <span class="rate">${(t.msg_rate_1min||0).toFixed(1)} msg/s</span>
        <span class="cache">cache ${state.cache_sizes[name]||0}</span>
      </div>
    </div>`;
  }).join('');
}

function renderChart() {
  if (state.selected.length === 0) {
    $('chart-hint').textContent = 'click a topic below to overlay';
    if (chart) { chart.clear(); }
    return;
  }
  $('chart-hint').textContent = state.selected.join(' | ');

  if (!chart) {
    chart = echarts.init($('chart'), null, { renderer: 'canvas' });
    window.addEventListener('resize', () => chart && chart.resize());
  }

  const range = state.timeRange;
  const nowMs = Date.now();
  const startTime = nowMs - range * 60 * 1000;

  const series = state.selected.map((name, i) => {
    const cached = (state.history_cache[name] && state.history_cache[name][range]) || [];
    // timestamp dedup
    const seen = new Map();
    for (const h of cached) {
      if (h.timestamp != null) seen.set(h.timestamp, h);
    }
    const deduped = [...seen.values()].sort((a, b) => a.timestamp - b.timestamp);
    const data = deduped.map(h => [h.timestamp * 1000, +(h.msg_count / 60).toFixed(2)]);

    // live point
    const liveRate = state.topics[name] ? (state.topics[name].msg_rate_1min || 0) : 0;
    const currentMinuteTs = Math.floor(Date.now() / 60000) * 60000;
    const lastTs = data.length > 0 ? data[data.length - 1][0] : 0;
    if (lastTs >= currentMinuteTs) {
      data[data.length - 1] = [currentMinuteTs, +liveRate.toFixed(2)];
    } else if (liveRate > 0) {
      data.push([currentMinuteTs, +liveRate.toFixed(2)]);
    }

    return {
      name, type: 'line', data, smooth: true, showSymbol: false,
      lineStyle: { width: 2.5, color: COLORS[i % COLORS.length] },
      itemStyle: { color: COLORS[i % COLORS.length] },
    };
  });

  chart.setOption({
    backgroundColor: 'transparent',
    animation: true, animationDuration: 300,
    tooltip: {
      trigger: 'axis',
      backgroundColor: 'rgba(11,20,40,0.95)',
      borderColor: '#1e3054',
      textStyle: { color: '#e8edf5', fontSize: 12 },
      valueFormatter: v => v != null ? v.toFixed(2) + ' msg/s' : '-',
    },
    legend: {
      type: 'scroll', top: 4, right: 4,
      textStyle: { color: '#7e8ca3', fontSize: 11 },
      pageTextStyle: { color: '#7e8ca3' },
      pageIconColor: '#7e8ca3',
      pageIconInactiveColor: '#4a5568',
    },
    grid: { left: 56, right: 20, top: 36, bottom: 56 },
    xAxis: {
      type: 'time', min: startTime, max: nowMs,
      axisLine: { lineStyle: { color: '#1e3054' } },
      axisTick: { lineStyle: { color: '#1e3054' } },
      axisLabel: { color: '#7e8ca3', fontSize: 11, formatter: '{HH}:{mm}' },
      splitLine: { show: false },
    },
    yAxis: {
      type: 'value', name: 'msg/s',
      nameTextStyle: { color: '#7e8ca3', fontSize: 11 },
      axisLine: { show: false },
      axisTick: { show: false },
      axisLabel: { color: '#7e8ca3', fontSize: 11 },
      splitLine: { lineStyle: { color: '#1e3054', type: 'dashed' } },
    },
    dataZoom: [
      { type: 'inside', xAxisIndex: 0 },
      { type: 'slider', height: 18, bottom: 6,
        backgroundColor: 'transparent', borderColor: '#1e3054',
        fillerColor: 'rgba(59,130,246,0.1)',
        handleStyle: { color: '#3b82f6', borderColor: '#3b82f6' },
        textStyle: { color: '#7e8ca3' },
        dataBackground: { lineStyle: { color: '#1e3054' } },
      },
    ],
    series,
  }, true);
}

/* ---- History refresh: only on user action (toggle topic / switch range) ---- */

/* ---- Utilities ---- */
function formatUptime(s) {
  if (s < 60) return Math.floor(s) + 's';
  if (s < 3600) return Math.floor(s/60) + 'm ' + Math.floor(s%60) + 's';
  if (s < 86400) return Math.floor(s/3600) + 'h ' + Math.floor((s%3600)/60) + 'm';
  return Math.floor(s/86400) + 'd ' + Math.floor((s%86400)/3600) + 'h';
}

function formatBytesRate(bps) {
  if (bps < 1024) return bps.toFixed(0) + ' B/s';
  if (bps < 1048576) return (bps/1024).toFixed(1) + ' KB/s';
  return (bps/1048576).toFixed(1) + ' MB/s';
}

function formatBytes(b) {
  if (b < 1024) return b + ' B';
  if (b < 1048576) return (b/1024).toFixed(1) + ' KB';
  return (b/1048576).toFixed(1) + ' MB';
}

function esc(s) { return String(s).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'})[c]); }

/* ---- Chart 30s auto-refresh ---- */
setInterval(() => {
  if (state.selected.length > 0) {
    state.history_cache = {};
    loadSelectedHistories().then(() => renderChart());
  }
}, 30000);

/* ---- Auto-select first topic on first SSE ---- */
let firstSelectDone = false;

/* ---- Init ---- */
connectSSE();
fetch('/api/v1/system/status').then(r=>r.json()).then(d => {
  state.uptime = d.uptime_seconds || 0;
  $('version-tag').textContent = 'v' + (d.version || '-');
  render();
}).catch(()=>{});
</script>
</body>
</html>
"""
