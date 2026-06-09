"""PulseMQ v2 深色卡片式 Web UI。

单文件 HTML，内嵌 CSS + JS。
- 顶部：4 个指标卡片
- 中部：ECharts 多 topic 流量曲线（最近 60 分钟，msg/s）
  - 高度 400px
  - tooltip、dataZoom、时间轴
  - 最多叠加 5 个 topic，LRU 淘汰
- 底部：topic 列表，点击切换图表叠加
- 数据源：EventSource('/api/v1/stats/stream') 实时刷新 + 选中时拉 history
- 静态资源：<script src="/static/echarts.min.js">
"""

from __future__ import annotations

INDEX_HTML = r"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>PulseMQ Publisher</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI","PingFang SC","Microsoft YaHei",monospace;background:#0f172a;color:#e2e8f0;min-height:100vh}
header{background:#1e293b;padding:16px 24px;border-bottom:1px solid #334155;display:flex;justify-content:space-between;align-items:center}
header h1{font-size:20px;color:#38bdf8}
#conn-status{font-size:12px;padding:4px 10px;border-radius:4px}
#conn-status.ok{background:#065f46;color:#6ee7b7}
#conn-status.bad{background:#7f1d1d;color:#fca5a5}
main{padding:24px;max-width:1400px;margin:0 auto}
.card-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:16px;margin-bottom:24px}
.card{background:#1e293b;border:1px solid #334155;border-radius:8px;padding:16px}
.card .label{font-size:12px;color:#94a3b8;margin-bottom:6px;text-transform:uppercase;letter-spacing:.05em}
.card .value{font-size:28px;font-weight:600;color:#f1f5f9}
.card .value.blue{color:#38bdf8}
.card .value.green{color:#6ee7b7}
.card .value.amber{color:#fbbf24}
.card .value.purple{color:#c084fc}
.chart-section{background:#1e293b;border:1px solid #334155;border-radius:8px;padding:20px;margin-bottom:24px}
.chart-title{font-size:14px;color:#94a3b8;margin-bottom:12px;display:flex;justify-content:space-between;align-items:center}
.chart-title .hint{font-size:11px;color:#64748b}
#chart{width:100%;height:400px}
.topic-list{display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:12px}
.topic-card{background:#16213e;border:1px solid #334155;border-radius:8px;padding:14px;cursor:pointer;transition:border-color .2s;position:relative}
.topic-card:hover{border-color:#94a3b8}
.topic-card.selected{border-color:#38bdf8;background:#1e3a5f}
.topic-card .name{color:#38bdf8;font-weight:600;font-size:14px;margin-bottom:4px}
.topic-card .info{color:#94a3b8;font-size:12px}
.topic-card .dot{position:absolute;top:14px;right:14px;width:10px;height:10px;border-radius:50%}
.empty{text-align:center;padding:40px;color:#64748b}
</style>
</head>
<body>
<header>
  <h1>PulseMQ Publisher</h1>
  <span id="conn-status" class="bad">未连接</span>
</header>
<main>
  <div class="card-grid" id="overview-cards">
    <div class="card"><div class="label">Topics</div><div class="value blue" id="v-topics">0</div></div>
    <div class="card"><div class="label">Msg/s</div><div class="value amber" id="v-msgs">0</div></div>
    <div class="card"><div class="label">Bytes/s</div><div class="value green" id="v-bytes">0</div></div>
    <div class="card"><div class="label">Uptime</div><div class="value purple" id="v-uptime">0s</div></div>
  </div>

  <div class="chart-section">
    <div class="chart-title">
      <span>流量曲线 (msg/s) — 点击 topic 卡片叠加（最多 5 个，LRU 淘汰）</span>
      <span class="hint" id="chart-hint">未选中任何 topic</span>
    </div>
    <div id="chart"></div>
  </div>

  <div class="chart-section">
    <div class="chart-title"><span>Topic 列表</span></div>
    <div class="topic-list" id="topic-list"></div>
  </div>
</main>

<script src="/static/echarts.min.js"></script>
<script>
const $ = id => document.getElementById(id);
const COLORS = ['#38bdf8','#fbbf24','#6ee7b7','#c084fc','#f87171'];
const MAX_SELECTED = 5;
let state = { topics: {}, cache_sizes: {}, history_cache: {}, selected: [], uptime: 0 };
let chart = null;

// ECharts dark 主题内置；不依赖外部主题文件

// SSE 连接
function connectSSE() {
  const es = new EventSource('/api/v1/stats/stream');
  es.onopen = () => { $('conn-status').textContent='已连接'; $('conn-status').className='ok'; };
  es.onmessage = ev => {
    try {
      const d = JSON.parse(ev.data);
      state.topics = d.topics || {};
      state.cache_sizes = d.cache_sizes || {};
      state.uptime = d.uptime_seconds || 0;
      render();
    } catch(e) { console.error('SSE parse', e); }
  };
  es.onerror = () => { $('conn-status').textContent='已断开'; $('conn-status').className='bad'; es.close(); setTimeout(connectSSE, 3000); };
}

function render() {
  const topics = Object.entries(state.topics);
  $('v-topics').textContent = topics.length;

  let totalRate = 0, totalBytes = 0;
  for (const [,t] of topics) { totalRate += t.msg_rate_1min || 0; totalBytes += t.bytes_total_current || 0; }
  $('v-msgs').textContent = totalRate.toFixed(1);
  $('v-bytes').textContent = formatBytes(totalBytes);
  $('v-uptime').textContent = formatUptime(state.uptime);

  // Topic 列表
  const list = $('topic-list');
  if (topics.length === 0) { list.innerHTML = '<div class="empty">暂无 topic</div>'; return; }
  list.innerHTML = topics.map(([name, t]) => {
    const selIdx = state.selected.indexOf(name);
    const isSel = selIdx >= 0;
    const dotColor = isSel ? COLORS[selIdx % COLORS.length] : 'transparent';
    return `<div class="topic-card ${isSel?'selected':''}" onclick="toggleTopic('${esc(name)}')">
      <div class="dot" style="background:${dotColor}"></div>
      <div class="name">${esc(name)}</div>
      <div class="info">${(t.msg_rate_1min||0).toFixed(1)} msg/s · cache ${state.cache_sizes[name]||0}</div>
    </div>`;
  }).join('');

  // 刷新图表
  renderChart();
}

async function toggleTopic(name) {
  const idx = state.selected.indexOf(name);
  if (idx >= 0) {
    state.selected.splice(idx, 1);
  } else {
    if (state.selected.length >= MAX_SELECTED) state.selected.shift();  // LRU 淘汰
    state.selected.push(name);
    // 拉历史
    if (!state.history_cache[name]) {
      try {
        const r = await fetch('/api/v1/topics/' + encodeURIComponent(name) + '/history?minutes=60');
        const d = await r.json();
        state.history_cache[name] = d.history || [];
      } catch(e) { state.history_cache[name] = []; }
    }
  }
  render();
}

function renderChart() {
  if (state.selected.length === 0) {
    $('chart-hint').textContent = '未选中任何 topic';
    if (chart) { chart.clear(); }
    return;
  }
  $('chart-hint').textContent = `已选 ${state.selected.length}/${MAX_SELECTED}: ${state.selected.join(', ')}`;

  if (!chart) {
    chart = echarts.init($('chart'), 'dark', { renderer: 'canvas' });
    window.addEventListener('resize', () => chart && chart.resize());
  }

  const series = state.selected.map((name, i) => {
    const hist = state.history_cache[name] || [];
    // msg_count → msg_rate（条/秒 = msg_count / 60）
    const data = hist
      .filter(h => h.timestamp != null)
      .map(h => [h.timestamp * 1000, +(h.msg_count / 60).toFixed(2)]);
    // 用当前 traffic 实时值覆盖最后一点
    const liveRate = state.topics[name] ? (state.topics[name].msg_rate_1min || 0) : 0;
    if (data.length > 0) data[data.length - 1] = [Date.now(), +liveRate.toFixed(2)];
    else if (liveRate > 0) data.push([Date.now(), +liveRate.toFixed(2)]);
    return {
      name, type: 'line', data, smooth: true, showSymbol: false,
      lineStyle: { width: 2, color: COLORS[i % COLORS.length] },
      itemStyle: { color: COLORS[i % COLORS.length] },
    };
  });

  chart.setOption({
    backgroundColor: 'transparent',
    tooltip: { trigger: 'axis', valueFormatter: v => v.toFixed(2) },
    legend: { type: 'scroll', top: 0, textStyle: { color: '#94a3b8' } },
    grid: { left: 60, right: 30, top: 40, bottom: 60 },
    xAxis: {
      type: 'time',
      axisLine: { lineStyle: { color: '#334155' } },
      axisLabel: { color: '#94a3b8' },
    },
    yAxis: {
      type: 'value', name: 'msg/s', nameTextStyle: { color: '#94a3b8' },
      axisLine: { lineStyle: { color: '#334155' } },
      axisLabel: { color: '#94a3b8' },
      splitLine: { lineStyle: { color: '#1e293b' } },
    },
    dataZoom: [
      { type: 'inside' },
      { type: 'slider', height: 20, bottom: 10, backgroundColor: '#0f172a', borderColor: '#334155' },
    ],
    series,
  }, true);
}

function formatUptime(s) {
  if (s < 60) return s.toFixed(0) + 's';
  if (s < 3600) return Math.floor(s/60) + 'm' + Math.floor(s%60) + 's';
  if (s < 86400) return Math.floor(s/3600) + 'h' + Math.floor((s%3600)/60) + 'm';
  return Math.floor(s/86400) + 'd' + Math.floor((s%86400)/3600) + 'h';
}

function formatBytes(b) {
  if (b < 1024) return b + ' B';
  if (b < 1048576) return (b/1024).toFixed(1) + ' KB';
  return (b/1048576).toFixed(1) + ' MB';
}

function esc(s) { return String(s).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'})[c]); }

connectSSE();
fetch('/api/v1/system/status').then(r=>r.json()).then(d => { state.uptime = d.uptime_seconds || 0; render(); }).catch(()=>{});
</script>
</body>
</html>
"""
