"""PulseMQ v2 深色卡片式 Web UI。

单文件 HTML，内嵌 CSS + JS + SVG 绘图。
- 顶部：4 个指标卡片
- 中部：选中 topic 的流量折线图（SVG，最近 60 分钟）
- 底部：topic 列表，点击切换图表
- 数据源：EventSource('/api/v1/stats/stream') 实时刷新
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
.chart-title{font-size:14px;color:#94a3b8;margin-bottom:12px}
.topic-list{display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:12px}
.topic-card{background:#16213e;border:1px solid #334155;border-radius:8px;padding:14px;cursor:pointer;transition:border-color .2s}
.topic-card:hover,.topic-card.active{border-color:#38bdf8}
.topic-card .name{color:#38bdf8;font-weight:600;font-size:14px;margin-bottom:4px}
.topic-card .info{color:#94a3b8;font-size:12px}
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

  <div class="chart-section" id="chart-section" style="display:none">
    <div class="chart-title" id="chart-title">选择一个 topic 查看流量曲线</div>
    <svg id="chart" viewBox="0 0 600 120" style="width:100%;height:120px"></svg>
    <div style="display:flex;gap:16px;font-size:11px;color:#94a3b8;margin-top:8px">
      <span><span style="color:#38bdf8">━</span> msg/s</span>
      <span><span style="color:#fbbf24">╌</span> bytes/s (KB)</span>
    </div>
  </div>

  <div class="chart-section">
    <div class="chart-title">Topic 列表</div>
    <div class="topic-list" id="topic-list"></div>
  </div>
</main>

<script>
const $ = id => document.getElementById(id);
let state = { topics: {}, cache_sizes: {}, history_cache: {}, selected_topic: null, uptime: 0 };

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
  list.innerHTML = topics.map(([name, t]) =>
    `<div class="topic-card ${state.selected_topic===name?'active':''}" onclick="selectTopic('${esc(name)}')">
      <div class="name">${esc(name)}</div>
      <div class="info">${(t.msg_rate_1min||0).toFixed(1)} msg/s · cache ${state.cache_sizes[name]||0}</div>
    </div>`
  ).join('');

  // 如果选中了 topic，刷新图表
  if (state.selected_topic) renderChart(state.selected_topic);
}

async function selectTopic(name) {
  state.selected_topic = name;
  $('chart-section').style.display = 'block';
  $('chart-title').textContent = name + ' — 最近 60 分钟流量';
  render();
  // 拉历史数据
  try {
    const r = await fetch('/api/v1/topics/' + encodeURIComponent(name) + '/history?minutes=60');
    const d = await r.json();
    state.history_cache[name] = d.history || [];
    renderChart(name);
  } catch(e) {}
}

function renderChart(name) {
  const history = state.history_cache[name] || [];
  if (history.length < 2) { $('chart').innerHTML = '<text x="300" y="60" text-anchor="middle" fill="#64748b" font-size="13">数据不足 (需运行 ≥ 1 分钟)</text>'; return; }

  const W = 600, H = 120, PAD = 10;
  const maxMsg = Math.max(...history.map(h => h.msg_count), 1);
  const maxBytes = Math.max(...history.map(h => h.bytes_total), 1);

  const xStep = (W - 2 * PAD) / Math.max(history.length - 1, 1);

  let msgPts = '', bytePts = '';
  history.forEach((h, i) => {
    const x = PAD + i * xStep;
    const yMsg = H - PAD - ((h.msg_count / maxMsg) * (H - 2 * PAD));
    const yByte = H - PAD - ((h.bytes_total / maxBytes) * (H - 2 * PAD));
    msgPts += `${x},${yMsg} `;
    bytePts += `${x},${yByte} `;
  });

  $('chart').innerHTML =
    `<line x1="${PAD}" y1="${H/2}" x2="${W-PAD}" y2="${H/2}" stroke="#334155" stroke-width="0.5"/>` +
    `<polyline fill="none" stroke="#38bdf8" stroke-width="2" points="${msgPts}"/>` +
    `<polyline fill="none" stroke="#fbbf24" stroke-width="1.5" stroke-dasharray="4,2" points="${bytePts}"/>`;
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
