"""PulseMQ 管理后台 Web UI (单页应用)。

5 个 tab: 概览 / 主题 / 客户端 / 用户 / 批量
- 纯 HTML/CSS/JS, 无 React/Vue, 无 Tailwind, 无外部 CDN
- 嵌入在 Python 字符串常量 INDEX_HTML 中
- 实时刷新: SSE EventSource (1s 一帧) + 手动刷新按钮
"""

from __future__ import annotations


# 静态首页 HTML (单页应用)
INDEX_HTML = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>PulseMQ 管理后台</title>
<style>
* { box-sizing: border-box; margin: 0; padding: 0; }
body {
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", "PingFang SC",
                 "Hiragino Sans GB", "Microsoft YaHei", sans-serif;
    background: #0f172a; color: #e2e8f0; min-height: 100vh;
}
header {
    background: #1e293b; padding: 16px 24px; border-bottom: 1px solid #334155;
    display: flex; justify-content: space-between; align-items: center;
}
header h1 { font-size: 20px; color: #38bdf8; }
#conn-status { font-size: 12px; padding: 4px 10px; border-radius: 4px; }
#conn-status.ok { background: #065f46; color: #6ee7b7; }
#conn-status.bad { background: #7f1d1d; color: #fca5a5; }
nav { background: #1e293b; display: flex; padding: 0 24px; border-bottom: 1px solid #334155; }
nav button {
    background: transparent; color: #94a3b8; border: none;
    padding: 14px 20px; cursor: pointer; font-size: 14px;
    border-bottom: 2px solid transparent; transition: 0.2s;
}
nav button:hover { color: #e2e8f0; }
nav button.active { color: #38bdf8; border-bottom-color: #38bdf8; }
main { padding: 24px; max-width: 1400px; margin: 0 auto; }
.tab { display: none; }
.tab.active { display: block; }
.card-grid {
    display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
    gap: 16px; margin-bottom: 24px;
}
.card {
    background: #1e293b; border: 1px solid #334155; border-radius: 8px;
    padding: 16px;
}
.card .label { font-size: 12px; color: #94a3b8; margin-bottom: 6px; }
.card .value { font-size: 24px; font-weight: 600; color: #f1f5f9; }
.card .value.sm { font-size: 16px; }
.card.warn .value { color: #fbbf24; }
.card.error .value { color: #f87171; }
.card.ok .value { color: #6ee7b7; }
table { width: 100%; border-collapse: collapse; background: #1e293b;
    border: 1px solid #334155; border-radius: 8px; overflow: hidden; }
th, td { padding: 10px 14px; text-align: left; border-bottom: 1px solid #334155;
    font-size: 13px; }
th { background: #0f172a; color: #94a3b8; font-weight: 500; text-transform: uppercase;
    font-size: 11px; letter-spacing: 0.05em; }
tr:hover { background: #273449; }
.badge { display: inline-block; padding: 2px 8px; border-radius: 4px;
    font-size: 11px; font-weight: 500; }
.badge.ok { background: #065f46; color: #6ee7b7; }
.badge.warn { background: #78350f; color: #fcd34d; }
.badge.error { background: #7f1d1d; color: #fca5a5; }
.badge.admin { background: #1e3a8a; color: #93c5fd; }
button.btn {
    background: #0284c7; color: white; border: none; padding: 8px 14px;
    border-radius: 4px; cursor: pointer; font-size: 13px;
}
button.btn:hover { background: #0369a1; }
button.btn.danger { background: #b91c1c; }
button.btn.danger:hover { background: #991b1b; }
button.btn.small { padding: 4px 10px; font-size: 12px; }
input, select {
    background: #0f172a; border: 1px solid #334155; color: #e2e8f0;
    padding: 6px 10px; border-radius: 4px; font-size: 13px;
    font-family: inherit;
}
input:focus, select:focus { outline: none; border-color: #38bdf8; }
label { display: block; font-size: 12px; color: #94a3b8; margin-bottom: 4px; }
.form-row { display: flex; gap: 12px; margin-bottom: 12px; flex-wrap: wrap; }
.form-row > div { flex: 1; min-width: 200px; }
.modal-bg {
    display: none; position: fixed; inset: 0; background: rgba(0,0,0,0.6);
    align-items: center; justify-content: center; z-index: 100;
}
.modal-bg.show { display: flex; }
.modal {
    background: #1e293b; border: 1px solid #334155; border-radius: 8px;
    padding: 24px; min-width: 420px; max-width: 600px;
}
.modal h3 { margin-bottom: 16px; color: #38bdf8; }
.modal-actions { display: flex; gap: 8px; justify-content: flex-end; margin-top: 16px; }
.toolbar { display: flex; gap: 8px; margin-bottom: 16px; align-items: center; }
.toolbar input { flex: 1; max-width: 300px; }
.sparkline { display: inline-block; vertical-align: middle; }
.muted { color: #64748b; font-size: 12px; }
.empty { text-align: center; padding: 40px; color: #64748b; }
pre { background: #0f172a; padding: 12px; border-radius: 4px;
    overflow-x: auto; font-size: 12px; }
.spinner { display: inline-block; width: 12px; height: 12px;
    border: 2px solid #38bdf8; border-top-color: transparent;
    border-radius: 50%; animation: spin 0.8s linear infinite; }
@keyframes spin { to { transform: rotate(360deg); } }
</style>
</head>
<body>
<header>
    <h1>PulseMQ 管理后台</h1>
    <div>
        <span id="conn-status" class="bad">未连接</span>
        <button class="btn small" onclick="refreshAll()">手动刷新</button>
    </div>
</header>
<nav>
    <button data-tab="overview" class="active">概览</button>
    <button data-tab="topics">主题</button>
    <button data-tab="clients">客户端</button>
    <button data-tab="users">用户/权限</button>
    <button data-tab="batch">批量配置</button>
</nav>
<main>
    <!-- 概览 -->
    <div id="tab-overview" class="tab active">
        <h2 style="margin-bottom: 16px; font-size: 18px;">系统状态</h2>
        <div class="card-grid" id="overview-cards"></div>
        <h2 style="margin-bottom: 16px; font-size: 18px;">实时指标 (1s 刷新)</h2>
        <div class="card-grid" id="realtime-cards"></div>
    </div>

    <!-- 主题 -->
    <div id="tab-topics" class="tab">
        <h2 style="margin-bottom: 16px; font-size: 18px;">主题监控</h2>
        <div class="toolbar">
            <input id="topic-filter" placeholder="按 topic 过滤" oninput="renderTopics()">
            <button class="btn small" onclick="loadTopics()">刷新</button>
        </div>
        <div id="topics-content"></div>
    </div>

    <!-- 客户端 -->
    <div id="tab-clients" class="tab">
        <h2 style="margin-bottom: 16px; font-size: 18px;">在线客户端</h2>
        <div class="toolbar">
            <span id="clients-count" class="muted"></span>
            <button class="btn small" onclick="loadClients()">刷新</button>
        </div>
        <div id="clients-content"></div>
    </div>

    <!-- 用户/权限 -->
    <div id="tab-users" class="tab">
        <h2 style="margin-bottom: 16px; font-size: 18px;">用户管理</h2>
        <div class="toolbar">
            <button class="btn" onclick="showCreateUser()">+ 添加用户</button>
            <button class="btn small" onclick="loadUsers()">刷新</button>
        </div>
        <div id="users-content"></div>

        <h2 style="margin: 24px 0 16px; font-size: 18px;">权限规则</h2>
        <div class="toolbar">
            <button class="btn" onclick="showGrantPerm()">+ 授予权限</button>
            <button class="btn small" onclick="loadPermissions()">刷新</button>
        </div>
        <div id="permissions-content"></div>
    </div>

    <!-- 批量配置 -->
    <div id="tab-batch" class="tab">
        <h2 style="margin-bottom: 16px; font-size: 18px;">BATCH 客户端配置</h2>
        <div class="form-row">
            <div>
                <label>选择用户</label>
                <select id="batch-user-select" onchange="loadBatchConfig()"></select>
            </div>
        </div>
        <div id="batch-config-form"></div>
    </div>
</main>

<!-- 添加用户模态框 -->
<div id="modal-user" class="modal-bg">
    <div class="modal">
        <h3>添加用户</h3>
        <div class="form-row">
            <div><label>用户名</label><input id="new-username" placeholder="alice"></div>
            <div><label>角色</label>
                <select id="new-role"><option value="user">user</option><option value="admin">admin</option></select>
            </div>
        </div>
        <div class="form-row">
            <div><label>命名空间</label><input id="new-namespace" placeholder="(可选)"></div>
            <div><label>最大连接数</label><input id="new-maxconn" type="number" value="10"></div>
        </div>
        <div class="modal-actions">
            <button class="btn" onclick="hideModal('modal-user')">取消</button>
            <button class="btn" onclick="submitCreateUser()">创建</button>
        </div>
    </div>
</div>

<!-- 授予权限模态框 -->
<div id="modal-perm" class="modal-bg">
    <div class="modal">
        <h3>授予权限</h3>
        <div class="form-row">
            <div><label>用户 ID</label><input id="perm-user-id" type="number"></div>
            <div><label>操作</label>
                <select id="perm-action">
                    <option value="pub">pub</option>
                    <option value="sub">sub</option>
                    <option value="query">query</option>
                </select>
            </div>
        </div>
        <div class="form-row">
            <div><label>Topic 模式</label><input id="perm-pattern" placeholder="*.mkt.* 或 team-a.>"></div>
        </div>
        <div class="modal-actions">
            <button class="btn" onclick="hideModal('modal-perm')">取消</button>
            <button class="btn" onclick="submitGrantPerm()">授予</button>
        </div>
    </div>
</div>

<script>
const $ = (id) => document.getElementById(id);
let state = {
    topics: [], clients: [], users: [], perms: [],
    realtime: null, snapshot: null, system: null
};

// ---- Tab 切换 ----
document.querySelectorAll('nav button').forEach(btn => {
    btn.addEventListener('click', () => {
        document.querySelectorAll('nav button').forEach(b => b.classList.remove('active'));
        document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
        btn.classList.add('active');
        $('tab-' + btn.dataset.tab).classList.add('active');
        // 切换时主动刷新一次
        if (btn.dataset.tab === 'topics') loadTopics();
        if (btn.dataset.tab === 'clients') loadClients();
        if (btn.dataset.tab === 'users') { loadUsers(); loadPermissions(); }
        if (btn.dataset.tab === 'batch') { loadUsers().then(populateBatchSelect); }
    });
});

// ---- SSE 接入 ----
function connectSSE() {
    const es = new EventSource('/api/v1/metrics/stream');
    es.onopen = () => { $('conn-status').textContent = '已连接'; $('conn-status').className = 'ok'; };
    es.onmessage = (ev) => {
        try {
            const data = JSON.parse(ev.data);
            state.realtime = data;
            renderRealtime();
            renderOverview();
        } catch (e) { console.error('SSE parse error', e); }
    };
    es.onerror = () => {
        $('conn-status').textContent = '已断开';
        $('conn-status').className = 'bad';
        es.close();
        setTimeout(connectSSE, 3000);  // 自动重连
    };
}

// ---- API 工具 ----
async function api(path, opts = {}) {
    try {
        const r = await fetch(path, {
            headers: { 'Content-Type': 'application/json' },
            ...opts
        });
        const text = await r.text();
        let data;
        try { data = JSON.parse(text); } catch { data = { raw: text }; }
        if (!r.ok) throw new Error(data.error || `HTTP ${r.status}`);
        return data;
    } catch (e) {
        console.error('API error', path, e);
        return null;
    }
}

async function loadTopics() {
    const data = await api('/api/v1/topics');
    if (data) { state.topics = data.topics || []; renderTopics(); }
}
async function loadClients() {
    const data = await api('/api/v1/clients');
    if (data) { state.clients = data.clients || []; renderClients(); }
}
async function loadUsers() {
    const data = await api('/api/v1/users');
    if (data) { state.users = data.users || []; renderUsers(); return data.users || []; }
    return [];
}
async function loadPermissions() {
    const data = await api('/api/v1/permissions');
    if (data) { state.perms = data.permissions || []; renderPermissions(); }
}
async function loadSystem() {
    const data = await api('/api/v1/system/status');
    if (data) { state.system = data; renderOverview(); }
}
async function loadBatchConfig() {
    const uid = $('batch-user-select').value;
    if (!uid) return;
    const data = await api(`/api/v1/users/${uid}/batch_config`);
    if (data) renderBatchForm(data);
}
async function loadTopicHistory(topic) {
    const data = await api(`/api/v1/topics/${encodeURIComponent(topic)}/history?minutes=60`);
    return data ? data.history : [];
}

function refreshAll() {
    loadTopics(); loadClients(); loadUsers().then(populateBatchSelect);
    loadPermissions(); loadSystem();
}

// ---- 渲染 ----
function renderRealtime() {
    const r = state.realtime;
    if (!r) return;
    const cards = [
        { label: 'msg/s (EWMA)', value: r.msg_rate },
        { label: 'record/s', value: r.record_rate },
        { label: 'bytes/s', value: r.bytes_rate },
        { label: 'p50 延迟 (ms)', value: r.latency_p50_ms, sm: true },
        { label: 'p99 延迟 (ms)', value: r.latency_p99_ms, sm: true },
        { label: '在线连接', value: r.active_connections },
        { label: '活跃订阅', value: r.active_subscriptions },
        { label: '丢弃总数', value: r.dropped_total },
        { label: '错误率 (/s)', value: r.error_rate, sm: true },
        { label: '客户端数', value: r.clients_online },
        { label: '引擎批大小', value: r.engine_batch_size, sm: true },
        { label: '并发使用率', value: (r.engine_concurrency_usage * 100).toFixed(1) + '%', sm: true }
    ];
    $('realtime-cards').innerHTML = cards.map(c =>
        `<div class="card"><div class="label">${c.label}</div>
         <div class="value ${c.sm ? 'sm' : ''}">${c.value ?? '-'}</div></div>`
    ).join('');
}

function renderOverview() {
    const s = state.system;
    const r = state.realtime;
    if (!s) return;
    const cards = [
        { label: '版本', value: s.version, sm: true },
        { label: '启动时间', value: new Date(s.start_time * 1000).toLocaleString(), sm: true },
        { label: '运行时长', value: formatUptime(s.uptime_seconds) },
        { label: 'msg/s (EWMA)', value: r ? r.msg_rate : '-' },
        { label: 'p50 延迟', value: r ? r.latency_p50_ms + ' ms' : '-', sm: true },
        { label: 'p99 延迟', value: r ? r.latency_p99_ms + ' ms' : '-', sm: true },
        { label: '在线客户端', value: r ? r.clients_online : '-' },
        { label: '背压状态', value: r && r.backpressure ? '触发' : '正常',
          warn: r && r.backpressure }
    ];
    $('overview-cards').innerHTML = cards.map(c =>
        `<div class="card ${c.warn ? 'warn' : ''}"><div class="label">${c.label}</div>
         <div class="value ${c.sm ? 'sm' : ''}">${c.value ?? '-'}</div></div>`
    ).join('');
}

function renderTopics() {
    const filter = ($('topic-filter').value || '').toLowerCase();
    const list = state.topics.filter(t => !filter || t.topic.toLowerCase().includes(filter));
    if (list.length === 0) {
        $('topics-content').innerHTML = '<div class="empty">暂无 topic 数据</div>';
        return;
    }
    let html = '<table><thead><tr>';
    html += '<th>Topic</th><th>msg/s</th><th>1min 计数</th><th>p50 (ms)</th><th>p99 (ms)</th><th>背压</th><th>操作</th>';
    html += '</tr></thead><tbody>';
    for (const t of list) {
        const bp = t.backpressure
            ? '<span class="badge error">触发</span>'
            : '<span class="badge ok">正常</span>';
        html += `<tr>
            <td><code>${escapeHtml(t.topic)}</code></td>
            <td>${t.msg_rate_1min.toFixed(2)}</td>
            <td>${t.msg_count_1min}</td>
            <td>${t.latency_p50_1min.toFixed(3)}</td>
            <td>${t.latency_p99_1min.toFixed(3)}</td>
            <td>${bp}</td>
            <td><button class="btn small" onclick="showTopicHistory('${escapeAttr(t.topic)}')">历史</button></td>
        </tr>`;
    }
    html += '</tbody></table>';
    $('topics-content').innerHTML = html;
}

async function showTopicHistory(topic) {
    const rows = await loadTopicHistory(topic);
    if (rows.length === 0) {
        alert('Topic ' + topic + ' 无历史数据 (需运行 ≥ 1 分钟)');
        return;
    }
    let msg = 'Topic: ' + topic + '\\n最近 ' + rows.length + ' 分钟:\\n\\n';
    for (const r of rows) {
        msg += `[${new Date(r.minute_ts * 1000).toLocaleTimeString()}] msgs=${r.msg_count} p50=${r.latency_p50_ms?.toFixed(2) ?? '-'} p99=${r.latency_p99_ms?.toFixed(2) ?? '-'}\\n`;
    }
    alert(msg);
}

function renderClients() {
    $('clients-count').textContent = `共 ${state.clients.length} 个在线客户端`;
    if (state.clients.length === 0) {
        $('clients-content').innerHTML = '<div class="empty">暂无在线客户端</div>';
        return;
    }
    let html = '<table><thead><tr>';
    html += '<th>Identity</th><th>用户 ID</th><th>连接时长</th><th>心跳</th><th>订阅数</th><th>msg in/s</th><th>msg out/s</th>';
    html += '</tr></thead><tbody>';
    const now = Date.now() / 1000;
    for (const c of state.clients) {
        const connDuration = formatUptime(now - c.connected_at);
        const hbAgo = (now - c.last_heartbeat).toFixed(1) + 's 前';
        html += `<tr>
            <td><code>${escapeHtml(c.identity.substring(0, 16))}…</code></td>
            <td>${c.user_id ?? '-'}</td>
            <td>${connDuration}</td>
            <td>${hbAgo}</td>
            <td>${c.subscribed_topics.length}</td>
            <td>${c.msg_in_rate_1min.toFixed(2)}</td>
            <td>${c.msg_out_rate_1min.toFixed(2)}</td>
        </tr>`;
    }
    html += '</tbody></table>';
    $('clients-content').innerHTML = html;
}

function renderUsers() {
    if (state.users.length === 0) {
        $('users-content').innerHTML = '<div class="empty">暂无用户</div>';
        return;
    }
    let html = '<table><thead><tr>';
    html += '<th>ID</th><th>用户名</th><th>角色</th><th>命名空间</th><th>禁用</th><th>API Key</th><th>操作</th>';
    html += '</tr></thead><tbody>';
    for (const u of state.users) {
        const roleBadge = u.role === 'admin'
            ? '<span class="badge admin">admin</span>'
            : `<span class="badge">${escapeHtml(u.role)}</span>`;
        const disabled = u.disabled ? '<span class="badge error">是</span>' : '<span class="badge ok">否</span>';
        const keyShort = u.api_key.substring(0, 12) + '…' + u.api_key.substring(u.api_key.length - 4);
        html += `<tr>
            <td>${u.id}</td>
            <td>${escapeHtml(u.username)}</td>
            <td>${roleBadge}</td>
            <td>${escapeHtml(u.namespace || '-')}</td>
            <td>${disabled}</td>
            <td><code>${escapeHtml(keyShort)}</code></td>
            <td>
                <button class="btn small" onclick="regenKey(${u.id})">重置 Key</button>
                <button class="btn small danger" onclick="deleteUser(${u.id}, '${escapeAttr(u.username)}')">删除</button>
            </td>
        </tr>`;
    }
    html += '</tbody></table>';
    $('users-content').innerHTML = html;
}

function renderPermissions() {
    if (state.perms.length === 0) {
        $('permissions-content').innerHTML = '<div class="empty">暂无权限规则</div>';
        return;
    }
    let html = '<table><thead><tr>';
    html += '<th>用户 ID</th><th>用户名</th><th>操作</th><th>Topic 模式</th><th>操作</th>';
    html += '</tr></thead><tbody>';
    for (const p of state.perms) {
        html += `<tr>
            <td>${p.user_id}</td>
            <td>${escapeHtml(p.username || '-')}</td>
            <td><span class="badge">${escapeHtml(p.action)}</span></td>
            <td><code>${escapeHtml(p.topic_pattern)}</code></td>
            <td>
                <button class="btn small danger" onclick="revokePerm(${p.user_id}, '${escapeAttr(p.topic_pattern)}', '${escapeAttr(p.action)}')">撤销</button>
            </td>
        </tr>`;
    }
    html += '</tbody></table>';
    $('permissions-content').innerHTML = html;
}

function renderBatchForm(data) {
    $('batch-config-form').innerHTML = `
        <div class="form-row">
            <div><label>batch_size</label><input id="bs-size" type="number" value="${data.batch_size}"></div>
            <div><label>batch_interval_ms</label><input id="bs-interval" type="number" value="${data.batch_interval_ms}"></div>
            <div><label>batch_max_wait_ms</label><input id="bs-wait" type="number" value="${data.batch_max_wait_ms}"></div>
        </div>
        <button class="btn" onclick="submitBatchConfig(${data.user_id})">保存</button>
    `;
}

function populateBatchSelect(users) {
    const sel = $('batch-user-select');
    sel.innerHTML = users.map(u => `<option value="${u.id}">${escapeHtml(u.username)} (${u.id})</option>`).join('');
    if (users.length > 0) loadBatchConfig();
}

// ---- 提交操作 ----
function showCreateUser() { $('modal-user').classList.add('show'); }
function showGrantPerm() { $('modal-perm').classList.add('show'); }
function hideModal(id) { $(id).classList.remove('show'); }

async function submitCreateUser() {
    const body = {
        username: $('new-username').value.trim(),
        role: $('new-role').value,
        namespace: $('new-namespace').value.trim(),
        max_connections: parseInt($('new-maxconn').value, 10)
    };
    if (!body.username) { alert('用户名不能为空'); return; }
    const r = await api('/api/v1/users', { method: 'POST', body: JSON.stringify(body) });
    if (r) { hideModal('modal-user'); loadUsers().then(populateBatchSelect); }
}
async function submitGrantPerm() {
    const body = {
        user_id: parseInt($('perm-user-id').value, 10),
        action: $('perm-action').value,
        topic_pattern: $('perm-pattern').value.trim()
    };
    if (!body.user_id || !body.topic_pattern) { alert('用户 ID 和 topic 模式必填'); return; }
    const r = await api('/api/v1/permissions', { method: 'POST', body: JSON.stringify(body) });
    if (r) { hideModal('modal-perm'); loadPermissions(); }
}
async function submitBatchConfig(uid) {
    const body = {
        batch_size: parseInt($('bs-size').value, 10),
        batch_interval_ms: parseInt($('bs-interval').value, 10),
        batch_max_wait_ms: parseInt($('bs-wait').value, 10)
    };
    const r = await api(`/api/v1/users/${uid}/batch_config`, { method: 'PUT', body: JSON.stringify(body) });
    if (r) alert('已保存');
}
async function regenKey(uid) {
    if (!confirm('确认重置用户 ' + uid + ' 的 API Key?')) return;
    await api(`/api/v1/users/${uid}/api_keys`, { method: 'POST' });
    loadUsers();
}
async function deleteUser(uid, name) {
    if (!confirm('确认删除用户 ' + name + ' (ID=' + uid + ')?')) return;
    await api(`/api/v1/users/${uid}`, { method: 'DELETE' });
    loadUsers();
}
async function revokePerm(uid, pattern, action) {
    if (!confirm('确认撤销权限 ' + action + ':' + pattern + '?')) return;
    await api(`/api/v1/permissions?user_id=${uid}&topic_pattern=${encodeURIComponent(pattern)}&action=${action}`, { method: 'DELETE' });
    loadPermissions();
}

// ---- 工具 ----
function formatUptime(seconds) {
    if (!seconds || seconds < 0) return '-';
    if (seconds < 60) return seconds.toFixed(1) + 's';
    if (seconds < 3600) return Math.floor(seconds / 60) + 'm' + Math.floor(seconds % 60) + 's';
    if (seconds < 86400) return Math.floor(seconds / 3600) + 'h' + Math.floor((seconds % 3600) / 60) + 'm';
    return Math.floor(seconds / 86400) + 'd' + Math.floor((seconds % 86400) / 3600) + 'h';
}
function escapeHtml(s) {
    if (s === null || s === undefined) return '';
    return String(s).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'})[c]);
}
function escapeAttr(s) { return escapeHtml(s).replace(/'/g, '&#39;'); }

// ---- 启动 ----
connectSSE();
loadSystem();
loadTopics();
loadClients();
loadUsers().then(populateBatchSelect);
loadPermissions();
// 兜底: 每 10s 拉一次全量 (SSE 只推指标, 不推列表)
setInterval(() => {
    if (document.querySelector('.tab[data-tab]') === null) return;
    loadTopics(); loadClients();
    loadUsers().then(populateBatchSelect);
    loadPermissions();
}, 10000);
</script>
</body>
</html>
"""
