// =====================================================================
//  net.js — 網頁 Client 與 Server 的 WebSocket 連線層
//  * 連到 ws://<網頁所在主機>:8765 (網頁從哪台電腦下載，就連哪台的 Server)
//  * request(action, data) 送出 JSON 請求，回傳 Promise，等 Server 回應
//  * Server 主動推播 (event) 交給 onEvent 處理
//  * 自動重連、連線狀態燈號、封包紀錄面板 (Demo 用)
// =====================================================================

const Net = (() => {
    const WS_PORT = 8765;
    const TIMEOUT_MS = 5000;
    const host = location.hostname || '127.0.0.1';
    const url = `ws://${host}:${WS_PORT}`;

    let ws = null;
    let reqId = 0;
    let retry = 0;
    let everConnected = false;
    const pending = new Map();
    const listeners = {};
    const logLines = [];

    function on(event, fn) { (listeners[event] = listeners[event] || []).push(fn); }
    function emit(event, data) { (listeners[event] || []).forEach(fn => { try { fn(data); } catch (e) { console.error(e); } }); }

    function connect() {
        setStatus('connecting');
        addLog('sys', `嘗試連線 ${url} ...`);
        try { ws = new WebSocket(url); } catch (e) { scheduleReconnect(); return; }

        ws.onopen = () => {
            retry = 0; everConnected = true;
            setStatus('online');
            addLog('sys', `WebSocket 連線成功 (${url})`);
            emit('open');
        };
        ws.onmessage = (ev) => {
            let msg;
            try { msg = JSON.parse(ev.data); } catch { return; }
            addLog('in', msg.type === 'event' ? `event:${msg.event}` : `${msg.ok ? 'OK' : 'FAIL ' + msg.code} ${msg.action || ''}`, msg);
            if (msg.type === 'response' && pending.has(msg.id)) {
                const p = pending.get(msg.id);
                clearTimeout(p.timer); pending.delete(msg.id);
                p.resolve(msg);
            } else if (msg.type === 'event') {
                emit(msg.event, msg.data);
            }
        };
        ws.onclose = () => {
            addLog('sys', '連線中斷');
            for (const [id, p] of pending) { clearTimeout(p.timer); p.resolve({ ok: false, code: 'DISCONNECTED', error: '與伺服器的連線中斷' }); }
            pending.clear();
            emit('close');
            scheduleReconnect();
        };
        ws.onerror = () => { /* onclose 會接著觸發 */ };
    }

    function scheduleReconnect() {
        retry++;
        const wait = Math.min(1000 * 2 ** (retry - 1), 8000);   // 1s, 2s, 4s, 8s...
        setStatus('offline', retry, wait);
        setTimeout(connect, wait);
    }

    function isOnline() { return ws && ws.readyState === WebSocket.OPEN; }

    function request(action, data = {}) {
        return new Promise(resolve => {
            if (!isOnline()) {
                resolve({ ok: false, code: 'DISCONNECTED', error: '目前無法連線到伺服器，請稍後再試' });
                return;
            }
            const id = ++reqId;
            const msg = { id, action, data };
            const timer = setTimeout(() => {
                pending.delete(id);
                resolve({ ok: false, code: 'TIMEOUT', error: '伺服器沒有回應 (逾時 5 秒)' });
            }, TIMEOUT_MS);
            pending.set(id, { resolve, timer });
            ws.send(JSON.stringify(msg));
            const shown = JSON.parse(JSON.stringify(msg));
            if (shown.data) { if (shown.data.password) shown.data.password = '***'; if (shown.data.token) shown.data.token = '***'; }
            addLog('out', action, shown);
        });
    }

    // ---------------- UI：連線燈號 + 封包紀錄 ----------------
    function ensureUI() {
        if (document.getElementById('netBadge')) return;
        const badge = document.createElement('div');
        badge.id = 'netBadge';
        badge.className = 'net-badge';
        badge.title = '點擊開啟封包紀錄';
        badge.onclick = () => document.getElementById('netLog').classList.toggle('open');
        document.body.appendChild(badge);

        const panel = document.createElement('div');
        panel.id = 'netLog';
        panel.className = 'net-log';
        panel.innerHTML = `<div class="net-log-head"><span>📡 封包紀錄 (Client ⇄ Server)</span>
            <span><button onclick="Net.clearLog()">清除</button><button onclick="document.getElementById('netLog').classList.remove('open')">✕</button></span></div>
            <div id="netLogBody" class="net-log-body"></div>`;
        document.body.appendChild(panel);
    }

    function setStatus(state, n, wait) {
        ensureUI();
        const b = document.getElementById('netBadge');
        b.dataset.state = state;
        if (state === 'online') b.textContent = `● 已連線 ${host}:${WS_PORT}`;
        else if (state === 'connecting') b.textContent = `◌ 連線中 ${host}:${WS_PORT}...`;
        else b.textContent = `✕ ${everConnected ? '連線中斷' : '無法連線'}，${Math.round(wait / 1000)} 秒後重試 (第 ${n} 次)`;
        emit('status', state);
    }

    function time() { const d = new Date(); return d.toTimeString().slice(0, 8) + '.' + String(d.getMilliseconds()).padStart(3, '0'); }

    function addLog(dir, title, obj) {
        ensureUI();
        const arrow = dir === 'out' ? '→ 送出' : dir === 'in' ? '← 收到' : '•';
        const body = obj ? JSON.stringify(obj) : '';
        logLines.push({ dir, text: `[${time()}] ${arrow} ${title}`, body: body.length > 300 ? body.slice(0, 300) + '…' : body });
        if (logLines.length > 200) logLines.shift();
        const el = document.getElementById('netLogBody');
        if (!el) return;
        el.innerHTML = logLines.map(l => `<div class="net-line ${l.dir}"><div>${escapeHtml(l.text)}</div>${l.body ? `<code>${escapeHtml(l.body)}</code>` : ''}</div>`).join('');
        el.scrollTop = el.scrollHeight;
    }
    function escapeHtml(s) { return s.replace(/[&<>"]/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c])); }
    function clearLog() { logLines.length = 0; addLog('sys', '紀錄已清除'); }

    return { connect, request, on, isOnline, clearLog, url };
})();
