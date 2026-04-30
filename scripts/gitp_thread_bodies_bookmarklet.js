/**
 * GitP Thread Body Capture Bookmarklet
 * (Stage 7b of community_reception, Apr 30, 2026)
 *
 * Same-origin thread body scraper for forums.giantitp.com (vBulletin),
 * which Cloudflare-blocks Stage 7a-ii Playwright. Phil opens any GitP
 * thread (already past Cloudflare), clicks the bookmarklet, the
 * bookmarklet fetches the pending-URL list from the bouncer, then
 * iterates: same-origin fetch() each URL (uses Phil's CF clearance
 * cookie + session), DOMParser → extract OP + first 20 replies, POST
 * each result to the bouncer's /forum/ingest-thread-body endpoint.
 *
 * Ethics: same as DDB / AO3 / Amazon bookmarklets — human-clicked
 * tool that runs while a human's authenticated browser is open. Not
 * automation pretending to be human; a human with help.
 *
 * Selectors (vBulletin 4 on GitP):
 *   - Post body: div[id^="post_message_"] — the container IS the body
 *   - Order in DOM: top-to-bottom matches OP → replies
 *   - Pagination: vBulletin threads paginate at 25 posts/page; first
 *     page gives us OP + 24 replies, plenty for our 20-reply cap
 *
 * Bouncer routes used:
 *   GET  /system/forum/url-list?forum=forums.giantitp.com
 *   POST /system/forum/ingest-thread-body
 */

(async function () {
  const BOUNCER = 'https://us-central1-dnd-trends-index.cloudfunctions.net/bouncer-api';
  const KEY = 'ArcaneLibrarian2026';
  const TARGET_FORUM = 'forums.giantitp.com';
  const MAX_REPLIES = 20;
  const MAX_CHARS_PER_POST = 2000;
  const FETCH_DELAY_MS = 2500;     // pacing per request
  const FETCH_TIMEOUT_MS = 30000;  // generous — vB pages can be slow
  const SENT_LOG_KEY = 'arcane_gitp_thread_sent_log';

  // ── Sanity: must be on GitP ───────────────────────────────────────────
  if (!location.hostname.endsWith(TARGET_FORUM)) {
    alert(`Open any ${TARGET_FORUM} page first (the bookmarklet uses your authenticated session via fetch).`);
    return;
  }

  // ── Reset panel on re-click ──────────────────────────────────────────
  const existing = document.getElementById('__gitp_thread_panel__');
  if (existing) existing.remove();

  const panel = document.createElement('div');
  panel.id = '__gitp_thread_panel__';
  Object.assign(panel.style, {
    position: 'fixed', top: '12px', right: '12px', zIndex: '999999',
    background: '#0f0f1e', color: '#e0e0ff',
    padding: '14px 16px', borderRadius: '12px',
    fontFamily: '-apple-system, BlinkMacSystemFont, sans-serif',
    fontSize: '13px', lineHeight: '1.4',
    width: '460px', maxHeight: '88vh', overflowY: 'auto',
    boxShadow: '0 8px 32px rgba(0,0,0,0.6)',
    border: '1px solid #2a2a4a',
  });
  panel.innerHTML = `
    <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:8px;">
      <div style="font-weight:600;font-size:14px;color:#e87722;">⚔️ GitP Thread Body Capture</div>
      <button id="__gitp_close_btn" style="background:none;border:none;color:#888;font-size:18px;cursor:pointer;line-height:1;">×</button>
    </div>
    <div id="__gitp_panel_body" style="display:flex;flex-direction:column;gap:10px;"></div>
  `;
  document.body.appendChild(panel);
  document.getElementById('__gitp_close_btn').onclick = () => {
    if (window.__gitp_bulk_running__) {
      if (!confirm('A bulk capture is in progress. Close anyway? (Captures already saved are kept.)')) return;
      window.__gitp_bulk_abort__ = true;
    }
    panel.remove();
  };

  const body = document.getElementById('__gitp_panel_body');
  const set = (html) => { body.innerHTML = html; };
  const status = (msg, color = '#e0e0ff') => {
    let el = document.getElementById('__gitp_status');
    if (!el) {
      el = document.createElement('div');
      el.id = '__gitp_status';
      body.prepend(el);
    }
    el.style.cssText = `padding:6px 8px;border-radius:6px;background:#1a1a2e;color:${color};font-size:12px;`;
    el.textContent = msg;
  };

  function escapeHtml(s) {
    return String(s || '').replace(/&/g, '&amp;').replace(/</g, '&lt;')
      .replace(/>/g, '&gt;').replace(/"/g, '&quot;').replace(/'/g, '&#39;');
  }

  // ── Sent log helpers (cross-session memory) ──────────────────────────
  function loadSentLog() {
    try { return JSON.parse(localStorage.getItem(SENT_LOG_KEY) || '[]'); }
    catch (_) { return []; }
  }
  function appendSentLog(entry) {
    const log = loadSentLog();
    log.unshift(entry);
    while (log.length > 500) log.pop();
    localStorage.setItem(SENT_LOG_KEY, JSON.stringify(log));
  }

  // ── Fetch URL list from bouncer ──────────────────────────────────────
  status('Loading pending-URL list from bouncer…');
  let urlList;
  try {
    const resp = await fetch(
      `${BOUNCER}/system/forum/url-list?forum=${encodeURIComponent(TARGET_FORUM)}`,
      { method: 'GET', headers: { 'X-Ritual-Key': KEY } }
    );
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    urlList = await resp.json();
  } catch (e) {
    status(`⚠️ Failed to load URL list: ${e.message}`, '#ff8888');
    return;
  }

  const pendingUrls = urlList.urls || [];
  if (pendingUrls.length === 0) {
    set(`
      <div style="background:#1a1a2e;padding:10px;border-radius:6px;">
        <div style="font-size:13px;font-weight:600;color:#5fdc7c;margin-bottom:6px;">
          ✅ No pending URLs
        </div>
        <div style="font-size:12px;color:#aaa;">
          All ${TARGET_FORUM} URLs already body-scraped.
        </div>
      </div>
    `);
    return;
  }

  // ── DOM extractor (vBulletin) ────────────────────────────────────────
  function normalizeText(text) {
    if (!text) return '';
    return text.replace(/\s+/g, ' ').trim().slice(0, MAX_CHARS_PER_POST);
  }

  function extractFromDoc(doc) {
    // vBulletin 4 — post bodies live in div[id^="post_message_"].
    // The container itself is the prose; we don't want any sibling
    // metadata (signatures, edit notices). Order in DOM = OP first,
    // then replies top-to-bottom.
    const containers = doc.querySelectorAll('div[id^="post_message_"]');
    if (!containers || containers.length === 0) {
      // Fallback selectors for vBulletin variants
      const fallback = doc.querySelectorAll('.postcontent, .post_content');
      if (!fallback || fallback.length === 0) return { op: '', replies: [] };
      return extractFromNodes(Array.from(fallback));
    }
    return extractFromNodes(Array.from(containers));
  }

  function extractFromNodes(nodes) {
    const texts = nodes.slice(0, MAX_REPLIES + 1)
      .map((n) => normalizeText(n.textContent))
      .filter((t) => t.length > 0);
    if (texts.length === 0) return { op: '', replies: [] };
    return { op: texts[0], replies: texts.slice(1, MAX_REPLIES + 1) };
  }

  function looksLikeCloudflareInterstitial(html) {
    return /Just a moment|cf-browser-verification|cf-challenge-running/i.test(html.slice(0, 2000));
  }

  // ── captureOne: fetch + parse + POST for one (ip_name, url) ──────────
  async function captureOne(ipName, url) {
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort('timeout'), FETCH_TIMEOUT_MS);
    let html, scrapeStatus = 'success', errorMessage = '';
    try {
      const resp = await fetch(url, {
        method: 'GET',
        credentials: 'include',
        headers: { 'Accept': 'text/html' },
        signal: ctrl.signal,
      });
      if (!resp.ok) {
        scrapeStatus = resp.status === 404 ? 'not_found'
          : resp.status === 403 ? 'cloudflare_blocked'
          : resp.status === 429 ? 'rate_limited'
          : 'other_error';
        errorMessage = `HTTP ${resp.status}`;
        html = '';
      } else {
        html = await resp.text();
        if (looksLikeCloudflareInterstitial(html)) {
          scrapeStatus = 'cloudflare_blocked';
          errorMessage = 'Cloudflare interstitial in body';
          html = '';
        }
      }
    } catch (e) {
      const isTimeout = e && (e.name === 'AbortError' || String(e).toLowerCase().includes('abort'));
      scrapeStatus = isTimeout ? 'other_error' : 'other_error';
      errorMessage = isTimeout ? 'fetch timeout' : (e.message || String(e));
      html = '';
    } finally {
      clearTimeout(timer);
    }

    let opText = '', replies = [];
    if (html) {
      const doc = new DOMParser().parseFromString(html, 'text/html');
      const out = extractFromDoc(doc);
      opText = out.op;
      replies = out.replies;
      if (!opText && scrapeStatus === 'success') {
        scrapeStatus = 'other_error';
        errorMessage = 'selector miss (no OP found)';
      }
    }

    const payload = {
      ip_name: ipName,
      url,
      forum_domain: TARGET_FORUM,
      op_text: opText,
      replies_text_combined: replies.join('\n\n---\n\n'),
      reply_count: replies.length,
      scrape_status: scrapeStatus,
      error_message: errorMessage,
    };

    const bouncerResp = await fetch(`${BOUNCER}/system/forum/ingest-thread-body`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-Ritual-Key': KEY },
      body: JSON.stringify(payload),
    });
    const data = await bouncerResp.json();
    if (!bouncerResp.ok || !data.inserted) {
      throw new Error(`bouncer: ${data.error || bouncerResp.status}`);
    }
    return { scrapeStatus, replyCount: replies.length, opChars: opText.length };
  }

  // ── Plan screen ──────────────────────────────────────────────────────
  function renderEntryScreen() {
    delete window.__gitp_bulk_running__;
    delete window.__gitp_bulk_abort__;

    const total = pendingUrls.length;
    const estTimeSec = Math.ceil(total * FETCH_DELAY_MS / 1000) + Math.ceil(total * 1.0);
    const estMin = Math.floor(estTimeSec / 60);
    const estS = estTimeSec % 60;

    set(`
      <div style="background:#1a1a2e;padding:10px;border-radius:6px;">
        <div style="font-size:13px;font-weight:600;color:#e87722;margin-bottom:6px;">
          ⚔️ ${TARGET_FORUM} bulk capture
        </div>
        <div style="font-size:12px;color:#aaa;line-height:1.5;">
          <b style="color:#e0e0ff;">${total}</b> pending URLs (Stage 7a-ii Playwright couldn't reach — Cloudflare).<br/>
          Same-origin fetch from your authenticated session passes Cloudflare transparently.<br/>
          Estimated time: ~${estMin}m ${estS}s
        </div>
        <div style="display:flex;gap:6px;margin-top:10px;">
          <button id="__gitp_run_btn" style="flex:1;background:#e87722;color:#fff;border:none;padding:9px;border-radius:5px;cursor:pointer;font-weight:600;font-size:12px;">
            ▶ Capture all ${total}
          </button>
        </div>
      </div>
      ${renderSentLog()}
    `);
    document.getElementById('__gitp_run_btn').onclick = () => runBulk(pendingUrls);
  }

  // ── Bulk runner ──────────────────────────────────────────────────────
  async function runBulk(tasks) {
    window.__gitp_bulk_running__ = true;
    delete window.__gitp_bulk_abort__;

    const results = { success: 0, cf_blocked: 0, not_found: 0, other_error: 0, posted_failed: 0 };
    const startTs = Date.now();
    const recent = [];

    function renderProgress() {
      const i = results.success + results.cf_blocked + results.not_found
              + results.other_error + results.posted_failed;
      const pct = tasks.length > 0 ? Math.round((i / tasks.length) * 100) : 0;
      const elapsedMs = Date.now() - startTs;
      const rate = i > 0 ? elapsedMs / i : 0;
      const remainingMs = (tasks.length - i) * rate;
      const etaSec = Math.ceil(remainingMs / 1000);
      const etaMin = Math.floor(etaSec / 60);
      const etaS = etaSec % 60;
      const recentHtml = recent.slice(-12).reverse().map((r) => {
        const icon = r.outcome === 'success' ? '✓'
                  : r.outcome === 'cloudflare_blocked' ? '☁'
                  : r.outcome === 'not_found' ? '?'
                  : '✗';
        const color = r.outcome === 'success' ? '#5fdc7c'
                   : r.outcome === 'cloudflare_blocked' ? '#d9a64a'
                   : '#ff8888';
        return `<div style="font-size:11px;padding:1px 0;color:${color};">${icon} <span style="color:#ccc;">${escapeHtml(r.ip_name)}</span> <span style="color:#666;">${escapeHtml(r.url_short)}</span> <span style="color:#888;">— ${escapeHtml(r.note)}</span></div>`;
      }).join('');
      set(`
        <div style="background:#1a1a2e;padding:10px;border-radius:6px;">
          <div style="font-size:13px;font-weight:600;color:#e87722;margin-bottom:6px;">⚔️ Capturing ${i} / ${tasks.length} (${pct}%)</div>
          <div style="background:#0a0a18;height:8px;border-radius:4px;overflow:hidden;">
            <span style="display:block;width:${pct}%;height:100%;background:#e87722;transition:width 0.2s;"></span>
          </div>
          <div style="font-size:11px;color:#aaa;margin-top:6px;display:flex;gap:12px;flex-wrap:wrap;">
            <span><b style="color:#5fdc7c;">${results.success}</b> ok</span>
            <span><b style="color:#d9a64a;">${results.cf_blocked}</b> cf</span>
            <span><b style="color:#888;">${results.not_found}</b> 404</span>
            <span><b style="color:#ff8888;">${results.other_error + results.posted_failed}</b> err</span>
            <span style="margin-left:auto;color:#888;">ETA ${etaMin}m ${etaS}s</span>
          </div>
        </div>
        <div style="background:#0a0a18;border:1px solid #2a2a4a;border-radius:6px;padding:8px;max-height:280px;overflow-y:auto;">
          <div style="font-size:11px;color:#888;margin-bottom:4px;">Live feed (most recent first):</div>
          ${recentHtml || '<i style="color:#666;font-size:11px;">starting…</i>'}
        </div>
        <button id="__gitp_abort_btn" style="background:#1a1a2e;color:#d9a64a;border:1px solid #2a2a4a;padding:7px;border-radius:4px;cursor:pointer;font-size:12px;width:100%;">
          ⏸ Pause / abort
        </button>
      `);
      document.getElementById('__gitp_abort_btn').onclick = () => {
        if (confirm('Abort the bulk capture? Already-saved captures are kept; remaining will not run.')) {
          window.__gitp_bulk_abort__ = true;
        }
      };
    }
    renderProgress();

    for (let i = 0; i < tasks.length; i++) {
      if (window.__gitp_bulk_abort__) break;
      const task = tasks[i];
      let outcome = 'other_error', note = '?';
      const url_short = (task.url || '').replace(/^https?:\/\/(?:forums\.)?giantitp\.com\//, '');
      try {
        const r = await captureOne(task.ip_name, task.url);
        outcome = r.scrapeStatus;
        if (r.scrapeStatus === 'success') {
          note = `${r.replyCount} replies, ${r.opChars} OP chars`;
          appendSentLog({
            ip_name: task.ip_name, url: task.url,
            reply_count: r.replyCount, op_chars: r.opChars,
            ts: new Date().toISOString(),
          });
        } else {
          note = outcome;
        }
      } catch (e) {
        outcome = 'posted_failed';
        note = (e.message || String(e)).slice(0, 80);
      }
      results[outcome] = (results[outcome] || 0) + 1;
      recent.push({ ip_name: task.ip_name, url_short, outcome, note });
      renderProgress();
      if (i < tasks.length - 1 && !window.__gitp_bulk_abort__) {
        await new Promise((r) => setTimeout(r, FETCH_DELAY_MS));
      }
    }

    window.__gitp_bulk_running__ = false;
    const elapsedSec = Math.floor((Date.now() - startTs) / 1000);
    const elapsedMin = Math.floor(elapsedSec / 60);
    const elapsedS = elapsedSec % 60;
    const aborted = !!window.__gitp_bulk_abort__;
    set(`
      <div style="background:#1a1a2e;padding:10px;border-radius:6px;">
        <div style="font-size:13px;font-weight:600;color:${aborted ? '#d9a64a' : '#5fdc7c'};margin-bottom:6px;">
          ${aborted ? '⏸ Capture paused' : '✅ Bulk capture complete'}
        </div>
        <div style="font-size:12px;color:#aaa;line-height:1.6;">
          <b style="color:#5fdc7c;">${results.success}</b> success
          · <b style="color:#d9a64a;">${results.cf_blocked}</b> cf-blocked
          · <b style="color:#888;">${results.not_found}</b> not-found
          · <b style="color:#ff8888;">${results.other_error + results.posted_failed}</b> errors<br/>
          Elapsed: ${elapsedMin}m ${elapsedS}s
        </div>
      </div>
      <button id="__gitp_back_btn" style="background:#e87722;color:#fff;border:none;padding:9px;border-radius:5px;cursor:pointer;font-weight:600;font-size:12px;">
        ↻ Refresh plan (re-fetch pending list)
      </button>
      <details open style="background:#0a0a18;border:1px solid #2a2a4a;border-radius:6px;padding:8px;">
        <summary style="cursor:pointer;font-size:11px;color:#888;">Full feed (${recent.length})</summary>
        <div style="margin-top:6px;max-height:300px;overflow-y:auto;">
          ${recent.slice().reverse().map((r) => {
            const icon = r.outcome === 'success' ? '✓'
                      : r.outcome === 'cloudflare_blocked' ? '☁'
                      : r.outcome === 'not_found' ? '?'
                      : '✗';
            const color = r.outcome === 'success' ? '#5fdc7c'
                       : r.outcome === 'cloudflare_blocked' ? '#d9a64a'
                       : '#ff8888';
            return `<div style="font-size:11px;padding:1px 0;color:${color};">${icon} <span style="color:#ccc;">${escapeHtml(r.ip_name)}</span> <span style="color:#666;">${escapeHtml(r.url_short)}</span> <span style="color:#888;">— ${escapeHtml(r.note)}</span></div>`;
          }).join('')}
        </div>
      </details>
    `);
    document.getElementById('__gitp_back_btn').onclick = () => {
      panel.remove();
      alert('Click the bookmarklet again to refresh the plan with the updated pending list.');
    };
  }

  // ── Sent log preview ─────────────────────────────────────────────────
  function renderSentLog() {
    const log = loadSentLog().slice(0, 8);
    if (!log.length) return '';
    const items = log.map((e) => {
      const url_short = (e.url || '').replace(/^https?:\/\/(?:forums\.)?giantitp\.com\//, '').slice(0, 50);
      return `<div style="font-size:11px;color:#888;padding:1px 0;">✓ ${escapeHtml(e.ip_name)} <span style="color:#666;">${escapeHtml(url_short)}</span> <span style="color:#5fdc7c;">(${e.reply_count} replies)</span></div>`;
    }).join('');
    return `
      <details style="border-top:1px solid #2a2a4a;padding-top:6px;">
        <summary style="cursor:pointer;color:#888;font-size:11px;">Recent sends (this browser, last ${log.length})</summary>
        <div style="margin-top:4px;">${items}</div>
      </details>
    `;
  }

  // ── Initial render ───────────────────────────────────────────────────
  renderEntryScreen();
})();
