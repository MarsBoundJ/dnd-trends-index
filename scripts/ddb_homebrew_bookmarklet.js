/**
 * D&D Beyond Homebrew Capture Bookmarklet
 * (Stage 6a of the community_reception multi-source composite, Apr 29, 2026)
 *
 * BULK MODE (default) — land on any dndbeyond.com page, click once,
 * bookmarklet sequentially fetches `/homebrew/<section>?filter-name=<IP>`
 * for all 40 priority IPs × 5 priority sections = 200 captures, parses
 * the filtered HTML response with DOMParser, POSTs each result to
 * /system/homebrew/ingest-ddb on the bouncer. Progress UI shows
 * live status. Skips already-captured combinations by default. ~5
 * minutes per full run.
 *
 * MANUAL MODE (fallback) — pick a single IP from a searchable dropdown
 * grouped by cohort with per-IP per-section progress bars. Bookmarklet
 * fills DDB's filter input + submits the form. Re-click on the filtered
 * page to capture. For ad-hoc one-offs or if bulk mode hits a problem.
 *
 * Selectors learned from Phil's F12 scout on Apr 29:
 *   - Filter URL pattern: /homebrew/<section>?filter-name=<IP>&filter-sort=adds-desc
 *   - Row container:    .list-row[data-slug] (with data-type)
 *   - Name link:        .list-row-name-primary-text a
 *   - Adds count:       .list-row-col-adds .list-row-primary-text
 *   - Views count:      .list-row-col-views .list-row-primary-text
 *   - Comments count:   .list-row-col-comments .list-row-primary-text
 *   - Rating points:    .list-row-rating-points .positive
 *   - Rating up/down:   .list-row-rating-counts .positive / .negative
 *   - Base class:       .list-row-col-baseclass .list-row-primary-text
 *   - Author:           .list-row-author-primary-text
 *   - Pagination:       .b-pagination — last page number is the page count
 *
 * Ethics: same-origin fetches with Phil's session cookies. Same logical
 * pattern as the Amazon bookmarklet (which fetches Amazon's own list
 * pages with the user's authenticated session). Not an automated bot
 * — a human-clicked tool that runs while a human's browser is open.
 */

(async function () {
  const BOUNCER = 'https://us-central1-dnd-trends-index.cloudfunctions.net/bouncer-api';
  const KEY = 'ArcaneLibrarian2026';

  const COHORT_LABELS = {
    marquee:   '⭐ Marquee positives',
    asymmetry: '⚖️  Asymmetry / negative',
    canary:    '🐦 Disambiguation canaries',
    active:    '🔥 Active-crossover tier',
    roundout:  '➕ Round-out',
  };
  const COHORT_COLORS = {
    marquee:   '#5fdc7c',
    asymmetry: '#d9a64a',
    canary:    '#7aa3ff',
    active:    '#e87722',
    roundout:  '#a3a3a3',
  };

  const SENT_LOG_KEY  = 'arcane_ddb_homebrew_sent_log';
  const IP_LIST_KEY   = 'arcane_ddb_homebrew_ip_list';
  const IP_LIST_TTL_MS = 24 * 60 * 60 * 1000; // 24h
  const FETCH_DELAY_MS = 2000;                 // pacing between DDB fetches
  const FETCH_TIMEOUT_MS = 45000;              // some /magic-items queries take 30s+
  const FETCH_RETRY_BACKOFF_MS = 5000;         // wait before single retry on timeout

  // Per-section filter-param map. DDB has two listing-component generations:
  //   - Newer 5e-2024 character-creation sections use `filter-name` (subclasses,
  //     species, feats, backgrounds).
  //   - Older sections use `filter-search` (spells, monsters, magic-items).
  // Empirically verified Apr 29, 2026 via console probe across all 7 valid
  // homebrew section paths. /races (legacy name) and /classes both 404 — DDB
  // renamed races to species in 5e 2024 and never hosted full classes.
  const SECTION_FILTER_PARAM = {
    'subclasses':  'filter-name',
    'species':     'filter-name',
    'feats':       'filter-name',
    'backgrounds': 'filter-name',
    'spells':      'filter-search',
    'monsters':    'filter-search',
    'magic-items': 'filter-search',
  };

  // ── Sanity: must be on D&D Beyond ─────────────────────────────────────────
  if (!location.hostname.endsWith('dndbeyond.com')) {
    alert('Open any dndbeyond.com page first (the bookmarklet uses your DDB session cookies via fetch).');
    return;
  }

  // ── Reset panel on re-click ───────────────────────────────────────────────
  const existing = document.getElementById('__ddb_homebrew_panel__');
  if (existing) existing.remove();

  const panel = document.createElement('div');
  panel.id = '__ddb_homebrew_panel__';
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
      <div style="font-weight:600;font-size:14px;color:#e87722;">📜 DDB Homebrew Capture</div>
      <button id="__ddb_close_btn" style="background:none;border:none;color:#888;font-size:18px;cursor:pointer;line-height:1;">×</button>
    </div>
    <div id="__ddb_panel_body" style="display:flex;flex-direction:column;gap:10px;"></div>
  `;
  document.body.appendChild(panel);
  document.getElementById('__ddb_close_btn').onclick = () => {
    if (window.__ddb_bulk_running__) {
      if (!confirm('A bulk capture is in progress. Close anyway? (Captures already saved are kept.)')) return;
      window.__ddb_bulk_abort__ = true;
    }
    panel.remove();
  };

  const body = document.getElementById('__ddb_panel_body');
  const set    = (html) => { body.innerHTML = html; };
  const status = (msg, color = '#e0e0ff') => {
    let el = document.getElementById('__ddb_status');
    if (!el) {
      el = document.createElement('div');
      el.id = '__ddb_status';
      body.prepend(el);
    }
    el.style.cssText = `padding:6px 8px;border-radius:6px;background:#1a1a2e;color:${color};font-size:12px;`;
    el.textContent = msg;
  };

  status('Loading priority list…');

  // ── Fetch IP list (cached) ────────────────────────────────────────────────
  let ipPayload = null;
  try {
    const cached = localStorage.getItem(IP_LIST_KEY);
    if (cached) {
      const parsed = JSON.parse(cached);
      if (Date.now() - parsed.fetched_at < IP_LIST_TTL_MS) ipPayload = parsed.payload;
    }
  } catch (_) {}
  if (!ipPayload) {
    try {
      const resp = await fetch(`${BOUNCER}/system/homebrew/ip-list`, {
        method: 'GET', headers: { 'X-Ritual-Key': KEY },
      });
      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
      ipPayload = await resp.json();
      localStorage.setItem(IP_LIST_KEY, JSON.stringify({
        fetched_at: Date.now(), payload: ipPayload,
      }));
    } catch (e) {
      status(`⚠️ Failed to load IP list: ${e.message}`, '#ff8888');
      return;
    }
  }

  const PRIORITY_IPS      = ipPayload.ips;
  const PRIORITY_SECTIONS = ipPayload.priority_sections;
  const ALL_SECTIONS      = ipPayload.all_sections;

  // ── Sent log helpers ──────────────────────────────────────────────────────
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

  // Per-IP-section completion derived from BOTH server-side timestamps
  // (returned by /system/homebrew/ip-list) AND the local sent log.
  function progressFor(ipName) {
    const log = loadSentLog();
    const fromLog = new Set(log.filter((e) => e.ip_name === ipName).map((e) => e.section));
    const ipRow = PRIORITY_IPS.find((x) => x.ip_name === ipName);
    const fromServer = ipRow ? Object.entries(ipRow.sections || {}).filter(([_, ts]) => !!ts).map(([s]) => s) : [];
    const all = new Set([...fromLog, ...fromServer]);
    const priorityDone = PRIORITY_SECTIONS.filter((s) => all.has(s));
    return { priorityDone: priorityDone.length, priorityTotal: PRIORITY_SECTIONS.length, allDone: Array.from(all) };
  }

  function isAlreadyCaptured(ipName, section) {
    const log = loadSentLog();
    if (log.some((e) => e.ip_name === ipName && e.section === section)) return true;
    const ipRow = PRIORITY_IPS.find((x) => x.ip_name === ipName);
    return !!(ipRow && ipRow.sections && ipRow.sections[section]);
  }

  // ── HTML escape ───────────────────────────────────────────────────────────
  function escapeHtml(s) {
    return String(s || '').replace(/&/g, '&amp;').replace(/</g, '&lt;')
      .replace(/>/g, '&gt;').replace(/"/g, '&quot;').replace(/'/g, '&#39;');
  }
  const escapeAttr = escapeHtml;

  // ── DOM parsers (used by both modes) ──────────────────────────────────────
  function parseRowsFromDoc(doc) {
    const rowEls = doc.querySelectorAll('.list-row[data-slug]');
    return Array.from(rowEls).map((row) => {
      const text = (sel) => {
        const el = row.querySelector(sel);
        return el ? el.textContent.trim() : '';
      };
      const num = (sel) => {
        const v = text(sel).replace(/,/g, '');
        const n = parseInt(v, 10);
        return Number.isFinite(n) ? n : 0;
      };
      const link = row.querySelector('.list-row-name-primary-text a');
      return {
        name:             link ? link.textContent.trim() : text('.list-row-name-primary-text'),
        slug:             row.getAttribute('data-slug') || '',
        url:              link ? link.getAttribute('href') : '',
        adds:             num('.list-row-col-adds .list-row-primary-text'),
        views:            num('.list-row-col-views .list-row-primary-text'),
        comments:         num('.list-row-col-comments .list-row-primary-text'),
        rating_points:    num('.list-row-rating-points .list-row-rating-primary-text.positive'),
        rating_positive:  num('.list-row-rating-counts .list-row-rating-secondary-text.positive'),
        rating_negative:  num('.list-row-rating-counts .list-row-rating-secondary-text.negative'),
        base_class:       text('.list-row-col-baseclass .list-row-primary-text'),
        author:           text('.list-row-author-primary-text'),
      };
    });
  }
  function parsePagesFromDoc(doc) {
    const pag = doc.querySelector('.b-pagination');
    if (!pag) return 1;
    const items = Array.from(pag.querySelectorAll('*'))
      .map((el) => el.textContent.trim())
      .filter((t) => /^\d+$/.test(t))
      .map((t) => parseInt(t, 10));
    return items.length ? Math.max(...items) : 1;
  }

  // ── Bulk-mode entry: plan screen ──────────────────────────────────────────
  function renderEntryScreen() {
    delete window.__ddb_bulk_running__;
    delete window.__ddb_bulk_abort__;

    // Compute the plan
    const tasks = [];
    for (const ip of PRIORITY_IPS) {
      for (const section of PRIORITY_SECTIONS) {
        tasks.push({
          ip_name: ip.ip_name, section, cohort: ip.cohort, rank: ip.rank,
          alreadyCaptured: isAlreadyCaptured(ip.ip_name, section),
        });
      }
    }
    const total = tasks.length;
    const done = tasks.filter((t) => t.alreadyCaptured).length;
    const pending = total - done;
    const estTimeSec = Math.ceil(pending * FETCH_DELAY_MS / 1000) + Math.ceil(pending * 0.5);
    const estMin = Math.floor(estTimeSec / 60);
    const estS = estTimeSec % 60;

    set(`
      <div style="background:#1a1a2e;padding:10px;border-radius:6px;">
        <div style="font-size:13px;font-weight:600;color:#e87722;margin-bottom:6px;">🚀 Bulk capture priority queue</div>
        <div style="font-size:12px;color:#aaa;line-height:1.5;">
          <b style="color:#e0e0ff;">${total}</b> total captures · <b style="color:#5fdc7c;">${done}</b> already done · <b style="color:#e87722;">${pending}</b> pending<br/>
          40 priority IPs × ${PRIORITY_SECTIONS.length} sections (<i>${PRIORITY_SECTIONS.join(', ')}</i>)<br/>
          Estimated time: ~${estMin}m ${estS}s
        </div>
        <div style="display:flex;gap:6px;margin-top:10px;">
          <button id="__ddb_bulk_pending_btn" style="flex:1;background:#e87722;color:#fff;border:none;padding:9px;border-radius:5px;cursor:pointer;font-weight:600;font-size:12px;" ${pending === 0 ? 'disabled' : ''}>
            ${pending === 0 ? '✅ All done' : `▶ Capture ${pending} pending`}
          </button>
          <button id="__ddb_bulk_all_btn" style="flex:0 0 auto;background:#1a1a2e;color:#aaa;border:1px solid #2a2a4a;padding:9px 12px;border-radius:5px;cursor:pointer;font-size:11px;">
            Force re-do all ${total}
          </button>
        </div>
      </div>

      <details style="background:#0a0a18;border:1px solid #2a2a4a;border-radius:6px;padding:8px 10px;">
        <summary style="cursor:pointer;font-size:11px;color:#888;">▾ Per-cohort progress</summary>
        <div style="margin-top:6px;">${renderCohortProgress(tasks)}</div>
      </details>

      <details style="background:#0a0a18;border:1px solid #2a2a4a;border-radius:6px;padding:8px 10px;">
        <summary style="cursor:pointer;font-size:11px;color:#888;">▸ Or: manual pick mode (single IP)</summary>
        <div style="margin-top:6px;font-size:11px;color:#888;line-height:1.5;">
          Manual mode opens a searchable dropdown. Pick one IP → bookmarklet
          fills DDB's filter input + submits the form. Re-click bookmarklet
          on the filtered page to capture. Use this for ad-hoc one-offs.
        </div>
        <button id="__ddb_manual_btn" style="margin-top:6px;background:#1a1a2e;color:#7aa3ff;border:1px solid #2a2a4a;padding:7px;border-radius:4px;cursor:pointer;font-size:12px;width:100%;">
          Open manual mode
        </button>
      </details>

      ${renderSentLog()}
    `);

    document.getElementById('__ddb_bulk_pending_btn').onclick = () => runBulk(tasks.filter((t) => !t.alreadyCaptured));
    document.getElementById('__ddb_bulk_all_btn').onclick     = () => {
      if (confirm(`Re-capture ALL ${total}? Existing rows in BQ will get duplicate harvested_at entries (gold view picks latest).`)) {
        runBulk(tasks);
      }
    };
    document.getElementById('__ddb_manual_btn').onclick       = () => renderManualPickMode();
  }

  function renderCohortProgress(tasks) {
    const cohorts = ['marquee', 'asymmetry', 'canary', 'active', 'roundout'];
    return cohorts.map((cohort) => {
      const cohortTasks = tasks.filter((t) => t.cohort === cohort);
      if (!cohortTasks.length) return '';
      const done = cohortTasks.filter((t) => t.alreadyCaptured).length;
      const pct = Math.round((done / cohortTasks.length) * 100);
      return `
        <div style="display:flex;align-items:center;gap:8px;font-size:11px;padding:2px 0;">
          <span style="flex:0 0 130px;color:${COHORT_COLORS[cohort]};">${COHORT_LABELS[cohort]}</span>
          <span style="flex:1;background:#1a1a2e;height:6px;border-radius:3px;overflow:hidden;">
            <span style="display:block;width:${pct}%;height:100%;background:${COHORT_COLORS[cohort]};"></span>
          </span>
          <span style="flex:0 0 50px;text-align:right;color:#888;">${done}/${cohortTasks.length}</span>
        </div>
      `;
    }).join('');
  }

  // ── Bulk-mode runner ──────────────────────────────────────────────────────
  async function runBulk(tasks) {
    window.__ddb_bulk_running__ = true;
    delete window.__ddb_bulk_abort__;

    const results = { saved: 0, empty: 0, failed: 0 };
    const startTs = Date.now();
    const recent = [];

    function renderProgress() {
      const i = results.saved + results.empty + results.failed;
      const pct = tasks.length > 0 ? Math.round((i / tasks.length) * 100) : 0;
      const elapsedMs = Date.now() - startTs;
      const rate = i > 0 ? elapsedMs / i : 0;
      const remainingMs = (tasks.length - i) * rate;
      const etaSec = Math.ceil(remainingMs / 1000);
      const etaMin = Math.floor(etaSec / 60);
      const etaS = etaSec % 60;
      const recentHtml = recent.slice(-12).reverse().map((r) => {
        const icon = r.outcome === 'saved' ? '✓' : r.outcome === 'empty' ? '∅' : '✗';
        const color = r.outcome === 'saved' ? '#5fdc7c' : r.outcome === 'empty' ? '#888' : '#ff8888';
        return `<div style="font-size:11px;padding:1px 0;color:${color};">${icon} <span style="color:#ccc;">${escapeHtml(r.ip_name)}</span> <span style="color:#666;">/${escapeHtml(r.section)}</span> <span style="color:#888;">— ${escapeHtml(r.note)}</span></div>`;
      }).join('');
      set(`
        <div style="background:#1a1a2e;padding:10px;border-radius:6px;">
          <div style="font-size:13px;font-weight:600;color:#e87722;margin-bottom:6px;">🚀 Capturing ${i} / ${tasks.length} (${pct}%)</div>
          <div style="background:#0a0a18;height:8px;border-radius:4px;overflow:hidden;">
            <span style="display:block;width:${pct}%;height:100%;background:#e87722;transition:width 0.2s;"></span>
          </div>
          <div style="font-size:11px;color:#aaa;margin-top:6px;display:flex;gap:12px;">
            <span><b style="color:#5fdc7c;">${results.saved}</b> saved</span>
            <span><b style="color:#888;">${results.empty}</b> empty</span>
            <span><b style="color:#ff8888;">${results.failed}</b> failed</span>
            <span style="margin-left:auto;color:#888;">ETA ${etaMin}m ${etaS}s</span>
          </div>
        </div>
        <div style="background:#0a0a18;border:1px solid #2a2a4a;border-radius:6px;padding:8px;max-height:280px;overflow-y:auto;">
          <div style="font-size:11px;color:#888;margin-bottom:4px;">Live feed (most recent first):</div>
          ${recentHtml || '<i style="color:#666;font-size:11px;">starting…</i>'}
        </div>
        <button id="__ddb_abort_btn" style="background:#1a1a2e;color:#d9a64a;border:1px solid #2a2a4a;padding:7px;border-radius:4px;cursor:pointer;font-size:12px;width:100%;">
          ⏸ Pause / abort
        </button>
      `);
      document.getElementById('__ddb_abort_btn').onclick = () => {
        if (confirm('Abort the bulk capture? Already-saved captures are kept; remaining will not run.')) {
          window.__ddb_bulk_abort__ = true;
        }
      };
    }
    renderProgress();

    for (let i = 0; i < tasks.length; i++) {
      if (window.__ddb_bulk_abort__) break;
      const task = tasks[i];
      let outcome = 'failed', note = '?';
      try {
        const result = await captureOne(task.ip_name, task.section);
        if (result.visible_items_count === 0) {
          outcome = 'empty';
          note = 'no rows';
        } else {
          outcome = 'saved';
          note = `${result.visible_items_count} items, top ${result.top_adds}`;
          appendSentLog({
            ip_name: task.ip_name, section: task.section,
            visible_items_count: result.visible_items_count,
            top_adds: result.top_adds,
            ts: new Date().toISOString(),
          });
        }
      } catch (e) {
        outcome = 'failed';
        note = e.message || String(e);
      }
      results[outcome]++;
      recent.push({ ip_name: task.ip_name, section: task.section, outcome, note });
      renderProgress();
      if (i < tasks.length - 1 && !window.__ddb_bulk_abort__) {
        await new Promise((r) => setTimeout(r, FETCH_DELAY_MS));
      }
    }

    window.__ddb_bulk_running__ = false;
    const elapsedSec = Math.floor((Date.now() - startTs) / 1000);
    const elapsedMin = Math.floor(elapsedSec / 60);
    const elapsedS = elapsedSec % 60;
    const aborted = !!window.__ddb_bulk_abort__;
    set(`
      <div style="background:#1a1a2e;padding:10px;border-radius:6px;">
        <div style="font-size:13px;font-weight:600;color:${aborted ? '#d9a64a' : '#5fdc7c'};margin-bottom:6px;">
          ${aborted ? '⏸ Capture paused' : '✅ Bulk capture complete'}
        </div>
        <div style="font-size:12px;color:#aaa;line-height:1.6;">
          <b style="color:#5fdc7c;">${results.saved}</b> saved
          · <b style="color:#888;">${results.empty}</b> empty (no homebrew exists)
          · <b style="color:#ff8888;">${results.failed}</b> failed<br/>
          Elapsed: ${elapsedMin}m ${elapsedS}s
        </div>
      </div>
      <button id="__ddb_back_btn" style="background:#e87722;color:#fff;border:none;padding:9px;border-radius:5px;cursor:pointer;font-weight:600;font-size:12px;">
        ↻ Refresh plan
      </button>
      <details open style="background:#0a0a18;border:1px solid #2a2a4a;border-radius:6px;padding:8px;">
        <summary style="cursor:pointer;font-size:11px;color:#888;">Full feed (${recent.length})</summary>
        <div style="margin-top:6px;max-height:300px;overflow-y:auto;">
          ${recent.slice().reverse().map((r) => {
            const icon = r.outcome === 'saved' ? '✓' : r.outcome === 'empty' ? '∅' : '✗';
            const color = r.outcome === 'saved' ? '#5fdc7c' : r.outcome === 'empty' ? '#888' : '#ff8888';
            return `<div style="font-size:11px;padding:1px 0;color:${color};">${icon} <span style="color:#ccc;">${escapeHtml(r.ip_name)}</span> <span style="color:#666;">/${escapeHtml(r.section)}</span> <span style="color:#888;">— ${escapeHtml(r.note)}</span></div>`;
          }).join('')}
        </div>
      </details>
    `);
    document.getElementById('__ddb_back_btn').onclick = () => {
      // Refresh server-side counts: clear cache, re-fetch
      try { localStorage.removeItem(IP_LIST_KEY); } catch (_) {}
      // Re-run the bookmarklet by removing the panel and re-clicking — simulate
      // a re-click by calling the IIFE's effective entry. Easiest: reload panel.
      panel.remove();
      // Re-enter by triggering a fresh outer IIFE
      const script = document.createElement('script');
      // We can't re-run our own IIFE inline cleanly, so just prompt the user
      alert('Click the bookmarklet again to refresh the plan with updated counts.');
    };
  }

  // captureOne: fetch + parse + POST for one (ip_name, section).
  // Single retry on timeout — DDB's /magic-items endpoint occasionally
  // takes 30s+ and trips the AbortController. Other failures don't retry.
  async function captureOne(ipName, section) {
    const ipQ = encodeURIComponent(ipName);
    const filterParam = SECTION_FILTER_PARAM[section] || 'filter-name';
    const url = `/homebrew/${section}?${filterParam}=${ipQ}&filter-sort=adds-desc`;

    async function fetchOnce() {
      const ctrl = new AbortController();
      const timer = setTimeout(() => ctrl.abort('timeout'), FETCH_TIMEOUT_MS);
      try {
        const resp = await fetch(url, {
          method: 'GET',
          credentials: 'include',
          headers: { 'Accept': 'text/html' },
          signal: ctrl.signal,
        });
        return resp;
      } finally {
        clearTimeout(timer);
      }
    }

    let resp;
    try {
      resp = await fetchOnce();
    } catch (e) {
      const isTimeout = e && (e.name === 'AbortError' || String(e).toLowerCase().includes('abort'));
      if (!isTimeout) throw e;
      // Retry once after backoff
      await new Promise((r) => setTimeout(r, FETCH_RETRY_BACKOFF_MS));
      try {
        resp = await fetchOnce();
      } catch (e2) {
        throw new Error('timeout (retried once)');
      }
    }
    if (!resp.ok) throw new Error(`DDB HTTP ${resp.status}`);
    const html = await resp.text();
    const doc = new DOMParser().parseFromString(html, 'text/html');
    const rows = parseRowsFromDoc(doc);
    const pages = parsePagesFromDoc(doc);
    const topAdds = rows.length ? Math.max(...rows.map((r) => r.adds)) : 0;
    const estTotal = pages * 30;

    const payload = {
      ip_name: ipName,
      ddb_section: section,
      search_query: ipName,
      visible_items_count: rows.length,
      pages_count: pages,
      estimated_total_count: estTotal,
      top_items: rows.slice(0, 30),
      source_url: new URL(url, location.origin).toString(),
      scraped_by: 'ddb_homebrew_bookmarklet_bulk',
    };

    const bouncerResp = await fetch(`${BOUNCER}/system/homebrew/ingest-ddb`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-Ritual-Key': KEY },
      body: JSON.stringify(payload),
    });
    const data = await bouncerResp.json();
    if (!bouncerResp.ok || !data.inserted) {
      throw new Error(`bouncer: ${data.error || bouncerResp.status}`);
    }
    return { visible_items_count: rows.length, top_adds: topAdds, pages };
  }

  // ── Manual mode: dropdown picker (legacy) ─────────────────────────────────
  function findSearchInput() {
    return (
      document.querySelector('input[name="filter-name"]') ||
      document.querySelector('input[name="filter-search"]') ||
      document.querySelector('input[name*="search" i]') ||
      document.querySelector('input[placeholder*="earch" i]:not([type="hidden"])') ||
      document.querySelector('.ddb-listing-filters input[type="text"]') ||
      document.querySelector('form input[type="text"]')
    );
  }

  function renderManualPickMode() {
    const cohorts = ['marquee', 'asymmetry', 'canary', 'active', 'roundout'];
    const groupHtml = cohorts.map((cohort) => {
      const ipsInCohort = PRIORITY_IPS.filter((ip) => ip.cohort === cohort);
      if (!ipsInCohort.length) return '';
      const items = ipsInCohort.map((ip) => {
        const prog = progressFor(ip.ip_name);
        const pct = prog.priorityTotal > 0 ? Math.round((prog.priorityDone / prog.priorityTotal) * 100) : 0;
        const sectionDoneIcon = '';  // section context unknown in manual entry from any page
        const allDoneIcon = pct === 100 ? ' ✅' : '';
        return `<div class="__ddb_pick_row" data-ip="${escapeAttr(ip.ip_name)}" style="padding:4px 6px;cursor:pointer;border-radius:4px;display:flex;align-items:center;justify-content:space-between;gap:8px;font-size:12px;" onmouseover="this.style.background='#1a1a2e'" onmouseout="this.style.background='transparent'">
          <span><span style="color:#e0e0ff;">${escapeHtml(ip.ip_name)}</span>${sectionDoneIcon}${allDoneIcon}</span>
          <span style="display:flex;align-items:center;gap:6px;">
            <span style="font-size:10px;color:#888;">${prog.priorityDone}/${prog.priorityTotal}</span>
            <span style="display:inline-block;width:50px;height:5px;background:#1a1a2e;border-radius:3px;overflow:hidden;">
              <span style="display:block;width:${pct}%;height:100%;background:${COHORT_COLORS[cohort]};"></span>
            </span>
          </span>
        </div>`;
      }).join('');
      return `<div style="margin-bottom:6px;">
        <div style="font-size:10px;color:${COHORT_COLORS[cohort]};font-weight:600;margin-bottom:3px;">${COHORT_LABELS[cohort]}</div>
        ${items}
      </div>`;
    }).join('');

    set(`
      <div style="font-size:11px;color:#888;">Manual mode — pick an IP, bookmarklet fills DDB's filter + submits. Re-click bookmarklet on filtered page to capture.</div>
      <input id="__ddb_search" type="text" placeholder="Type to filter the list…" style="width:100%;background:#1a1a2e;color:#e0e0ff;border:1px solid #2a2a4a;padding:7px;border-radius:4px;font-size:13px;box-sizing:border-box;" />
      <div id="__ddb_pick_list" style="max-height:340px;overflow-y:auto;background:#0a0a18;padding:6px;border-radius:6px;border:1px solid #2a2a4a;">
        ${groupHtml}
      </div>
      <button id="__ddb_back_to_bulk_btn" style="background:#1a1a2e;color:#7aa3ff;border:1px solid #2a2a4a;padding:6px;border-radius:4px;cursor:pointer;font-size:11px;">
        ◂ Back to bulk mode
      </button>
      ${renderSentLog()}
    `);

    document.querySelectorAll('.__ddb_pick_row').forEach((el) => {
      el.onclick = () => onManualPickClick(el.getAttribute('data-ip'));
    });
    const searchEl = document.getElementById('__ddb_search');
    searchEl.oninput = () => {
      const q = searchEl.value.toLowerCase();
      document.querySelectorAll('.__ddb_pick_row').forEach((el) => {
        const name = el.getAttribute('data-ip').toLowerCase();
        el.style.display = !q || name.includes(q) ? '' : 'none';
      });
    };
    searchEl.focus();
    document.getElementById('__ddb_back_to_bulk_btn').onclick = () => renderEntryScreen();
  }

  function onManualPickClick(ipName) {
    // Detect current section (if any) to pick the right filter param
    const currentSection = (location.pathname.split('/').filter(Boolean).pop() || '');
    const sectionFromUrl = SECTION_FILTER_PARAM[currentSection] ? currentSection : null;

    // If not on a homebrew section page, navigate to /subclasses (most-popular section)
    if (!sectionFromUrl) {
      const targetSection = 'subclasses';
      const param = SECTION_FILTER_PARAM[targetSection];
      location.href = `/homebrew/${targetSection}?${param}=${encodeURIComponent(ipName)}&filter-sort=adds-desc`;
      return;
    }

    const filterParam = SECTION_FILTER_PARAM[sectionFromUrl];
    const input = findSearchInput();
    if (!input) {
      // Direct URL navigation fallback (uses the right param for this section)
      const url = new URL(location.href);
      url.searchParams.set(filterParam, ipName);
      url.searchParams.set('filter-sort', 'adds-desc');
      location.href = url.toString();
      return;
    }
    const nativeSetter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value').set;
    nativeSetter.call(input, ipName);
    input.dispatchEvent(new Event('input',  { bubbles: true }));
    input.dispatchEvent(new Event('change', { bubbles: true }));

    const form = input.closest('form') ||
                 document.querySelector('form[action*="homebrew"]');
    if (form) {
      try { form.submit(); return; } catch (_) {}
    }
    // URL-fallback if form submit failed
    const url = new URL(location.href);
    url.searchParams.set(filterParam, ipName);
    url.searchParams.set('filter-sort', 'adds-desc');
    location.href = url.toString();
  }

  // ── Sent log preview ──────────────────────────────────────────────────────
  function renderSentLog() {
    const log = loadSentLog().slice(0, 8);
    if (!log.length) return '';
    const items = log.map((e) =>
      `<div style="font-size:11px;color:#888;padding:1px 0;">✓ ${escapeHtml(e.ip_name)} <span style="color:#666;">/${escapeHtml(e.section)}</span> <span style="color:#5fdc7c;">(${e.visible_items_count} items, top ${e.top_adds || 0})</span></div>`
    ).join('');
    return `
      <details style="border-top:1px solid #2a2a4a;padding-top:6px;">
        <summary style="cursor:pointer;color:#888;font-size:11px;">Recent sends (this browser, last ${log.length})</summary>
        <div style="margin-top:4px;">${items}</div>
      </details>
    `;
  }

  // ── Initial render: bulk plan screen ──────────────────────────────────────
  renderEntryScreen();
})();
