/**
 * D&D Beyond Homebrew Capture Bookmarklet
 * (Stage 6a of the community_reception multi-source composite, Apr 29, 2026)
 *
 * Two-state floating panel:
 *
 *   PICK mode    — when no filter active. Dropdown of 40 priority IPs
 *                   (grouped by cohort, with per-IP per-section progress
 *                   bars). On selection: fill DDB's search input + submit
 *                   the filter form. Page reloads with filtered results.
 *
 *   CAPTURE mode — when a filter IS active and listing rows are visible.
 *                   Reads the .list-row elements, presents a confirm
 *                   summary (IP / section / visible items / top adds),
 *                   POSTs to /system/homebrew/ingest-ddb on the bouncer.
 *
 * On bookmarklet click the panel decides which state based on the page:
 *   - filter-search input has a non-empty value → CAPTURE mode
 *   - else → PICK mode
 *
 * Ethics: this is human-wielded UI tooling, not an automated scraper.
 * Phil clicks links / types in DDB's own UI; the bookmarklet just reads
 * what's already rendered and POSTs structured rows home. No fetch() to
 * D&D Beyond, no auto-iteration.
 *
 * Sent log: per-(ip, section) timestamps persisted in localStorage so
 * progress survives page reloads + browser restarts. The bouncer's
 * /system/homebrew/ip-list route also returns server-side sent counts so
 * a fresh browser starts with the right state.
 *
 * Selectors learned from Phil's F12 scout on Apr 29:
 *   - Row container:    .list-row (with data-slug + data-type)
 *   - Name link:        .list-row-name-primary-text a
 *   - Adds count:       .list-row-col-adds .list-row-primary-text
 *   - Views count:      .list-row-col-views .list-row-primary-text
 *   - Comments count:   .list-row-col-comments .list-row-primary-text
 *   - Rating points:    .list-row-rating-points .positive
 *   - Rating up/down:   .list-row-rating-counts .positive / .negative
 *   - Base class:       .list-row-col-baseclass .list-row-primary-text
 *   - Author:           .list-row-author-primary-text
 *   - Pagination:       .b-pagination — last page number is the page count
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
  const IP_LIST_TTL_MS = 24 * 60 * 60 * 1000; // 24 hours

  // ── Sanity: must be on D&D Beyond homebrew ────────────────────────────────
  if (!location.hostname.endsWith('dndbeyond.com') ||
      !location.pathname.startsWith('/homebrew/')) {
    alert(
      'Open a D&D Beyond homebrew section first.\n' +
      'e.g. dndbeyond.com/homebrew/subclasses'
    );
    return;
  }

  const section = location.pathname.split('/').filter(Boolean).pop();
  if (!section || section === 'homebrew') {
    alert('Navigate INTO a homebrew section (subclasses / spells / monsters / etc.)');
    return;
  }

  // ── Remove pre-existing panel (re-clicks reset) ───────────────────────────
  const existing = document.getElementById('__ddb_homebrew_panel__');
  if (existing) existing.remove();

  // ── Build panel skeleton ──────────────────────────────────────────────────
  const panel = document.createElement('div');
  panel.id = '__ddb_homebrew_panel__';
  Object.assign(panel.style, {
    position: 'fixed', top: '12px', right: '12px', zIndex: '999999',
    background: '#0f0f1e', color: '#e0e0ff',
    padding: '14px 16px', borderRadius: '12px',
    fontFamily: '-apple-system, BlinkMacSystemFont, sans-serif',
    fontSize: '13px', lineHeight: '1.4',
    width: '440px', maxHeight: '85vh', overflowY: 'auto',
    boxShadow: '0 8px 32px rgba(0,0,0,0.6)',
    border: '1px solid #2a2a4a',
  });
  panel.innerHTML = `
    <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:8px;">
      <div style="font-weight:600;font-size:14px;color:#e87722;">📜 DDB Homebrew Capture</div>
      <button id="__ddb_close_btn" style="background:none;border:none;color:#888;font-size:18px;cursor:pointer;">×</button>
    </div>
    <div id="__ddb_panel_body" style="display:flex;flex-direction:column;gap:10px;"></div>
  `;
  document.body.appendChild(panel);
  document.getElementById('__ddb_close_btn').onclick = () => panel.remove();

  const body = document.getElementById('__ddb_panel_body');
  const set = (html) => { body.innerHTML = html; };
  const append = (html) => { body.insertAdjacentHTML('beforeend', html); };
  const status = (msg, color = '#e0e0ff') => {
    const el = document.getElementById('__ddb_status') ||
      (() => { const e = document.createElement('div'); e.id = '__ddb_status'; body.prepend(e); return e; })();
    el.style.cssText = `padding:6px 8px;border-radius:6px;background:#1a1a2e;color:${color};font-size:12px;`;
    el.textContent = msg;
  };

  status('Loading…');

  // ── Fetch IP list (cached) ────────────────────────────────────────────────
  let ipPayload = null;
  try {
    const cached = localStorage.getItem(IP_LIST_KEY);
    if (cached) {
      const parsed = JSON.parse(cached);
      if (Date.now() - parsed.fetched_at < IP_LIST_TTL_MS) {
        ipPayload = parsed.payload;
      }
    }
  } catch (_) {}
  if (!ipPayload) {
    try {
      const resp = await fetch(`${BOUNCER}/system/homebrew/ip-list`, {
        method: 'GET',
        headers: { 'X-Ritual-Key': KEY },
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

  // ── Parse the page: rows, search input, pagination ────────────────────────
  function parseRows() {
    const rowEls = document.querySelectorAll('.list-row[data-slug]');
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

  function findSearchInput() {
    // Phil's F12 scout showed the input value didn't auto-populate from URL, so
    // we try several plausible selectors. The first selector is the most
    // specific; later ones are progressively looser.
    return (
      document.querySelector('input[name="filter-search"]') ||
      document.querySelector('input[name*="search" i]') ||
      document.querySelector('input[placeholder*="earch" i]:not([type="hidden"])') ||
      document.querySelector('.ddb-listing-filters input[type="text"]') ||
      document.querySelector('form input[type="text"]')
    );
  }

  function parsePages() {
    // Pagination "1 2 3 4 5 … 2832 Next" — the second-to-last item before
    // "Next" is the last page number.
    const pag = document.querySelector('.b-pagination');
    if (!pag) return 1;
    const items = Array.from(pag.querySelectorAll('.b-pagination-item, li, a, span'))
      .map((el) => el.textContent.trim())
      .filter((t) => /^\d+$/.test(t))
      .map((t) => parseInt(t, 10));
    return items.length ? Math.max(...items) : 1;
  }

  // ── Sent log helpers ──────────────────────────────────────────────────────
  function loadSentLog() {
    try { return JSON.parse(localStorage.getItem(SENT_LOG_KEY) || '[]'); }
    catch (_) { return []; }
  }
  function appendSentLog(entry) {
    const log = loadSentLog();
    log.unshift(entry);
    while (log.length > 200) log.pop();
    localStorage.setItem(SENT_LOG_KEY, JSON.stringify(log));
  }

  // Per-IP-section completion derived from BOTH the server-side
  // ip-list response AND the local sent-log (most-recent wins).
  function progressFor(ipName) {
    const log = loadSentLog();
    const fromLog = new Set(log.filter((e) => e.ip_name === ipName).map((e) => e.section));
    const ipRow = PRIORITY_IPS.find((x) => x.ip_name === ipName);
    const fromServer = ipRow ? Object.entries(ipRow.sections || {}).filter(([_, ts]) => !!ts).map(([s]) => s) : [];
    const all = new Set([...fromLog, ...fromServer]);
    const priorityDone = PRIORITY_SECTIONS.filter((s) => all.has(s));
    return {
      priorityDone:  priorityDone.length,
      priorityTotal: PRIORITY_SECTIONS.length,
      allDone:       Array.from(all),
    };
  }

  const rows         = parseRows();
  const searchInput  = findSearchInput();
  const activeQuery  = searchInput && searchInput.value ? searchInput.value.trim() : '';
  const totalPages   = parsePages();
  // Heuristic for "filter looks applied": EITHER the input has a value, OR
  // the URL has filter-search and there are very few pages (an unfiltered
  // section has 1000s of pages).
  const urlHasFilter = !!new URLSearchParams(location.search).get('filter-search');
  const filterApplied = (!!activeQuery && rows.length > 0) ||
                        (urlHasFilter && totalPages > 0 && totalPages < 100);
  const queryFromUrl = new URLSearchParams(location.search).get('filter-search') || '';
  const effectiveQuery = activeQuery || queryFromUrl;

  // ── Render: shared "page meta" header ─────────────────────────────────────
  function renderHeader() {
    return `
      <div style="background:#1a1a2e;padding:8px 10px;border-radius:6px;font-size:12px;">
        <div><b>Section:</b> <span style="color:#e87722;">/homebrew/${section}</span></div>
        ${filterApplied ?
          `<div><b>Active filter:</b> <span style="color:#5fdc7c;">"${effectiveQuery || '(visible)'}"</span></div>` :
          `<div style="color:#888;">No filter detected</div>`}
        <div style="color:#888;font-size:11px;margin-top:2px;">
          ${rows.length} rows visible · ${totalPages} page${totalPages !== 1 ? 's' : ''} total
        </div>
      </div>
    `;
  }

  // ── Render: CAPTURE mode ──────────────────────────────────────────────────
  function renderCaptureMode() {
    const top = rows.slice(0, 5).map((r, i) =>
      `<div style="padding:2px 0;font-size:11px;"><span style="color:#888;">${i + 1}.</span> ${escapeHtml(r.name)} · <b style="color:#5fdc7c;">${r.adds}</b> adds${r.base_class ? ` · ${escapeHtml(r.base_class)}` : ''}</div>`
    ).join('');

    // Best-guess IP from active query — matched against priority list
    const guess = guessIpFromQuery(effectiveQuery);

    set(`
      ${renderHeader()}
      <div style="background:#1a1a2e;padding:10px;border-radius:6px;">
        <div style="font-size:11px;color:#888;margin-bottom:4px;">Top ${Math.min(5, rows.length)} by adds:</div>
        ${top || '<i style="color:#888;">no rows parsed</i>'}
      </div>
      <div>
        <label style="font-size:11px;color:#888;">Save as IP:</label>
        <select id="__ddb_capture_ip" style="width:100%;background:#1a1a2e;color:#e0e0ff;border:1px solid #2a2a4a;padding:6px;border-radius:4px;margin-top:4px;font-size:13px;">
          ${PRIORITY_IPS.map((ip) =>
            `<option value="${escapeAttr(ip.ip_name)}" ${ip.ip_name === guess ? 'selected' : ''}>${escapeHtml(ip.ip_name)} (${COHORT_LABELS[ip.cohort].split(' ')[0]})</option>`
          ).join('')}
        </select>
      </div>
      <button id="__ddb_save_btn" style="background:#e87722;color:#fff;border:none;padding:10px;border-radius:6px;cursor:pointer;font-weight:600;font-size:13px;">
        💾 Save ${rows.length} ${section} for selected IP
      </button>
      <div style="border-top:1px solid #2a2a4a;padding-top:8px;">
        <button id="__ddb_pickmode_btn" style="background:#1a1a2e;color:#7aa3ff;border:1px solid #2a2a4a;padding:6px;border-radius:4px;cursor:pointer;font-size:12px;width:100%;">
          ↻ Pick a different IP (clears filter)
        </button>
      </div>
      ${renderSentLog()}
    `);

    document.getElementById('__ddb_save_btn').onclick = onCaptureClick;
    document.getElementById('__ddb_pickmode_btn').onclick = onClearFilterClick;
  }

  // ── Render: PICK mode ─────────────────────────────────────────────────────
  function renderPickMode() {
    const cohorts = ['marquee', 'asymmetry', 'canary', 'active', 'roundout'];
    const groupHtml = cohorts.map((cohort) => {
      const ipsInCohort = PRIORITY_IPS.filter((ip) => ip.cohort === cohort);
      if (!ipsInCohort.length) return '';
      const items = ipsInCohort.map((ip) => {
        const prog = progressFor(ip.ip_name);
        const pct = prog.priorityTotal > 0 ?
          (prog.priorityDone / prog.priorityTotal) : 0;
        const isSectionDone = prog.allDone.includes(section);
        const fadeStyle = isSectionDone ? 'opacity:0.55;' : '';
        const sectionDoneIcon = isSectionDone ? ' ✓' : '';
        const allDoneIcon = pct === 1 ? ' ✅' : '';
        const bar = renderBar(prog.priorityDone, prog.priorityTotal, COHORT_COLORS[cohort]);
        return `<div class="__ddb_pick_row" data-ip="${escapeAttr(ip.ip_name)}" style="${fadeStyle}padding:4px 6px;cursor:pointer;border-radius:4px;display:flex;align-items:center;justify-content:space-between;gap:8px;font-size:12px;" onmouseover="this.style.background='#1a1a2e'" onmouseout="this.style.background='transparent'">
          <span><span style="color:#e0e0ff;">${escapeHtml(ip.ip_name)}</span>${sectionDoneIcon}${allDoneIcon}</span>
          <span style="display:flex;align-items:center;gap:6px;">
            <span style="font-size:10px;color:#888;">${prog.priorityDone}/${prog.priorityTotal}</span>
            ${bar}
          </span>
        </div>`;
      }).join('');
      return `<div style="margin-bottom:8px;">
        <div style="font-size:11px;color:${COHORT_COLORS[cohort]};font-weight:600;margin-bottom:4px;">${COHORT_LABELS[cohort]} (${ipsInCohort.length})</div>
        ${items}
      </div>`;
    }).join('');

    set(`
      ${renderHeader()}
      <div>
        <input id="__ddb_search" type="text" placeholder="Type to filter the list…" style="width:100%;background:#1a1a2e;color:#e0e0ff;border:1px solid #2a2a4a;padding:7px;border-radius:4px;font-size:13px;box-sizing:border-box;" />
      </div>
      <div id="__ddb_pick_list" style="max-height:380px;overflow-y:auto;background:#0a0a18;padding:6px;border-radius:6px;border:1px solid #2a2a4a;">
        ${groupHtml}
      </div>
      <div style="font-size:11px;color:#888;text-align:center;">
        Pick an IP → bookmarklet types it into DDB's search + submits filter.<br/>
        Re-click bookmarklet on the filtered page to capture.
      </div>
      ${renderSentLog()}
    `);

    // Wire up clicks + filter
    document.querySelectorAll('.__ddb_pick_row').forEach((el) => {
      el.onclick = () => onPickClick(el.getAttribute('data-ip'));
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
  }

  function renderBar(done, total, color) {
    const pct = total > 0 ? Math.round((done / total) * 100) : 0;
    return `<span style="display:inline-block;width:60px;height:6px;background:#1a1a2e;border-radius:3px;overflow:hidden;"><span style="display:block;width:${pct}%;height:100%;background:${color};"></span></span>`;
  }

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

  // ── Action handlers ───────────────────────────────────────────────────────
  function guessIpFromQuery(q) {
    if (!q) return null;
    const lq = q.toLowerCase();
    let best = null;
    for (const ip of PRIORITY_IPS) {
      if (ip.ip_name.toLowerCase() === lq) return ip.ip_name;
      if (ip.ip_name.toLowerCase().includes(lq) || lq.includes(ip.ip_name.toLowerCase())) {
        best = best || ip.ip_name;
      }
    }
    return best;
  }

  function onPickClick(ipName) {
    status(`→ Filtering DDB for "${ipName}"…`, '#e87722');
    const input = findSearchInput();
    if (!input) {
      status('⚠️ Could not find the DDB search input. Type "' + ipName + '" into the page filter manually, then click Apply.', '#ff8888');
      return;
    }
    // Set value via the React-friendly path (some controlled inputs need the
    // native setter to fire properly)
    const nativeSetter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value').set;
    nativeSetter.call(input, ipName);
    input.dispatchEvent(new Event('input',  { bubbles: true }));
    input.dispatchEvent(new Event('change', { bubbles: true }));

    // Find the form / submit button
    const form = input.closest('form') ||
                 document.querySelector('form[action*="homebrew"]') ||
                 document.querySelector('.ddb-listing-filters form');
    const submitBtn = (form && form.querySelector('button[type="submit"], input[type="submit"]')) ||
                      document.querySelector('.ddb-listing-filters-field-list-item-submit, [class*="submit"]');
    if (form) {
      try { form.submit(); return; } catch (e) {}
    }
    if (submitBtn) {
      submitBtn.click();
      return;
    }
    status('⚠️ Filled the input but couldn\'t auto-submit. Press Enter / click Filter manually.', '#d9a64a');
  }

  function onClearFilterClick() {
    // Navigate to the section root with no filter
    location.href = `/homebrew/${section}`;
  }

  async function onCaptureClick() {
    const ipName = document.getElementById('__ddb_capture_ip').value;
    if (!ipName) { status('⚠️ Select an IP first', '#ff8888'); return; }
    const btn = document.getElementById('__ddb_save_btn');
    btn.disabled = true; btn.textContent = '… saving …';

    const topAdds = rows.length ? Math.max(...rows.map((r) => r.adds)) : 0;
    const estTotal = totalPages * 30; // 30 rows per page upper-bound

    const payload = {
      ip_name: ipName,
      ddb_section: section,
      search_query: effectiveQuery,
      visible_items_count: rows.length,
      pages_count: totalPages,
      estimated_total_count: estTotal,
      top_items: rows.slice(0, 30),
      source_url: location.href,
      scraped_by: 'ddb_homebrew_bookmarklet',
    };

    try {
      const resp = await fetch(`${BOUNCER}/system/homebrew/ingest-ddb`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-Ritual-Key': KEY,
        },
        body: JSON.stringify(payload),
      });
      const data = await resp.json();
      if (resp.ok && data.inserted) {
        appendSentLog({
          ip_name: ipName,
          section,
          visible_items_count: rows.length,
          top_adds: topAdds,
          ts: new Date().toISOString(),
        });
        status(`✅ Saved ${rows.length} ${section} for ${ipName} (top adds: ${topAdds}).`, '#5fdc7c');
        btn.style.background = '#5fdc7c';
        btn.textContent = '✓ Saved — click anywhere to dismiss';
        // Refresh in 1.2s to PICK mode so user can move on
        setTimeout(() => {
          renderPickMode();
        }, 1500);
      } else {
        status(`⚠️ Bouncer error: ${data.error || resp.status}`, '#ff8888');
        btn.disabled = false; btn.textContent = '💾 Retry save';
      }
    } catch (e) {
      status(`⚠️ Network error: ${e.message}`, '#ff8888');
      btn.disabled = false; btn.textContent = '💾 Retry save';
    }
  }

  // ── Helpers: HTML escape ──────────────────────────────────────────────────
  function escapeHtml(s) {
    return String(s || '')
      .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
  }
  function escapeAttr(s) { return escapeHtml(s); }

  // ── Initial render based on detected state ────────────────────────────────
  if (filterApplied && rows.length > 0) {
    renderCaptureMode();
  } else {
    renderPickMode();
  }
})();
