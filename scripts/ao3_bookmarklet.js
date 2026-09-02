/**
 * AO3 D&D-Crossover-Count Bookmarklet — batch-confirm edition
 * (Stage 4 of community_reception, Apr 27 2026; rebuilt Sep 2 2026 as work
 *  item B of docs/data_capture_hardening_plan.md)
 *
 * WHAT CHANGED AND WHY
 *
 * The original flow was: load page -> click bookmarklet -> click a native
 * confirm() -> POST. Three interactions per IP, ~75 for a 25-IP round, and the
 * modal showed one number in isolation.
 *
 * Now: click the bookmarklet on each page (it stashes silently), then review
 * every capture in one table and send once. ~27 interactions instead of ~75,
 * with byte-identical AO3 traffic.
 *
 * It is also a CORRECTNESS fix, not just ergonomics. BG3's 49,020 sitting next
 * to LotR's 84 is obvious in a list and invisible one dialog at a time — that
 * artifact survived four months of per-page confirmations. Outlier detection is
 * what the confirmation step was always for; a table is simply better at it.
 *
 * NO NATIVE DIALOGS ANYWHERE. confirm()/prompt() are unusable for this:
 *   - Claude's browser pane auto-suppresses them, so confirm() silently
 *     returns false and every capture reads as "cancelled".
 *   - In CDP-driven Chrome a dialog blocks the renderer outright, freezing
 *     even screenshots.
 * An in-page panel works in both, and in a normal human browser.
 *
 * Ethics unchanged: human-wielded UI tooling, not an automated scraper. Phil
 * clicks a link, AO3 renders the page as for any reader, the bookmarklet reads
 * the count he can already see. No fetch() to AO3, no auto-iteration.
 */

(async function () {
  const BOUNCER = 'https://us-central1-dnd-trends-index.cloudfunctions.net/bouncer-api';
  const KEY = 'ArcaneLibrarian2026';
  const PLATFORM = 'ao3';
  const STASH = 'arcane_ao3_batch';

  // ── Stash helpers ───────────────────────────────────────────────────────
  const load = () => {
    try { return JSON.parse(localStorage.getItem(STASH) || '[]'); }
    catch (_) { return []; }
  };
  const save = (rows) => localStorage.setItem(STASH, JSON.stringify(rows));

  // ── UI shell ────────────────────────────────────────────────────────────
  const old = document.getElementById('__ao3_batch__');
  if (old) old.remove();

  const ui = document.createElement('div');
  ui.id = '__ao3_batch__';
  Object.assign(ui.style, {
    position: 'fixed', top: '12px', right: '12px', zIndex: 999999,
    background: '#0d0d1a', color: '#e0e0ff', padding: '14px 16px',
    borderRadius: '10px', fontFamily: 'monospace', fontSize: '13px',
    width: '460px', maxHeight: '86vh', overflowY: 'auto',
    boxShadow: '0 8px 32px rgba(0,0,0,0.7)', lineHeight: '1.45',
  });
  document.body.appendChild(ui);
  const esc = (s) => String(s == null ? '' : s)
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');

  // ── Page reading (unchanged logic — this part was never the problem) ────
  function extractWorkCount() {
    for (const h of document.querySelectorAll('h2.heading, h3.heading')) {
      const m = h.textContent.match(/of\s+([\d,]+)\s+Works?\s+in\b/i);
      if (m) return parseInt(m[1].replace(/,/g, ''), 10);
    }
    for (const h of document.querySelectorAll('h2.heading, h3.heading')) {
      const m = h.textContent.trim().match(/^([\d,]+)\s+Works?\s+in\b/i);
      if (m) return parseInt(m[1].replace(/,/g, ''), 10);
    }
    for (const h of document.querySelectorAll('h2.heading, h3.heading')) {
      const m = h.textContent.match(/^([\d,]+)\s+Found\b/);
      if (m) return parseInt(m[1].replace(/,/g, ''), 10);
    }
    for (const h of document.querySelectorAll('h2.heading, h3.heading')) {
      if (/0\s+(works|found)/i.test(h.textContent)) return 0;
      if (/no works found/i.test(h.textContent)) return 0;
    }
    const pag = document.querySelector('ol.pagination li.next');
    if (pag) {
      const t = pag.textContent.match(/of\s+([\d,]+)/i);
      if (t) return parseInt(t[1].replace(/,/g, ''), 10);
    }
    return null;
  }

  const params = new URLSearchParams(location.search);
  const ipFilter = (params.get('work_search[other_tag_names]') || '').trim();

  // ── Capture the current page, if it is one ──────────────────────────────
  let notice = '';
  if (location.hostname.endsWith('archiveofourown.org')) {
    const ip = (params.get('_arcane_ip') || '').trim();
    const count = extractWorkCount();

    if (!ipFilter) {
      // The filter IS the measurement. AO3 silently ignores a missing one and
      // returns the site-wide set, so an unfiltered page looks identical to a
      // filtered one. On Sep 1 this stored 10,886 as one IP's crossover count.
      notice = '<b style="color:#ff8888">Not captured — no IP filter on this URL.</b><br>'
             + 'This page is every D&amp;D crossover on AO3, not a D&amp;D × IP count. '
             + 'Use a deep link from <code>print_fanfic_capture_urls.py</code>.';
    } else if (!ip) {
      notice = '<b style="color:#ff8888">Not captured — no <code>_arcane_ip</code> marker.</b><br>'
             + 'Use a generated deep link so attribution cannot be mistyped.';
    } else if (count === null) {
      notice = '<b style="color:#ff8888">Not captured — could not read a work count.</b><br>'
             + 'Make sure the results header is visible.';
    } else {
      const rows = load();
      const prev = rows.findIndex((r) => r.ip_name === ip);
      const row = {
        ip_name: ip,
        platform: PLATFORM,
        platform_canonical: decodeURIComponent(ipFilter),
        work_count: count,
        source_url: location.href,
        scraped_by: 'ao3_bookmarklet_batch',
      };
      if (prev >= 0) { rows[prev] = row; notice = `Updated <b>${esc(ip)}</b> → ${count.toLocaleString()}`; }
      else { rows.push(row); notice = `Captured <b>${esc(ip)}</b> → ${count.toLocaleString()}`; }
      save(rows);
    }
  } else {
    notice = 'Not on AO3 — showing the current batch.';
  }

  // ── Render ──────────────────────────────────────────────────────────────
  // Flags mirror gold_data.fanfic_capture_guard, applied at capture time so a
  // bad number is questioned before it is sent rather than after it lands.
  function flagsFor(r, all) {
    const f = [];
    if (r.work_count === 0) {
      f.push(['#ff8888', 'ZERO — every AO3 zero so far was a stale or unfilterable tag, never a real absence']);
    }
    const others = all.filter((x) => x !== r).map((x) => x.work_count).sort((a, b) => a - b);
    const med = others.length ? others[Math.floor(others.length / 2)] : null;
    if (med && med > 0 && r.work_count > med * 50) {
      f.push(['#ff8888', `${Math.round(r.work_count / med)}× the batch median (${med}) — likely metatag inflation`]);
    } else if (med && med > 0 && r.work_count > med * 10) {
      f.push(['#d9a64a', `${(r.work_count / med).toFixed(1)}× the batch median (${med})`]);
    }
    return f;
  }

  function render() {
    const rows = load();
    const flagged = rows.map((r) => ({ r, f: flagsFor(r, rows) }));
    const anyCritical = flagged.some((x) => x.f.some((y) => y[0] === '#ff8888'));

    ui.innerHTML = `
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px">
        <b style="color:#3b82f6;font-size:14px">📚 AO3 Batch Capture</b>
        <button id="__x" style="background:none;border:none;color:#888;font-size:18px;cursor:pointer">×</button>
      </div>
      <div style="background:#1a1a2e;padding:7px 9px;border-radius:6px;margin-bottom:9px">${notice}</div>
      <div style="margin-bottom:6px;color:#aaa">Batch: <b style="color:#e0e0ff">${rows.length}</b> IP(s)</div>
      ${rows.length ? `<table style="width:100%;border-collapse:collapse;font-size:12px">${
        flagged.sort((a, b) => b.r.work_count - a.r.work_count).map(({ r, f }) => `
          <tr style="border-bottom:1px solid #2a2a4a">
            <td style="padding:3px 0">${esc(r.ip_name)}${f.map((x) =>
              `<div style="color:${x[0]};font-size:11px">⚠ ${esc(x[1])}</div>`).join('')}</td>
            <td style="text-align:right;padding:3px 0 3px 8px;white-space:nowrap">
              ${r.work_count.toLocaleString()}
              <button data-del="${esc(r.ip_name)}" style="background:none;border:none;color:#666;cursor:pointer">✕</button>
            </td>
          </tr>`).join('')}</table>` : '<i style="color:#666">Nothing captured yet.</i>'}
      ${anyCritical ? '<div style="color:#ff8888;margin-top:8px;font-size:12px">⚠ Resolve or remove flagged rows before sending.</div>' : ''}
      <div style="display:flex;gap:6px;margin-top:11px">
        <button id="__send" ${rows.length ? '' : 'disabled'} style="flex:1;background:${rows.length ? '#3b82f6' : '#333'};color:#fff;border:none;padding:9px;border-radius:5px;cursor:pointer;font-weight:600">Send all ${rows.length || ''}</button>
        <button id="__clr" style="background:#1a1a2e;color:#d9a64a;border:1px solid #2a2a4a;padding:9px 11px;border-radius:5px;cursor:pointer">Clear</button>
      </div>
      <div id="__status" style="margin-top:8px;color:#aaa"></div>`;

    ui.querySelector('#__x').onclick = () => ui.remove();
    ui.querySelectorAll('[data-del]').forEach((b) => {
      b.onclick = () => { save(load().filter((r) => r.ip_name !== b.dataset.del)); notice = 'Removed.'; render(); };
    });
    ui.querySelector('#__clr').onclick = () => { save([]); notice = 'Batch cleared.'; render(); };
    ui.querySelector('#__send').onclick = send;
  }

  async function send() {
    const rows = load();
    const st = ui.querySelector('#__status');
    if (!rows.length) {
      // Say so. A silent return here reads EXACTLY like a failed send, and on
      // Sep 2 that is how a successful batch got reported as broken: the first
      // click sent nine rows and cleared the stash, the panel did not redraw,
      // so the second click landed on an empty batch and did nothing visible.
      st.innerHTML = '<b style="color:#d9a64a">Nothing to send — the batch is empty.</b>';
      return;
    }
    st.textContent = `Sending ${rows.length}…`;
    try {
      const resp = await fetch(`${BOUNCER}/system/fanfic/ingest-crossover-count`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-Ritual-Key': KEY },
        body: JSON.stringify(rows),
      });
      const data = await resp.json();
      if (resp.ok && data.inserted) {
        // Only clear on confirmed success. Clearing optimistically would lose a
        // whole round of captures to one transient failure.
        save([]);
        // RE-RENDER. Clearing the stash without redrawing leaves the full table
        // on screen, so a successful send looks identical to a no-op — the one
        // status line reporting it sits at the bottom of a scrolling panel and
        // is usually out of view. The entire value of a batch table is that the
        // screen reflects the state; it has to keep doing that after the send.
        notice = `<b style="color:#5fdc7c">✅ Sent ${data.inserted} to BigQuery. Batch cleared.</b>`;
        render();
        ui.scrollTop = 0;   // notice renders at the top; make sure it is seen
        return;
      } else {
        st.innerHTML = `<b style="color:#ff8888">⚠ Bouncer: ${esc(data.error || resp.status)}</b><br>Batch kept — retry.`;
      }
    } catch (e) {
      st.innerHTML = `<b style="color:#ff8888">⚠ Network: ${esc(e.message)}</b><br>Batch kept — retry.`;
    }
  }

  // ── Cross-tab sync ──────────────────────────────────────────────────────
  // A capture round has ~25 AO3 tabs open at once, each with its own panel.
  // localStorage is shared across them, but a panel holds whatever it rendered
  // when it was drawn — so removing an IP in one tab left it visible in the
  // other 24, and each of those stale panels was one click away from sending a
  // batch the user thought they had edited.
  //
  // The `storage` event fires in every OTHER tab of the origin when the stash
  // changes, and deliberately NOT in the tab that wrote it (that one re-renders
  // directly). So this is exactly the right hook: one listener per panel, and
  // every open panel converges on the same batch.
  //
  // It covers sending too. When one tab sends and clears, every other panel
  // empties rather than continuing to display rows that are already in BigQuery.
  //
  // Re-clicking the bookmarklet in a tab replaces the panel, so the previous
  // handler is removed first — otherwise each click would leave a listener
  // behind, closed over a detached panel, re-rendering something nobody sees.
  if (window.__ao3BatchSync) {
    window.removeEventListener('storage', window.__ao3BatchSync);
  }
  window.__ao3BatchSync = (e) => {
    if (e.key && e.key !== STASH) return;      // e.key is null on clear()
    if (!document.body.contains(ui)) return;   // panel closed in this tab
    notice = load().length
      ? '<span style="color:#d9a64a">Batch updated in another tab.</span>'
      : '<span style="color:#d9a64a">Batch cleared or sent in another tab.</span>';
    render();
  };
  window.addEventListener('storage', window.__ao3BatchSync);

  render();
})();
