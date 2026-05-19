/**
 * BackerKit RPG/D&D Bookmarklet — Inertia-JSON + popup-relay edition
 *
 * Why this exists: the server-side Cloud Function harvester
 * (cloud_functions/backerkit_harvester/main.py) gets a deterministic
 * 403 Forbidden from BackerKit's edge (GCP datacenter IPs are flagged).
 * Confirmed persistent in Cloud Run logs (every scheduled run since
 * >=May 13 2026, <1s 403 = IP/WAF block). The proven mitigation —
 * already used for the Cloudflare-blocked TTRPG forums — is the
 * human-wielded bookmarklet: run in Phil's own signed-in browser
 * (residential IP, real session), same-origin, so no bot/IP block.
 *
 * Run on any backerkit.com page while signed in.
 *
 * How it works (mirrors the Kickstarter bookmarklet):
 * 1. Same-origin fetch of BackerKit's Inertia JSON endpoint for the
 *    role-playing-games collection (X-Inertia header → JSON not HTML).
 * 2. Parse the crowdfunding/projects array; classify + normalise
 *    fields to the commercial_data.backerkit_projects schema.
 * 3. Open an about:blank popup (fresh context — BackerKit CSP does
 *    not apply) and inject a sender that POSTs to the bouncer.
 *
 * Field parity with cloud_functions/backerkit_harvester/main.py so the
 * bouncer route can write the SAME commercial_data.backerkit_projects
 * table and every downstream view keeps working unchanged.
 */

(async function () {
  const BOUNCER = 'https://us-central1-dnd-trends-index.cloudfunctions.net/bouncer-api';
  const KEY = 'ArcaneLibrarian2026';
  // Mirror the Cloud Function's single source URL exactly (parity).
  const SRC = 'https://www.backerkit.com/c/collections/role-playing-games?sort_by=trending';

  const DND_KEYWORDS = [
    '5e', '5th edition', 'd&d', 'dungeons', '2024 compatible',
    'black flag', 'tales of the valiant', 'mcdm', 'dragonbane',
  ];
  const OSR_KEYWORDS = ['osr', 'old school', 'old-school', 'b/x', 'odnd', 'ad&d'];

  function classifySystem(text) {
    const t = (text || '').toLowerCase();
    if (DND_KEYWORDS.some(k => t.includes(k))) return '5e Compatible';
    if (OSR_KEYWORDS.some(k => t.includes(k))) return 'OSR';
    return 'RPG (Other)';
  }
  function parseAmount(s) {
    if (!s) return 0.0;
    const cleaned = String(s).replace(/[^\d.]/g, '');
    const v = parseFloat(cleaned);
    return isNaN(v) ? 0.0 : v;
  }
  function daysRemaining(endedAtStr) {
    // BackerKit format: "April 30, 2026 at 10:00 AM PDT"
    try {
      const datePart = String(endedAtStr).split(' at ')[0];
      const dt = new Date(datePart);
      if (isNaN(dt.getTime())) return 0;
      const ms = dt.getTime() - Date.now();
      return Math.max(0, Math.round(ms / 86400000));
    } catch (e) { return 0; }
  }

  // ── UI ────────────────────────────────────────────────────────────────────
  const ui = document.createElement('div');
  Object.assign(ui.style, {
    position: 'fixed', top: '12px', right: '12px', zIndex: 999999,
    background: '#0d0d1a', color: '#e0e0ff', padding: '14px 18px',
    borderRadius: '10px', fontFamily: 'monospace', fontSize: '13px',
    maxWidth: '360px', boxShadow: '0 4px 24px rgba(0,0,0,0.7)', lineHeight: '1.5',
  });
  const titleEl = document.createElement('div');
  titleEl.style.cssText = 'font-weight:bold;font-size:14px;margin-bottom:6px;color:#f97316';
  titleEl.textContent = '🎲 BackerKit RPG Harvest';
  const statusEl = document.createElement('div');
  statusEl.textContent = 'Initialising...';
  ui.appendChild(titleEl);
  ui.appendChild(statusEl);
  document.body.appendChild(ui);
  const log = (msg) => { statusEl.textContent = msg; console.log('[bk-bk]', msg); };
  const close = (d) => setTimeout(() => ui.remove(), d || 8000);

  // ── Sanity: must be on BackerKit ──────────────────────────────────────────
  if (!location.hostname.endsWith('backerkit.com')) {
    log('⚠️ Not on backerkit.com — open BackerKit (signed in) first.');
    close();
    return;
  }

  // ── Fetch the Inertia JSON (same-origin, your session, residential IP) ─────
  log('Fetching RPG collection (trending)...');
  let data;
  try {
    const resp = await fetch(SRC, {
      method: 'GET',
      credentials: 'include',
      headers: {
        'X-Inertia': 'true',
        'X-Requested-With': 'XMLHttpRequest',
        'Accept': 'application/json',
      },
    });
    if (!resp.ok) {
      log(`⚠️ Fetch failed: HTTP ${resp.status}. Are you signed in to BackerKit?`);
      close(12000);
      return;
    }
    data = await resp.json();
  } catch (e) {
    log(`⚠️ Network error: ${e.message}`);
    console.error('[bk-bk]', e);
    close(12000);
    return;
  }

  // CF read data["crowdfunding/projects"] off the top level; standard
  // Inertia nests page props under .props — try both for robustness.
  const projects =
    (data && data['crowdfunding/projects']) ||
    (data && data.props && data.props['crowdfunding/projects']) ||
    [];

  if (!projects.length) {
    log('⚠️ No projects in response. BackerKit may have changed the Inertia shape — check console.');
    console.warn('[bk-bk] raw response keys:', data && Object.keys(data));
    close(15000);
    return;
  }

  const rows = projects.map(p => {
    const title = p.title || '';
    const idStr = String(p.id || '');
    return {
      project_id: idStr,
      title: title,
      creator: p.creator_name || '',
      funding_usd: parseAmount(p.raised_amount),
      backers_count: parseInt(p.backers, 10) || 0,
      days_remaining: daysRemaining(p.ended_at || ''),
      system_tag: classifySystem(title + ' ' + idStr),
      source_url: p.formatted_permalink || '',
    };
  }).filter(r => r.project_id && r.title);

  if (!rows.length) {
    log('⚠️ Parsed 0 valid rows (missing project_id/title).');
    close(12000);
    return;
  }

  log(`Harvested ${rows.length} projects. Opening send popup...`);

  // ── about:blank popup (CSP-free context) posts to the bouncer ─────────────
  const popup = window.open('about:blank', 'bk_send', 'width=420,height=200');
  if (!popup) {
    log('⚠️ Popup blocked! Allow popups for backerkit.com and re-run.');
    close(12000);
    return;
  }
  const rowsJson = JSON.stringify(rows).replace(/<\/script>/gi, '<\\/script>');

  popup.document.write(`<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>BK Send</title>
<style>body{font-family:monospace;font-size:13px;padding:16px;background:#0d0d1a;color:#e0e0ff}
h3{color:#05ce78;margin:0 0 10px}</style></head>
<body><h3>🎲 BackerKit Send</h3><div id="s">Sending...</div>
<script>
(async function(){
  const el = document.getElementById('s');
  const log = m => { el.textContent = m; console.log('[bk-send]', m); };
  const rows = ${rowsJson};
  const BOUNCER = '${BOUNCER}';
  const KEY = '${KEY}';
  try {
    const r = await fetch(BOUNCER + '/system/backerkit/ingest-projects', {
      method: 'POST',
      headers: {'Content-Type':'application/json','X-Ritual-Key': KEY},
      body: JSON.stringify(rows)
    });
    const d = await r.json();
    if(d.skipped){ log('Already ingested today — skipped.'); return; }
    const inserted = d.inserted || rows.length;
    log('✅ Done! ' + inserted + ' BackerKit projects ingested.');
  } catch(e){
    log('⚠️ Error: ' + e.message);
    console.error('[bk-send]', e);
    return;
  }
  setTimeout(() => window.close(), 8000);
})();
<\/script></body></html>`);
  popup.document.close();

  log('✅ Popup opened — watch it for completion. This panel will close.');
  close(8000);
})();
