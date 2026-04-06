/**
 * Kickstarter TTRPG/D&D Bookmarklet — Part 2: Send
 *
 * Run this on ANY page (e.g. google.com) AFTER running KS-Harvest on
 * kickstarter.com in the same tab. Reads the project data from window.name
 * and sends it to the bouncer for ingestion into BigQuery.
 *
 * Why a separate bookmarklet: Kickstarter's CSP blocks outbound fetch()
 * to external domains. window.name survives page navigation, so we harvest
 * on kickstarter.com and send from a neutral page.
 */

(async function () {
  const BOUNCER = 'https://us-central1-dnd-trends-index.cloudfunctions.net/bouncer-api';
  const KEY = 'ArcaneLibrarian2026';

  // ── UI ────────────────────────────────────────────────────────────────────
  const ui = document.createElement('div');
  Object.assign(ui.style, {
    position: 'fixed', top: '12px', right: '12px', zIndex: 999999,
    background: '#0d0d1a', color: '#e0e0ff', padding: '14px 18px',
    borderRadius: '10px', fontFamily: 'monospace', fontSize: '13px',
    maxWidth: '360px', boxShadow: '0 4px 24px rgba(0,0,0,0.7)', lineHeight: '1.5',
  });
  const titleEl = document.createElement('div');
  titleEl.style.cssText = 'font-weight:bold;font-size:14px;margin-bottom:6px;color:#05ce78';
  titleEl.textContent = '🎲 KS Send (2/2)';
  const statusEl = document.createElement('div');
  statusEl.textContent = 'Reading harvested data...';
  ui.appendChild(titleEl);
  ui.appendChild(statusEl);
  document.body.appendChild(ui);

  const log = (msg) => { statusEl.textContent = msg; console.log('[ks-send]', msg); };

  // ── Read from window.name ─────────────────────────────────────────────────
  let projects;
  try {
    const parsed = JSON.parse(window.name || '{}');
    projects = parsed.ks_harvest;
  } catch (e) {
    log('⚠️ Could not read harvest data. Did you run KS-Harvest first in this tab?');
    setTimeout(() => ui.remove(), 10000);
    return;
  }

  if (!projects || projects.length === 0) {
    log('⚠️ No harvest data found. Run KS-Harvest on kickstarter.com first.');
    setTimeout(() => ui.remove(), 10000);
    return;
  }

  log(`Sending ${projects.length} projects to bouncer...`);

  // ── Send to bouncer in chunks ─────────────────────────────────────────────
  const CHUNK = 100;
  let inserted = 0;

  for (let i = 0; i < projects.length; i += CHUNK) {
    const chunk = projects.slice(i, i + CHUNK);
    try {
      const r = await fetch(`${BOUNCER}/system/kickstarter/ingest-projects`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-Ritual-Key': KEY },
        body: JSON.stringify(chunk),
      });
      const d = await r.json();
      if (d.skipped) { log('Already ingested today — skipped.'); break; }
      inserted += d.inserted || chunk.length;
      log(`Sent chunk ${Math.floor(i / CHUNK) + 1} — ${inserted} inserted so far...`);
    } catch (e) {
      console.error('[ks-send] send error', e);
      log('⚠️ Send error — check console.');
      break;
    }
  }

  // Clear window.name after successful send
  window.name = '';
  const summary = `✅ Done! ${inserted} Kickstarter projects ingested.`;
  log(summary);
  console.log('[ks-send]', summary);
  setTimeout(() => ui.remove(), 15000);
})();
