/**
 * Kickstarter TTRPG/D&D Bookmarklet — GraphQL + popup-relay edition
 *
 * Run on any kickstarter.com page while logged in.
 *
 * How it works:
 * 1. Fetches projects via Kickstarter's GraphQL API (same-origin, no CSP issue)
 * 2. Opens an about:blank popup and injects the send script + data into it
 * 3. The popup is a fresh browsing context — Kickstarter's CSP does not apply
 * 4. Popup sends the data to the bouncer and reports back
 *
 * Fields collected per project:
 *   project_id, name, creator, backers_count, pledged_usd, goal_usd,
 *   percent_funded, category, status, end_date, is_dnd_centric, blurb, url
 */

(async function () {
  const BOUNCER = 'https://us-central1-dnd-trends-index.cloudfunctions.net/bouncer-api';
  const KEY = 'ArcaneLibrarian2026';
  const GQL = 'https://www.kickstarter.com/graph';
  const CATEGORY_ID = '34';
  const PER_PAGE = 20;
  const MAX_PAGES = 5;

  const SORTS = [
    { sort: 'MAGIC',    label: 'Popular' },
    { sort: 'NEWEST',   label: 'Newest' },
    { sort: 'END_DATE', label: 'Ending Soon' },
  ];

  const DND_KEYWORDS = [
    'd&d', 'dungeons', 'dragons', 'ttrpg', 'tabletop rpg', 'roleplaying',
    'role-playing', 'role playing', 'dnd', 'pathfinder', 'starfinder',
    'shadowrun', 'call of cthulhu', 'warhammer', 'fantasy rpg', 'rpg supplement',
    'adventure module', 'campaign setting', 'sourcebook', 'dungeon master',
    'game master', 'gm screen', 'dice set', 'miniatures', 'tokens', 'battlemaps',
    'world anvil', 'foundry vtt', 'roll20', '5e', 'osr', 'old school renaissance',
  ];

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
  titleEl.textContent = '🎲 Kickstarter D&D Harvest';
  const statusEl = document.createElement('div');
  statusEl.textContent = 'Initialising...';
  ui.appendChild(titleEl);
  ui.appendChild(statusEl);
  document.body.appendChild(ui);

  const log = (msg) => { statusEl.textContent = msg; console.log('[ks-bk]', msg); };

  // ── Helpers ───────────────────────────────────────────────────────────────
  function decodeProjectId(b64) {
    try { const m = atob(b64).match(/(\d+)$/); return m ? parseInt(m[1], 10) : null; }
    catch { return null; }
  }

  function isDndCentric(name, desc) {
    const text = ((name || '') + ' ' + (desc || '')).toLowerCase();
    return DND_KEYWORDS.some(kw => text.includes(kw));
  }

  function buildQuery(sort, cursor) {
    const afterArg = cursor ? `, after: "${cursor}"` : '';
    return `{ projects(categoryId: "${CATEGORY_ID}", sort: ${sort}, first: ${PER_PAGE}${afterArg}) {
      pageInfo { hasNextPage endCursor }
      edges { node {
        id name description state deadlineAt backersCount
        goal { amount currency } pledged { amount currency }
        creator { name } url category { name }
      } }
    } }`;
  }

  const csrfToken = document.querySelector('meta[name="csrf-token"]')?.content || '';

  async function fetchPage(sort, cursor) {
    try {
      const resp = await fetch(GQL, {
        method: 'POST', credentials: 'include',
        headers: { 'Content-Type': 'application/json', 'X-CSRF-Token': csrfToken },
        body: JSON.stringify({ query: buildQuery(sort, cursor) }),
      });
      if (!resp.ok) { console.warn('[ks-bk] HTTP', resp.status); return null; }
      const data = await resp.json();
      if (data.errors) console.warn('[ks-bk] GQL errors', data.errors);
      return data?.data?.projects || null;
    } catch (e) { console.warn('[ks-bk] fetch error', e); return null; }
  }

  function parseNode(node) {
    const project_id = decodeProjectId(node.id);
    if (!project_id) return null;
    const pledged_usd = parseFloat((node.pledged?.amount || '0').replace(/[^0-9.]/g, '')) || 0;
    const goal_usd = parseFloat((node.goal?.amount || '0').replace(/[^0-9.]/g, '')) || 0;
    return {
      project_id,
      name:           node.name || '',
      creator:        node.creator?.name || '',
      backers_count:  node.backersCount || 0,
      pledged_usd,
      goal_usd,
      percent_funded: goal_usd > 0 ? Math.round((pledged_usd / goal_usd) * 100) : 0,
      category:       node.category?.name || 'Tabletop Games',
      status:         (node.state || 'live').toLowerCase(),
      end_date:       node.deadlineAt ? new Date(node.deadlineAt * 1000).toISOString() : null,
      is_dnd_centric: isDndCentric(node.name, node.description),
      blurb:          (node.description || '').slice(0, 300),
      url:            node.url || '',
    };
  }

  // ── Harvest ───────────────────────────────────────────────────────────────
  const allProjects = [];
  const seenIds = new Set();

  for (const { sort, label } of SORTS) {
    log(`Fetching: ${label}...`);
    let cursor = null;
    for (let page = 1; page <= MAX_PAGES; page++) {
      const result = await fetchPage(sort, cursor);
      if (!result?.edges?.length) break;
      let pageCount = 0;
      for (const { node } of result.edges) {
        const p = parseNode(node);
        if (!p || seenIds.has(p.project_id)) continue;
        seenIds.add(p.project_id);
        allProjects.push(p);
        pageCount++;
      }
      log(`${label} p${page}: +${pageCount} (total: ${allProjects.length})`);
      if (!result.pageInfo?.hasNextPage) break;
      cursor = result.pageInfo.endCursor;
      await new Promise(r => setTimeout(r, 400));
    }
  }

  if (allProjects.length === 0) {
    log('⚠️ No projects found — check console for API errors.');
    setTimeout(() => ui.remove(), 10000);
    return;
  }

  log(`Harvested ${allProjects.length} projects. Opening send popup...`);

  // ── Open about:blank popup and inject sender script ───────────────────────
  // about:blank is a fresh context — Kickstarter's CSP does not apply to it.
  // We can script it freely via popup.document before it navigates anywhere.
  const popup = window.open('about:blank', 'ks_send', 'width=420,height=200');

  if (!popup) {
    log('⚠️ Popup blocked! Allow popups for kickstarter.com and try again.');
    setTimeout(() => ui.remove(), 12000);
    return;
  }

  // Embed projects as JSON directly in the injected script so no relay needed
  const projectsJson = JSON.stringify(allProjects).replace(/<\/script>/gi, '<\\/script>');

  popup.document.write(`<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>KS Send</title>
<style>body{font-family:monospace;font-size:13px;padding:16px;background:#0d0d1a;color:#e0e0ff}
h3{color:#05ce78;margin:0 0 10px}</style></head>
<body><h3>🎲 Kickstarter Send</h3><div id="s">Sending...</div>
<script>
(async function(){
  const el = document.getElementById('s');
  const log = m => { el.textContent = m; console.log('[ks-send]', m); };
  const projects = ${projectsJson};
  const BOUNCER = '${BOUNCER}';
  const KEY = '${KEY}';
  const CHUNK = 100;
  let inserted = 0;
  for(let i = 0; i < projects.length; i += CHUNK){
    const chunk = projects.slice(i, i + CHUNK);
    try {
      const r = await fetch(BOUNCER + '/system/kickstarter/ingest-projects', {
        method: 'POST',
        headers: {'Content-Type':'application/json','X-Ritual-Key': KEY},
        body: JSON.stringify(chunk)
      });
      const d = await r.json();
      if(d.skipped){ log('Already ingested today — skipped.'); return; }
      inserted += d.inserted || chunk.length;
      log('Sent chunk ' + (Math.floor(i/CHUNK)+1) + ' — ' + inserted + ' inserted...');
    } catch(e){
      log('⚠️ Error: ' + e.message);
      console.error('[ks-send]', e);
      return;
    }
  }
  log('✅ Done! ' + inserted + ' projects ingested.');
  setTimeout(() => window.close(), 8000);
})();
<\/script></body></html>`);
  popup.document.close();

  log(`✅ Popup opened — watch it for completion. This panel will close.`);
  setTimeout(() => ui.remove(), 8000);
})();
