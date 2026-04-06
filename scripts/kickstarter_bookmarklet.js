/**
 * Kickstarter TTRPG/D&D Bookmarklet — Part 1: Harvest
 *
 * Run this on kickstarter.com while logged in.
 * It collects projects via the GraphQL API and stores them in window.name
 * (which survives page navigation). Then navigate to any other page
 * (e.g. google.com) and click the KS-Send bookmark to send to the bouncer.
 *
 * Why window.name: Kickstarter's server-side CSP blocks outbound fetch()
 * to external domains (including cloudfunctions.net). window.name is a
 * string that persists across navigation in the same tab, so we use it
 * as a relay between the two bookmarklets.
 */

(async function () {
  const GQL = 'https://www.kickstarter.com/graph';
  const CATEGORY_ID = '34'; // Tabletop Games
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
  titleEl.textContent = '🎲 KS Harvest (1/2)';
  const statusEl = document.createElement('div');
  statusEl.textContent = 'Initialising...';
  ui.appendChild(titleEl);
  ui.appendChild(statusEl);
  document.body.appendChild(ui);

  const log = (msg) => { statusEl.textContent = msg; console.log('[ks-bk]', msg); };

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
      name:          node.name || '',
      creator:       node.creator?.name || '',
      backers_count: node.backersCount || 0,
      pledged_usd,
      goal_usd,
      percent_funded: goal_usd > 0 ? Math.round((pledged_usd / goal_usd) * 100) : 0,
      category:      node.category?.name || 'Tabletop Games',
      status:        (node.state || 'live').toLowerCase(),
      end_date:      node.deadlineAt ? new Date(node.deadlineAt * 1000).toISOString() : null,
      is_dnd_centric: isDndCentric(node.name, node.description),
      blurb:         (node.description || '').slice(0, 300),
      url:           node.url || '',
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

  // ── Store in window.name for cross-page relay ─────────────────────────────
  window.name = JSON.stringify({ ks_harvest: allProjects });
  log(`✅ Stored ${allProjects.length} projects. Now go to google.com and click KS-Send.`);
  console.log('[ks-bk] Stored', allProjects.length, 'projects in window.name');
  // Keep UI visible so user can read the instruction
})();
