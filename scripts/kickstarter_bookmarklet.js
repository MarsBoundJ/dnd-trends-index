/**
 * Kickstarter TTRPG/D&D Bookmarklet — GraphQL edition
 *
 * Usage: Paste the minified version (kickstarter_bookmarklet.txt) into a
 * browser bookmark URL field. Click the bookmark while on any Kickstarter
 * page that you are logged into.
 *
 * Why GraphQL: Kickstarter pages are React-rendered. A fetch() of the HTML
 * returns empty card containers (server-side shell only); project data is
 * injected client-side after hydration. The public GraphQL endpoint at
 * /graph bypasses this entirely and returns structured JSON directly.
 *
 * Sends to ONE endpoint per run:
 *   POST /system/kickstarter/ingest-projects → commercial_data.kickstarter_projects
 *
 * Fields collected per project:
 *   project_id, name, creator, backers_count, pledged_usd, goal_usd,
 *   percent_funded, category, status, end_date, is_dnd_centric, blurb, url
 *
 * Pagination: cursor-based (Relay-style) via pageInfo.endCursor
 * Dedup:      seenIds Set prevents duplicates across sort orders
 * Auth:       X-CSRF-Token from page meta tag (required by Kickstarter)
 */

(async function () {
  // ── Config ────────────────────────────────────────────────────────────────
  const BOUNCER = 'https://us-central1-dnd-trends-index.cloudfunctions.net/bouncer-api';
  const KEY = 'ArcaneLibrarian2026';
  const GQL = 'https://www.kickstarter.com/graph';
  const CATEGORY_ID = '34'; // Tabletop Games
  const PER_PAGE = 20;
  const MAX_PAGES = 5; // 20 items/page → up to 100 per sort order

  // Three sort orders to maximise coverage; duplicates filtered by seenIds
  const SORTS = [
    { sort: 'MAGIC',    label: 'Popular' },
    { sort: 'NEWEST',   label: 'Newest' },
    { sort: 'END_DATE', label: 'Ending Soon' },
  ];

  // Keywords that mark a project as D&D / TTRPG-centric
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
  ui.id = '__ks_bk_ui__';
  Object.assign(ui.style, {
    position: 'fixed', top: '12px', right: '12px', zIndex: 999999,
    background: '#0d0d1a', color: '#e0e0ff', padding: '14px 18px',
    borderRadius: '10px', fontFamily: 'monospace', fontSize: '13px',
    maxWidth: '360px', boxShadow: '0 4px 24px rgba(0,0,0,0.7)',
    lineHeight: '1.5',
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

  // Kickstarter GraphQL IDs are base64-encoded strings like "Project-339473088"
  // Decode to extract the numeric ID stored in the database
  function decodeProjectId(b64) {
    try {
      const decoded = atob(b64); // e.g. "Project-339473088"
      const m = decoded.match(/(\d+)$/);
      return m ? parseInt(m[1], 10) : null;
    } catch {
      return null;
    }
  }

  function isDndCentric(name, blurb) {
    const text = ((name || '') + ' ' + (blurb || '')).toLowerCase();
    return DND_KEYWORDS.some(kw => text.includes(kw));
  }

  // ── GraphQL query ─────────────────────────────────────────────────────────
  // Uses variables so terser doesn't mangle the query string.
  // usdPledged is a convenience float field; pledged/goal are Money objects.
  const GQL_QUERY = `
    query FetchProjects($categoryId: String!, $sort: ProjectSort!, $first: Int!, $after: String) {
      projects(categoryId: $categoryId, sort: $sort, first: $first, after: $after) {
        pageInfo { hasNextPage endCursor }
        edges {
          node {
            id
            name
            blurb
            state
            deadlineAt
            backersCount
            usdPledged
            goal { amount currency }
            pledged { amount currency }
            creator { name }
            url
            category { name }
          }
        }
      }
    }
  `;

  // CSRF token is required — Kickstarter rejects unauthenticated GQL POSTs
  const csrfToken = document.querySelector('meta[name="csrf-token"]')?.content || '';

  // ── Fetch one page of results ─────────────────────────────────────────────
  async function fetchPage(sort, cursor) {
    try {
      const resp = await fetch(GQL, {
        method: 'POST',
        credentials: 'include',
        headers: {
          'Content-Type': 'application/json',
          'X-CSRF-Token': csrfToken,
        },
        body: JSON.stringify({
          query: GQL_QUERY,
          variables: {
            categoryId: CATEGORY_ID,
            sort,
            first: PER_PAGE,
            after: cursor || null,
          },
        }),
      });
      if (!resp.ok) { console.warn('[ks-bk] HTTP', resp.status); return null; }
      const data = await resp.json();
      if (data.errors) { console.warn('[ks-bk] GQL errors', data.errors); }
      return data?.data?.projects || null;
    } catch (e) {
      console.warn('[ks-bk] fetch error', e);
      return null;
    }
  }

  // ── Parse one GraphQL project node → row ─────────────────────────────────
  function parseNode(node) {
    const project_id = decodeProjectId(node.id);
    if (!project_id) return null;

    // usdPledged is a pre-converted float; fall back to pledged.amount for
    // non-USD campaigns (amount strings may include decimals or commas)
    const pledged_usd = parseFloat(node.usdPledged) ||
      parseFloat((node.pledged?.amount || '0').replace(/[^0-9.]/g, '')) || 0;
    const goal_usd =
      parseFloat((node.goal?.amount || '0').replace(/[^0-9.]/g, '')) || 0;
    const percent_funded = goal_usd > 0
      ? Math.round((pledged_usd / goal_usd) * 100) : 0;

    // deadlineAt is a Unix timestamp (seconds)
    const end_date = node.deadlineAt
      ? new Date(node.deadlineAt * 1000).toISOString()
      : null;

    return {
      project_id,
      name:          node.name || '',
      creator:       node.creator?.name || '',
      backers_count: node.backersCount || 0,
      pledged_usd,
      goal_usd,
      percent_funded,
      category:      node.category?.name || 'Tabletop Games',
      status:        (node.state || 'live').toLowerCase(),
      end_date,
      is_dnd_centric: isDndCentric(node.name, node.blurb),
      blurb:         (node.blurb || '').slice(0, 300),
      url:           node.url || '',
    };
  }

  // ── Main harvest loop ─────────────────────────────────────────────────────
  const allProjects = [];
  const seenIds = new Set();

  for (const { sort, label } of SORTS) {
    log(`Fetching: ${label}...`);
    let cursor = null;
    let sortCount = 0;

    for (let page = 1; page <= MAX_PAGES; page++) {
      const result = await fetchPage(sort, cursor);
      if (!result?.edges?.length) {
        log(`${label} — no results on page ${page}, stopping`);
        break;
      }

      let pageCount = 0;
      for (const { node } of result.edges) {
        const project = parseNode(node);
        if (!project) continue;
        if (seenIds.has(project.project_id)) continue;
        seenIds.add(project.project_id);
        allProjects.push(project);
        sortCount++;
        pageCount++;
      }

      log(`${label} p${page}: +${pageCount} (total: ${allProjects.length})`);

      if (!result.pageInfo?.hasNextPage) break;
      cursor = result.pageInfo.endCursor;

      // Polite delay between pages
      await new Promise(r => setTimeout(r, 400));
    }

    log(`${label}: ${sortCount} new projects`);
  }

  if (allProjects.length === 0) {
    log('⚠️ No projects found — check console for API errors.');
    setTimeout(() => ui.remove(), 10000);
    return;
  }

  // ── Send to bouncer ───────────────────────────────────────────────────────
  log(`Sending ${allProjects.length} projects to bouncer...`);

  const CHUNK = 100;
  let inserted = 0;

  for (let i = 0; i < allProjects.length; i += CHUNK) {
    const chunk = allProjects.slice(i, i + CHUNK);
    try {
      const r = await fetch(`${BOUNCER}/system/kickstarter/ingest-projects`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-Ritual-Key': KEY },
        body: JSON.stringify(chunk),
      });
      const d = await r.json();
      if (d.skipped) { log('Already ingested today — skipped.'); break; }
      inserted += d.inserted || chunk.length;
    } catch (e) {
      console.error('[ks-bk] send error', e);
    }
  }

  const summary = `Done! ${inserted} Kickstarter projects ingested.`;
  log(summary);
  console.log('[ks-bk]', summary);
  setTimeout(() => ui.remove(), 12000);
})();
