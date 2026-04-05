/**
 * Kickstarter TTRPG/D&D Bookmarklet
 *
 * Usage: Paste the minified version into a browser bookmark URL field.
 * Click the bookmark while on any Kickstarter page — it will fetch the
 * Tabletop Games category (live + recently launched projects) and send
 * data to the dnd-trends bouncer.
 *
 * Sends to ONE endpoint per run:
 *   POST /system/kickstarter/ingest-projects → kickstarter_projects table
 *
 * Kickstarter uses Cloudflare on their API but the browse/search pages
 * are rendered server-side and accessible from a real browser session.
 * The bookmarklet fetches pages with credentials:include so Cloudflare
 * sees a legitimate logged-in browser request.
 *
 * Fields collected per project:
 *   project_id, name, creator, backers_count, pledged_usd, goal_usd,
 *   percent_funded, category, status, end_date, is_dnd_centric, blurb, url
 */

(async function () {
  // ── Config ────────────────────────────────────────────────────────────────
  const BOUNCER = 'https://us-central1-dnd-trends-index.cloudfunctions.net/bouncer-api';
  const KEY = 'ArcaneLibrarian2026';
  const MAX_PAGES = 5; // ~20 items/page → up to 100 projects

  // Kickstarter category pages to scrape
  // All verified as tabletop/RPG-relevant categories
  const CATEGORIES = [
    { url: 'https://www.kickstarter.com/discover/categories/tabletop%20games/tabletop%20games?sort=magic&seed=2694958', label: 'Tabletop Games (Popular)' },
    { url: 'https://www.kickstarter.com/discover/categories/tabletop%20games/tabletop%20games?sort=newest', label: 'Tabletop Games (New)' },
    { url: 'https://www.kickstarter.com/discover/categories/tabletop%20games/tabletop%20games?sort=end_date', label: 'Tabletop Games (Ending Soon)' },
  ];

  // Keywords that indicate a D&D / TTRPG project
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
  const title = document.createElement('div');
  title.style.cssText = 'font-weight:bold;font-size:14px;margin-bottom:6px;color:#f97316';
  title.textContent = '🎲 Kickstarter D&D Harvest';
  const status = document.createElement('div');
  status.textContent = 'Initialising...';
  ui.appendChild(title);
  ui.appendChild(status);
  document.body.appendChild(ui);

  const log = (msg) => { status.textContent = msg; console.log('[ks-bk]', msg); };

  // ── Helpers ───────────────────────────────────────────────────────────────

  function isDndCentric(name, blurb) {
    const text = ((name || '') + ' ' + (blurb || '')).toLowerCase();
    return DND_KEYWORDS.some(kw => text.includes(kw));
  }

  // Extract project_id from Kickstarter URL
  // e.g. /projects/creator/project-name → hash the path as a stable id
  // Kickstarter uses numeric IDs internally — visible in og:url meta tags
  function extractProjectId(doc) {
    const ogUrl = doc.querySelector('meta[property="og:url"]');
    if (ogUrl) {
      const m = ogUrl.content.match(/\/projects\/(\d+)\//);
      if (m) return parseInt(m[1], 10);
    }
    return null;
  }

  // Parse a single project card from the discover/category listing page
  function parseProjectCard(card) {
    try {
      // Project link and ID
      const linkEl = card.querySelector('a[href*="/projects/"]');
      if (!linkEl) return null;
      const href = linkEl.getAttribute('href');
      const url = href.startsWith('http') ? href : 'https://www.kickstarter.com' + href;

      // Extract numeric project ID from URL if present
      // e.g. https://www.kickstarter.com/projects/creator/slug
      // Kickstarter's discovery page encodes data in data-* attributes or JSON
      const idMatch = url.match(/kickstarter\.com\/projects\/([^/]+)\/([^/?]+)/);
      // Use a hash of creator/slug as stable integer ID when numeric ID unavailable
      const creatorSlug = idMatch ? (idMatch[1] + '/' + idMatch[2]) : url;
      const project_id = Math.abs(
        creatorSlug.split('').reduce((h, c) => ((h << 5) - h + c.charCodeAt(0)) | 0, 0)
      );

      // Title
      const nameEl = card.querySelector('h2, h3, [data-test-id="project-name"], .project-name');
      const name = nameEl ? nameEl.textContent.trim() : '';
      if (!name) return null;

      // Creator
      const creatorEl = card.querySelector(
        '[data-test-id="project-creator"], .creator-name, a[href*="/profile/"]'
      );
      const creator = creatorEl ? creatorEl.textContent.trim().replace(/^by\s+/i, '') : '';

      // Blurb / description
      const blurbEl = card.querySelector(
        'p.project-description, [data-test-id="project-blurb"], .short-blurb, p'
      );
      const blurb = blurbEl ? blurbEl.textContent.trim().slice(0, 300) : '';

      // Funding progress — Kickstarter exposes these in data attrs or text
      const pledgedEl = card.querySelector(
        '[data-test-id="pledged-amount"], .pledged .amount, .money.usd'
      );
      const pledgedStr = pledgedEl ? pledgedEl.textContent.replace(/[^0-9.]/g, '') : '0';
      const pledged_usd = parseFloat(pledgedStr) || 0;

      const goalEl = card.querySelector(
        '[data-test-id="goal"], .goal .amount'
      );
      const goalStr = goalEl ? goalEl.textContent.replace(/[^0-9.]/g, '') : '0';
      const goal_usd = parseFloat(goalStr) || 0;

      const percent_funded = goal_usd > 0 ? Math.round((pledged_usd / goal_usd) * 100) : 0;

      // Backers count
      const backersEl = card.querySelector(
        '[data-test-id="backers-count"], .backers-count .num, .num-backers'
      );
      const backers_count = backersEl
        ? parseInt(backersEl.textContent.replace(/[^0-9]/g, ''), 10) || 0
        : 0;

      // End date
      const endEl = card.querySelector(
        '[data-test-id="deadline"], time[datetime], .deadline'
      );
      const end_date = endEl
        ? (endEl.getAttribute('datetime') || endEl.textContent.trim())
        : null;

      // Status: live, ended, successful, etc.
      const stateEl = card.querySelector('[data-test-id="project-state"], .state');
      const status_text = stateEl ? stateEl.textContent.trim().toLowerCase() : 'live';

      return {
        project_id,
        name,
        creator,
        backers_count,
        pledged_usd,
        goal_usd,
        percent_funded,
        category: 'Tabletop Games',
        status: status_text || 'live',
        end_date: end_date || null,
        is_dnd_centric: isDndCentric(name, blurb),
        blurb,
        url,
      };
    } catch (e) {
      console.warn('[ks-bk] card parse error', e);
      return null;
    }
  }

  // ── Fetch one page ────────────────────────────────────────────────────────
  async function fetchPage(baseUrl, page) {
    const sep = baseUrl.includes('?') ? '&' : '?';
    const url = page > 1 ? `${baseUrl}${sep}page=${page}` : baseUrl;
    try {
      const resp = await fetch(url, {
        credentials: 'include',
        headers: { 'Accept': 'text/html', 'X-Requested-With': '' },
      });
      if (!resp.ok) { console.warn('[ks-bk] HTTP', resp.status, url); return null; }
      const html = await resp.text();
      return new DOMParser().parseFromString(html, 'text/html');
    } catch (e) {
      console.warn('[ks-bk] fetch error', url, e);
      return null;
    }
  }

  // ── Main harvest loop ─────────────────────────────────────────────────────
  const allProjects = [];
  const seenIds = new Set();

  for (const { url: catUrl, label } of CATEGORIES) {
    log(`Fetching: ${label}...`);
    let catCount = 0;

    for (let page = 1; page <= MAX_PAGES; page++) {
      const doc = await fetchPage(catUrl, page);
      if (!doc) break;

      // Kickstarter renders project cards with various selectors across layouts
      const cards = doc.querySelectorAll(
        '[data-test-id="project-card"], ' +
        '.js-react-proj-card, ' +
        'article.project-card, ' +
        '.project-thumbnail, ' +
        'li[data-pid]'
      );

      if (cards.length === 0) {
        log(`${label} — no cards on page ${page}, stopping`);
        break;
      }

      let pageCount = 0;
      for (const card of cards) {
        const project = parseProjectCard(card);
        if (!project) continue;
        if (seenIds.has(project.project_id)) continue;
        seenIds.add(project.project_id);
        allProjects.push(project);
        catCount++;
        pageCount++;
      }

      log(`${label} p${page}: +${pageCount} (total: ${allProjects.length})`);
      if (pageCount === 0) break;

      // Polite delay between pages
      await new Promise(r => setTimeout(r, 600));
    }

    log(`${label}: ${catCount} projects harvested`);
  }

  if (allProjects.length === 0) {
    log('⚠️ No projects found. Kickstarter layout may have changed — check console.');
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
