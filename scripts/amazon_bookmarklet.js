/**
 * Amazon TTRPG/D&D Best-Seller Bookmarklet
 *
 * Usage: Paste the minified version into a browser bookmark URL field.
 * Click the bookmark from any tab — it will fetch all configured Amazon
 * pages automatically (no manual navigation needed) and send data to the
 * dnd-trends bouncer.
 *
 * Sends to TWO endpoints per run:
 *   1. /system/library/ingest-catalog  → catalog_supply (Gemini enrichment)
 *   2. /system/amazon/ingest-ranks     → amazon_daily_stats (rank tracking)
 */

(async function () {
  // ── Config ────────────────────────────────────────────────────────────────
  const BOUNCER = 'https://us-central1-dnd-trends-index.cloudfunctions.net/bouncer-api';
  const KEY = 'ArcaneLibrarian2026';
  const MAX_PAGES = 4;   // Amazon shows 50 items/page → up to 200 items/source
  const TODAY = new Date().toISOString().slice(0, 10);

  // Source definitions: [path, label, type]
  // type: 'bestseller' | 'movers' | 'new-releases' | 'wished'
  // All node IDs verified live in browser April 2026
  const SOURCES = [
    // ── Best Sellers ────────────────────────────────────────────────────────
    ['/Best-Sellers-Books-Dungeons-Dragons-Game/zgbs/books/16215',               'D&D Books',         'Best Sellers'],
    ['/Best-Sellers-Books-Fantasy-Gaming/zgbs/books/16211',                      'All RPG Books',     'Best Sellers'],
    ['/Best-Sellers-Toys-Games-Games-Accessories/zgbs/toys-and-games/166220011', 'Games & Accessories','Best Sellers'],
    ['/Best-Sellers-Toys-Games-Role-Playing-Dice/zgbs/toys-and-games/1265808011','RPG Dice',          'Best Sellers'],
    // ── Movers & Shakers (trend velocity) ───────────────────────────────────
    ['/gp/movers-and-shakers/books/16215',               'D&D Books',            'Movers & Shakers'],
    ['/gp/movers-and-shakers/books/16211',               'All RPG Books',        'Movers & Shakers'],
    ['/gp/movers-and-shakers/toys-and-games/166220011',  'Games & Accessories',  'Movers & Shakers'],
    // ── New Releases (launch momentum) ──────────────────────────────────────
    ['/gp/new-releases/books/16215',                     'D&D Books',            'New Releases'],
    ['/gp/new-releases/toys-and-games/166220011',        'Games & Accessories',  'New Releases'],
    // ── Most Wished For (demand/acquisition signal) ──────────────────────────
    ['/gp/most-wished-for/books/16215',                  'D&D Books',            'Most Wished For'],
  ];

  // ── UI ────────────────────────────────────────────────────────────────────
  const ui = document.createElement('div');
  ui.id = '__amz_bk_ui__';
  Object.assign(ui.style, {
    position: 'fixed', top: '12px', right: '12px', zIndex: 999999,
    background: '#1a1a2e', color: '#e0e0ff', padding: '14px 18px',
    borderRadius: '10px', fontFamily: 'monospace', fontSize: '13px',
    maxWidth: '340px', boxShadow: '0 4px 24px rgba(0,0,0,0.6)',
    lineHeight: '1.5',
  });
  const title = document.createElement('div');
  title.style.cssText = 'font-weight:bold;font-size:14px;margin-bottom:6px;color:#a78bfa';
  title.textContent = '🧙 Amazon D&D Harvest';
  const status = document.createElement('div');
  status.textContent = 'Initialising...';
  ui.appendChild(title);
  ui.appendChild(status);
  document.body.appendChild(ui);

  const log = (msg) => { status.textContent = msg; console.log('[amz-bk]', msg); };

  // ── DOM parsing ───────────────────────────────────────────────────────────
  function parseProducts(doc, label, listType) {
    const items = [];

    // Amazon renders product cards with data-asin on the grid item container.
    // Multiple layout variants exist — try the broadest selector first.
    const candidates = Array.from(doc.querySelectorAll('[data-asin]'))
      .filter(el => /^[A-Z0-9]{10}$/.test(el.dataset.asin || ''));

    for (const el of candidates) {
      const asin = el.dataset.asin;

      // Rank: badge showing "#1", "1", etc.
      const rankEl = el.querySelector(
        '.zg-bdg-text, .zg-badge-text, [class*="zg-bdg"], [class*="zg-badge"]'
      );
      const rank = rankEl
        ? parseInt(rankEl.textContent.replace(/[^0-9]/g, ''), 10) || 0
        : 0;

      // Title: several layout variants
      const titleEl = el.querySelector(
        '.p13n-sc-truncated, [class*="p13n-sc-truncated"], ' +
        '[class*="p13n-sc-css-line-clamp"], ' +
        'a[title], a.a-link-normal span.a-text-normal'
      );
      const title =
        (titleEl && (titleEl.getAttribute('title') || titleEl.textContent || '')).trim();
      if (!title) continue;

      // Price
      const priceEl = el.querySelector(
        '.p13n-sc-price, [class*="p13n-sc-price"], .a-color-price'
      );
      const priceStr = priceEl ? priceEl.textContent.trim() : '';
      const price = parseFloat(priceStr.replace(/[^0-9.]/g, '')) || 0;

      // Author / byline ("by Author Name")
      const authorEl = el.querySelector(
        'span.a-color-secondary, .a-row .a-color-base.a-size-small'
      );
      const author = authorEl
        ? authorEl.textContent.trim().replace(/^by\s+/i, '').trim()
        : '';

      // Star rating — aria-label e.g. "4.8 out of 5 stars"
      const ratingEl = el.querySelector('span[aria-label*="out of 5"]');
      const ratingMatch = ratingEl
        ? ratingEl.getAttribute('aria-label').match(/([\d.]+)\s+out of/)
        : null;
      const starRating = ratingMatch ? parseFloat(ratingMatch[1]) : 0;

      // Review count — aria-label e.g. "1,234 ratings"
      const reviewEl = el.querySelector(
        'span[aria-label*="rating"], span[aria-label*="review"]'
      );
      const reviewCount = reviewEl
        ? parseInt(reviewEl.getAttribute('aria-label').replace(/[^0-9]/g, ''), 10) || 0
        : 0;

      items.push({ asin, rank, title, price, author, starRating, reviewCount, label, listType });
    }

    // De-duplicate by ASIN (keep the one with the lowest rank)
    const seen = new Map();
    for (const item of items) {
      if (!seen.has(item.asin) || item.rank < seen.get(item.asin).rank) {
        seen.set(item.asin, item);
      }
    }
    return [...seen.values()];
  }

  // ── Fetch one page ────────────────────────────────────────────────────────
  async function fetchPage(path, pageNum) {
    const sep = path.includes('?') ? '&' : '?';
    const url = `https://www.amazon.com${path}${pageNum > 1 ? sep + 'pg=' + pageNum : ''}`;
    try {
      const resp = await fetch(url, { credentials: 'include' });
      if (!resp.ok) return null;
      const html = await resp.text();
      return new DOMParser().parseFromString(html, 'text/html');
    } catch {
      return null;
    }
  }

  // ── Main harvest loop ─────────────────────────────────────────────────────
  const allProducts = [];   // for ingest-catalog
  const rankRows = [];      // for ingest-ranks

  let totalFetched = 0;

  for (const [path, label, listType] of SOURCES) {
    log(`Fetching ${listType}: ${label}…`);

    // Fetch up to MAX_PAGES pages for this source in parallel
    const pageDocs = await Promise.all(
      Array.from({ length: MAX_PAGES }, (_, i) => fetchPage(path, i + 1))
    );

    let sourceCount = 0;
    const seenAsins = new Set();

    for (const doc of pageDocs) {
      if (!doc) continue;
      const items = parseProducts(doc, label, listType);
      for (const item of items) {
        if (seenAsins.has(item.asin)) continue;
        seenAsins.add(item.asin);
        sourceCount++;

        // catalog_supply row
        const rankTier =
          item.rank <= 10  ? 'Top 10'  :
          item.rank <= 50  ? 'Top 50'  :
          item.rank <= 100 ? 'Top 100' : 'Top 200';

        allProducts.push({
          collected_date: TODAY,
          source: 'Amazon',
          title: item.title,
          publisher: item.author || '',
          seller_tier: rankTier,
          price: item.price,
          rating: item.starRating,
          tags: ['Amazon', label, listType, 'V10-auto'],
          system_tag: '',
          edition_tag: '',
          asin: item.asin,
        });

        // amazon_daily_stats row
        rankRows.push({
          asin: item.asin,
          rank: item.rank,
          price_cents: Math.round(item.price * 100),
          date: TODAY,
          title: item.title,
          category: `${listType}: ${label}`,
          author: item.author,
          rating: item.starRating,
          review_count: item.reviewCount,
        });
      }
    }

    totalFetched += sourceCount;
    log(`${listType}: ${label} → ${sourceCount} products (total: ${totalFetched})`);
  }

  if (allProducts.length === 0) {
    log('⚠️ No products found. Amazon layout may have changed.');
    setTimeout(() => ui.remove(), 8000);
    return;
  }

  // ── Send to bouncer ────────────────────────────────────────────────────────
  log(`Sending ${allProducts.length} products to bouncer…`);

  const CHUNK = 500;
  let catalogOk = 0, ranksOk = 0;

  for (let i = 0; i < allProducts.length; i += CHUNK) {
    const chunk = allProducts.slice(i, i + CHUNK);
    try {
      const r = await fetch(`${BOUNCER}/system/library/ingest-catalog`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-Ritual-Key': KEY },
        body: JSON.stringify(chunk),
      });
      const d = await r.json();
      catalogOk += d.inserted || chunk.length;
    } catch (e) {
      console.error('[amz-bk] catalog chunk failed', e);
    }
  }

  for (let i = 0; i < rankRows.length; i += CHUNK) {
    const chunk = rankRows.slice(i, i + CHUNK);
    try {
      const r = await fetch(`${BOUNCER}/system/amazon/ingest-ranks`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-Ritual-Key': KEY },
        body: JSON.stringify(chunk),
      });
      const d = await r.json();
      ranksOk += d.inserted || chunk.length;
    } catch (e) {
      console.error('[amz-bk] ranks chunk failed', e);
    }
  }

  // ── Done ──────────────────────────────────────────────────────────────────
  const summary = `✅ Done! ${catalogOk} catalog rows · ${ranksOk} rank rows`;
  log(summary);
  console.log('[amz-bk]', summary);

  setTimeout(() => ui.remove(), 12000);
})();
