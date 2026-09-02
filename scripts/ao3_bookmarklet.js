/**
 * AO3 D&D-Crossover-Count Bookmarklet
 * (Stage 4 of the community_reception multi-source composite, Apr 27, 2026)
 *
 * Usage:
 *   1. From the install page, install the bookmarklet to your bookmark bar.
 *   2. Click an AO3 deep-link from the install page (it navigates to the
 *      D&D x [IP] search URL with an &_arcane_ip=... marker so the
 *      bookmarklet knows which seed-list IP this search is for).
 *   3. After the AO3 page loads, click the bookmarklet.
 *   4. Bookmarklet reads the visible work count from the DOM, shows a
 *      confirm modal ("save 4,532 works for The Lord of the Rings?"),
 *      POSTs to the bouncer.
 *
 * Ethics: this is human-wielded UI tooling, NOT an automated scraper.
 * Phil clicks a link, AO3 renders the page in his browser like for any
 * human user; the bookmarklet just reads the DOM count he can see and
 * sends it home. No fetch() to AO3, no auto-iteration.
 *
 * AO3 work count selectors (order of preference):
 *   - <h2 class="heading">N,NNN Works in <tag></h2>     (tag pages)
 *   - <h3 class="heading">N,NNN Found</h3>              (search results)
 *   - <h2 class="heading"><a>tag</a> 0 results</h2>     (zero-results variant)
 */

(async function () {
  const BOUNCER = 'https://us-central1-dnd-trends-index.cloudfunctions.net/bouncer-api';
  const KEY = 'ArcaneLibrarian2026';
  const PLATFORM = 'ao3';

  // ── UI ──────────────────────────────────────────────────────────────────
  const ui = document.createElement('div');
  ui.id = '__ao3_bk_ui__';
  Object.assign(ui.style, {
    position: 'fixed', top: '12px', right: '12px', zIndex: 999999,
    background: '#1a1a2e', color: '#e0e0ff', padding: '14px 18px',
    borderRadius: '10px', fontFamily: 'monospace', fontSize: '13px',
    maxWidth: '380px', boxShadow: '0 4px 24px rgba(0,0,0,0.6)',
    lineHeight: '1.5',
  });
  const titleEl = document.createElement('div');
  titleEl.style.cssText = 'font-weight:bold;font-size:14px;margin-bottom:6px;color:#e87722';
  titleEl.textContent = '📚 AO3 Crossover Capture';
  const statusEl = document.createElement('div');
  statusEl.textContent = 'Reading page...';
  ui.appendChild(titleEl);
  ui.appendChild(statusEl);
  document.body.appendChild(ui);

  const log = (msg) => { statusEl.innerHTML = msg; console.log('[ao3-bk]', msg.replace(/<[^>]+>/g, '')); };
  const close = (delay) => setTimeout(() => ui.remove(), delay || 8000);

  // ── Sanity: must be on AO3 ──────────────────────────────────────────────
  if (!location.hostname.endsWith('archiveofourown.org')) {
    log('⚠️ Not on AO3 — open an AO3 search URL first.');
    close();
    return;
  }

  // ── Extract IP from URL marker (_arcane_ip=...) or fall back to prompt ──
  const params = new URLSearchParams(location.search);
  let ip_name = params.get('_arcane_ip') || '';
  if (!ip_name) {
    ip_name = (prompt(
      'Which seed-list IP is this AO3 search for?\n' +
      '(Type the exact IP name from the seed list, e.g. "The Lord of the Rings")'
    ) || '').trim();
  }
  if (!ip_name) {
    log('⚠️ No IP specified — aborting.');
    close();
    return;
  }

  // ── Extract work count from DOM ─────────────────────────────────────────
  // AO3 has multiple H2 formats depending on result count + pagination:
  //   "1 - 20 of 26 Works in <tag>"    (paginated — we want 26, not 1)
  //   "26 Works in <tag>"              (single page — we want 26)
  //   "1,234 Found"                    (search results endpoint)
  //   "No works found"                 (zero results variant)
  //
  // Pattern A handles the paginated form first because it's the most
  // common (most filtered queries return >20 results); pattern B handles
  // the single-page case; patterns C/D handle edge cases.
  function extractWorkCount() {
    // Pattern A: paginated — "1 - 20 of 26 Works in <tag>" — capture 26
    for (const h of document.querySelectorAll('h2.heading, h3.heading')) {
      const text = h.textContent;
      const m = text.match(/of\s+([\d,]+)\s+Works?\s+in\b/i);
      if (m) return parseInt(m[1].replace(/,/g, ''), 10);
    }
    // Pattern B: single-page — "26 Works in <tag>" (no "of X")
    for (const h of document.querySelectorAll('h2.heading, h3.heading')) {
      const text = h.textContent.trim();
      // Must look like "N Works in <tag>" — anchored at start to avoid
      // false-matches on the paginated form
      const m = text.match(/^([\d,]+)\s+Works?\s+in\b/i);
      if (m) return parseInt(m[1].replace(/,/g, ''), 10);
    }
    // Pattern C: search results endpoint — "1,234 Found"
    for (const h of document.querySelectorAll('h2.heading, h3.heading')) {
      const m = h.textContent.match(/^([\d,]+)\s+Found\b/);
      if (m) return parseInt(m[1].replace(/,/g, ''), 10);
    }
    // Pattern D: zero-results variants
    for (const h of document.querySelectorAll('h2.heading, h3.heading')) {
      if (/0\s+(works|found)/i.test(h.textContent)) return 0;
      if (/no works found/i.test(h.textContent)) return 0;
    }
    // Pattern E: pagination element fallback — "of N" anywhere
    const pag = document.querySelector('ol.pagination li.next');
    if (pag) {
      const total = pag.textContent.match(/of\s+([\d,]+)/i);
      if (total) return parseInt(total[1].replace(/,/g, ''), 10);
    }
    return null;
  }

  const work_count = extractWorkCount();
  if (work_count === null) {
    log('⚠️ Could not find work count on this page.<br>Make sure the AO3 search results page is loaded with a count visible (e.g. "4,532 Works in...").');
    close(12000);
    return;
  }

  // ── Extract canonical tag(s) from the page if available ─────────────────
  // AO3 search results page H2 includes the primary tag name; capture it
  // so the audit trail records what AO3 actually showed.
  let canonical = '';
  const h2 = document.querySelector('h2.heading');
  if (h2) {
    // AO3's H2 sometimes mashes "Navigation and Actions" or "Sort and
    // Filter" UI text into the same textContent — strip those before
    // capturing the tag name.
    const m = h2.textContent.match(/Works in\s+(.+?)(?:Navigation and Actions|Sort and Filter|$)/i);
    if (m) canonical = m[1].trim();
  }
  // Filter sidebar 'Other tags' (when present) gives us the SECONDARY tag
  // — append it so the audit trail shows the IP-side tag too.
  const otherTagsParam = params.get('work_search[other_tag_names]');
  if (otherTagsParam) {
    canonical = canonical
      ? `${canonical} + ${decodeURIComponent(otherTagsParam)}`
      : decodeURIComponent(otherTagsParam);
  }

  // ── Sanity guard 1: the IP filter must actually be present ──────────────
  // `work_search[other_tag_names]` IS the D&D × IP filter — it is what makes
  // this a crossover query rather than "every D&D crossover work on AO3".
  // AO3 silently ignores a missing/malformed filter and returns the site-wide
  // set, with no visible error, so an unfiltered page looks exactly like a
  // filtered one.
  //
  // This is not hypothetical: on Sep 1, 2026 a capture from a hand-navigated
  // D&D tag page (other_tag_names empty) stored 10,886 as if it were one IP's
  // crossover count — the site-wide D&D total. Guard 2 below missed it because
  // 10,886 sits under the 100k threshold.
  //
  // Deliberately strict: refuse whenever the filter is absent, at any count.
  // A false refusal costs one re-click; a false accept silently poisons the
  // composite with a number ~1000x the real scale. Capture URLs from
  // `scripts/print_fanfic_capture_urls.py` always carry this param.
  const ipFilter = (params.get('work_search[other_tag_names]') || '').trim();
  if (!ipFilter) {
    log(
      '⚠️ No IP filter on this search — nothing saved.<br>' +
      `This page shows <b>${work_count.toLocaleString()}</b> works, which is ` +
      'every D&D crossover on AO3, not a D&D × IP count.<br>' +
      'AO3 drops a missing filter silently, so the page looks normal.<br>' +
      'Use a deep-link from <code>print_fanfic_capture_urls.py</code> ' +
      '(it carries <code>other_tag_names</code> + <code>_arcane_ip</code>).'
    );
    close(20000);
    return;
  }

  // ── Sanity guard 2: refuse implausibly high counts ──────────────────────
  // Kept as a backstop for the case where a filter IS present but AO3 still
  // returned something site-wide-shaped. A real D&D × IP crossover count is
  // in the hundreds-to-low-thousands range.
  if (!canonical && work_count > 100000) {
    log(
      '⚠️ Likely unfiltered AO3 search.<br>' +
      `Work count ${work_count.toLocaleString()} is too high for a real ` +
      'D&D x IP crossover (expected hundreds–thousands).<br>' +
      'No canonical tag detected on page header — most likely the URL ' +
      'is missing the tag filter. Check the URL and re-run.'
    );
    close(15000);
    return;
  }

  // ── Confirm modal ───────────────────────────────────────────────────────
  const confirmed = confirm(
    `Save AO3 crossover count?\n\n` +
    `IP: ${ip_name}\n` +
    `Platform: AO3\n` +
    `Work count: ${work_count.toLocaleString()}\n` +
    `Canonical tag: ${canonical || '(not detected)'}\n\n` +
    `OK to save, Cancel to abort.`
  );
  if (!confirmed) {
    log('Cancelled.');
    close(3000);
    return;
  }

  // ── POST to bouncer ─────────────────────────────────────────────────────
  log(`Saving ${work_count.toLocaleString()} works for "${ip_name}"...`);
  try {
    const resp = await fetch(`${BOUNCER}/system/fanfic/ingest-crossover-count`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-Ritual-Key': KEY },
      body: JSON.stringify({
        ip_name,
        platform: PLATFORM,
        platform_canonical: canonical,
        work_count,
        source_url: location.href,
        scraped_by: 'ao3_bookmarklet',
      }),
    });
    const data = await resp.json();
    if (resp.ok && data.inserted) {
      log(`✅ Saved! ${ip_name}: ${work_count.toLocaleString()} works on AO3.`);
    } else {
      log(`⚠️ Bouncer error: ${data.error || resp.status}`);
    }
  } catch (e) {
    log(`⚠️ Network error: ${e.message}`);
    console.error('[ao3-bk] failed:', e);
  }
  close(8000);
})();
