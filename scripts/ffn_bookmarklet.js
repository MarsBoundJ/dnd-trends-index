/**
 * FFN D&D-Crossover-Count Bookmarklet
 * (Stage 4 of the community_reception multi-source composite, Apr 27, 2026)
 *
 * Usage:
 *   1. From the install page, install the bookmarklet to your bookmark bar.
 *   2. Click an FFN deep-link from the install page (it navigates to the
 *      D&D x [IP] crossover page with an &_arcane_ip=... marker).
 *      URL pattern:
 *        /[Fandom1]-and-[Fandom2]-Crossovers/[ID1]/[ID2]/?_arcane_ip=...
 *   3. After the FFN page loads, click the bookmarklet.
 *   4. Bookmarklet extracts the story count from the DOM, shows confirm
 *      modal, POSTs to bouncer.
 *
 * Ethics: same as AO3 bookmarklet — human-wielded UI tool, not a bot.
 *
 * FFN crossover-page count selectors (FFN markup is older + less consistent):
 *   - Pagination element shows "Page 1 of N" with N pages, ~25 stories/page
 *   - Sometimes "X Stories" near the top (in some layouts)
 *   - The reliable pattern: count visible <div class="z-list zhover zpointer">
 *     story rows on page 1 + pages_count × 25 (approx)
 *   - For an exact count, FFN doesn't always expose one. We accept N×25
 *     as approximate when the exact count isn't visible.
 */

(async function () {
  const BOUNCER = 'https://us-central1-dnd-trends-index.cloudfunctions.net/bouncer-api';
  const KEY = 'ArcaneLibrarian2026';
  const PLATFORM = 'ffn';

  const ui = document.createElement('div');
  Object.assign(ui.style, {
    position: 'fixed', top: '12px', right: '12px', zIndex: 999999,
    background: '#0d0d1a', color: '#e0e0ff', padding: '14px 18px',
    borderRadius: '10px', fontFamily: 'monospace', fontSize: '13px',
    maxWidth: '380px', boxShadow: '0 4px 24px rgba(0,0,0,0.7)',
    lineHeight: '1.5',
  });
  const titleEl = document.createElement('div');
  titleEl.style.cssText = 'font-weight:bold;font-size:14px;margin-bottom:6px;color:#3b82f6';
  titleEl.textContent = '📖 FFN Crossover Capture';
  const statusEl = document.createElement('div');
  statusEl.textContent = 'Reading page...';
  ui.appendChild(titleEl);
  ui.appendChild(statusEl);
  document.body.appendChild(ui);

  const log = (msg) => { statusEl.innerHTML = msg; console.log('[ffn-bk]', msg.replace(/<[^>]+>/g, '')); };
  const close = (delay) => setTimeout(() => ui.remove(), delay || 8000);

  // ── Sanity: must be on FFN ──────────────────────────────────────────────
  if (!location.hostname.endsWith('fanfiction.net')) {
    log('⚠️ Not on FFN — open an FFN crossover URL first.');
    close();
    return;
  }

  // ── Extract IP from URL marker or prompt ────────────────────────────────
  const params = new URLSearchParams(location.search);
  let ip_name = params.get('_arcane_ip') || '';
  if (!ip_name) {
    ip_name = (prompt(
      'Which seed-list IP is this FFN crossover for?\n' +
      '(Type the exact IP name from the seed list, e.g. "The Lord of the Rings")'
    ) || '').trim();
  }
  if (!ip_name) {
    log('⚠️ No IP specified — aborting.');
    close();
    return;
  }

  // ── Extract story count from DOM (best-effort suggestion only) ──────────
  // FFN doesn't expose an authoritative total count in a stable place,
  // and historical "25 stories per page" assumptions broke (Phil's HP ×
  // D&D test on Apr 28 showed 35 on page 1, undermining the math).
  //
  // Strategy: try a few patterns to SUGGEST a count, but ALWAYS prompt
  // the user to verify against the actual page. Manual confirmation is
  // more reliable than fragile DOM scraping for FFN.
  function suggestStoryCount() {
    // Pattern A: explicit "X-Y of Z" or "of Z" pattern (some FFN pages)
    for (const c of document.querySelectorAll('center, .pagination, body')) {
      const text = c.textContent;
      const m = text.match(/of\s+([\d,]+)\b/i);
      if (m) {
        const n = parseInt(m[1].replace(/,/g, ''), 10);
        if (n > 0 && n < 1000000) return { count: n, via: 'matched "of N" in page text' };
      }
    }
    // Pattern B: count visible story rows (last-page case, or single-page result)
    const stories = document.querySelectorAll(
      'div.z-list.zhover.zpointer, div.z-list, div[data-storyid]'
    );
    if (stories.length > 0) {
      return { count: stories.length, via: `counted ${stories.length} story rows on this page (may not be total)` };
    }
    return null;
  }

  const suggestion = suggestStoryCount();
  const promptText = suggestion
    ? `Total stories for ${ip_name} × D&D on FFN?\n\n` +
      `Suggested: ${suggestion.count} (${suggestion.via})\n\n` +
      `Look at the page (bottom usually shows "Page X of Y" or similar).\n` +
      `Press OK to accept ${suggestion.count}, or type the correct number:`
    : `Total stories for ${ip_name} × D&D on FFN?\n\n` +
      `Could not auto-detect from the page DOM.\n` +
      `Look at the page (bottom usually shows "Page X of Y" or similar).\n` +
      `Enter the total story count:`;

  const userInput = prompt(
    promptText,
    suggestion ? String(suggestion.count) : ''
  );
  if (userInput === null) {
    log('Cancelled.');
    close(3000);
    return;
  }
  const work_count = parseInt(String(userInput).replace(/[^\d]/g, ''), 10);
  if (!work_count && work_count !== 0) {
    log('⚠️ Invalid count.');
    close(3000);
    return;
  }

  // ── Extract canonical fandom-pair from URL path ─────────────────────────
  // /[Fandom1]-and-[Fandom2]-Crossovers/[ID1]/[ID2]/
  let canonical = '';
  const pathMatch = location.pathname.match(
    /^\/([^/]+)-and-([^/]+)-Crossovers\/(\d+)\/(\d+)\/?/
  );
  if (pathMatch) {
    canonical = `${pathMatch[1].replace(/-/g, ' ')} x ${pathMatch[2].replace(/-/g, ' ')} (FFN ids ${pathMatch[3]}/${pathMatch[4]})`;
  } else {
    canonical = location.pathname;
  }

  // ── Confirm modal ───────────────────────────────────────────────────────
  const confirmed = confirm(
    `Save FFN crossover count?\n\n` +
    `IP: ${ip_name}\n` +
    `Platform: FFN\n` +
    `Story count: ${work_count.toLocaleString()} (approx if from pagination)\n` +
    `Canonical: ${canonical}\n\n` +
    `OK to save, Cancel to abort.`
  );
  if (!confirmed) {
    log('Cancelled.');
    close(3000);
    return;
  }

  // ── POST to bouncer ─────────────────────────────────────────────────────
  log(`Saving ${work_count.toLocaleString()} stories for "${ip_name}"...`);
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
        scraped_by: 'ffn_bookmarklet',
      }),
    });
    const data = await resp.json();
    if (resp.ok && data.inserted) {
      log(`✅ Saved! ${ip_name}: ${work_count.toLocaleString()} stories on FFN.`);
    } else {
      log(`⚠️ Bouncer error: ${data.error || resp.status}`);
    }
  } catch (e) {
    log(`⚠️ Network error: ${e.message}`);
    console.error('[ffn-bk] failed:', e);
  }
  close(8000);
})();
