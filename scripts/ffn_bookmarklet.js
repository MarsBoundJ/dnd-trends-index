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

  // ── Extract story count from DOM ────────────────────────────────────────
  // FFN's crossover-page count is harder to extract than AO3's because the
  // page rarely shows an explicit total count. Strategy:
  //   1. Look for "Page 1 of N" pagination at top → multiply N by ~25
  //   2. If page 1 has fewer than 25 stories AND there's no "next" link,
  //      use the actual visible count
  //   3. Fall back to manual entry if we can't detect anything
  function extractStoryCount() {
    // Pattern 1: pagination — "Page 1 of N" (N is the last-page number)
    const centerEls = document.querySelectorAll('center');
    for (const c of centerEls) {
      const text = c.textContent;
      // "Page 1 of 47" — capture the last page number
      const m = text.match(/Page\s+\d+\s+of\s+(\d+)/i);
      if (m) {
        const totalPages = parseInt(m[1], 10);
        // Count stories on current page (page 1 typically full at 25)
        const onCurrentPage = document.querySelectorAll(
          'div.z-list.zhover.zpointer, div.z-list'
        ).length;
        // FFN paginates ~25 stories per page; the last page may have fewer
        // so this is an approximate upper bound.
        if (totalPages === 1) return onCurrentPage;
        // Estimate: (totalPages - 1) full pages of 25 + current page count
        // (assumes we are on page 1 and current page is full)
        return (totalPages - 1) * 25 + onCurrentPage;
      }
    }
    // Pattern 2: no pagination = single page; count visible stories
    const stories = document.querySelectorAll(
      'div.z-list.zhover.zpointer, div.z-list'
    );
    if (stories.length > 0) return stories.length;
    return null;
  }

  let work_count = extractStoryCount();
  if (work_count === null) {
    const manual = prompt(
      'Could not auto-detect story count.\n\n' +
      'Look at the page (pagination at bottom often shows "Page 1 of N").\n' +
      'Enter the number of stories manually:'
    );
    if (!manual) {
      log('Cancelled.');
      close(3000);
      return;
    }
    work_count = parseInt(manual.replace(/[^\d]/g, ''), 10);
    if (!work_count) {
      log('⚠️ Invalid count.');
      close(3000);
      return;
    }
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
