/**
 * GitP UA Audit one-shot bookmarklet — Trusight A1 LIVE
 *
 * Run on any page of the GitP thread:
 *   https://forums.giantitp.com/showthread.php?717528-UA-Villainous-Options-2
 *
 * What it does:
 *   1. Walks all pages of the thread (vBulletin paginates 25 posts/page).
 *   2. Same-origin fetch() each page using Phil's authenticated session
 *      (bypasses Cloudflare, which blocks anonymous Playwright/curl).
 *   3. Parses each page's DOM, extracts: post number, author, date, body text
 *      (quotes stripped via .bbcode_container removal).
 *   4. Downloads a single JSON file to Phil's Downloads folder.
 *
 * No backend dependency — paste the JSON back to Claude for classification.
 *
 * vBulletin selectors (GitP runs vBulletin 4):
 *   Post container:  li.postbitlegacy, li.postcontainer
 *   Author:          .username (or strong inside)
 *   Date:            .postdate, .date
 *   Body:            div[id^="post_message_"]   (inside is blockquote.postcontent)
 *   Quoted-block:    .bbcode_container (strip before extracting body text)
 *
 * Pagination: vBulletin links contain "/page<N>" suffix on the thread URL.
 */

(async function () {
  const FETCH_DELAY_MS = 1200;
  const FETCH_TIMEOUT_MS = 30000;

  if (!location.hostname.endsWith('forums.giantitp.com')) {
    alert('Open the GitP thread first, then click the bookmarklet.');
    return;
  }

  // ── UI panel ─────────────────────────────────────────────────────────
  const oldPanel = document.getElementById('__trusight_gitp_panel__');
  if (oldPanel) oldPanel.remove();
  const panel = document.createElement('div');
  panel.id = '__trusight_gitp_panel__';
  Object.assign(panel.style, {
    position: 'fixed', top: '12px', right: '12px', zIndex: '999999',
    background: '#0a0a18', color: '#e0e0ff',
    padding: '14px 16px', borderRadius: '12px',
    fontFamily: '-apple-system, BlinkMacSystemFont, sans-serif',
    fontSize: '13px', lineHeight: '1.5',
    width: '440px', maxHeight: '85vh', overflowY: 'auto',
    boxShadow: '0 8px 32px rgba(0,0,0,0.7)',
    border: '1px solid #e87722',
  });
  panel.innerHTML = `
    <div style="font-weight:700;font-size:14px;color:#e87722;margin-bottom:8px;">
      Trusight A1 — GitP UA Audit
    </div>
    <div id="__ts_status" style="padding:8px;background:#1a1a2e;border-radius:6px;font-size:12px;">
      Starting...
    </div>
  `;
  document.body.appendChild(panel);
  const status = (msg) => { document.getElementById('__ts_status').textContent = msg; };

  // ── Find thread URL pattern ─────────────────────────────────────────
  // GitP thread URLs: showthread.php?717528-UA-Villainous-Options-2[/pageN]
  const m = location.search.match(/^\?(\d+)-([^&]+)/);
  if (!m) {
    alert('Run this on a GitP showthread.php URL (e.g., /showthread.php?717528-UA-...)');
    panel.remove();
    return;
  }
  const threadId = m[1];
  const threadSlug = m[2];
  const baseThreadUrl = `${location.origin}/showthread.php?${threadId}-${threadSlug}`;

  // ── Determine total page count from current page ────────────────────
  // vBulletin pagination links live in .pagenav or anchors with /pageN/ in href.
  let maxPage = 1;
  document.querySelectorAll('a[href*="/page"]').forEach(a => {
    const pm = a.href.match(/\/page(\d+)/);
    if (pm) {
      const p = parseInt(pm[1], 10);
      if (p > maxPage) maxPage = p;
    }
  });
  status(`Thread ${threadId}: ${maxPage} page(s) detected. Fetching...`);

  // ── Helper: fetch with timeout ──────────────────────────────────────
  async function fetchWithTimeout(url, timeoutMs) {
    const ctrl = new AbortController();
    const t = setTimeout(() => ctrl.abort(), timeoutMs);
    try {
      const res = await fetch(url, { credentials: 'include', signal: ctrl.signal });
      const text = await res.text();
      return { ok: res.ok, status: res.status, text };
    } finally { clearTimeout(t); }
  }

  // ── Helper: extract posts from a parsed document ────────────────────
  function extractPosts(doc) {
    const posts = [];
    // Try the modern selector first; fall back to legacy.
    let nodes = doc.querySelectorAll('li.postbitlegacy, li.postcontainer');
    if (nodes.length === 0) {
      // Older vB: <table id="post12345">
      nodes = doc.querySelectorAll('table[id^="post"]');
    }
    nodes.forEach((post) => {
      const idAttr = post.id || '';
      const postNum = idAttr.replace(/\D/g, '');

      // Author
      let author = '?';
      const userEl = post.querySelector('.username strong, .username, a.username');
      if (userEl) author = userEl.textContent.trim();

      // Date
      let date = '';
      const dateEl = post.querySelector('.date, .postdate');
      if (dateEl) date = dateEl.textContent.trim().replace(/\s+/g, ' ');

      // Body — div[id^="post_message_"] holds the full post HTML
      const bodyContainer = post.querySelector('div[id^="post_message_"], blockquote.postcontent');
      let body = '';
      if (bodyContainer) {
        const clone = bodyContainer.cloneNode(true);
        // Strip quoted-blocks
        clone.querySelectorAll('.bbcode_container, .bbcode_quote, .quote_container').forEach(q => q.remove());
        body = clone.textContent.replace(/\s+/g, ' ').trim();
      }

      if (postNum && body) {
        posts.push({ post_num: postNum, author, date, body, body_chars: body.length });
      }
    });
    return posts;
  }

  // ── Walk all pages ──────────────────────────────────────────────────
  const allPosts = [];
  for (let p = 1; p <= maxPage; p++) {
    const url = p === 1 ? baseThreadUrl : `${baseThreadUrl}/page${p}`;
    status(`Page ${p}/${maxPage}: fetching...`);
    try {
      const { ok, status: code, text } = await fetchWithTimeout(url, FETCH_TIMEOUT_MS);
      if (!ok) {
        console.error(`Page ${p} HTTP ${code}`);
        continue;
      }
      const doc = new DOMParser().parseFromString(text, 'text/html');
      const posts = extractPosts(doc).map(po => ({ ...po, page: p }));
      allPosts.push(...posts);
      status(`Page ${p}/${maxPage}: extracted ${posts.length} posts (running total ${allPosts.length})`);
    } catch (err) {
      console.error(`Page ${p} error:`, err);
    }
    if (p < maxPage) await new Promise(r => setTimeout(r, FETCH_DELAY_MS));
  }

  // ── Build output ────────────────────────────────────────────────────
  const out = {
    audit_label: 'Trusight A1 LIVE — UA 2026 Villainous Options 2',
    thread_url: baseThreadUrl,
    thread_id: threadId,
    thread_slug: threadSlug,
    thread_title: document.title.replace(/\s*-\s*Giant in the Playground.*$/i, '').trim(),
    pages_walked: maxPage,
    total_posts: allPosts.length,
    harvested_at: new Date().toISOString(),
    forum: 'forums.giantitp.com',
    posts: allPosts,
  };

  // ── Download as JSON ────────────────────────────────────────────────
  const json = JSON.stringify(out, null, 2);
  const blob = new Blob([json], { type: 'application/json' });
  const filename = `gitp_ua_villainous_options_2_${threadId}.json`;
  const a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);

  status(`✓ Done. Downloaded ${allPosts.length} posts across ${maxPage} pages → ${filename}`);

  // Auto-close after a few seconds
  setTimeout(() => panel.remove(), 8000);
})();
