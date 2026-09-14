/**
 * AO3 D&D-Crossover-Count Bookmarklet — batch-confirm edition
 * (Stage 4 of community_reception, Apr 27 2026; rebuilt Sep 2 2026 as work
 *  item B of docs/data_capture_hardening_plan.md)
 *
 * WHAT CHANGED AND WHY
 *
 * The original flow was: load page -> click bookmarklet -> click a native
 * confirm() -> POST. Three interactions per IP, ~75 for a 25-IP round, and the
 * modal showed one number in isolation.
 *
 * Now: click the bookmarklet on each page (it stashes silently), then review
 * every capture in one table and send once. ~27 interactions instead of ~75,
 * with byte-identical AO3 traffic.
 *
 * It is also a CORRECTNESS fix, not just ergonomics. BG3's 49,020 sitting next
 * to LotR's 84 is obvious in a list and invisible one dialog at a time — that
 * artifact survived four months of per-page confirmations. Outlier detection is
 * what the confirmation step was always for; a table is simply better at it.
 *
 * NO NATIVE DIALOGS ANYWHERE. confirm()/prompt() are unusable for this:
 *   - Claude's browser pane auto-suppresses them, so confirm() silently
 *     returns false and every capture reads as "cancelled".
 *   - In CDP-driven Chrome a dialog blocks the renderer outright, freezing
 *     even screenshots.
 * An in-page panel works in both, and in a normal human browser.
 *
 * Ethics unchanged: human-wielded UI tooling, not an automated scraper. Phil
 * clicks a link, AO3 renders the page as for any reader, the bookmarklet reads
 * the count he can already see. No fetch() to AO3, no auto-iteration.
 */

(async function () {
  const BOUNCER = 'https://us-central1-dnd-trends-index.cloudfunctions.net/bouncer-api';
  const KEY = 'ArcaneLibrarian2026';
  const PLATFORM = 'ao3';
  const STASH = 'arcane_ao3_batch';

  // ── Stash helpers ───────────────────────────────────────────────────────
  const load = () => {
    try { return JSON.parse(localStorage.getItem(STASH) || '[]'); }
    catch (_) { return []; }
  };
  const save = (rows) => localStorage.setItem(STASH, JSON.stringify(rows));

  // ── UI shell ────────────────────────────────────────────────────────────
  const old = document.getElementById('__ao3_batch__');
  if (old) old.remove();

  const ui = document.createElement('div');
  ui.id = '__ao3_batch__';
  Object.assign(ui.style, {
    position: 'fixed', top: '12px', right: '12px', zIndex: 999999,
    background: '#0d0d1a', color: '#e0e0ff', padding: '14px 16px',
    borderRadius: '10px', fontFamily: 'monospace', fontSize: '13px',
    width: '460px', maxHeight: '86vh', overflowY: 'auto',
    boxShadow: '0 8px 32px rgba(0,0,0,0.7)', lineHeight: '1.45',
  });
  document.body.appendChild(ui);
  const esc = (s) => String(s == null ? '' : s)
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');

  // ── Page reading (unchanged logic — this part was never the problem) ────
  function extractWorkCount() {
    for (const h of document.querySelectorAll('h2.heading, h3.heading')) {
      const m = h.textContent.match(/of\s+([\d,]+)\s+Works?\s+in\b/i);
      if (m) return parseInt(m[1].replace(/,/g, ''), 10);
    }
    for (const h of document.querySelectorAll('h2.heading, h3.heading')) {
      const m = h.textContent.trim().match(/^([\d,]+)\s+Works?\s+in\b/i);
      if (m) return parseInt(m[1].replace(/,/g, ''), 10);
    }
    for (const h of document.querySelectorAll('h2.heading, h3.heading')) {
      const m = h.textContent.match(/^([\d,]+)\s+Found\b/);
      if (m) return parseInt(m[1].replace(/,/g, ''), 10);
    }
    for (const h of document.querySelectorAll('h2.heading, h3.heading')) {
      if (/0\s+(works|found)/i.test(h.textContent)) return 0;
      if (/no works found/i.test(h.textContent)) return 0;
    }
    const pag = document.querySelector('ol.pagination li.next');
    if (pag) {
      const t = pag.textContent.match(/of\s+([\d,]+)/i);
      if (t) return parseInt(t[1].replace(/,/g, ''), 10);
    }
    return null;
  }

  // ── Read-back filter verification ───────────────────────────────────────
  // Until now `platform_canonical` was ECHOED from our own URL and never read
  // back from AO3, so the stored tag looked identical whether the filter was
  // applied, ignored, or matched nothing. Every Sep 1-2 failure sailed through
  // on that: the unfiltered 10,886, the bare-synonym Avatar 15, the BG3 49,029.
  //
  // This asks AO3 what it thinks it did, using only the page the human already
  // loaded. No fetch(), no navigation — the ToS constraint is why this tool is
  // human-wielded, and verification must not quietly turn it into a crawler.
  //
  // Two independent signals, because either can be absent on a given layout:
  //   form  — AO3 repopulates its own filter box with the tags it applied
  //   works — the works it listed should actually carry the fandom we asked for
  // A signal that cannot be read returns nothing rather than passing. Silence
  // is reported as UNVERIFIED, never as verified; that distinction is the whole
  // point of the exercise.

  // AO3 names a fandom at several levels and blurbs show the CHILD, not the
  // umbrella we filtered on: ask for "Avatar: The Last Airbender & Related
  // Fandoms" and the works say "Avatar: The Last Airbender (TV 2005)". So
  // compare on a normalised base — umbrella suffix off, trailing qualifier off.
  function normTag(s) {
    return String(s == null ? '' : s)
      .replace(/&amp;/g, '&')
      .toLowerCase()
      .replace(/\s*-\s*all media types\s*$/, '')
      .replace(/\s*&\s*related fandoms\s*$/, '')
      .replace(/\s*\([^)]*\)\s*$/, '')
      .replace(/[^\p{L}\p{N}|]+/gu, ' ')
      .trim();
  }

  // "Wiedzmin | The Witcher" — AO3 joins localised titles with a pipe and a
  // work may be listed under either side. Both identify the same fandom.
  function tagAlts(s) {
    return normTag(s).split('|')
      .map((x) => x.trim())
      .filter((x) => x.length >= 3);
  }

  // Containment in ONE direction only: what AO3 lists may be MORE specific than
  // what we asked for (umbrella "the witcher" -> child "the witcher 3 wild
  // hunt"), never less. Allowing the other direction matched any shorter name
  // that happened to be a prefix — "Avatar" the 2009 film would have satisfied
  // a filter for "Avatar: The Last Airbender", which is the exact class of
  // wrong-but-plausible match this whole function exists to catch.
  function tagsOverlap(wanted, seen) {
    const A = tagAlts(wanted);
    const B = tagAlts(seen);
    return A.some((a) => B.some((b) => b === a || b.includes(a)));
  }

  function verifyFilter(wantedTag) {
    const signals = [];

    // Signal 1 — AO3's own filter box, which it refills with what it applied.
    const box = document.querySelector(
      'input[name="work_search[other_tag_names]"], #work_search_other_tag_names'
    );
    if (box) {
      const echoed = (box.value || '').trim();
      if (!echoed) {
        signals.push(['fail', 'AO3’s filter box is empty — it applied no "other tags" filter', 'form']);
      } else if (tagsOverlap(wantedTag, echoed)) {
        signals.push(['pass', `AO3 confirms the filter: "${echoed}"`, 'form']);
      } else {
        signals.push(['fail', `AO3 applied "${echoed}", we asked for "${wantedTag}"`, 'form']);
      }
    }

    // Signal 2 — semantic, and the stronger of the two: do the works AO3 listed
    // actually carry this fandom? A form field can echo a string the server
    // never used; a page of works cannot fake being tagged.
    const blurbs = Array.from(document.querySelectorAll('li.blurb, .work.blurb')).slice(0, 20);
    let checked = 0;
    let carrying = 0;
    for (const b of blurbs) {
      const links = Array.from(b.querySelectorAll('h5.fandoms a.tag, .fandoms a.tag'));
      if (!links.length) continue;
      checked++;
      if (links.some((a) => tagsOverlap(wantedTag, a.textContent))) carrying++;
    }
    if (checked) {
      if (carrying === 0) {
        signals.push(['fail', `none of the ${checked} works listed carry that fandom`, 'works']);
      } else if (carrying / checked >= 0.8) {
        signals.push(['pass', `${carrying}/${checked} listed works carry the fandom`, 'works']);
      } else {
        signals.push(['warn', `only ${carrying}/${checked} listed works carry the fandom`, 'works']);
      }
    }

    // The form box is AO3 stating what it applied; the works corroborate it.
    // So an empty box is decisive, but works that fail to match while the box
    // agrees is more likely my name-matching missing an oddly-named child than
    // proof the filter was dropped — the LotR umbrella lists works tagged only
    // "The Hobbit". Downgrade that combination to a warning rather than
    // refusing a real capture on it.
    const formOk = signals.some((x) => x[0] === 'pass' && x[2] === 'form');
    const graded = signals.map((x) =>
      (x[0] === 'fail' && x[2] === 'works' && formOk) ? ['warn', x[1], x[2]] : x);

    const verdict = graded.some((x) => x[0] === 'fail') ? 'failed'
                  : graded.some((x) => x[0] === 'pass') ? 'verified'
                  : 'unverified';
    return {
      verdict,
      warn: graded.some((x) => x[0] === 'warn'),
      detail: graded.map((x) => x[1]).join('; '),
    };
  }

  const params = new URLSearchParams(location.search);
  const ipFilter = (params.get('work_search[other_tag_names]') || '').trim();

  // ── Capture the current page, if it is one ──────────────────────────────
  let notice = '';
  if (location.hostname.endsWith('archiveofourown.org')) {
    const ip = (params.get('_arcane_ip') || '').trim();
    const count = extractWorkCount();
    // params.get() has already decoded; a second pass is a no-op on every tag
    // we generate but throws on a literal '%'. Keep the old value on failure.
    let wantedTag = ipFilter;
    try { wantedTag = decodeURIComponent(ipFilter); } catch (e) { /* keep raw */ }
    const ver = ipFilter ? verifyFilter(wantedTag) : null;

    if (!ipFilter) {
      // The filter IS the measurement. AO3 silently ignores a missing one and
      // returns the site-wide set, so an unfiltered page looks identical to a
      // filtered one. On Sep 1 this stored 10,886 as one IP's crossover count.
      notice = '<b style="color:#ff8888">Not captured — no IP filter on this URL.</b><br>'
             + 'This page is every D&amp;D crossover on AO3, not a D&amp;D × IP count. '
             + 'Use a deep link from <code>print_fanfic_capture_urls.py</code>.';
    } else if (!ip) {
      notice = '<b style="color:#ff8888">Not captured — no <code>_arcane_ip</code> marker.</b><br>'
             + 'Use a generated deep link so attribution cannot be mistyped.';
    } else if (ver && ver.verdict === 'failed') {
      // Refuse, the same way a missing filter is refused. A capture AO3 itself
      // contradicts is not a weaker number, it is a different measurement.
      notice = '<b style="color:#ff8888">Not captured — AO3 did not apply the tag we asked for.</b><br>'
             + esc(ver.detail) + '<br>'
             + 'Regenerate the link with <code>print_fanfic_capture_urls.py</code>; a saved bookmark goes stale whenever the seed tags change.';
    } else if (count === null) {
      notice = '<b style="color:#ff8888">Not captured — could not read a work count.</b><br>'
             + 'Make sure the results header is visible.';
    } else {
      const rows = load();
      const prev = rows.findIndex((r) => r.ip_name === ip);
      const row = {
        ip_name: ip,
        platform: PLATFORM,
        platform_canonical: decodeURIComponent(ipFilter),
        work_count: count,
        source_url: location.href,
        scraped_by: 'ao3_bookmarklet_batch',
        // Ignored by the ingest route today (it whitelists columns), kept on
        // the row so the review table can show it and so persisting it later
        // is a server change only.
        verification_verdict: ver ? ver.verdict : 'unverified',
        verification_warn: !!(ver && ver.warn),
        verification_detail: ver ? ver.detail : '',
      };
      if (prev >= 0) { rows[prev] = row; notice = `Updated <b>${esc(ip)}</b> → ${count.toLocaleString()}`; }
      else { rows.push(row); notice = `Captured <b>${esc(ip)}</b> → ${count.toLocaleString()}`; }
      save(rows);
    }
  } else {
    notice = 'Not on AO3 — showing the current batch.';
  }

  // ── Render ──────────────────────────────────────────────────────────────
  // Flags mirror gold_data.fanfic_capture_guard, applied at capture time so a
  // bad number is questioned before it is sent rather than after it lands.
  function flagsFor(r, all) {
    const f = [];
    if (r.work_count === 0) {
      f.push(['#ff8888', 'ZERO — every AO3 zero so far was a stale or unfilterable tag, never a real absence']);
    }
    // Not a failure — we simply could not read AO3's own filter state on that
    // page. It stays sendable, but it must not look like a checked capture.
    if (r.verification_warn) {
      f.push(['#d9a64a', `Filter confirmed, but the works disagree — ${r.verification_detail}`]);
    }
    if (r.verification_verdict === 'unverified') {
      f.push(['#d9a64a', 'UNVERIFIED — AO3’s filter state was unreadable; the tag above is only what we asked for']);
    }
    const others = all.filter((x) => x !== r).map((x) => x.work_count).sort((a, b) => a - b);
    const med = others.length ? others[Math.floor(others.length / 2)] : null;
    if (med && med > 0 && r.work_count > med * 50) {
      f.push(['#ff8888', `${Math.round(r.work_count / med)}× the batch median (${med}) — likely metatag inflation`]);
    } else if (med && med > 0 && r.work_count > med * 10) {
      f.push(['#d9a64a', `${(r.work_count / med).toFixed(1)}× the batch median (${med})`]);
    }
    return f;
  }

  // The review table shows the CANONICAL TAG under each IP, not just the count.
  // A wrong tag captures a plausible number with no other symptom: the bare
  // synonym "Avatar: The Last Airbender" read 15 works instead of the umbrella's
  // 60, looked normal in a list of counts, and went out THREE times on Sep 2
  // before anyone noticed — each time silently dropping Avatar from the
  // ranking, because a non-canonical tag has no fandom total to join to. The
  // count can't reveal that. The tag can, at the one moment someone is already
  // looking: the review before Send.
  function render() {
    const rows = load();
    const flagged = rows.map((r) => ({ r, f: flagsFor(r, rows) }));
    const anyCritical = flagged.some((x) => x.f.some((y) => y[0] === '#ff8888'));

    ui.innerHTML = `
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px">
        <b style="color:#3b82f6;font-size:14px">📚 AO3 Batch Capture</b>
        <button id="__x" style="background:none;border:none;color:#888;font-size:18px;cursor:pointer">×</button>
      </div>
      <div style="background:#1a1a2e;padding:7px 9px;border-radius:6px;margin-bottom:9px">${notice}</div>
      <div style="margin-bottom:6px;color:#aaa">Batch: <b style="color:#e0e0ff">${rows.length}</b> IP(s)</div>
      ${rows.length ? `<table style="width:100%;border-collapse:collapse;font-size:12px">${
        flagged.sort((a, b) => b.r.work_count - a.r.work_count).map(({ r, f }) => `
          <tr style="border-bottom:1px solid #2a2a4a">
            <td style="padding:3px 0">${esc(r.ip_name)}
              <div style="color:#7c7c9a;font-size:11px;word-break:break-word">${esc(r.platform_canonical)}</div>${
              r.verification_verdict === 'verified'
                ? `<div style="color:#5fdc7c;font-size:11px">✓ ${esc(r.verification_detail)}</div>` : ''}${f.map((x) =>
              `<div style="color:${x[0]};font-size:11px">⚠ ${esc(x[1])}</div>`).join('')}</td>
            <td style="text-align:right;padding:3px 0 3px 8px;white-space:nowrap">
              ${r.work_count.toLocaleString()}
              <button data-del="${esc(r.ip_name)}" style="background:none;border:none;color:#666;cursor:pointer">✕</button>
            </td>
          </tr>`).join('')}</table>` : '<i style="color:#666">Nothing captured yet.</i>'}
      ${anyCritical ? '<div style="color:#ff8888;margin-top:8px;font-size:12px">⚠ Resolve or remove flagged rows before sending.</div>' : ''}
      <div style="display:flex;gap:6px;margin-top:11px">
        <button id="__send" ${rows.length ? '' : 'disabled'} style="flex:1;background:${rows.length ? '#3b82f6' : '#333'};color:#fff;border:none;padding:9px;border-radius:5px;cursor:pointer;font-weight:600">Send all ${rows.length || ''}</button>
        <button id="__clr" style="background:#1a1a2e;color:#d9a64a;border:1px solid #2a2a4a;padding:9px 11px;border-radius:5px;cursor:pointer">Clear</button>
      </div>
      <div id="__status" style="margin-top:8px;color:#aaa"></div>`;

    ui.querySelector('#__x').onclick = () => ui.remove();
    ui.querySelectorAll('[data-del]').forEach((b) => {
      b.onclick = () => { save(load().filter((r) => r.ip_name !== b.dataset.del)); notice = 'Removed.'; render(); };
    });
    ui.querySelector('#__clr').onclick = () => { save([]); notice = 'Batch cleared.'; render(); };
    ui.querySelector('#__send').onclick = send;
  }

  async function send() {
    const rows = load();
    const st = ui.querySelector('#__status');
    if (!rows.length) {
      // Say so. A silent return here reads EXACTLY like a failed send, and on
      // Sep 2 that is how a successful batch got reported as broken: the first
      // click sent nine rows and cleared the stash, the panel did not redraw,
      // so the second click landed on an empty batch and did nothing visible.
      st.innerHTML = '<b style="color:#d9a64a">Nothing to send — the batch is empty.</b>';
      return;
    }
    st.textContent = `Sending ${rows.length}…`;
    try {
      const resp = await fetch(`${BOUNCER}/system/fanfic/ingest-crossover-count`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-Ritual-Key': KEY },
        body: JSON.stringify(rows),
      });
      const data = await resp.json();
      if (resp.ok && data.inserted) {
        // Only clear on confirmed success. Clearing optimistically would lose a
        // whole round of captures to one transient failure.
        save([]);
        // RE-RENDER. Clearing the stash without redrawing leaves the full table
        // on screen, so a successful send looks identical to a no-op — the one
        // status line reporting it sits at the bottom of a scrolling panel and
        // is usually out of view. The entire value of a batch table is that the
        // screen reflects the state; it has to keep doing that after the send.
        notice = `<b style="color:#5fdc7c">✅ Sent ${data.inserted} to BigQuery. Batch cleared.</b>`;
        render();
        ui.scrollTop = 0;   // notice renders at the top; make sure it is seen
        return;
      } else {
        st.innerHTML = `<b style="color:#ff8888">⚠ Bouncer: ${esc(data.error || resp.status)}</b><br>Batch kept — retry.`;
      }
    } catch (e) {
      st.innerHTML = `<b style="color:#ff8888">⚠ Network: ${esc(e.message)}</b><br>Batch kept — retry.`;
    }
  }

  // ── Cross-tab sync ──────────────────────────────────────────────────────
  // A capture round has ~25 AO3 tabs open at once, each with its own panel.
  // localStorage is shared across them, but a panel holds whatever it rendered
  // when it was drawn — so removing an IP in one tab left it visible in the
  // other 24, and each of those stale panels was one click away from sending a
  // batch the user thought they had edited.
  //
  // The `storage` event fires in every OTHER tab of the origin when the stash
  // changes, and deliberately NOT in the tab that wrote it (that one re-renders
  // directly). So this is exactly the right hook: one listener per panel, and
  // every open panel converges on the same batch.
  //
  // It covers sending too. When one tab sends and clears, every other panel
  // empties rather than continuing to display rows that are already in BigQuery.
  //
  // Re-clicking the bookmarklet in a tab replaces the panel, so the previous
  // handler is removed first — otherwise each click would leave a listener
  // behind, closed over a detached panel, re-rendering something nobody sees.
  if (window.__ao3BatchSync) {
    window.removeEventListener('storage', window.__ao3BatchSync);
  }
  window.__ao3BatchSync = (e) => {
    if (e.key && e.key !== STASH) return;      // e.key is null on clear()
    if (!document.body.contains(ui)) return;   // panel closed in this tab
    notice = load().length
      ? '<span style="color:#d9a64a">Batch updated in another tab.</span>'
      : '<span style="color:#d9a64a">Batch cleared or sent in another tab.</span>';
    render();
  };
  window.addEventListener('storage', window.__ao3BatchSync);

  render();
})();
