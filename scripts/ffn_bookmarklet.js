/**
 * FFN D&D-Crossover-Count Bookmarklet
 * (Stage 4 of community_reception, Apr 27-28, 2026 — index-mode added)
 *
 * Two modes, auto-detected from URL:
 *
 *   INDEX mode  (URL: /crossovers/Dungeons-and-Dragons/1116/)
 *     Scans the page for all fandoms with D&D crossovers, fuzzy-matches
 *     each against the seed list, batch-POSTs the matches in one shot.
 *     One click captures dozens of IPs.
 *
 *   PAIR mode  (URL: /[A]-and-[B]-Crossovers/[ID]/[ID]/)
 *     Single-IP capture. Prompts the user for the count (with auto-
 *     detected suggestion as default) and the IP attribution.
 *
 * Index counts on FFN can be off by ±1 vs. the actual story count on
 * each pair-page (FFN's index sometimes includes deleted/private stories).
 * This is acceptable signal for a relative-ranking use case but is
 * recorded in `scraped_by` so we can distinguish index- vs pair-captures
 * in the gold view if we want to.
 *
 * Ethics: human-wielded UI tooling. Phil clicks the bookmarklet on a
 * page he's already browsing in his normal session. Not a bot.
 */

(async function () {
  const BOUNCER = 'https://us-central1-dnd-trends-index.cloudfunctions.net/bouncer-api';
  const KEY = 'ArcaneLibrarian2026';
  const PLATFORM = 'ffn';

  // ── Embedded seed-list IP names (synced from scripts/seed_ub_candidate_ips.py) ──
  // Used for fuzzy-matching FFN fandom names → our canonical IP names.
  const SEED_IPS = ["3 Body Problem","Alice in Borderland","Andor","Arcane","Attack on Titan","Avatar: The Last Airbender","Balatro","Baldur's Gate 3","Berserk","Blacktongue Thief","Bloodborne","Blue Lock","Caves of Qud","Chainsaw Man","Children of Time","Citizen Sleeper","Claymore","Cowboy Bebop","Cradle","Crusader Kings 3","Cthulhu Mythos","Cyberpunk 2077","Dark Souls","Dead Boy Detectives","Deep Rock Galactic","Delicious in Dungeon","Demon Slayer","Destiny 2","Disco Elysium","Discworld","Divinity: Original Sin 2","Doctor Who","Dorohedoro","Dune","Dungeon Crawler Carl","Dwarf Fortress","Elden Ring","Escape from Tarkov","Fallout","Final Fantasy XIV","Final Fantasy XVI","For All Mankind","Foundation","Frieren: Beyond Journey's End","From","Gamera","Genshin Impact","Goblin Slayer","Godzilla","Hades","Hardcore Leveling Warrior","Hazbin Hotel","Helldivers 2","Helluva Boss","Hilda","Hollow Knight","Hollow Knight: Silksong","Honkai: Star Rail","House of the Dragon","Hunt: Showdown","Inscryption","Invincible","Jujutsu Kaisen","Kill Six Billion Demons","Konosuba","Kpop Demon Hunters","Lethal Company","Lies of P","Made in Abyss","Malazan Book of the Fallen","Mistborn","Mob Psycho 100","Monster Hunter Wilds","Murder Drones","Mushoku Tensei","Nioh 2","Noblesse","Norco","Omniscient Reader's Viewpoint","One Piece","Over the Garden Wall","Overlord","Pacific Rim","Pantheon","Path of Exile 2","Pathfinder: Wrath of the Righteous","Pentiment","Percy Jackson and the Olympians","Persona 5 Royal","Pillars of Eternity II: Deadfire","Rain World","Re:Zero","Red Rising","Rimworld","Roadwarden","Scavengers Reign","Sea of Thieves","Sekiro: Shadows Die Twice","Severance","Shogun","Signalis","Silo","Solo Leveling","Spy x Family","Squid Game","Stranger Things","Sun Eater","That Time I Got Reincarnated as a Slime","The Boys","The Broken Earth","The First Law","The God of High School","The Kingkiller Chronicle","The Legend of Korra","The Locked Tomb","The Lord of the Rings","The Lord of the Rings: The Rings of Power","The Magnus Archives","The Mandalorian","The Murderbot Diaries","The Poppy War","The Sandman","The Stormlight Archive","The Wheel of Time","The Witcher","The Witcher 3","Tokyo Ghoul","Total War: Warhammer 3","Tower of God","Triangle Strategy","True Detective: Night Country","Tyranny","Undertale","Unicorn Overlord","Unordinary","Valheim","Vinland Saga","Warframe","Warhammer 40,000: Rogue Trader","Welcome to Night Vale","Wuthering Waves","XCOM 2"];

  function normalize(s) {
    return (s || '').toLowerCase().replace(/[^a-z0-9]/g, '');
  }

  function fuzzyMatchIP(ffnName) {
    const ffnNorm = normalize(ffnName);
    if (!ffnNorm) return null;
    // Pass 1: exact normalized match
    for (const ip of SEED_IPS) {
      if (normalize(ip) === ffnNorm) return ip;
    }
    // Pass 2: substring (either direction). Avoid matching short FFN names
    // ("From", "Hilda") to longer seed names by requiring the FFN name be
    // at least 4 chars or be a full-word match — substring of >50% length.
    if (ffnNorm.length < 4) return null;
    for (const ip of SEED_IPS) {
      const ipNorm = normalize(ip);
      if (ipNorm.length < 3) continue;
      if (ipNorm.includes(ffnNorm) && ffnNorm.length >= ipNorm.length * 0.5) return ip;
      if (ffnNorm.includes(ipNorm) && ipNorm.length >= ffnNorm.length * 0.5) return ip;
    }
    return null;
  }

  // ── UI ──────────────────────────────────────────────────────────────────
  const ui = document.createElement('div');
  Object.assign(ui.style, {
    position: 'fixed', top: '12px', right: '12px', zIndex: 999999,
    background: '#0d0d1a', color: '#e0e0ff', padding: '14px 18px',
    borderRadius: '10px', fontFamily: 'monospace', fontSize: '13px',
    maxWidth: '420px', boxShadow: '0 4px 24px rgba(0,0,0,0.7)',
    lineHeight: '1.5',
  });
  const titleEl = document.createElement('div');
  titleEl.style.cssText = 'font-weight:bold;font-size:14px;margin-bottom:6px;color:#3b82f6';
  titleEl.textContent = '📖 FFN Crossover Capture';
  const statusEl = document.createElement('div');
  statusEl.textContent = 'Detecting page mode...';
  ui.appendChild(titleEl);
  ui.appendChild(statusEl);
  document.body.appendChild(ui);

  const log = (msg) => { statusEl.innerHTML = msg; console.log('[ffn-bk]', msg.replace(/<[^>]+>/g, '')); };
  const close = (delay) => setTimeout(() => ui.remove(), delay || 8000);

  if (!location.hostname.endsWith('fanfiction.net')) {
    log('⚠️ Not on FFN — open an FFN page first.');
    close();
    return;
  }

  // ── Detect page mode ────────────────────────────────────────────────────
  const isIndexPage = /^\/crossovers\/Dungeons-and-Dragons\/\d+\/?/i.test(location.pathname);
  const pairMatch = location.pathname.match(
    /^\/([^/]+)-and-([^/]+)-Crossovers\/(\d+)\/(\d+)\/?/i
  );

  // ────────────────────────────────────────────────────────────────────
  // INDEX MODE — capture all fandoms on the D&D crossover index page
  // ────────────────────────────────────────────────────────────────────
  if (isIndexPage) {
    log('Index page detected. Scanning fandoms...');

    // Find all anchor tags pointing to D&D crossover pair pages
    const links = Array.from(document.querySelectorAll(
      'a[href*="-and-Dungeons-and-Dragons-Crossovers/"]'
    ));

    // Extract (ffn_name, count, source_url, fandom_id) per link
    const allCandidates = [];
    for (const a of links) {
      let ffnName = a.textContent.trim();
      // FFN sometimes embeds the count in the link text: "Magic: The Gathering (2)"
      const inlineCount = ffnName.match(/^(.+?)\s*\((\d+)\)\s*$/);
      let count = null;
      if (inlineCount) {
        ffnName = inlineCount[1].trim();
        count = parseInt(inlineCount[2], 10);
      }
      // If count not in link text, look at parent / sibling text
      if (count === null) {
        const parentText = a.parentElement ? a.parentElement.textContent : '';
        const m = parentText.match(/\((\d+)\)/);
        if (m) count = parseInt(m[1], 10);
      }
      // Extract IDs from URL for the source attribution
      const hrefMatch = a.href.match(/\/([^/]+)-and-Dungeons-and-Dragons-Crossovers\/(\d+)\/(\d+)\/?/i);
      if (!hrefMatch) continue;
      allCandidates.push({
        ffnName,
        count: count || 0,
        href: a.href,
        ffnFandomId: hrefMatch[2],
      });
    }
    log(`Found ${allCandidates.length} D&D crossover fandoms on the index.`);

    // Fuzzy-match against seed list
    const matches = [];
    for (const c of allCandidates) {
      const matchedIp = fuzzyMatchIP(c.ffnName);
      if (matchedIp && c.count > 0) {
        matches.push({ ...c, ip_name: matchedIp });
      }
    }
    if (matches.length === 0) {
      log('⚠️ No seed-list IPs matched. Either the page changed or seed list needs updating.');
      close(12000);
      return;
    }
    // Dedup: same ip_name might match multiple FFN entries (Doctor Who 2005
    // vs classic). Keep the highest-count match per ip_name.
    const dedup = new Map();
    for (const m of matches) {
      const ex = dedup.get(m.ip_name);
      if (!ex || m.count > ex.count) dedup.set(m.ip_name, m);
    }
    const finalMatches = [...dedup.values()];

    // Build a preview table for the confirm modal
    const previewLines = finalMatches
      .sort((a, b) => b.count - a.count)
      .slice(0, 20)
      .map(m => `  ${m.ip_name.padEnd(35)} ${m.count} (FFN: ${m.ffnName})`)
      .join('\n');
    const overflow = finalMatches.length > 20
      ? `\n  ... and ${finalMatches.length - 20} more`
      : '';

    const confirmed = confirm(
      `Save ${finalMatches.length} FFN crossover counts?\n\n` +
      `Top matches by count:\n${previewLines}${overflow}\n\n` +
      `Note: index-page counts can be off by ±1 vs. the actual story\n` +
      `count on each pair-page (FFN includes some deleted stories in\n` +
      `the index). Acceptable for relative-ranking use cases.\n\n` +
      `OK to save all, Cancel to abort.`
    );
    if (!confirmed) {
      log('Cancelled.');
      close(3000);
      return;
    }

    log(`Saving ${finalMatches.length} matches to bouncer...`);
    const payload = finalMatches.map(m => ({
      ip_name: m.ip_name,
      platform: PLATFORM,
      platform_canonical: m.ffnName + ` (FFN id ${m.ffnFandomId}/1116)`,
      work_count: m.count,
      source_url: m.href,
      scraped_by: 'ffn_bookmarklet_index',
    }));
    try {
      const resp = await fetch(`${BOUNCER}/system/fanfic/ingest-crossover-count`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-Ritual-Key': KEY },
        body: JSON.stringify(payload),
      });
      const data = await resp.json();
      if (resp.ok && data.inserted) {
        log(`✅ Saved ${data.inserted} FFN matches.`);
      } else {
        log(`⚠️ Bouncer error: ${data.error || resp.status}`);
      }
    } catch (e) {
      log(`⚠️ Network error: ${e.message}`);
      console.error('[ffn-bk] failed:', e);
    }
    close(10000);
    return;
  }

  // ────────────────────────────────────────────────────────────────────
  // PAIR MODE — single-IP capture from a /X-and-D&D-Crossovers/ page
  // ────────────────────────────────────────────────────────────────────
  if (!pairMatch) {
    log('⚠️ Not on a recognized FFN page.<br>Open the D&D crossover index ' +
        '(/crossovers/Dungeons-and-Dragons/1116/) or a specific crossover ' +
        'pair page (/X-and-Dungeons-and-Dragons-Crossovers/...).');
    close(15000);
    return;
  }

  const params = new URLSearchParams(location.search);
  let ip_name = params.get('_arcane_ip') || '';
  if (!ip_name) {
    ip_name = (prompt(
      'Which seed-list IP is this FFN crossover for?\n' +
      '(Type the exact IP name from the seed list)'
    ) || '').trim();
  }
  if (!ip_name) {
    log('⚠️ No IP specified — aborting.');
    close();
    return;
  }

  // Best-effort count suggestion (manual entry overrides)
  function suggestStoryCount() {
    for (const c of document.querySelectorAll('center, .pagination, body')) {
      const text = c.textContent;
      const m = text.match(/of\s+([\d,]+)\b/i);
      if (m) {
        const n = parseInt(m[1].replace(/,/g, ''), 10);
        if (n > 0 && n < 1000000) return { count: n, via: 'matched "of N" in page text' };
      }
    }
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

  const canonical = `${pairMatch[1].replace(/-/g, ' ')} x ${pairMatch[2].replace(/-/g, ' ')} (FFN ids ${pairMatch[3]}/${pairMatch[4]})`;

  const confirmed = confirm(
    `Save FFN crossover count?\n\n` +
    `IP: ${ip_name}\n` +
    `Platform: FFN\n` +
    `Story count: ${work_count.toLocaleString()}\n` +
    `Canonical: ${canonical}\n\n` +
    `OK to save, Cancel to abort.`
  );
  if (!confirmed) {
    log('Cancelled.');
    close(3000);
    return;
  }

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
        scraped_by: 'ffn_bookmarklet_pair',
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
