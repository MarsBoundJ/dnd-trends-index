// BackerKit pagination probe — read-only. Does GETs, posts nothing.
//
// HOW TO RUN. Open any page on https://www.backerkit.com/ in a tab where you
// are SIGNED IN, then F12 → Console → paste this whole file → Enter.
//
// WHY IT EXISTS. The 2026-09-24 harvest captured 10 BackerKit projects against
// Kickstarter's 252. Ten looked like a page size, and the obvious next step was
// to add pagination. This was written to find out how BackerKit pages before
// writing that code.
//
// WHAT IT MEASURED, 2026-09-24. There is no pagination to add.
//
//     page=2         ->  10 projects, 0 NOT on page 1
//     page=1&page=2  ->  10 projects, 0 NOT on page 1
//     offset=10      ->  10 projects, 0 NOT on page 1
//     per_page=50    ->  10 projects, 0 NOT on page 1
//
// All four conventions are silently ignored: same ten ids every time. The
// response has exactly one top-level key, "crowdfunding/projects", props is
// empty, and a recursive walk for page/total/next/cursor/offset/limit/meta/
// links found nothing anywhere in the payload.
//
// THE SHAPE OF THAT CHECK IS THE POINT. Every one of those four requests
// returned 10 projects and HTTP 200. Counting results would have read as four
// successes. Only comparing the returned ids against page 1 exposed that the
// parameter did nothing — which is the same failure mode as the Amazon byline
// selector that scored 80% while returning "Kindle Edition" for every sample.
// A probe that measures the wrong thing agrees with the code.
//
// THREE OTHER FINDINGS FROM THE SAME RUN:
//
//   1. `id` is a SLUG, not a number:
//      "the-one-ring-rpg-bestiary-gondor-campaign-updated-rules".
//      Stable, so nothing is broken by storing it as project_id — but it
//      retroactively justifies dropping the id from the system classifier.
//      A slug is prose, and bkClassify(title + ' ' + project_id) would have
//      matched keywords inside these.
//
//   2. ALL TEN PROJECTS HAD ALREADY ENDED (ended: 10, live: 0), confirmed on
//      a fresh fetch rather than inferred from the harvest. So days_remaining
//      is 0 for every row this URL will ever produce, and the bookmarklet's
//      Math.max(0, …) clamp destroys 100% of the signal rather than a
//      minority of it.
//
//   3. THE PAYLOAD CARRIES 16 KEYS AND THE HARVESTER READS 7. Unused:
//      funding_goal, funding_percentage, started_at, has_raised_funding_goal?,
//      ended_and_chargeable?, creator_id, hero_image_url, comments,
//      project_updates. Kickstarter rows carry goal_usd and percent_funded;
//      BackerKit rows carry no goal at all, and the data to fix that is
//      already in the response we throw away.
//
// The amounts are not trivial: the sample project was The One Ring RPG at
// $2,107,889 across 8,805 backers. Ten projects is a thin slice, not a
// worthless one.

(async () => {
  const BASE = "https://www.backerkit.com/c/collections/role-playing-games?sort_by=trending";
  const H = { "X-Inertia": "true", "X-Requested-With": "XMLHttpRequest", "Accept": "application/json" };
  const hdr = (t) => console.log("%c" + t, "color:#a78bfa;font-weight:bold");

  if (!location.hostname.endsWith("backerkit.com")) {
    console.log("%cRun this from a backerkit.com tab.", "color:#e11;font-weight:bold"); return;
  }

  async function get(url) {
    const r = await fetch(url, { method: "GET", credentials: "include", headers: H });
    if (!r.ok) return { err: "HTTP " + r.status };
    try { return { data: await r.json() }; } catch (e) { return { err: "not JSON: " + e.message }; }
  }
  // Mirrors bkExtractProjects in catalog_harvester_extension/backerkit.js.
  const projectsOf = (d) => !d ? [] :
    (Array.isArray(d["crowdfunding/projects"]) ? d["crowdfunding/projects"]
     : (d.props && Array.isArray(d.props["crowdfunding/projects"])) ? d.props["crowdfunding/projects"] : []);

  console.log("%cBackerKit pagination probe", "color:#a78bfa;font-weight:bold;font-size:14px");

  const p1 = await get(BASE);
  if (p1.err) { console.log("%cpage 1 failed: " + p1.err + " — signed in?", "color:#e11;font-weight:bold"); return; }
  const a1 = projectsOf(p1.data);
  hdr("\n1. One fetch (what production does today)");
  console.log("   top-level keys :", Object.keys(p1.data || {}));
  console.log("   props keys     :", Object.keys((p1.data && p1.data.props) || {}));
  console.log("   projects found :", a1.length);

  hdr("\n2. Anything pagination-shaped in the response");
  const RE = /page|total|count|next|prev|cursor|more|offset|limit|meta|links|pagination/i;
  const hits = [];
  (function walk(o, path, depth) {
    if (!o || typeof o !== "object" || depth > 6) return;
    for (const k of Object.keys(o)) {
      const v = o[k], p = path ? path + "." + k : k;
      if (RE.test(k) && (v === null || typeof v !== "object")) hits.push([p, v]);
      else if (RE.test(k) && Array.isArray(v)) hits.push([p, "[array len " + v.length + "]"]);
      else if (v && typeof v === "object" && !Array.isArray(v)) walk(v, p, depth + 1);
      else if (Array.isArray(v) && v.length && typeof v[0] === "object" && p.indexOf("projects") === -1) walk(v[0], p + "[0]", depth + 1);
    }
  })(p1.data, "", 0);
  if (hits.length) hits.slice(0, 40).forEach(([p, v]) => console.log("   ", p, "=", v));
  else console.log("   (none found — pagination is probably a query param only)");

  // The decisive check: NEW IDS, not result counts. An ignored parameter
  // returns a full page of the same projects and a 200.
  hdr("\n3. Does page=2 return DIFFERENT projects?");
  const ids1 = new Set(a1.map(p => String(p && p.id)));
  for (const param of ["page=2", "page=1&page=2", "offset=10", "per_page=50"]) {
    const r = await get(BASE + "&" + param);
    if (r.err) { console.log("   " + param.padEnd(14) + " -> " + r.err); continue; }
    const arr = projectsOf(r.data);
    const fresh = arr.map(p => String(p && p.id)).filter(id => !ids1.has(id)).length;
    console.log("   " + param.padEnd(14) + " -> " + String(arr.length).padStart(3) + " projects, " +
                fresh + " NOT on page 1" + (fresh > 0 ? "   <-- PAGINATES" : ""));
    await new Promise(r => setTimeout(r, 400));
  }

  hdr("\n4. Field names the harvester assumes");
  const s = a1[0] || {};
  console.log("   sample project keys:", Object.keys(s));
  ["id", "title", "raised_amount", "backers", "ended_at", "creator_name", "formatted_permalink"]
    .forEach(f => console.log("   " + (f in s ? "  ok  " : " MISSING ") + f.padEnd(20) +
                              (f in s ? JSON.stringify(s[f]).slice(0, 60) : "")));

  hdr("\n5. Ended vs live in this sample");
  const now = Date.now();
  let ended = 0, unparseable = 0;
  a1.forEach(p => {
    const t = new Date(String(p.ended_at || "").split(" at ")[0]).getTime();
    if (!Number.isFinite(t)) unparseable++; else if (t < now) ended++;
  });
  console.log("   ended:", ended, "| live:", a1.length - ended - unparseable, "| unparseable date:", unparseable);

  console.log("%c\nCopy everything above back to Claude.", "color:#2a2;font-weight:bold");
})();
