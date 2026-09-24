// Arcane Incursion — BackerKit harvest
//
// Ported from scripts/backerkit_bookmarklet.js, whose last row is dated
// 2026-05-19.
//
// WHY THE EXTENSION IS THE ONLY HOME THIS CAN HAVE. Unlike the other streams,
// BackerKit is not merely unscheduled — it is unreachable from a server.
// cloud_functions/backerkit_harvester/main.py takes a deterministic 403 from
// BackerKit's edge because GCP datacenter IPs are flagged, persistent in Cloud
// Run logs since at least 2026-05-13. The fix is not a better retry: it is a
// residential IP and a real signed-in session, which means a real browser. The
// bookmarklet was the workaround; the extension is the same workaround with a
// schedule attached.
//
// The about:blank popup relay is gone for the same reason as Kickstarter's: an
// injected extension script is not bound by the page's CSP, proven by the
// Amazon harvest posting 337 rows through Amazon's CSP on 2026-09-23.
//
// WIRE FORMAT DELIBERATELY UNCHANGED — and here the reason is sharper than for
// Kickstarter. The bookmarklet zeroes unmeasured values:
//
//     parseAmount()   -> 0.0 when absent or unparseable
//     backers_count   -> parseInt(...) || 0
//     days_remaining  -> 0 on a parse failure, and clamped at 0
//
// but sending null instead would change nothing, because the BOUNCER re-zeroes
// them on arrival:
//
//     'funding_usd':   float(r.get('funding_usd') or 0.0),
//     'backers_count': int(r.get('backers_count') or 0),
//     'days_remaining': int(r.get('days_remaining') or 0),
//     'system_tag':    str(r.get('system_tag', 'RPG (Other)'))[:100],
//
// Python's `or` turns a null into a zero just as reliably as the bookmarklet
// did. So abstention for BackerKit is a bouncer change plus a BigQuery schema
// check, not a harvester change, and it needs a deploy. Out of scope here.
//
// What this does instead is COUNT the defaults, so the size of the problem is
// known before anyone changes the contract. days_remaining is the one to watch:
// zero is returned for "ended today", "could not parse the date" and "ended
// months ago" alike, and those are three different facts.
//
// One thing that IS fixed: the bookmarklet classified the system by feeding
// `title + ' ' + project_id` to the keyword matcher. An id is not prose, and an
// id containing "5e" would have classified the project as 5e Compatible. The id
// no longer reaches the classifier; only the title does.

const BK_BASE = "https://www.backerkit.com/c/collections/";

// Decorative, and measured to be so. On 2026-09-24 all ten sort_by values
// tried -- trending, newest, ending_soon, most_funded, most_backed,
// recently_launched, popular, end_date, launch_date, funded -- returned the
// SAME ten projects. Kept because it is the URL the site itself uses, and
// because a sort that starts working costs nothing to already be sending.
const BK_SORT = "?sort_by=trending";

// The only axis that widens coverage. Pagination does not exist here: page,
// offset and per_page are all silently ignored, as is sort_by. Different
// COLLECTIONS return genuinely different projects, so this is how the stream
// grows. Measured the same day, against a running union of ids:
//
//     role-playing-games   10 projects, 10 new
//     tabletop-games       10 projects, 10 new
//     card-games            8 projects,  8 new
//     comics               10 projects, 10 new   (off-domain, excluded)
//     board-games / games / miniatures            404
//
// 28 projects instead of 10. comics is left out deliberately: this is a D&D
// trends index and those rows would be noise, not coverage. Adding it back is
// one string. Three of six guessed slugs 404, so the list is empirical --
// a slug that stops existing must not take the run down with it.
// See scripts/probe_backerkit_pagination.js.
const BK_COLLECTIONS = ["role-playing-games", "tabletop-games", "card-games"];

const BK_LANDING = BK_BASE + BK_COLLECTIONS[0] + BK_SORT;
const BK_ENDPOINT =
    ENDPOINT.replace("system/library/ingest-catalog", "system/backerkit/ingest-projects");

async function runBackerkitExtractionInPage(siteName, ritualKey, endpoint, base, sort, collections) {
    try {
        if (!location.hostname.endsWith("backerkit.com")) {
            return { site: siteName, success: false, error: "Not on backerkit.com (url=" + location.href + ")" };
        }

        // ---------- BackerKit normalisation (sliced by scripts/test_backerkit_harvest.js) ----------

        function bkAmount(v) {
            if (v === null || v === undefined || v === "") return null;
            const cleaned = String(v).replace(/[^\d.]/g, "");
            if (cleaned === "" || cleaned === ".") return null;
            const n = parseFloat(cleaned);
            return Number.isFinite(n) ? n : null;
        }

        function bkInt(v) {
            if (v === null || v === undefined || v === "") return null;
            const cleaned = String(v).replace(/[^\d-]/g, "");
            if (cleaned === "" || cleaned === "-") return null;
            const n = parseInt(cleaned, 10);
            return Number.isFinite(n) ? n : null;
        }

        // BackerKit renders "April 30, 2026 at 10:00 AM PDT". Returns null when
        // the date cannot be read, so "unparseable" is distinguishable from
        // "ends today" — the bookmarklet returned 0 for both, and also for a
        // campaign that ended months ago, because of the Math.max(0, …) clamp.
        function bkDaysRemaining(endedAt, nowMs) {
            if (!endedAt) return null;
            const datePart = String(endedAt).split(" at ")[0];
            const dt = new Date(datePart);
            const t = dt.getTime();
            if (!Number.isFinite(t)) return null;
            return Math.round((t - nowMs) / 86400000);
        }

        const BK_DND = ["5e", "5th edition", "d&d", "dungeons", "2024 compatible",
                        "black flag", "tales of the valiant", "mcdm", "dragonbane"];
        const BK_OSR = ["osr", "old school", "old-school", "b/x", "odnd", "ad&d"];

        // Title only. The bookmarklet also passed the project id, which is not
        // prose: an id containing "5e" would have classified the project.
        function bkClassify(title) {
            const t = String(title || "").toLowerCase();
            if (BK_DND.some(k => t.indexOf(k) !== -1)) return "5e Compatible";
            if (BK_OSR.some(k => t.indexOf(k) !== -1)) return "OSR";
            return "RPG (Other)";
        }

        function bkBuildRow(p, nowMs) {
            const projectId = String((p && p.id) || "").trim();
            const title = String((p && p.title) || "").trim();
            if (!projectId || !title) return null;

            const funding = bkAmount(p.raised_amount);
            const backers = bkInt(p.backers);
            const days = bkDaysRemaining(p.ended_at, nowMs);
            const creator = p.creator_name ? String(p.creator_name) : null;

            const defaulted = [];
            if (funding === null) defaulted.push("funding_usd");
            if (backers === null) defaulted.push("backers_count");
            if (days === null) defaulted.push("days_remaining");
            if (creator === null) defaulted.push("creator");
            // A campaign that has already ended reports a negative day count,
            // which the bookmarklet clamped to 0 and so made indistinguishable
            // from one ending today.
            if (days !== null && days < 0) defaulted.push("ended_clamped");

            return {
                defaulted: defaulted,
                row: {
                    project_id: projectId,
                    title: title,
                    creator: creator || "",
                    funding_usd: funding === null ? 0.0 : funding,
                    backers_count: backers === null ? 0 : backers,
                    days_remaining: days === null ? 0 : Math.max(0, days),
                    system_tag: bkClassify(title),
                    source_url: p.formatted_permalink || ""
                }
            };
        }

        function bkSummarise(built) {
            const counts = {};
            for (const b of built) {
                for (const f of b.defaulted) counts[f] = (counts[f] || 0) + 1;
            }
            const parts = Object.keys(counts).sort().map(k => k + " " + counts[k]);
            return parts.length ? parts.join(", ") : "no defaulted fields";
        }

        // Inertia nests page props under .props, but the retired Cloud Function
        // read the key off the top level. Try both rather than assume.
        function bkExtractProjects(data) {
            if (!data) return [];
            if (Array.isArray(data["crowdfunding/projects"])) return data["crowdfunding/projects"];
            if (data.props && Array.isArray(data.props["crowdfunding/projects"])) {
                return data.props["crowdfunding/projects"];
            }
            return [];
        }

        // Collections overlap, so the union has to be deduped by project id --
        // and the per-collection tally is kept so a slug that silently stops
        // returning anything is visible as "0/0" instead of just shrinking the
        // total. A collection that errors is recorded, never dropped.
        function bkDedupeProjects(batches) {
            const seen = new Set();
            const projects = [];
            const perCollection = [];
            for (const b of (batches || [])) {
                const got = (b && b.projects) || [];
                let added = 0;
                for (const p of got) {
                    const id = String((p && p.id) || "").trim();
                    if (!id || seen.has(id)) continue;
                    seen.add(id);
                    projects.push(p);
                    added++;
                }
                perCollection.push({
                    collection: b && b.collection,
                    returned: got.length,
                    added: added,
                    error: (b && b.error) || null
                });
            }
            return { projects: projects, perCollection: perCollection };
        }

        function bkCoverage(perCollection) {
            return (perCollection || []).map(function (c) {
                return c.error ? (c.collection + " FAILED(" + c.error + ")")
                               : (c.collection + " " + c.added + "/" + c.returned);
            }).join(", ");
        }

        // ---------- end BackerKit normalisation ----------

        // One request per collection. A 404 on one slug is expected over time
        // -- three of six guessed slugs already 404 -- so a failure is recorded
        // against that collection and the run continues with the rest.
        const batches = [];
        let lastKeys = null;
        for (const collection of collections) {
            try {
                const res = await fetch(base + collection + sort, {
                    method: "GET",
                    credentials: "include",
                    headers: {
                        "X-Inertia": "true",
                        "X-Requested-With": "XMLHttpRequest",
                        "Accept": "application/json"
                    }
                });
                if (!res.ok) {
                    batches.push({ collection: collection, projects: [], error: "HTTP " + res.status });
                    continue;
                }
                const data = await res.json();
                lastKeys = Object.keys(data || {}).slice(0, 8).join(",");
                batches.push({ collection: collection, projects: bkExtractProjects(data) });
            } catch (e) {
                batches.push({ collection: collection, projects: [], error: e.message });
            }
            await new Promise(r => setTimeout(r, 400));
        }

        const merged = bkDedupeProjects(batches);
        const projects = merged.projects;
        const coverage = bkCoverage(merged.perCollection);

        if (!projects.length) {
            const allFailed = merged.perCollection.every(c => c.error);
            return {
                site: siteName, success: false,
                error: allFailed
                    ? ("Every collection failed (" + coverage + ") — signed in to BackerKit?")
                    : ("No projects across " + collections.length + " collections (" + coverage +
                       "; last top-level keys: " + (lastKeys || "none") +
                       ") — the response shape may have changed")
            };
        }

        const nowMs = Date.now();
        const built = projects.map(p => bkBuildRow(p, nowMs)).filter(Boolean);
        if (!built.length) {
            return {
                site: siteName, success: false,
                error: "Parsed 0 valid rows from " + projects.length +
                       " projects (project_id and title are both required)"
            };
        }

        const post = await fetch(endpoint, {
            method: "POST",
            headers: { "Content-Type": "application/json", "X-Ritual-Key": ritualKey },
            body: JSON.stringify(built.map(b => b.row))
        });
        if (!post.ok) throw new Error("ingest HTTP " + post.status);
        const out = await post.json().catch(() => ({}));

        return {
            site: siteName,
            success: true,
            count: out.skipped ? 0 : (out.inserted || built.length),
            skipped: !!out.skipped,
            projectsSeen: projects.length,
            defaultedFields: bkSummarise(built),
            tierSummary: bkSummarise(built) + " | " + coverage,
            coverage: coverage,
            shelves: merged.perCollection.map(c => c.collection)
        };
    } catch (e) {
        return { site: siteName, success: false, error: e.message };
    }
}
