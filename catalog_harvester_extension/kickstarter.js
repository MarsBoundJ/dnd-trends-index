// Arcane Incursion — Kickstarter harvest
//
// Ported from scripts/kickstarter_bookmarklet.js, whose last row is dated
// 2026-05-18. It did not break; nobody clicked it.
//
// WHY THIS BELONGS IN THE EXTENSION. The GraphQL endpoint needs a signed-in
// session and a CSRF token read from the page, so it has to run in a real
// browser on a real login. A Cloud Function cannot do that. The extension runs
// in Yorri's own browser on a schedule, which is exactly the missing piece.
//
// WHAT THE PORT DROPS. The bookmarklet opened an about:blank popup and wrote a
// sender script into it, because Kickstarter's page CSP blocks a fetch to the
// bouncer from page context. An injected extension script runs in an isolated
// world and is not bound by the page's CSP — proven by the Amazon harvest on
// 2026-09-23, which posted 337 rows through Amazon's own CSP without a relay.
// So the popup, the popup-blocked failure mode, and the HTML-injection escaping
// all disappear.
//
// WHAT THE PORT DELIBERATELY DOES NOT CHANGE: the wire format.
//
// The bookmarklet records unmeasured values as zeros and fabricates two
// defaults outright:
//
//     backers_count:  node.backersCount || 0
//     pledged_usd:    parseFloat(...) || 0
//     status:         (node.state || 'live').toLowerCase()     <- invents "live"
//     category:       node.category?.name || 'Tabletop Games'  <- invents a category
//
// That is the same defect class as the Amazon rank bug, and it matters more
// here: Kickstarter and BackerKit are the only streams carrying cardinal data
// (real dollars, real backer counts), and a zero standing in for "unknown" is
// indistinguishable from a project that genuinely raised nothing.
//
// It is NOT fixed here, and that is a deliberate call. The bouncer passes these
// rows to insert_rows_json with skip_invalid_rows=True. If any of these columns
// is REQUIRED in BigQuery, sending null would cause the row to be dropped
// silently — losing data while the run reports success. The schema cannot be
// checked from this environment (no bq client), so changing the wire format
// blind trades a visible wrong number for an invisible missing row.
//
// Instead this counts them. The result carries defaultedFields, so the size of
// the problem is measured before anything is changed. Fixing it needs a schema
// check, a bouncer change and a deploy — none of which belong in a harvester PR.

const KS_LANDING = "https://www.kickstarter.com/discover/categories/games/tabletop%20games";
const KS_ENDPOINT =
    ENDPOINT.replace("system/library/ingest-catalog", "system/kickstarter/ingest-projects");
const KS_CATEGORY_ID = "34";
const KS_PER_PAGE = 20;
const KS_MAX_PAGES = 5;

async function runKickstarterExtractionInPage(siteName, ritualKey, endpoint,
                                              categoryId, perPage, maxPages) {
    try {
        if (!location.hostname.endsWith("kickstarter.com")) {
            return { site: siteName, success: false, error: "Not on kickstarter.com (url=" + location.href + ")" };
        }

        // ---------- Kickstarter normalisation (sliced by scripts/test_kickstarter_harvest.js) ----------

        // The project id arrives base64-encoded as "Project-1234567".
        function ksDecodeId(b64) {
            try {
                const decoded = atob(String(b64 || ""));
                const m = decoded.match(/(\d+)$/);
                return m ? parseInt(m[1], 10) : null;
            } catch (e) {
                return null;
            }
        }

        function ksAmount(v) {
            if (v === null || v === undefined) return null;
            const cleaned = String(v).replace(/[^\d.]/g, "");
            if (cleaned === "" || cleaned === ".") return null;
            const n = parseFloat(cleaned);
            return Number.isFinite(n) ? n : null;
        }

        // deadlineAt is a Unix timestamp in SECONDS. The bookmarklet multiplied
        // by 1000 and called toISOString() unguarded — on anything unexpected
        // that is `new Date(NaN).toISOString()`, which throws RangeError and
        // takes the whole harvest down rather than one field.
        function ksDeadline(v) {
            if (v === null || v === undefined || v === "") return null;
            const n = Number(v);
            if (!Number.isFinite(n) || n <= 0) return null;
            const d = new Date(n * 1000);
            if (!Number.isFinite(d.getTime())) return null;
            return d.toISOString();
        }

        const KS_DND_KEYWORDS = [
            "d&d", "dungeons", "dragons", "ttrpg", "tabletop rpg", "roleplaying",
            "role-playing", "role playing", "dnd", "pathfinder", "starfinder",
            "shadowrun", "call of cthulhu", "warhammer", "fantasy rpg", "rpg supplement",
            "adventure module", "campaign setting", "sourcebook", "dungeon master",
            "game master", "gm screen", "dice set", "miniatures", "tokens", "battlemaps",
            "world anvil", "foundry vtt", "roll20", "5e", "osr", "old school renaissance"
        ];

        function ksIsDndCentric(name, desc) {
            const text = ((name || "") + " " + (desc || "")).toLowerCase();
            return KS_DND_KEYWORDS.some(kw => text.indexOf(kw) !== -1);
        }

        // Builds the row AND reports which fields fell back to a default, so the
        // scale of the fabrication is measurable without changing what is sent.
        // See the header: the wire format is deliberately unchanged.
        function ksBuildRow(node) {
            const projectId = ksDecodeId(node && node.id);
            if (projectId === null) return null;

            const pledged = ksAmount(node.pledged && node.pledged.amount);
            const goal = ksAmount(node.goal && node.goal.amount);
            const backers = (typeof node.backersCount === "number") ? node.backersCount : null;
            const state = node.state ? String(node.state).toLowerCase() : null;
            const category = (node.category && node.category.name) ? node.category.name : null;
            const creator = (node.creator && node.creator.name) ? node.creator.name : null;

            const defaulted = [];
            if (pledged === null) defaulted.push("pledged_usd");
            if (goal === null) defaulted.push("goal_usd");
            if (backers === null) defaulted.push("backers_count");
            if (state === null) defaulted.push("status");
            if (category === null) defaulted.push("category");
            if (creator === null) defaulted.push("creator");

            return {
                defaulted: defaulted,
                row: {
                    project_id: projectId,
                    name: node.name || "",
                    creator: creator || "",
                    backers_count: backers || 0,
                    pledged_usd: pledged || 0,
                    goal_usd: goal || 0,
                    // Only computable when a goal was actually read. Zero here
                    // means "no progress", which is a different claim.
                    percent_funded: (goal !== null && goal > 0 && pledged !== null)
                        ? Math.round((pledged / goal) * 100) : 0,
                    category: category || "Tabletop Games",
                    status: state || "live",
                    end_date: ksDeadline(node.deadlineAt),
                    is_dnd_centric: ksIsDndCentric(node.name, node.description),
                    blurb: (node.description || "").slice(0, 300),
                    url: node.url || ""
                }
            };
        }

        function ksSummarise(built) {
            const counts = {};
            for (const b of built) {
                for (const f of b.defaulted) counts[f] = (counts[f] || 0) + 1;
            }
            const parts = Object.keys(counts).sort().map(k => k + " " + counts[k]);
            return parts.length ? parts.join(", ") : "no defaulted fields";
        }

        // ---------- end Kickstarter normalisation ----------

        const GQL = "https://www.kickstarter.com/graph";
        const csrf = (document.querySelector('meta[name="csrf-token"]') || {}).content || "";

        function buildQuery(sort, cursor) {
            const after = cursor ? ', after: "' + cursor + '"' : "";
            return "{ projects(categoryId: \"" + categoryId + "\", sort: " + sort +
                   ", first: " + perPage + after + ") {" +
                   " pageInfo { hasNextPage endCursor }" +
                   " edges { node { id name description state deadlineAt backersCount" +
                   " goal { amount currency } pledged { amount currency }" +
                   " creator { name } url category { name } } } } }";
        }

        async function fetchPage(sort, cursor) {
            const res = await fetch(GQL, {
                method: "POST",
                credentials: "include",
                headers: { "Content-Type": "application/json", "X-CSRF-Token": csrf },
                body: JSON.stringify({ query: buildQuery(sort, cursor) })
            });
            if (!res.ok) throw new Error("GraphQL HTTP " + res.status);
            const data = await res.json();
            if (data && data.errors && data.errors.length) {
                throw new Error("GraphQL errors: " + JSON.stringify(data.errors).slice(0, 200));
            }
            return (data && data.data && data.data.projects) || null;
        }

        const SORTS = ["MAGIC", "NEWEST", "END_DATE"];
        const built = [];
        const seen = new Set();
        let pagesFetched = 0;

        for (const sort of SORTS) {
            let cursor = null;
            for (let page = 0; page < maxPages; page++) {
                const result = await fetchPage(sort, cursor);
                if (!result || !result.edges || !result.edges.length) break;
                pagesFetched++;
                for (const edge of result.edges) {
                    const b = ksBuildRow(edge && edge.node);
                    if (!b || seen.has(b.row.project_id)) continue;
                    seen.add(b.row.project_id);
                    built.push(b);
                }
                if (!result.pageInfo || !result.pageInfo.hasNextPage) break;
                cursor = result.pageInfo.endCursor;
                await new Promise(r => setTimeout(r, 400));
            }
        }

        if (!built.length) {
            return {
                site: siteName, success: false,
                error: "No projects parsed (pages fetched=" + pagesFetched +
                       ", csrf=" + (csrf ? "present" : "MISSING") + ") — signed in?"
            };
        }

        // ONE request, not chunked. The bouncer's dedup guard fires on the first
        // chunk and skips every chunk after it, so chunking silently truncates
        // the day's capture to the first 500 rows.
        const res = await fetch(endpoint, {
            method: "POST",
            headers: { "Content-Type": "application/json", "X-Ritual-Key": ritualKey },
            body: JSON.stringify(built.map(b => b.row))
        });
        if (!res.ok) throw new Error("ingest HTTP " + res.status);
        const data = await res.json().catch(() => ({}));

        return {
            site: siteName,
            success: true,
            count: data.skipped ? 0 : (data.inserted || built.length),
            skipped: !!data.skipped,
            pagesFetched: pagesFetched,
            dndCentric: built.filter(b => b.row.is_dnd_centric).length,
            defaultedFields: ksSummarise(built),
            tierSummary: ksSummarise(built),
            shelves: SORTS
        };
    } catch (e) {
        return { site: siteName, success: false, error: e.message };
    }
}
