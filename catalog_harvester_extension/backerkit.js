// Arcane Incursion — BackerKit harvest
//
// WHY THE EXTENSION IS THE ONLY HOME THIS CAN HAVE. Unlike the other streams,
// BackerKit is not merely unscheduled — it is unreachable from a server.
// cloud_functions/backerkit_harvester/main.py takes a deterministic 403 from
// BackerKit's edge because GCP datacenter IPs are flagged, persistent in Cloud
// Run logs since at least 2026-05-13. The fix is not a better retry: it is a
// residential IP and a real signed-in session, which means a real browser.
//
// ─────────────────────────────────────────────────────────────────────────
// WHAT CHANGED, AND WHY THE PREVIOUS TWO DESIGNS WERE WRONG
//
// V1 fetched ONE curated collection as Inertia JSON and got 10 projects.
// V2 walked three collections and got 28. Both were the wrong page.
//
//     /c/collections/<slug>          curated shelf, 10 items, JSON
//     /c/categories/<slug>/projects  the real listing, 700 items, HTML
//
// The collections endpoint cannot be paged and cannot be sorted: `page`,
// `offset`, `per_page` and all ten `sort_by` values tried were SILENTLY
// IGNORED, every one returning the same ten projects with HTTP 200. Counting
// results would have read as fourteen successes; only comparing returned ids
// against a running union exposed it. See
// scripts/probe_backerkit_pagination.js.
//
// The categories page has no JSON representation at all (406 to
// Accept: application/json) and is server-rendered HTML behind infinite
// scroll, 28 projects per batch. So this reads the DOM of a real tab, the
// same way the DMs Guild and DriveThruRPG harvesters read metal.php.
//
// ─────────────────────────────────────────────────────────────────────────
// CURRENCY IS NOT DECORATION
//
// Measured across 700 projects: $ 67%, then EUR, GBP, C$, A$, NZ$, CHF, S$,
// with exactly one currency per card in 696 of 700. The old bkAmount stripped
// every non-digit — the currency marker included — and the result went to a
// column named funding_usd, so A$228,597 was stored as 228,597 US dollars.
// Same shape as the DMs Guild price bug, and just as invisible: a plausible
// number in a USD column is not something any downstream check would flag.
//
// funding_usd is now populated ONLY when the currency really is USD. Every
// amount is kept verbatim in funding_amount with funding_currency beside it.
// Conversion belongs in a view over an FX table — convert at ingest and the
// original is gone, along with any chance of revising the method.
//
// ─────────────────────────────────────────────────────────────────────────
// FIELDS COME FROM THEIR OWN NODES, NOT FROM SLICED-UP TEXT
//
// Every earlier attempt parsed a concatenated textContent, which is why the
// blurb boundary was guesswork. The card has dedicated elements, and the full
// text of the clipped ones lives in a `title` attribute:
//
//     h3[title]                  the title, untruncated
//     p[title]                   the summary, untruncated (CSS line-clamps
//                                the visible copy; the attribute does not)
//     a[title]                   the creator's display name
//     "#N Most funded"           the rank badge
//     "$13,298,059" / "of $250,000 goal" / "55,972" / "backers"
//
// Tailwind arbitrary-value classes (shadow-[inset_0_1px_0_0_rgba(...)]) are
// deliberately NOT used as selectors: they need heavy escaping and they are
// build artifacts that change without warning. The card is found by walking
// up from a project link while exactly ONE DISTINCT project url is inside.
// Counting links instead of distinct urls is what made an earlier probe
// report 0% currency coverage — there are ~3 links per card, so the walk
// stopped on an inner wrapper and returned a partial card that looked whole.

const BK_CATEGORY = "role-playing-games";
const BK_LANDING =
    "https://www.backerkit.com/c/categories/" + BK_CATEGORY + "/projects?sort_by=trending";
const BK_MAX_SCROLLS = 40;      // measured: plateaus at 700 after ~25 batches of 28
const BK_SCROLL_WAIT_MS = 1100;
const BK_ENDPOINT =
    ENDPOINT.replace("system/library/ingest-catalog", "system/backerkit/ingest-projects");

async function runBackerkitExtractionInPage(siteName, ritualKey, endpoint,
                                            categorySlug, maxScrolls, scrollWaitMs) {
    try {
        if (!location.hostname.endsWith("backerkit.com")) {
            return { site: siteName, success: false, error: "Not on backerkit.com (url=" + location.href + ")" };
        }
        if (location.pathname.indexOf("/c/categories/") === -1) {
            return {
                site: siteName, success: false,
                error: "Not on a category listing (url=" + location.href +
                       ") — a sign-in redirect looks like this"
            };
        }

        // ---------- BackerKit normalisation (sliced by scripts/test_crowdfunding_harvest.js) ----------

        // Eight markers measured on the live listing, plus the ones BackerKit
        // could plausibly add. Order does NOT matter, despite appearances:
        // alternation picks the leftmost POSITION first and only prefers an
        // earlier alternative among those matching at the SAME position. At
        // the "A" of "A$228,597" the bare "$" cannot match, so "A$" is the
        // only candidate. Verified by running both orderings over all eight.
        // (An earlier version of this comment claimed order was load-bearing.
        // It is not, and a mutation reordering the list was correctly GREEN --
        // there was nothing to catch. What IS load-bearing is that every
        // marker round-trips, which the suite checks one by one.)
        const BK_CURRENCIES = ["NZ$", "CA$", "US$", "HK$", "C$", "A$", "S$", "R$",
                               "$", "€", "£", "¥", "CHF", "SEK", "NOK", "DKK", "PLN"];
        const BK_CUR_RE = "(" + BK_CURRENCIES.map(function (c) {
            return c.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
        }).join("|") + ")";

        // Only these are actually US dollars. Everything else goes to
        // funding_amount + funding_currency and leaves funding_usd null.
        function bkIsUsd(cur) { return cur === "$" || cur === "US$"; }

        function bkNum(s) {
            if (s === null || s === undefined) return null;
            const n = parseFloat(String(s).replace(/,/g, ""));
            return Number.isFinite(n) ? n : null;
        }

        // Two layouts, both measured: 93% carry a goal, 7% (curated storefront
        // entries) say "funds raised" instead and have none. Together 100%.
        function bkMoney(text) {
            const t = String(text || "");
            const goal = new RegExp(BK_CUR_RE + "\\s?([\\d][\\d,\\.]*)\\s*of\\s*" +
                                    BK_CUR_RE + "\\s?([\\d][\\d,\\.]*)\\s*goal").exec(t);
            if (goal) {
                return { currency: goal[1], raised: bkNum(goal[2]), goal: bkNum(goal[4]) };
            }
            const funds = new RegExp(BK_CUR_RE + "\\s?([\\d][\\d,\\.]*)\\s*funds\\s*raised").exec(t);
            if (funds) {
                return { currency: funds[1], raised: bkNum(funds[2]), goal: null };
            }
            return { currency: null, raised: null, goal: null };
        }

        function bkBackers(text) {
            const m = /([\d][\d,]*)\s*backers?/.exec(String(text || ""));
            return m ? bkNum(m[1]) : null;
        }

        // "20 days left" -> 20 and live. "Ended" -> null and ended, because the
        // card states no end DATE: we know it finished, not when. The old code
        // returned 0 for that, which is indistinguishable from ending today.
        function bkTiming(text) {
            const t = String(text || "");
            const d = /(\d+)\s*days?\s*left/.exec(t);
            if (d) return { days: parseInt(d[1], 10), status: "live" };
            const h = /(\d+)\s*hours?\s*left/.exec(t);
            if (h) return { days: 0, status: "live" };
            if (/[Ee]nded/.test(t)) return { days: null, status: "ended" };
            return { days: null, status: null };
        }

        // "#1 Most funded", "#2 Trending this week" — the label varies with the
        // sort, so only the number is kept.
        function bkRank(text) {
            const m = /#(\d+)\s/.exec(String(text || ""));
            return m ? parseInt(m[1], 10) : null;
        }

        // /c/projects/<creator-slug>/<project-slug>
        function bkIdsFromUrl(url) {
            const parts = String(url || "").split("?")[0].split("/").filter(Boolean);
            const i = parts.indexOf("projects");
            if (i === -1 || parts.length < i + 3) return { projectId: null, creatorSlug: null };
            return { projectId: parts[i + 2], creatorSlug: parts[i + 1] };
        }

        function bkPrettySlug(slug) {
            if (!slug) return null;
            return String(slug).split("-").filter(Boolean)
                .map(function (w) { return w.charAt(0).toUpperCase() + w.slice(1); }).join(" ");
        }

        const BK_DND = ["5e", "5th edition", "d&d", "dungeons", "2024 compatible",
                        "black flag", "tales of the valiant", "mcdm", "dragonbane"];
        const BK_OSR = ["osr", "old school", "old-school", "b/x", "odnd", "ad&d"];

        // Title AND blurb. The blurb is where a project says what system it is
        // for — "Ink Ribbon - A Survival Horror Tabletop RPG" tells you nothing,
        // while its summary names Resident Evil and Silent Hill. The project id
        // is still never passed: it is a slug, which is not prose.
        function bkClassify(title, blurb) {
            const t = (String(title || "") + " " + String(blurb || "")).toLowerCase();
            if (BK_DND.some(function (k) { return t.indexOf(k) !== -1; })) return "5e Compatible";
            if (BK_OSR.some(function (k) { return t.indexOf(k) !== -1; })) return "OSR";
            return "RPG (Other)";
        }

        function bkBuildRow(card) {
            const ids = bkIdsFromUrl(card.url);
            const title = (card.title || "").trim();
            // Both are REQUIRED in BigQuery. A row without them would be dropped
            // silently by skip_invalid_rows, so it is never sent.
            if (!ids.projectId || !title) return null;

            const money = bkMoney(card.text);
            const backers = bkBackers(card.text);
            const timing = bkTiming(card.text);
            const creator = (card.creator || "").trim() || bkPrettySlug(ids.creatorSlug);
            const blurb = (card.blurb || "").trim() || null;
            const usd = money.currency !== null && bkIsUsd(money.currency);

            const defaulted = [];
            if (money.raised === null) defaulted.push("funding_amount");
            if (money.currency === null) defaulted.push("funding_currency");
            if (money.goal === null) defaulted.push("goal_amount");
            if (backers === null) defaulted.push("backers_count");
            if (timing.days === null) defaulted.push("days_remaining");
            if (blurb === null) defaulted.push("blurb");
            if (!card.creator) defaulted.push("creator_from_slug");
            if (money.currency !== null && !usd) defaulted.push("non_usd_" + money.currency);

            return {
                defaulted: defaulted,
                row: {
                    project_id: ids.projectId,
                    title: title,
                    creator: creator,
                    // Null, never 0. The bouncer no longer re-zeroes these.
                    funding_usd: usd ? money.raised : null,
                    funding_amount: money.raised,
                    funding_currency: money.currency,
                    goal_amount: money.goal,
                    backers_count: backers,
                    days_remaining: timing.days,
                    status: timing.status,
                    blurb: blurb,
                    category: card.category || null,
                    trending_rank: bkRank(card.rankText),
                    system_tag: bkClassify(title, blurb),
                    source_url: card.url
                }
            };
        }

        function bkSummarise(built) {
            const counts = {};
            for (const b of built) {
                for (const f of b.defaulted) counts[f] = (counts[f] || 0) + 1;
            }
            const parts = Object.keys(counts).sort().map(function (k) { return k + " " + counts[k]; });
            return parts.length ? parts.join(", ") : "no defaulted fields";
        }

        // ---------- end BackerKit normalisation ----------

        const PROJECT_LINK = 'a[href*="/c/projects/"]';
        const urlOf = function (a) { return a.href.split("?")[0]; };
        const distinctProjects = function (el) {
            return new Set(Array.from(el.querySelectorAll(PROJECT_LINK)).map(urlOf)).size;
        };
        const countLoaded = function () {
            return new Set(Array.from(document.querySelectorAll(PROJECT_LINK)).map(urlOf)).size;
        };

        // Infinite scroll, 28 per batch. Stops on a genuine plateau rather than
        // a fixed count: 700 is where it settled on 2026-09-25, but 700 is
        // exactly 25 x 28, which is the shape of a server-side cap rather than
        // a natural end — so the loop must not hard-code it.
        let previous = -1, scrolls = 0;
        for (let i = 0; i < maxScrolls; i++) {
            window.scrollTo(0, document.body.scrollHeight);
            await new Promise(function (r) { setTimeout(r, scrollWaitMs); });
            scrolls++;
            const now = countLoaded();
            if (now === previous) break;
            previous = now;
        }
        window.scrollTo(0, 0);

        // Walk up while only ONE DISTINCT project is inside, keeping the LAST
        // such ancestor — the largest element that still describes one project.
        function cardFor(a) {
            let el = a.parentElement, best = null, depth = 0;
            while (el && depth < 12) {
                if (distinctProjects(el) === 1) best = el; else break;
                el = el.parentElement; depth++;
            }
            return best;
        }
        // Clipped elements keep their full text in `title`; prefer it.
        function fullText(el) {
            if (!el) return null;
            const a = el.getAttribute && el.getAttribute("title");
            const t = (a && a.trim()) || el.textContent || "";
            return t.replace(/\s+/g, " ").trim() || null;
        }

        const seen = new Set();
        const cards = [];
        document.querySelectorAll(PROJECT_LINK).forEach(function (a) {
            const url = urlOf(a);
            if (seen.has(url)) return;
            const el = cardFor(a);
            if (!el) return;
            seen.add(url);

            const titleEl = el.querySelector("h3");
            const blurbEl = el.querySelector("p");
            const text = (el.textContent || "").replace(/\s+/g, " ").trim();
            const titleText = fullText(titleEl);

            // The creator's name is on an a[title] that is not the title itself.
            let creator = null;
            const titled = Array.from(el.querySelectorAll("a[title], span[title]"));
            for (const c of titled) {
                const v = fullText(c);
                if (v && v !== titleText) { creator = v; break; }
            }

            const rankEl = Array.from(el.querySelectorAll("div, span"))
                .map(function (n) { return (n.textContent || "").trim(); })
                .filter(function (t) { return /^#\d+\s/.test(t); })[0] || null;

            cards.push({
                url: url, text: text, title: titleText, blurb: fullText(blurbEl),
                creator: creator, rankText: rankEl,
                category: /Role-Playing Games/.test(text) ? "Role-Playing Games" : null
            });
        });

        const built = cards.map(bkBuildRow).filter(Boolean);
        if (!built.length) {
            return {
                site: siteName, success: false,
                error: "Parsed 0 rows from " + cards.length + " cards on " + categorySlug +
                       " after " + scrolls + " scrolls (project_id and title are both required)"
            };
        }

        // ONE request. The bouncer's dedup guard on this route is a
        // whole-request date check, so a second request would be refused and
        // the day's capture truncated.
        const post = await fetch(endpoint, {
            method: "POST",
            headers: { "Content-Type": "application/json", "X-Ritual-Key": ritualKey },
            body: JSON.stringify(built.map(function (b) { return b.row; }))
        });
        if (!post.ok) throw new Error("ingest HTTP " + post.status);
        const out = await post.json().catch(function () { return {}; });

        const usdRows = built.filter(function (b) { return b.row.funding_usd !== null; }).length;
        const coverage = "scrolls " + scrolls + ", cards " + cards.length +
                         ", rows " + built.length + ", usd " + usdRows;

        return {
            site: siteName,
            success: true,
            count: out.skipped ? 0 : (out.inserted || built.length),
            skipped: !!out.skipped,
            projectsSeen: cards.length,
            scrolls: scrolls,
            usdRows: usdRows,
            defaultedFields: bkSummarise(built),
            tierSummary: bkSummarise(built) + " | " + coverage,
            coverage: coverage,
            shelves: [categorySlug]
        };
    } catch (e) {
        return { site: siteName, success: false, error: e.message };
    }
}
