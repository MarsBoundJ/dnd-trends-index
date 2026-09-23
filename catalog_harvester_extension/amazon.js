// Arcane Incursion — Amazon best-seller harvest
//
// Ported from scripts/amazon_bookmarklet.js, which stopped producing rows on
// 2026-05-18 for the only reason a manual capture ever stops: nobody clicked it.
// The extraction logic was never broken. It had no runner. This gives it the
// same scheduled, eviction-resilient runner the DMs Guild and DriveThruRPG
// harvests already use.
//
// WHY A TAB AND NOT A WORKER FETCH. The detail pass talks to a JSON API from the
// service worker directly. Amazon has no such API here — these are rendered
// best-seller pages, and reading them needs DOMParser, which a Manifest V3
// service worker does not have. So Amazon is harvested the same way DMs Guild
// is: open a tab, inject an extractor, let the page's own origin do the
// fetching. The injected function is serialized by chrome.scripting, so it
// cannot reference anything in this file's scope — everything it needs is
// either inside it or passed as an argument. That is why the constants below
// are handed in rather than closed over.
//
// WHAT CHANGED IN THE PORT, AND WHY IT HAD TO. The bookmarklet recorded an
// unmeasured value as zero:
//
//     const rank = rankEl ? parseInt(...) || 0 : 0;
//     const rankTier = item.rank <= 10 ? 'Top 10' : ...
//
// A product whose rank badge did not parse was therefore written as rank 0 and
// tier "Top 10" — the best tier on the scale, asserted about a product we could
// not rank at all. That is the same defect as the V9 DMs Guild tier bug: not a
// crash, not a gap, but a confident wrong number that looks exactly like a
// right one.
//
// It is contained today, by two filters that were written for other reasons:
// setup_catalog_market_silver.sql takes Amazon only WHERE rank > 0, and takes
// catalog_supply only WHERE source IN ('DMs Guild','DriveThruRPG'). So the
// fabricated tier never reaches norm_catalog_market or the cross-platform join.
// Two things are still wrong with leaving it: the raw tables hold assertions
// nobody measured, and `rank > 0` DROPS those products silently rather than
// flagging them, so a parser regression reads as a smaller catalogue rather
// than as a broken parse.
//
// So this port abstains. An unmeasured rank is null, and a null rank has no
// tier — never "Top 10". Same for price, rating and review count.
//
// Rows carry tag "V12-ext" where the bookmarklet wrote "V10-auto", so pre-fix
// and post-fix Amazon rows stay distinguishable in BigQuery without a date
// lookup.

// Verified live in-browser April 2026; unchanged in the port.
const AMAZON_SOURCES = [
    ["/Best-Sellers-Books-Dungeons-Dragons-Game/zgbs/books/16215",               "D&D Books",          "Best Sellers"],
    ["/Best-Sellers-Books-Fantasy-Gaming/zgbs/books/16211",                      "All RPG Books",      "Best Sellers"],
    ["/Best-Sellers-Toys-Games-Games-Accessories/zgbs/toys-and-games/166220011", "Games & Accessories","Best Sellers"],
    ["/Best-Sellers-Toys-Games-Role-Playing-Dice/zgbs/toys-and-games/1265808011","RPG Dice",           "Best Sellers"],
    ["/gp/new-releases/books/16215",                                             "D&D Books",          "New Releases"],
    ["/gp/new-releases/toys-and-games/166220011",                                "Games & Accessories","New Releases"],
    ["/gp/most-wished-for/books/16215",                                          "D&D Books",          "Most Wished For"]
];

const AMAZON_MAX_PAGES = 6;   // ~20 items/page, so this covers the top 100
const AMAZON_LANDING = "https://www.amazon.com" + AMAZON_SOURCES[0][0];
const AMAZON_RANKS_ENDPOINT =
    ENDPOINT.replace("system/library/ingest-catalog", "system/amazon/ingest-ranks");

// ---------- In-page extraction (serialized and injected) ----------
// Runs inside an amazon.com tab. Cannot reference anything above this line.

async function runAmazonExtractionInPage(siteName, ritualKey, catalogEndpoint, ranksEndpoint,
                                         sources, maxPages, chunkSize) {
    try {
        const today = new Date().toISOString().split("T")[0];

        // Every fetch below targets https://www.amazon.com. From any other
        // origin those are cross-origin, get blocked, and the run ends looking
        // like an Amazon layout change instead of a wrong tab.
        if (!location.hostname.endsWith("amazon.com")) {
            return { site: siteName, success: false, error: "Not on amazon.com (url=" + location.href + ")" };
        }

        // ---------- Amazon normalisation (sliced by scripts/test_amazon_harvest.js) ----------
        //
        // Pure, and deliberately the only place a raw string becomes a number.
        // parseAmazonCards below returns TEXT exactly as the page gave it; every
        // coercion happens here, so the abstain rule has one home and the tests
        // can reach it without a DOM.

        function amzInt(v) {
            if (v === null || v === undefined) return null;
            const digits = String(v).replace(/[^\d]/g, "");
            if (digits === "") return null;
            const n = parseInt(digits, 10);
            return Number.isFinite(n) ? n : null;
        }

        function amzFloat(v) {
            if (v === null || v === undefined) return null;
            const cleaned = String(v).replace(/[^\d.]/g, "");
            if (cleaned === "" || cleaned === ".") return null;
            const n = parseFloat(cleaned);
            return Number.isFinite(n) ? n : null;
        }

        function amzText(v) {
            if (v === null || v === undefined) return null;
            const s = String(v).trim().replace(/^by\s+/i, "").trim();
            return s === "" ? null : s;
        }

        // A rank we could not read has no tier. The bookmarklet's version of
        // this returned "Top 10" for rank 0, which is the whole bug.
        function amazonRankTier(rank) {
            if (rank === null || rank === undefined) return null;
            if (rank <= 0) return null;
            if (rank <= 10) return "Top 10";
            if (rank <= 50) return "Top 50";
            if (rank <= 100) return "Top 100";
            return "Top 200";
        }

        // Extracts the leading number from an aria-label like "4.8 out of 5 stars".
        // Not amzFloat: that strips every non-digit, so "4.8 out of 5" would
        // become 4.85 — a plausible-looking rating assembled from two numbers.
        function amzRatingFromLabel(label) {
            if (!label) return null;
            const m = String(label).match(/([\d.]+)\s*out of/i);
            return m ? amzFloat(m[1]) : null;
        }

        function buildAmazonRows(raw, collectedDate) {
            const rank = amzInt(raw.rankText);
            const price = amzFloat(raw.priceText);
            const rating = amzRatingFromLabel(raw.ratingLabel);
            const reviews = amzInt(raw.reviewLabel);
            const tier = amazonRankTier(rank);

            return {
                asin: raw.asin,
                rank: rank,
                catalog: {
                    collected_date: collectedDate,
                    source: "Amazon",
                    title: raw.title,
                    publisher: amzText(raw.author),
                    seller_tier: tier,
                    price: price,
                    rating: rating,
                    tags: ["Amazon", raw.label, raw.listType, "V12-ext"],
                    system_tag: "",
                    edition_tag: "",
                    asin: raw.asin
                },
                rankRow: {
                    asin: raw.asin,
                    rank: rank,
                    price_cents: price === null ? null : Math.round(price * 100),
                    date: collectedDate,
                    title: raw.title,
                    category: raw.listType + ": " + raw.label,
                    author: amzText(raw.author),
                    rating: rating,
                    review_count: reviews
                }
            };
        }

        // Keep the best (lowest) rank per ASIN. A null rank never displaces a
        // real one, and never wins a comparison by being treated as zero.
        function dedupeByAsin(rows) {
            const best = new Map();
            for (const row of rows) {
                const seen = best.get(row.asin);
                if (!seen) { best.set(row.asin, row); continue; }
                if (row.rank === null) continue;
                if (seen.rank === null || row.rank < seen.rank) best.set(row.asin, row);
            }
            return [...best.values()];
        }

        // ---------- end Amazon normalisation ----------

        // Returns raw TEXT per card. No parsing here on purpose — see above.
        function parseAmazonCards(doc, label, listType) {
            const out = [];
            const cards = Array.from(doc.querySelectorAll("[data-asin]"))
                .filter(el => /^[A-Z0-9]{10}$/.test(el.dataset.asin || ""));

            for (const el of cards) {
                const titleEl = el.querySelector(
                    ".p13n-sc-truncated, [class*=\"p13n-sc-truncated\"], " +
                    "[class*=\"p13n-sc-css-line-clamp\"], " +
                    "a[title], a.a-link-normal span.a-text-normal");
                const title = (titleEl && (titleEl.getAttribute("title") || titleEl.textContent) || "").trim();
                if (!title) continue;

                const rankEl = el.querySelector(
                    ".zg-bdg-text, .zg-badge-text, [class*=\"zg-bdg\"], [class*=\"zg-badge\"]");
                const priceEl = el.querySelector(
                    ".p13n-sc-price, [class*=\"p13n-sc-price\"], .a-color-price");
                const authorEl = el.querySelector(
                    "span.a-color-secondary, .a-row .a-color-base.a-size-small");
                const ratingEl = el.querySelector("span[aria-label*=\"out of 5\"]");
                const reviewEl = el.querySelector(
                    "span[aria-label*=\"rating\"], span[aria-label*=\"review\"]");

                out.push({
                    asin: el.dataset.asin,
                    title: title,
                    rankText: rankEl ? rankEl.textContent : null,
                    priceText: priceEl ? priceEl.textContent : null,
                    author: authorEl ? authorEl.textContent : null,
                    ratingLabel: ratingEl ? ratingEl.getAttribute("aria-label") : null,
                    reviewLabel: reviewEl ? reviewEl.getAttribute("aria-label") : null,
                    label: label,
                    listType: listType
                });
            }
            return out;
        }

        async function fetchPage(path, pageNum) {
            const sep = path.includes("?") ? "&" : "?";
            const url = "https://www.amazon.com" + path + (pageNum > 1 ? sep + "pg=" + pageNum : "");
            try {
                const res = await fetch(url, { credentials: "include" });
                if (!res.ok) return null;
                return new DOMParser().parseFromString(await res.text(), "text/html");
            } catch (e) {
                return null;
            }
        }

        // ---------- Harvest ----------
        const built = [];
        const sourceCounts = {};
        let pagesFetched = 0, pagesFailed = 0;

        for (const [path, label, listType] of sources) {
            const docs = await Promise.all(
                Array.from({ length: maxPages }, (_, i) => fetchPage(path, i + 1)));

            const perSource = [];
            for (const doc of docs) {
                if (!doc) { pagesFailed++; continue; }
                pagesFetched++;
                for (const raw of parseAmazonCards(doc, label, listType)) {
                    perSource.push(buildAmazonRows(raw, today));
                }
            }
            const deduped = dedupeByAsin(perSource);
            sourceCounts[listType + ": " + label] = deduped.length;
            built.push(...deduped);
        }

        if (built.length === 0) {
            return {
                site: siteName, success: false,
                error: "No products parsed (pages ok=" + pagesFetched + ", failed=" + pagesFailed +
                       ") — Amazon layout may have changed"
            };
        }

        // Across sources, keep one row per ASIN at its best rank.
        const finalRows = dedupeByAsin(built);
        const unranked = finalRows.filter(r => r.rank === null).length;

        // ---------- Send ----------
        async function postChunks(endpoint, rows) {
            let ok = 0, skipped = false;
            for (let i = 0; i < rows.length; i += chunkSize) {
                const chunk = rows.slice(i, i + chunkSize);
                const res = await fetch(endpoint, {
                    method: "POST",
                    headers: { "Content-Type": "application/json", "X-Ritual-Key": ritualKey },
                    body: JSON.stringify(chunk)
                });
                if (!res.ok) throw new Error("ingest HTTP " + res.status + " at " + endpoint);
                const data = await res.json().catch(() => ({}));
                if (data.skipped) { skipped = true; break; }
                ok += data.inserted || chunk.length;
            }
            return { ok: ok, skipped: skipped };
        }

        const catalog = await postChunks(catalogEndpoint, finalRows.map(r => r.catalog));
        const ranks = await postChunks(ranksEndpoint, finalRows.map(r => r.rankRow));

        return {
            site: siteName,
            success: true,
            count: catalog.ok,
            rankRows: ranks.ok,
            ranksSkipped: ranks.skipped,
            unrankedProducts: unranked,
            pagesFetched: pagesFetched,
            pagesFailed: pagesFailed,
            shelves: Object.keys(sourceCounts),
            tierSummary: sourceCounts
        };
    } catch (e) {
        return { site: siteName, success: false, error: e.message };
    }
}
