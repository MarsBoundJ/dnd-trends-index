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

        // A binding, a format note, or a Kindle points line — none of which is a
        // person. Measured 2026-09-23: the previous byline selector returned
        // "Kindle Edition", "Hardcover" and "Paperback" for most cards.
        function amzIsNotAByline(s) {
            const t = String(s || "").trim();
            if (!t) return true;
            if (/^(kindle edition|hardcover|paperback|audible audiobook|audiobook|spiral-bound|board book|mass market paperback|library binding|card book|game|toy|calendar|comic|digital)$/i.test(t)) return true;
            if (/^\d+\s*formats?\s+available$/i.test(t)) return true;
            if (/^\d+\s*pts\.?$/i.test(t)) return true;
            if (/^\$/.test(t)) return true;
            if (/^#\d+$/.test(t)) return true;
            if (/out of 5/i.test(t)) return true;
            return false;
        }

        // Amazon renders the title and the byline as the SAME clamp element,
        // separated only by order, and a card may have no byline at all (the
        // 2024 Player's Handbook does not). So: the first line is the title,
        // and the byline is the next line that is neither a repeat of the title
        // nor a format label. No byline found means null — never the format.
        function pickByline(lines, title) {
            if (!Array.isArray(lines)) return null;
            for (let i = 0; i < lines.length; i++) {
                const t = String(lines[i] || "").trim();
                if (!t || t === title) continue;
                if (amzIsNotAByline(t)) continue;
                return t;
            }
            return null;
        }

        // A price must look like money. `.a-color-price` on a Kindle card
        // carries "18 pts" — reward points — which any bare number parse turns
        // into a confident $18.00.
        function amzPrice(v) {
            if (v === null || v === undefined) return null;
            const s = String(v);
            if (!/[$£€¥]/.test(s)) return null;
            return amzFloat(s);
        }

        // Both numbers live in ONE aria-label: "4.8 out of 5 stars, 3,762 ratings".
        // Splitting them apart here keeps the two-number hazard in one place.
        function amzStars(label) {
            const s = String(label || "");
            const r = s.match(/([\d.]+)\s*out of\s*5/i);
            const n = s.match(/([\d,]+)\s*(?:ratings?|reviews?)/i);
            return {
                rating: r ? amzFloat(r[1]) : null,
                reviews: n ? amzInt(n[1]) : null
            };
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

        function buildAmazonRows(raw, collectedDate) {
            const rank = amzInt(raw.rankText);
            const price = amzPrice(raw.priceText);
            const stars = amzStars(raw.starsLabel);
            const rating = stars.rating;
            const reviews = stars.reviews;
            const tier = amazonRankTier(rank);

            return {
                asin: raw.asin,
                rank: rank,
                catalog: {
                    collected_date: collectedDate,
                    source: "Amazon",
                    title: raw.title,
                    publisher: amzText(pickByline(raw.lines, raw.title)),
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
                    author: amzText(pickByline(raw.lines, raw.title)),
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

        // ---------- Selectors (mirrored by scripts/probe_amazon_selectors.js) ----------
        //
        // Amazon's markup moves, and these were last confirmed against a live
        // page in April 2026. They are named here rather than inlined so the
        // DevTools probe can test the SAME strings production uses — a probe
        // that checks different selectors is worse than no probe, because it
        // reports health for markup nobody reads.
        //
        // scripts/test_amazon_harvest.js asserts this block is byte-identical
        // to the probe's copy. If you change one, change both or the suite
        // fails.
        const AMZ_SEL = {
            card:   "[data-asin]",
            // Title AND byline are both clamp divs, separated only by order,
            // and the class carries a build hash (_g3dy1, _1Fn1y) that changes
            // between deploys — so match the stable substring, never the hash.
            // Measured 2026-09-23: the Player's Handbook card has one such line
            // (no byline at all); "The Dread from the Drows" has two.
            line:   "[class*=\"p13n-sc-css-line-clamp\"], .p13n-sc-truncated, [class*=\"p13n-sc-truncated\"]",
            // Fallback for layouts predating the clamp divs.
            titleAlt: "a[title], a.a-link-normal span.a-text-normal",
            rank:   ".zg-bdg-text, .zg-badge-text, [class*=\"zg-bdg\"], [class*=\"zg-badge\"]",
            // Deliberately NOT .a-color-price: on a Kindle card that class
            // carries "18 pts" (reward points), which reads as a plausible
            // $18.00. amzPrice also requires a currency symbol, so this is
            // guarded twice.
            price:  "[class*=\"p13n-sc-price\"], .p13n-sc-price",
            // ONE element carries both numbers:
            //   aria-label="4.8 out of 5 stars, 3,762 ratings"
            // The previous selectors looked for a <span>; the label sits on an
            // <a>, which is why rating and review both read 0/30 on 2026-09-23.
            stars:  "[aria-label*=\"out of 5\"]"
        };
        // ---------- end selectors ----------

        // Returns raw TEXT per card. No parsing here on purpose — see above.
        function parseAmazonCards(doc, label, listType) {
            const out = [];
            const cards = Array.from(doc.querySelectorAll(AMZ_SEL.card))
                .filter(el => /^[A-Z0-9]{10}$/.test(el.dataset.asin || ""));

            for (const el of cards) {
                // Every clamp line in order. The first is the title; the byline
                // is chosen from the rest by pickByline, which knows a format
                // label from a person.
                const lines = Array.from(el.querySelectorAll(AMZ_SEL.line))
                    .map(n => (n.textContent || "").trim().replace(/\s+/g, " "))
                    .filter(Boolean);

                let title = lines[0] || "";
                if (!title) {
                    const alt = el.querySelector(AMZ_SEL.titleAlt);
                    title = (alt && (alt.getAttribute("title") || alt.textContent) || "").trim();
                }
                if (!title) continue;

                const rankEl = el.querySelector(AMZ_SEL.rank);
                const priceEl = el.querySelector(AMZ_SEL.price);
                const starsEl = el.querySelector(AMZ_SEL.stars);

                out.push({
                    asin: el.dataset.asin,
                    title: title,
                    lines: lines,
                    rankText: rankEl ? rankEl.textContent : null,
                    priceText: priceEl ? priceEl.textContent : null,
                    starsLabel: starsEl ? starsEl.getAttribute("aria-label") : null,
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
