// Amazon selector probe — does the harvester's markup still exist?
//
// HOW TO RUN. Open an Amazon best-seller page in Chrome, e.g.
//   https://www.amazon.com/Best-Sellers-Books-Dungeons-Dragons-Game/zgbs/books/16215
// then F12 → Console → paste this whole file → Enter.
//
// It fetches nothing and sends nothing. It reads the page already on screen and
// prints a table of which selectors still match, plus three sample values per
// field so a selector that matches the WRONG element is visible too — a green
// match count is not the same as correct data.
//
// WHY THIS EXISTS. catalog_harvester_extension/amazon.js was ported from a
// bookmarklet whose selectors were last confirmed against a live page in April
// 2026. Amazon's markup moves. The harvester now abstains rather than inventing
// a rank, so a selector that has rotted shows up as nulls rather than as
// fabricated "Top 10" rows — but it still shows up as missing data, and this is
// the cheapest way to find out before a scheduled run does.
//
// The AMZ_SEL block below is asserted byte-identical to the one in amazon.js by
// scripts/test_amazon_harvest.js. Change one, change both.

(function () {
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

    if (!location.hostname.endsWith("amazon.com")) {
        console.log("%cNot on amazon.com — open a best-seller page first.", "color:#e11;font-weight:bold");
        return;
    }

    const cards = Array.from(document.querySelectorAll(AMZ_SEL.card))
        .filter(el => /^[A-Z0-9]{10}$/.test(el.dataset.asin || ""));

    console.log("%cAmazon selector probe", "color:#a78bfa;font-weight:bold;font-size:14px");
    console.log("URL:", location.href);
    console.log("Product cards found:", cards.length);

    if (cards.length === 0) {
        console.log("%cThe CARD selector itself matched nothing — everything below is moot.",
            "color:#e11;font-weight:bold");
        console.log("Look for the attribute that now marks a product tile and report it back.");
        return;
    }

    // Read each field exactly as the harvester does, including the parts that
    // are not a plain querySelector: the byline is picked from the clamp lines
    // by order, and rating and review count are split out of one aria-label.
    const FORMAT = /^(kindle edition|hardcover|paperback|audible audiobook|audiobook|spiral-bound|board book|mass market paperback|library binding|card book|game|toy|calendar|comic|digital)$/i;

    function readCard(el) {
        const lines = Array.from(el.querySelectorAll(AMZ_SEL.line))
            .map(n => (n.textContent || "").trim().replace(/\s+/g, " "))
            .filter(Boolean);

        let title = lines[0] || "";
        if (!title) {
            const alt = el.querySelector(AMZ_SEL.titleAlt);
            title = (alt && (alt.getAttribute("title") || alt.textContent) || "").trim();
        }

        let byline = null;
        for (const t of lines) {
            if (!t || t === title) continue;
            if (FORMAT.test(t) || /^\d+\s*formats?\s+available$/i.test(t) ||
                /^\d+\s*pts\.?$/i.test(t) || /^\$/.test(t) || /^#\d+$/.test(t) ||
                /out of 5/i.test(t)) continue;
            byline = t;
            break;
        }

        const rankEl = el.querySelector(AMZ_SEL.rank);
        const priceEl = el.querySelector(AMZ_SEL.price);
        const starsEl = el.querySelector(AMZ_SEL.stars);
        const starsLabel = starsEl ? starsEl.getAttribute("aria-label") : null;
        const priceText = priceEl ? (priceEl.textContent || "").trim() : null;

        const rMatch = starsLabel && starsLabel.match(/([\d.]+)\s*out of\s*5/i);
        const nMatch = starsLabel && starsLabel.match(/([\d,]+)\s*(?:ratings?|reviews?)/i);

        return {
            title: title || null,
            byline: byline,
            rank: rankEl ? (rankEl.textContent || "").trim() : null,
            // A price must look like money — "18 pts" is reward points.
            price: priceText && /[$£€¥]/.test(priceText) ? priceText : null,
            rating: rMatch ? rMatch[1] : null,
            reviews: nMatch ? nMatch[1] : null
        };
    }

    const fields = ["title", "byline", "rank", "price", "rating", "reviews"];
    const rows = [];
    const samples = {};
    const read = cards.map(readCard);

    for (const f of fields) {
        const vals = read.map(r => r[f]).filter(v => v !== null && v !== "");
        samples[f] = vals.slice(0, 3).map(v => String(v).slice(0, 70));
        rows.push({
            field: f,
            matched: vals.length + "/" + cards.length,
            pct: Math.round((vals.length / cards.length) * 100) + "%",
            verdict: vals.length === 0 ? "BROKEN"
                   : vals.length < cards.length * 0.5 ? "PARTIAL"
                   : "ok"
        });
    }

    console.table(rows);

    console.log("%cSample values (a selector can match the WRONG element and still score 100%)",
        "color:#a78bfa;font-weight:bold");
    for (const f of fields) {
        console.log("  " + f.padEnd(7) + ":", samples[f].length ? samples[f] : "(nothing)");
    }

    // The rank badge is the one that matters most: it is the field the
    // harvester now abstains on, so if it has rotted the capture still
    // succeeds and simply records no ranks at all.
    const rankRow = rows.find(r => r.field === "rank");
    console.log("");
    if (rankRow.verdict === "BROKEN") {
        console.log("%cRANK BADGE NOT FOUND.", "color:#e11;font-weight:bold");
        console.log("Every product would be captured with rank null and no tier — the capture");
        console.log("would look successful and carry no ranking at all. Right-click the '#1'");
        console.log("badge on the page, Inspect, and report the element's class list.");
    } else if (rankRow.verdict === "PARTIAL") {
        console.log("%cRank badge found on only " + rankRow.pct + " of cards.", "color:#e90;font-weight:bold");
        console.log("Measured 2026-09-23: BOTH Best Sellers and New Releases carried rank");
        console.log("badges at 100%, so PARTIAL here is a real regression, not a page type.");
    } else {
        console.log("%cRank badge healthy (" + rankRow.pct + ").", "color:#2a2;font-weight:bold");
    }

    console.log("");
    console.log("Copy this whole output back to Claude — the table alone is not enough,");
    console.log("the sample values are what show a selector matching the wrong thing.");
})();
