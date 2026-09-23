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
        title:  ".p13n-sc-truncated, [class*=\"p13n-sc-truncated\"], [class*=\"p13n-sc-css-line-clamp\"], a[title], a.a-link-normal span.a-text-normal",
        rank:   ".zg-bdg-text, .zg-badge-text, [class*=\"zg-bdg\"], [class*=\"zg-badge\"]",
        price:  ".p13n-sc-price, [class*=\"p13n-sc-price\"], .a-color-price",
        author: "span.a-color-secondary, .a-row .a-color-base.a-size-small",
        rating: "span[aria-label*=\"out of 5\"]",
        review: "span[aria-label*=\"rating\"], span[aria-label*=\"review\"]"
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

    // Per-field: how many cards yield a value, and what the first few look like.
    const fields = ["title", "rank", "price", "author", "rating", "review"];
    const rows = [];
    const samples = {};

    for (const f of fields) {
        let hits = 0;
        const seen = [];
        for (const el of cards) {
            const node = el.querySelector(AMZ_SEL[f]);
            if (!node) continue;
            // Read the same way the harvester reads it.
            let val;
            if (f === "title")       val = node.getAttribute("title") || node.textContent;
            else if (f === "rating" || f === "review") val = node.getAttribute("aria-label");
            else                     val = node.textContent;
            val = (val || "").trim().replace(/\s+/g, " ");
            if (!val) continue;
            hits++;
            if (seen.length < 3) seen.push(val.slice(0, 70));
        }
        samples[f] = seen;
        rows.push({
            field: f,
            matched: hits + "/" + cards.length,
            pct: Math.round((hits / cards.length) * 100) + "%",
            verdict: hits === 0 ? "BROKEN"
                   : hits < cards.length * 0.5 ? "PARTIAL"
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
        console.log("Expected on Best Sellers lists. New Releases and Most Wished For pages");
        console.log("legitimately have no rank badges, so PARTIAL is normal on those two.");
    } else {
        console.log("%cRank badge healthy (" + rankRow.pct + ").", "color:#2a2;font-weight:bold");
    }

    console.log("");
    console.log("Copy this whole output back to Claude — the table alone is not enough,");
    console.log("the sample values are what show a selector matching the wrong thing.");
})();
