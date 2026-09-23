// DMs Guild / DriveThruRPG selector probe — is the shelf harvest reading the
// right things, or merely reading something?
//
// HOW TO RUN. Open a metal page in Chrome:
//   https://www.dmsguild.com/metal.php      or
//   https://www.drivethrurpg.com/metal.php
// then F12 → Console → paste this whole file → Enter.
//
// It reads the page already on screen. Fetches nothing, sends nothing.
//
// WHY THIS EXISTS EVEN THOUGH THE HARVEST WORKS. The 2026-09-23 run captured
// 1,090 and 1,363 products with tier counts summing EXACTLY to their totals.
// That is real evidence — but only that every product got A tier. The V9 bug
// gave products the WRONG tier (read off a neighbouring product's title) and it
// summed exactly too. Sums prove coverage, never correctness.
//
// So this checks the values, not the counts. Three things it is looking for
// specifically, each a hazard already present in the extraction code:
//
//   1. PRICE FALLBACK. .productSpecialPrice/.cy-prc matched only 60 of 1,090
//      products on 2026-09-23, so 90% of prices come from the text fallback.
//      WHY it misses is now known: the price is a bare text node directly
//      inside the card container (<th class="smallText">), not a classed
//      element. Those two classes are SALE-price classes. There is no regular-
//      price selector to find, so the text path is the primary path for ~90% of
//      products permanently, which is what makes anchoring it the fix rather
//      than a stopgap.
//      DriveThruRPG measured the same day: 128 of 354 parsed products poisoned
//      (36.2%), including "$2022" for "Traveller Core Rulebook Update 2022",
//      a $30.00 book. The anchored pattern recovers 125 of the 128.
//      That fallback was unanchored — optional dollar sign, first number wins —
//      and the card text starts with the title, so 420 of 1,090 products were priced
//      from a digit in their own name ($5 for "…(5e)", $80 for "80 Maps…").
//      It is now anchored to a currency symbol. This still reports how often
//      the fallback fires, because a selector matching 5% of products is worth
//      knowing about even when the fallback is sound.
//
//   2. TIER ATTRIBUTION. Any product whose tier resolves to a metal with no
//      shelf on the page is the V9 bug returning.
//
//   3. RATING AND PUBLISHER ARE NOT READ AT ALL. The extractor hard-codes
//      rating: 0 and publisher: "<site> (Universal)". Those are not captured
//      values, they are placeholders, and 0 is indistinguishable from a product
//      rated zero. Reported here so the placeholder is visible rather than
//      assumed to be data.
//
// The selectors below are asserted byte-identical to the ones in
// catalog_harvester_extension/background.js by scripts/test_catalog_selectors.js.

(function () {
    // ---------- Selectors (mirrored from background.js) ----------
    const CAT_SEL = {
        heading:   ".infoBoxHeading, h1, h2, h3, h4, h5",
        link:      "a[href*=\"/product/\"]",
        container: "td, th, div.obs-title-card, div.title-strip-title-card",
        price:     ".productSpecialPrice, .cy-prc",
        desc:      ".product-description, .smallText"
    };
    // ---------- end selectors ----------

    const host = location.hostname;
    if (!/dmsguild\.com$|drivethrurpg\.com$/.test(host)) {
        console.log("%cNot on a OneBookShelf storefront — open metal.php first.",
            "color:#e11;font-weight:bold");
        return;
    }

    const isMetal = location.pathname.indexOf("metal.php") !== -1;
    const TIERS = ["Adamantine", "Mithral", "Platinum", "Gold", "Silver", "Electrum", "Copper"];
    const TIER_HEADING = new RegExp("^(" + TIERS.join("|") + ")\\s+Metal\\s+Products$", "i");

    console.log("%cOneBookShelf selector probe", "color:#a78bfa;font-weight:bold;font-size:14px");
    console.log("URL:", location.href, "| metal page:", isMetal);

    // ── Shelves ─────────────────────────────────────────────────────────────
    const shelves = [];
    document.querySelectorAll(CAT_SEL.heading).forEach(function (el) {
        const m = TIER_HEADING.exec((el.innerText || el.textContent || "").trim());
        if (m) shelves.push({ el: el, tier: m[1].charAt(0).toUpperCase() + m[1].slice(1).toLowerCase() });
    });

    console.log("\nShelf headings found:", shelves.length,
                shelves.length ? "→ " + shelves.map(s => s.tier).join(", ") : "");
    if (isMetal && shelves.length === 0) {
        console.log("%cNO SHELF HEADINGS on a metal page — every product would be tier Unknown.",
            "color:#e11;font-weight:bold");
    }

    function tierFor(el) {
        let tier = null;
        for (const s of shelves) {
            if (s.el.compareDocumentPosition(el) & Node.DOCUMENT_POSITION_FOLLOWING) tier = s.tier;
            else break;
        }
        return tier;
    }

    // ── Products ────────────────────────────────────────────────────────────
    const links = document.querySelectorAll(CAT_SEL.link);
    console.log("Product links:", links.length);
    if (!links.length) {
        console.log("%cThe LINK selector matched nothing — everything below is moot.",
            "color:#e11;font-weight:bold");
        return;
    }

    const seen = new Set();
    const rows = [];
    let noContainer = 0;
    let rejectedTitle = 0;

    links.forEach(link => {
        const url = link.href.split("?")[0];
        if (seen.has(url)) return;

        const container = link.closest(CAT_SEL.container);
        if (!container) { noContainer++; return; }

        const title = (link.innerText || link.textContent || "").trim();
        if (title.length < 2 || /^\d+$/.test(title)) { rejectedTitle++; return; }

        // Record the url only AFTER the title check, exactly as production does
        // (it calls productMap.set at the end of the body). Marking it earlier
        // consumes the url on a product's IMAGE link — which has no text — so
        // the real title link that follows is then skipped as a duplicate.
        // Measured 2026-09-23: that mistake turned 2,185 links into 2 parsed
        // products and 1,088 "rejected titles", while the live harvest captured
        // 1,090. Pinning the selectors is not enough; the loop has to match too.
        seen.add(url);

        // Mirror production's two price paths, but record WHICH one fired.
        const priceEl = container.querySelector(CAT_SEL.price);
        let price = null, priceVia = null;
        if (priceEl) {
            price = parseFloat(priceEl.innerText.replace(/[^\d.]/g, ""));
            priceVia = "selector";
        } else {
            const m = container.innerText.match(/\$\s*([\d,]+\.?\d*)/);
            if (m) { price = parseFloat(m[1].replace(/,/g, "")); priceVia = "regex-fallback"; }
        }

        rows.push({
            url: url,
            title: title.replace(/^\d+\.\s*/, ""),
            tier: tierFor(container) || (isMetal ? "Unknown" : "Normal"),
            price: Number.isFinite(price) ? price : null,
            priceVia: priceVia,
            hasDesc: !!container.querySelector(CAT_SEL.desc)
        });
    });

    console.log("Products parsed:", rows.length,
                "| links with no container:", noContainer,
                "| titles rejected:", rejectedTitle);

    // ── Tier attribution ────────────────────────────────────────────────────
    const tierCounts = {};
    rows.forEach(r => { tierCounts[r.tier] = (tierCounts[r.tier] || 0) + 1; });
    console.log("\n%cTier attribution", "color:#a78bfa;font-weight:bold");
    console.table(Object.keys(tierCounts).sort().map(t => ({
        tier: t, count: tierCounts[t],
        pct: Math.round(tierCounts[t] / rows.length * 100) + "%"
    })));

    const onPage = shelves.map(s => s.tier);
    const impossible = Object.keys(tierCounts).filter(
        t => t !== "Unknown" && t !== "Normal" && onPage.indexOf(t) === -1);
    if (impossible.length) {
        console.log("%cIMPOSSIBLE TIERS: " + impossible.join(", "),
            "color:#e11;font-weight:bold");
        console.log("These metals have no shelf on this page. This is the V9 bug — a tier read");
        console.log("off a neighbouring product's title. Report immediately.");
        impossible.forEach(t => console.log("   e.g.", rows.filter(r => r.tier === t).slice(0, 3).map(r => r.title)));
    } else {
        console.log("%cNo impossible tiers — every tier assigned has a shelf on this page.",
            "color:#2a2;font-weight:bold");
    }
    if (tierCounts.Unknown) {
        console.log("%c" + tierCounts.Unknown + " products resolved to Unknown.",
            "color:#e90;font-weight:bold");
    }

    // ── Price: the hazard ───────────────────────────────────────────────────
    const viaSel = rows.filter(r => r.priceVia === "selector");
    const viaRe = rows.filter(r => r.priceVia === "regex-fallback");
    const noPrice = rows.filter(r => r.priceVia === null);

    console.log("\n%cPrice path", "color:#a78bfa;font-weight:bold");
    console.table([
        { path: ".productSpecialPrice/.cy-prc", count: viaSel.length },
        { path: "regex fallback ($-anchored)", count: viaRe.length },
        { path: "no price at all", count: noPrice.length }
    ]);
    console.log("  selector samples:", viaSel.slice(0, 3).map(r => r.price));
    if (viaRe.length) {
        console.log("%c  regex-fallback samples — check these against the real page price:",
            "color:#e90;font-weight:bold");
        viaRe.slice(0, 5).forEach(r => console.log("     $" + r.price, "←", r.title.slice(0, 55)));
        console.log("  A price taken from the first number in the card text can be a digit");
        console.log("  inside the TITLE (\"B3 Palace…\" -> $3). Verify a few by eye.");
    }

    // ── Fields that are not captured at all ─────────────────────────────────
    console.log("\n%cNot captured by this harvest (placeholders, not data)",
        "color:#a78bfa;font-weight:bold");
    console.log("  rating    : always 0   — the shelf pass never reads a rating");
    console.log("  publisher : always \"" + (host.indexOf("dmsguild") !== -1 ? "DMs Guild" : "DriveThruRPG") +
                " (Universal)\" — a placeholder, not a publisher");
    console.log("  Both are captured properly by the detail pass into catalog_detail.");

    // ── Samples ─────────────────────────────────────────────────────────────
    console.log("\n%cFirst 3 rows as the harvester would send them",
        "color:#a78bfa;font-weight:bold");
    rows.slice(0, 3).forEach(r => console.log("  ", {
        title: r.title.slice(0, 60), tier: r.tier, price: r.price, via: r.priceVia,
        url: r.url.slice(-40)
    }));

    console.log("\n%cCopy everything above back to Claude.", "color:#2a2;font-weight:bold");
})();
