// Amazon card structure dump — what IS the markup now?
//
// Paste into the Chrome console on any Amazon list page (same as the selector
// probe). Reads the page on screen; fetches and sends nothing.
//
// WHY THIS EXISTS. The selector probe answers "do our selectors still match?".
// When the answer is no — or worse, "yes but they are matching the wrong
// element" — this answers "then what should they be?" without a round trip of
// right-click-inspect-report-repeat.
//
// Measured 2026-09-23 on /gp/new-releases/books/16215: the author selector
// scored 93% while returning "Kindle Edition" and "Hardcover" for two of three
// samples. It was matching the format label. The rating and review selectors
// matched nothing at all.

(function () {
    if (!location.hostname.endsWith("amazon.com")) {
        console.log("%cNot on amazon.com.", "color:#e11;font-weight:bold");
        return;
    }

    const cards = Array.from(document.querySelectorAll("[data-asin]"))
        .filter(el => /^[A-Z0-9]{10}$/.test(el.dataset.asin || ""));

    console.log("%cAmazon card structure dump", "color:#a78bfa;font-weight:bold;font-size:14px");
    console.log("URL:", location.href, "| cards:", cards.length);
    if (!cards.length) return;

    const CARDS_TO_DUMP = 2;

    for (let i = 0; i < Math.min(CARDS_TO_DUMP, cards.length); i++) {
        const card = cards[i];
        console.log("\n%c──────── CARD " + (i + 1) + "  asin=" + card.dataset.asin + " ────────",
            "color:#a78bfa;font-weight:bold");

        const seen = new Set();
        for (const el of card.querySelectorAll("*")) {
            const cls = (el.getAttribute("class") || "").trim();
            const aria = el.getAttribute("aria-label");
            // Own text only — skip containers that merely wrap other elements,
            // otherwise every ancestor repeats its children's text.
            const own = Array.from(el.childNodes)
                .filter(n => n.nodeType === 3)
                .map(n => n.textContent)
                .join(" ")
                .trim()
                .replace(/\s+/g, " ");

            if (!own && !aria) continue;

            const key = el.tagName + "|" + cls + "|" + own.slice(0, 40) + "|" + (aria || "");
            if (seen.has(key)) continue;
            seen.add(key);

            const line = "  <" + el.tagName.toLowerCase() + ">" +
                (cls ? " ." + cls.split(/\s+/).join(".") : "") +
                (aria ? "\n       aria-label: " + JSON.stringify(aria) : "") +
                (own ? "\n       text: " + JSON.stringify(own.slice(0, 90)) : "");
            console.log(line);
        }
    }

    // ── Targeted hunt ────────────────────────────────────────────────────────
    // Independent of the dump above: look across ALL cards for the three fields
    // that are broken or wrong, so a field that happens to be missing on cards
    // 1 and 2 is still found.

    console.log("\n%c──────── TARGETED HUNT (all " + cards.length + " cards) ────────",
        "color:#a78bfa;font-weight:bold");

    function hunt(name, test, read) {
        const hits = [];
        for (const card of cards) {
            for (const el of card.querySelectorAll("*")) {
                if (!test(el)) continue;
                const cls = (el.getAttribute("class") || "").trim();
                hits.push({
                    selector: el.tagName.toLowerCase() + (cls ? "." + cls.split(/\s+/).join(".") : ""),
                    value: String(read(el) || "").trim().replace(/\s+/g, " ").slice(0, 60)
                });
                break;
            }
            if (hits.length >= 5) break;
        }
        console.log("\n  " + name + ":");
        if (!hits.length) { console.log("    (no candidates found anywhere)"); return; }
        // Collapse identical selectors so the shape is obvious at a glance.
        const byS = {};
        for (const h of hits) (byS[h.selector] = byS[h.selector] || []).push(h.value);
        for (const s of Object.keys(byS)) console.log("    " + s + "\n       →", byS[s]);
    }

    // Anything carrying a star rating, however it is labelled.
    hunt("STAR RATING candidates",
        el => /out of 5/i.test(el.getAttribute("aria-label") || "") ||
              /out of 5 stars/i.test(el.textContent || "") && el.children.length === 0,
        el => el.getAttribute("aria-label") || el.textContent);

    // Review counts: a bare number, often in a link next to the stars.
    hunt("REVIEW COUNT candidates",
        el => el.children.length === 0 && /^\(?[\d,]{1,9}\)?$/.test((el.textContent || "").trim()) &&
              (el.textContent || "").trim().replace(/[^\d]/g, "").length >= 1,
        el => el.textContent);

    // Author/byline: a leaf whose text is neither a price, a format, nor a number.
    const FORMATS = /^(kindle edition|hardcover|paperback|audible|audiobook|spiral-bound|board book|mass market paperback|library binding|cards|game|toy)$/i;
    hunt("AUTHOR / BYLINE candidates (formats excluded)",
        el => {
            if (el.children.length) return false;
            const t = (el.textContent || "").trim();
            if (!t || t.length > 60) return false;
            if (FORMATS.test(t)) return false;
            if (/^\$/.test(t)) return false;
            if (/^\(?[\d,.]+\)?$/.test(t)) return false;
            if (/^#\d+$/.test(t)) return false;
            if (/out of 5/i.test(t)) return false;
            return true;
        },
        el => el.textContent);

    // And what IS matching today, so the wrong-element case is explicit.
    console.log("\n  WHAT OUR CURRENT AUTHOR SELECTOR GRABS:");
    const cur = [];
    for (const card of cards.slice(0, 5)) {
        const el = card.querySelector("span.a-color-secondary, .a-row .a-color-base.a-size-small");
        cur.push(el ? (el.textContent || "").trim().replace(/\s+/g, " ").slice(0, 50) : "(none)");
    }
    console.log("   ", cur);

    console.log("\n%cCopy everything above back to Claude.", "color:#2a2;font-weight:bold");
})();
