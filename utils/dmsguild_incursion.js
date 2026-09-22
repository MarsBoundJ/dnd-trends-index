(function () {
    const existing = document.getElementById('arcane-incursion-panel');
    if (existing) { existing.remove(); return; }

    console.log("⚔ Arcane Incursion Initiated (V11 - The Librarian Engine)...");

    const today = new Date().toISOString().split('T')[0];
    const productMap = new Map(); // Use Map to dedup by URL
    const isMetalPage = window.location.pathname.includes('metal.php');
    const source = window.location.hostname.includes('dmsguild') ? "DMs Guild" : "DriveThruRPG";

    const tiers = ['Adamantine', 'Mithral', 'Platinum', 'Gold', 'Silver', 'Electrum', 'Copper'];

    // A shelf tier comes from a section heading -- "Adamantine Metal Products" in
    // a div.infoBoxHeading -- and from nothing else.
    //
    // The original V9 walked backwards asking "does any preceding element MENTION
    // a metal?", which read the tier off a NEIGHBOURING PRODUCT'S TITLE. Real
    // examples measured on the live page Sep 16 2026: "Trophy Gold" made the next
    // product Gold; "A Copper For A Song Battlemaps" made it Copper; "B3 Palace of
    // the Silver Princess" made it Silver. metal.php has only THREE shelves
    // (Adamantine, Mithral, Platinum), yet 40% of every capture came back Gold,
    // Silver or Copper -- tiers with no section on the page at all. The bug was
    // present from the stream's first run.
    //
    // So: match the heading exactly and anchored, which a product title cannot
    // satisfy, and treat "no heading above me" as UNKNOWN rather than guessing.
    const TIER_HEADING = new RegExp('^(' + tiers.join('|') + ')\\s+Metal\\s+Products$', 'i');

    // Headings in document order. querySelectorAll guarantees that ordering, which
    // is what makes the "last heading above me" lookup below correct.
    const shelves = [];
    document.querySelectorAll('.infoBoxHeading, h1, h2, h3, h4, h5').forEach(function (el) {
        const m = TIER_HEADING.exec((el.innerText || el.textContent || '').trim());
        if (m) {
            const t = m[1];
            shelves.push({ el: el, tier: t.charAt(0).toUpperCase() + t.slice(1).toLowerCase() });
        }
    });

    // The product's tier is the last shelf heading preceding it in document order.
    function findTierForElement(el) {
        let tier = null;
        for (const s of shelves) {
            if (s.el.compareDocumentPosition(el) & Node.DOCUMENT_POSITION_FOLLOWING) {
                tier = s.tier;
            } else {
                break; // headings are in order, so nothing later can precede el
            }
        }
        return tier;
    }

    // --- UNIFIED HARVEST ---
    // Target all product links. DTRPG Metal page is usually a giant table.
    const allLinks = document.querySelectorAll('a[href*="/product/"]');
    console.log("⚔ Scanned " + allLinks.length + " links. Filtering...");

    allLinks.forEach(link => {
        const url = link.href.split('?')[0];
        if (productMap.has(url)) return; // Avoid duplicate links for same product (image + text)

        const container = link.closest('td, th, div.obs-title-card, div.title-strip-title-card');
        if (!container) return;

        const titleText = (link.innerText || link.textContent || '').trim();
        if (titleText.length < 2 || /^\d+$/.test(titleText)) return; // Skip image links or tiny text

        // Extraction logic
        let price = 0.0;
        const specialPrice = container.querySelector('.productSpecialPrice, .cy-prc');
        if (specialPrice) {
            price = parseFloat(specialPrice.innerText.replace(/[^\d.]/g, '')) || 0.0;
        } else {
            // Find price in text nodes of container
            const m = container.innerText.match(/\$?([\d.]+)/);
            if (m) price = parseFloat(m[1]) || 0.0;
        }

        // On a metal page every product sits under a shelf heading, so a miss is a
        // real defect and is reported as such. Off a metal page there are no
        // shelves at all, and "Normal" is the honest answer rather than a failure.
        const tier = findTierForElement(container) || (isMetalPage ? "Unknown" : "Normal");

        let snippet = "";
        const descEl = container.querySelector('.product-description, .smallText');
        if (descEl) {
            snippet = (descEl.innerText || descEl.textContent || '').trim();
            // Optional: Truncate very long snippets
            if (snippet.length > 500) snippet = snippet.substring(0, 500) + '...';
        }

        productMap.set(url, {
            collected_date: today,
            source: source,
            title: titleText.replace(/^\d+\.\s*/, ''), // Strip ranking numbers
            publisher: source + " (Universal)",
            seller_tier: tier,
            price: price,
            rating: 0,
            tags: [isMetalPage ? "Metal List" : "Browse", tier, "V11"],
            snippet: snippet,
            product_url: url
        });
    });

    const products = Array.from(productMap.values());

    if (products.length === 0) {
        alert('⚔ Incursion Failed: No products detected.');
        return;
    }

    // The breakdown below is the check that would have caught the V9 bug on sight:
    // a page whose only shelves are Adamantine/Mithral/Platinum cannot legitimately
    // yield Gold, Silver or Copper rows. Show both lists and the contradiction is
    // unmissable before anything is transmitted.
    const tierCounts = {};
    products.forEach(function (p) {
        tierCounts[p.seller_tier] = (tierCounts[p.seller_tier] || 0) + 1;
    });
    const shelfNames = shelves.map(function (s) { return s.tier; });
    const unknownCount = tierCounts['Unknown'] || 0;
    const breakdown = Object.keys(tierCounts)
        .sort(function (a, b) { return tierCounts[b] - tierCounts[a]; })
        .map(function (t) {
            const bad = (t === 'Unknown') || (isMetalPage && shelfNames.indexOf(t) === -1);
            const colour = bad ? 'rgb(255,136,136)' : 'rgb(201, 145, 58)';
            return '<p style="margin:2px 0;font-size:13px;">' + t +
                '<span style="float:right;color:' + colour + ';">' + tierCounts[t] + '</span></p>';
        }).join('');

    // UI Overlay (Neon Chrome)
    const panel = document.createElement('div');
    panel.id = 'arcane-incursion-panel';
    Object.assign(panel.style, {
        position: 'fixed', top: '20px', right: '20px', width: '340px', padding: '24px',
        background: 'rgba(10, 15, 25, 0.95)', border: '2px solid rgb(201, 145, 58)', borderRadius: '12px',
        color: 'rgb(248, 250, 252)', zIndex: '2147483647', fontFamily: 'Inter, sans-serif',
        boxShadow: '0 15px 40px rgba(0,0,0,0.8)', backdropFilter: 'blur(10px)', textAlign: 'left'
    });

    panel.innerHTML = `
        <h2 style="color:rgb(201, 145, 58);margin:0 0 20px 0;font-weight:bold;letter-spacing:1px;text-align:center;text-transform:uppercase;">⚔ Incursion V11</h2>
        <div style="background:rgba(255,255,255,0.05);padding:15px;border-radius:8px;margin-bottom:20px;font-size:15px;line-height:1.6;border-left:4px solid rgb(201, 145, 58);">
            <p style="margin:5px 0;">Source: <span style="float:right;color:rgb(201, 145, 58);">${source}</span></p>
            <p style="margin:5px 0;">Harvester: <span style="float:right;color:rgb(201, 145, 58);">The Librarian Engine</span></p>
            <p style="margin:10px 0;font-weight:bold;font-size:18px;">Total items: <span style="float:right;color:rgb(34, 197, 94);">${products.length}</span></p>
            <p style="font-size:12px;color:#999;margin:10px 0 4px 0;">Shelves on page: ${shelfNames.length ? shelfNames.join(', ') : 'none'}</p>
            ${breakdown}
            ${unknownCount ? '<p style="color:rgb(255,136,136);font-size:12px;margin:8px 0 0 0;">' + unknownCount + ' product(s) sat under no shelf heading — reported as Unknown, never guessed.</p>' : ''}
        </div>
        <button id="beam-btn" style="width:100%;padding:16px;background:rgb(201, 145, 58);border:none;border-radius:8px;font-weight:900;cursor:pointer;color:black;text-transform:uppercase;letter-spacing:1px;font-size:14px;box-shadow:0 0 15px rgba(201,145,58,0.4);">🚀 Transmit Batch</button>
        <div id="progress-container" style="display:none;margin-top:15px;">
             <div style="width:100%;height:8px;background:#333;border-radius:4px;overflow:hidden;">
                 <div id="progress-bar" style="width:0%;height:100%;background:rgb(34, 197, 94);transition:width 0.3s;"></div>
             </div>
             <p id="progress-text" style="font-size:12px;margin-top:5px;text-align:center;color:#999;">Preparing transmission...</p>
        </div>
        <button id="close-incursion" style="width:100%;margin-top:12px;background:transparent;border:1px solid #333;color:#666;padding:10px;border-radius:8px;cursor:pointer;font-size:12px;">Dismiss Overlay</button>
    `;

    document.body.appendChild(panel);
    document.getElementById('close-incursion').onclick = () => panel.remove();
    // Ritual key is shared across all Arcane bookmarklets (Amazon,
    // Kickstarter, DMsGuild/DTRPG). Hardcoded to match — /admin/harvest
    // is gated by the Auth.js email allowlist, so distribution of this
    // bookmarklet is effectively private. Removed the per-run prompt
    // that used to guard the public legacy admin page.
    const KEY = 'ArcaneLibrarian2026';

    document.getElementById('beam-btn').onclick = async () => {
        const btn = document.getElementById('beam-btn');
        const prog = document.getElementById('progress-container');
        const bar = document.getElementById('progress-bar');
        const progText = document.getElementById('progress-text');

        btn.style.display = 'none';
        prog.style.display = 'block';

        // Chunking for large batches (1000 items per chunk)
        const chunkSize = 1000;
        let successCount = 0;

        for (let i = 0; i < products.length; i += chunkSize) {
            const chunk = products.slice(i, i + chunkSize);
            const percent = Math.round(((i + chunk.length) / products.length) * 100);
            bar.style.width = percent + '%';
            progText.innerText = `Transmitting Chunk ${Math.floor(i / chunkSize) + 1} (${percent}%)`;

            try {
                const res = await fetch('https://us-central1-dnd-trends-index.cloudfunctions.net/bouncer-api/system/library/ingest-catalog', {
                    method: 'POST', headers: { 'Content-Type': 'application/json', 'X-Ritual-Key': KEY },
                    body: JSON.stringify(chunk)
                });
                if (res.ok) {
                    successCount += chunk.length;
                } else {
                    const errText = await res.text();
                    alert(`⚠ BATCH ERROR at item ${i}: ` + errText);
                    btn.style.display = 'block';
                    prog.style.display = 'none';
                    return;
                }
            } catch (e) {
                alert(`🚨 NETWORK ERROR at item ${i}: ` + e.message);
                btn.style.display = 'block';
                prog.style.display = 'none';
                return;
            }
        }

        alert(`📜 SUCCESS: ${successCount} items archived in BigQuery.`);
        panel.remove();
    };
}());
