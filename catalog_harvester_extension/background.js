// Arcane Incursion - Background Service Worker
// Handles scheduling (Mon 6am, daily retry, skip Sat) and tab lifecycle.
//
// WHY THIS FILE IS SHAPED AROUND THE WORKER DYING.
//
// Manifest V3 terminates an idle service worker after ~30 seconds. When that
// happens mid-harvest, everything held in memory goes with it: the promise
// tracking the run, the tabs.onUpdated listener, and the setTimeout meant to
// fail a stuck site. Nothing resumes and nothing cleans up.
//
// Measured on Sep 22 2026, first real run: DMs Guild harvested and logged
// (1090 products, tiers clean), the DriveThruRPG tab opened and rendered, then
// the worker was evicted. harvestInProgress was left true in storage with no
// code path alive to clear it, which disabled Run Now permanently -- the popup
// read "Harvest in progress" forever and offered no way out. The only escape
// was writing to chrome.storage by hand from the service-worker console.
// Reloading the extension does not help: storage survives it.
//
// So the in-progress flag is now a LEASE with a timestamp, and a stale lease is
// not a running harvest. A killed worker costs one run instead of bricking the
// extension. chrome.alarms, which Chrome persists and which wakes a fresh
// worker, is the backstop that setTimeout cannot be.

const ENDPOINT = "https://us-central1-dnd-trends-index.cloudfunctions.net/bouncer-api/system/library/ingest-catalog";
const CHUNK_SIZE = 1000;
const ALARM_NAME = "catalog-daily";
const WATCHDOG_ALARM = "harvest-watchdog";
const SITE_TIMEOUT_MS = 3 * 60 * 1000;

// The detail pass lives in its own file: a resumable cursor over the product
// API, unrelated to the shelf harvest's tab lifecycle. Loaded after ENDPOINT,
// which it derives its own ingest URL from. amazon.js likewise.
importScripts("detail.js");
importScripts("amazon.js");

// SITES is declared after the imports because the Amazon entry's landing URL and
// source list belong to amazon.js, not here.
//
// `ready` is the substring a tab's URL must contain before injection. It used to
// be a hard-coded "metal.php" check, which was correct for exactly two sites and
// silently wrong for any third: an Amazon tab never contains "metal.php", so the
// harvest would have waited out the full three-minute timeout and reported a
// stuck site rather than a mis-configured one.
//
// `extractor` picks which injected function runs. OneBookShelf's two storefronts
// share one; Amazon needs its own because it is a different page shape with two
// destination tables.
const SITES = [
    { name: "DMs Guild",    url: "https://www.dmsguild.com/metal.php",    ready: "metal.php",  extractor: "catalog" },
    { name: "DriveThruRPG", url: "https://www.drivethrurpg.com/metal.php", ready: "metal.php",  extractor: "catalog" },
    { name: "Amazon",       url: AMAZON_LANDING,                           ready: "amazon.com", extractor: "amazon"  }
];

// ---------- Harvest lease ----------
// Pure, and sliced out by scripts/test_harvester_lease.js. Keep it that way:
// the staleness rule is the thing that stops a dead run disabling the harvester,
// and it is worth testing without a browser.

const HARVEST_STALE_MS = 10 * 60 * 1000;

function evaluateLease(stored, now) {
    const held = !!(stored && stored.harvestInProgress);
    if (!held) return { held: false, running: false, stale: false, ageMs: 0 };

    const startedAt = (stored && typeof stored.harvestStartedAt === "number") ? stored.harvestStartedAt : 0;

    // No timestamp at all means the lease was written by a build older than this
    // one, or by a worker that died before stamping it. Either way there is no
    // live run behind it, so it must not hold the harvester shut.
    if (!startedAt) return { held: true, running: false, stale: true, ageMs: Infinity };

    const ageMs = now - startedAt;

    // A negative age means the clock moved backwards (system time change, a DST
    // step). Treat that as stale too, rather than letting a dead lease look
    // fresh until the clock catches up.
    const stale = ageMs < 0 || ageMs > HARVEST_STALE_MS;
    return { held: true, running: !stale, stale: stale, ageMs: ageMs };
}

// ---------- end harvest lease ----------

// ---------- Scheduling helpers ----------

function nextSixAM() {
    const now = new Date();
    const next = new Date(now);
    next.setHours(6, 0, 0, 0);
    if (next <= now) next.setDate(next.getDate() + 1);
    return next.getTime();
}

// Returns "YYYY-MM-DD" of the Monday that starts the current week.
function getMondayOfWeek(date) {
    const d = new Date(date);
    const day = d.getDay(); // 0=Sun
    const diff = day === 0 ? -6 : 1 - day;
    d.setDate(d.getDate() + diff);
    return d.toISOString().split("T")[0];
}

// Ensure the alarm exists and fires at the next 6am, then every 24h.
function ensureAlarm() {
    chrome.alarms.get(ALARM_NAME, (alarm) => {
        if (!alarm) {
            chrome.alarms.create(ALARM_NAME, {
                when: nextSixAM(),
                periodInMinutes: 24 * 60
            });
            console.log("[Incursion] Alarm created, next fire:", new Date(nextSixAM()).toString());
        }
    });
}

// ---------- Lease storage ----------

async function readLease() {
    const stored = await chrome.storage.local.get(["harvestInProgress", "harvestStartedAt"]);
    return evaluateLease(stored, Date.now());
}

async function takeLease() {
    await chrome.storage.local.set({
        harvestInProgress: true,
        harvestStartedAt: Date.now(),
        harvestTabIds: [],
        harvestLog: []
    });
    // Persisted by Chrome, so it survives the worker being evicted. This is the
    // whole point: it is the one thing that can still run after the harvest's
    // own in-memory machinery is gone.
    chrome.alarms.create(WATCHDOG_ALARM, { when: Date.now() + HARVEST_STALE_MS });
}

async function releaseLease(extra) {
    await chrome.alarms.clear(WATCHDOG_ALARM);
    await chrome.storage.local.set(Object.assign({
        harvestInProgress: false,
        harvestStartedAt: 0,
        harvestTabIds: []
    }, extra || {}));
}

// Tab ids are kept in storage, not in a variable, so a fresh worker can still
// close the tabs a dead one left open.
async function trackTab(tabId) {
    const { harvestTabIds = [] } = await chrome.storage.local.get("harvestTabIds");
    if (harvestTabIds.indexOf(tabId) === -1) harvestTabIds.push(tabId);
    await chrome.storage.local.set({ harvestTabIds });
}

async function untrackTab(tabId) {
    const { harvestTabIds = [] } = await chrome.storage.local.get("harvestTabIds");
    await chrome.storage.local.set({ harvestTabIds: harvestTabIds.filter((id) => id !== tabId) });
}

// Give up on a run: close whatever tabs are still open, keep the results of the
// sites that DID finish, mark the rest failed, and free the lease.
async function abandonHarvest(reason) {
    const { harvestTabIds = [], harvestLog = [] } = await chrome.storage.local.get(["harvestTabIds", "harvestLog"]);

    for (const id of harvestTabIds) {
        try { await chrome.tabs.remove(id); } catch (e) { /* already closed */ }
    }

    // A site that completed before the interruption keeps its real result --
    // on Sep 22 that was a clean 1090-product DMs Guild capture, and throwing
    // it away would have hidden the fact that the harvest itself worked.
    const finished = harvestLog.map((e) => e.site);
    const results = harvestLog.slice();
    SITES.forEach((s) => {
        if (finished.indexOf(s.name) === -1) {
            results.push({ site: s.name, success: false, error: reason });
        }
    });

    await releaseLease({ lastRunResults: results, lastRunDate: new Date().toISOString() });
    console.warn("[Incursion] Harvest abandoned: " + reason);
}

const INTERRUPTED = "Interrupted — Chrome shut the harvester down mid-run.";

// ---------- Lifecycle ----------

chrome.runtime.onInstalled.addListener(() => {
    ensureAlarm();
});

chrome.runtime.onStartup.addListener(() => {
    ensureAlarm();
});

// ---------- Alarm handler ----------

chrome.alarms.onAlarm.addListener(async (alarm) => {
    // The backstop. If this fires and a lease is still held, the run that took
    // it never finished -- clean up instead of leaving the extension stuck.
    if (alarm.name === WATCHDOG_ALARM) {
        const lease = await readLease();
        if (lease.held) await abandonHarvest(INTERRUPTED);
        return;
    }

    // The detail pass runs on its own schedule and must not be confused with
    // the weekly shelf harvest: it holds no lease, opens no tabs, and a tick
    // that finds an empty queue simply does nothing.
    if (alarm.name === DETAIL_ALARM) {
        const { ritualKey } = await chrome.storage.local.get("ritualKey");
        if (!ritualKey) {
            console.warn("[Incursion] Detail pass needs a ritual key — stopping.");
            await detailStop("no ritual key");
            return;
        }
        await detailTick(ritualKey);
        return;
    }

    if (alarm.name !== ALARM_NAME) return;

    const today = new Date();
    const dayOfWeek = today.getDay(); // 0=Sun, 6=Sat

    // Skip Saturday
    if (dayOfWeek === 6) {
        console.log("[Incursion] Saturday — skipping.");
        return;
    }

    const { ritualKey, lastSuccessWeek } = await chrome.storage.local.get(["ritualKey", "lastSuccessWeek"]);

    if (!ritualKey) {
        console.warn("[Incursion] No ritual key configured. Open the popup to set it.");
        return;
    }

    const thisWeek = getMondayOfWeek(today);
    if (lastSuccessWeek === thisWeek) {
        console.log("[Incursion] Already succeeded this week (" + thisWeek + ") — skipping.");
        return;
    }

    // Overlap guard. The scheduled path used to ignore the in-progress flag
    // entirely, so a daily retry could open a second pair of tabs on top of a
    // run already going and post both to ingest.
    const lease = await readLease();
    if (lease.running) {
        console.warn("[Incursion] A harvest is already running — skipping this fire.");
        return;
    }
    if (lease.stale) await abandonHarvest(INTERRUPTED);

    console.log("[Incursion] Starting harvest for week of " + thisWeek);
    await runHarvest(ritualKey, thisWeek);
});

// ---------- Harvest orchestration ----------

async function runHarvest(ritualKey, weekKey) {
    await takeLease();

    const results = [];

    for (const site of SITES) {
        const result = await harvestSite(site, ritualKey);
        results.push(result);
        await appendLog(result);
    }

    const allOk = results.every(r => r.success);

    // Deduped across sites and stored as {store, productId} — the exact shape
    // detailStart expects, so queueing is a read rather than a transformation.
    const seen = new Set();
    const harvestedProducts = [];
    results.forEach(r => (r.productIds || []).forEach(id => {
        const key = r.site + ":" + id;
        if (!seen.has(key)) { seen.add(key); harvestedProducts.push({ store: r.site, productId: id }); }
    }));
    await chrome.storage.local.set({ harvestedProducts });

    await releaseLease({
        lastRunResults: results,
        lastRunDate: new Date().toISOString()
    });

    if (allOk) {
        await chrome.storage.local.set({ lastSuccessWeek: weekKey });
        console.log("[Incursion] Harvest complete — all " + SITES.length + " sites succeeded.");
    } else {
        const failed = results.filter(r => !r.success).map(r => r.site).join(", ");
        console.warn("[Incursion] Harvest partial failure — failed: " + failed);
    }
}

// Opens a tab, waits for it to load, injects the harvest script, returns result.
function harvestSite(site, ritualKey) {
    return new Promise((resolve) => {
        chrome.tabs.create({ url: site.url, active: false }, (tab) => {
            const tabId = tab.id;
            let settled = false;
            let timer = null;

            trackTab(tabId);

            // Every exit goes through here, so a site cannot resolve twice and
            // cannot leave its tab or its listener behind.
            function finish(result) {
                if (settled) return;
                settled = true;
                if (timer !== null) clearTimeout(timer);
                chrome.tabs.onUpdated.removeListener(onUpdated);
                chrome.tabs.remove(tabId).catch(() => {});
                untrackTab(tabId);
                resolve(result);
            }

            function onUpdated(updatedId, info, updatedTab) {
                if (updatedId !== tabId || info.status !== "complete") return;
                // Wait until we're actually on the target page (not a Cloudflare
                // challenge, not an interstitial). Per-site, because "metal.php"
                // is meaningless on any site but OneBookShelf's two.
                if (!updatedTab.url || !updatedTab.url.includes(site.ready)) {
                    console.log("[Incursion] " + site.name + " tab not yet on " + site.ready +
                                " (url=" + updatedTab.url + "), waiting...");
                    return;
                }
                chrome.tabs.onUpdated.removeListener(onUpdated);

                // Inject the harvest runner directly via scripting
                injectViaScripting(tabId, site, ritualKey, finish);
            }

            chrome.tabs.onUpdated.addListener(onUpdated);

            // Fast path only: this fails a stuck site promptly WHILE THE WORKER
            // IS ALIVE. It cannot be the real timeout, because setTimeout dies
            // with the worker along with this promise and the listener above.
            // WATCHDOG_ALARM covers that case.
            timer = setTimeout(() => {
                finish({ site: site.name, success: false, error: "Timeout (3min)" });
            }, SITE_TIMEOUT_MS);
        });
    });
}

// Uses chrome.scripting.executeScript (preferred in MV3) to run extraction in-page.
function injectViaScripting(tabId, site, ritualKey, finish) {
    // Each extractor is serialized by chrome.scripting, so it gets everything it
    // needs as arguments — it cannot see this file's scope once injected.
    const job = site.extractor === "amazon"
        ? { func: runAmazonExtractionInPage,
            args: [site.name, ritualKey, ENDPOINT, AMAZON_RANKS_ENDPOINT,
                   AMAZON_SOURCES, AMAZON_MAX_PAGES, CHUNK_SIZE] }
        : { func: runExtractionInPage,
            args: [site.name, ritualKey, ENDPOINT, CHUNK_SIZE] };

    chrome.scripting.executeScript(
        {
            target: { tabId },
            func: job.func,
            args: job.args
        },
        (injectionResults) => {
            if (chrome.runtime.lastError) {
                finish({ site: site.name, success: false, error: chrome.runtime.lastError.message });
                return;
            }
            const result = injectionResults?.[0]?.result;
            finish(result || { site: site.name, success: false, error: "No result from injected script" });
        }
    );
}

// ---------- In-page extraction function (serialized and injected) ----------
// This runs inside the metal.php tab. Cannot reference background.js variables.

async function runExtractionInPage(siteName, ritualKey, endpoint, chunkSize) {
    try {
        const today = new Date().toISOString().split("T")[0];
        const productMap = new Map();
        const isMetalPage = window.location.pathname.includes("metal.php");

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
        const allLinks = document.querySelectorAll('a[href*="/product/"]');

        allLinks.forEach(link => {
            const url = link.href.split("?")[0];
            if (productMap.has(url)) return;

            const container = link.closest("td, th, div.obs-title-card, div.title-strip-title-card");
            if (!container) return;

            const titleText = (link.innerText || link.textContent || "").trim();
            if (titleText.length < 2 || /^\d+$/.test(titleText)) return;

            let price = 0.0;
            const specialPrice = container.querySelector(".productSpecialPrice, .cy-prc");
            if (specialPrice) {
                price = parseFloat(specialPrice.innerText.replace(/[^\d.]/g, "")) || 0.0;
            } else {
                const m = container.innerText.match(/\$?([\d.]+)/);
                if (m) price = parseFloat(m[1]) || 0.0;
            }

            // On a metal page every product sits under a shelf heading, so a miss is a
            // real defect and is reported as such. Off a metal page there are no
            // shelves at all, and "Normal" is the honest answer rather than a failure.
            const tier = findTierForElement(container) || (isMetalPage ? "Unknown" : "Normal");

            let snippet = "";
            const descEl = container.querySelector(".product-description, .smallText");
            if (descEl) {
                snippet = (descEl.innerText || descEl.textContent || "").trim();
                if (snippet.length > 500) snippet = snippet.substring(0, 500) + "...";
            }

            productMap.set(url, {
                collected_date: today,
                source: siteName,
                title: titleText.replace(/^\d+\.\s*/, ""),
                publisher: siteName + " (Universal)",
                seller_tier: tier,
                price,
                rating: 0,
                product_url: url,
                tags: [isMetalPage ? "Metal List" : "Browse", tier, "V11-auto"],
                snippet
            });
        });

        const products = Array.from(productMap.values());
        if (products.length === 0) {
            return { site: siteName, success: false, error: "No products found on page" };
        }

        // THE SAME CHECK AS THE BOOKMARKLET, ENFORCED RATHER THAN DISPLAYED.
        //
        // The bookmarklet prints a tier breakdown next to the shelves it found and
        // lets a human see the contradiction before clicking Transmit. Nobody is
        // watching this one -- it fires at 6am into a background tab -- so the
        // equivalent protection is to refuse the send instead of drawing it. A
        // tier with no shelf on the page is exactly the V9 signature, and V9 shipped
        // 6,760 such rows (35% of the stream) precisely because nothing ever
        // blocked them.
        //
        // Deliberately a hard failure, not a filter: a page that produces impossible
        // tiers is a page we have misread, so the honest move is to send nothing and
        // surface it in the popup, not to quietly transmit the subset that looks fine.
        const tierCounts = {};
        products.forEach(p => { tierCounts[p.seller_tier] = (tierCounts[p.seller_tier] || 0) + 1; });
        const shelfNames = shelves.map(s => s.tier);
        const tierSummary = Object.keys(tierCounts)
            .sort((a, b) => tierCounts[b] - tierCounts[a])
            .map(t => t + " " + tierCounts[t]).join(", ");

        if (isMetalPage) {
            if (shelfNames.length === 0) {
                return {
                    site: siteName, success: false, tierSummary,
                    error: "Refused to transmit: no metal shelf headings found on metal.php — " +
                           "the page layout has changed and every tier would be a guess."
                };
            }
            const impossible = Object.keys(tierCounts)
                .filter(t => t !== "Unknown" && shelfNames.indexOf(t) === -1);
            if (impossible.length) {
                return {
                    site: siteName, success: false, tierSummary,
                    error: "Refused to transmit: " + impossible.join(", ") +
                           " has no shelf on this page (shelves: " + shelfNames.join(", ") + ")."
                };
            }
        }

        let successCount = 0;
        for (let i = 0; i < products.length; i += chunkSize) {
            const chunk = products.slice(i, i + chunkSize);
            const res = await fetch(endpoint, {
                method: "POST",
                headers: { "Content-Type": "application/json", "X-Ritual-Key": ritualKey },
                body: JSON.stringify(chunk)
            });
            if (!res.ok) {
                const errText = await res.text();
                return { site: siteName, success: false, error: `HTTP ${res.status}: ${errText}`, partial: successCount };
            }
            successCount += chunk.length;
        }

        // The product id is in every URL we just harvested. Keeping it turns the
        // shelf pass into the detail pass's queue at zero extra cost — nothing
        // needs re-visiting to find out which products exist.
        const productIds = products
            .map(p => (String(p.product_url).match(/\/product\/(\d+)/) || [])[1])
            .filter(Boolean);

        return { site: siteName, success: true, count: successCount, tierSummary,
                 shelves: shelfNames, productIds };
    } catch (e) {
        return { site: siteName, success: false, error: e.message };
    }
}

// ---------- Log helper ----------

async function appendLog(entry) {
    const { harvestLog = [] } = await chrome.storage.local.get("harvestLog");
    harvestLog.push({ ...entry, ts: new Date().toISOString() });
    await chrome.storage.local.set({ harvestLog: harvestLog.slice(-50) }); // keep last 50 entries
}

// ---------- Message listener (popup) ----------

chrome.runtime.onMessage.addListener((msg, _sender, sendResponse) => {
    if (msg.type === "RUN_NOW") {
        (async () => {
            const { ritualKey } = await chrome.storage.local.get("ritualKey");
            if (!ritualKey) {
                sendResponse({ ok: false, error: "No ritual key configured" });
                return;
            }
            const lease = await readLease();
            if (lease.running) {
                sendResponse({ ok: false, error: "Harvest already in progress" });
                return;
            }
            // A stale lease is a dead run, not a live one. Clear it and go.
            if (lease.stale) await abandonHarvest(INTERRUPTED);

            sendResponse({ ok: true });
            await runHarvest(ritualKey, getMondayOfWeek(new Date()));
        })();
        return true; // async sendResponse
    }

    if (msg.type === "CANCEL") {
        (async () => {
            await abandonHarvest("Cancelled.");
            sendResponse({ ok: true });
        })();
        return true;
    }

    if (msg.type === "DETAIL_START") {
        (async () => {
            const { ritualKey } = await chrome.storage.local.get("ritualKey");
            if (!ritualKey) { sendResponse({ ok: false, error: "No ritual key configured" }); return; }
            const queue = Array.isArray(msg.queue) ? msg.queue : [];
            if (!queue.length) { sendResponse({ ok: false, error: "Empty queue" }); return; }
            await detailStart(queue);
            sendResponse({ ok: true, queued: queue.length });
        })();
        return true;
    }

    if (msg.type === "DETAIL_STOP") {
        (async () => { await detailStop("stopped from the popup"); sendResponse({ ok: true }); })();
        return true;
    }

    if (msg.type === "DETAIL_STATUS") {
        (async () => {
            const s = await chrome.storage.local.get(
                ["detailQueue", "detailCursor", "detailDone", "detailErrors", "detailStartedAt"]);
            const total = (s.detailQueue || []).length;
            sendResponse({
                total,
                cursor: s.detailCursor || 0,
                done: s.detailDone || 0,
                errors: (s.detailErrors || []).length,
                lastErrors: (s.detailErrors || []).slice(-3),
                running: total > 0 && (s.detailCursor || 0) < total,
                startedAt: s.detailStartedAt || null
            });
        })();
        return true;
    }

    if (msg.type === "GET_STATUS") {
        (async () => {
            const data = await chrome.storage.local.get([
                "lastSuccessWeek", "lastRunDate", "lastRunResults",
                "harvestInProgress", "harvestStartedAt", "ritualKey"
            ]);
            // The popup asks "is a harvest running?", which is not the same
            // question as "is the flag set?" -- that gap is what stranded it.
            const lease = evaluateLease(data, Date.now());
            data.harvestActive = lease.running;
            data.harvestStuck = lease.held && lease.stale;
            sendResponse(data);
        })();
        return true;
    }
});
