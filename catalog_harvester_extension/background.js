// Arcane Incursion - Background Service Worker
// Handles scheduling (Mon 6am, daily retry, skip Sat) and tab lifecycle.

const SITES = [
    { name: "DMs Guild",    url: "https://www.dmsguild.com/metal.php" },
    { name: "DriveThruRPG", url: "https://www.drivethrurpg.com/metal.php" }
];

const ENDPOINT = "https://us-central1-dnd-trends-index.cloudfunctions.net/bouncer-api/system/library/ingest-catalog";
const CHUNK_SIZE = 1000;
const ALARM_NAME = "catalog-daily";

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

// ---------- Lifecycle ----------

chrome.runtime.onInstalled.addListener(() => {
    ensureAlarm();
});

chrome.runtime.onStartup.addListener(() => {
    ensureAlarm();
});

// ---------- Alarm handler ----------

chrome.alarms.onAlarm.addListener(async (alarm) => {
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

    console.log("[Incursion] Starting harvest for week of " + thisWeek);
    await runHarvest(ritualKey, thisWeek);
});

// ---------- Harvest orchestration ----------

async function runHarvest(ritualKey, weekKey) {
    await chrome.storage.local.set({ harvestInProgress: true, harvestLog: [] });

    const results = [];

    for (const site of SITES) {
        const result = await harvestSite(site, ritualKey);
        results.push(result);
        await appendLog(result);
    }

    const allOk = results.every(r => r.success);

    await chrome.storage.local.set({
        harvestInProgress: false,
        lastRunResults: results,
        lastRunDate: new Date().toISOString()
    });

    if (allOk) {
        await chrome.storage.local.set({ lastSuccessWeek: weekKey });
        console.log("[Incursion] Harvest complete — both sites succeeded.");
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

            function onUpdated(updatedId, info, updatedTab) {
                if (updatedId !== tabId || info.status !== "complete") return;
                // Wait until we're actually on the target page (not a Cloudflare challenge)
                if (!updatedTab.url || !updatedTab.url.includes("metal.php")) {
                    console.log("[Incursion] Tab not yet on metal.php (url=" + updatedTab.url + "), waiting...");
                    return;
                }
                chrome.tabs.onUpdated.removeListener(onUpdated);

                // Inject the harvest runner directly via scripting
                injectViaScripting(tabId, site, ritualKey, resolve);
            }

            chrome.tabs.onUpdated.addListener(onUpdated);

            // Timeout: give up after 3 minutes
            setTimeout(() => {
                chrome.tabs.onUpdated.removeListener(onUpdated);
                chrome.tabs.remove(tabId).catch(() => {});
                resolve({ site: site.name, success: false, error: "Timeout (3min)" });
            }, 3 * 60 * 1000);
        });
    });
}

// Uses chrome.scripting.executeScript (preferred in MV3) to run extraction in-page.
function injectViaScripting(tabId, site, ritualKey, resolve) {
    chrome.scripting.executeScript(
        {
            target: { tabId },
            func: runExtractionInPage,
            args: [site.name, ritualKey, ENDPOINT, CHUNK_SIZE]
        },
        (injectionResults) => {
            chrome.tabs.remove(tabId).catch(() => {});
            if (chrome.runtime.lastError) {
                resolve({ site: site.name, success: false, error: chrome.runtime.lastError.message });
                return;
            }
            const result = injectionResults?.[0]?.result;
            if (result) {
                resolve(result);
            } else {
                resolve({ site: site.name, success: false, error: "No result from injected script" });
            }
        }
    );
}

// ---------- In-page extraction function (serialized and injected) ----------
// This runs inside the metal.php tab. Cannot reference background.js variables.

async function runExtractionInPage(siteName, ritualKey, endpoint, chunkSize) {
    try {
        const today = new Date().toISOString().split("T")[0];
        const tiers = ["Adamantine", "Mithral", "Platinum", "Gold", "Silver", "Electrum", "Copper"];
        const productMap = new Map();

        function findTierForElement(el) {
            let prev = el.previousElementSibling;
            while (prev) {
                const text = (prev.innerText || prev.textContent || "").trim();
                for (const t of tiers) { if (text.includes(t)) return t; }
                const childHeading = prev.querySelector(".infoBoxHeading");
                if (childHeading) {
                    const hText = childHeading.innerText || childHeading.textContent || "";
                    for (const t of tiers) { if (hText.includes(t)) return t; }
                }
                prev = prev.previousElementSibling;
            }
            if (el.parentElement && el.parentElement !== document.body) {
                return findTierForElement(el.parentElement);
            }
            return "Normal";
        }

        const isMetalPage = window.location.pathname.includes("metal.php");
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

            const tier = findTierForElement(container);

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
                tags: [isMetalPage ? "Metal List" : "Browse", tier, "V10-auto"],
                snippet
            });
        });

        const products = Array.from(productMap.values());
        if (products.length === 0) {
            return { site: siteName, success: false, error: "No products found on page" };
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

        return { site: siteName, success: true, count: successCount };
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

// ---------- Message listener (for popup "Run Now") ----------

chrome.runtime.onMessage.addListener((msg, _sender, sendResponse) => {
    if (msg.type === "RUN_NOW") {
        chrome.storage.local.get(["ritualKey", "harvestInProgress"], async ({ ritualKey, harvestInProgress }) => {
            if (harvestInProgress) {
                sendResponse({ ok: false, error: "Harvest already in progress" });
                return;
            }
            if (!ritualKey) {
                sendResponse({ ok: false, error: "No ritual key configured" });
                return;
            }
            const weekKey = getMondayOfWeek(new Date());
            sendResponse({ ok: true });
            await runHarvest(ritualKey, weekKey);
        });
        return true; // async sendResponse
    }

    if (msg.type === "GET_STATUS") {
        chrome.storage.local.get(
            ["lastSuccessWeek", "lastRunDate", "lastRunResults", "harvestInProgress", "ritualKey"],
            (data) => sendResponse(data)
        );
        return true;
    }
});
