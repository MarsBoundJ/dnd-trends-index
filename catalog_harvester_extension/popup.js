// Arcane Incursion - Popup script

function getMondayOfWeek(date) {
    const d = new Date(date);
    const day = d.getDay();
    const diff = day === 0 ? -6 : 1 - day;
    d.setDate(d.getDate() + diff);
    return d.toISOString().split("T")[0];
}

function formatDate(isoStr) {
    if (!isoStr) return "—";
    const d = new Date(isoStr);
    return d.toLocaleDateString("en-US", { weekday: "short", month: "short", day: "numeric", hour: "2-digit", minute: "2-digit" });
}

function setBadge(el, text, type) {
    el.textContent = text;
    el.className = "badge " + type;
}

async function refreshStatus() {
    const status = await chrome.runtime.sendMessage({ type: "GET_STATUS" });

    const thisWeek = getMondayOfWeek(new Date());
    const scheduleBadge = document.getElementById("schedule-badge");
    const weekBadge = document.getElementById("week-badge");
    const lastRunEl = document.getElementById("last-run");
    const progressEl = document.getElementById("in-progress-indicator");
    const runBtn = document.getElementById("run-now-btn");

    // Key configured?
    setBadge(scheduleBadge, status.ritualKey ? "Mon 6am (active)" : "No key — paused", status.ritualKey ? "ok" : "err");

    // This week success?
    if (status.lastSuccessWeek === thisWeek) {
        setBadge(weekBadge, "Done ✓", "ok");
    } else {
        const today = new Date().getDay();
        setBadge(weekBadge, today === 6 ? "Pending (skip Sat)" : "Pending", "warn");
    }

    lastRunEl.textContent = formatDate(status.lastRunDate);

    // "Is a harvest running?" is NOT the same question as "is the flag set?".
    // A run killed mid-flight leaves the flag set with nothing behind it, which
    // is what disabled Run Now indefinitely on Sep 22. The background script
    // answers the real question via the lease's age; trust that, not the flag.
    const stuckEl = document.getElementById("stuck-indicator");
    const cancelBtn = document.getElementById("cancel-btn");

    if (status.harvestActive) {
        progressEl.style.display = "block";
        stuckEl.style.display = "none";
        runBtn.disabled = true;
        cancelBtn.style.display = "block";
    } else {
        progressEl.style.display = "none";
        stuckEl.style.display = status.harvestStuck ? "block" : "none";
        runBtn.disabled = false;
        // Offer the escape hatch while anything is still held, so a stuck run
        // is always one click from cleared rather than a console command.
        cancelBtn.style.display = status.harvestStuck ? "block" : "none";
    }

    // Show last results
    if (status.lastRunResults && status.lastRunResults.length > 0) {
        const section = document.getElementById("results-section");
        const list = document.getElementById("results-list");
        section.style.display = "block";
        // The tier breakdown is shown on success as well as failure. The V9 bug
        // survived for six months because a capture that "worked" was never looked
        // at — a run reporting 4,000 items and a Gold count on a three-shelf page
        // should be readable here without opening BigQuery.
        list.innerHTML = status.lastRunResults.map(r => `
            <div class="site-result">
                <span>${r.site}</span>
                ${r.success
                    ? `<span class="count">${r.count.toLocaleString()} items</span>`
                    : `<span class="error">${r.error || "failed"}</span>`}
            </div>
            ${r.tierSummary ? `<div class="tier-summary">${r.tierSummary}</div>` : ""}
        `).join("");
    }
}

// Load saved key into input (masked)
chrome.storage.local.get("ritualKey", ({ ritualKey }) => {
    if (ritualKey) {
        document.getElementById("ritual-key-input").value = ritualKey;
    }
});

// Save key
document.getElementById("save-key-btn").addEventListener("click", () => {
    const key = document.getElementById("ritual-key-input").value.trim();
    if (!key) return;
    chrome.storage.local.set({ ritualKey: key }, () => {
        const msg = document.getElementById("save-msg");
        msg.textContent = "Key saved.";
        setTimeout(() => { msg.textContent = ""; }, 2000);
        refreshStatus();
    });
});

// Run now
document.getElementById("run-now-btn").addEventListener("click", () => {
    const runMsg = document.getElementById("run-msg");
    runMsg.textContent = "Starting…";
    chrome.runtime.sendMessage({ type: "RUN_NOW" }, (resp) => {
        if (resp.ok) {
            runMsg.textContent = "Harvest started — check back shortly.";
            document.getElementById("run-now-btn").disabled = true;
            document.getElementById("in-progress-indicator").style.display = "block";
        } else {
            runMsg.textContent = resp.error || "Error";
        }
    });
});

// Cancel / clear a stuck harvest
document.getElementById("cancel-btn").addEventListener("click", () => {
    const runMsg = document.getElementById("run-msg");
    runMsg.textContent = "Cancelling…";
    chrome.runtime.sendMessage({ type: "CANCEL" }, () => {
        runMsg.textContent = "Harvest cancelled.";
        setTimeout(() => { runMsg.textContent = ""; }, 3000);
        refreshStatus();
    });
});

// ---------- Detail pass ----------

async function refreshDetail() {
    const s = await chrome.runtime.sendMessage({ type: "DETAIL_STATUS" });
    const box = document.getElementById("detail-status");
    const btn = document.getElementById("detail-btn");
    if (!s) return;
    if (s.running) {
        box.textContent = `Detail pass: ${s.cursor}/${s.total} · ${s.done} stored` +
                          (s.errors ? ` · ${s.errors} errors` : "");
        btn.textContent = "Stop Detail Pass";
        btn.dataset.mode = "stop";
    } else {
        const { harvestedProducts = [] } = await chrome.storage.local.get("harvestedProducts");
        box.textContent = harvestedProducts.length
            ? `${harvestedProducts.length} products from the last harvest ready`
            : "Run a harvest first — it collects the product list";
        btn.textContent = "Start Detail Pass";
        btn.dataset.mode = "start";
        btn.disabled = harvestedProducts.length === 0;
    }
}

document.getElementById("detail-btn").addEventListener("click", async () => {
    const btn = document.getElementById("detail-btn");
    if (btn.dataset.mode === "stop") {
        await chrome.runtime.sendMessage({ type: "DETAIL_STOP" });
    } else {
        const { harvestedProducts = [] } = await chrome.storage.local.get("harvestedProducts");
        const r = await chrome.runtime.sendMessage({ type: "DETAIL_START", queue: harvestedProducts });
        if (!r || !r.ok) {
            document.getElementById("detail-status").textContent = (r && r.error) || "Could not start";
            return;
        }
    }
    refreshDetail();
});

// Poll for status updates while popup is open
refreshStatus(); refreshDetail();
const interval = setInterval(() => { refreshStatus(); refreshDetail(); }, 3000);
window.addEventListener("unload", () => clearInterval(interval));
