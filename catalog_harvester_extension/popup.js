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

    // In progress?
    if (status.harvestInProgress) {
        progressEl.style.display = "block";
        runBtn.disabled = true;
    } else {
        progressEl.style.display = "none";
        runBtn.disabled = false;
    }

    // Show last results
    if (status.lastRunResults && status.lastRunResults.length > 0) {
        const section = document.getElementById("results-section");
        const list = document.getElementById("results-list");
        section.style.display = "block";
        list.innerHTML = status.lastRunResults.map(r => `
            <div class="site-result">
                <span>${r.site}</span>
                ${r.success
                    ? `<span class="count">${r.count.toLocaleString()} items</span>`
                    : `<span class="error">${r.error || "failed"}</span>`}
            </div>
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

// Poll for status updates while popup is open
refreshStatus();
const interval = setInterval(refreshStatus, 3000);
window.addEventListener("unload", () => clearInterval(interval));
