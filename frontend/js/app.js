const API_URL = 'http://localhost:8081';
let currentSource = 'google';
let currentTab = 'search';

// Initialize
document.addEventListener('DOMContentLoaded', () => {
    fetchLeaderboards('google');
});

async function switchTab(tab) {
    currentTab = tab;
    // 1. Update Tab Buttons
    document.querySelectorAll('.nav-tab').forEach(t => t.classList.remove('active'));
    document.querySelector(`[data-tab="${tab}"]`).classList.add('active');

    // 2. Update Source Buttons
    const sourceSelector = document.getElementById('source-selector');
    if (tab === 'search') {
        sourceSelector.innerHTML = `
            <span class="label">SOURCE:</span>
            <button id="btn-google" class="source-btn active" onclick="switchSource('google')">> GOOGLE TRENDS</button>
            <button id="btn-fandom" class="source-btn" onclick="switchSource('fandom')">FANDOM</button>
            <button id="btn-wikipedia" class="source-btn" onclick="switchSource('wikipedia')">WIKIPEDIA</button>
        `;
        switchSource('google');
    } else if (tab === 'social') {
        sourceSelector.innerHTML = `
            <span class="label">SOURCE:</span>
            <button id="btn-reddit" class="source-btn active" onclick="switchSource('reddit')">> REDDIT</button>
            <button id="btn-youtube" class="source-btn" onclick="switchSource('youtube')">YOUTUBE</button>
        `;
        switchSource('reddit');
    } else if (tab === 'sales') {
        sourceSelector.innerHTML = `
            <span class="label">SOURCE:</span>
            <button id="btn-bgg" class="source-btn active" onclick="switchSource('bgg')">> BGG (COMMERCIAL)</button>
        `;
        switchSource('bgg');
    }
}

async function switchSource(source) {
    currentSource = source;

    // Toggle Button States
    document.querySelectorAll('.source-btn').forEach(btn => {
        btn.classList.remove('active');
        btn.innerText = btn.innerText.replace('> ', '');
    });

    const activeBtn = document.getElementById(`btn-${source}`);
    if (activeBtn) {
        activeBtn.classList.add('active');
        activeBtn.innerText = '> ' + activeBtn.innerText;
    }

    // Clear and Reload
    const deck = document.getElementById('accordion-deck');
    deck.innerHTML = '<div class="skeleton-ribbon"></div><div class="skeleton-ribbon"></div><div class="skeleton-ribbon"></div>';

    await fetchLeaderboards(source);
}

async function fetchLeaderboards(source) {
    try {
        const response = await fetch(`${API_URL}/leaderboards?source=${source}`);
        const data = await response.json();
        renderDeck(data);
    } catch (error) {
        console.error("Scrying failed:", error);
        document.getElementById('accordion-deck').innerHTML = `<div style="text-align:center; padding:2rem; color:red;">Connection to the Arcane Weave lost.</div>`;
    }
}

function renderDeck(categories) {
    const container = document.getElementById('accordion-deck');
    container.innerHTML = '';

    categories.forEach((cat, index) => {
        const ribbon = document.createElement('div');
        ribbon.className = `accordion-ribbon ${currentSource}-mode`;
        ribbon.innerHTML = `
            <div class="ribbon-header">
                <span style="opacity:0.5; margin-right:10px;">${index + 1 < 10 ? '0' + (index + 1) : index + 1}</span>
                ${cat.category.toUpperCase()}
            </div>
            <div class="ribbon-meta">
                <span class="heat-badge">🔥 ${Math.round(cat.heat)}% Heat</span>
                <span class="top-item" style="margin-left:15px; color:#888; font-size:0.9rem;">
                    Top: <span style="color:#fff">${cat.items[0]?.name || 'None'}</span>
                </span>
            </div>
        `;

        const drawer = document.createElement('div');
        drawer.className = 'leaderboard-drawer';
        drawer.innerHTML = renderTable(cat.items);

        ribbon.addEventListener('click', () => {
            const isOpen = drawer.classList.contains('open');
            document.querySelectorAll('.leaderboard-drawer').forEach(d => d.classList.remove('open'));
            if (!isOpen) drawer.classList.add('open');
        });

        container.appendChild(ribbon);
        container.appendChild(drawer);
    });
}

function renderTable(items) {
    const scoreHeader = currentSource === 'bgg' ? 'Buy Score' : 'Score';
    const trendHeader = currentSource === 'google' ? 'Trend' :
        currentSource === 'bgg' ? 'Ownership' : 'Source';

    const rows = items.slice(0, 20).map(item => `
        <tr>
            <td class="rank-col number-font">#${item.rank}</td>
            <td class="concept-col">${item.name}</td>
            <td class="score-col number-font">${Math.round(item.score)}</td>
            <td class="trend-col">
                ${renderTrendOrBadge(item)}
            </td>
        </tr>
    `).join('');

    return `
        <table>
            <thead><tr><th>Rank</th><th>Concept</th><th>${scoreHeader}</th><th>${trendHeader}</th></tr></thead>
            <tbody>${rows}</tbody>
        </table>
    `;
}

function renderTrendOrBadge(item) {
    if (currentSource === 'fandom' && item.source) {
        return `<span class="wiki-badge" data-wiki="${item.source}">${item.source}</span>`;
    } else if (currentSource === 'wikipedia' || item.source === 'wikipedia') {
        return `<span class="wiki-badge" data-wiki="wikipedia">[WIKI]</span>`;
    } else if (currentSource === 'bgg') {
        return `<span class="stats-badge" title="Quality: ${item.quality}/10">📦 ${item.owned.toLocaleString()} OWNED</span>`;
    } else if (currentSource === 'reddit') {
        const sentiment = item.sentiment > 0.2 ? 'Positive' : item.sentiment < -0.2 ? 'Negative' : 'Neutral';
        return `<span class="sentiment-badge ${sentiment.toLowerCase()}">${sentiment}</span>`;
    } else {
        return `<span style="color:#888; font-size:0.8rem;">--- STABLE</span>`;
    }
}
