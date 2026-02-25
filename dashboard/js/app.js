const API_URL = 'http://localhost:8081'; // Adjust if cloud function URL differs
let currentSource = 'google';

// Initialize
document.addEventListener('DOMContentLoaded', () => {
    switchTab('search');
});

async function switchTab(tab) {
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
            <button id="btn-amazon" class="source-btn" onclick="switchSource('amazon')">AMAZON</button>
            <button id="btn-kickstarter" class="source-btn" onclick="switchSource('kickstarter')">KICKSTARTER</button>
            <button id="btn-backerkit" class="source-btn" onclick="switchSource('backerkit')">BACKERKIT</button>
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
        console.log(`[ARCANE] Scrying source: ${source}...`);
        const response = await fetch(`${API_URL}/leaderboards?source=${source}`);

        if (!response.ok) {
            const errData = await response.json().catch(() => ({}));
            throw new Error(`API returned ${response.status}: ${errData.error || response.statusText}`);
        }

        const data = await response.json();
        console.log(`[ARCANE] Data received:`, data);

        if (!Array.isArray(data)) {
            throw new Error(`Expected array but received ${typeof data}. Path might be invalid or returning error object.`);
        }

        renderDeck(data);
    } catch (error) {
        console.error("Scrying failed:", error);
        document.getElementById('accordion-deck').innerHTML = `
            <div style="text-align:center; padding:2rem; color:#ff6b6b; background: rgba(255,0,0,0.1); border: 1px solid #ff6b6b; border-radius: 8px;">
                <strong>CONNECTION TO THE ARCANE WEAVE LOST</strong><br>
                <div style="font-family: 'Roboto Mono', monospace; font-size: 0.8rem; margin-top: 10px; color: #fff; opacity: 0.8;">
                    SIG: ${error.message}
                </div>
            </div>
        `;
    }
}

function renderDeck(categories) {
    const container = document.getElementById('accordion-deck');
    container.innerHTML = '';

    try {
        if (!categories || categories.length === 0) {
            container.innerHTML = `<div style="text-align:center; padding:2rem; opacity:0.5;">No trends found in the stars for ${currentSource}.</div>`;
            return;
        }

        categories.forEach((cat, index) => {
            // 1. Create Ribbon
            const ribbon = document.createElement('div');
            const modeClass = {
                'fandom': 'fandom-mode',
                'wikipedia': 'wiki-mode',
                'reddit': 'social-mode',
                'youtube': 'social-mode',
                'bgg': 'bgg-mode',
                'amazon': 'bgg-mode',
                'kickstarter': 'bgg-mode',
                'backerkit': 'bgg-mode'
            }[currentSource] || '';
            ribbon.className = `accordion-ribbon ${modeClass}`;
            ribbon.innerHTML = `
            <div class="ribbon-header">
                <span style="opacity:0.5; margin-right:10px;">${index + 1 < 10 ? '0' + (index + 1) : index + 1}</span>
                ${(cat.category || 'UNCATEGORIZED').toUpperCase()}
            </div>
            <div class="ribbon-meta">
                <span class="heat-badge">🔥 ${Math.round(cat.heat || 0)}% Activity</span>
                <span class="top-item" style="margin-left:15px; color:#888; font-size:0.9rem;">
                    Top: <span style="color:#fff">${cat.items[0]?.name || 'None'}</span> (${Math.round(cat.items[0]?.score || 0)})
                </span>
            </div>
        `;

            // 2. Create Drawer
            const drawer = document.createElement('div');
            drawer.className = 'leaderboard-drawer';
            drawer.innerHTML = renderTable(cat.items);

            // 3. Interaction
            ribbon.addEventListener('click', () => {
                const isOpen = drawer.classList.contains('open');
                // Close all others (Accordion behavior)
                document.querySelectorAll('.leaderboard-drawer').forEach(d => d.classList.remove('open'));

                if (!isOpen) drawer.classList.add('open');
            });

            container.appendChild(ribbon);
            container.appendChild(drawer);
        });
    } catch (err) {
        console.error("[ARCANE] Render crash:", err);
        container.innerHTML = `<div style="color:red; text-align:center; padding:2rem;">Render Crash: ${err.message}</div>`;
    }
}

function renderTable(items) {
    if (currentSource === 'youtube') {
        const cards = items.slice(0, 10).map(item => {
            const takes = (item.creator_takes || []).slice(0, 2).map(take => `
                <div class="creator-take">
                    <div class="verdict-text">"${take.verdict}"</div>
                    <div class="quote-tag">SIGNAL: ${take.context_quote} (${take.sentiment_label})</div>
                </div>
            `).join('');

            return `
                <div class="oracle-card">
                    <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:10px;">
                        <span style="font-weight:bold; color:var(--color-social); font-size:1.1rem;">${item.name}</span>
                        <span class="consensus-badge">CONSENSUS: ${Math.round(item.consensus_score)}%</span>
                    </div>
                    ${takes}
                    <div style="margin-top:10px; font-size:0.75rem; color:#666; text-align:right;">
                        Expert Sampling: ${item.creator_count} Creators
                    </div>
                </div>
            `;
        }).join('');
        return `<div class="oracle-deck-container">${cards}</div>`;
    }

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

    let col4Name = 'Trend';
    if (currentSource === 'fandom' || currentSource === 'wikipedia') col4Name = 'Wiki';
    if (currentSource === 'reddit' || currentSource === 'youtube') col4Name = 'Sentiment';
    if (['bgg', 'amazon', 'kickstarter', 'backerkit'].includes(currentSource)) col4Name = 'Momentum';

    const col3Name = (['bgg', 'amazon', 'kickstarter', 'backerkit'].includes(currentSource)) ? 'Buy Score' : 'Score';

    return `
        <table>
            <thead><tr><th>Rank</th><th>Concept</th><th>${col3Name}</th><th>${col4Name}</th></tr></thead>
            <tbody>${rows}</tbody>
        </table>
    `;
}

function renderTrendOrBadge(item) {
    if (currentSource === 'fandom' && item.source) {
        return `<span class="wiki-badge" data-wiki="${item.source}">${item.source}</span>`;
    } else if (currentSource === 'wikipedia' || item.source === 'wikipedia') {
        return `<span class="wiki-badge" data-wiki="wikipedia">[WIKI]</span>`;
    } else if (currentSource === 'reddit' || currentSource === 'youtube') {
        const sent = item.sentiment;
        let icon = 'sentiment_neutral';
        let color = 'sent-neut';
        if (sent > 0.05) { icon = 'sentiment_very_satisfied'; color = 'sent-pos'; }
        else if (sent < -0.05) { icon = 'sentiment_very_dissatisfied'; color = 'sent-neg'; }

        const trail = (item.history || []).map(h => {
            let dotColor = '#444';
            if (h === 1) dotColor = '#00ff00';
            else if (h === -1) dotColor = '#ff0000';
            return `<span style="display:inline-block; width:4px; height:4px; border-radius:50%; background:${dotColor}; margin:0 1px;"></span>`;
        }).join('');

        return `
            <div class="sentiment-container" style="display:flex; align-items:center;">
                <span class="material-symbols-outlined ${color}" style="font-size:1.2rem; margin-right:8px;">${icon}</span>
                <div class="trail-container" style="display:flex;">${trail}</div>
            </div>
        `;
    } else if (currentSource === 'bgg') {
        const quality = item.quality || 0;
        return `<span class="stats-badge" title="Quality: ${quality.toFixed(1)}/10">📦 ${item.owned.toLocaleString()} OWNED</span>`;
    } else if (currentSource === 'amazon') {
        const priceStr = item.formatted_price || '$0.00';
        return `<span class="stats-badge" title="Amazon Sales Rank: ${item.sales_rank}">📦 #${item.sales_rank.toLocaleString()} | ${priceStr}</span>`;
    } else if (currentSource === 'kickstarter' || currentSource === 'backerkit') {
        return `<span class="stats-badge" title="Daily Funding Velocity">📈 ${item.velocity} / DAY</span>`;
    } else {
        return `<span style="color:#888; font-size:0.8rem;">--- STABLE</span>`;
    }
}
