const API_URL = 'http://localhost:8082'; // Development Bouncer
let currentSource = 'google';
let chatHistory = [];
let heatmapActive = false;


// Initialize
document.addEventListener('DOMContentLoaded', () => {
    switchTab('market-pulse');
    fetchArcanePulse();
    renderDeltasTable();
    renderEditionChart();
    renderDMShortageChart();
    renderMomentumSparklines();
});

async function fetchArcanePulse() {
    try {
        const response = await fetch(`${API_URL}/system/vista/summary`);
        if (response.ok) {
            const data = await response.json();
            const migrationPercent = (data.migration_index * 100).toFixed(1);
            const deficitRatio = data.dm_shortage_index.toFixed(1);

            // Navbar updates
            document.getElementById('pulse-migration').innerText = migrationPercent;
            document.getElementById('pulse-deficit').innerText = deficitRatio;

            // KPI Grid Updates
            const metricMig = document.getElementById('metric-migration');
            const metricDef = document.getElementById('metric-deficit');
            const metric3p = document.getElementById('metric-3p');

            if (metricMig) metricMig.innerText = migrationPercent;
            if (metricDef) metricDef.innerText = deficitRatio;
            if (metric3p) {
                metric3p.innerText = data.gap_3p_count || '0';
            }

            // Market Weather Update
            updateMarketWeather(data);
        }
    } catch (err) {
        console.error("Pulse fetch failed", err);
    }
}

function updateMarketWeather(metrics) {
    const conditionEl = document.getElementById('weather-condition');
    if (!conditionEl) return;

    let condition = "STABLE";
    let color = "var(--text-muted)";

    if (metrics.positive_sentiment > 0.6) {
        condition = "FAVORABLE";
        color = "#69db7c";
    }
    if (metrics.migration_index > 0.1) {
        condition = "TRANSITIONING";
        color = "var(--color-search)";
    }
    if (metrics.dm_shortage_index > 1.4) {
        condition = "DM DROUGHT";
        color = "#ff3377";
    }

    conditionEl.innerText = `Condition: ${condition}`;
    conditionEl.style.color = color;
    conditionEl.style.textShadow = `0 0 15px ${color}55`;

    const hero = document.getElementById('market-weather-hero');
    if (hero) hero.style.borderLeftColor = color;
}

function toggleTheme() {
    const body = document.body;
    const isLit = body.classList.toggle('theme-lit');
    const toggleBtn = document.getElementById('theme-toggle');
    if (isLit) {
        toggleBtn.innerText = '[LIT]';
        toggleBtn.style.color = '#ff6b6b';
    } else {
        toggleBtn.innerText = '[VOID]';
        toggleBtn.style.color = 'var(--color-search)';
    }
}

function toggleHeatmap() {
    heatmapActive = !heatmapActive;
    const toggleBtn = document.getElementById('heatmap-toggle');
    if (heatmapActive) {
        toggleBtn.innerText = '[ON]';
        toggleBtn.style.color = '#fbbf24';
        toggleBtn.style.textShadow = '0 0 10px rgba(251,191,36,0.5)';
    } else {
        toggleBtn.innerText = '[OFF]';
        toggleBtn.style.color = 'var(--text-muted)';
        toggleBtn.style.textShadow = 'none';
    }

    // Refresh current view
    if (document.getElementById('market-pulse-view').style.display === 'none') {
        switchSource(currentSource);
    }
}


async function switchTab(tab) {
    // 1. Update Tab Buttons
    document.querySelectorAll('.nav-tab').forEach(t => t.classList.remove('active'));
    document.querySelector(`[data-tab="${tab}"]`).classList.add('active');

    // Hide/Show major sections
    const standardControls = document.getElementById('standard-controls');
    const deck = document.getElementById('accordion-deck');
    const marketPulse = document.getElementById('market-pulse-view');

    if (tab === 'market-pulse') {
        if (standardControls) standardControls.style.display = 'none';
        if (deck) deck.style.display = 'none';
        if (marketPulse) marketPulse.style.display = 'block';
        return; // Don't fetch leaderboards
    } else {
        if (standardControls) standardControls.style.display = 'block';
        if (deck) deck.style.display = 'block';
        if (marketPulse) marketPulse.style.display = 'none';
    }

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
            <button id="btn-bgg" class="source-btn active" onclick="switchSource('bgg')">> BGG (GAMES)</button>
            <button id="btn-rpggeek" class="source-btn" onclick="switchSource('rpggeek')">RPGGEEK (BOOKS)</button>
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
                'rpggeek': 'bgg-mode',
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

    const rows = items.slice(0, 20).map(item => {
        let heatmapStyle = '';
        if (heatmapActive && item.opportunity_index > 0) {
            const opacity = Math.min(item.opportunity_index / 100, 0.4);
            heatmapStyle = `style="background: rgba(251, 191, 36, ${opacity}); transition: background 0.5s ease;"`;
        }

        return `
            <tr ${heatmapStyle}>
                <td class="rank-col number-font">#${item.rank}</td>
                <td class="concept-col">${item.name}</td>
                <td class="score-col number-font">${Math.round(item.score)}</td>
                <td class="trend-col">
                    ${renderTrendOrBadge(item)}
                </td>
            </tr>
        `;
    }).join('');


    let col4Name = 'Trend';
    if (currentSource === 'fandom' || currentSource === 'wikipedia') col4Name = 'Wiki';
    if (currentSource === 'reddit' || currentSource === 'youtube') col4Name = 'Sentiment';
    if (['bgg', 'rpggeek', 'amazon', 'kickstarter', 'backerkit'].includes(currentSource)) col4Name = 'Market';

    const col3Name = (['bgg', 'rpggeek', 'amazon', 'kickstarter', 'backerkit'].includes(currentSource)) ? 'Buy Score' : 'Score';

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
    } else if (currentSource === 'bgg' || currentSource === 'rpggeek') {
        const quality = item.quality || 0;
        return `<span class="stats-badge" title="Quality: ${quality.toFixed(1)}/10">📦 ${item.owned.toLocaleString()} OWNED | ⭐ ${quality.toFixed(1)}</span>`;
    } else if (currentSource === 'amazon') {
        const priceStr = item.formatted_price || '$0.00';
        return `<span class="stats-badge" title="Amazon Sales Rank: ${item.sales_rank}">📦 #${item.sales_rank.toLocaleString()} | ${priceStr}</span>`;
    } else if (currentSource === 'kickstarter' || currentSource === 'backerkit') {
        return `<span class="stats-badge" title="Daily Funding Velocity">📈 ${item.velocity} / DAY</span>`;
    } else {
        return `<span style="color:#888; font-size:0.8rem;">--- STABLE</span>`;
    }
}

function handleChatKeyPress(event) {
    if (event.key === 'Enter') {
        sendMessage();
    }
}

async function sendMessage() {
    const input = document.getElementById('chat-input');
    const message = input.value.trim();
    if (!message) return;

    // Add user message to window
    appendChatMessage('user', `<strong>You:</strong> ${message}`);
    chatHistory.push({ role: 'user', content: message });
    input.value = '';

    const sendBtn = document.getElementById('chat-send');
    sendBtn.disabled = true;
    sendBtn.innerText = '[THINKING...]';

    try {
        const response = await fetch(`${API_URL}/system/analyst/chat`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ message: message })
        });

        if (response.ok) {
            const data = await response.json();
            appendChatMessage('bot', `<strong>Arcane Analyst:</strong> ${data.reply}`);
            chatHistory.push({ role: 'analyst', content: data.reply });
        } else {
            appendChatMessage('bot', `<strong>Error:</strong> The weave is tangled. Could not reach the Analyst.`);
        }
    } catch (err) {
        console.error("Chat error", err);
        appendChatMessage('bot', `<strong>Error:</strong> Failed to communicate with the Oracle.`);
    } finally {
        sendBtn.disabled = false;
        sendBtn.innerText = '[SEND]';
    }
}

function appendChatMessage(role, html) {
    const window = document.getElementById('chat-window');
    const div = document.createElement('div');
    div.className = `chat-message ${role}`;
    // Replace newlines with <br> for simple markdown-like formatting if received
    div.innerHTML = html.replace(/\n/g, '<br>');
    window.appendChild(div);
    window.scrollTop = window.scrollHeight;
}

// Briefing Logic
async function generateBriefing() {
    if (chatHistory.length === 0) {
        alert("The Scrying Chamber is empty. Chat with the Analyst first.");
        return;
    }

    const btn = document.getElementById('btn-generate-briefing');
    btn.disabled = true;
    btn.innerText = '[ ⏳ DISTILLING ... ]';

    try {
        const response = await fetch(`${API_URL}/system/analyst/briefing`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ chat_history: chatHistory })
        });

        if (response.ok) {
            const data = await response.json();
            document.getElementById('briefing-content').innerHTML = data.summary_text.replace(/\n/g, '<br>');
            document.getElementById('briefing-modal').style.display = 'flex';
        } else {
            alert('Failed to generate briefing.');
        }
    } catch (err) {
        console.error("Briefing error", err);
        alert('Error connecting to the Analyst.');
    } finally {
        btn.disabled = false;
        btn.innerText = '[ 📋 GENERATE BRIEFING ]';
    }
}

function closeBriefingModal() {
    document.getElementById('briefing-modal').style.display = 'none';
}

async function emailBriefing() {
    const email = document.getElementById('team-email').value;
    if (!email) {
        alert("Please enter a destination email.");
        return;
    }

    const btn = document.querySelector('.briefing-actions .source-btn:last-child');
    btn.innerText = '[ ✉️ DISPATCHING... ]';
    btn.disabled = true;

    const summaryText = document.getElementById('briefing-content').innerText;

    try {
        const response = await fetch(`${API_URL}/system/vista/email-report`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ email: email, summary: summaryText })
        });

        if (response.ok) {
            btn.innerText = '[ ✅ DISPATCH SENT ]';
            setTimeout(() => { btn.innerText = '[EMAIL TO TEAM]'; btn.disabled = false; }, 3000);
        } else {
            alert('Email dispatch failed.');
            btn.innerText = '[EMAIL TO TEAM]';
            btn.disabled = false;
        }
    } catch (err) {
        console.error("Email error", err);
        alert('Failed to dispatch email.');
        btn.innerText = '[EMAIL TO TEAM]';
        btn.innerText = '[EMAIL TO TEAM]';
        btn.disabled = false;
    }
}

async function renderDeltasTable() {
    try {
        const response = await fetch(`${API_URL}/leaderboards?source=google`);
        if (!response.ok) return;
        const data = await response.json();

        let allItems = [];
        for (const cat of data) {
            for (const item of cat.items) {
                const diff = (item.current_score || 0) - (item.score || 0);
                allItems.push({ name: item.name, delta: diff });
            }
        }

        allItems.sort((a, b) => b.delta - a.delta);
        const topMovers = allItems.slice(0, 10);

        const tbody = document.getElementById('deltas-tbody');
        if (!tbody) return;

        tbody.innerHTML = '';
        topMovers.forEach(item => {
            const tr = document.createElement('tr');
            tr.style.borderBottom = "1px solid var(--glass-border)";
            const isPositive = item.delta > 0;
            const color = isPositive ? '#69db7c' : (item.delta < 0 ? '#ff6b6b' : 'var(--text-muted)');
            const sign = isPositive ? '+' : '';

            tr.innerHTML = `
                <td style="padding: 0.5rem 0;">${item.name}</td>
                <td style="padding: 0.5rem 0; text-align: right; color: ${color}; font-weight: bold;">
                    ${sign}${item.delta.toFixed(1)}
                </td>
            `;
            tbody.appendChild(tr);
        });
    } catch (e) {
        console.error("Deltas render error:", e);
    }
}

async function renderEditionChart() {
    try {
        const response = await fetch(`${API_URL}/system/vista/chart_data`);
        if (!response.ok) return;
        const data = await response.json();

        const ctx = document.getElementById('editionChart');
        if (!ctx) return;

        new Chart(ctx, {
            type: 'line',
            data: {
                labels: data.labels,
                datasets: [
                    {
                        label: '2024 Revision',
                        data: data.dataset_2024,
                        borderColor: '#ff6b6b',
                        backgroundColor: 'rgba(255, 107, 107, 0.1)',
                        fill: true,
                        tension: 0.4
                    },
                    {
                        label: 'Legacy 5e',
                        data: data.dataset_legacy,
                        borderColor: '#4dabf7',
                        backgroundColor: 'rgba(77, 171, 247, 0.1)',
                        fill: true,
                        tension: 0.4
                    }
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                interaction: {
                    mode: 'index',
                    intersect: false,
                },
                plugins: {
                    legend: {
                        labels: { color: '#e0e0e0', font: { family: 'Inter' } }
                    }
                },
                scales: {
                    x: { ticks: { color: '#888' }, grid: { color: '#333' } },
                    y: {
                        ticks: { color: '#888', callback: function (value) { return value + '%' } },
                        grid: { color: '#333' }
                    }
                }
            }
        });
    } catch (e) {
        console.error("Chart render error:", e);
    }
}

async function renderDMShortageChart() {
    try {
        const response = await fetch(`${API_URL}/system/vista/dm-shortage`);
        if (!response.ok) return;
        const data = await response.json();

        const ctx = document.getElementById('dmShortageChart');
        if (!ctx) return;

        const playerGrowth = (data.find(d => d.persona === 'PLAYER')?.yoy_growth || 0) * 100;
        const dmGrowth = (data.find(d => d.persona === 'DM')?.yoy_growth || 0) * 100;

        new Chart(ctx, {
            type: 'bar',
            data: {
                labels: ['DM GROWTH', 'PLAYER GROWTH'],
                datasets: [{
                    label: 'YoY Growth %',
                    data: [dmGrowth, playerGrowth],
                    backgroundColor: [
                        dmGrowth < 0 ? 'rgba(255, 51, 119, 0.6)' : 'rgba(0, 212, 255, 0.6)',
                        playerGrowth < 0 ? 'rgba(255, 51, 119, 0.6)' : 'rgba(0, 212, 255, 0.6)'
                    ],
                    borderColor: [
                        dmGrowth < 0 ? '#ff3377' : '#00d4ff',
                        playerGrowth < 0 ? '#ff3377' : '#00d4ff'
                    ],
                    borderWidth: 1
                }]
            },
            options: {
                indexAxis: 'y',
                responsive: true,
                maintainAspectRatio: false,
                scales: {
                    x: {
                        beginAtZero: true,
                        grid: { color: '#333' },
                        ticks: { color: '#888', callback: v => v + '%' }
                    },
                    y: {
                        grid: { display: false },
                        ticks: { color: '#fff', font: { family: 'Cinzel', size: 12 } }
                    }
                },
                plugins: {
                    legend: { display: false }
                }
            }
        });
    } catch (e) {
        console.error("DM Shortage chart error:", e);
    }
}

async function renderMomentumSparklines() {
    try {
        const response = await fetch(`${API_URL}/system/vista/momentum`);
        if (!response.ok) return;
        const data = await response.json();

        const container = document.getElementById('sparklines-container');
        if (!container) return;
        container.innerHTML = '';

        data.forEach((item, index) => {
            const row = document.createElement('div');
            row.className = 'sparkline-row';
            row.innerHTML = `
                <div class="sparkline-label">${item.concept}</div>
                <div class="sparkline-canvas-container" style="position: relative; height: 30px; width: 100px;">
                    <canvas id="sparkline-${index}"></canvas>
                </div>
            `;
            container.appendChild(row);

            const ctx = document.getElementById(`sparkline-${index}`);
            new Chart(ctx, {
                type: 'line',
                data: {
                    labels: item.trend.map((_, i) => i),
                    datasets: [{
                        data: item.trend,
                        borderColor: 'var(--color-search)',
                        borderWidth: 2,
                        pointRadius: 0,
                        fill: false,
                        tension: 0.4
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: { legend: { display: false }, tooltip: { enabled: false } },
                    scales: {
                        x: { display: false },
                        y: { display: false }
                    }
                }
            });
        });
    } catch (e) {
        console.error("Sparklines error:", e);
    }
}
