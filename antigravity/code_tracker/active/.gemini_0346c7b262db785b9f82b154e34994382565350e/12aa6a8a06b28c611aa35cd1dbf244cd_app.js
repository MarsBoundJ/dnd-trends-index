Ú6
const API_URL = "https://bouncer-api-kfh5mgjgiq-uc.a.run.app";

let activeCategory = null;
let globalTrendData = [];

// Mock data fallback in case API fails (for demo purposes)
const MOCK_DATA = [
    { name: "Vecna", category: "Villain", score: 95, norm_wiki: 0.98, norm_fandom: 0.92, norm_youtube: 0.85, rarity: "Legendary" },
    { name: "Deck of Many Things", category: "Magic Item", score: 88, norm_wiki: 0.90, norm_fandom: 0.85, norm_youtube: 0.70, rarity: "Very Rare" },
    { name: "Strahd von Zarovich", category: "Villain", score: 82, norm_wiki: 0.85, norm_fandom: 0.80, norm_youtube: 0.60, rarity: "Very Rare" },
    { name: "Mimic", category: "Monster", score: 75, norm_wiki: 0.70, norm_fandom: 0.80, norm_youtube: 0.50, rarity: "Rare" },
    { name: "Owlbear", category: "Monster", score: 65, norm_wiki: 0.60, norm_fandom: 0.70, norm_youtube: 0.40, rarity: "Rare" },
];

async function fetchData() {
    const container = document.getElementById('card-grid');
    const ticker = document.getElementById('ticker-content');

    try {
        const response = await fetch(`${API_URL}/trends`);
        let data;

        if (!response.ok) {
            console.warn("API failed, using Codex Archives (Mock Data)");
            data = MOCK_DATA; // Fallback
        } else {
            data = await response.json();
        }

        globalTrendData = data;
        renderTicker(data, ticker);
        renderCards(data);
        renderChart(data);
        fetchCategories();

    } catch (error) {
        console.error("Critical Failure:", error);
        globalTrendData = MOCK_DATA;
        renderCards(MOCK_DATA);
    }
}

async function fetchCategories() {
    try {
        const res = await fetch(`${API_URL}/categories`);
        const data = await res.json();
        const grid = document.getElementById('mana-grid');

        grid.innerHTML = data.map(cat => `
            <div class="mana-tile" onclick="toggleFilter('${cat.category}')" id="tile-${cat.category.replace(/\s+/g, '-')}">
                <div class="tile-name">${cat.category}</div>
                <div class="tile-score">${Math.round(cat.avg_category_score)}</div>
            </div>
        `).join('');
    } catch (e) {
        console.warn("Category fetch failed:", e);
    }
}

function toggleFilter(category) {
    if (activeCategory === category) {
        activeCategory = null;
    } else {
        activeCategory = category;
    }

    document.querySelectorAll('.mana-tile').forEach(el => el.classList.remove('active'));
    if (activeCategory) {
        const targetId = `tile-${category.replace(/\s+/g, '-')}`;
        const targetEl = document.getElementById(targetId);
        if (targetEl) targetEl.classList.add('active');
    }

    renderCards(globalTrendData);
}

function renderTicker(data, element) {
    // Top 5 items for ticker
    const top5 = data.slice(0, 5).map(item => `â˜… ${item.name} (${Math.round(item.score)})`).join("   ");
    element.textContent = top5 + "   " + top5; // Duplicate for smooth scroll
}

function renderCards(data) {
    const container = document.getElementById('card-grid');
    container.innerHTML = '';

    const filteredData = activeCategory
        ? data.filter(item => item.category === activeCategory)
        : data;

    filteredData.slice(0, 12).forEach(item => {
        const card = document.createElement('div');
        card.className = 'arcane-card';

        const rarityClass = `rarity-${item.rarity.toLowerCase().replace(' ', '-')}`;

        const wikiPct = Math.round((item.norm_wiki || 0) * 100);
        const fanPct = Math.round((item.norm_fandom || 0) * 100);
        const ytbPct = Math.round((item.norm_youtube || 0) * 100);

        card.innerHTML = `
            <div class="card-header">
                <div class="card-content">
                    <span class="card-title">${item.name}</span>
                    <span class="category-badge" data-cat="${item.category}">${item.category}</span>
                </div>
                <span class="card-rarity ${rarityClass}">${item.rarity}</span>
            </div>
            <div class="stat-block">
                <div class="stat-row" title="Wikipedia Interest">
                    <span class="label">WIKI</span>
                    <div class="progress-track">
                        <div class="stat-bar wiki" style="width: ${wikiPct}%"></div>
                    </div>
                    <span class="value">${wikiPct}</span>
                </div>
                <div class="stat-row" title="Fandom Activity">
                    <span class="label">FAN</span>
                    <div class="progress-track">
                        <div class="stat-bar fandom" style="width: ${fanPct}%"></div>
                    </div>
                    <span class="value">${fanPct}</span>
                </div>
                <div class="stat-row" title="YouTube Velocity">
                    <span class="label">YTB</span>
                    <div class="progress-track">
                        <div class="stat-bar youtube" style="width: ${ytbPct}%"></div>
                    </div>
                    <span class="value">${ytbPct}</span>
                </div>
            </div>
            <div class="trend-score-pill">
                 <span class="stat-label">Total Power:</span>
                 <span class="stat-value">${Math.round(item.score)}</span>
            </div>
        `;
        container.appendChild(card);
    });

    if (filteredData.length === 0) {
        container.innerHTML = '<div class="loading">No artifacts found in this sector...</div>';
    }
}

function renderChart(data) {
    const ctx = document.getElementById('trendChart').getContext('2d');

    const existingChart = Chart.getChart(ctx);
    if (existingChart) existingChart.destroy();

    const top10 = data.slice(0, 10);

    new Chart(ctx, {
        type: 'radar',
        data: {
            labels: top10.map(d => d.name),
            datasets: [{
                label: 'Trend Power',
                data: top10.map(d => d.score),
                backgroundColor: 'rgba(157, 0, 255, 0.2)',
                borderColor: 'rgba(157, 0, 255, 1)',
                borderWidth: 2,
                pointBackgroundColor: '#ffd700'
            }]
        },
        options: {
            scales: {
                r: {
                    angleLines: { color: 'rgba(255,255,255,0.1)' },
                    grid: { color: 'rgba(255,255,255,0.1)' },
                    pointLabels: { color: '#a0a0b0', font: { family: 'Cinzel' } },
                    ticks: { display: false }
                }
            },
            plugins: {
                legend: { display: false }
            }
        }
    });
}

// Initialize
document.addEventListener('DOMContentLoaded', fetchData);
Ú6"(0346c7b262db785b9f82b154e34994382565350e2/file:///C:/Users/Yorri/.gemini/dashboard/app.js:file:///C:/Users/Yorri/.gemini