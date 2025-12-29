· 
const API_URL = "https://get-trend-data-appfh5mgjgiq-uc.a.run.app";

// Mock data fallback in case API fails (for demo purposes)
const MOCK_DATA = [
    { name: "Vecna", score: 95, hype_score: 98, play_score: 92, rarity: "Legendary" },
    { name: "Deck of Many Things", score: 88, hype_score: 90, play_score: 85, rarity: "Very Rare" },
    { name: "Strahd von Zarovich", score: 82, hype_score: 85, play_score: 80, rarity: "Very Rare" },
    { name: "Mimic", score: 75, hype_score: 70, play_score: 80, rarity: "Rare" },
    { name: "Owlbear", score: 65, hype_score: 60, play_score: 70, rarity: "Rare" },
];

async function fetchData() {
    const container = document.getElementById('card-grid');
    const ticker = document.getElementById('ticker-content');

    try {
        const response = await fetch(API_URL);
        let data;

        if (!response.ok) {
            console.warn("API failed, using Codex Archives (Mock Data)");
            data = MOCK_DATA; // Fallback
        } else {
            data = await response.json();
        }

        renderTicker(data, ticker);
        renderCards(data, container);
        renderChart(data);

    } catch (error) {
        console.error("Critical Failure:", error);
        // Ensure UI isn't empty on error
        renderCards(MOCK_DATA, container);
    }
}

function renderTicker(data, element) {
    // Top 5 items for ticker
    const top5 = data.slice(0, 5).map(item => `â˜… ${item.name} (${Math.round(item.score)})`).join("   ");
    element.textContent = top5 + "   " + top5; // Duplicate for smooth scroll
}

function renderCards(data, container) {
    container.innerHTML = '';

    // Take Top 12
    data.slice(0, 12).forEach(item => {
        const card = document.createElement('div');
        card.className = 'arcane-card';

        // Normalize rarity class
        const rarityClass = `rarity-${item.rarity.toLowerCase().replace(' ', '-')}`;

        card.innerHTML = `
            <div class="card-header">
                <span class="card-title">${item.name}</span>
                <span class="card-rarity ${rarityClass}">${item.rarity}</span>
            </div>
            <div class="stat-block">
                <div class="stat-box">
                    <span class="stat-label">STR (Play)</span>
                    <span class="stat-value">${Math.round(item.play_score || item.score * 0.8)}</span>
                </div>
                <div class="stat-box">
                    <span class="stat-label">CHA (Hype)</span>
                    <span class="stat-value">${Math.round(item.hype_score || item.score * 0.9)}</span>
                </div>
                <div class="stat-box">
                    <span class="stat-label">CON (Score)</span>
                    <span class="stat-value" style="color:white">${Math.round(item.score)}</span>
                </div>
            </div>
        `;
        container.appendChild(card);
    });
}

function renderChart(data) {
    const ctx = document.getElementById('trendChart').getContext('2d');
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
· "(0f1ceee2742f32be6a66898aa01f4fd3b072102f2/file:///C:/Users/Yorri/.gemini/dashboard/app.js:file:///C:/Users/Yorri/.gemini