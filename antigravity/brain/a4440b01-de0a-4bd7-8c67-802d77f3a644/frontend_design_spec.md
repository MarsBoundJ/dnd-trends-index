# Frontend Design Specification: Arcane Analytics
**Project Name:** Arcane Analytics Dashboard  
**Framework:** Vanilla Web (No Framework)  
**Version:** 1.0  

---

## 1. UI/UX Philosophy: "The Glassmorphic Ledger"
The dashboard is designed as a premium, data-dense interface that blends high-tech analytics with a "Dark Fantasy" aesthetic.

### Key Visual Tokens:
- **Aesthetic**: Glassmorphism (semi-transparent backgrounds with backdrop blur).
- **Typography**: 
    - **Cinzel**: Used for headers and high-fantasy accents.
    - **Inter**: Used for high-readability data points and labels.
- **Color Palette**: 
    - **Primary**: Deep Obsidian (`#0a0a0c`)
    - **Accent**: Arcane Purple (`#9d00ff`) and Gold (`#ffd700`)
    - **Rarity Coding**: Legendary (Orange), Very Rare (Purple), Rare (Blue), Common (Grey).

---

## 2. Technical Architecture
The frontend is a Single Page Application (SPA) designed for zero-latency feel and high visual fidelity.

### Components:
1.  **Initiative Ticker**: A continuous horizontal scroller displaying real-time top trends.
2.  **Scrying Chart (Radar)**: Powered by `Chart.js`. Visualizes the "Trend Power" of the Top 10 entities in a multi-dimensional plot.
3.  **Arcane Cards (Grid)**: Display granular metrics from the backend.
    - **WIKI**: Wikipedia interest score.
    - **FAN**: Fandom wiki activity.
    - **YTB**: YouTube view velocity.

---

## 3. Data Integration Pattern
The dashboard consumes a standard JSON payload from the **Bouncer API**.

### Client-Side Logic (`app.js`):
- **Fetch Loop**: Asynchronous `fetch` calls to the `/trends` endpoint.
- **Resilience**: Implements a `MOCK_DATA` fallback. If the API returns a non-200 response or network fails, the UI automatically populates with cached "Codex Archives" data to ensure a seamless demo experience.
- **Normalization**: Percentile values (0.0 - 1.0) from the backend are multiplied by 100 and rounded for user-friendly "Point" values.

```javascript
// Integration Example
const response = await fetch(`${API_URL}/trends`);
const data = await response.json();
renderCards(data, container); // Dynamically builds the DOM
```

---

## 4. AI Studio Implementation Guide
When instructing an AI to modify this frontend:
- **Style Overrides**: All styling is handled via CSS Variables and the `.arcane-card` class in `style.css`.
- **Dynamic Content**: Data is bound to the DOM via template strings in `app.js`.
- **Charting**: To add new axes, modify the `radar` config in the `renderChart` function.
