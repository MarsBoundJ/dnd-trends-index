# Arcane Analytics: Technical Addendum

This document contains specific technical configuration, formulas, and design tokens to be appended to the Unified Project Documentation.

## 1. Mathematical Formulas & Normalization

### Fandom Hype Score
The `hype_score` is a linear decay function based on the article's rank (1-100) in the trending list.
*   **Formula**: `Hype = 1.01 - (Rank * 0.01)`
*   **Result**: Rank 1 = 1.00, Rank 100 = 0.01

### Silver Layer Normalization
To compare vastly different metrics (Wiki Views vs. Reddit Mentions), all raw values are normalized using a rolling 24-hour percentile rank within their specific source.

*   **Logic (SQL)**:
    ```sql
    PERCENT_RANK() OVER (
        PARTITION BY capture_date 
        ORDER BY raw_value ASC
    )
    ```
*   **Goal**: A score of `0.95` means this keyword performed better than 95% of *other keywords* on that specific platform for that day.

## 2. API JSON Payload
The **Bouncer API** returns a consolidated `DailyManifest` object. The Dashboard front-end expects this exact structure.

```json
{
  "status": "success",
  "meta": {
    "date": "2025-12-31",
    "record_count": 25
  },
  "data": [
    {
      "keyword": "Vecna",
      "trend_score": 98.5,
      "category": "Lore",
      "metrics": {
        "wiki_rank": 0.99,
        "fandom_rank": 0.95,
        "youtube_velocity": 0.88,
        "roll20_rank": 0.10
      },
      "pulse": {
        "headline": "Vecna's Return Sparks Theorycrafting",
        "hook": "New evidence in the 2024 DMG suggests the Whispered One is the primary antagonist.",
        "persona": "Sage"
      }
    }
  ]
}
```

## 3. Targeted Fandom Wikis (Registry)
The scraper actively monitors these 13 specific wikis, selected for their distinct audience signals.

1.  **`dnd5e`**: Mechanics, Rules, Class features (Player utilty).
2.  **`forgottenrealms`**: Primary Lore, Canon history (DM research).
3.  **`criticalrole`**: Influencer trends, Exandria lore.
4.  **`eberron`**: Steampunk/Noir fantasy signals.
5.  **`ravenloft`**: Horror/Gothic signals.
6.  **`dragonlance`**: High Fantasy/War signals.
7.  **`spelljammer`**: Sci-Fi/Astral signals.
8.  **`planescape`**: Multiverse/Philosophical signals.
9.  **`greyhawk`**: OSR/Classic signals.
10. **`darksun`**: Survival/Psionic signals.
11. **`mystara`**: Retro/Basic D&D signals.
12. **`dungeonsdragons`**: General aggregator.
13. **`5point5`**: 2024 Revision tracking.

## 4. Design Tokens (Glassmorphism)
These CSS variables (from `dashboard/style.css`) define the "Arcane Glass" aesthetic.

```css
:root {
    /* Backgrounds */
    --bg-dark: #0f0f13;
    --bg-panel: #1a1a2e;
    
    /* Text & Accents */
    --text-primary: #e0e0e0;
    --text-secondary: #a0a0b0;
    --accent-gold: #ffd700;    /* Primary Highlight */
    --accent-purple: #9d00ff;  /* Magic/Arcane */
    --accent-blue: #00d4ff;    /* Tech/Wiki */
    
    /* Glass Effect */
    --border-glass: rgba(255, 255, 255, 0.1);
    --shadow-glow: 0 0 15px rgba(157, 0, 255, 0.2);
    
    /* Fonts */
    --font-heading: 'Cinzel', serif;
    --font-body: 'Inter', sans-serif;
}
```
