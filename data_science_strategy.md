# D&D Trend Intelligence: Data Science Strategy

This document outlines the analytical techniques designed to transform raw Google Trends data into actionable insights for Content Creators and the Community.

## 1. The "Billboard" Charts (Dynamic Leaderboards)
We will adapt the list size based on the "Depth" of the category (using >200 items as the threshold).
*   **Technique**: `GROUP BY category, keyword`.
    *   **Large Categories (Monsters, Spells)**: **Top 40** (Billboard 40).
    *   **Niche Categories (Locations, Feats)**: **Top 20**.
*   **Value**: Ensures we don't list "bottom of the barrel" items for small categories while showing the full breadth of large ones.

## 2. Volatility & Hype Cycles (Exploratory)
Before defining "Momentum", we must understand the *shape* of the data.
*   **Volatility Profile**: Calculate Standard Deviation of interest over time.
    *   **Stable**: High Volume, Low Volatility (e.g., "Dragon"). *Evergreen Content*.
    *   **Spike**: Low Volume, High Volatility (e.g., "Vecna" during a release). *News Content*.
*   **Long-Term Trends**: Compare Monthly and Quarterly averages to find slow-burn risers vs. flash-in-the-pan fads.

## 3. The "Blue Ocean" Opportunity Matrix
This connects Search Volume (Demand) with Content Saturation (Supply - approximated by Wiki/Fandom presence or just "Knowledge Base" presence).
*   **Quadrants**:
    *   **High Demand / Low Saturation**: **"The Blue Ocean"**. (e.g., specific obscure 2e monsters trending due to Stranger Things). **Action**: Make multiple videos/articles.
    *   **High Demand / High Saturation**: **"The Red Ocean"**. (e.g., Tarrasque, Beholder). **Action**: High quality required to compete.
    *   **Low Demand / High Saturation**: **"Legacy/Archives"**.
    *   **Low Demand / Low Saturation**: **"The Void"**.

## 4. The "DM's Pulse" (Weekly Seasonality)
We will aggregate interest scores by `DayOfWeek` (0-6).
*   **Hypothesis**:
    *   **Utility Terms** (Conditions, Spells) peak on **Game Nights** (Fri/Sat/Sun).
    *   **Inspiration Terms** (Lore, Locations, Builds) peak on **Prep Days** (Tue/Wed).
*   **Value**: Tailor release schedules. "Release Build Guides on Tuesday", "Release Rules Explainers on Friday".

## 5. Share of Voice (Category Dominance)
A normalized comparison of total interest volume per category.
*   **Technique**: Sum of all `interest` scores in `Monster` vs. `Spell`.
*   **Value**: Understanding the macro-trends. Are people shifting from "Mechanics" (Player focus) to "Lore" (DM/Fan focus)?

## Next Steps
We will try to implement a script `analyze_trends_deep_dive.py` to calculate these metrics from the BigQuery Pilot data.
