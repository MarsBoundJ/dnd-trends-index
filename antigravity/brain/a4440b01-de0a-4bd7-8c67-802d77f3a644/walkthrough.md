# Phase 2 Pilot Walkthrough: Google Trends Pipeline

We have successfully completed the Pilot phase for the D&D Trends Index. This phase validated the end-to-end data pipeline from keyword expansion to automated BigQuery ingestion using rotating proxies.

## 🏁 Final Status: 100% Complete
The pilot targeted all **Classes** and **Subclasses** from the concept library.

| Metric | Result |
| :--- | :--- |
| **Total Pilot Terms (Targets)** | 1,619 |
| **Unique Search Strings** | 1,207 |
| **Successfully Processed** | **1,207 (100.0%)** |
| **Total Rows in BigQuery** | 63,976 |

> [!NOTE]
> The difference between 1,619 terms and 1,207 unique strings is due to duplicates in the concept library (e.g., "Alchemist" appearing in multiple source books) that normalize to the same search term.

## 📈 Initial Insights
By analyzing the pilot data, we can already see powerful trends emerging in the D&D community:

### 1. Edition War: 5e vs. 2024
We tracked terms with "5e" (2014 ruleset) vs. "2024" (New ruleset).

| Category | Total Interest (Pilot Data) |
| :--- | :--- |
| **5e (2014)** | 114,861 |
| **2024 (New)** | 47,661 |

**Insight:** Legacy "5e" terminology remains **~2.4x more popular** in search volume than the new "2024" identifiers.

### 2. High-Interest Intents
The "Other" category, which includes `[term] build` and subclass nicknames (e.g., "Lore Bard", "Shadow Monk"), generated **290,739 total interest points**—dwarfing the ruleset-specific searches. This validates our expansion strategy of focusing on **Builds** and **Nicknames**.

## 🛡️ Technical Resilience
The pipeline proved robust against Google Trend's aggressive rate limiting:
- **Proxy Solution:** Successfully implemented IP Authentication via `http://p.webshare.io:9999`.
- **Retry Logic:** Implemented a 500-item proxy rotation and an outer retry loop to handle intermittent blocks.
- **Niche Handling:** Added logic to record "Zero Interest" placeholders for very obscure terms, ensuring the scraper doesn't stall on infinite retries.

## ⏭️ Next Steps
With the pilot pipeline confirmed, we are ready for **Phase 3: Full Rollout**.
- Expand to all 13,000+ terms (Spells, Monsters, NPCs).
- Performance optimization for the larger dataset.
- Implement the "Monthly Incremental" logic for automated updates.
