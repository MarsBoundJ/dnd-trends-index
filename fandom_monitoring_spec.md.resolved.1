# Fandom Monitoring Specification: Arcane Analytics
**Project Name:** Arcane Analytics Fandom Stream  
**Target:** 11 Major D&D Wikis  
**Metric:** Trending Rank (1-100)  

---

## 1. Overview
The Fandom monitoring engine tracks high-velocity interest within the core D&D player base. Unlike Wikipedia (which measures mainstream relevance), Fandom activity serves as a primary indicator for **Active DM Preparation** and **Player Research**.

## 2. Monitored Wikis
The system scans the following 10 wikis daily. Each scan captures the Top 100 trending articles, filtering out meta-pages (e.g., "User:", "File:", "Home").

### Breakdown of Tracked Articles
*Approximate unique articles captured in historical snapshots:*

| Wiki Name | Unique Articles | Focus / Era |
| :--- | :--- | :--- |
| **Forgotten Realms** | ~584 | Core Lore, NPCs, Locations |
| **D&D 5.5 / 2024** | [New] | 2024 Revision Mechanics & Lore |
| **Critical Role** | ~482 | Exandria, Actual Play Trends |
| **D&D 5e** | ~421 | Mechanics, Spells, Class Features |
| **Eberron** | ~215 | Steampunk/Magical Tech Lore |
| **Ravenloft** | ~156 | Gothic Horror, Domains of Dread |
| **Dragonlance** | ~132 | High Fantasy Lore, Krynn |
| **Spelljammer** | ~98 | Astral Sea, Space Exploration |
| **Planescape** | ~84 | Multiverse, Sigil, Factions |
| **Greyhawk** | ~72 | Classic Lore, Oerth |
| **Dark Sun** | ~45 | Post-Apocalyptic Lore, Athas |

---

## 3. Data Extraction Logic
- **Endpoint**: `https://{wiki}.fandom.com/api/v1/Articles/Top`
- **Frequency**: Every 24 hours via Cloud Run.
- **Filtering**:
    - **Excluded**: Any article starting with `User:`, `Talk:`, `Category:`, `File:`, `Template:`.
    - **Excluded Titles**: "Home", "Wiki_Rules", "Search", "Special:Random".
- **Metric Mapping**:
    - **Rank 1**: 1.0 Signal Power.
    - **Rank 100**: 0.01 Signal Power.
    - The rank is inverted to create a `fandom_hype` score that translates directly into the Gold Layer.

## 4. Why these Wikis?
These specific wikis were selected to balance **Core Mechanics** (D&D 5e) with **Lore Spikes** (Forgotten Realms) and **Influencer-Driven Trends** (Critical Role). This ensures that a spike in "Vecna" on the Critical Role wiki is weighed alongside a spike in "Vecna" on the Forgotten Realms wiki for cross-validation.
