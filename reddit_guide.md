# **Phase 4: Reddit Sentiment & Stats Engine \- Implementation Guide (Updated)**

## **🎯 Objective**

To build a high-volume social listening pipeline that scrapes 20+ D\&D-focused subreddits, matches comments against an 18,000-keyword master library using the Aho-Corasick algorithm, and performs context-aware sentiment analysis via Gemini 1.5 Flash for "viral" events.

---

## **🏗️ Architectural Overview**

1. **Ingestion:** Python (PRAW) running on a scheduled interval (Cloud Run or Cloud Functions).  
2. **Processing:**  
   * **Broad Stats:** Multi-pattern keyword matching via pyahocorasick.  
   * **Deep Sentiment:** Sentiment and Persona classification via **Gemini 1.5 Flash** (Vertex AI).  
3.   
4. **Storage:** BigQuery (dnd-trends-index dataset).  
5. **Security:** Reddit and GCP credentials managed via Secret Manager.

---

## **🛠️ Implementation Checklist for Antigravity**

### **🟢 Task 4.1: Subreddit Registry Setup**

**Action:** Create BigQuery table concept\_library.subreddit\_registry.  
**Schema:** subreddit\_name (STRING), tier (INT64), signal\_type (STRING), weight (FLOAT64), purpose (STRING).  
**Action:** Populate the table using the **Data Supplement** below.

### **🟢 Task 4.2: Keyword Trie Construction (Broad Stats)**

**Action:** Develop a Python module matcher.py using pyahocorasick.  
**Requirement:** Fetch 18k keywords from BigQuery; build an Aho-Corasick Automaton.  
**Logic:** Implement a search function to process comments in one pass.

### **🟢 Task 4.3: Reddit Scraper Development (PRAW)**

* **Action:** Create reddit\_harvester.py.  
  **Logic:**  
  * Fetch "Hot" and "Top" posts (Limit: 50 per sub).  
  * **Filtering (Logic \#2):** If the subreddit is Tier 2 or 3, only count mentions if an **Anchor Word** (e.g., 5e, class, DM, initiative, roll) is present in the text.

**Meme Volume Rule:** For r/dndmemes, apply a multiplier of **0.3x** to the mention\_count to prevent viral jokes from drowning out technical trends.

### **🟢 Task 4.4: Persona & Sentiment Logic (Gemini 1.5 Flash)**

**Trigger:** Set a "Heat Threshold" of **5,000 upvotes** for r/dndmemes.  
**Persona Detection:** Instruction Gemini to distinguish between **Player** (Heuristic: "My character/build") vs. **DM** (Heuristic: "My players/encounter").

### **🟢 Task 4.5: BigQuery Schema Finalization**

**Action:** Create tables reddit\_daily\_metrics and reddit\_viral\_events.

---

## **📊 Data Supplement: Subreddit Master List**

*Antigravity: Use this list to populate concept\_library.subreddit\_registry.*

| Subreddit | Tier | Signal Type | Weight | Purpose |
| :---- | :---- | :---- | :---- | :---- |
| **r/dndnext** | 1 | Technical/Mechanic | 1.2 | High-intent mechanics discussion. |
| **r/3d6** | 1 | PC Builds | 1.2 | Leading indicator for character builds. |
| **r/DnDHomebrew** | 1 | Leading/Innovation | 1.1 | Tracks future trends & homebrew needs. |
| **r/UnearthedArcana** | 1 | Design/Mechanic | 1.1 | High-quality homebrew traction. |
| **r/DnD** | 1 | General/Vibe | 0.7 | Massive volume, general community pulse. |
| **r/dndmemes** | 1 | Viral/Volume | 0.3 | Cultural pulse (Volume Only unless viral). |
| **r/DMAcademy** | 1 | DM Resources | 1.0 | DM pain points and monster usage. |
| **r/lfg** | 2 | Market Demand | 1.0 | Tracks system popularity via tags. |
| **r/Pathfinder2e** | 2 | Competitive | 0.9 | Context for D\&D sentiment shifts. |
| **r/DungeonsAndDragons** | 2 | General | 0.8 | Secondary general D\&D discussion. |
| **r/rpghorrorstories** | 3 | Negative Sentiment | 1.0 | Deep-dive into community pain points. |
| **r/BehindTheTables** | 3 | Lore/Flavor | 0.7 | Leading indicator for adventure themes. |
| **r/osr** | 3 | Niche/Lore | 0.8 | Tracks Batch 6 (Lore/Settings) trends. |
| **r/criticalrole** | 3 | Media/AP | 0.9 | Influencer-driven interest spikes. |
| **r/dimension20** | 3 | Media/AP | 0.9 | Influencer-driven interest spikes. |
| **r/adventurersleague** | 3 | Organized Play | 1.0 | Official play environment trends. |

---

## **🚦 Verification Points for Antigravity**

1. **Dnd-Trends Integration:** Ensure keyword\_id in Reddit tables successfully joins with your master concept\_library.  
2. **Logic \#2 Verification:** In non-Tier-1 subs, verify that a mention of "Fly" (Keyword) is ignored unless the word "5e" or "Spell" (Anchor) is also present.  
3. **Meme Sentinel:** Confirm the "Top 1%" trigger successfully initiates a Vertex AI call when a post crosses 5,000 upvotes.

**Antigravity:** Begin by creating the BigQuery subreddit\_registry table using the data provided in the Data Supplement.

