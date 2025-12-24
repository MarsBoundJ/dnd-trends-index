# Phase 2.5: Data Disambiguation & Collision Filtering

## Goal
Improve data purity by excluding search terms that are semantically broad or collide with other categories (Spells, Monsters) or external intellectual properties (Baldur's Gate 3, Diablo, Skyrim).

## User Review Required
> [!IMPORTANT]
> Some terms (e.g., "Warden", "Champion", "Raven Queen") are inherently ambiguous across multiple gaming franchises. We will prioritize **Data Quality** over **Exhaustiveness**, meaning we will drop terms that cannot be cleanly disambiguated.

### 3. Volume-Weighted Risk Analysis
We will apply different levels of scrutiny based on search volume:
- **High-Volume Outliers**: Manual review of top terms by interest.
- **Ambiguous Terms (< 500 Interest)**: If an MCI collision occurs, mandatory transformation to `[term] Dnd`.
    *   *Verification*: `Champion Dnd` (2765) > `Champion 5e` (2316) > `Champion Dnd 5e` (460).
    *   *Decision*: "Dnd" is the primary disambiguator.
- **Low-Volume terms (< 200 Interest)**: Aggressive auto-pruning. If a low-volume term hits the MCI or is a generic single word, it is **dropped** by default to clear "low-level noise."

### 4. Official vs. 3rd Party Separation
We will implement a logical separation (metadata tag) instead of separating the files.
- **Goal**: Allow "Official Only" reporting while maintaining a unified ecosystem view.
- **Method**: Add `is_official` (BOOL) column to `expanded_search_terms`.
- **Logic**: 
    - `TRUE` if `source_book` is in the Official WotC List (PHB, XGtE, TCoE, FToD, etc.).
    - `FALSE` if `source_book` is "Partnered Content" (MCDM, Grim Hollow, etc.).

### 5. Execution Steps
1.  **Draft the MCI**: Export all non-Class/Subclass terms from BigQuery to a local JSON list.
2.  **Tag Metadata**: Update `expand_keywords.py` to check `concept_library.source_book` and assign `is_official`.
3.  **Cleanse Expanded Terms**: Write a script `cleanse_expanded_terms.py` to run the current `trend_data_pilot` against the MCI.
4.  **Purge & Refill**: Delete the "dirty" records from `trend_data_pilot` and re-run with qualified strings.

## Phase 3: Category-Sequenced Batch Collection

### Strategy
We execute the data collection in prioritized batches to monitor quality and handle collisions dynamically.

### Batches
1.  **Backgrounds & Feats** (Completed)
    *   Validation: 100% success rate. 52-week data collected.
2.  **Races** (Completed & Refined)
    *   *Crucial Pivot*: Split "Monstrous Races" (Orc, Goblin) from Monsters.
    *   **Race Data**: collected as `[Race] Race 5e` (e.g., `Orc Race 5e`) to isolate PC intent.
3.  **Monsters, NPCs, Villains** (In Progress)
    *   **Monster Data**: collected as `[Monster] 5e` or `[Monster] Dnd`.
    *   *Safety*: "Stat block" terms were purged and replaced with "Dnd" to avoid BG3 wiki confusion.
    *   *Execution*: Parallel launch of 3 collectors to handle ~7,000 terms.
4.  **Spells, Items, MagicItems** (Completed)
    *   **Scope**: ~5,300 terms.
    *   **Magic Items**: Largest component (~3,740 terms).
5.  **Community & Influencers** (Running)
    *   **Scope**: YouTubers, Podcasts, Websites.
    *   **Strategy**: Identity-based expansion (`[Name]`, `[Name] Dnd`).
6.  **Lore** (Running)
    *   **Scope**: Deities, Factions, Planes, Settings.
7.  **Mechanics** (Running)
    *   **Scope**: Invocations, Rules, Builds, Slang.
8.  **Meta** (Running)
    *   **Scope**: AI Art, Tools, Editions.
9.  **Events & Beta** (Running)
    *   **Scope**: UA Content (~700 terms) & Conventions (~280 terms).
    *   **Strategy**: "Event-based" expansion.

### 7. API Strategy Note
*   **Technical Reality**: Google Trends `today 12-m` returns 52 weeks in a single API call.
*   **Decision**: We keep the full 52-week resolution (standard year-over-year data) but stick strictly to the **Category Stops** to allow for "fail fast" pivots.

## Verification Plan
### Automated Tests
- Script to count collisions between Category A and Category B search strings.
- Count of terms containing "build" vs "D&D 5e".

### Manual Verification
- Review the "Top 50" list to ensure no non-D&D terms (like "Warden" from another game) are spiking the charts.
