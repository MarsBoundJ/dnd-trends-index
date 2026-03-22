# Arcane Analytics — Concept Library Reference
**GCP Project:** `dnd-trends-index`
**Last Updated:** 2026-03-22

---

## What the Concept Library Is

`dnd_trends_categorized.concept_library` is the master keyword database — the authoritative list of every D&D/TTRPG term being tracked. Each row represents one concept and its Google Trends score.

---

## Known Scale
- 314 rules terms
- 416 spells
- 125 feats across four sourcebooks
- Species, magic items, monsters, classes, subclasses

---

## Category Taxonomy (Current)

### Official Content Categories
| Category | Description |
|---|---|
| `class` | Official WotC character classes (Fighter, Wizard, etc.) |
| `subclass` | Official WotC subclasses (Champion, Evocation, etc.) |
| `spell` | Official spells |
| `feat` | Official feats |
| `monster` | Generic monsters (Goblin, Dragon, etc.) — NOT named NPCs |
| `species` | Player character species (formerly races) |
| `magic_item` | Named magic items |
| `rules` | Rules terms and mechanics |

### Edition Comparison Framework
Terms are tagged for edition relevance:
- `BOTH` — exists in both 2014 and 2024 editions
- `CHANGED_2024` — exists in both but meaningfully changed
- `2024_ONLY` — new in 2024 PHB/DMG/MM
- `2014_ONLY` — removed or replaced in 2024

### Key empirical finding
Users search bare class/spell names, not creator-attributed variants. "2024" outperforms "5.5e" as a search qualifier. Confirmed through empirical Google Trends testing.

---

## Four Known Messy Areas

### 1. Homebrew Classes / Subclasses
**Status:** Incomplete — category coverage uncertain
**Problem:** Homebrew content is creator-attributed, not mechanic-attributed. People search `"blood hunter"` or `"blood hunter 5e"` not `"homebrew class dnd"`. The long tail of creator content (Matt Mercer, Kibbles' Tasty, etc.) is not systematically tracked.
**Proposed solution:** 
- Add `homebrew_class` and `homebrew_subclass` as explicit categories
- Use the related queries discovery pipeline to surface actual search terms organically rather than enumerating manually
- Decision needed: whether to track creator attribution (e.g. "Mercer homebrew") as a separate field
**Decision status:** DEFERRED — Phil to confirm category taxonomy

### 2. Unearthed Arcana (UA)
**Status:** Abandoned in prior work — not currently tracked
**Why it matters:** UA is WotC's R&D signal — it shows where D&D mechanics are heading. Major driver of future classes, subclasses, and new mechanics. Historical UA goes back to 2014 when 5e launched.
**Problem:** UA content has three life stages:
1. `playtest` — UA only, may be revised or abandoned
2. `revised` — appeared in subsequent UA with changes
3. `official` — published in a sourcebook
4. `abandoned` — never made it to print

A term like `"ranger playtest"` means something very different depending on when it was searched.
**Proposed solution:**
- UA as its own category with a `status` field (`playtest / revised / official / abandoned`)
- Enables tracing demand curves from playtest through publication
- Allows correlation of UA announcement → search spike → publication
**Decision needed:** Whether to backfill historical UA terms from 2014 or start fresh from current UA
**Decision status:** DEFERRED

### 3. Baldur's Gate 3 (BG3)
**Status:** Not cleanly separated from TTRPG signals — risk of data pollution
**Problem:** BG3 is a massive driver of D&D search interest, but BG3-specific terms (Astarion, Shadowheart, Githyanki, etc.) represent video game search intent, not TTRPG search intent. Mixing them pollutes category scores.
**The nuance:** BG3 terms ARE worth tracking precisely *because* they drive TTRPG demand — they're leading indicators of new player interest. They should be tracked but clearly labeled.
**Proposed solution:**
- `videogame_bg3` as an explicit isolated category
- Gemini standing instruction: any term where primary search intent is clearly the video game → `videogame_bg3`
- "Bridge terms" (Astarion, Shadowheart, Githyanki, Tiefling) tracked as BG3 terms with a `drives_ttrpg_interest` flag
- Bridge terms serve as demand driver signals in the dashboard, not TTRPG category scores
**Decision needed:** Final list of bridge terms; whether BG3 gets its own dashboard section
**Decision status:** DEFERRED

### 4. Edition Comparison (2014 vs 2024)
**Status:** Framework exists, implementation partial
**What's built:** BOTH/CHANGED_2024/2024_ONLY/2014_ONLY tagging system
**What's missing:** Consistent application across all categories; some categories more complete than others
**Key challenge:** Some 2024 changes are cosmetic (rename) vs. mechanical (redesign) — the tagging doesn't currently distinguish these

---

## Monster Category — Known Data Quality Issue

Named NPCs and villains were incorrectly categorized as generic monsters during initial data entry. Examples: Strahd, Vecna, Acererak — these are named characters, not generic monster types.

**Active remediation:** `monster-classifier` Cloud Function using Vertex AI/Gemini
- Targets Monster entries from `dnd_keywords.csv`
- Writes PENDING classification suggestions to `ai_suggestions` table
- Architecture conclusion: regex-based filtering can only cleanly flag a small number of entries; Gemini judgment is the primary filter

---

## Concept Variations (Planned — Not Yet Built)

Each concept in `concept_library` will eventually have associated search variants tracked in a separate `concept_variations` table:

```
concept: "ranger"
search_variants: ["ranger dnd", "ranger 2024", "dnd ranger guide", "ranger 5e build"]
best_variant: "ranger dnd"    ← used for leaderboard scoring
```

The variants array gives Gemini a behavioral fingerprint of each concept, dramatically improving its ability to classify whether new related query terms are variants or new concepts.

See `ARCHITECTURE.md` for the full `concept_variations` schema.

---

## Category Decisions Still Needed

| Decision | Context | Status |
|---|---|---|
| Homebrew category taxonomy | `homebrew_class` / `homebrew_subclass` — confirm and add | DEFERRED |
| Creator attribution tracking | Track "Mercer", "Kibbles" etc. as a field? | DEFERRED |
| UA backfill scope | All UA since 2014 or current UA only? | DEFERRED |
| UA status field values | Confirm: playtest / revised / official / abandoned | DEFERRED |
| BG3 bridge terms list | Which BG3 terms drive TTRPG demand? | DEFERRED |
| BG3 dashboard treatment | Own section or integrated with demand driver signals? | DEFERRED |
