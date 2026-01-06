# Implementation Plan: Categorization Retrofit (Safe Merge)

This plan outlines the surgical enrichment of the `concept_library` with keyword categories while preserving existing data.

## Proposed Changes

### [Component] BigQuery (Library & Views)
#### [MODIFY] `concept_library` (Data Update)
- Surgical MERGE of category data from `dnd_keywords.csv` into the production library.
- Preserves existing rows while updating categories for matched keywords and adding new ones.

#### [MODIFY] `silver_data.norm_fandom` (View)
- Retrofit to JOIN against `concept_library` for category propagation.

#### [MODIFY] `gold_data.trend_scores` (View)
- Expose the `category` column for dashboard-level filtering.

---

## Execution Steps
1. **Safety Backups**: Snapshot `fandom_daily_metrics` and `concept_library`. (COMPLETED)
2. **Surgical Merge**:
   - Load `antigravity/scratch/dnd_keywords.csv` into `staging_categories`.
   - Execute SQL MERGE to update/insert concepts.
   - Drop `staging_categories`.
3. **View Retrofit**:
   - Update Silver layer normalization view.
   - Update Gold layer trend score view.
4. **Frontend Activation**:
   - Update `style.css` with Fandom bar and Category badge styles.
   - Update `app.js` to render categories and Fandom progress bars.
5. **Sector Intelligence (Category Leaderboard)**:
   - Add `/categories` endpoint to Bouncer API.
   - Add `sector-analysis` ticker container to `index.html`.
   - Implement ticker fetching and rendering in `app.js`.
   - Style horizontally scrolling sector items in `style.css`.
6. **Verification**: Query the Gold views and verify the UI updates locally via browser or inspection.

## Verification Plan
1. **Join Integrity**: Verify that `norm_fandom` correctly associates keywords with categories.
2. **Dashboard Readiness**: Confirm `trend_scores` contains the `category` field for all relevant keywords.
