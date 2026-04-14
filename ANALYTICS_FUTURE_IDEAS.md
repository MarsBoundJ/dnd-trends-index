# Analytics: Future Ideas (Set Aside for Later)

Ideas from Perplexity and Gemini evaluated during the single-stream analytics session (April 13, 2026) and the composite analytics session (April 13, 2026). Each was set aside because current data or infrastructure doesn't yet support it. Worth revisiting as the platform matures and data collection expands.

This is a **living document** — when one of these ideas becomes buildable, implement it, then delete the entry (or mark it as shipped).

---

## Part 1 — Single-Stream Enhancements

Expansions to existing data streams that would unlock new analytics but require new scraping or API work.

### 1. Fandom Wikis — Network Centrality & Editor Breadth
- **Source:** Perplexity
- **Idea:** Track intra-wiki cross-links to each article page (network centrality) to identify "hub" lore pages. Also track distinct editors per article to distinguish community-wide interest from single-editor obsession.
- **Why set aside:** We collect `edit_count` and `view_count` but not distinct editor IDs or link graph data. Would require expanding the Fandom scraper to pull revision history (editor usernames) and parse wikitext for internal links.
- **Future path:** Fandom API has revision history endpoints. Add `distinct_editors_30d` column to `fandom_daily_metrics`. Link graph would require a separate crawl pass.

### 2. Amazon — Review Text NLP via Vertex AI
- **Source:** Gemini
- **Idea:** Run Gemini/Vertex AI sentiment analysis over Amazon review text to isolate casual fan and gift-buyer opinions — a demographic completely silent on Reddit and YouTube.
- **Why set aside:** We currently collect `asin`, `rank`, and `price_cents` from Amazon, but NOT review text. The Amazon bookmarklet harvester focuses on product metadata.
- **Future path:** Amazon Product Advertising API provides review snippets. Or expand the bookmarklet to capture review text. Would need a new BQ table `amazon_reviews` with `asin`, `review_text`, `rating`, `date`, then a Vertex AI batch sentiment pipeline.

### 3. Roll20 — Campaign Session Retention
- **Source:** Gemini
- **Idea:** Track how many Roll20 campaigns persist over multiple sessions vs fizzle after 1-2 sessions. Measures true "stickiness" of adventure modules.
- **Why set aside:** We get weekly rank snapshots from Roll20's public listings, not per-campaign session-level data. Roll20 doesn't expose campaign longevity data publicly.
- **Future path:** Would require Roll20 API access (if it exists) or a fundamentally different scraping approach tracking individual campaign listings over time.

### 4. YouTube — Content Format Classifier
- **Source:** Gemini
- **Idea:** Classify YouTube videos into format types: actual play, build guide, lore explainer, review, DM prep, news/reaction. Knowing that "8 of the top 10 Vecna videos are lore explainers" tells a different story than "8 of the top 10 are build guides."
- **Why set aside:** Our `yt_video_intelligence` table has Gemini-generated `concept_name`, `verdict`, and `sentiment_label`, but no format/genre classification.
- **Future path:** Add a `content_format` column to `yt_video_intelligence` and update the Gemini prompt in the intelligence pipeline to also classify format. **Relatively easy lift — just a schema change + prompt update.** This is the lowest-hanging fruit in Part 1.

### 5. Wikipedia — Edit Quality Analysis (Reverts vs Constructive)
- **Source:** Perplexity
- **Idea:** Track frequency of reverted edits vs constructive edits on D&D Wikipedia articles. High revert rate = controversial topic. Stable constructive edits = settled consensus.
- **Why set aside:** We collect `daily_views` from the Wikimedia pageviews API but not edit history. Wikipedia's API does expose revision history with revert detection.
- **Future path:** Add a Wikipedia edit harvester that pulls recent revisions per tracked article. New BQ table `wikipedia_edit_activity` with `article_title`, `date`, `edit_count`, `revert_count`, `distinct_editors`.

### 6. Itch.io — Download/Sales Stats
- **Source:** Perplexity
- **Idea:** Track download counts and sales data per Itch.io product to measure actual adoption, not just listing existence.
- **Why set aside:** Itch.io doesn't publicly expose download counts for most products. Our `itchio_products` table has `title`, `price`, `tags` but no volume metrics.
- **Future path:** Some Itch.io creators display download counts on their pages. Could selectively scrape visible counts. Or track price history changes as a proxy for sales activity (we already have `itchio_price_history`).

---

## Part 2 — Composite Analytics Future Ideas

Cross-stream analytics ideas evaluated during the composite layer build. Each was deferred either because of temporal limitations (need more historical data), infrastructure gaps (need materialization jobs), or because the idea belongs to a later phase.

### 7. Seasonality View
- **Source:** Perplexity (#5 in their list)
- **Idea:** Detect recurring seasonal patterns per concept (October spikes for horror themes, convention season surges, holiday gift-buying cycles) using day-of-year profiles across 2-3 years of data.
- **Why set aside:** Needs 2-3 years of historical data to detect patterns reliably. We have weeks-to-months of most streams, not years.
- **Future path:** Becomes buildable around Q4 2027 with continuous data collection. Implementation: BigQuery window functions over week-of-year aggregates from `composite_concept_index` snapshots. Output: a `seasonal_profile` score per concept plus expected-peak-week annotations.

### 8. Correlation Explorer / Concept Clusters
- **Source:** Perplexity (#6)
- **Idea:** Compute pairwise concept correlations across composite scores, then cluster concepts into behavioral themes. Store `cluster_id` and nearest neighbors so users can browse "concepts that behave like X."
- **Why set aside:** Requires ML clustering (k-means, graph community detection) — can't do in pure SQL views. BigQuery ML could approximate.
- **Future path:** Two options. (a) BigQuery ML `CREATE MODEL` with k-means on the 5-bucket feature vectors from `composite_concept_index`. (b) Offline Python pipeline with scikit-learn, write results back to a `gold_concept_clusters` table. Option A is cleaner for staying in-warehouse.

### 9. Concept Detail Time-Series Materialization
- **Source:** Perplexity (#4)
- **Idea:** Materialize per-concept, per-date rows with all bucketed scores + rolling averages + derivatives. Feeds drill-down charts in the concept detail drawer.
- **Why set aside:** Infrastructure/data-engineering concern, not an analytical composite. Needs scheduled materialization jobs, not views.
- **Future path:** Scheduled query that appends daily snapshots to a new `gold_concept_daily` table. Should be built **before** the frontend's concept detail drawer (build step 8) if we want real time-series sparklines rather than on-demand queries.

### 10. Source Reliability Panel
- **Source:** Perplexity (#7)
- **Idea:** Per-source diagnostics showing variance contribution, weight distribution, and anomaly flags across the composite. Answers "which streams are pulling the composite score around most?"
- **Why set aside:** Internal QA tool, not WotC-facing. Valuable for operational confidence but not a presentation slide.
- **Future path:** Build as an internal monitoring view after composites have been running for 2-4 weeks. Lives in the Admin section, not public site. Informs confidence-score calibration.

### 11. Narrative Summaries / Weekly Story
- **Source:** Perplexity (#8)
- **Idea:** Weekly AI-generated text summarizing top movers, emerging concepts, and key score changes, grounded in structured "story facts."
- **Why set aside:** Downstream of composites — needs them to exist first. Also a presentation/automation task, not a SQL view.
- **Future path:** Scheduled Cloud Function that queries composite views, formats structured JSON, sends to Gemini for narrative generation. Store in `weekly_story_facts` table. **This is the backend half of the frontend's Articles feature** — when we build the articles generator (frontend step 9), this is what it becomes.

### 12. 18-Month Leading Indicator Tracker
- **Source:** Gemini (#2 in their list)
- **Idea:** Time-shifted JOIN proving Itch.io jam themes predict Kickstarter success 12-18 months later. Uses `DATE_ADD` offset on join condition to show "jam theme X peaked in month N, Kickstarter category Y peaked in month N+12-18."
- **Why set aside:** Brilliant concept but needs 18+ months of parallel Itch.io + Kickstarter data to prove the lag statistically.
- **Future path:** Becomes provable around Q4 2027. In the meantime, the Creator Economy Dashboard captures the *current* leading signal without proving the historical lag. When data exists, this becomes the "proof" slide that validates the leading indicator thesis — very high stakeholder-impact view.

### 13. Creator Ripple Effect / ROI Tracker
- **Source:** Gemini (#4)
- **Idea:** Event-triggered analysis measuring ecosystem lift after a major YouTube video drop. Pre-14d vs post-14d scores on Google Trends + Fandom + Reddit for the concepts mentioned in the video.
- **Why set aside:** Needs daily-granularity time series across multiple streams, event-anchored. Can't do without the materialization layer.
- **Future path:** Requires `gold_concept_daily` (idea #9 above) to exist first. Then becomes a stored-procedure-style query parameterized by (video_drop_date, concept_ids).

### 14. Campaign Attrition Cohort Analysis
- **Source:** Gemini (#6)
- **Idea:** Funnel chart showing Fandom Wiki traffic decay across sequential chapters of adventure modules (Chapter 1 → Chapter 2 → Chapter 3 drop-off). Measures whether players actually finish published adventures.
- **Why set aside:** Needs chapter-level metadata in `concept_library` — mapping "which concept = which chapter of which module." Requires manual tagging for ~20 major adventure modules.
- **Future path:** Add `module_name` and `chapter_sequence` columns to `concept_library`. Then this becomes a straightforward Fandom percentile aggregation grouped by sequence position. **Very WotC-relevant** — answers "are people actually playing through Curse of Strahd or dropping it after Barovia?"

---

## Prioritization Hints

If we come back to this list and want to pick the highest-leverage items first:

1. **#4 (YouTube format classifier)** — easiest lift, schema change + prompt update. Ships in a day.
2. **#11 (Narrative summaries / weekly story)** — becomes the backend for the frontend Articles feature. Build these together.
3. **#9 (Concept detail time-series materialization)** — unblocks #13 (creator ripple) and powers the frontend concept drawer's sparklines properly.
4. **#14 (Campaign attrition)** — highest WotC stakeholder value relative to effort. Mostly a tagging task, not a pipeline change.
5. **#8 (Correlation clusters)** — unlocks a whole new browsing mode ("concepts that behave like X"). Moderate effort, high UX payoff.
6. **#7 (Seasonality)** and **#12 (18-month leading indicator)** — wait for time. Neither is buildable until late 2027.

Everything else is useful but lower priority.

---

**This list lives in the repo so other AI assistants and future sessions have the full context alongside the code. When an idea ships, remove or mark it here.**
