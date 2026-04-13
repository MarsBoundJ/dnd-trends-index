# Single-Stream Analytics Plan
## dnd-trends-index — Per-Source Intelligence Layer

Each data stream measures a different dimension of engagement. A Google search is curiosity. A Reddit comment is opinion. A Roll20 game is a multi-hour commitment. A Kickstarter pledge is a financial vote. The analytics must reflect this.

**Architecture**: Each stream gets a dedicated `gold_data.analytics_{stream}` view that produces a standardized output any downstream composite can consume.

---

## 1. Google Trends — The Curiosity Radar

**What it measures**: Top-of-funnel search interest. Reactive to media events, nostalgia cycles, and product launches.

**Raw source**: `dnd_trends_categorized.trend_data_pilot` → `date`, `term_id`, `search_term`, `interest`
**Existing silver**: `silver_data.view_google_mapping` (joins to concept_library)
**Existing gold**: `gold_data.deep_dive_metrics`, `study_edition_migration`, `study_dm_shortage`

### Analytics to Build

| Metric | Logic | WotC Use Case |
|--------|-------|---------------|
| **Billboard Top 25** | Rank concepts by 7-day avg `interest` score | Weekly pulse report: "what's hot right now" |
| **Momentum (30d)** | `(avg_last_14d - avg_prev_14d) / NULLIF(avg_prev_14d, 0)` | Detect rising/falling concepts before they hit leaderboards |
| **Lifecycle State** | Classify via momentum + volatility: `emerging` (momentum > 0.3, low baseline), `surging` (momentum > 0.3, high baseline), `stable` (low std_dev/median ratio), `fading` (momentum < -0.2), `seasonal` (recurring annual spikes) | Product Dev: which dormant settings (Dark Sun, Spelljammer) are organically ripening for a reboot |
| **Volatility Band** | `STDDEV(interest) / NULLIF(AVG(interest), 0)` over 90-day window | Separate "always discussed" from "spiked once and died" |
| **Breakout Detection** | Interest jumps from <10 to >50 within 7 days | Marketing: time ad spend to organic curiosity spikes (movie releases, Stranger Things moments) |
| **Seed Yield** | Count of related queries discovered per original seed term | Optimize which seeds to keep vs retire in the trend harvester |
| **Percentile vs Own History** | Current 7d avg vs 12-month percentile rank of itself | Avoid cross-concept comparison bias — measure each concept against its own baseline |

**View output columns**: `concept_name`, `category`, `current_interest`, `momentum_14d`, `lifecycle_state`, `volatility_cv`, `percentile_vs_self`, `is_breakout`, `rank_in_category`

---

## 2. Reddit — The Community Heat Engine

**What it measures**: Deep engagement, mechanic debates, sentiment, and virality. The community's unfiltered opinion.

**Raw sources**: `reddit_daily_metrics` → `mention_count`, `weighted_score`; `reddit_viral_events` → `upvotes`, `sentiment`, `topic`
**Existing gold**: `gold_data.view_social_leaderboards` (7-day history array, heat_score)

### Analytics to Build

| Metric | Logic | WotC Use Case |
|--------|-------|---------------|
| **Volume (normalized)** | `mention_count / subreddit_avg_daily_posts` — normalize per sub size | Compare "big fish in small pond" vs "drop in the ocean" |
| **Community Heat Profile** | Classify: `loud_niche` (high volume in 1-2 subs), `broad_casual` (moderate across 5+ subs), `controversial` (high volume + mixed sentiment), `quiet_but_present` (steady low mentions) | Marketing: know whether buzz is deep fandom or mainstream chatter |
| **Virality Score** | From `reddit_viral_events`: `COUNT(events where upvotes > 5000)` per concept per 30 days | Track which concepts break containment and go mainstream |
| **Sentiment Polarity** | `AVG(weighted_score)` per concept — positive = excitement, negative = frustration | Game Design: high volume + negative sentiment = broken mechanic needing errata |
| **Engagement Velocity** | Weighted score growth rate over 7-day windows | Early warning: sentiment shifting before volume changes |
| **Subreddit Spread** | `COUNT(DISTINCT subreddit)` per concept | Distinguish "r/BaldursGate3 obsession" from "all of D&D Reddit is talking about it" |
| **Co-mention Clusters** | Concepts that appear together in same subreddit+date windows | Discover natural concept affinities the taxonomy doesn't capture |

**View output columns**: `concept_name`, `category`, `mention_count_30d`, `normalized_volume`, `heat_profile`, `sentiment_avg`, `viral_events_30d`, `subreddit_spread`, `momentum_7d`

---

## 3. YouTube — The Creator Investment Tracker

**What it measures**: What content creators are betting their time on, and what audiences are watching.

**Raw sources**: `youtube_videos` → `velocity_24h`, `channel_name`, `is_short`, `matched_keywords`; `yt_video_intelligence` → `concept_name`, `verdict`, `sentiment_label`
**Existing gold**: `gold_data.view_youtube_consensus` (creator_count, consensus_score)

### Analytics to Build

| Metric | Logic | WotC Use Case |
|--------|-------|---------------|
| **Creator Diversity** | `COUNT(DISTINCT channel_name)` per concept | Distinguish "one big channel" from "widespread creator adoption" — the latter is the real signal |
| **View Velocity (7d rolling)** | `SUM(velocity_24h)` for videos mentioning concept, published in last 7 days | Current audience appetite, not historical noise |
| **Upload Cadence** | Videos per week mentioning concept (from `matched_keywords`) | Creator investment trend: are MORE creators covering this, or fewer? |
| **Consensus Score** | From existing `view_youtube_consensus` — multiple creators independently covering same topic | Product Dev: organic "what's hot" independent of any single influencer |
| **Format Mix** | Ratio of Shorts (`is_short=TRUE`) vs long-form per concept | Shorts = viral/casual reach. Long-form = deep engagement. Both = full funnel |
| **Sentiment Divergence** | Concepts where `sentiment_label` is mixed across creators | Early warning: community split on a topic (e.g., controversial rule change) |
| **New Channel Discovery Rate** | Channels first appearing in registry per week scoring 75+ | Ecosystem health: is the D&D creator economy growing or contracting? |

**View output columns**: `concept_name`, `category`, `creator_count`, `view_velocity_7d`, `upload_cadence_weekly`, `consensus_score`, `shorts_ratio`, `sentiment_mix`, `is_trending_up`

---

## 4. Fandom Wikis — The DM Prep Radar

**What it measures**: Deep lore engagement and active campaign preparation. People don't browse D&D wikis casually — they read them because they're prepping a session.

**Raw source**: `fandom_daily_metrics` → `wiki_slug`, `article_title`, `hype_score`, `view_count`, `edit_count`
**Existing silver**: `silver_data.norm_fandom`, `silver_data.view_fandom_mapping`
**Existing gold**: `gold_data.view_fandom_leaderboards`

### Analytics to Build

| Metric | Logic | WotC Use Case |
|--------|-------|---------------|
| **Edit Velocity** | Week-over-week change in `edit_count` per article | Which lore is being actively curated RIGHT NOW |
| **Hype Score Ranking** | Existing `hype_score` (0-1.0) ranked within category | Quick-glance leaderboard of fan attention |
| **Cross-Wiki Hotspots** | Same concept trending on 2+ wiki_slugs simultaneously | Strongest signal: when both forgottenrealms AND baldursgate wikis light up for the same concept |
| **Campaign Attrition Signal** | For adventure-category concepts: compare early-chapter wiki views vs late-chapter views | Product Dev: if Curse of Strahd Chapter 1-2 traffic is 10x Chapter 8, campaigns are fizzling — designers can study why |
| **Edit Breadth** | Track if edits come from hype_score spikes (single event) or steady accumulation | "Evergreen anchor" (steady edits) vs "flash-in-the-pan" (one spike) |
| **View-to-Edit Ratio** | `view_count / NULLIF(edit_count, 0)` | High views + zero edits = "settled lore." High edits + low views = "active revision, niche audience" |
| **Lore Depth Classification** | Based on sustained hype_score + edit history: `evergreen_anchor`, `active_revision`, `forgotten_stub`, `flash_spike` | Identify which concepts have deep community investment vs surface-level awareness |

**View output columns**: `concept_name`, `category`, `wiki_slug`, `hype_score`, `edit_velocity_7d`, `cross_wiki_count`, `lore_depth_class`, `view_to_edit_ratio`, `rank_in_category`

---

## 5. Wikipedia — The Mainstream Awareness Gauge

**What it measures**: Penetration beyond the D&D bubble into general-audience awareness. The "legitimacy" signal.

**Raw source**: `wikipedia_daily_views` → `date`, `article_title`, `views`
**Existing silver**: `silver_data.norm_wikipedia`
**Existing gold**: `gold_data.view_wikipedia_leaderboards`

### Analytics to Build

| Metric | Logic | WotC Use Case |
|--------|-------|---------------|
| **View Momentum (7d/30d)** | Rolling average comparison: `avg_7d / NULLIF(avg_30d, 0)` — ratio > 1.2 = rising | Detect concepts breaking out of niche into mainstream |
| **Mainstream Crossover Score** | Absolute view volume (not percentile) — Wikipedia readers are general public, not D&D players | Marketing: measure ROI of mass-market efforts (movies, Lego sets, Converse collabs). Did they drive non-players to learn about Drizzt? |
| **Seasonal Pattern Detection** | Compare current month views to same-month-last-year (when we have 12mo+ data) | Predict product launch timing: October = horror (Strahd, Ravenloft), summer = convention buzz |
| **Release Spike Attribution** | View spike within ±7 days of known product release dates (from DDB catalog) | Measure whether new releases drive genuine curiosity |
| **Steady State vs Spike** | `STDDEV(views) / AVG(views)` — low CV = steady awareness, high CV = event-driven | Distinguish "always-searched" evergreen concepts from "only during events" |

**View output columns**: `concept_name`, `category`, `views_7d`, `views_30d`, `momentum_ratio`, `mainstream_score`, `volatility_cv`, `is_rising`, `rank_in_category`

---

## 6. BGG / RPGGeek — The Commitment Index

**What it measures**: Actual ownership and sustained quality ratings. BGG owners have physically purchased or committed to a product — this is deeper than a search or a comment.

**Raw sources**: `bgg_product_stats` → `owned_count`, `quality_score`; `rpggeek_product_stats` → `owned_count`, `average_rating`, `geek_rating`, `rank`
**Existing gold**: `gold_data.view_bgg_leaderboards`

### Analytics to Build

| Metric | Logic | WotC Use Case |
|--------|-------|---------------|
| **Commitment Index** | `owned_count * quality_score` — volume weighted by satisfaction | Single metric: how many people have it AND like it |
| **Buyer's Remorse Detection** | High `owned_count` + low `quality_score` (< 6.0) | Product Dev: marketing pushed it well but product underdelivered. Don't repeat these mistakes |
| **Cult Classic Detection** | Low `owned_count` + high `quality_score` (> 8.0) | Marketing: candidate for reprint/digital push — the underlying product is strong, just undersold |
| **Ownership Velocity** | Week-over-week Δ in `owned_count` | Which products are still being actively acquired (not just legacy ownership) |
| **BGG vs RPGGeek Gap** | Same concept's rank/ownership on both platforms | Boardgame crossover appeal: big on BGG but not RPGGeek = gateway product pulling in non-TTRPG players |
| **Rating Trend** | Rolling `quality_score` / `average_rating` change over 90 days | Detect products whose community perception is shifting (post-errata improvements, or post-hype disappointment) |

**View output columns**: `concept_name`, `category`, `owned_count`, `quality_score`, `commitment_index`, `ownership_velocity`, `product_archetype` (cult_classic / mainstream_hit / buyers_remorse / sleeper), `rank_bgg`, `rank_rpggeek`

---

## 7. Roll20 — The "Actually Played" Signal

**What it measures**: What's happening at the virtual table. Multi-hour time commitments, not opinions.

**Raw source**: `commercial_data.roll20_rankings` → `rank`, `title`, `publisher`, `category`
**Existing silver**: `silver_data.norm_roll20`

### Analytics to Build

| Metric | Logic | WotC Use Case |
|--------|-------|---------------|
| **Play Score** | Existing `norm_roll20` PERCENT_RANK (inverted rank) | Core "actually played" metric |
| **VTT Adoption Velocity** | Week-over-week rank change per title | Which modules are gaining/losing table time |
| **Coffee Table Book Detection** | Cross-reference: high Amazon sales rank + low Roll20 adoption = "bought but not played" | Product Dev: dictates whether to invest in VTT map packs, tokens, digital assets for that line |
| **Staying Power** | Weeks in Top 20 (count of appearances in rolling 90-day window) | Separate "launch spike" products from genuine evergreens |
| **Publisher Concentration** | Share of Top 20 held by WotC vs third-party publishers | Competitive intelligence: is WotC losing table-share to Kobold Press, Paizo, etc? |

**View output columns**: `concept_name`, `category`, `roll20_rank`, `play_score`, `rank_velocity_7d`, `weeks_in_top20`, `publisher`, `is_wotc`

---

## 8. Crowdfunding (Kickstarter + BackerKit) — The Wallet Vote

**What it measures**: What players will spend $50-$200 on that WotC isn't currently providing. Financial proof of unmet demand.

**Raw sources**: `kickstarter_projects` → `backers_count`, `pledged_usd`, `goal_usd`, `percent_funded`, `status`; `backerkit_projects` → `funding_usd`, `backers_count`, `days_remaining`
**Existing silver**: `silver_data.norm_crowdfunding`

### Analytics to Build

| Metric | Logic | WotC Use Case |
|--------|-------|---------------|
| **Funding Intensity** | `pledged_usd / NULLIF(goal_usd, 0)` — overfund ratio | The bigger the overfund, the stronger the unmet demand signal |
| **Funding Velocity** | `pledged_usd / GREATEST(1, days_since_launch)` — $/day | Separates "slow burn" from "day-one spike" from "dead on arrival" |
| **Missing Feature Heatmap** | Group campaigns by mapped concept/category — sum total pledged per category | Product Dev: if $5M flows to "Monster Harvesting Systems," WotC has hard proof that 5e is missing that mechanic |
| **Campaign Archetype** | Classify: `day_one_spike` (>50% funded in 48h), `slow_burn` (funded but took >20 days), `overfunded_hit` (>300%), `failed` (<100% at close) | Pattern recognition across the D&D creator economy |
| **Backer Profile** | `pledged_usd / NULLIF(backers_count, 0)` — avg pledge size | High avg pledge = premium/collector audience. Low avg = mass-market appeal |
| **Category Concentration** | % of total D&D crowdfunding $ going to each concept category | Which categories attract the most money — adventures? sourcebooks? accessories? VTT tools? |

**View output columns**: `project_name`, `category`, `concept_name`, `platform`, `pledged_usd`, `backers_count`, `overfund_ratio`, `velocity_per_day`, `campaign_archetype`, `avg_pledge`

---

## 9. Amazon — The Mass Market & Gifting Signal

**What it measures**: Mainstream retail demand. Casual buyers, holiday gifting, and evergreen physical sales. The Amazon buyer skews more casual than any other source — parents buying gifts, newcomers picking up their first PHB.

**Raw source**: `amazon_daily_stats` → `asin`, `rank`, `price_cents`, `date`
**Existing gold**: `gold_data.view_amazon_leaderboards`

### Analytics to Build

| Metric | Logic | WotC Use Case |
|--------|-------|---------------|
| **Sales Rank Percentile** | `PERCENT_RANK() OVER (ORDER BY rank DESC)` within D&D products | Normalize Amazon's notoriously volatile rank numbers |
| **Evergreen Decay Rate** | Days from release to falling out of Top 100 (compare across products) | Marketing: which books have legs vs which spike-and-fade |
| **Price Stability** | `STDDEV(price_cents) / AVG(price_cents)` over 30 days | Detect discounting, deals, or supply issues |
| **Holiday Gifting Signal** | Rank improvement in Nov-Dec vs annual average | Identify products that over-index as gifts (casual/mainstream indicator) |
| **Availability Signal** | Gaps in daily rank data = likely out-of-stock | Supply chain intelligence |

**View output columns**: `concept_name`, `category`, `asin`, `current_rank`, `rank_percentile`, `rank_momentum_7d`, `price_current`, `price_stability`, `days_in_top100`

---

## 10. Itch.io — The 18-Month Crystal Ball

**What it measures**: Bleeding-edge indie designer intent. What creators are building NOW predicts what the broader market will want in 12-18 months.

**Raw sources**: `itchio_products` → `title`, `tags`, `aesthetic_clusters`, `list_type`; `itchio_jams` → `jam_title`, `theme_keywords`, `submission_count`
**Existing views**: None yet

### Analytics to Build

| Metric | Logic | WotC Use Case |
|--------|-------|---------------|
| **Jam Theme Heatmap** | `theme_keywords` frequency across active jams, weighted by `submission_count` | Product Dev: if "Solo Journaling" and "Cozy Fantasy" flood jam submissions, WotC knows the wind direction |
| **Creator Diversity per Theme** | `COUNT(DISTINCT creator)` per tag/aesthetic_cluster | Distinguish "one prolific creator" from "widespread movement" |
| **Release Cadence** | New products per week by tag/cluster | Is a theme accelerating or plateauing? |
| **Aesthetic Cluster Trends** | From `aesthetic_clusters` table — track cluster growth over time | Leading indicator: indie aesthetic movements → mainstream adoption |
| **Price Signal** | `AVG(price)` and `is_pwyw` ratio per theme | Free/PWYW = experimental. Priced = creator confidence in market demand |

**View output columns**: `theme_or_tag`, `category`, `jam_count`, `product_count`, `creator_count`, `submission_total`, `release_cadence_weekly`, `trend_direction`, `avg_price`

---

## 11. DMs Guild / DriveThruRPG — The Creator Economy Gap Finder

**What it measures**: What the third-party creator ecosystem is supplying. Community creators fill gaps in official WotC content — their best-sellers are a roadmap of unmet demand.

**Raw sources**: `dtrpg_velocity` → `product_name`, `medal_level`, `category`, `rank`; `dtrpg_inventory` → `description`, `commercial_weight`
**Existing views**: None yet

### Analytics to Build

| Metric | Logic | WotC Use Case |
|--------|-------|---------------|
| **Catalog Density** | Count of products per concept category | Where are third-party creators flooding in? That's where official content is thin |
| **Medal Concentration** | `COUNT(medal_level IN ('Gold','Platinum'))` per category | Quality-filtered: not just volume, but what's actually selling well |
| **Top Seller Theme Analysis** | Top 10 products by rank, grouped by mapped concept | Product Dev: if 8/10 top sellers are "Tier 4 One-Shots," WotC knows they've neglected high-level play |
| **New Release Velocity** | Products added per week by category | Detect supply surges — creator economy reacting to demand signal |
| **Price Tier Distribution** | Histogram of prices per category | Are creators pricing at $5 (impulse) or $30 (premium supplement)? |

**View output columns**: `category`, `product_count`, `medal_gold_plus_count`, `avg_rank`, `new_releases_30d`, `avg_price`, `top_product_name`

---

## 12. Steam — The Video Game Engagement Pulse

**What it measures**: Active player populations for D&D video games (primarily BG3). Real-time engagement, not just ownership.

**Raw sources**: `steam_player_counts` → `app_name`, `concurrent_players`; `steam_reviews` → `total_positive`, `total_negative`, `review_score_pct`
**Existing gold**: Feeds `cross_pollination_digital`

### Analytics to Build

| Metric | Logic | WotC Use Case |
|--------|-------|---------------|
| **Player Population Trend** | 7-day rolling avg of `concurrent_players` + week-over-week change | Is BG3 growing, stable, or declining? |
| **Review Sentiment Score** | `total_positive / (total_positive + total_negative)` | Overall community satisfaction with D&D video game experiences |
| **Review Momentum** | Change in `review_score_pct` over 30 days | Detect review-bombing or post-patch sentiment shifts |
| **Peak vs Off-Peak Ratio** | If multiple daily snapshots: `MAX(concurrent) / AVG(concurrent)` | Engagement pattern: steady play vs weekend-warrior spikes |

**View output columns**: `app_name`, `concurrent_players_7d_avg`, `player_trend`, `review_score`, `review_momentum`, `rank`

---

## 13. Modding (mod.io + Nexus) — The BG3 Creativity Index

**What it measures**: Community creative investment in D&D video games. Modding = the deepest form of engagement: players building for other players.

**Raw sources**: `modio_mods` → `downloads_total`, `subscribers_total`, `tags`; `nexus_mods` → `downloads_total`, `endorsement_count`
**Existing gold**: Feeds `cross_pollination_digital` (aggregated)

### Analytics to Build

| Metric | Logic | WotC Use Case |
|--------|-------|---------------|
| **Download Velocity** | Week-over-week Δ in `downloads_total` per mod | Which mods are actively growing vs legacy installs |
| **Platform Comparison** | Same mod concept on mod.io vs Nexus — download ratio | Understand platform preferences in the BG3 community |
| **Tag Heatmap** | Frequency of `tags` across all mods | What are modders building? Class mods? Cosmetics? QoL? New races? |
| **Endorsement Rate** | `endorsement_count / NULLIF(downloads_total, 0)` (Nexus) | Quality signal: high downloads + high endorsement = community-approved |
| **Total Ecosystem Size** | `SUM(downloads_total)` across both platforms, trended over time | Is the BG3 modding scene growing or contracting? |

**View output columns**: `mod_name_or_aggregate`, `platform`, `downloads_total`, `download_velocity_7d`, `endorsement_rate`, `top_tags`

---

## 14. Twitch — The Live Entertainment Signal

**What it measures**: Real-time streaming viewership. What people choose to watch live = current entertainment value.

**Raw source**: `twitch_viewership` → `category_name`, `total_viewers`, `total_streams`, `top_tags`
**Existing gold**: Feeds `cross_pollination_digital`

### Analytics to Build

| Metric | Logic | WotC Use Case |
|--------|-------|---------------|
| **Viewership Trend** | Week-over-week change in `total_viewers` per category | Is D&D streaming growing or contracting? |
| **Streamer-to-Viewer Ratio** | `total_viewers / NULLIF(total_streams, 0)` | High ratio = few big streamers dominate. Low ratio = broad grassroots streaming |
| **Category Comparison** | BG3 vs D&D tabletop vs Critical Role categories side by side | Where is the live audience? Video game play, actual play shows, or something else? |
| **Tag Analysis** | Parse `top_tags` for trending themes | What are streamers tagging their D&D content with? |

**View output columns**: `category_name`, `total_viewers`, `total_streams`, `viewer_per_stream`, `viewership_trend_7d`, `top_tags`

---

## 15. AO3 — The Fandom Depth Gauge

**What it measures**: Fanfiction output. The most labor-intensive form of fan engagement — writing thousands of words about characters you love.

**Raw source**: `ao3_tag_counts` → `tag_name`, `tag_type`, `work_count`, `fetch_date`
**Existing gold**: Feeds `cross_pollination_digital`

### Analytics to Build

| Metric | Logic | WotC Use Case |
|--------|-------|---------------|
| **Work Count Velocity** | Week-over-week Δ in `work_count` per tag | Which characters are inspiring NEW fiction right now, not just legacy totals |
| **Character vs Relationship Ratio** | For each character: ratio of their solo tag to their relationship tags | Characters with high relationship-tag ratios = strong "shipping" culture = strong emotional attachment |
| **BG3 vs Classic D&D Split** | Compare BG3 character work counts to classic D&D character work counts | Quantify how much BG3 has shifted fandom creative output |
| **Fandom Tag Dominance** | `work_count` of "Baldur's Gate" vs "Dungeons & Dragons" fandom tags | Is AO3 D&D fandom primarily BG3-driven or broader? |

**View output columns**: `tag_name`, `tag_type`, `work_count`, `work_velocity_weekly`, `character_ship_ratio`, `fandom_split`, `rank_in_type`

---

## 16. D&D Beyond Catalog — The Official Supply Signal

**What it measures**: WotC's own digital catalog. What's available, featured, and priced.

**Raw source**: `dndbeyond_catalog` → `sku`, `title`, `price`, `bundle_price`, `format`, `is_featured`
**Existing gold**: `digital_streams_health` (featured count only)

### Analytics to Build

| Metric | Logic | WotC Use Case |
|--------|-------|---------------|
| **Catalog Size Trend** | Total SKUs over time | Is the digital catalog growing? |
| **Featured Rotation** | Which products rotate in/out of `is_featured` | Track WotC's own merchandising priorities |
| **Price Architecture** | Distribution of `price` and `bundle_price` across products | Understand pricing strategy and bundle economics |
| **Format Mix** | % of catalog that is `digital_only` vs `book_and_digital` vs `physical` | Digital transformation tracking |
| **Supply Gap Indicator** | Categories with high Google Trends demand but zero DDB catalog entries | Direct opportunity identification |

**View output columns**: `sku`, `title`, `price`, `format`, `is_featured`, `catalog_category`, `days_since_added`

---

## Implementation Priority

### Tier 1 — Build First (richest data, most analytical value)
1. **Google Trends** — deepest time series, most concepts covered
2. **Reddit** — richest engagement data, sentiment available
3. **YouTube** — creator consensus + intelligence data
4. **Fandom Wikis** — unique DM-prep signal, cross-wiki possible

### Tier 2 — Build Next (strong single-source stories)
5. **Wikipedia** — mainstream crossover gauge
6. **BGG/RPGGeek** — commitment index, buyer archetypes
7. **Roll20** — "actually played" signal (sparse but unique)
8. **Crowdfunding** — financial proof of unmet demand

### Tier 3 — Build Last (newer data, views still forming)
9. **Amazon** — mass market signal
10. **Itch.io** — leading indicator (needs view infrastructure)
11. **DMs Guild/DTRPG** — creator economy (needs view infrastructure)
12. **Steam/Mods/Twitch/AO3** — already feed cross-pollination views, add per-stream depth
13. **DDB Catalog** — supply-side reference

---

## Standardized Output Contract

Every `analytics_{stream}` view MUST include these columns for downstream composite consumption:

```sql
concept_name       STRING    -- Mapped to concept_library
category           STRING    -- From concept_library
signal_type        STRING    -- What this stream measures (curiosity/engagement/ownership/play/supply/financial)
primary_metric     FLOAT64   -- The stream's single best number for this concept
momentum           FLOAT64   -- Direction of change (-1 to +1 scale)
confidence         STRING    -- HIGH/MEDIUM/LOW based on data density
stream_name        STRING    -- 'google_trends', 'reddit', 'youtube', etc.
snapshot_date      DATE      -- When this was computed
```

This contract means any composite view can `UNION ALL` across streams without knowing their internals.
