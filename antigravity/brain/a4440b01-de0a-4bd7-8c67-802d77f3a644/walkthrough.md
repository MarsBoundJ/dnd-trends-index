# BackerKit Crowdfunding Engine Walkthrough

> [!IMPORTANT]
> **Phase 7 (RPGGeek Ownership)** is currently **BLOCKED** pending a BoardGameGeek API Key.
> **Phase 8 (BackerKit Support)** is **COMPLETE** with reduced scope on the analysis join.

## 1. BackerKit Scraper Implementation

We successfully implemented a scraper for **BackerKit Crowdfunding** to track high-value TTRPG campaigns.

### Key Features
- **Anti-Detection**: Uses `curl_cffi` to impersonate Chrome 120 and bypass Cloudflare protection.
- **Robust Selectors**: Targets `project-card-small-component` to reliable extract project data even when class names shift.
- **5e Filtering**: Analyzes project titles and blurbs to categorize items as "5e Compatible", "OSR", or generic "RPG".
- **Data Extracted**:
  - Title & Creator
  - Funding Amount (USD)
  - Backer Count
  - System Tag (e.g. 5e Compatible, OSR)

### Execution Output
The scraper successfully extracted trending projects from the "Role Playing Games" collection:
```text
Extracted 5 RPG projects.
- Toon the Cartoon Roleplaying Game Second Edition (RPG (Other)): $97,901
- Use Up All Your Sick Days (RPG (Other)): $8,535
...
✅ Success using curl_cffi.
```

## 2. Data Warehousing

We established the following BigQuery infrastructure:

### Schema: `commercial_data.backerkit_projects`
| Field | Type | Description |
|---|---|---|
| `project_id` | STRING | Unique slug |
| `title` | STRING | Project Title |
| `funding_usd` | FLOAT | Current funding |
| `backers_count` | INTEGER | Total backers |
| `system_tag` | STRING | 5e/OSR/etc |

### Analysis: `commercial_data.backerkit_hype_analysis`
A derived table calculating the **Hype Signal** (Funding per Backer) joined with the `concept_library` to identify high-value keywords.

## 3. Next Steps

1. **Unblock Phase 7**: Provide BGG API Key to enable RPGGeek scraping.
2. **Restore Dependency**: Locate or recreate `concept_library.dnd_keywords` to enable the full Hype Multiplier analysis.
3. **Automate**: Schedule `backerkit_scraper.py` to run weekly.

## 4. Google Shopping Market Index (Phase 11)

We implemented a "Product Availability Engine" to distinguish between homebrew concepts and commercial products.

### Key Features
- **Fetch Engine**: `shopping_sampler.py` queries the top daily trending keywords from BigQuery.
- **Fallbacks**: Supports `Schema.org/Offer`, `og:type=product`, and retailer whitelist (Roll20, Shopify) to detect availability even without explicit pricing.
- **Quota Safety**: Hard-capped at 95 queries/day to remain within the Google Custom Search Free Tier.

### Execution Output
```text
Found 5 trending keywords from BQ.
Searching: Vecna Eve of Ruin...
  -> Found 1 products
Searching: Burnock Mill...
  -> Found 1 products
Inserted 5 shopping records.
```

### Schema: `commercial_data.google_shopping_snapshots`
| Field | Type | Description |
|---|---|---|
| `keyword` | STRING | Trending Term |
| `retailer` | STRING | Seller (e.g. Roll20, Hollow Press) |
| `price` | FLOAT | Price (if available), else NULL |
| `snapshot_date` | DATE | Date of check |

## 5. YouTube Influencer Intelligence (Phase 12)

We established the comprehensive tracking infrastructure for D&D creators.

### Components
- **Registry**: `dnd-trends-index.social_data.youtube_channel_registry` seeded with **59 Channels** (Tiers 1-6).
- **Listener**: `youtube_listener.py` successfully detected **63 new videos** from the last 7 days.
- **Velocity Tracker**: `youtube_stats_updater.py` logic verified (fetched 300k+ views for Critical Role's latest episode).

### BigQuery Schema (`youtube_videos`)
| Field | Type | Description |
|---|---|---|
| `video_id` | STRING | PK |
| `channel_name` | STRING | e.g. "Dungeon Dudes" |
| `tier` | INT64 | 1 (Massive) to 6 (Emerging) |
| `view_count` | INT64 | Updated daily |
| `velocity_24h` | INT64 | Growth metric |
| `sentiment_pos_ratio` | FLOAT | 0.0 to 1.0 (Higher is positive) |
| `matched_keywords` | STRING[] | Detected terms (e.g. "Vecna") |

> [!NOTE]
> **Streaming Buffer**: Updates to `youtube_videos` (Velocity Tracker) and Enrichment layers (Sentiment/Keywords) may fail if run immediately after ingestion due to BigQuery streaming buffer protections. This resolves automatically after ~90 minutes.

### Analytics Layer
- **Sentiment**: `youtube_sentiment_engine.py` uses VADER to score top 20 comments. (Sample Score: 40% Positive).
- **Context**: `youtube_keyword_matcher.py` maps titles to the "Concept Library" (20k+ terms).

## 6. Roll20 VTT Market Engine (Phase 13)

We successfully reverse-engineered the **Roll20 Marketplace** to track the "Best Selling" digital assets.

### Discovery: The Hidden JSON API
Initial scraping attempts failed because Roll20 uses a "skeleton" HTML page that hydrates via JavaScript.
By analyzing the `marketplace-search.js` source code, we discovered a hidden **POST API** used by the frontend:

- **Endpoint**: `https://marketplace.roll20.net/browse/search`
- **Method**: `POST`
- **Payload**: `{"filters[category][]": "itemtype:Games", "sortby": "popular"}`
- **Headers**: Requires `X-Requested-With: XMLHttpRequest`

Switching to this API allowed us to bypass complex parsing and retrieve clean JSON data directly.

![Roll20 Search Results](C:/Users/Yorri/.gemini/antigravity/brain/a4440b01-de0a-4bd7-8c67-802d77f3a644/marketplace_search_results_1766935204741.png)

### Execution Output
```text
Fetching Page 1...
Fetching Page 2...
...
Scraped 100 items.
Successfully inserted 100 rows into commercial_data.roll20_rankings
```

### Schema: `roll20_rankings`
| Field | Type | Description |
|---|---|---|
| `snapshot_date` | DATE | Date of scrape |
| `rank` | INT64 | 1-100 Position |
| `title` | STRING | Asset Name |
| `publisher` | STRING | e.g. Wizards of the Coast |

## 7. Wikipedia Cultural Intelligence (Phase 14)

We established a "Mainstream Relevance" monitor by tracking daily page views for ~5,400 D&D-related Wikipedia articles.

### 7.1 Recursive Discovery
Instead of a manual seed list, we built a **Recursive Category Crawler** (`wiki_crawler.py`) that starts at `Category:Dungeons_&_Dragons` and digs 2 levels deep.
- **Depth 2**: Found **5,475 articles** (filtering out Lists, Templates, and generic terms).
- **Coverage**: Includes Novels, Modules, Video Games (Baldur's Gate), and Lore.

### 7.2 Daily Views Streamer
The `wikipedia_scraper.py` iterates through the registry and fetches 30-day view history from the **Wikimedia REST API**. 
- **Streaming**: Pushes data to BigQuery in batches of 50 to provide real-time feedback.
- **Metric**: Daily User Views (Desktop + Mobile).

### Schema: `wikipedia_daily_views`
| Field | Type | Description |
|---|---|---|
| `date` | DATE | View date |
| `article_title` | STRING | Wikipedia Title |
| `views` | INTEGER | Daily Views |
| `category` | STRING | Parent Tag (e.g. Forgotten_Realms_novels) |

## 8. Fandom Wiki Intelligence Engine (Phase 15)

To capture "Deep Lore" and "Hardcore DM" trends, we targeted the **Top 100 Trending Articles** from the major D&D wikis using the Fandom internal API (`/api/v1/Articles/Top`).

### 8.1 Active Monitors
- **Forgotten Realms Wiki**: Tracks lore, gods, and setting details.
- **D&D 5e Wiki**: Tracks mechanics, classes, and spells.
- **Critical Role Wiki**: Tracks hype cycles from the show.
- **Eberron Wiki**: Control group for specific setting interest.
- **Ravenloft, Dragonlance, Dark Sun, Spelljammer, Planescape, Greyhawk**: "Deep Dive" setting tracking.

### 8.2 Execution Results
The `fandom_scraper.py` successfully retrieved and cleaned **400 trending topics** (100 per wiki) and pushed them to BigQuery.

### Schema: `fandom_trending`
| Field | Type | Description |
|---|---|---|
| `snapshot_date` | DATE | Date of scrape |
| `wiki_name` | STRING | e.g. 'forgottenrealms' |
| `rank` | INTEGER | 1-100 (Velocity Signal) |
| `article_title` | STRING | Clickable Title |


## 10. The Arcane Dashboard (Phase 17)

We adopted a **Serverless + Vanilla JS Architecture** to visualize the Trend Scores safely and cost-effectively without complex build tools.

### 10.1 The Bouncer (API Bridge)
- **Component**: Google Cloud Function (`get_trend_data`).
- **Purpose**: Securely queries BigQuery `gold_data.trend_scores` and serves JSON to the frontend.
- **Security**: IAM configured for public invocation (Read-Only).
- **Endpoint**: `https://get-trend-data-appfh5mgjgiq-uc.a.run.app`

### 10.2 Frontend Architecture
- **Tech**: HTML5 + Vanilla JS + Chart.js (CDN).
- **Styling**: Custom "Arcane Analytics" Theme (Glassmorphism, Dark Mode).
- **Components**:
    - **Stat Block Cards**: Visualizing trends as RPG monsters (STR=Play, CHA=Hype).

## 11. The AI Journalist (Phase 18)

To avoid local resource constraints, we migrated the "Editorial Engine" to a **Serverless Architecture**.

### 11.1 The "Journalist" Cloud Function
- **Component**: `dnd-daily-journalist` (Gen 2 Cloud Function).
- **Endpoint**: `https://dnd-daily-journalist-kfh5mgjgiq-uc.a.run.app`
- **Logic**:
    1.  Queries 3 pre-calculated **BigQuery Views** (`view_trend_spikes`, `view_platform_gaps`, `view_sentiment_divergence`).
    2.  Prompts **Gemini 1.5 Pro** with a persona (e.g., "The Tavern Keeper").
    3.  Saves the generated article to `gold_data.daily_articles`.

### 11.2 Narrative Signals
We isolated three specific story types:
1.  **Meteoric Riser**: >50% growth in 7 days.
2.  **Ghost Hype**: High Wikia Visits but Low Roll20 Usage.
3.  **Silent Killer**: High Roll20 Usage but Low Social Buzz.

### 11.3 Automation
- **Scheduler**: `trigger-daily-journalist` (Daily @ 4:30 AM CST).
- **Status**: ✅ **ACTIVE & SUCCESSFUL**.
- **The Newsroom**: Now generates **3 articles per day** (Tavern Keeper, Sage, Goblin).
- **Architecture**: Uses DML SQL inserts for synchronized multi-persona publication.
- **Slow Lane Workflow**: Triggers Weekly Commercial Scrapers (Kickstarter, Roll20).
