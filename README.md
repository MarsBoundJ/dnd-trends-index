# D&D Trends Index

> **Arcane Analytics** — market intelligence infrastructure for the tabletop RPG industry

[![GCP](https://img.shields.io/badge/GCP-Cloud%20Functions%20%7C%20BigQuery%20%7C%20Vertex%20AI-4285F4?logo=google-cloud&logoColor=white)](https://cloud.google.com)
[![Python](https://img.shields.io/badge/Python-3.11-3776AB?logo=python&logoColor=white)](https://python.org)
[![BigQuery](https://img.shields.io/badge/BigQuery-Medallion%20Architecture-669DF6?logo=googlebigquery&logoColor=white)](https://cloud.google.com/bigquery)
[![Status](https://img.shields.io/badge/Status-Live%20Production-brightgreen)](https://github.com/MarsBoundJ/dnd-trends-index)

---

## What This Is

**D&D Trends Index** is a live data pipeline that tracks what tabletop RPG players are searching for, talking about, funding, and buying — aggregating signals from 8 distinct data sources into a unified, queryable intelligence layer.

The system maintains a canonical library of ~11,000 D&D concepts (monsters, spells, classes, sourcebooks, settings) and continuously monitors their cultural momentum across search, social, community, and commercial channels. A composite **Trend Score** normalizes every signal to a percentile rank, making it possible to compare a search trend directly against Reddit buzz, Fandom wiki edits, and crowdfunding velocity.

The intended consumer of this data is anyone who needs to know what the D&D community cares about *right now* — and what they will care about in 12–18 months.

---

## The Problem It Solves

The tabletop RPG market lacks a Bloomberg terminal. Publishers, designers, and content creators make decisions about what to develop, print, and market based on intuition and lagging sales data. By the time a trend shows up on Amazon bestseller lists, it's already crested.

This project was built to surface signals earlier and triangulate them more reliably:

- **Search trends** tell you what players are actively curious about today
- **Reddit & Fandom activity** shows community excitement and discourse momentum
- **Itch.io indie releases** function as a 12–18 month leading indicator — indie creators respond to player demand faster than publishers, so a surge in indie games around a theme reliably precedes mainstream commercial interest
- **Kickstarter/BackerKit crowdfunding** captures funded interest — players voting with money
- **BoardGameGeek ownership & ratings** tracks the long tail of community adoption

No single source is sufficient. The system's value is in the triangulation.

---

## Data Sources

| Source | Signal Type | Collection Method | Cadence |
|---|---|---|---|
| **Google Trends** | Search interest (relative 0–100 scale) | Playwright browser automation + pytrends | Daily |
| **Reddit** | Community discussion volume & virality | PRAW API, ~26 D&D subreddits, keyword trie matching | Daily |
| **Fandom Wikis** | Article edit frequency & community engagement | MediaWiki API, 13 wikis (D&D5e, Forgotten Realms, Critical Role…) | Daily |
| **YouTube** | Upload velocity & content focus | YouTube Data API, curated channel registry | Daily |
| **Itch.io** | Indie game releases & jam participation | RSS feed + HTML scraping (React JSON extraction) | Weekly |
| **BoardGameGeek / RPGGeek** | Product ownership & community ratings | BGG XML API v2 | Mon/Wed/Fri |
| **Kickstarter / BackerKit** | Crowdfunding campaigns & backer counts | Bookmarklet harvester + Cloud Function ingestion | On-demand |
| **Wikipedia** | Article edit activity | MediaWiki API | Daily |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         DATA SOURCES                            │
│  Google   Reddit   Fandom   YouTube   Itch.io   BGG   KS/BK    │
└─────────────┬───────────────────────────────────────────────────┘
              │
              ▼
┌─────────────────────────────────────────────────────────────────┐
│                   COLLECTION LAYER (GCP)                        │
│  16 Cloud Functions (Python 3.11)  ·  Cloud Scheduler           │
│  Cloud Workflows (parallel orchestration)                       │
│  Rate-aware scheduling · Residential proxy routing              │
│  Circuit breakers · Bandwidth caps                              │
└─────────────┬───────────────────────────────────────────────────┘
              │
              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    STORAGE — BigQuery                           │
│  RAW LAYER       → Immutable snapshots (append-only)           │
│  SILVER LAYER    → PERCENT_RANK normalized views (0.0–1.0)     │
│  GOLD LAYER      → Composite Trend Score + AI-generated content │
└─────────────┬───────────────────────────────────────────────────┘
              │
              ▼
┌─────────────────────────────────────────────────────────────────┐
│                      API LAYER (Bouncer)                        │
│  Cloud Function REST gateway  ·  CORS-enabled                   │
│  /leaderboards  /emerging  /catalog  /health  /analyst/chat     │
└─────────────┬───────────────────────────────────────────────────┘
              │
              ▼
┌─────────────────────────────────────────────────────────────────┐
│                     FRONTEND DASHBOARD                          │
│  "Arcane Analytics" — glassmorphic UI (HTML/CSS/JS)             │
│  Search · Social · Sales tabs  ·  Admin review queue           │
└─────────────────────────────────────────────────────────────────┘
```

### Data Model

The system follows the **Medallion architecture** pattern:

- **Raw layer** (`dnd_trends_raw`): Immutable landing tables — one row per data point per source per day. Never mutated after insert.
- **Silver layer** (`silver_data`): SQL views that apply `PERCENT_RANK()` window functions, normalizing each source's scores onto a 0.0–1.0 scale so they can be combined.
- **Gold layer** (`gold_data`): Composite Trend Score (weighted average of normalized hype, play, and commercial signals) plus AI-generated daily articles.

The concept library (`dnd_trends_categorized.concept_library`) is the canonical entity register — ~11,000 D&D concepts with category, source book, and taxonomy tags. All data is ultimately joined back to this library, so every leaderboard answer maps cleanly to a named entity.

---

## Engineering Challenges

This section describes the real problems encountered building a production data pipeline against APIs designed for human users.

### 1. Google Trends at Scale

**Problem**: Google Trends has no official API. The unofficial `pytrends` library is rate-limited aggressively, and querying 18,000+ search terms naively would take months and trigger IP blocks.

**Solution**: A Playwright-based browser scraper routes requests through a residential proxy to avoid datacenter IP blocks. A watermark system tracks which terms have been collected recently, batching work across daily runs rather than attempting a full scan each time. A `related_queries` discovery function runs separately to continuously identify *new* terms organically — the seed set expands as the community invents new terminology.

**Bandwidth safeguards**: After a stuck retry loop burned 3.2GB of proxy bandwidth in a single session (37 minutes × ~6 retries × 1–2MB per Playwright page load), the scraper gained a circuit breaker (3 consecutive failures = abort), a hard runtime cap (45 minutes), and a proxy health check that fails fast before processing any terms.

### 2. Rate Limiting Across All Sources

**Problem**: Every data source has different rate limit behavior. BGG requires a 2-second sleep between API calls. Google Trends triggers 429s on sustained querying. Reddit's PRAW has per-minute limits.

**Solution**: Per-source sleep budgets baked into each harvester, with polite backoff. BGG recently started blocking GCP datacenter IP ranges entirely (SSL EOF at the handshake layer), requiring routing through residential proxy — a fix that costs ~185KB of proxy bandwidth per run (trivial) while fully restoring data collection.

### 3. Self-Expanding Keyword Discovery

**Problem**: The D&D meta constantly generates new terms — new sourcebooks, homebrew supplements, streamer coinages, rules system editions. A static keyword list becomes stale within weeks.

**Solution**: A `related_queries_discovery` function runs daily, calling pytrends' `related_queries()` and `related_topics()` for seed keywords. Novel terms not already in the concept library are written to an `emerging_terms` table and surfaced in the admin dashboard for human review. Approved terms are inserted into the concept library, where a downstream function generates search variants and makes them active for data collection. The seed set itself grows as curated terms are promoted.

### 4. Deduplication in Serverless Environments

**Problem**: Cloud Scheduler can trigger functions multiple times (retries on timeout, accidental double-fires). A single duplicate run inserted 64 rows on one date where 32 was correct.

**Solution**: Each harvester opens with a date-level dedup guard — a cheap `COUNT(*)` query against the target table for today's date. If rows already exist, the function returns `{"status": "skipped"}` immediately. This is a simple, audit-friendly pattern that doesn't require distributed locks.

### 5. Itch.io Data Extraction

**Problem**: Itch.io has no public API. The RSS feeds are limited (only one genre-tagged feed is public). Jam listings are rendered by a React frontend — no static HTML to parse.

**Solution**: The RSS feed is fetched directly (no proxy needed — it's public). Jam data is extracted by locating the `R.Jam.FilteredJamCalendar({...})` React bootstrap JSON embedded in the page source and parsing it with a regex, avoiding the need for a headless browser. This produces structured jam metadata (title, start/end dates, submission counts) with a single HTTP request.

---

## Key Findings

These are patterns that emerged from the data and informed how the system was designed:

**Itch.io as a leading indicator.** Indie game jam participation around a theme consistently precedes mainstream commercial interest by 12–18 months. When indie creators start building games around a concept — a monster, a setting, a rules mechanic — it signals player appetite before publishers have responded. This is now a first-class data stream.

**Search ≠ Play ≠ Buy.** A concept can have high search volume (curiosity), low Reddit activity (not discussed in community), and strong BGG ownership (purchased but not hyped). These three signals identify different player relationships — browsing, engaged, and committed. Separating them gives a more nuanced picture than any single metric.

**Rising queries outperform top queries.** pytrends surfaces both "top" and "rising" related queries. Rising queries — terms growing rapidly from a low base — are the early-warning signal. Top queries are already saturated. The system weights rising queries more heavily in the discovery pipeline.

**Blue Ocean opportunities.** Overlaying search demand against commercial supply (BackerKit/BGG) identifies categories where players are actively searching but publishers haven't shipped product. This is the clearest signal for new product development decisions.

---

## Tech Stack

| Layer | Technologies |
|---|---|
| **Languages** | Python 3.11, JavaScript (ES6), SQL, YAML |
| **Cloud** | GCP: Cloud Functions (Gen 2), Cloud Run, Cloud Scheduler, Cloud Workflows, Cloud Storage |
| **Database** | BigQuery (medallion architecture: raw → silver → gold) |
| **AI/ML** | Vertex AI — Gemini 1.5 Flash (concept classification, article generation, relevance scoring) |
| **Browser automation** | Playwright (headless Firefox, proxy-routed) |
| **APIs** | Reddit PRAW, YouTube Data API v3, pytrends, BGG/RPGGeek XML API v2, Fandom MediaWiki API |
| **Data ingestion** | `google-cloud-bigquery` Python client, `insert_rows_json` for streaming inserts |
| **Frontend** | Vanilla HTML5/CSS/JS, ApexCharts, glassmorphic UI design |
| **Proxy** | Webshare residential rotating proxy (US) |

---

## Project Status

**Live in production.** Scrapers run daily on Cloud Scheduler. Current data streams:

| Stream | Status | Notes |
|---|---|---|
| Google Trends | ✅ Active | ~300 terms/day, circuit breaker active |
| Reddit | ✅ Active | 26 subreddits, keyword trie |
| Fandom Wikis | ✅ Active | 13 wikis |
| YouTube | ✅ Active | Curated channel registry |
| Itch.io | ✅ Active | RSS + jam scraper |
| BGG / RPGGeek | ✅ Active | Mon/Wed/Fri, proxy-routed |
| BackerKit | ✅ Active | Bookmarklet + ingestion endpoint |
| Wikipedia | ✅ Active | Daily |

---

## Repository Structure

```
cloud_functions/      # 16 Cloud Functions — one per data source or processing task
harvesters/           # Local scripts and test harnesses
bouncer/              # Central REST API gateway (Cloud Function)
frontend/             # Arcane Analytics dashboard (HTML/CSS/JS)
workflows/            # Cloud Workflows YAML orchestration
underlying_defs/      # SQL view definitions
Source_of_truth/      # Architecture specs, schema documentation, data strategy
scripts/              # Browser bookmarklets (Kickstarter, Amazon harvesting)
utils/                # Operational utilities
```

---

## Contact

Built and maintained by [@MarsBoundJ](https://github.com/MarsBoundJ).

For collaboration inquiries or data access requests, open an issue.
