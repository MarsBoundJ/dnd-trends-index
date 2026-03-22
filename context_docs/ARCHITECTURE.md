# Arcane Analytics — Architecture Reference
**GCP Project:** `dnd-trends-index`
**Last Updated:** 2026-03-22

---

## GCP Infrastructure Overview

### Cloud Workflows
| Workflow | Purpose | Schedule |
|---|---|---|
| `dnd-fast-lane` | Daily scrapers: Reddit, Fandom, YouTube, Wiki, Google Trends, Itch.io, BGG, RPGGeek | Daily at Mercury hour (Chaldean) |
| `dnd-slow-lane` | Weekly commercial scrapers: Kickstarter, BackerKit, Roll20, RPGGeek | Weekly |

### Cloud Run Services (always-on)
| Service URL | Purpose |
|---|---|
| `reddit-harvester-187467566422.us-central1.run.app` | Reddit scraper |
| `fandom-scraper-187467566422.us-central1.run.app` | Fandom wiki scraper |
| `youtube-listener-187467566422.us-central1.run.app` | YouTube data collector |
| `wikipedia-scraper-187467566422.us-central1.run.app` | Wikipedia scraper |
| `bgg-harvester-187467566422.us-central1.run.app` | BGG + RPGGeek (rpg:true/false body param) |
| `bouncer-api-187467566422.us-central1.run.app` | Internal API gateway, view refresher, health endpoint |
| `caldean-calculator-187467566422.us-central1.run.app` | Chaldean hour calculator — creates daily scrape jobs |

### Cloud Run Jobs (triggered, not always-on)
| Job | Purpose |
|---|---|
| `google-trends-job` | Google Trends pytrends scraper with Webshare residential proxy |
| `itchio-rss-harvester` | Itch.io RSS feed ingestion |
| `wiki-discovery-bot` | Wikipedia discovery, runs daily at 02:00 UTC |

### Cloud Functions
| Function | Purpose |
|---|---|
| `monster-classifier` | Vertex AI/Gemini classifier for concept_library Monster category cleanup |
| `bouncer-api` | (Also deployed as Cloud Run — handles /system/health, /refresh_views) |
| `related-queries-discovery` | **NEW — scaffolded, not yet deployed** — pytrends related_queries() + related_topics() |

### Cloud Scheduler Jobs
| Job | Schedule | Target | Auth |
|---|---|---|---|
| `caldean-daily-calculator` | `0 0 * * *` UTC | caldean-calculator Cloud Run | OIDC |
| `caldean-daily-updater` | `0 0 * * *` UTC | caldean-calculator Cloud Run | OIDC |
| `caldean-master-trigger` | Dynamic (set by calculator) | `dnd-fast-lane` workflow | **OAuth** (cloud-platform scope) |
| `scrape-YYYY-MM-DD` | Dynamic Mercury hour | `dnd-fast-lane` workflow | **OAuth** (cloud-platform scope) |
| `schedule-wiki-discovery` | `0 2 * * *` UTC | wiki-discovery-bot Cloud Run job | OAuth |
| `trigger-daily-journalist` | `30 4 * * *` Chicago | dnd-daily-journalist Cloud Run | None (⚠️ returning code 13) |
| `shabbat-catchup` | Dynamic (Havdalah time) | `dnd-fast-lane` workflow | OAuth |

**Critical auth note:** Jobs targeting the Workflow Executions API (`workflowexecutions.googleapis.com`) MUST use OAuth with `cloud-platform` scope. OIDC will return 401.

---

## BigQuery Dataset Structure

### `dnd_trends_categorized` (core dataset)
| Table | Description |
|---|---|
| `concept_library` | Master keyword database — all tracked D&D terms with categories, scores |

### `dnd_trends_raw` (new dataset — created by related queries pipeline)
| Table | Description |
|---|---|
| `related_queries` | Raw pytrends related_queries() + related_topics() output, partitioned by fetched_at |
| `emerging_terms` | Rising terms not in concept_library, review_status = PENDING/ACCEPTED/REJECTED |

### Other datasets (existing)
- Gold views refreshed by `bouncer-api` `/refresh_views` endpoint after each workflow run

---

## Service Accounts
| Account | Used by |
|---|---|
| `antigravity-turbo-agent@dnd-trends-index.iam.gserviceaccount.com` | Antigravity DevContainer, most scheduler jobs |
| `187467566422-compute@developer.gserviceaccount.com` | Default compute SA, some workflow executions |
| `dnd-trends-index@appspot.gserviceaccount.com` | App Engine default SA (test-ping job — not meaningful) |

---

## Proxy Configuration
- **Provider:** Webshare residential proxies
- **Host:** `p.webshare.io` port `80`
- **Credentials:** Stored in Secret Manager as `webshare-proxy-user` and `webshare-proxy-pass`
- **Used by:** `google-trends-job` Cloud Run job, `related-queries-discovery` Cloud Function
- **Pattern:** One keyword per pytrends call, 8–15s jitter delay between requests

---

## Key Source Files
| File | Purpose |
|---|---|
| `utils/schedule_manager.py` | Chaldean Cycle — Mercury hour calculation, Shabbat skip, Havdalah catchup, job creation |
| `bouncer/main.py` | Bouncer API — health endpoint, view refresh |
| `cloud_functions/monster_classifier/main.py` | Reference architecture for Gemini-based classifiers |
| `cloud_functions/related_queries_discovery/main.py` | **NEW** — related queries Cloud Function (scaffolded 2026-03-22) |
| `dashboard/health.html` | Health monitoring dashboard (currently shows mocked data) |

---

## The Related Queries Discovery Pipeline (New — In Progress)

### What it does
1. Calls pytrends `related_queries()` and `related_topics()` for seed keywords
2. Writes all results to `dnd_trends_raw.related_queries`
3. Cross-references Rising terms against `concept_library`
4. Writes novel terms to `dnd_trends_raw.emerging_terms` with `review_status = PENDING`

### Default seed keywords
`"dungeons and dragons"`, `"dnd 5e"`, `"dnd 2024"`, `"pathfinder 2e"`, `"ttrpg"`, `"one dnd"`, `"dnd beyond"`

### Key implementation details
- `is_breakout` boolean captures pytrends `"Breakout"` string (>200% growth)
- Deduplication within a run — a term appearing across multiple seeds written to `emerging_terms` once
- HTTP trigger, accepts optional `{"seeds": [...], "dry_run": true}` body
- Not yet added to `dnd-fast-lane` workflow — needs scheduling decision

### Files delivered
```
cloud_functions/related_queries_discovery/
├── main.py
├── requirements.txt
├── deploy.sh
└── sql/schema.sql
```

---

## Planned: Variant Resolution Pipeline (Not Yet Built)

### Purpose
Determine whether a Rising term from related queries is:
- A **search variant** of an existing `concept_library` keyword (different phrasing, same concept)
- A **new concept** deserving its own row

### Planned stages
1. **Fuzzy match pre-filter** — token overlap / edit distance against `concept_library`
2. **Gemini classification** — receives candidate term + seed keyword + existing concept + known variants array
3. **Claude staging review** — checks Gemini decisions, flags disagreements, produces report
4. **Phil final approval** — greenlights promotions to live tables

### Schema needed
New table: `concept_variations`
```
concept_id        STRING    FK → concept_library
variant_string    STRING    The actual search string tested
is_best_variant   BOOL      Currently used for leaderboard scoring
source            STRING    "search_variant_generator" | "related_queries_discovery" | "manual"
date_added        TIMESTAMP
status            STRING    "active" | "deprecated"
```
