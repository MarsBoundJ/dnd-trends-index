# Exported Scraping Work

This folder contains the core scripts and data verification for the "Fandom and Wikipedia" fact-finding mission.

## Key Scripts

### 1. Wikipedia Discovery (`wiki_crawler.py`)
*   **Purpose**: Crawls Wikipedia recursively starting from `Category:Dungeons_&_Dragons`.
*   **Key Logic**:
    *   Filters out lists, templates, and generic terms.
    *   Uses a "Category Tree" approach (Depth 3).
*   **Output**: Pushes unique found articles to BigQuery (`social_data.wikipedia_article_registry`).

### 2. Wikipedia Views Scraper (`wikipedia_scraper.py`)
*   **Purpose**: Fetches daily view counts for the articles found by the crawler.
*   **Logic**:
    *   Reads from `social_data.wikipedia_article_registry`.
    *   Hits `wikimedia.org/api/rest_v1/metrics/pageviews`.
    *   Writes to `social_data.wikipedia_daily_views`.

### 3. Fandom Scraper (`fandom_scraper.py`)
*   **Purpose**: Scrapes "Top Articles" (Trending) from specific Fandom wikis.
*   **Target Wikis**: `dnd5e`, `forgottenrealms`, `criticalrole`, `eberron`, and 9 others.
*   **Output**: Writes to `dnd_trends_raw.fandom_daily_metrics`.

### 4. Fandom Raw Data (`fandom_*.txt`)
*   **Origin**: These files (found in `1_raw`) likely represent seeded lists of articles or previous crawl results.
*   **Files**: `fandom_monsters.txt`, `fandom_spells.txt`, etc.

## Supporting Files
*   **`fandom_spec.md`**: Detailed monitoring specification for Fandom wikis, explaining the "Why" and "How" of the monitoring strategy.
*   **`setup_wiki_schema.py`**: Creates the BigQuery tables for Wikipedia data.
*   **`setup_fandom_schema.py`**: Creates the BigQuery tables for Fandom data.
*   **`wiki_crawl_log.txt`**: Original log file showing the crawling process and the categories it expanded into (proof of finding 3,292 articles).
*   **`gcp-key.json`**: Authentication key (ensure this is valid/safe).

## Usage
These files are ready to be run in a Python environment.
1.  Install dependencies: `pip install -r requirements.txt`
2.  Run Crawler: `python wiki_crawler.py`
3.  Run Scraper: `python wikipedia_scraper.py`
