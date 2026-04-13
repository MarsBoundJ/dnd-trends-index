# Digital & Video Game Data Streams — Technical Specification

**Status:** Planned (April 13, 2026)
**Priority:** WS3b — builds on completed WS3 Data Quality work
**Goal:** Track D&D's digital footprint to capture the tabletop-to-digital feedback loop

## Strategic Context

WotC/Hasbro are pivoting to a digital-first ecosystem. Key evidence:
- BG3 and Monopoly Go carried Hasbro's recent balance sheets
- D&D Beyond acquisition positions digital as primary distribution
- Project Sigil (3D VTT) represents major digital investment
- Physical books now bundle digital DLC (e.g., Astarion's Book of Hungers)

**The Astarion Example:** BG3's vampire rogue companion became a viral sensation, driving millions of new players (especially non-traditional D&D demographics) into the ecosystem. WotC capitalized with official character sheets, MTG crossovers, and digital cosmetics. This feedback loop (digital game popularity > tabletop supplement sales) is the core insight we're building to measure.

## New Data Streams (8 total)

### Stream 1: Steam API — "Player Pulse"

**What:** Concurrent player counts, review sentiment, achievement stats, DLC catalog, patch dates for D&D video games.

**D&D Games to Track:**

| Game | Steam App ID | Status |
|------|-------------|--------|
| Baldur's Gate 3 | 1086940 | Flagship |
| Neverwinter | 109600 | Active MMO |
| Solasta: Crown of the Magister | 1096530 | SRD 5.1 |
| Idle Champions of the Forgotten Realms | 627690 | Official D&D |
| Dark Alliance | 623280 | Completeness |

**Endpoints:**

| Data Point | Endpoint | Auth | Frequency |
|-----------|----------|------|-----------|
| Concurrent players | `ISteamUserStats/GetNumberOfCurrentPlayers` | API key | Every 6 hours |
| Review counts & score | `store.steampowered.com/appreviews/{appid}?json=1` | None | Daily |
| Achievement unlock % | `ISteamUserStats/GetGlobalAchievementPercentagesForApp` | API key | Weekly |
| App metadata & DLC | `store.steampowered.com/api/appdetails?appids={id}` | None | Weekly |

**Rate Limits:** 100,000 requests/day (keyed), ~200/5min (store API).

**BQ Tables:**
- `dnd_trends_raw.steam_player_counts` — Partitioned by snapshot_date. Fields: app_id, app_name, concurrent_players, snapshot_ts.
- `dnd_trends_raw.steam_reviews` — Partitioned by fetch_date. Fields: app_id, total_positive, total_negative, review_score_pct, recent_positive, recent_negative.
- `dnd_trends_raw.steam_achievements` — Partitioned by fetch_date. Fields: app_id, achievement_name, unlock_pct.

**Intelligence Value:**
- Review sentiment analysis via Gemini: "camp interactions and romance" praise in BG3 reviews = WotC should invest in downtime/NPC relationship mechanics for tabletop
- Achievement unlock % reveals what content players actually engage with vs skip
- Patch date correlation: spikes in players/Reddit/search activity around patches

---

### Stream 2: mod.io — BG3 Official Mod Platform

**What:** BG3's official cross-platform mod system (not Steam Workshop). 100M+ downloads by Jan 2025.

**Endpoints:**
- `GET /v1/games/{game_id}/mods` — Sort by downloads_today, downloads_total, subscribers_total, date_added
- Game Object — total mod count, downloads in last 24h, total downloads, daily average

**Auth:** Free API key from mod.io registration.

**BQ Table:** `dnd_trends_raw.modio_mods` — Partitioned by fetch_date. Fields: mod_id, mod_name, downloads_total, downloads_today, subscribers, date_added, tags, summary.

**Intelligence Value ("Mod = Missing Feature" Tracker):**
- If "Level 20 Cap" mod gets 500k downloads = quantifiable demand for expanded level content
- If "Artificer Class" mod is trending = WotC has data-backed reason to prioritize for tabletop
- Mod categories/tags reveal player priorities

---

### Stream 3: Nexus Mods API — Community Mod Ecosystem

**What:** Largest PC modding community. BG3 has a massive presence.

**Endpoints:**
- `GET /v1/games/baldursgate3/mods/trending.json` — Currently trending
- `GET /v1/games/baldursgate3/mods/latest_added.json` — New mods
- `GET /v1/games/baldursgate3/mods/latest_updated.json` — Recently updated
- `GET /v1/games/baldursgate3/mods/{mod_id}.json` — Individual mod detail

**Auth:** Free API key from Nexus Mods account settings.
**Rate Limit:** 20,000 requests/day.

**BQ Table:** `dnd_trends_raw.nexus_mods` — Partitioned by fetch_date. Fields: mod_id, mod_name, category, downloads, endorsements, date_added, summary.

---

### Stream 4: Twitch Helix API — Streaming + VTT Market Share

**What:** Viewer counts for D&D game categories, plus stream tag mining for VTT market share.

**Categories to Track:**
- "Baldur's Gate 3"
- "Dungeons & Dragons"
- Other D&D games as relevant

**Endpoints:**
- `GET /helix/streams?game_id={id}` — Active streams, paginated (sum viewers for total)
- `GET /helix/games?name={name}` — Get game_id

**Auth:** OAuth client credentials from dev.twitch.tv.
**Rate Limit:** 800 points/min.

**Unique Signal — VTT Tag Mining:**
Scrape stream tags for: "Roll20", "Foundry VTT", "D&D Beyond", "Owlbear Rodeo", "Talespire". This gives actual VTT market share data from streamer usage.

**BQ Table:** `dnd_trends_raw.twitch_viewership` — Partitioned by snapshot_date. Fields: category_name, total_streams, total_viewers, top_tags (REPEATED STRING), snapshot_ts.

---

### Stream 5: AO3 (Archive of Our Own) — Narrative Engagement

**What:** Fanfiction tag counts as a metric for cultural obsession with D&D/BG3 characters.

**Method:** BeautifulSoup scrape of tag pages. No API available but HTML is cleanly structured.

**Tags to Track:**
- BG3 companions: Astarion, Shadowheart, Gale, Karlach, Lae'zel, Wyll, Halsin, Minthara
- D&D characters: Drizzt, Strahd, Jarlaxle, Vecna
- Fandoms: "Baldur's Gate (Video Games)", "Dungeons & Dragons (Roleplaying Game)"

**Frequency:** Weekly (tag counts change slowly).

**BQ Table:** `dnd_trends_raw.ao3_tag_counts` — Partitioned by fetch_date. Fields: tag_name, tag_type (character/fandom/relationship), work_count, fetch_date.

**Intelligence Value:** Pure "narrative engagement" metric. If Astarion has 50,000 stories, WotC knows a comic, novel, or supplement featuring him will sell. Nobody else tracks this — high wow-factor for the WotC pitch.

---

### Stream 6: D&D Beyond Product Catalog

**What:** Track WotC's own digital strategy moves by scraping the public product catalog.

**Data Points:**
- New digital product launches (dates, titles, prices)
- Format: digital-only vs book+digital bundles vs digital DLC for physical books
- Featured/promoted products on front page
- Price changes over time

**Method:** Playwright or requests (may need Cloudflare bypass).
**Frequency:** Daily or weekly.

**BQ Table:** `dnd_trends_raw.dndbeyond_catalog` — Fields: product_id, title, format, price, is_featured, first_seen, last_seen.

---

### Stream 7: r/BaldursGate3 Subreddit

**What:** Add r/BaldursGate3 (1.4M+ members) to existing Reddit harvester.

**Implementation:** Single row insert into `subreddit_registry` table. Zero new infrastructure.

**Intelligence Value:** Catches BG3-to-tabletop crossover discussion. Entity mentions (Astarion, Shadowheart, etc.) in a gaming sub that cross-references with D&D tabletop concepts.

---

### Stream 8: YouTube Shorts (Short-Form Virality)

**What:** Filter existing YouTube API data by duration < 60 seconds with D&D/BG3 hashtags.

**Hashtags:** #bg3, #astarion, #dnd, #baldursgate3, #dnd5e

**Implementation:** Add duration filter to existing YouTube listener queries.

**Intelligence Value:** Short-form virality is a leading indicator — spikes predict broader interest 2-4 weeks later.

---

## Schema Enhancements

### Concept Library Origin Tagging

Add columns to `dnd_trends_categorized.concept_library`:
- `origin` (STRING) — Values: "video_game", "tabletop", "streaming", "community", "mixed"
- `canonical_source` (STRING) — Values: "BG3", "PHB 2024", "Critical Role", "Forgotten Realms", etc.

This enables the digital-to-tabletop crossover analysis: track when a video_game-origin concept starts appearing in tabletop signals.

### Cross-Pollination Index (Gold Layer)

For any concept (e.g., "Mind Flayer"), create a composite view showing:

**Digital Footprint:**
- Steam review mentions
- Twitch stream titles/tags
- mod.io + Nexus Mods download counts
- AO3 fanfiction count

**Paper Footprint:**
- Kickstarter projects
- DMs Guild supplements
- Wiki edits (Fandom + Wikipedia)
- Google Trends interest score

**Timeline View:** Shows how digital spikes predict tabletop spikes with a measurable lag (typically 2-4 weeks).

---

## Build Order

| Phase | Streams | Effort |
|-------|---------|--------|
| 1 | Steam API + mod.io + r/BaldursGate3 (subreddit add) | Steam & mod.io are new Cloud Functions; subreddit is a BQ insert |
| 2 | Nexus Mods + Twitch | Two new Cloud Functions |
| 3 | AO3 + YouTube Shorts filter | One new function + existing function enhancement |
| 4 | D&D Beyond catalog + concept origin tagging | Scraper + schema migration |
| 5 | Cross-Pollination Index (gold layer view) | BQ views after all streams have data |

## Sources Not Pursued

| Source | Reason |
|--------|--------|
| TikTok | Hostile to scraping, restricted API. YouTube Shorts covers short-form signal. |
| GOG | No useful API for player/sales data |
| Console stores (PS/Xbox) | No public APIs for player counts or rankings |
| D&D Beyond user data | No public API, aggressive CAPTCHAs. Catalog scraping only. |
| SteamCharts/SteamDB | Against ToS to scrape. Just poll Steam's own API directly. |
| SteamSpy | Low priority bonus; inaccurate owner estimates. Can add later if needed. |

## API Keys Needed

| Service | Registration URL | Cost |
|---------|-----------------|------|
| Steam Web API | steamcommunity.com/dev | Free |
| mod.io | mod.io (register) | Free |
| Nexus Mods | nexusmods.com account settings | Free |
| Twitch | dev.twitch.tv | Free |
