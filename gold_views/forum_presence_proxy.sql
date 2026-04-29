-- ═══════════════════════════════════════════════════════════════════════
-- gold_data.forum_presence_proxy — Stage 7 of community_reception
-- ═══════════════════════════════════════════════════════════════════════
--
-- Per-IP "forum presence intensity" computed from Google Custom Search
-- of the 4 major TTRPG forums (EN World, Giant in the Playground,
-- RPG.net, Dragonsfoot).
--
-- ─── WHY THIS IS A DIFFERENT DIMENSION ────────────────────────────────
--
-- Reception (Reddit Stage 5)    — players talking
-- Acquisition (Reddit P2)        — IP fans wanting D&D
-- BGG (Stage 3)                  — board-game wallet voting
-- AO3 (Stage 4)                  — narrative-fan creative output
-- Homebrew (Stage 6)             — D&D mechanical creators
-- Forums (THIS STAGE)            — DM/whale demographic, brand purity
--
-- The audience here is the "Whale" buyer: older, more system-literate,
-- protective of D&D's identity, and wields purchasing power for $50
-- hardcovers and $150 miniature sets. Per Gemini's framing:
-- "Reddit is full of Players. AO3 is full of Fans. Traditional forums
-- are full of Dungeon Masters."
--
-- High forum presence indicates the IP has registered with the DM
-- demographic. Low presence (especially while Reddit/AO3 show signal)
-- means the IP is mainstream-popular but hasn't penetrated the
-- DM-decision-maker layer yet.
--
-- ─── v1 LIMITATIONS ────────────────────────────────────────────────────
--
-- Presence-only — no sentiment classification yet. We capture thread
-- counts + top URLs but don't read the thread content.
--
-- v2 (deferred to post-Expo): scrape the captured top URLs via
-- Playwright (or bookmarklet pattern for Cloudflare-blocked forums)
-- and classify sentiment per thread. The schema captures top_thread_urls
-- specifically so v2 can layer cleanly without re-running CSE.
--
-- ─── SCORE FORMULA ─────────────────────────────────────────────────────
--
-- Log-scale normalize total_results_combined within the dataset:
--
--   forum_presence_score = log10(total_results + 1) / log10(MAX + 1)
--
-- Same pattern as AO3 fanfic_proxy_score. Heavy-tailed distribution
-- (mainstream IPs in thousands, niche in single digits) is correctly
-- represented on a log scale.
--
-- ─── ABSTENTION ────────────────────────────────────────────────────────
--
-- Lower threshold than Reddit (>=1 vs >=5) because each forum thread
-- represents disproportionate effort — these are long-form discussion
-- forums, not quick-hit social feeds.
--
--   = 0 results          →  NULL with status='no_forum_signal'
--   1-10 results         →  scored with confidence='LOW'
--   11-100 results       →  scored with confidence='MEDIUM'
--   100+ results         →  scored with confidence='HIGH'
--
-- ─── PER-FORUM BREAKDOWN ───────────────────────────────────────────────
--
-- The top 10 URLs per IP are bucketed by forum_domain via URL parsing,
-- producing per-forum hit counts (out of top 10) for the data trail.
-- Useful for showing "this IP has presence on EN World but not
-- Dragonsfoot" patterns.
-- ═══════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW `dnd-trends-index.gold_data.forum_presence_proxy` AS

WITH

  latest_per_ip AS (
    SELECT *
    FROM `dnd-trends-index.dnd_trends_raw.forum_presence_counts`
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY ip_name ORDER BY harvested_at DESC
    ) = 1
  ),

  -- ────────────────────────────────────────────────────────────────────
  -- Compute the dataset-wide max for log-scale normalization.
  -- Done as a CROSS JOIN because BQ doesn't allow aggregate-functions
  -- inside expressions in the same SELECT.
  -- ────────────────────────────────────────────────────────────────────
  max_results AS (
    SELECT MAX(total_results_combined) AS max_total
    FROM latest_per_ip
    WHERE total_results_combined > 0
  ),

  -- ────────────────────────────────────────────────────────────────────
  -- Per-forum bucketing of the top URLs.  Each top_thread_urls entry
  -- has a forum_domain field; UNNEST + COUNTIF gives us per-forum hit
  -- counts (out of top 10 results returned by Google).
  -- ────────────────────────────────────────────────────────────────────
  per_forum_buckets AS (
    SELECT
      l.ip_name,
      COUNTIF(t.forum_domain = 'enworld.org')           AS enworld_top_hits,
      COUNTIF(t.forum_domain = 'forums.giantitp.com')   AS giantitp_top_hits,
      COUNTIF(t.forum_domain = 'rpg.net')               AS rpgnet_top_hits,
      COUNTIF(t.forum_domain = 'dragonsfoot.org')       AS dragonsfoot_top_hits
    FROM latest_per_ip l, UNNEST(l.top_thread_urls) AS t
    GROUP BY l.ip_name
  ),

  -- Pull the top URL + title for the data trail (for the IP detail panel)
  top_url_per_ip AS (
    SELECT
      l.ip_name,
      ARRAY_AGG(STRUCT(t.url, t.title, t.forum_domain) ORDER BY t.forum_domain LIMIT 1)[OFFSET(0)] AS top_url
    FROM latest_per_ip l, UNNEST(l.top_thread_urls) AS t
    GROUP BY l.ip_name
  ),

  joined AS (
    SELECT
      s.ip_name,
      s.medium,
      s.tier,
      l.total_results_combined,
      l.harvested_at,
      l.top_thread_urls,
      pf.enworld_top_hits,
      pf.giantitp_top_hits,
      pf.rpgnet_top_hits,
      pf.dragonsfoot_top_hits,
      tu.top_url AS top_url_struct
    FROM `dnd-trends-index.dnd_trends_raw.ub_candidate_seeds` s
    LEFT JOIN latest_per_ip l USING (ip_name)
    LEFT JOIN per_forum_buckets pf USING (ip_name)
    LEFT JOIN top_url_per_ip tu USING (ip_name)
  )

SELECT
  j.ip_name,
  j.medium,
  j.tier,

  -- ─── THE SCORE ────────────────────────────────────────────────────────
  CASE
    WHEN COALESCE(j.total_results_combined, 0) = 0 THEN NULL
    ELSE ROUND(
      SAFE_DIVIDE(
        LOG10(j.total_results_combined + 1),
        LOG10((SELECT max_total FROM max_results) + 1)
      ),
      4
    )
  END AS forum_presence_score,

  -- ─── STATUS + CONFIDENCE ──────────────────────────────────────────────
  CASE
    WHEN COALESCE(j.total_results_combined, 0) = 0 THEN 'no_forum_signal'
    ELSE 'sufficient'
  END AS forum_status,

  CASE
    WHEN COALESCE(j.total_results_combined, 0) = 0 THEN 'NONE'
    WHEN j.total_results_combined <= 10 THEN 'LOW'
    WHEN j.total_results_combined <= 100 THEN 'MEDIUM'
    ELSE 'HIGH'
  END AS forum_signal_confidence,

  -- ─── DATA TRAIL ───────────────────────────────────────────────────────
  COALESCE(j.total_results_combined, 0) AS total_results_combined,
  COALESCE(j.enworld_top_hits, 0)       AS enworld_top_hits,
  COALESCE(j.giantitp_top_hits, 0)      AS giantitp_top_hits,
  COALESCE(j.rpgnet_top_hits, 0)        AS rpgnet_top_hits,
  COALESCE(j.dragonsfoot_top_hits, 0)   AS dragonsfoot_top_hits,
  j.top_url_struct.url AS top_thread_url,
  j.top_url_struct.title AS top_thread_title,
  j.top_url_struct.forum_domain AS top_thread_forum,
  j.top_thread_urls,  -- full list of top 10 — for v2 sentiment scraping
  j.harvested_at,

  -- ─── HUMAN-READABLE REASONING ─────────────────────────────────────────
  CASE
    WHEN COALESCE(j.total_results_combined, 0) = 0 THEN
      'No forum-thread mentions in the 4 monitored TTRPG forums.'
    ELSE
      CONCAT(
        CAST(j.total_results_combined AS STRING),
        ' forum-thread mentions across EN World/GitP/RPG.net/Dragonsfoot. ',
        'Top URLs from: EN World ', CAST(COALESCE(j.enworld_top_hits, 0) AS STRING),
        ', GitP ', CAST(COALESCE(j.giantitp_top_hits, 0) AS STRING),
        ', RPG.net ', CAST(COALESCE(j.rpgnet_top_hits, 0) AS STRING),
        ', Dragonsfoot ', CAST(COALESCE(j.dragonsfoot_top_hits, 0) AS STRING),
        '. Sample thread: "',
        SUBSTR(COALESCE(j.top_url_struct.title, ''), 1, 80),
        IF(LENGTH(COALESCE(j.top_url_struct.title, '')) > 80, '..."', '"')
      )
  END AS forum_reasoning,

  -- Standardized output contract
  'community_reception' AS signal_type,
  'forum_presence_google_cse' AS stream_name,
  CURRENT_DATE() AS snapshot_date

FROM joined j
ORDER BY forum_presence_score DESC NULLS LAST;
