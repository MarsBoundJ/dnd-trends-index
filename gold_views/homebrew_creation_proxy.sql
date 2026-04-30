-- ═══════════════════════════════════════════════════════════════════════
-- gold_data.homebrew_creation_proxy — Stage 6 of community_reception
-- ═══════════════════════════════════════════════════════════════════════
--
-- Per-IP "homebrew creation intensity" score, computed from existing
-- Stage 5 classified mentions filtered to r/UnearthedArcana — the
-- premier D&D-homebrew design subreddit.
--
-- ─── WHY THIS IS A DIFFERENT DIMENSION ────────────────────────────────
--
-- Reception (Stage 5 P1)        — "what D&D players SAY about IP X"
-- Acquisition (Stage 5 P2)      — "what IP X's fans say about D&D"
-- BGG (Stage 3)                 — "what tabletop gamers BUY of IP X"
-- AO3 (Stage 4)                 — "what fans WRITE in stories about IP X"
-- Homebrew (THIS STAGE)         — "what fans BUILD as game design"
--
-- Per ChatGPT's framing: "We don't just measure what fans say or
-- consume — we measure what they BUILD themselves when the official
-- product doesn't exist."
--
-- This is revealed mechanical demand under friction. Building a
-- homebrew subclass takes 5-15 hours of game design labor; people
-- only do that when they're "so desperate to play this IP that WotC
-- hasn't published it yet."
--
-- ─── DATA SOURCE ──────────────────────────────────────────────────────
--
-- We DO NOT need a new harvest. Our Stage 5 PRAW search already
-- captured posts from r/UnearthedArcana (one of the 7 D&D
-- subreddits we scan). The AI Bouncer already classified them with
-- crossover_attitude. The Stage 5 reception_proxy view averaged
-- across all 7 subreddits — this Stage 6 view filters to just
-- UnearthedArcana to extract the homebrew-creation slice.
--
-- ─── v2 (Stage 6c original) — UPVOTE-WEIGHTED + TYPE EXTRACTION ──────
--
-- Apr 30, 2026 v2 enhancements per the original Stage 6c plan in
-- docs/v2_session_seed_prompt.md:
--
-- 1. UPVOTE-WEIGHTED scoring: a 312-upvote BG3 post should contribute
--    more to attitude_avg than a 4-upvote post. We use log-scale
--    upvote weighting (1 + LOG10(score+1)) so a 312-upvote post is
--    ~3.5x weighted vs a 4-upvote post, not 78x. Sentiment-weighted
--    polarity (positive = +1.0, divisive = -0.4, negative = -1.0)
--    preserved from v1.
--
-- 2. HOMEBREW TYPE extraction via SQL pattern matching on post titles
--    (no LLM call needed — only 14 posts; deterministic patterns
--    cover them well: "subclass" / "race" / "spell" / "item"+"items"
--    / "monster" / fall-through to "other"). Surfaces per-type counts
--    in the data trail.
--
-- 3. TOP POST per IP by upvotes (replaces v1's MAX-by-post_id sample).
--    The most-upvoted post is the highest-quality anchor for the data
--    trail UI.
--
-- ─── SCORE FORMULA ────────────────────────────────────────────────────
--
--   weight_per_post = 1 + LOG10(upvotes + 1)
--   attitude_avg = SUM(attitude_score * weight) / SUM(weight)
--   homebrew_creation_score = (attitude_avg + 1) / 2
--
-- ─── ABSTENTION ───────────────────────────────────────────────────────
--
--   = 0 mentions  →  NULL with status='no_homebrew_signal'
--   1-2 mentions  →  scored with confidence='LOW'
--   3-5 mentions  →  scored with confidence='MEDIUM'
--   6+ mentions   →  scored with confidence='HIGH'
-- ═══════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW `dnd-trends-index.gold_data.homebrew_creation_proxy` AS

WITH

  -- ────────────────────────────────────────────────────────────────────
  -- Filter to r/UnearthedArcana confirmed-relevant mentions, joined
  -- with the candidate-post upvote count (score) + title.
  -- ────────────────────────────────────────────────────────────────────
  homebrew_mentions AS (
    SELECT
      c.ip_name,
      c.post_id,
      c.subreddit,
      c.crossover_attitude,
      c.confidence,
      p.title,
      p.score AS upvotes,
      p.num_comments,
      p.url AS post_url,
      CASE c.crossover_attitude
        WHEN 'positive'      THEN  1.0
        WHEN 'mentions_only' THEN  0.0
        WHEN 'divisive'      THEN -0.4
        WHEN 'negative'      THEN -1.0
        ELSE NULL
      END AS attitude_score,
      -- v2 upvote-log weight: 1 + log10(upvotes+1)
      -- 0 upvotes → 1.0, 4 → 1.7, 17 → 2.26, 57 → 2.76, 117 → 3.07, 312 → 3.50
      1.0 + LOG10(GREATEST(p.score, 0) + 1) AS upvote_weight,
      -- v2 type via SQL pattern matching on title
      CASE
        WHEN REGEXP_CONTAINS(LOWER(p.title), r'\bsubclass\b|\bcollege\b|\bcircle\b|\bpath of\b|\bdomain\b|\bschool of\b|\boath of\b|\bway of\b|\bpatron\b') THEN 'subclass'
        WHEN REGEXP_CONTAINS(LOWER(p.title), r'\brace\b|\bspecies\b|\bancestry\b|\bbackground\b') THEN 'race'
        WHEN REGEXP_CONTAINS(LOWER(p.title), r'\bspell\b|\bcantrip\b') THEN 'spell'
        WHEN REGEXP_CONTAINS(LOWER(p.title), r'\bitems?\b|\bweapon\b|\barmor\b|\bartifact\b|\bmask\b') THEN 'item'
        WHEN REGEXP_CONTAINS(LOWER(p.title), r'\bmonster\b|\bcreature\b|\benemy\b|\benemies\b|\bvillain\b|\bboss\b') THEN 'monster'
        WHEN REGEXP_CONTAINS(LOWER(p.title), r'\bfeats?\b') THEN 'feat'
        WHEN REGEXP_CONTAINS(LOWER(p.title), r'\brules?\b|\bconditions?\b|\bmechanic') THEN 'rules'
        ELSE 'other'
      END AS homebrew_type
    FROM `dnd-trends-index.dnd_trends_raw.reddit_ub_classified_mentions` c
    JOIN `dnd-trends-index.dnd_trends_raw.reddit_ub_candidate_posts` p
      ON p.post_id = c.post_id AND p.ip_name = c.ip_name
    WHERE c.is_about_ip = TRUE
      AND c.crossover_attitude != 'not_about_ip'
      AND c.subreddit = 'UnearthedArcana'
  ),

  per_ip AS (
    SELECT
      ip_name,
      COUNT(*) AS homebrew_mention_count,
      COUNTIF(crossover_attitude = 'positive')      AS homebrew_positive_count,
      COUNTIF(crossover_attitude = 'mentions_only') AS homebrew_mentions_only_count,
      COUNTIF(crossover_attitude = 'divisive')      AS homebrew_divisive_count,
      COUNTIF(crossover_attitude = 'negative')      AS homebrew_negative_count,
      -- v2 upvote-weighted attitude average
      SAFE_DIVIDE(
        SUM(attitude_score * upvote_weight),
        NULLIF(SUM(upvote_weight), 0)
      ) AS attitude_avg,
      -- v2 type breakdown
      COUNTIF(homebrew_type = 'subclass') AS subclass_count,
      COUNTIF(homebrew_type = 'race')     AS race_count,
      COUNTIF(homebrew_type = 'spell')    AS spell_count,
      COUNTIF(homebrew_type = 'item')     AS item_count,
      COUNTIF(homebrew_type = 'monster')  AS monster_count,
      COUNTIF(homebrew_type = 'feat')     AS feat_count,
      COUNTIF(homebrew_type = 'rules')    AS rules_count,
      COUNTIF(homebrew_type = 'other')    AS other_type_count,
      SUM(upvotes) AS total_upvotes,
      MAX(upvotes) AS top_post_upvotes
    FROM homebrew_mentions
    GROUP BY ip_name
  ),

  -- ────────────────────────────────────────────────────────────────────
  -- Top post per IP (highest upvotes among positive-attitude posts)
  -- — replaces v1's MAX(post_id) sampling. Includes type for the trail.
  -- ────────────────────────────────────────────────────────────────────
  top_post_per_ip AS (
    SELECT
      ip_name,
      ARRAY_AGG(STRUCT(title, upvotes, num_comments, post_url, homebrew_type)
                ORDER BY upvotes DESC LIMIT 1)[OFFSET(0)] AS top_post
    FROM homebrew_mentions
    WHERE crossover_attitude IN ('positive', 'divisive')
    GROUP BY ip_name
  ),

  joined AS (
    SELECT
      s.ip_name,
      s.medium,
      s.tier,
      p.homebrew_mention_count,
      p.homebrew_positive_count,
      p.homebrew_mentions_only_count,
      p.homebrew_divisive_count,
      p.homebrew_negative_count,
      p.attitude_avg,
      p.subclass_count,
      p.race_count,
      p.spell_count,
      p.item_count,
      p.monster_count,
      p.feat_count,
      p.rules_count,
      p.other_type_count,
      p.total_upvotes,
      p.top_post_upvotes,
      tp.top_post
    FROM `dnd-trends-index.dnd_trends_raw.ub_candidate_seeds` s
    LEFT JOIN per_ip p ON p.ip_name = s.ip_name
    LEFT JOIN top_post_per_ip tp ON tp.ip_name = s.ip_name
  )

SELECT
  j.ip_name,
  j.medium,
  j.tier,

  -- ─── THE SCORE (upvote-weighted attitude average, [0, 1]) ─────────────
  CASE
    WHEN COALESCE(j.homebrew_mention_count, 0) = 0 THEN NULL
    ELSE ROUND((j.attitude_avg + 1.0) / 2.0, 4)
  END AS homebrew_creation_score,

  -- ─── STATUS + CONFIDENCE ──────────────────────────────────────────────
  CASE
    WHEN COALESCE(j.homebrew_mention_count, 0) = 0 THEN 'no_homebrew_signal'
    ELSE 'sufficient'
  END AS homebrew_status,

  CASE
    WHEN COALESCE(j.homebrew_mention_count, 0) = 0 THEN 'NONE'
    WHEN j.homebrew_mention_count <= 2 THEN 'LOW'
    WHEN j.homebrew_mention_count <= 5 THEN 'MEDIUM'
    ELSE 'HIGH'
  END AS homebrew_signal_confidence,

  -- ─── VOLUME + ATTITUDE BREAKDOWN ──────────────────────────────────────
  COALESCE(j.homebrew_mention_count, 0)       AS homebrew_mention_count,
  COALESCE(j.homebrew_positive_count, 0)      AS homebrew_positive_count,
  COALESCE(j.homebrew_mentions_only_count, 0) AS homebrew_mentions_only_count,
  COALESCE(j.homebrew_divisive_count, 0)      AS homebrew_divisive_count,
  COALESCE(j.homebrew_negative_count, 0)      AS homebrew_negative_count,

  -- ─── v2 TYPE BREAKDOWN ────────────────────────────────────────────────
  COALESCE(j.subclass_count, 0)   AS homebrew_subclass_count,
  COALESCE(j.race_count, 0)       AS homebrew_race_count,
  COALESCE(j.spell_count, 0)      AS homebrew_spell_count,
  COALESCE(j.item_count, 0)       AS homebrew_item_count,
  COALESCE(j.monster_count, 0)    AS homebrew_monster_count,
  COALESCE(j.feat_count, 0)       AS homebrew_feat_count,
  COALESCE(j.rules_count, 0)      AS homebrew_rules_count,
  COALESCE(j.other_type_count, 0) AS homebrew_other_type_count,
  COALESCE(j.total_upvotes, 0)    AS homebrew_total_upvotes,
  COALESCE(j.top_post_upvotes, 0) AS homebrew_top_post_upvotes,

  -- ─── TOP POST (highest-upvoted positive-attitude post) ───────────────
  j.top_post.title         AS sample_post_title,
  j.top_post.upvotes       AS sample_post_upvotes,
  j.top_post.num_comments  AS sample_post_comments,
  j.top_post.post_url      AS sample_post_url,
  j.top_post.homebrew_type AS sample_post_type,

  -- ─── HUMAN-READABLE REASONING ─────────────────────────────────────────
  CASE
    WHEN COALESCE(j.homebrew_mention_count, 0) = 0 THEN
      'No homebrew creation detected in r/UnearthedArcana over the last 30 days.'
    ELSE
      CONCAT(
        CAST(j.homebrew_mention_count AS STRING),
        ' homebrew creation signal(s) in r/UnearthedArcana ',
        '(', CAST(COALESCE(j.total_upvotes, 0) AS STRING), ' total upvotes). ',
        'Top: "', SUBSTR(COALESCE(j.top_post.title, ''), 1, 60),
        IF(LENGTH(COALESCE(j.top_post.title, '')) > 60, '..."', '"'),
        ' (', CAST(COALESCE(j.top_post.upvotes, 0) AS STRING), ' upvotes, ',
        COALESCE(j.top_post.homebrew_type, 'other'), '). ',
        'Types: ',
        CAST(COALESCE(j.subclass_count, 0) AS STRING), ' subclass / ',
        CAST(COALESCE(j.spell_count, 0)    AS STRING), ' spell / ',
        CAST(COALESCE(j.monster_count, 0)  AS STRING), ' monster / ',
        CAST(COALESCE(j.item_count, 0)     AS STRING), ' item / ',
        CAST(COALESCE(j.race_count, 0)     AS STRING), ' race / ',
        CAST(COALESCE(j.other_type_count, 0) + COALESCE(j.rules_count, 0) + COALESCE(j.feat_count, 0) AS STRING), ' other.'
      )
  END AS homebrew_reasoning,

  -- Standardized output contract
  'community_reception'                  AS signal_type,
  'reddit_unearthedarcana_homebrew_v2'   AS stream_name,
  CURRENT_DATE()                         AS snapshot_date

FROM joined j
ORDER BY homebrew_creation_score DESC NULLS LAST;
