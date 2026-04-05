-- Silver Layer: Cross-Platform Title Matching
-- Finds products that appear on multiple platforms (Amazon, DMs Guild, DriveThruRPG)
-- using normalized title matching + Jaccard token similarity.
--
-- Matching pipeline:
--   1. Normalize  — strip parentheticals, edition tags, volume tags, punctuation
--                   NOTE: colon-based subtitle stripping intentionally omitted —
--                   "Magic: The Gathering" stripping to "magic" caused false positives.
--                   Jaccard naturally penalises extra subtitle tokens instead.
--   2. Tokenize   — split into words, remove stop words & single-char tokens
--   3. Jaccard    — intersection / union of token sets
--   4. Confidence — high ≥ 0.8 | medium ≥ 0.65 | borderline ≥ 0.4
--
-- Guards against trivially short matches:
--   - Both titles must produce ≥ 2 tokens after normalization
--   - Intersection must be ≥ 2 tokens
--
-- match_confidence values:
--   'high'       → very likely the same product, safe to treat as a match
--   'medium'     → probably the same, worth a quick review
--   'borderline' → possible match, needs human confirmation
--
-- Always compares each platform's latest available snapshot so stale dates
-- don't pollute results.

CREATE OR REPLACE VIEW `dnd-trends-index.silver_data.catalog_title_matches` AS

WITH

-- ── Step 1: pin each platform to its most recent snapshot ───────────────────
latest_per_platform AS (
  SELECT platform, MAX(snapshot_date) AS latest_date
  FROM `dnd-trends-index.silver_data.norm_catalog_market`
  GROUP BY platform
),

latest AS (
  SELECT
    m.title,
    m.platform,
    m.product_id,
    m.score_market,
    m.snapshot_date,
    m.seller_tier,
    m.raw_rank
  FROM `dnd-trends-index.silver_data.norm_catalog_market` m
  JOIN latest_per_platform lp
    ON m.platform  = lp.platform
   AND m.snapshot_date = lp.latest_date
),

-- ── Step 2: normalize titles ─────────────────────────────────────────────────
-- Rules applied in order (each feeds the next):
--   a) lower-case everything
--   b) strip parentheticals  e.g. (5e)  (D&D Core Rulebook)
--   c) strip written-out edition tags  e.g. "Second Edition"
--   d) strip shorthand edition tags  e.g. "5e"  "2e"
--   e) strip volume / book tags  e.g. "Vol. 2"  "Book 3"
--   f) strip all remaining non-alphanumeric characters
--   g) collapse runs of whitespace to a single space
normalized AS (
  SELECT
    title             AS original_title,
    platform,
    product_id,
    score_market,
    snapshot_date,
    seller_tier,
    raw_rank,
    TRIM(REGEXP_REPLACE(
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(
              REGEXP_REPLACE(
                LOWER(title),
                r'\s*\([^)]*\)', ''
              ),
              r'\b(first|second|third|fourth|fifth|sixth|1st|2nd|3rd|4th|5th|6th)\s+edition\b', ''
            ),
            r'\b[2-9]e\b', ''
          ),
          r'\bvol(ume)?\.?\s*\d+\b|\bbook\s+\d+\b', ''
        ),
        r'[^a-z0-9\s]', ''
      ),
      r'\s+', ' '
    ))                AS norm_title
  FROM latest
),

-- ── Step 3: tokenize — split on spaces, drop stop words & short tokens ───────
tokenized AS (
  SELECT
    original_title,
    norm_title,
    platform,
    product_id,
    score_market,
    snapshot_date,
    seller_tier,
    raw_rank,
    ARRAY(
      SELECT tok
      FROM UNNEST(SPLIT(norm_title, ' ')) AS tok
      WHERE LENGTH(tok) > 1
        AND tok NOT IN (
          'the','a','an','of','for','in','to','and','or','with',
          'on','at','by','from','into','as','its','is','are',
          'be','been','was','were','do','does','did','will',
          'that','this','these','those','it','my','your','our'
        )
    ) AS tokens
  FROM normalized
  WHERE LENGTH(TRIM(norm_title)) > 0
),

-- ── Step 4: split by platform role ──────────────────────────────────────────
amazon AS (
  SELECT * FROM tokenized WHERE platform = 'Amazon'
),
catalog AS (
  SELECT * FROM tokenized WHERE platform IN ('DMs Guild', 'DriveThruRPG')
),

-- ── Step 5: cross-join and score ─────────────────────────────────────────────
-- Jaccard = |intersection| / |union|
--   intersection: tokens from amazon that also appear in catalog
--   union:        distinct tokens across both arrays
pairs AS (
  SELECT
    a.original_title   AS amazon_title,
    c.original_title   AS catalog_title,
    a.norm_title       AS amazon_norm,
    c.norm_title       AS catalog_norm,
    c.platform         AS catalog_platform,
    a.score_market     AS amazon_score,
    c.score_market     AS catalog_score,
    a.snapshot_date    AS amazon_date,
    c.snapshot_date    AS catalog_date,
    c.seller_tier,
    a.raw_rank         AS amazon_rank,
    SAFE_DIVIDE(
      (SELECT COUNTIF(tok IN UNNEST(c.tokens)) FROM UNNEST(a.tokens) AS tok),
      (SELECT COUNT(DISTINCT tok)
       FROM UNNEST(ARRAY_CONCAT(a.tokens, c.tokens)) AS tok)
    )                  AS jaccard_score
  FROM amazon  a
  CROSS JOIN catalog c
  -- Guard 1: both sides must have at least 2 meaningful tokens
  WHERE ARRAY_LENGTH(a.tokens) >= 2
    AND ARRAY_LENGTH(c.tokens) >= 2
  -- Guard 2: must share at least 2 tokens (prevents single-word 1.0 scores)
    AND (SELECT COUNTIF(tok IN UNNEST(c.tokens)) FROM UNNEST(a.tokens) AS tok) >= 2
)

-- ── Final: filter to meaningful matches, attach confidence label ─────────────
SELECT
  amazon_title,
  catalog_title,
  catalog_platform,
  amazon_norm,
  catalog_norm,
  ROUND(jaccard_score, 3)                              AS jaccard_score,
  CASE
    WHEN jaccard_score >= 0.8  THEN 'high'
    WHEN jaccard_score >= 0.65 THEN 'medium'
    WHEN jaccard_score >= 0.4  THEN 'borderline'
  END                                                  AS match_confidence,
  amazon_score,
  catalog_score,
  ROUND((amazon_score + catalog_score) / 2, 1)        AS combined_score,
  seller_tier,
  amazon_rank,
  amazon_date,
  catalog_date
FROM pairs
WHERE jaccard_score >= 0.4
ORDER BY jaccard_score DESC, combined_score DESC;
