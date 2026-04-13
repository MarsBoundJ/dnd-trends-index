-- BGG/RPGGeek Analytics: The Commitment Index
-- Dataset: gold_data.analytics_bgg
-- Per-product ownership, quality, and archetype classification
-- Combines data from both BoardGameGeek and RPGGeek platforms
--
-- Usage: SELECT * FROM `dnd-trends-index.gold_data.analytics_bgg`
--        WHERE product_archetype = 'cult_classic' ORDER BY commitment_index DESC

CREATE OR REPLACE VIEW `dnd-trends-index.gold_data.analytics_bgg` AS

WITH -- Latest BGG snapshot per product
bgg_latest AS (
  SELECT concept_name, owned_count, quality_score, date,
    ROW_NUMBER() OVER (PARTITION BY concept_name ORDER BY date DESC) as rn
  FROM `dnd-trends-index.dnd_trends_raw.bgg_product_stats`
),
bgg AS (
  SELECT concept_name, owned_count as bgg_owned, quality_score as bgg_quality, date as bgg_date
  FROM bgg_latest WHERE rn = 1
),

-- Previous BGG snapshot (for velocity calculation)
bgg_prev AS (
  SELECT concept_name, owned_count as prev_owned, date as prev_date,
    ROW_NUMBER() OVER (PARTITION BY concept_name ORDER BY date DESC) as rn
  FROM `dnd-trends-index.dnd_trends_raw.bgg_product_stats`
  WHERE date < (SELECT MAX(date) FROM `dnd-trends-index.dnd_trends_raw.bgg_product_stats`)
),

-- Latest RPGGeek snapshot per product
rpg_latest AS (
  SELECT concept_name, owned_count, quality_score, date,
    ROW_NUMBER() OVER (PARTITION BY concept_name ORDER BY date DESC) as rn
  FROM `dnd-trends-index.dnd_trends_raw.rpggeek_product_stats`
),
rpg AS (
  SELECT concept_name, owned_count as rpg_owned, quality_score as rpg_quality, date as rpg_date
  FROM rpg_latest WHERE rn = 1
),

-- Union all products from both platforms
all_products AS (
  SELECT concept_name FROM bgg
  UNION DISTINCT
  SELECT concept_name FROM rpg
)

SELECT
  p.concept_name,

  -- BGG metrics
  COALESCE(b.bgg_owned, 0) as bgg_owned,
  ROUND(COALESCE(b.bgg_quality, 0), 2) as bgg_quality,
  b.bgg_date,

  -- RPGGeek metrics
  COALESCE(r.rpg_owned, 0) as rpg_owned,
  ROUND(COALESCE(r.rpg_quality, 0), 2) as rpg_quality,
  r.rpg_date,

  -- Combined ownership (max of both platforms)
  GREATEST(COALESCE(b.bgg_owned, 0), COALESCE(r.rpg_owned, 0)) as max_owned,

  -- Best quality score (either platform, ignoring zeros = unrated)
  CASE
    WHEN COALESCE(b.bgg_quality, 0) > 0 AND COALESCE(r.rpg_quality, 0) > 0
      THEN GREATEST(b.bgg_quality, r.rpg_quality)
    WHEN COALESCE(b.bgg_quality, 0) > 0 THEN b.bgg_quality
    WHEN COALESCE(r.rpg_quality, 0) > 0 THEN r.rpg_quality
    ELSE 0
  END as best_quality,

  -- Commitment Index: ownership × quality (volume weighted by satisfaction)
  ROUND(
    GREATEST(COALESCE(b.bgg_owned, 0), COALESCE(r.rpg_owned, 0))
    * CASE
        WHEN COALESCE(b.bgg_quality, 0) > 0 THEN b.bgg_quality
        WHEN COALESCE(r.rpg_quality, 0) > 0 THEN r.rpg_quality
        ELSE 5.0  -- neutral default for unrated
      END
    / 10.0,  -- normalize: perfect product = owned_count * 1.0
    1
  ) as commitment_index,

  -- Ownership velocity: change since previous snapshot
  COALESCE(b.bgg_owned, 0) - COALESCE(bp.prev_owned, 0) as ownership_velocity,
  bp.prev_date as velocity_since_date,

  -- BGG vs RPGGeek gap (positive = stronger on BGG, negative = stronger on RPGGeek)
  COALESCE(b.bgg_owned, 0) - COALESCE(r.rpg_owned, 0) as bgg_rpg_gap,

  -- Platform presence
  CASE
    WHEN COALESCE(b.bgg_owned, 0) > 0 AND COALESCE(r.rpg_owned, 0) > 0 THEN 'both'
    WHEN COALESCE(b.bgg_owned, 0) > 0 THEN 'bgg_only'
    WHEN COALESCE(r.rpg_owned, 0) > 0 THEN 'rpggeek_only'
    ELSE 'neither'
  END as platform_presence,

  -- Product archetype classification
  CASE
    -- Cult classic: low ownership but high quality (≥8.0)
    WHEN GREATEST(COALESCE(b.bgg_owned, 0), COALESCE(r.rpg_owned, 0)) < 20
         AND (COALESCE(b.bgg_quality, 0) >= 8.0 OR COALESCE(r.rpg_quality, 0) >= 8.0)
      THEN 'cult_classic'
    -- Mainstream hit: high ownership AND decent quality
    WHEN GREATEST(COALESCE(b.bgg_owned, 0), COALESCE(r.rpg_owned, 0)) >= 50
         AND (COALESCE(b.bgg_quality, 0) >= 6.0 OR COALESCE(r.rpg_quality, 0) >= 6.0)
      THEN 'mainstream_hit'
    -- Buyers remorse: high ownership but LOW quality (<6.0, must have rating)
    WHEN GREATEST(COALESCE(b.bgg_owned, 0), COALESCE(r.rpg_owned, 0)) >= 30
         AND (COALESCE(b.bgg_quality, 0) > 0 AND COALESCE(b.bgg_quality, 0) < 6.0)
      THEN 'buyers_remorse'
    -- Sleeper: low ownership, no strong quality signal yet
    WHEN GREATEST(COALESCE(b.bgg_owned, 0), COALESCE(r.rpg_owned, 0)) < 10
      THEN 'sleeper'
    -- Default
    ELSE 'established'
  END as product_archetype,

  -- Rank by commitment
  ROW_NUMBER() OVER (
    ORDER BY GREATEST(COALESCE(b.bgg_owned, 0), COALESCE(r.rpg_owned, 0))
    * CASE
        WHEN COALESCE(b.bgg_quality, 0) > 0 THEN b.bgg_quality
        WHEN COALESCE(r.rpg_quality, 0) > 0 THEN r.rpg_quality
        ELSE 5.0
      END DESC
  ) as rank_by_commitment,

  -- Standardized output contract
  'ownership' as signal_type,
  CAST(GREATEST(COALESCE(b.bgg_owned, 0), COALESCE(r.rpg_owned, 0)) AS FLOAT64) as primary_metric,
  ROUND(SAFE_DIVIDE(
    COALESCE(b.bgg_owned, 0) - COALESCE(bp.prev_owned, 0),
    GREATEST(COALESCE(bp.prev_owned, 0), 1)
  ), 3) as momentum,
  CASE
    WHEN COALESCE(b.bgg_quality, 0) > 0 AND COALESCE(b.bgg_owned, 0) >= 5 THEN 'HIGH'
    WHEN COALESCE(b.bgg_owned, 0) > 0 OR COALESCE(r.rpg_owned, 0) > 0 THEN 'MEDIUM'
    ELSE 'LOW'
  END as confidence,
  'bgg_rpggeek' as stream_name,
  CURRENT_DATE() as snapshot_date

FROM all_products p
LEFT JOIN bgg b ON p.concept_name = b.concept_name
LEFT JOIN (SELECT * FROM bgg_prev WHERE rn = 1) bp ON p.concept_name = bp.concept_name
LEFT JOIN rpg r ON p.concept_name = r.concept_name;
