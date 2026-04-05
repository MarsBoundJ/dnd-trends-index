-- Silver Layer: Cross-Platform Market Rank Normalization
-- Unifies Amazon numeric rank and DMs Guild/DriveThruRPG tier labels
-- into a common score_market (0–100) using PERCENT_RANK.
--
-- Join key:  title (fuzzy, best available until product IDs are backfilled)
-- Amazon:    lower rank = better  (rank 1 → score ~100, rank 100 → score ~0)
--            Pre-deduped by (asin, date) keeping MIN(rank) so a product that
--            appears in multiple category lists only counts once.
-- DMs Guild / DriveThruRPG: tier mapped to 1–7 ordinal, then PERCENT_RANK

CREATE OR REPLACE VIEW `dnd-trends-index.silver_data.norm_catalog_market` AS

WITH
-- Step 1: collapse Amazon rows so each ASIN appears once per day,
-- taking its best (lowest) rank across all category lists.
amazon_deduped AS (
    SELECT
        asin,
        title,
        date,
        MIN(rank) AS rank
    FROM `dnd-trends-index.dnd_trends_raw.amazon_daily_stats`
    WHERE rank > 0
    GROUP BY asin, title, date
),

-- Step 2: compute PERCENT_RANK on the clean, deduplicated Amazon set.
amazon AS (
    SELECT
        title,
        asin                        AS product_id,
        'Amazon'                    AS platform,
        date                        AS snapshot_date,
        CAST(NULL AS STRING)        AS list_name,
        rank                        AS raw_rank,
        CAST(NULL AS STRING)        AS seller_tier,
        -- Lower rank is better → invert so rank 1 → 100
        ROUND(
            (1.0 - PERCENT_RANK() OVER (
                PARTITION BY date
                ORDER BY rank ASC
            )) * 100, 1
        )                           AS score_market
    FROM amazon_deduped
),

catalog AS (
    SELECT
        title,
        COALESCE(product_url, title) AS product_id,
        source                       AS platform,
        DATE(collected_date)         AS snapshot_date,
        seller_tier                  AS list_name,
        CAST(NULL AS INT64)          AS raw_rank,
        seller_tier,
        -- Map tier to ordinal so PERCENT_RANK has something to order by
        ROUND(
            PERCENT_RANK() OVER (
                PARTITION BY DATE(collected_date), source
                ORDER BY CASE seller_tier
                    WHEN 'Adamantine' THEN 7
                    WHEN 'Mithral'    THEN 6
                    WHEN 'Platinum'   THEN 5
                    WHEN 'Gold'       THEN 4
                    WHEN 'Silver'     THEN 3
                    WHEN 'Electrum'   THEN 2
                    WHEN 'Copper'     THEN 1
                    ELSE 0
                END ASC
            ) * 100, 1
        )                            AS score_market
    FROM `dnd-trends-index.dnd_trends_raw.catalog_supply`
    WHERE source IN ('DMs Guild', 'DriveThruRPG')
      AND seller_tier IS NOT NULL
      AND seller_tier NOT IN ('Normal', '')
),

combined AS (
    SELECT * FROM amazon
    UNION ALL
    SELECT * FROM catalog
)

-- Dedup: one row per title+platform+date, keeping best rank and highest score
SELECT
    title,
    ANY_VALUE(product_id)   AS product_id,
    platform,
    snapshot_date,
    ANY_VALUE(seller_tier)  AS seller_tier,
    MIN(raw_rank)           AS raw_rank,
    MAX(score_market)       AS score_market
FROM combined
GROUP BY title, platform, snapshot_date;
