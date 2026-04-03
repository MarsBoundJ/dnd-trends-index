-- Silver Layer: Cross-Platform Market Rank Normalization
-- Unifies Amazon numeric rank and DMs Guild/DriveThruRPG tier labels
-- into a common score_market (0–100) using PERCENT_RANK.
--
-- Join key:  title (fuzzy, best available until product IDs are backfilled)
-- Amazon:    lower rank = higher score  (rank 1 → ~100, rank 100 → ~0)
-- DMs Guild / DriveThruRPG: tier mapped to 1–7 ordinal, then PERCENT_RANK

CREATE OR REPLACE VIEW `dnd-trends-index.silver_data.norm_catalog_market` AS

WITH amazon AS (
    SELECT
        title,
        asin                        AS product_id,
        'Amazon'                    AS platform,
        date                        AS snapshot_date,
        category                    AS list_name,
        rank                        AS raw_rank,
        CAST(NULL AS STRING)        AS seller_tier,
        -- Lower rank is better → invert with 1 - PERCENT_RANK
        ROUND(
            (1.0 - PERCENT_RANK() OVER (
                PARTITION BY date
                ORDER BY rank ASC
            )) * 100, 1
        )                           AS score_market
    FROM `dnd-trends-index.dnd_trends_raw.amazon_daily_stats`
    WHERE rank > 0
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
)

SELECT * FROM amazon
UNION ALL
SELECT * FROM catalog;
