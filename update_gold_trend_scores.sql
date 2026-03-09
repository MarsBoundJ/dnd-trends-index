-- Gold Layer: Finalize Trend Scores with Crowdfunding
CREATE OR REPLACE VIEW `dnd-trends-index.gold_data.trend_scores` AS
WITH universe AS (
    SELECT date, keyword FROM `dnd-trends-index.silver_data.norm_wikipedia`
    UNION DISTINCT
    SELECT date, keyword FROM `dnd-trends-index.silver_data.norm_fandom`
    UNION DISTINCT
    SELECT date, keyword FROM `dnd-trends-index.silver_data.norm_roll20`
    UNION DISTINCT
    SELECT date, keyword FROM `dnd-trends-index.silver_data.norm_youtube`
    UNION DISTINCT
    SELECT date, search_term as keyword FROM `dnd-trends-index.dnd_trends_categorized.trend_data_pilot`
    UNION DISTINCT
    SELECT date, keyword FROM `dnd-trends-index.silver_data.norm_buy`
    UNION DISTINCT
    SELECT date, name as keyword FROM `dnd-trends-index.silver_data.norm_crowdfunding`
),
buy_signals AS (
    SELECT date, keyword, AVG(score_buy) as avg_buy
    FROM `dnd-trends-index.silver_data.norm_buy`
    GROUP BY 1, 2
),
cf_signals AS (
    SELECT date, name as keyword, AVG(score_crowdfund) as avg_cf
    FROM `dnd-trends-index.silver_data.norm_crowdfunding`
    GROUP BY 1, 2
),
daily_scores AS (
    SELECT 
        u.date,
        u.keyword,
        w.score_wiki,
        f.score_fandom,
        r.score_roll20,
        y.score_youtube,
        g.interest as score_google,
        b.avg_buy,
        cf.avg_cf
    FROM universe u
    LEFT JOIN `dnd-trends-index.silver_data.norm_wikipedia` w
        ON u.date = w.date AND u.keyword = w.keyword
    LEFT JOIN `dnd-trends-index.silver_data.norm_fandom` f
        ON u.date = f.date AND u.keyword = f.keyword
    LEFT JOIN `dnd-trends-index.silver_data.norm_roll20` r
        ON u.date = r.date AND u.keyword = r.keyword
    LEFT JOIN `dnd-trends-index.silver_data.norm_youtube` y
        ON u.date = y.date AND u.keyword = y.keyword
    LEFT JOIN `dnd-trends-index.dnd_trends_categorized.trend_data_pilot` g
        ON u.date = g.date AND u.keyword = g.search_term
    LEFT JOIN buy_signals b
        ON u.date = b.date AND u.keyword = b.keyword
    LEFT JOIN cf_signals cf
        ON u.date = cf.date AND u.keyword = cf.keyword
)
SELECT
    ds.date,
    ds.keyword,
    cat.category,
    COALESCE(score_wiki, 0) as norm_wiki,
    COALESCE(score_fandom, 0) as norm_fandom,
    COALESCE(score_roll20, 0) as norm_roll20,
    COALESCE(score_youtube, 0) as norm_youtube,
    COALESCE(score_google, 0) as norm_google,
    
    -- Hype Component (Wiki + Fandom + YouTube + Google)
    (
        (COALESCE(score_wiki, 0) + COALESCE(score_fandom, 0) + COALESCE(score_youtube, 0) + COALESCE(score_google, 0)) / 
        GREATEST(1, 
            IF(score_wiki IS NOT NULL, 1, 0) +
            IF(score_fandom IS NOT NULL, 1, 0) +
            IF(score_youtube IS NOT NULL, 1, 0) +
            IF(score_google IS NOT NULL, 1, 0)
        )
    ) as hype_score,
    
    -- Play Component (Roll20)
    COALESCE(score_roll20, 0) as play_score,
    
    -- Buy Component (Kickstarter/BackerKit + Amazon/BGG)
    (COALESCE(avg_cf, 0) * 0.5 + COALESCE(avg_buy, 0) * 0.5) as buy_score,

    -- Composite Trend Score (Weighted: 50% Hype, 30% Play, 20% Buy)
    (
        (
            (
                (COALESCE(score_wiki, 0) + COALESCE(score_fandom, 0) + COALESCE(score_youtube, 0) + COALESCE(score_google, 0)) / 
                GREATEST(1, 
                    IF(score_wiki IS NOT NULL, 1, 0) +
                    IF(score_fandom IS NOT NULL, 1, 0) +
                    IF(score_youtube IS NOT NULL, 1, 0) +
                    IF(score_google IS NOT NULL, 1, 0)
                )
            ) * 0.5
        ) + 
        (COALESCE(score_roll20, 0) * 0.3) +
        ((COALESCE(avg_cf, 0) * 0.5 + COALESCE(avg_buy, 0) * 0.5) * 0.2)
    ) * 100 as trend_score_raw
    
FROM daily_scores ds
LEFT JOIN (
    SELECT DISTINCT original_keyword, category 
    FROM `dnd-trends-index.dnd_trends_categorized.expanded_search_terms`
) cat ON ds.keyword = cat.original_keyword
WHERE ds.date IS NOT NULL;
