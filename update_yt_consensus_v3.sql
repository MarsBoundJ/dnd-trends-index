-- Oracle Deck: Upgrade Consensus View (Gold Layer) - FIXED V3
CREATE OR REPLACE VIEW `dnd-trends-index.gold_data.view_youtube_consensus` AS
WITH joined_intelligence AS (
    SELECT 
        i.*,
        COALESCE(c.concept_name, i.concept_name) as canonical_name,
        COALESCE(c.category, i.category) as canonical_category
    FROM `dnd-trends-index.dnd_trends_raw.yt_video_intelligence` i
    LEFT JOIN `dnd-trends-index.dnd_trends_categorized.concept_library` c ON i.concept_name = c.concept_name
)
SELECT 
    canonical_name as concept_name,
    canonical_category as category,
    COUNT(DISTINCT video_id) as creator_count,
    -- Calculate Consensus: (Positive Hits / Total Hits)
    ROUND(SAFE_DIVIDE(COUNTIF(sentiment_label = 'Positive'), COUNT(*)) * 100) as consensus_score,
    -- Pick the most 'Confident' quotes to display
    ARRAY_AGG(STRUCT(verdict, context_quote, sentiment_label) ORDER BY confidence DESC LIMIT 3) as creator_takes
FROM joined_intelligence
WHERE reported_not_creator = FALSE
GROUP BY 1, 2;
