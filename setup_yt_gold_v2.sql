-- Oracle Deck: Upgrade Consensus View (Gold Layer)
CREATE OR REPLACE VIEW `dnd-trends-index.gold_data.view_youtube_consensus` AS
SELECT 
    concept_name,
    category,
    COUNT(DISTINCT video_id) as creator_count,
    -- Calculate Consensus: (Positive Hits / Total Hits)
    ROUND(SAFE_DIVIDE(COUNTIF(sentiment_label = 'Positive'), COUNT(*)) * 100) as consensus_score,
    -- Pick the most 'Confident' quotes to display
    ARRAY_AGG(STRUCT(verdict, context_quote, sentiment_label) ORDER BY confidence DESC LIMIT 2) as creator_takes
FROM `dnd-trends-index.dnd_trends_raw.yt_video_intelligence`
WHERE reported_not_creator = FALSE
GROUP BY 1, 2;
