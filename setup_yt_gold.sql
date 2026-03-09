-- Oracle Deck: Create Consensus View (Gold Layer)
CREATE OR REPLACE VIEW `dnd-trends-index.gold_data.view_youtube_consensus` AS
SELECT 
    concept_name,
    category,
    COUNT(DISTINCT video_id) as creator_count,
    -- Average sentiment score (scaled to 0-100)
    AVG(sentiment_score) * 50 + 50 as consensus_score,
    -- Aggregate the best quotes
    ARRAY_AGG(STRUCT(verdict, context_quote, sentiment_label) LIMIT 3) as creator_takes
FROM `dnd-trends-index.dnd_trends_raw.yt_video_intelligence`
WHERE reported_not_creator = FALSE -- Only show true expert opinions
GROUP BY 1, 2;
