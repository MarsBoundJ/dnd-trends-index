SELECT 
    concept_name, 
    category, 
    sentiment_label, 
    sentiment_score, 
    verdict, 
    context_quote, 
    reported_not_creator 
FROM `dnd-trends-index.dnd_trends_raw.yt_video_intelligence` 
LIMIT 5;
