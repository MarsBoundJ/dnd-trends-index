-- Oracle v2: Create Intelligence Silver Table
CREATE OR REPLACE TABLE `dnd-trends-index.dnd_trends_raw.yt_video_intelligence` (
    video_id STRING NOT NULL,
    processing_date DATE DEFAULT CURRENT_DATE(),
    concept_name STRING,
    category STRING,
    sentiment_label STRING, -- 'Positive', 'Negative', 'Mixed', 'Neutral'
    sentiment_score FLOAT64, -- Map Pos=1.0, Mix=0.0, Neg=-1.0
    verdict STRING, -- The one-sentence summary.
    context_quote STRING, -- The specific phrase driving the rating.
    reported_not_creator BOOL, -- TRUE if they are quoting Reddit/Community.
    confidence FLOAT64 -- 0.0 to 1.0.
);
