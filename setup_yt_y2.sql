-- Phase Y2: Create Expert Analysis Table
CREATE OR REPLACE TABLE `dnd-trends-index.dnd_trends_raw.yt_expert_analysis` (
    video_id STRING NOT NULL,
    analysis_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    tactical_score FLOAT64, -- -1.0 (Nerf/Bad Design) to 1.0 (Buff/Great Design)
    narrative_score FLOAT64, -- -1.0 (Hostile/Negative) to 1.0 (Hype/Positive)
    drama_level INT64, -- 0 (None) to 5 (Viral Controversy)
    summary STRING, 
    tactical_signals STRING, -- JSON string or STRING for highlights
    narrative_signals STRING, -- JSON string or STRING for highlights
    extracted_concepts ARRAY<STRING>
);
