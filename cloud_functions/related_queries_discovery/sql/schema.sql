-- =============================================================================
-- Arcane Analytics — dnd-trends-index
-- Schema: dnd_trends_raw (new dataset for raw ingestion)
-- Run these once in BigQuery Console before first Cloud Function invocation.
-- The Cloud Function also calls create_table(exists_ok=True) at runtime,
-- so these are primarily for documentation / manual pre-provisioning.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- 1. Raw related queries / topics from pytrends
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `dnd-trends-index.dnd_trends_raw.related_queries`
(
    run_id       STRING    NOT NULL,   -- 16-char MD5 run identifier
    term_id      STRING    NOT NULL,   -- MD5 of seed|term|data_type|result_type
    seed_keyword STRING    NOT NULL,   -- The keyword sent to pytrends
    term         STRING    NOT NULL,   -- Related query or topic title returned
    data_type    STRING    NOT NULL,   -- "query" | "topic"
    result_type  STRING    NOT NULL,   -- "top"   | "rising"
    topic_type   STRING,               -- e.g. "Role-playing game" (topics only)
    value        INT64,                -- Relative interest (0-100) or % rise
    is_breakout  BOOL      NOT NULL,   -- TRUE when value = "Breakout" (>+200%)
    geo          STRING    NOT NULL,   -- "US" or "WW"
    timeframe    STRING    NOT NULL,   -- pytrends timeframe string
    fetched_at   TIMESTAMP NOT NULL    -- UTC fetch time (partition key)
)
PARTITION BY DATE(fetched_at)
CLUSTER BY seed_keyword, result_type, data_type
OPTIONS (
    description = "Raw pytrends related_queries() and related_topics() output",
    partition_expiration_days = 365
);


-- ---------------------------------------------------------------------------
-- 2. Emerging terms flagged for review (Rising + not in concept_library)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `dnd-trends-index.dnd_trends_raw.emerging_terms`
(
    run_id        STRING    NOT NULL,
    term_id       STRING    NOT NULL,   -- FK → related_queries.term_id
    term          STRING    NOT NULL,
    seed_keyword  STRING    NOT NULL,
    data_type     STRING    NOT NULL,   -- "query" | "topic"
    value         INT64,
    is_breakout   BOOL      NOT NULL,
    review_status STRING    NOT NULL,   -- PENDING | ACCEPTED | REJECTED
    flagged_at    TIMESTAMP NOT NULL    -- partition key
)
PARTITION BY DATE(flagged_at)
CLUSTER BY review_status
OPTIONS (
    description = "Rising terms not present in concept_library, queued for human review"
);


-- =============================================================================
-- Useful analysis queries
-- =============================================================================

-- Latest run: all Rising terms, breakouts first
-- SELECT
--     seed_keyword,
--     term,
--     data_type,
--     value,
--     is_breakout,
--     fetched_at
-- FROM `dnd-trends-index.dnd_trends_raw.related_queries`
-- WHERE result_type = 'rising'
--   AND DATE(fetched_at) = CURRENT_DATE()
-- ORDER BY is_breakout DESC, value DESC;


-- Pending review queue
-- SELECT
--     term,
--     seed_keyword,
--     data_type,
--     value,
--     is_breakout,
--     flagged_at
-- FROM `dnd-trends-index.dnd_trends_raw.emerging_terms`
-- WHERE review_status = 'PENDING'
-- ORDER BY is_breakout DESC, value DESC;


-- Term velocity: how many runs has an emerging term appeared in?
-- SELECT
--     term,
--     COUNT(DISTINCT run_id)  AS run_appearances,
--     MAX(value)              AS peak_value,
--     LOGICAL_OR(is_breakout) AS ever_breakout
-- FROM `dnd-trends-index.dnd_trends_raw.emerging_terms`
-- GROUP BY term
-- ORDER BY run_appearances DESC, peak_value DESC;
