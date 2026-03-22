-- =============================================================================
-- Arcane Analytics — dnd-trends-index
-- Schema: concept_variations table + staging table for variant review pipeline
-- Run in BigQuery Console or via Antigravity before deploying variant pipeline.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- 1. concept_variations
--    Tracks all known search strings that map to a concept in concept_library.
--    Separate from concept_library to avoid DML rewrites on the master table.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `dnd-trends-index.dnd_trends_categorized.concept_variations`
(
    variation_id     STRING    NOT NULL,   -- MD5 of concept_id + variant_string
    concept_id       STRING    NOT NULL,   -- FK → concept_library.keyword (lowercase)
    variant_string   STRING    NOT NULL,   -- Actual search string e.g. "ranger dnd"
    is_best_variant  BOOL      NOT NULL,   -- TRUE = currently used for leaderboard scoring
    source           STRING    NOT NULL,   -- See source values below
    status           STRING    NOT NULL,   -- "active" | "deprecated"
    date_added       TIMESTAMP NOT NULL,
    added_by         STRING    NOT NULL,   -- "search_variant_generator" | "related_queries_discovery" | "manual" | "gemini_classifier"
    trend_score      INT64,                -- Last known Google Trends score for this variant (0-100)
    notes            STRING                -- Optional human annotation
)
PARTITION BY DATE(date_added)
CLUSTER BY concept_id, status
OPTIONS (
    description = "All known search string variants for each concept_library keyword"
);

-- Source values:
--   "search_variant_generator"    — produced by the existing variant testing system
--   "related_queries_discovery"   — surfaced by pytrends related_queries pipeline
--   "gemini_classifier"           — suggested by Gemini during variant resolution
--   "manual"                      — added directly by Phil


-- -----------------------------------------------------------------------------
-- 2. variant_staging
--    Holds Gemini classification decisions pending Claude review and Phil approval.
--    Nothing in this table touches concept_library or concept_variations until
--    Phil gives final sign-off.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `dnd-trends-index.dnd_trends_categorized.variant_staging`
(
    staging_id          STRING    NOT NULL,   -- MD5 of run_id + term
    run_id              STRING    NOT NULL,   -- FK → emerging_terms.run_id
    term                STRING    NOT NULL,   -- Candidate term from related queries
    seed_keyword        STRING    NOT NULL,   -- Seed that surfaced this term
    
    -- Fuzzy match pre-filter results
    fuzzy_match_concept STRING,               -- Best fuzzy match from concept_library (if any)
    fuzzy_match_score   FLOAT64,              -- Similarity score 0.0-1.0
    fuzzy_match_method  STRING,               -- "token_overlap" | "edit_distance" | "none"
    
    -- Gemini classification
    gemini_decision     STRING,               -- "variant" | "new_concept" | "noise"
    gemini_parent       STRING,               -- concept_library keyword if decision = "variant"
    gemini_category     STRING,               -- Suggested category if decision = "new_concept"
    gemini_confidence   STRING,               -- "high" | "medium" | "low"
    gemini_reasoning    STRING,               -- Gemini's explanation (for Claude review)
    
    -- Claude review
    claude_decision     STRING,               -- "agree" | "override"
    claude_override     STRING,               -- Claude's decision if overriding Gemini
    claude_notes        STRING,               -- Claude's reasoning for any override
    
    -- Final status
    review_status       STRING    NOT NULL,   -- "pending_claude" | "pending_phil" | "approved" | "rejected"
    phil_notes          STRING,               -- Phil's annotation at approval
    
    created_at          TIMESTAMP NOT NULL,
    updated_at          TIMESTAMP NOT NULL
)
PARTITION BY DATE(created_at)
CLUSTER BY review_status, gemini_decision
OPTIONS (
    description = "Staging area for variant resolution pipeline — Gemini decisions awaiting Claude review and Phil approval"
);
