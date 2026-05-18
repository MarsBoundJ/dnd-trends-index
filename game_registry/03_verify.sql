-- =============================================================================
-- game_registry — verification + diagnostic JOIN (proof-of-method)
-- =============================================================================
-- Read-only. Run after 02_populate.sql. Documents the checks that validated
-- batch 1 on 2026-05-18, including the diagnostic reception JOIN whose result
-- is itself a pitch finding. See memory: project_rules_substrate_architecture.md
-- =============================================================================

-- (1) Per-class counts + grand total. Expect 4 per class, 48 total,
--     canon_true = cur2024 = not_srd = distinct_lineages = same as n.
SELECT IFNULL(parent_class,"== TOTAL ==") parent_class,
       COUNT(*) n,
       COUNTIF(is_current_canonical) canon_true,
       COUNTIF(lifecycle_status="current_2024") cur2024,
       COUNTIF(NOT is_srd) not_srd,
       COUNT(DISTINCT lineage_id) distinct_lineages
FROM `dnd-trends-index.game_registry.subclasses`
GROUP BY ROLLUP(parent_class)
ORDER BY (parent_class IS NULL), parent_class;

-- (2) Alias-bridge spot check: renamed subclasses must carry their pre-2024
--     names (the load-bearing reception JOIN keys).
SELECT canonical_name, ARRAY_TO_STRING(aliases," | ") alias_bridge
FROM `dnd-trends-index.game_registry.subclasses`
WHERE subclass_id IN ("barbarian_path_of_the_wild_heart","sorcerer_aberrant_sorcery",
                      "wizard_evoker","monk_warrior_of_shadow",
                      "warlock_great_old_one_patron","ranger_beast_master")
ORDER BY canonical_name;

-- (3) Vocabulary lexicon: 3 seed rows, all safe_for_deliverables = FALSE.
SELECT term, register, safe_for_deliverables, safe_translation
FROM `dnd-trends-index.game_registry.vocabulary_lexicon`
ORDER BY term;

-- (4) DIAGNOSTIC JOIN — proof-of-method.
--     2026-05-18 result: 48/48 match concept_library (registry binds cleanly to
--     the existing concept layer, zero name-matching debt at subclass grain);
--     0/48 match reddit_reception_proxy (that proxy is IP/franchise-keyed, NOT
--     subclass-keyed) -> empirically confirms subclass-level reception tagging
--     (feature-intelligence component #2) is genuinely net-new. The 0 is the
--     informative finding, not a failure.
WITH al AS (
  SELECT DISTINCT subclass_id, parent_class, canonical_name, LOWER(TRIM(a)) la
  FROM `dnd-trends-index.game_registry.subclasses`, UNNEST(aliases) a
),
cl AS (
  SELECT DISTINCT subclass_id, canonical_name
  FROM al
  JOIN `dnd-trends-index.dnd_trends_categorized.concept_library` c
    ON LOWER(TRIM(c.concept_name)) = al.la
),
rd AS (
  SELECT DISTINCT subclass_id, canonical_name
  FROM al
  JOIN `dnd-trends-index.gold_data.reddit_reception_proxy` r
    ON LOWER(TRIM(r.ip_name)) = al.la OR LOWER(TRIM(r.stream_name)) = al.la
)
SELECT
  (SELECT COUNT(*) FROM `dnd-trends-index.game_registry.subclasses`) total_subclasses,
  (SELECT COUNT(*) FROM cl) matched_concept_library,
  (SELECT COUNT(*) FROM rd) matched_reddit_reception,
  (SELECT STRING_AGG(canonical_name, ", " ORDER BY canonical_name) FROM cl) concept_hits;
