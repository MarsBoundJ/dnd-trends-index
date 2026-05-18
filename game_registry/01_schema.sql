-- =============================================================================
-- game_registry — schema (authoritative, final state)
-- =============================================================================
-- Trusight feature-intelligence component #1: the subclass spine.
-- Provenance + taxonomy + reception JOIN keys. Facts/metadata ONLY — no
-- copyrighted mechanical text (legal firewall). Re-runnable: IF NOT EXISTS.
--
-- Run order: 01_schema.sql  ->  02_populate.sql
-- Project/dataset: dnd-trends-index.game_registry
-- See memory: project_rules_substrate_architecture.md
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS `dnd-trends-index.game_registry`
OPTIONS(description="Trusight game-content registry. Provenance + taxonomy + reception JOIN keys. Facts/metadata only; legal + epistemic + vocabulary firewalls apply. See memory: project_rules_substrate_architecture.md");

CREATE TABLE IF NOT EXISTS `dnd-trends-index.game_registry.subclasses`
(
  subclass_id STRING OPTIONS(description="Stable surrogate key, e.g. bard_college_of_lore"),
  canonical_name STRING OPTIONS(description="Display name, e.g. College of Lore"),
  aliases ARRAY<STRING> OPTIONS(description="JOIN KEYS to gold_data.*_proxy / dnd_trends_categorized.concept_library. Maintain per ub_ip_alias_library alias discipline. Load-bearing: registry is worthless without clean joins."),
  parent_class STRING OPTIONS(description="Parent class, e.g. Bard"),
  source_book STRING OPTIONS(description="Full source book title, e.g. Tasha Cauldron of Everything"),
  source_book_short STRING OPTIONS(description="Short code: PHB2014 / PHB2024 / XGE / TCoE / etc."),
  edition STRING OPTIONS(description="5e-2014 | 5e-2024. Legacy editions only if/when resonance work extends."),
  publication_year INT64 OPTIONS(description="e.g. 2014, 2020, 2024"),
  is_srd BOOL OPTIONS(description="LEGAL FLAG: is this subclass in the CC-BY SRD? Governs whether license-safe mechanical text may ever be stored. Non-SRD = facts/provenance/commentary only."),
  power_source_axis STRING OPTIONS(description="INTERNAL-EVAL-ONLY. 4e-derived (Arcane/Divine/Primal/Martial/Psionic/Shadow). Analytical/JOIN use only. NEVER surfaced to WoTC un-translated; check game_registry.vocabulary_lexicon."),
  role_axis STRING OPTIONS(description="INTERNAL-EVAL-ONLY. 4e/optimizer (Striker/Defender/Controller/Leader + folksonomy). NEVER surfaced un-translated; check vocabulary_lexicon."),
  fantasy_flavor_tags ARRAY<STRING> OPTIONS(description="The invented axis (cadre-confirmed differentiator): gothic / wuxia / eldritch-horror / cottagecore / etc."),
  design_history_note STRING OPTIONS(description="Freeform: errata, reworks, retirements, edition migration notes"),
  last_verified DATE OPTIONS(description="Provenance hygiene: when this row facts were last checked against source. The stale-CONTEXT.md lesson, made structural."),
  is_current_canonical BOOL OPTIONS(description="TRUE for the 48 2024-PHB subclasses (deliverable-facing canonical layer). Legacy/superseded rows = FALSE."),
  lineage_id STRING OPTIONS(description="Groups all named versions of one subclass concept across books/editions (e.g. barb_wildheart groups 2014 Path of the Totem Warrior + 2024 Path of the Wild Heart). Reception JOINs union the alias sets across a lineage; resonance/history cites it."),
  lifecycle_status STRING OPTIONS(description="current_2024 | superseded_renamed | superseded_redesigned_same_name | legacy_compatible_not_in_2024")
)
OPTIONS(
  description="Trusight feature-intelligence component #1 (subclass spine). Provenance + taxonomy + reception JOIN keys. Facts/metadata only, NO copyrighted mechanical text (legal firewall). Build classes/subclasses first; monsters/spells deferred. See memory: project_rules_substrate_architecture.md"
);

CREATE TABLE IF NOT EXISTS `dnd-trends-index.game_registry.vocabulary_lexicon`
(
  term STRING OPTIONS(description="A label/term that may appear in analysis or deliverables"),
  register STRING OPTIONS(description="official_2024 | official_retired | optimizer_jargon | 4e_edition | community_folksonomy"),
  safe_for_deliverables BOOL OPTIONS(description="FALSE = internal-eval only; must translate before surfacing to WoTC"),
  safe_translation STRING OPTIONS(description="WoTC-safe phrasing to substitute when safe_for_deliverables is FALSE"),
  history_note STRING OPTIONS(description="Why this register: retirement history / community reception of the term itself")
)
OPTIONS(
  description="Vocabulary firewall (4th firewall layer). Makes the internal-vs-deliverable vocabulary discipline checkable, not advisory. Seed row: Arcane/Divine/Primal = official_retired (One D&D 2022 UA spell-list framework, reverted UA7 Sept 2023, Crawford/class-identity, fan-rejected). See memory: project_rules_substrate_architecture.md"
);
