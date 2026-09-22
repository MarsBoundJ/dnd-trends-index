-- catalog_detail — per-product facts from the storefront API
--
-- Run in the BigQuery console. This is a CREATE TABLE IF NOT EXISTS, so it is
-- safe to re-run; it does not touch catalog_supply.
--
-- WHAT THIS IS FOR. catalog_supply answers "what is selling" — a weekly shelf
-- snapshot of title, price and medal tier. It cannot answer "what IS it": how
-- long, by whom, for which system, at what level, in what format. Those come
-- from the product API, one call per product, and land here.
--
-- One row per (collected_date, source, product_id). Re-harvesting a product on
-- a later date adds a row rather than replacing one, so a price change, a tier
-- promotion or a page-count revision is visible as a series. Gold views should
-- take the latest row per product unless they are deliberately looking at
-- movement.
--
-- NULL MEANS UNMEASURED, AND THAT DISTINCTION IS LOAD-BEARING. The harvester
-- writes null rather than zero or "" for anything absent, because this schema's
-- whole value is telling "27 pages" apart from "we do not know". Several of
-- these columns exist specifically because a zero lied:
--
--   page_count      — the API returns "104" on one store and 27 on the other.
--   file_size_bytes — the API's own top-level filesize read 0 while the real
--                     size sat in files[0].size.
--   rating_count    — the sum of the star buckets, which is NOT review_count.
--                     Measured: 94 ratings against 28 written reviews on one
--                     product, 12 against 6 on another. Using review_count as
--                     the denominator of an average is wrong by 3x.
--   is_pwyw         — a pay-what-you-want price is what the creator ASKS, not
--                     what buyers paid. Any revenue proxy of price x units is
--                     wrong for these rows in an unknown direction.

CREATE TABLE IF NOT EXISTS `dnd-trends-index.dnd_trends_raw.catalog_detail` (
  collected_date          DATE      NOT NULL,
  source                  STRING    NOT NULL,   -- 'DMs Guild' | 'DriveThruRPG'
  product_id              INT64     NOT NULL,   -- the join key; rows without one are rejected at ingest
  title                   STRING,

  -- Provenance. is_community_content is the line between indie creators and
  -- WotC back-catalogue; segment on it before any claim about "what creators
  -- are selling".
  publisher_id            INT64,
  publisher_name          STRING,
  is_community_content    BOOL,
  community_author_id     INT64,
  community_author_alias  STRING,
  creator_level_id        INT64,                -- DMs Guild only: the platform's own creator grade

  -- Physical
  page_count              INT64,
  file_name               STRING,
  file_size_bytes         INT64,
  scanned_pdf             BOOL,
  watermarked             BOOL,
  is_bundle               BOOL,

  -- Commercial
  price                   FLOAT64,
  special_price           FLOAT64,
  lowest_digital_price    FLOAT64,
  lowest_print_price      FLOAT64,
  on_sale                 BOOL,
  is_pwyw                 BOOL,

  -- Reception. Keep rating_count and review_count apart; they are different
  -- denominators and conflating them silently rescales every average.
  rating                  FLOAT64,
  rating_count            INT64,                -- sum of the star buckets
  review_count            INT64,                -- written reviews
  rating_1                INT64,
  rating_2                INT64,
  rating_3                INT64,
  rating_4                INT64,
  rating_5                INT64,

  -- The medal tier, straight from the API rather than read off a shelf
  -- heading. Present on every product, including ones no bestseller shelf
  -- lists — which the shelf harvest can never see.
  ranking_tier            STRING,

  -- Dates. date_available plus a weekly tier series is what makes time-to-tier
  -- computable: how long a product takes to reach Adamantine.
  date_created            STRING,
  date_available          STRING,
  date_modified           STRING,
  file_last_modified      STRING,

  -- Credited individuals, not just the publishing entity.
  authors                 ARRAY<STRING>,
  artists                 ARRAY<STRING>,
  editors                 ARRAY<STRING>,
  contributors            ARRAY<STRING>,

  -- Seller-declared, so these measure DISCLOSURE, not incidence.
  ai_disclosed            BOOL,
  handmade                BOOL,

  sku                     STRING,
  isbn                    STRING,

  -- The storefront's own facet tags, with the axis each one belongs to.
  -- axis is NULL when a filter could not be placed under a known root: a real
  -- tag we cannot categorise, never a guess. Query WHERE axis IS NOT NULL when
  -- grouping, and treat the nulls as coverage to investigate.
  filters ARRAY<STRUCT<
    filter_id  INT64,
    parent_id  INT64,
    axis       STRING,
    label      STRING,
    depth      INT64
  >>,

  -- Stamped so rows classified under different vocabularies never blend
  -- silently. Storefronts add and retire facets; without this, a category's
  -- apparent growth may just be the week it was introduced.
  taxonomy_version        STRING,
  harvester_version       STRING
)
PARTITION BY collected_date
CLUSTER BY source, product_id;
