-- Phase: Enrichment Write-Back — add new columns to catalog_supply
-- Run each statement separately in BQ console

ALTER TABLE `dnd-trends-index.dnd_trends_raw.catalog_supply`
ADD COLUMN IF NOT EXISTS primary_category STRING;

ALTER TABLE `dnd-trends-index.dnd_trends_raw.catalog_supply`
ADD COLUMN IF NOT EXISTS setting STRING;

ALTER TABLE `dnd-trends-index.dnd_trends_raw.catalog_supply`
ADD COLUMN IF NOT EXISTS summary STRING;

ALTER TABLE `dnd-trends-index.dnd_trends_raw.catalog_supply`
ADD COLUMN IF NOT EXISTS product_url STRING;
