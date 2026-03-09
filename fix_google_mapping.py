from google.cloud import bigquery

client = bigquery.Client(project='dnd-trends-index', location='US')

# Fix central mapping view
fix_mapping_sql = """
CREATE OR REPLACE VIEW `dnd-trends-index.silver_data.view_google_mapping` AS
SELECT 
    t.term_id,
    t.original_keyword as raw_search_term,
    t.google_keyword,
    t.concept_name as canonical_concept,
    lib.category
FROM `dnd-trends-index.dnd_trends_categorized.expanded_search_terms` AS t
JOIN `dnd-trends-index.dnd_trends_categorized.concept_library` AS lib ON t.concept_name = lib.concept_name
"""

print("Fixing view_google_mapping...")
client.query(fix_mapping_sql).result()
print("Mapping view fixed.")
