from google.cloud import bigquery

client = bigquery.Client(project='dnd-trends-index', location='US')

# Fix central mapping view
fix_mapping_sql = """
CREATE OR REPLACE VIEW `dnd-trends-index.silver_data.view_google_mapping` AS
SELECT 
    term_id,
    original_keyword as raw_search_term,
    google_keyword,
    concept_name as canonical_concept,
    category
FROM `dnd-trends-index.dnd_trends_categorized.expanded_search_terms`
JOIN `dnd-trends-index.dnd_trends_categorized.concept_library` USING (concept_name)
"""

print("Fixing view_google_mapping...")
client.query(fix_mapping_sql).result()
print("Mapping view fixed successfully.")
