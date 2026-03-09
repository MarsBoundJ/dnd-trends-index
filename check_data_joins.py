from google.cloud import bigquery

client = bigquery.Client()

print("--- expanded_search_terms (Fighter) ---")
q1 = "SELECT * FROM `dnd-trends-index.dnd_trends_categorized.expanded_search_terms` WHERE original_keyword LIKE '%Fighter%' LIMIT 2"
for row in client.query(q1):
    print(row)

print("\n--- concept_library (Fighter) ---")
q2 = "SELECT * FROM `dnd-trends-index.dnd_trends_categorized.concept_library` WHERE concept_name LIKE '%Fighter%' LIMIT 2"
for row in client.query(q2):
    print(row)

print("\n--- trend_data_pilot SAMPLE ---")
q3 = "SELECT term_id, search_term, date, interest FROM `dnd-trends-index.dnd_trends_categorized.trend_data_pilot` LIMIT 5"
for row in client.query(q3):
    print(row)
