¨from google.cloud import bigquery
import csv

PURGE_FILE = 'purge_list.csv'
TABLE_ID = 'dnd-trends-index.dnd_trends_categorized.trend_data_pilot'

def purge_bq():
    client = bigquery.Client()
    
    terms_to_delete = []
    with open(PURGE_FILE, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            terms_to_delete.append(row['search_term'])
            
    if not terms_to_delete:
        print("No terms to delete.")
        return

    print(f"Purging {len(terms_to_delete)} terms from {TABLE_ID}...")
    
    # Construct query (batching if necessary, but 54 is small)
    # Using parameterized query or just formatting strictly
    formatted_terms = ", ".join([f"'{t}'" for t in terms_to_delete])
    
    query = f"""
        DELETE FROM `{TABLE_ID}`
        WHERE search_term IN ({formatted_terms})
    """
    
    query_job = client.query(query)
    query_job.result() # Wait for job
    
    print("Purge Successful.")

if __name__ == "__main__":
    purge_bq()
¨*cascade08"(5ea7c1b525e148ee9cb36b57c32852aa3816ac0220file:///C:/Users/Yorri/.gemini/purge_pilot_bq.py:file:///C:/Users/Yorri/.gemini