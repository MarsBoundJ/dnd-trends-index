from google.cloud import bigquery
import json

client = bigquery.Client()

def library_audit():
    print("--- Library Integrity Audit ---")
    sql = "SELECT is_active, COUNT(*) as count FROM `dnd-trends-index.dnd_trends_categorized.concept_library` GROUP BY 1"
    try:
        query_job = client.query(sql, location="US")
        results = query_job.to_dataframe()
        print(results.to_string(index=False))
        total = results['count'].sum()
        print(f"Total Keywords: {total}")
    except Exception as e:
        print(f"Error in Library Audit: {e}")

def google_autopsy():
    print("\n--- Google Trends Data Autopsy ---")
    sql = """
    SELECT date, keyword, value 
    FROM `dnd-trends-index.dnd_trends_raw.trend_data_pilot` 
    WHERE date BETWEEN '2026-02-01' AND '2026-02-06' 
    ORDER BY date DESC, keyword ASC 
    LIMIT 100
    """
    try:
        query_job = client.query(sql, location="US")
        results = query_job.to_dataframe()
        print(results.to_string(index=False))
    except Exception as e:
        print(f"Error in Google Autopsy: {e}")

if __name__ == "__main__":
    library_audit()
    google_autopsy()
