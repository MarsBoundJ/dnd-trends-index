from google.cloud import bigquery
import json

client = bigquery.Client()

def google_autopsy():
    print("Executing Google Autopsy...")
    sql = """
    SELECT date, search_term, interest 
    FROM `dnd-trends-index.dnd_trends_categorized.trend_data_pilot` 
    WHERE date BETWEEN '2026-02-01' AND '2026-02-06' 
    ORDER BY date DESC, search_term ASC 
    LIMIT 100
    """
    try:
        query_job = client.query(sql, location="US")
        results = query_job.to_dataframe()
        data = results.to_dict(orient="records")
        # Handle date serialization
        for row in data:
            if hasattr(row['date'], 'isoformat'):
                row['date'] = str(row['date'])
        
        with open("google_autopsy.json", "w") as f:
            json.dump(data, f, indent=4)
        print("Autopsy data written to google_autopsy.json")
    except Exception as e:
        print(f"Error in Google Autopsy: {e}")

if __name__ == "__main__":
    google_autopsy()
