from google.cloud import bigquery
import json

client = bigquery.Client()

def google_autopsy():
    print("Executing Google Autopsy...")
    sql = """
    SELECT date, keyword, value 
    FROM `dnd-trends-index.dnd_trends_categorized.trend_data_pilot` 
    WHERE date BETWEEN '2026-02-01' AND '2026-02-06' 
    ORDER BY date DESC, keyword ASC 
    LIMIT 100
    """
    try:
        # Try without explicit location first, or set to US if known
        query_job = client.query(sql, location="US")
        results = query_job.to_dataframe()
        data = results.to_dict(orient="records")
        # Handle datetime serialization
        for row in data:
            if hasattr(row['date'], 'isoformat'):
                row['date'] = row['date'].isoformat()
        
        with open("google_autopsy.json", "w") as f:
            json.dump(data, f, indent=4)
        print("Autopsy data written to google_autopsy.json")
    except Exception as e:
        print(f"Error in Google Autopsy: {e}")

if __name__ == "__main__":
    google_autopsy()
