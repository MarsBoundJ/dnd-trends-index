from google.cloud import bigquery
import json

def GetCategories():
    client = bigquery.Client()
    sql = """
        SELECT category, count(*) as count 
        FROM `dnd-trends-index.dnd_trends_categorized.concept_library` 
        GROUP BY category 
        ORDER BY count DESC
    """
    try:
        query_job = client.query(sql)
        results = [dict(row) for row in query_job.result()]
        print(json.dumps(results, indent=2))
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    GetCategories()
