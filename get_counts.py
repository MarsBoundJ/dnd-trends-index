from google.cloud import bigquery
import json

client = bigquery.Client()

def get_counts():
    report = {}
    tables = [
        ("wiki_social", "dnd-trends-index.social_data.wikipedia_article_registry"),
        ("wiki_categorized", "dnd-trends-index.dnd_trends_categorized.wikipedia_article_registry")
    ]
    for key, table in tables:
        try:
            query = f"SELECT count(*) as count FROM `{table}`"
            df = client.query(query, location="US").to_dataframe()
            report[key] = int(df['count'][0])
        except Exception as e:
            report[key] = f"Error: {e}"
    return report

if __name__ == "__main__":
    results = get_counts()
    with open("diag_counts.json", "w") as f:
        json.dump(results, f)
    print("Counts written to diag_counts.json")
