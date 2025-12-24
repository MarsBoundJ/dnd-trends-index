import csv
from google.cloud import bigquery

OUTPUT_FILE = 'batch1_summary.csv'

def generate_report():
    client = bigquery.Client()
    
    print("Aggregating Batch 1 Data (Backgrounds & Feats)...")
    
    # Query to sum interest and get metadata
    query = """
        SELECT 
            e.category,
            e.search_term,
            e.is_official,
            SUM(t.interest) as total_interest,
            COUNT(*) as weeks_of_data
        FROM `dnd-trends-index.dnd_trends_categorized.trend_data_pilot` t
        JOIN `dnd-trends-index.dnd_trends_categorized.expanded_search_terms` e
        ON t.search_term = e.search_term
        WHERE e.category IN ('Background', 'Feat')
        GROUP BY 1, 2, 3
        ORDER BY 1, 4 DESC
    """
    
    results = client.query(query).result()
    
    rows = []
    for row in results:
        rows.append({
            "Category": row.category,
            "Search Term": row.search_term,
            "Is Official": row.is_official,
            "Total Interest": row.total_interest,
            "Data Points": row.weeks_of_data
        })
        
    print(f"Fetched {len(rows)} rows.")
    
    # Write to CSV
    keys = ["Category", "Search Term", "Is Official", "Total Interest", "Data Points"]
    with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(rows)
        
    print(f"Report saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    generate_report()
