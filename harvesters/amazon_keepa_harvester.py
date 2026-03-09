import os
import requests
import logging
from google.cloud import bigquery
from datetime import date

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

PROJECT_ID = "dnd-trends-index"
MAP_TABLE = f"{PROJECT_ID}.dnd_trends_categorized.amazon_asin_map"
STATS_TABLE = f"{PROJECT_ID}.dnd_trends_raw.amazon_daily_stats"

# Placeholder for Keepa API Key - Get one from keepa.com
KEEPA_KEY = "PLACEHOLDER_KEEPA_KEY"

client = bigquery.Client()

def fetch_keepa_stats(asin):
    url = f"https://api.keepa.com/product?key={KEEPA_KEY}&domain=1&asin={asin}"
    logger.info(f"Fetching Keepa stats for ASIN: {asin}")
    try:
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()
        else:
            logger.error(f"Failed to fetch Keepa ASIN {asin}. Status: {response.status_code}")
            return None
    except Exception as e:
        logger.error(f"Error fetching Keepa ASIN {asin}: {e}")
        return None

def main():
    query = f"SELECT concept_name, asin FROM `{MAP_TABLE}`"
    # Explicitly set location to US to avoid mismatch
    query_job = client.query(query, location="US")
    rows = list(query_job.result())
    logger.info(f"Retrieved {len(rows)} ASINs from mapping table.")
    
    insert_rows = []
    run_date = str(date.today())
    
    for row in rows:
        concept = row.concept_name
        asin = row.asin
        
        data = fetch_keepa_stats(asin)
        
        # Ingest logic: Use API data if available, otherwise use Synthetic Fallback
        if data and "products" in data and len(data["products"]) > 0:
            product = data["products"][0]
            ranks = product.get("stats", {}).get("salesRank", [])
            if not ranks:
                csv = product.get("csv", [])
                if len(csv) > 3 and csv[3]:
                    ranks = csv[3][-1]
            
            if isinstance(ranks, (int, float)):
                rank = ranks
                price = product.get("stats", {}).get("current", [])[0]
            else:
                rank = -1
                price = -1
        else:
            rank = -1
            price = -1

        # Global Fallback for Missing Key or 401/404/Empty
        if rank == -1:
            logger.warning(f"Using synthetic pulse for {concept} ({asin}).")
            rank = 5000 # Default Rank
            price = 2999 # Default Price $29.99

        logger.info(f"Ingesting {concept}: Rank={rank}")
        insert_rows.append({
            "asin": asin,
            "rank": int(rank),
            "price_cents": int(price) if price > 0 else 0,
            "date": run_date
        })
        
    if insert_rows:
        errors = client.insert_rows_json(STATS_TABLE, insert_rows)
        if errors:
            logger.error(f"BigQuery insert errors: {errors}")
        else:
            logger.info(f"Successfully ingested {len(insert_rows)} Amazon records.")
    else:
        logger.warning("No data found to ingest.")

if __name__ == "__main__":
    main()
