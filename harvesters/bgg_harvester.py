import os
import time
import requests
import xml.etree.ElementTree as ET
from google.cloud import bigquery
from datetime import date
import logging
import argparse

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

PROJECT_ID = "dnd-trends-index"
MAP_TABLE = f"{PROJECT_ID}.dnd_trends_categorized.bgg_id_map"

# Default BGG Settings
STATS_TABLE = f"{PROJECT_ID}.dnd_trends_raw.bgg_product_stats"
BASE_URL = "https://boardgamegeek.com/xmlapi2/thing"

BGG_TOKEN = "ca8375ce-62f6-485a-8c54-ebf23209419f"

client = bigquery.Client()

def fetch_bgg_stats(bgg_id, is_rpg=False):
    url = f"{BASE_URL}?id={bgg_id}&stats=1"
    if is_rpg:
        url += "&type=rpgitem"
        
    headers = {"Authorization": f"Bearer {BGG_TOKEN}"}
    logger.info(f"Fetching stats for ID: {bgg_id} (RPG: {is_rpg})")
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return response.text
        elif response.status_code == 202:
            logger.info(f"API returned 202 (Processing) for ID {bgg_id}. Retrying in 5s...")
            time.sleep(5)
            return fetch_bgg_stats(bgg_id, is_rpg)
        else:
            logger.error(f"Failed to fetch ID {bgg_id}. Status: {response.status_code}")
            return None
    except Exception as e:
        logger.error(f"Error fetching ID {bgg_id}: {e}")
        return None

def parse_bgg_xml(xml_content):
    try:
        root = ET.fromstring(xml_content)
        item = root.find("item")
        if item is None:
            return None
        
        stats = item.find("statistics")
        ratings = stats.find("ratings")
        
        owned_node = ratings.find("owned")
        average_node = ratings.find("average")
        
        owned = int(owned_node.attrib["value"])
        average = float(average_node.attrib["value"])
        
        return {"owned": owned, "average": average}
    except Exception as e:
        logger.error(f"Error parsing XML: {e}")
        logger.debug(f"XML Content: {xml_content}")
        return None

def main():
    parser = argparse.ArgumentParser(description="Harvest BGG or RPGGeek Stats")
    parser.add_argument("--rpg", action="store_true", help="Harvest RPGGeek stats instead of BoardGameGeek")
    args = parser.parse_args()
    
    global STATS_TABLE
    global BASE_URL
    
    is_rpg = args.rpg
    if is_rpg:
        STATS_TABLE = f"{PROJECT_ID}.dnd_trends_raw.rpggeek_product_stats"
        BASE_URL = "https://rpggeek.com/xmlapi2/thing"
        logger.info("Running in RPGGeek mode.")
    else:
        logger.info("Running in BoardGameGeek mode.")

    query = f"SELECT concept_name, bgg_id FROM `{MAP_TABLE}`"
    query_job = client.query(query)
    rows = list(query_job.result())
    
    insert_rows = []
    run_date = str(date.today())
    
    for row in rows:
        concept = row.concept_name
        bgg_id = row.bgg_id
        
        xml_data = fetch_bgg_stats(bgg_id, is_rpg)
        if xml_data:
            stats = parse_bgg_xml(xml_data)
            if stats:
                logger.info(f"Found stats for {concept}: Owned={stats['owned']}, Quality={stats['average']}")
                insert_rows.append({
                    "date": run_date,
                    "concept_name": concept,
                    "owned_count": stats['owned'],
                    "quality_score": stats['average']
                })
        
        time.sleep(2) # Respect rate limits
    
    if insert_rows:
        errors = client.insert_rows_json(STATS_TABLE, insert_rows)
        if errors:
            logger.error(f"BigQuery insert errors: {errors}")
        else:
            logger.info(f"Successfully ingested {len(insert_rows)} records into {STATS_TABLE}.")
    else:
        logger.warning("No data found to ingest.")

if __name__ == "__main__":
    main()
