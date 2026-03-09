import functions_framework
import json
from google.cloud import bigquery
import requests
import xml.etree.ElementTree as ET
from datetime import date
import time
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PROJECT_ID = "dnd-trends-index"
MAP_TABLE = f"{PROJECT_ID}.dnd_trends_categorized.bgg_id_map"
BGG_TOKEN = "ca8375ce-62f6-485a-8c54-ebf23209419f"

def fetch_bgg_stats(bgg_id, is_rpg, base_url):
    url = f"{base_url}?id={bgg_id}&stats=1"
    if is_rpg:
        url += "&type=rpgitem"
    headers = {"Authorization": f"Bearer {BGG_TOKEN}"}
    try:
        r = requests.get(url, headers=headers, timeout=30)
        if r.status_code == 200: return r.text
        if r.status_code == 202:
            logger.info(f"API 202 for {bgg_id}. Sleeping 5s.")
            time.sleep(5)
            return fetch_bgg_stats(bgg_id, is_rpg, base_url)
        logger.error(f"Error {r.status_code} for {bgg_id}")
        return None
    except Exception as e:
        logger.error(f"Fetch failed for {bgg_id}: {e}")
        return None

def parse_bgg_xml(xml_content):
    try:
        root = ET.fromstring(xml_content)
        item = root.find("item")
        if item is None: return None
        stats_node = item.find("statistics")
        if stats_node is None: return None
        ratings = stats_node.find("ratings")
        if ratings is None: return None
        
        owned_node = ratings.find("owned")
        avg_node = ratings.find("average")
        
        owned = int(owned_node.attrib["value"]) if owned_node is not None else 0
        average = float(avg_node.attrib["value"]) if avg_node is not None else 0.0
        return {"owned": owned, "average": average}
    except Exception as e:
        logger.error(f"Parse error: {e}")
        return None

@functions_framework.http
def bgg_harvester_http(request):
    """
    HTTP entry point for BGG/RPGGeek Harvester.
    Accepts JSON: {"rpg": true/false}
    """
    data = request.get_json(silent=True) or {}
    is_rpg = data.get("rpg", False)
    
    logger.info(f"🚀 Starting BGG Harvester (RPG: {is_rpg})")
    
    client = bigquery.Client()
    if is_rpg:
        stats_table = f"{PROJECT_ID}.dnd_trends_raw.rpggeek_product_stats"
        base_url = "https://rpggeek.com/xmlapi2/thing"
    else:
        stats_table = f"{PROJECT_ID}.dnd_trends_raw.bgg_product_stats"
        base_url = "https://boardgamegeek.com/xmlapi2/thing"

    query = f"SELECT concept_name, bgg_id FROM `{MAP_TABLE}`"
    rows = list(client.query(query).result())
    logger.info(f"Processing {len(rows)} IDs")
    
    insert_rows = []
    run_date = str(date.today())
    
    for row in rows:
        xml = fetch_bgg_stats(row.bgg_id, is_rpg, base_url)
        if xml:
            stats = parse_bgg_xml(xml)
            if stats:
                insert_rows.append({
                    "date": run_date,
                    "concept_name": row.concept_name,
                    "owned_count": stats['owned'],
                    "quality_score": stats['average']
                })
        time.sleep(2) # Respect rate limits
    
    result = {"status": "success", "rows_processed": len(rows), "rows_inserted": 0}
    if insert_rows:
        errors = client.insert_rows_json(stats_table, insert_rows)
        if not errors:
            result["rows_inserted"] = len(insert_rows)
        else:
            result["status"] = "partial_error"
            result["errors"] = errors
            logger.error(f"BQ Errors: {errors}")
            
    return json.dumps(result), 200
