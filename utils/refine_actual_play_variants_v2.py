import os
import uuid
import datetime
from google.cloud import bigquery

if not os.getenv("GOOGLE_APPLICATION_CREDENTIALS") and os.path.exists("/workspaces/dnd-trends/dnd-key.json"):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/workspaces/dnd-trends/dnd-key.json"

client = bigquery.Client(project="dnd-trends-index")

DATASET_ID = "dnd_trends_categorized"
EXPANDED_TABLE = f"{DATASET_ID}.expanded_search_terms"

def main():
    print("Surgical Insertion: Actual Play Variants V2")
    
    new_terms = [
        {
            "original_keyword": "Legends of Avantris: Surpasa",
            "search_term": "Legends of Avantris Surpasa",
            "category": "Actual Play"
        }
    ]
    
    created_at = datetime.datetime.now().isoformat()
    rows_to_insert = []
    
    for t in new_terms:
        rows_to_insert.append({
            "term_id": str(uuid.uuid4()),
            "original_keyword": t["original_keyword"],
            "category": t["category"],
            "search_term": t["search_term"],
            "expansion_rule": "manual_variant_refinement_v2",
            "created_at": created_at,
            "is_pilot": True
        })

    print(f"Inserting {len(rows_to_insert)} new variant...")
    errors = client.insert_rows_json(EXPANDED_TABLE, rows_to_insert)
    if not errors:
        print("Successfully inserted variant.")
    else:
        print(f"BQ Insertion Errors: {errors}")

if __name__ == "__main__":
    main()
