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
    print("Surgical Refinement: Actual Play Variants")
    
    # 1. Add requested and validated variants
    new_terms = [
        {
            "original_keyword": "Legends of Avantris: Surpasa",
            "search_term": "Legends of Avantris Surpasa", # No colon variant
            "category": "Actual Play"
        },
        {
            "original_keyword": "Titansgrave: The Ashes of Valkana",
            "search_term": "Titansgrave",
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

    if rows_to_insert:
        print(f"Inserting {len(rows_to_insert)} new variants...")
        client.insert_rows_json(EXPANDED_TABLE, rows_to_insert)

    # 2. Cleanup/Deactivate the 0-score "Avantris Surpasa" variant
    # We use UPDATE instead of DELETE because of streaming buffer
    print("Deactivating low-fidelity variants...")
    deactivate_query = f"""
        UPDATE `{EXPANDED_TABLE}`
        SET is_pilot = FALSE
        WHERE search_term IN ('Avantris Surpasa', 'Legends of Avantris: Surpasa Dnd', 'Legends of Avantris: Surpasa 5e')
    """
    client.query(deactivate_query).result()
    
    # We should also handle Titansgrave variants if they drag it down
    # But Titansgrave (standalone) is 1.89, which is the baseline.

    print("Successfully refined variants.")

if __name__ == "__main__":
    main()
