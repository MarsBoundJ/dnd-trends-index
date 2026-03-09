
import os
import uuid
import datetime
from google.cloud import bigquery

if not os.getenv("GOOGLE_APPLICATION_CREDENTIALS") and os.path.exists("/workspaces/dnd-trends/dnd-key.json"):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/workspaces/dnd-trends/dnd-key.json"

client = bigquery.Client(project="dnd-trends-index")

DATASET_ID = "dnd_trends_categorized"
SOURCE_TABLE = f"{DATASET_ID}.concept_library"
DEST_TABLE = f"{DATASET_ID}.expanded_search_terms"

def main():
    print("Surgical Refinement: Background & Build Categories")
    
    # 1. Identify NOISY terms to delete
    # These are terms in Background or Build that LACK 5e, dnd, or 2024
    noisy_query = f"""
        SELECT term_id, search_term, category
        FROM `{DEST_TABLE}`
        WHERE category IN ('Background', 'Build')
        AND NOT REGEXP_CONTAINS(search_term, '(?i)(5e|Dnd|2024|5\\\\.5)')
    """
    noisy_terms = list(client.query(noisy_query).result())
    print(f"Found {len(noisy_terms)} noisy terms to remove.")

    if noisy_terms:
        # BQ DELETE can be slow or blocked by streaming buffer. 
        # But these categories are likely not in the buffer right now.
        term_ids = [f"'{r['term_id']}'" for r in noisy_terms]
        delete_query = f"DELETE FROM `{DEST_TABLE}` WHERE term_id IN ({','.join(term_ids)})"
        try:
            client.query(delete_query).result()
            print(f"Successfully deleted {len(noisy_terms)} terms.")
        except Exception as e:
            print(f"Delete failed (likely streaming buffer): {e}")

    # 2. Add MISSING qualified terms
    print("Fetching active concepts...")
    concepts_query = f"""
        SELECT concept_name, category 
        FROM `{SOURCE_TABLE}`
        WHERE category IN ('Background', 'Build') AND is_active = TRUE
    """
    concepts = list(client.query(concepts_query).result())
    
    # Check existing
    existing_query = f"SELECT original_keyword, search_term FROM `{DEST_TABLE}` WHERE category IN ('Background', 'Build')"
    existing_rows = list(client.query(existing_query).result())
    existing_map = {(row['original_keyword'], row['search_term']) for row in existing_rows}

    new_rows = []
    created_at = datetime.datetime.now().isoformat()
    
    for concept in concepts:
        name = concept['concept_name']
        cat = concept['category']
        
        # Determine target variations based on new rules
        if cat == 'Background':
            targets = [f"{name} 5e", f"{name} 2024", f"{name} Dnd"]
        else: # Build
            targets = [f"{name} 5e build", f"{name} Dnd build", f"{name} 5e"]
            
        for t in targets:
            if (name, t) not in existing_map:
                new_rows.append({
                    "term_id": str(uuid.uuid4()),
                    "original_keyword": name,
                    "category": cat,
                    "search_term": t,
                    "expansion_rule": "refinement_recovery",
                    "created_at": created_at,
                    "is_pilot": True
                })

    if new_rows:
        print(f"Inserting {len(new_rows)} new qualified search terms...")
        errors = client.insert_rows_json(DEST_TABLE, new_rows)
        if errors:
            print(f"Insert errors: {errors}")
        else:
            print("Successfully added new terms.")
    else:
        print("No new terms to add.")

if __name__ == "__main__":
    main()
