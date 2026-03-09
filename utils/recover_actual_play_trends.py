
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
    print("Fetching Actual Play concepts...")
    query = f"""
        SELECT concept_name, category 
        FROM `{SOURCE_TABLE}`
        WHERE category = 'Actual Play' AND is_active = TRUE
    """
    concepts = list(client.query(query).result())
    print(f"Found {len(concepts)} active Actual Play concepts.")

    print("Checking for existing expansions...")
    existing_query = f"SELECT original_keyword, search_term FROM `{DEST_TABLE}` WHERE category = 'Actual Play'"
    existing_rows = list(client.query(existing_query).result())
    existing_map = {(row['original_keyword'], row['search_term']) for row in existing_rows}

    new_rows = []
    for concept in concepts:
        name = concept['concept_name']
        cat = concept['category']
        
        # Rule 1: Standalone
        if (name, name) not in existing_map:
            new_rows.append({
                "term_id": str(uuid.uuid4()),
                "original_keyword": name,
                "category": cat,
                "search_term": name,
                "expansion_rule": "standalone_recovery",
                "created_at": datetime.datetime.now().isoformat(),
                "is_pilot": True
            })
        
        # Rule 2: [Term] Dnd
        dnd_term = f"{name} Dnd"
        if (name, dnd_term) not in existing_map:
            new_rows.append({
                "term_id": str(uuid.uuid4()),
                "original_keyword": name,
                "category": cat,
                "search_term": dnd_term,
                "expansion_rule": "suffix_dnd_recovery",
                "created_at": datetime.datetime.now().isoformat(),
                "is_pilot": True
            })

    if not new_rows:
        print("No new expansions needed.")
        return

    print(f"Inserting {len(new_rows)} new search terms...")
    schema = [
        bigquery.SchemaField("term_id", "STRING"),
        bigquery.SchemaField("original_keyword", "STRING"),
        bigquery.SchemaField("category", "STRING"),
        bigquery.SchemaField("search_term", "STRING"),
        bigquery.SchemaField("expansion_rule", "STRING"),
        bigquery.SchemaField("created_at", "TIMESTAMP"),
        bigquery.SchemaField("is_pilot", "BOOL"),
    ]
    job_config = bigquery.LoadJobConfig(schema=schema)
    errors = client.insert_rows_json(DEST_TABLE, new_rows)
    
    if errors:
        print(f"Errors: {errors}")
    else:
        print("Successfully updated expansions.")

if __name__ == "__main__":
    main()
