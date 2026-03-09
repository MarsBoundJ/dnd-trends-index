import os
import uuid
import datetime
from google.cloud import bigquery

if not os.getenv("GOOGLE_APPLICATION_CREDENTIALS") and os.path.exists("/workspaces/dnd-trends/dnd-key.json"):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/workspaces/dnd-trends/dnd-key.json"

client = bigquery.Client(project="dnd-trends-index")

DATASET_ID = "dnd_trends_categorized"
CONCEPT_TABLE = f"{DATASET_ID}.concept_library"
EXPANDED_TABLE = f"{DATASET_ID}.expanded_search_terms"

def main():
    print("Surgical Refinement: Cards & Reference Materials (v2)")
    
    # 1. Archive existing concepts
    archive_query = f"""
        UPDATE `{CONCEPT_TABLE}`
        SET is_active = FALSE
        WHERE category = 'Cards & Reference Materials'
    """
    client.query(archive_query).result()
    print("Archived old concepts.")

    # 2. Define new concepts and their specific search variations
    new_data = [
        {
            "name": "Condition Marker Cards",
            "searches": ["dnd 5e condition cards", "dnd condition markers", "dnd condition rings for minis", "dnd status effect cards"]
        },
        {
            "name": "Spell Cards (5e Official)",
            "searches": [
                "dnd 5e spell cards", "official dnd spell cards", "dnd spell card deck",
                "dnd 5e cleric spell cards", "dnd 5e wizard spell cards",
                "dnd 5e bard spell cards", "dnd 5e druid spell cards", 
                "dnd 5e paladin spell cards", "dnd 5e ranger spell cards",
                "dnd 5e sorcerer spell cards", "dnd 5e warlock spell cards"
            ]
        },
        {
            "name": "Feat & Class Reference Cards",
            "searches": ["dnd 5e feat cards", "dnd class ability cards", "dnd 5e class quick reference cards", "dnd 5e feat and class cards"]
        },
        {
            "name": "Monster Reference Cards",
            "searches": ["dnd 5e monster cards", "dnd monster stat cards", "dnd monster quick reference cards", "dnd encounter cards"]
        }
    ]

    new_concepts = []
    new_expansions = []
    created_at_dt = datetime.datetime.now()
    created_at_str = created_at_dt.isoformat()

    for item in new_data:
        concept_name = item["name"]
        
        # Add Concept (correct schema)
        new_concepts.append({
            "concept_name": concept_name,
            "category": "Cards & Reference Materials",
            "is_active": True,
            "source_book": "User Requested Refinement"
        })

        # Add Expanded Terms
        for s in item["searches"]:
            new_expansions.append({
                "term_id": str(uuid.uuid4()),
                "original_keyword": concept_name,
                "category": "Cards & Reference Materials",
                "search_term": s,
                "expansion_rule": "manual_variant_refinement",
                "created_at": created_at_str,
                "is_pilot": True
            })

    # Insert Concepts
    print(f"Inserting {len(new_concepts)} new concepts...")
    errors = client.insert_rows_json(CONCEPT_TABLE, new_concepts)
    if errors:
        print(f"Concept Insert Errors: {errors}")

    # Insert Expansions
    print(f"Inserting {len(new_expansions)} new search terms...")
    errors = client.insert_rows_json(EXPANDED_TABLE, new_expansions)
    if errors:
        print(f"Expansion Insert Errors: {errors}")
    
    print("Successfully updated Cards & Reference Materials.")

if __name__ == "__main__":
    main()
