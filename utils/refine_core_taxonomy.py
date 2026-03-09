import os
from google.cloud import bigquery

if not os.getenv("GOOGLE_APPLICATION_CREDENTIALS") and os.path.exists("/workspaces/dnd-trends/dnd-key.json"):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/workspaces/dnd-trends/dnd-key.json"

client = bigquery.Client(project="dnd-trends-index")

DATASET_ID = "dnd_trends_categorized"
CONCEPT_TABLE = f"{DATASET_ID}.concept_library"
EXPANDED_TABLE = f"{DATASET_ID}.expanded_search_terms"

def main():
    print("Surgical Refinement: Core Rules & Lore Taxonomy")
    
    # 1. Rename "Character" to "Core Rules & Mechanics"
    print("Renaming category 'Character' to 'Core Rules & Mechanics'...")
    rename_concept_query = f"""
        UPDATE `{CONCEPT_TABLE}`
        SET category = 'Core Rules & Mechanics'
        WHERE category = 'Character'
    """
    rename_expanded_query = f"""
        UPDATE `{EXPANDED_TABLE}`
        SET category = 'Core Rules & Mechanics'
        WHERE category = 'Character'
    """
    client.query(rename_concept_query).result()
    client.query(rename_expanded_query).result()

    # 2. Relocate legendary NPCs to "Lore & NPCs"
    print("Moving Legendary NPCs to 'Lore & NPCs'...")
    npcs = ["Drizzt Do'Urden", "Elminster"]
    npc_placeholders = ",".join([f"'{npc}'" for npc in npcs])
    
    move_concept_query = f"""
        UPDATE `{CONCEPT_TABLE}`
        SET category = 'Lore & NPCs'
        WHERE concept_name IN ({npc_placeholders})
    """
    move_expanded_query = f"""
        UPDATE `{EXPANDED_TABLE}`
        SET category = 'Lore & NPCs'
        WHERE original_keyword IN ({npc_placeholders})
    """
    client.query(move_concept_query).result()
    client.query(move_expanded_query).result()

    print("Successfully updated taxonomy.")

if __name__ == "__main__":
    main()
