import os
from google.cloud import bigquery

if not os.getenv("GOOGLE_APPLICATION_CREDENTIALS") and os.path.exists("/workspaces/dnd-trends/dnd-key.json"):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/workspaces/dnd-trends/dnd-key.json"

client = bigquery.Client(project="dnd-trends-index")

DATASET_ID = "dnd_trends_categorized"
CONCEPT_TABLE = f"{DATASET_ID}.concept_library"
EXPANDED_TABLE = f"{DATASET_ID}.expanded_search_terms"

def main():
    print("Surgical Refinement: Consolidating Builds and Refining Roles")
    
    # 1. Consolidate "Build Name" into "Build"
    print("Merging category 'Build Name' into 'Build'...")
    client.query(f"UPDATE `{CONCEPT_TABLE}` SET category = 'Build' WHERE category = 'Build Name'").result()
    client.query(f"UPDATE `{EXPANDED_TABLE}` SET category = 'Build' WHERE category = 'Build Name'").result()

    # 2. Refine "Character Role" overlaps
    print("Relocating Character Role entries...")
    
    # Feats
    feat_names = ["Great Weapon Master", "Polearm Master", "Crossbow Expert", "Sharpshooter"]
    feat_placeholders = ",".join([f"'{f}'" for f in feat_names])
    client.query(f"UPDATE `{CONCEPT_TABLE}` SET category = 'Feat' WHERE concept_name IN ({feat_placeholders})").result()
    client.query(f"UPDATE `{EXPANDED_TABLE}` SET category = 'Feat' WHERE original_keyword IN ({feat_placeholders})").result()

    # Archetypes
    archetype_names = ["Tank", "Support", "Controller", "Blaster", "Striker", "Nova"]
    arch_placeholders = ",".join([f"'{a}'" for a in archetype_names])
    client.query(f"UPDATE `{CONCEPT_TABLE}` SET category = 'Character Archetypes' WHERE concept_name IN ({arch_placeholders})").result()
    client.query(f"UPDATE `{EXPANDED_TABLE}` SET category = 'Character Archetypes' WHERE original_keyword IN ({arch_placeholders})").result()

    # 3. Handle duplicates or miscategorized items
    # If an item already exists in the target category, we might want to archive the old one.
    # For now, we trust the category merge is sufficient for score consolidation in the view.

    print("Successfully consolidated taxonomy.")

if __name__ == "__main__":
    main()
