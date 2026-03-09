import os
from google.cloud import bigquery

# Configuration
PROJECT_ID = "dnd-trends-index"
DATASET_ID = "dnd_trends_categorized"
CONCEPT_TABLE = f"{PROJECT_ID}.{DATASET_ID}.concept_library"

# Auto-load creds if local
if not os.getenv("GOOGLE_APPLICATION_CREDENTIALS") and os.path.exists("/workspaces/dnd-trends/dnd-key.json"):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/workspaces/dnd-trends/dnd-key.json"

client = bigquery.Client(project=PROJECT_ID)

def run_query(query, description):
    print(f"--- {description} ---")
    try:
        query_job = client.query(query)
        query_job.result()
        print("Success.")
    except Exception as e:
        print(f"FAILED: {e}")

def task_1_renames():
    renames = [
        # Art Category Fixes
        ("concept_name = 'Fiverr Artist Dnd'", "concept_name = 'Fiverr D&D Artist 5e'"),
        ("concept_name = 'Horror Art Dnd'", "concept_name = 'Horror D&D Art Dnd'"),
        ("concept_name = 'Character Sheet Art Dnd'", "concept_name = 'Character Sheet Art 5e'"),
        ("concept_name = 'Instagram Artists Dnd'", "concept_name = 'Instagram D&D Artists 5e'"),
        ("concept_name = 'Anime Art Dnd'", "concept_name = 'Anime D&D Art 5e'"),
        ("concept_name = 'Monk Character Art Dnd'", "concept_name = 'Monk Character Art 5e'"),
        ("concept_name = 'Gnome Character Art Dnd'", "concept_name = 'Gnome Character Art 5e'"),
        ("concept_name = 'Reddit Dnd Art'", "concept_name = 'Reddit D&D Art 5e'"),
        ("concept_name = 'Art Nouveau Dnd'", "concept_name = 'Art Nouveau D&D Dnd'"),
        ("concept_name = 'Photorealistic Art Dnd'", "concept_name = 'Photorealistic D&D Dnd'"),
        ("concept_name = 'Sketch Style Art Dnd'", "concept_name = 'Sketch Style D&D Art Dnd'"),
        # Accessory Fixes
        ("concept_name = 'Dice Dragons Dnd'", "concept_name = 'Dice Dragons 5e'"),
        ("concept_name = 'Spell Reference Folders Dnd'", "concept_name = 'Spell Reference Folders 5e'"),
        ("concept_name = 'Artisan Dice Dnd'", "concept_name = 'Artisan Dice 5e'"),
        ("concept_name = 'Commission-Painted Miniatures Dnd'", "concept_name = 'Commission-Painted Miniatures 5e'"),
        ("concept_name = 'Concentration Markers Dnd'", "concept_name = 'Concentration Markers 5e'"),
        ("concept_name = 'Condition Marker Cards Dnd'", "concept_name = 'Condition Marker Cards 5e'"),
        ("concept_name = 'Collectible Dnd Merch'", "concept_name = 'Collectible D&D Merchandise 5e'"),
        # Influencer Fixes
        ("concept_name = 'World Anvil Blog'", "concept_name = 'World Anvil Blog Dnd'"),
        ("concept_name = 'Master the Dungeon'", "concept_name = 'Master the Dungeon Dnd'"),
        ("concept_name = 'Tabletop Builds'", "concept_name = 'Tabletop Builds Dnd'")
    ]
    
    for set_clause, where_clause, in renames:
        q = f"UPDATE `{CONCEPT_TABLE}` SET {set_clause} WHERE {where_clause}"
        run_query(q, f"Renaming: {where_clause} -> {set_clause}")

def task_2_build_injection():
    q = f"""
    UPDATE `{CONCEPT_TABLE}`
    SET concept_name = CONCAT(concept_name, ' Dnd')
    WHERE category = 'Build'
      AND NOT REGEXP_CONTAINS(concept_name, r'(?i)( Dnd| 5e| 2024)$');
    """
    run_query(q, "Task 2: Build Context Injection")

def task_3_data_injection():
    q = f"""
    INSERT INTO `{CONCEPT_TABLE}` (concept_name, category, is_active, source_book)
    VALUES 
    ('Bladesinger', 'Subclass', TRUE, 'Manual Injection Phase 23'),
    ('Bladesinger 5e', 'Subclass', TRUE, 'Manual Injection Phase 23');
    """
    run_query(q, "Task 3: Injecting Bladesinger Data")

if __name__ == "__main__":
    print("Starting Phase 23: Surgical Data Hygiene...")
    task_1_renames()
    task_2_build_injection()
    task_3_data_injection()
    print("Surgical Hygiene Complete.")
