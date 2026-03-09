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

def task_2_consolidation():
    # A. The Archive (Soft Delete)
    q_a = f"""
    UPDATE `{CONCEPT_TABLE}`
    SET is_active = FALSE, category = 'Archive'
    WHERE category IN (
        'Wearables & Apparel', 'Immersion & Atmosphere', 'Novelty & Collectible Items', 
        'Storage & Organization', 'Tokens & Condition Markers', 'Concept'
    )
    """
    run_query(q_a, "Group A: Archive (Soft Delete)")

    # B. Core Mechanics Merges
    q_b = f"""
    UPDATE `{CONCEPT_TABLE}`
    SET category = 'Mechanic'
    WHERE category IN (
        'Ability Scores', 'Combat', 'Combat Mechanics', 'Combat Tactics', 
        'Core Mechanic', 'Defense', 'Game Design', 'General Game Terms', 
        'Proficiency', 'Sense', 'Slang', 'Weapon', 'Community Learning', 
        'Community Lingo', 'DM Terms', 'Spellcasting', 'Spells', 'Spells & Effects'
    )
    """
    run_query(q_b, "Group B: Core Mechanics Merges")

    # C. Magic Item Merges
    q_c = f"""
    UPDATE `{CONCEPT_TABLE}`
    SET category = 'MagicItem'
    WHERE category IN ('Usable Item', 'Supernatural Gift', 'Item')
    """
    run_query(q_c, "Group C: Magic Item Merges")

    # D. Spell Merges
    q_d = f"""
    UPDATE `{CONCEPT_TABLE}`
    SET category = 'Spell'
    WHERE category IN ('Facilities', 'Environment')
    """
    run_query(q_d, "Group D: Spell Merges")

    # E. Equipment Merges
    q_e = f"""
    UPDATE `{CONCEPT_TABLE}`
    SET category = 'Equipment'
    WHERE category IN ('Tool', 'Currency')
    """
    run_query(q_e, "Group E: Equipment Merges")

    # f. Art Consolidation
    q_f = f"""
    UPDATE `{CONCEPT_TABLE}`
    SET category = 'Art'
    WHERE category IN ('Art Style', 'Art Subject', 'Art Type', 'Artist', 'Miniatures', 'Miniatures & Figures')
    """
    run_query(q_f, "Group F: Art Consolidation")

    # G. Accessory Merges
    q_g = f"""
    UPDATE `{CONCEPT_TABLE}`
    SET category = 'Accessory'
    WHERE category = 'Search Trend'
    """
    run_query(q_g, "Group G: Accessory Merges")

    # H. Specific Moves & Renames
    moves = [
        ("category = 'Monster'", "category = 'Species'", "Species to Monster"),
        ("category = 'Species & Lineage'", "category = 'Race'", "Race to Species & Lineage"),
        ("category = 'Invocations and Pacts'", "category = 'Invocation'", "Invocation to Invocations and Pacts")
    ]
    
    for set_clause, where_clause, desc in moves:
        q_h = f"UPDATE `{CONCEPT_TABLE}` SET {set_clause} WHERE {where_clause}"
        run_query(q_h, f"Group H: {desc}")

if __name__ == "__main__":
    print("Starting Phase 22: Taxonomy Consolidation...")
    task_2_consolidation()
    print("Consolidation Complete.")
