
import os
from google.cloud import bigquery

# Configuration
PROJECT_ID = "dnd-trends-index"
CONCEPT_TABLE = f"{PROJECT_ID}.dnd_trends_categorized.concept_library"

# Auto-load creds if local
if not os.getenv("GOOGLE_APPLICATION_CREDENTIALS") and os.path.exists("/workspaces/dnd-trends/dnd-key.json"):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/workspaces/dnd-trends/dnd-key.json"

client = bigquery.Client(project=PROJECT_ID)

def run_query(query, description):
    print(f"--- {description} ---")
    try:
        query_job = client.query(query)
        query_job.result()  # Wait for job to complete
        print("Success.")
    except Exception as e:
        print(f"FAILED: {e}")

def task_1_schema_upgrade():
    # BigQuery doesn't support ADD COLUMN with DEFAULT in one go.
    # Step 1: Add Columns
    q1 = f"""
    ALTER TABLE `{CONCEPT_TABLE}`
    ADD COLUMN IF NOT EXISTS is_active BOOL,
    ADD COLUMN IF NOT EXISTS canonical_parent STRING,
    ADD COLUMN IF NOT EXISTS sub_tags ARRAY<STRING>;
    """
    run_query(q1, "Task 1.1: Add Columns")
    
    # Step 2: Set Defaults via UPDATE (for existing rows)
    q2 = f"UPDATE `{CONCEPT_TABLE}` SET is_active = TRUE WHERE is_active IS NULL"
    run_query(q2, "Task 1.2: Set is_active Default")
    
    # Step 3: Set schema default for future rows (Optional, but good practice)
    q3 = f"ALTER TABLE `{CONCEPT_TABLE}` ALTER COLUMN is_active SET DEFAULT TRUE"
    run_query(q3, "Task 1.3: Set Schema Default")

def task_2_cleanup():
    # Group A: Archive
    archive_list = [
        'Steel price', 'Immortal build', 'Avatar 5e', 'Walk the Walk Dnd', 'Fortitude Dnd',
        'Than Dnd', 'Than 5e', 'Mordenkainen Dnd', 'Mordenkainen 5e', 'Horns Dnd',
        'Violet Dnd', 'Violet 5e', 'Thaco Dnd', 'Trinket Dnd', 'The Horror Dnd',
        'The Horror 5e', 'Xanathar Dnd', 'Xanathar 5e', 'Excess Dnd', 'The Companion price',
        'Wave price', 'Cold Seal price', 'Drown price', 'Fire Seal price', 'Code Legal price',
        'Dragon Slayer price', 'Acid Seal price', 'Vault Keys price', 'Lightning Seal price',
        'Poison Seal price', 'Thunder Seal price', 'Rebuke 5e', '4e 5e', '3e 5e', '2e 5e',
        'Dreamers 2024', 'Humans 2024', 'Myconid 5e', 'Mandrake 2024', 'Lies Dnd',
        'Commando Build', 'DnD Builds Dnd', 'DnD Shorts Dnd', 'Thaumaturge Dnd',
        'Creature Design D&D Dnd', 'Concept', 'The Ordning 5e', 'Event', 'Hook Horror Hunt',
        '5. Seasonality'
    ]
    # Format list for SQL IN clause
    archive_str = ", ".join([f"'{x}'" for x in archive_list])
    
    q_a = f"""
    UPDATE `{CONCEPT_TABLE}` 
    SET is_active = FALSE, category = 'Archive' 
    WHERE concept_name IN ({archive_str});
    """
    run_query(q_a, "Task 2 Group A: Archive")

    # Group B: Mechanic
    mechanic_list = [
        'Stat Block Dnd', 'Stat Block 5e', 'Concentration 5e', 'Melee dnd', 'Melee 5e',
        'Ritual 5e', 'Ranged 5e', 'Dungeon Master Dnd', 'Dungeon Master 5e', 'Downtime 5e',
        'Feat 5e', 'Initiative 5e', 'Action 5e', 'D20 5e', 'Roll20 5e', 'DEX 5e',
        'Illusion 5e', 'Homebrew 5e', 'Bonus Action 5e', 'Turn 5e', 'Saving Throw 5e',
        'Encounter 5e', 'Spell Slot 5e', 'DM 5e', 'Dodge 5e', 'Movement 5e', 'PC 5e',
        'Campaign 5e', 'Flanking 5e', 'CON 5e', 'Disengage 5e', 'PHB 5e', 'Advantage 5e',
        'XP 5e', 'Bond 5e', 'Multiclass 5e', 'DMG 5e', 'Support 5e', 'Light Property Dnd'
    ]
    mechanic_str = ", ".join([f"'{x}'" for x in mechanic_list])
    
    q_b = f"""
    UPDATE `{CONCEPT_TABLE}`
    SET category = 'Mechanic'
    WHERE concept_name IN ({mechanic_str});
    """
    run_query(q_b, "Task 2 Group B: Move to Mechanic")

    # Group C: Item
    item_list = ['Cloak 5e', 'Cloak Dnd', 'Trinket 5e', 'Moonbow Dnd', 'Torch Dnd']
    item_str = ", ".join([f"'{x}'" for x in item_list])
    
    q_c = f"""
    UPDATE `{CONCEPT_TABLE}`
    SET category = 'Item'
    WHERE concept_name IN ({item_str});
    """
    run_query(q_c, "Task 2 Group C: Move to Item")

    # Group D: Feat
    q_d = f"""
    UPDATE `{CONCEPT_TABLE}`
    SET category = 'Feat'
    WHERE concept_name IN ('Telekinetic 5e');
    """
    run_query(q_d, "Task 2 Group D: Move to Feat")
    
    # Group E: Class
    q_e = f"""
    UPDATE `{CONCEPT_TABLE}`
    SET category = 'Class'
    WHERE concept_name IN ('Subclasses 5e', 'The Ranger 5e');
    """
    run_query(q_e, "Task 2 Group E: Move to Class")
    
    # Group F: Spell
    q_f = f"""
    UPDATE `{CONCEPT_TABLE}`
    SET category = 'Spell'
    WHERE concept_name IN ('Knock 5e', 'Knock Dnd');
    """
    run_query(q_f, "Task 2 Group F: Move to Spell")
    
    # Group G: Setting
    q_g = f"""
    UPDATE `{CONCEPT_TABLE}`
    SET category = 'Setting'
    WHERE concept_name IN ('Dragonlance dnd', 'Rime Dnd', 'Rime 5e');
    """
    run_query(q_g, "Task 2 Group G: Move to Setting")
    
    # Group H: Rename (Handle PK Constraint by deactivating old and creating new?)
    # Since concept_name is likely PK, we can't update it directly if it's partitioned/clustered on it or if strict PK.
    # But BigQuery doesn't enforce PKs rigidly in standard tables unless specified.
    # Assuming we can UPDATE concept_name if no conflict.
    # However user instruction said: "Since we cannot easily rename Primary Keys, insert new rows and mark old ones as inactive."
    
    renames = [
        ("Defender dnd", "Defender sword dnd"),
        ("Snap Dnd", "Snapjaw Dnd"),
        ("Snap 5e", "Snapjaw 5e"),
        ("Rogue (UA) UA", "Rogue UA"),
        ("5e 5e", "5e")
    ]
    
    print("--- Task 2 Group H: Specific Renames ---")
    for old, new in renames:
        # Check if old exists
        check_q = f"SELECT * FROM `{CONCEPT_TABLE}` WHERE concept_name = '{old}'"
        rows = list(client.query(check_q).result())
        
        if not rows:
            print(f"Skipping {old} -> {new}: Old term not found.")
            continue
            
        # Insert New (Copy columns from old)
        # We need a dynamic insert to carry over other fields like category?
        # Let's simple select * from old, change name, insert.
        
        insert_q = f"""
        INSERT INTO `{CONCEPT_TABLE}` (concept_name, category, is_active, canonical_parent, sub_tags)
        SELECT '{new}', category, TRUE, canonical_parent, sub_tags
        FROM `{CONCEPT_TABLE}`
        WHERE concept_name = '{old}';
        """
        try:
            client.query(insert_q).result()
            print(f"Inserted {new}")
            
            # Deactivate Old
            deactivate_q = f"""
            UPDATE `{CONCEPT_TABLE}`
            SET is_active = FALSE, category = 'Archive'
            WHERE concept_name = '{old}'
            """
            client.query(deactivate_q).result()
            print(f"Archived {old}")
            
        except Exception as e:
            print(f"Error renaming {old} to {new}: {e}")

def task_3_taxonomy():
    types = ['Aberration', 'Beast', 'Celestial', 'Construct', 'Dragon', 'Elemental', 'Fey', 
             'Fiend', 'Giant', 'Humanoid', 'Monstrosity', 'Ooze', 'Plant', 'Undead']
    
    for t in types:
        q = f"""
        UPDATE `{CONCEPT_TABLE}`
        SET sub_tags = ARRAY_CONCAT(COALESCE(sub_tags, []), ['{t}'])
        WHERE category = 'Monster' AND concept_name LIKE '%{t}%'
        """
        run_query(q, f"Task 3: Injecting Type {t}")
        
    tags = ['Demon', 'Devil', 'Yugoloth', 'Goblinoid', 'Lizardfolk', 'Grimlock', 
            'Gem Dragon', 'Metallic Dragon', 'Chromatic Dragon']
            
    for t in tags:
         q = f"""
        UPDATE `{CONCEPT_TABLE}`
        SET sub_tags = ARRAY_CONCAT(COALESCE(sub_tags, []), ['{t}'])
        WHERE category = 'Monster' AND concept_name LIKE '%{t}%'
        """
         run_query(q, f"Task 3: Injecting Tag {t}")

def task_4_canonical_view():
    q = f"""
    CREATE OR REPLACE VIEW `dnd-trends-index.gold_data.view_leaderboard_clean` AS
    WITH cleaned AS (
        SELECT
            -- 1. Strip Suffixes for Display
            TRIM(REGEXP_REPLACE(search_term, r'(?i) (5e|Dnd|2024|dnd|5\.5|price|build)$', '')) as display_name,
            category,
            interest as trend_score_raw
        FROM `{PROJECT_ID}.dnd_trends_categorized.trend_data_pilot` t
        LEFT JOIN `{CONCEPT_TABLE}` c ON t.search_term = c.concept_name
        WHERE 
            -- 2. Exclude Archived terms
            (c.is_active IS NULL OR c.is_active = TRUE)
            AND (c.category IS NULL OR c.category != 'Archive')
    )
    SELECT 
        display_name,
        category,
        -- 3. Max-Signal Aggregation (Zombie 5e [80] + Zombie Dnd [40] = Zombie [80])
        MAX(trend_score_raw) as score,
        -- 4. Count how many variations fed this score
        COUNT(*) as variation_count
    FROM cleaned
    GROUP BY 1, 2
    ORDER BY score DESC;
    """
    # Note: I adjusted the query slightly to match our Pilot table structure.
    # gold_data.trend_scores doesn't exist yet, we use dnd_trends_categorized.trend_data_pilot
    # Also we join with concept_library to check is_active.
    
    run_query(q, "Task 4: Creating Canonical View")

if __name__ == "__main__":
    print("Starting Deep Data Hygiene...")
    task_1_schema_upgrade()
    task_2_cleanup()
    task_3_taxonomy()
    task_4_canonical_view()
    print("Phase 21 Partial Completion.")
