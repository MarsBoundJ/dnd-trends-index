¥from google.cloud import bigquery

# Config
DATASET_ID = 'dnd-trends-index.dnd_trends_categorized'
TABLE_ID = f'{DATASET_ID}.subreddit_registry'

# Data from Guide
SUBREDDITS = [
    {"subreddit_name": "dndnext", "tier": 1, "signal_type": "Technical/Mechanic", "weight": 1.2, "purpose": "High-intent mechanics discussion."},
    {"subreddit_name": "3d6", "tier": 1, "signal_type": "PC Builds", "weight": 1.2, "purpose": "Leading indicator for character builds."},
    {"subreddit_name": "DnDHomebrew", "tier": 1, "signal_type": "Leading/Innovation", "weight": 1.1, "purpose": "Tracks future trends & homebrew needs."},
    {"subreddit_name": "UnearthedArcana", "tier": 1, "signal_type": "Design/Mechanic", "weight": 1.1, "purpose": "High-quality homebrew traction."},
    {"subreddit_name": "DnD", "tier": 1, "signal_type": "General/Vibe", "weight": 0.7, "purpose": "Massive volume, general community pulse."},
    {"subreddit_name": "dndmemes", "tier": 1, "signal_type": "Viral/Volume", "weight": 0.3, "purpose": "Cultural pulse (Volume Only unless viral)."},
    {"subreddit_name": "DMAcademy", "tier": 1, "signal_type": "DM Resources", "weight": 1.0, "purpose": "DM pain points and monster usage."},
    {"subreddit_name": "lfg", "tier": 2, "signal_type": "Market Demand", "weight": 1.0, "purpose": "Tracks system popularity via tags."},
    {"subreddit_name": "Pathfinder2e", "tier": 2, "signal_type": "Competitive", "weight": 0.9, "purpose": "Context for D&D sentiment shifts."},
    {"subreddit_name": "DungeonsAndDragons", "tier": 2, "signal_type": "General", "weight": 0.8, "purpose": "Secondary general D&D discussion."},
    {"subreddit_name": "rpghorrorstories", "tier": 3, "signal_type": "Negative Sentiment", "weight": 1.0, "purpose": "Deep-dive into community pain points."},
    {"subreddit_name": "BehindTheTables", "tier": 3, "signal_type": "Lore/Flavor", "weight": 0.7, "purpose": "Leading indicator for adventure themes."},
    {"subreddit_name": "osr", "tier": 3, "signal_type": "Niche/Lore", "weight": 0.8, "purpose": "Tracks Batch 6 (Lore/Settings) trends."},
    {"subreddit_name": "criticalrole", "tier": 3, "signal_type": "Media/AP", "weight": 0.9, "purpose": "Influencer-driven interest spikes."},
    {"subreddit_name": "dimension20", "tier": 3, "signal_type": "Media/AP", "weight": 0.9, "purpose": "Influencer-driven interest spikes."},
    {"subreddit_name": "adventurersleague", "tier": 3, "signal_type": "Organized Play", "weight": 1.0, "purpose": "Official play environment trends."}
]

def setup_registry():
    client = bigquery.Client()
    
    schema = [
        bigquery.SchemaField("subreddit_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("tier", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("signal_type", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("weight", "FLOAT", mode="REQUIRED"),
        bigquery.SchemaField("purpose", "STRING", mode="NULLABLE"),
    ]
    
    table = bigquery.Table(TABLE_ID, schema=schema)
    
    print(f"Creating table {TABLE_ID}...")
    try:
        table = client.create_table(table)  # Make an API request.
        print(f"Created table {table.project}.{table.dataset_id}.{table.table_id}")
    except Exception as e:
        print(f"Table might already exist: {e}")
        # Identify if we need to clear it? Let's just append/overwrite logic if needed. 
        # For now, assumes fresh start or error.
        # Actually, let's truncate if exists to ensure fresh list?
        # No, just let it exist.
    
    print("Inserting rows...")
    errors = client.insert_rows_json(TABLE_ID, SUBREDDITS)
    if errors == []:
        print("New rows have been added.")
    else:
        print("Encountered errors while inserting rows: {}".format(errors))

if __name__ == "__main__":
    setup_registry()
¥*cascade08"(f973a887b13d985b6bb9f24c433c2ee4d5a88d0420file:///C:/Users/Yorri/.gemini/setup_registry.py:file:///C:/Users/Yorri/.gemini