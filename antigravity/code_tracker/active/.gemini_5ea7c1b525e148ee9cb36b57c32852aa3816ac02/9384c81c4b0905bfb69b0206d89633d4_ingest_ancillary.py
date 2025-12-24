«%import csv
import uuid
import datetime
import os
from google.cloud import bigquery

# DB Config
TABLE_ID = 'dnd-trends-index.dnd_trends_categorized.concept_library'

# File Config (Filename -> Config)
FILE_MAP = {
    'Art_and_Artists_All_Cleaned.csv': {
        'col_map': {'Keyword': 'concept_name', 'Category': 'category'},
        'default_category': 'Art'
    },
    'D&D_ACTUAL_PLAY_PODCASTS_COMPREHENSIVE_UPDATED.csv': {
        'col_map': {'Podcast Name': 'concept_name', 'Keyword': 'concept_name'}, # Fallback
        'default_category': 'Podcast'
    },
    'D&D_AI_ART.csv': {
        'col_map': {'Keyword': 'concept_name'},
        'default_category': 'AI'
    },
    'D&D_AI_DM.csv': {
        'col_map': {'Keyword': 'concept_name'},
        'default_category': 'AI'
    },
    'D&D_AI_WORLD.csv': {
        'col_map': {'Keyword': 'concept_name'},
        'default_category': 'AI'
    },
    'D&D_BUILD_NAMES_FINAL.csv': {
        'col_map': {'Build Name': 'concept_name', 'Category': 'category'},
        'default_category': 'Build'
    },
    'D&D_CHARACTER_ROLES_AND_CONCEPTS.csv': {
        'col_map': {'Concept': 'concept_name', 'Role': 'category'},
        'default_category': 'Concept'
    },
    'D&D_Influencers_Webfirst_updated_38.csv': {
        'col_map': {'Site Name': 'concept_name'},
        'default_category': 'Influencer'
    },
    'D&D_TERMINOLOGY_AND_SLANG_FINAL.csv': {
        'col_map': {'Term': 'concept_name', 'Keyword': 'concept_name'},
        'default_category': 'Slang'
    },
    'D&D_TOOLS_AND_ACCESSORIES_COMPREHENSIVE.csv': {
        'col_map': {'Tool': 'concept_name', 'Name': 'concept_name', 'Keyword': 'concept_name'},
        'default_category': 'Accessory'
    },
    'D&D_YOUTUBE_INFLUENCER_CHANNELS_MERGED (2).csv': {
        'col_map': {'Channel Name': 'concept_name'},
        'default_category': 'Youtube'
    },
    'UA_Raw.csv': {
        'col_map': {'Title': 'concept_name'},
        'default_category': 'UA Content'
    },
    'dnd_conventions.csv': {
        'col_map': {'name': 'concept_name'},
        'default_category': 'Convention'
    }
}

DOWNLOADS_DIR = r"C:\Users\Yorri\Downloads"

def ingest_ancillary():
    client = bigquery.Client()
    rows_to_insert = []
    
    print("Starting ingestion...")
    
    for filename, config in FILE_MAP.items():
        filepath = os.path.join(DOWNLOADS_DIR, filename)
        if not os.path.exists(filepath):
            print(f"SKIPPING: {filename} (Not found)")
            continue
            
        print(f"Processing {filename}...")
        
        with open(filepath, 'r', encoding='utf-8-sig') as f: # utf-8-sig to handle BOM
            reader = csv.DictReader(f)
            
            # Smart Column Detection
            concept_col = None
            category_col = None
            
            # Find concept column
            for potential_col, target in config['col_map'].items():
                if target == 'concept_name' and potential_col in reader.fieldnames:
                    concept_col = potential_col
                    break
            
            # Find category column
            if 'col_map' in config and 'Category' in config['col_map']: # direct map
                 pass
            elif 'Category' in reader.fieldnames:
                category_col = 'Category'
            
            if not concept_col:
                print(f"  ERROR: Could not find concept column in {filename}. Headers: {reader.fieldnames}")
                continue
                
            count = 0
            for row in reader:
                term = row[concept_col].strip()
                if not term: continue
                
                cat = row[category_col].strip() if category_col and row.get(category_col) else config['default_category']
                
                # BigQuery Row - Simplified Schema based on inferred structure
                bq_row = {
                    "concept_name": term,
                    "category": cat,
                    "source_book": filename
                }
                rows_to_insert.append(bq_row)
                count += 1
            print(f"  Loaded {count} rows.")

    print(f"Total rows to insert: {len(rows_to_insert)}")
    
    # BATCH INSERT
    BATCH_SIZE = 5000
    for i in range(0, len(rows_to_insert), BATCH_SIZE):
        batch = rows_to_insert[i:i+BATCH_SIZE]
        print(f"Inserting batch {i}...")
        errors = client.insert_rows_json(TABLE_ID, batch)
        if errors:
            print(f"Errors: {errors[:5]}")
            
    print("Ingestion Complete.")

if __name__ == "__main__":
    ingest_ancillary()
‚ *cascade08‚ø*cascade08ø€ *cascade08€„*cascade08„ö *cascade08öñ *cascade08ñ¡*cascade08¡«% *cascade08"(5ea7c1b525e148ee9cb36b57c32852aa3816ac0222file:///C:/Users/Yorri/.gemini/ingest_ancillary.py:file:///C:/Users/Yorri/.gemini