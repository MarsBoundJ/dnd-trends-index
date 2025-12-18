°Fimport os
import csv
import datetime
from google.cloud import bigquery
import re

# --- 1. CONFIGURATION ---
PROJECT_ID = "dnd-trends-index" 

DATASET_ID = "dnd_trends_categorized"
TABLE_ID = "concept_library"
CSV_FILENAME = "UA_Raw.csv" 

# --- 2. HELPER FUNCTION: Parse Row ---


def parse_ua_row(csv_row):
    """
    Takes a single row from UA_Raw.csv and returns MULTIPLE concept rows
    by splitting the 'Contents' column.
    """
    rows_to_return = []
    
    try:
        # Get base info
        article_title = csv_row.get('Title', 'Unknown UA')
        date = csv_row.get('Date', 'Unknown Date')
        contents_raw = csv_row.get('Contents', '')
        
        # Extract Year for disambiguation (e.g. 2015, 2016)
        # Format is usually YYYY-MM-DD
        year = date.split('-')[0] if date and '-' in date else "UnknownYear"
        
        # 1. Create a row for the Article itself (some people search for "UA Eberron")
        base_row = {
            "concept_name": article_title,
            "category": "Unearthed Arcana",
            "source_book": "Wizards of the Coast (Web)",
            "ruleset": "5e Playtest",
            "tags": [
                "Category:Unearthed_Arcana",
                f"Concept:{article_title.replace(' ', '_')}",
                "Type:Article",
                f"Date:{date}",
                "Source:Official_Website"
            ],
            "last_processed_at": None
        }
        rows_to_return.append(base_row)

        # 2. Explode the 'Contents' column into individual concepts
        if contents_raw:
            # Split by semicolons (;) which separate major items in your list
            # Also clean up parentheses explanations
            items = contents_raw.split(';')
            
            for item in items:
                clean_item = item.strip()
                if not clean_item:
                    continue
                
                # --- Disambiguation & Normalization Logic ---
                # We want to catch major class updates like "Revised Ranger" or "Mystic class"
                # and normalize them to "Ranger (UA 2015)" to avoid collision with standard keywords
                # or collisions between UAs.
                
                lower_item = clean_item.lower()
                concept_name = f"{clean_item} (UA)" # Default fallback
                
                # Heuristics for Base Classes
                # Map regex-like strings to the desired base name
                if "revised ranger" in lower_item or "the ranger, revised" in lower_item:
                    concept_name = f"Ranger (UA {year})"
                elif "mystic class" in lower_item or "psionics/mystic update" in lower_item:
                    concept_name = f"Mystic (UA {year})"
                elif "artificer class" in lower_item or "the artificer revisited" in lower_item:
                    concept_name = f"Artificer (UA {year})"
                elif "wizard (artificer)" in lower_item:
                     # SPECIAL CASE: Early UA listed Artificer as a Wizard subclass.
                     # User correction: Artificer is its own class.
                     concept_name = f"Artificer (UA {year})"
                elif "revised class options" in lower_item:
                    # Specific article generic title, maybe skip or keep as is?
                    # The text is usually the contents. 
                    pass
                
                # Note: Subclasses like "Warlock (Hexblade)" usually don't need the year 
                # to be unique, but if we wanted to be strict we could append it there too.
                # For now, we only target the big collisions identified in planning.
                
                # Determine sub-category based on keywords
                sub_cat = "UA Content"
                if "Class" in clean_item or "Wizard" in clean_item or "Ranger" in clean_item:
                    sub_cat = "UA Class"
                elif "Subclass" in clean_item or "Domain" in clean_item or "College" in clean_item:
                    sub_cat = "UA Subclass"
                elif "Race" in clean_item or "Lineage" in clean_item or "Elf" in clean_item:
                    sub_cat = "UA Race"
                elif "Feat" in clean_item:
                    sub_cat = "UA Feat"
                elif "Spell" in clean_item:
                    sub_cat = "UA Spell"

                item_row = {
                    "concept_name": concept_name,
                    "category": sub_cat,
                    "source_book": article_title, # The specific article is the source
                    "ruleset": "5e Playtest",
                    "tags": [
                        f"Category:{sub_cat.replace(' ', '_')}",
                        f"Concept:{concept_name.replace(' ', '_')}",
                        f"SourceArticle:{article_title.replace(' ', '_')}",
                        "Type:Playtest_Content",
                        "Source:WotC"
                    ],
                    "last_processed_at": None
                }
                rows_to_return.append(item_row)
        
        return rows_to_return

    except Exception as e:
        print(f"WARN: Failed to parse row for '{csv_row.get('Title')}': {e}")
        return []

# --- 3. MAIN EXECUTION ---

def main():
    print(f"Starting Engine 1: CSV UA Expanded Importer for {CSV_FILENAME}...")
    
    # 1. Read data from CSV
    print(f"Reading data from {CSV_FILENAME}...")
    all_parsed_rows = []
    
    try:
        with open(CSV_FILENAME, mode='r', encoding='utf-8-sig') as file:
            reader = csv.DictReader(file)
            for row in reader:
                # parse_ua_row returns a LIST of rows
                new_rows = parse_ua_row(row)
                all_parsed_rows.extend(new_rows)
    except FileNotFoundError:
        print(f"CRITICAL: File not found: {CSV_FILENAME}.")
        return
    except Exception as e:
        print(f"CRITICAL: Failed to read CSV file: {e}")
        return
            
    print(f"Successfully parsed {len(all_parsed_rows)} expanded concepts from CSV.")

    if not all_parsed_rows:
        print("No rows to insert. Exiting.")
        return

    # 2. Deduplicate
    print(f"Deduplicating {len(all_parsed_rows)} rows...")
    deduplicated_rows = []
    seen_keys = set() 
    for row in all_parsed_rows:
        unique_key = (row['concept_name'], row['source_book'])
        if unique_key not in seen_keys:
            deduplicated_rows.append(row)
            seen_keys.add(unique_key)
    
    print(f"Found {len(deduplicated_rows)} unique rows.")

    # 3. Load to BigQuery
    try:
        bq_client = bigquery.Client(project=PROJECT_ID)
        table_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
        temp_table_id = f"{PROJECT_ID}.{DATASET_ID}.temp_csv_load_{int(datetime.datetime.now().timestamp())}"
        
        print(f"Loading rows into temporary table...")

        schema = [
            bigquery.SchemaField("concept_name", "STRING"),
            bigquery.SchemaField("category", "STRING"),
            bigquery.SchemaField("source_book", "STRING"),
            bigquery.SchemaField("ruleset", "STRING"),
            bigquery.SchemaField("tags", "STRING", mode="REPEATED"),
            bigquery.SchemaField("last_processed_at", "TIMESTAMP"),
        ]
        job_config = bigquery.LoadJobConfig(schema=schema)

        job = bq_client.load_table_from_json(
            deduplicated_rows, temp_table_id, job_config=job_config
        )
        job.result() 
        print(f"Loaded data into temporary table.")

        # 4. Run MERGE
        print("Running MERGE...")
        merge_query = f"""
        MERGE `{table_id}` AS T
        USING `{temp_table_id}` AS S
        ON T.concept_name = S.concept_name AND T.source_book = S.source_book
        
        WHEN NOT MATCHED BY TARGET THEN
          INSERT (concept_name, category, source_book, ruleset, tags, last_processed_at)
          VALUES (S.concept_name, S.category, S.source_book, S.ruleset, S.tags, S.last_processed_at)
        
        WHEN MATCHED THEN
          UPDATE SET
            T.category = S.category,
            T.tags = S.tags,
            T.ruleset = S.ruleset
        """
        
        merge_job = bq_client.query(merge_query)
        merge_job.result() 
        
        print(f"MERGE complete. {merge_job.num_dml_affected_rows} rows affected.")

        # 5. Clean up
        bq_client.delete_table(temp_table_id)
        print(f"Job complete!")

    except Exception as e:
        print(f"CRITICAL: Failed to load data into BigQuery: {e}")
        if 'temp_table_id' in locals():
            bq_client.delete_table(temp_table_id, not_found_ok=True)

if __name__ == "__main__":
    main()® ®™
™ª ª¯
¯Œ ŒÂ
ÂÊ ÊÎ
ÎÌ Ìè
èë ëí
íì ìï
ïñ ñ®
®© ©¨
¨≠ ≠∞
∞± ±¥
¥µ µº
ºΩ Ω¿
¿¡ ¡√
√ƒ ƒ≈
≈∆ ∆–
–— —”
”‘ ‘÷
÷◊ ◊Ì
ÌÒ Ò¯
¯˘ ˘á
áâ âã
ãè èî
îï ïñ
ñ¢ ¢•
•© ©¿
¿‘ ‘„
„‰ ‰Ì
ÌÓ Ó©
©™ ™‡
‡· ·Â
ÂÊ Êˆ
ˆ˜ ˜¢
¢£ £™
™≠ ≠‘
‘÷ ÷Ï
ÏÌ Ìí
íì ìö
öõ õ≥
≥µ µ∫
∫ª ªæ
æø øŒ
Œœ œ—
—“ “Ç
ÇÑ ÑÖ
Öâ âê
ê§ §Ü
Üó ó¢
¢£ £Ã
ÃŒ ŒŸ
Ÿ⁄ ⁄È
ÈÍ Íˇ
ˇÄ ÄÜ
Üã ã∆
∆« «Á
ÁÈ È∞
∞≤ ≤⁄
⁄‹ ‹ﬁ
ﬁ°F 28file:///c:/Users/Yorri/Downloads/importer_ua_expanded.py