ÿ,
import os
import uuid
import datetime
import re
from google.cloud import bigquery

# Initialize Client
# Note: Using the authenticated client from environment
client = bigquery.Client(project="dnd-trends-index")

DATASET_ID = "dnd_trends_categorized"
SOURCE_TABLE = f"{DATASET_ID}.concept_library"
DEST_TABLE = f"{DATASET_ID}.expanded_search_terms"

def generate_nickname(term, category):
    """
    Applies logic to convert formal names to nicknames based on analysis.
    Returns the nickname or None if no nickname rule applies.
    """
    term_lower = term.lower()
    
    # Bard: "College of X" -> "X Bard"
    if "college of" in term_lower:
        # regex to capture the part after 'College of '
        match = re.search(r"college of (.+)", term, re.IGNORECASE)
        if match:
            core_name = match.group(1).strip()
            return f"{core_name} Bard"

    # Druid: "Circle of X" -> "X Druid"
    if "circle of" in term_lower:
        match = re.search(r"circle of (?:the )?(.+)", term, re.IGNORECASE)
        if match:
            core_name = match.group(1).strip()
            return f"{core_name} Druid"

    # Monk: "Way of X" -> "X Monk"
    if "way of" in term_lower:
        match = re.search(r"way of (?:the )?(.+)", term, re.IGNORECASE)
        if match:
            core_name = match.group(1).strip()
            return f"{core_name} Monk"
            
    # Paladin: Keep Oath, but maybe generate the alternate?
    # Logic from plan: "Keep full Oath of X (it performs well) AND generate X Paladin as backup"
    if "oath of" in term_lower:
         match = re.search(r"oath of (?:the )?(.+)", term, re.IGNORECASE)
         if match:
             core_name = match.group(1).strip()
             return f"{core_name} Paladin"

    return None

def expand_term(row):
    """
    Generates a list of dictionaries for the expanded terms.
    """
    original = row['concept_name']
    category = row['category']
    results = []
    
    def add_result(search_term, rule):
        results.append({
            "term_id": str(uuid.uuid4()),
            "original_keyword": original,
            "category": category,
            "search_term": search_term,
            "expansion_rule": rule,
            "created_at": datetime.datetime.now().isoformat(),
            "is_pilot": True
        })

    # --- RULES ENGINE ---
    
    # 1. CLASS
    if category == 'Class':
        # Rule: [Term] 5e
        add_result(f"{original} 5e", "suffix_5e")
        # Rule: [Term] 2024
        add_result(f"{original} 2024", "suffix_2024")
        # Rule: [Term] build
        add_result(f"{original} build", "suffix_build")
        
    # 2. SUBCLASS
    elif category == 'Subclass':
        # Rule: [Term] 5e (Always safe)
        add_result(f"{original} 5e", "suffix_5e")
        
        # Rule: [Term] build
        add_result(f"{original} build", "suffix_build")
        
        # Rule: Nickname Generation (The "Lore Bard" logic)
        nickname = generate_nickname(original, category)
        if nickname:
            add_result(nickname, "nickname_gen")
            # If we generated a nickname, usually "Nickname 5e" is also good? 
            # Analysis showed "Shadow Monk" (3248) vs "Shadow Monk 5e" (not tested directly but implied).
            # The plan was just "Shadow Monk". Let's stick to the generated nickname as a standalone for now.
            
            # Special Paladin Case: Plan said "Keep full Oath of X... AND generate X Paladin"
            # Since we generated the nickname above, we also need to ensure the ORIGINAL name gets a fair shake 
            # if the nickname completely replaced it?
            # Actually, we always keep the expansions based on 'original'. 
            # Wait, if 'original' is "Oath of Devotion", `original + 5e` = "Oath of Devotion 5e".
            # The nickname `Devotion Paladin` is added as an EXTRA term. Perfect.

    return results

def main():
    print("Fetching Pilot Data (Classes & Subclasses)...")
    
    # query to get only relevant categories
    query = """
        SELECT concept_name, category 
        FROM `dnd-trends-index.dnd_trends_categorized.concept_library`
        WHERE category IN ('Class', 'Subclass')
    """
    query_job = client.query(query)
    rows = list(query_job.result())
    
    print(f"Found {len(rows)} source keywords.")
    
    all_expanded_rows = []
    for row in rows:
        expansion = expand_term(row)
        all_expanded_rows.extend(expansion)
        
    print(f"Generated {len(all_expanded_rows)} expanded search terms.")
    
    # Batch Upload
    # Define Schema for explicit typing
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
    
    # Chunking for safety (though 15k rows is small for BQ)
    chunk_size = 5000
    for i in range(0, len(all_expanded_rows), chunk_size):
        chunk = all_expanded_rows[i : i + chunk_size]
        errs = client.insert_rows_json(DEST_TABLE, chunk)
        if errs:
            print(f"Encountered errors in chunk {i}: {errs}")
        else:
            print(f"Successfully uploaded chunk {i} - {i+len(chunk)}")

    print("Pilot Expansion Complete.")

if __name__ == "__main__":
    main()
ÿ,*cascade08"(fe48be399963714eecd073f750d2380851978ddc2.file:///C:/Users/Yorri/.gemini/expand_pilot.py:file:///C:/Users/Yorri/.gemini