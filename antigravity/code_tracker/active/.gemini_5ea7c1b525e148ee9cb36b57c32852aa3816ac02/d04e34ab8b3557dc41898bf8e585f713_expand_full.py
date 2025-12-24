áFimport csv
import json
import uuid
import datetime
import re
from google.cloud import bigquery

# Config
MCI_FILE = 'master_collision_index.json'
REFILL_FILE = 'refill_list.csv'
DEST_TABLE = 'dnd-trends-index.dnd_trends_categorized.expanded_search_terms'
SOURCE_TABLE = 'dnd-trends-index.dnd_trends_categorized.concept_library'

# Official Books (heuristic list based on WotC)
OFFICIAL_SOURCES = [
    'phb', 'players handbook', 'dmg', 'dungeon masters guide', 'mm', 'monster manual',
    'xanathars', 'tashas', 'fizbans', 'mordenkainens', 'volos', 'eberron', 'ravnica', 
    'theros', 'strixhaven', 'spelljammer', 'planescape', 'dragonlance', 'bigby',
    'basic rules', 'srd', 'vecna', 'shattered obelisk', 'phantandelver', 
    'icewind dale', 'tomb of annihilation', 'curse of strahd'
]

def load_mci():
    with open(MCI_FILE, 'r', encoding='utf-8') as f:
        data = json.load(f)
    return set(data['all_combined'])

def load_refill_map():
    # Map 'search_term' -> 'new_term' (e.g. Warden -> Warden Dnd)
    mapping = {}
    try:
        with open(REFILL_FILE, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get('new_term'):
                    mapping[row['search_term']] = row['new_term']
    except FileNotFoundError:
        pass
    return mapping

def is_official_source(source_str):
    if not source_str: return False
    s = source_str.lower()
    return any(bk in s for bk in OFFICIAL_SOURCES)

def generate_terms(row, mci_set, refill_map):
    term_base = row['concept_name']
    cat = row['category']
    source = row['source_book']
    # term_id = row['term_id']  <-- Caused KeyError, and unused.
    
    is_official = is_official_source(source)
    
    expansions = []
    
    # Base variations based on Category
    variations = []
    
    # 1. Class/Subclass (Legacy/Refill Logic)
    if cat == 'Class':
        variations = [f"{term_base} 5e", f"{term_base} build", f"{term_base} 2024"]
    elif cat == 'Subclass':
        variations = [f"{term_base} 5e", f"{term_base} build"]
        # Nickname logic (simplified)
        if "College of" in term_base: variations.append(f"{term_base.replace('College of ', '')} Bard")
        if "Circle of" in term_base: variations.append(f"{term_base.replace('Circle of ', '')} Druid")
        if "Way of" in term_base: variations.append(f"{term_base.replace('Way of ', '')} Monk")
        if "Oath of" in term_base: variations.append(f"{term_base.replace('Oath of ', '')} Paladin")
        
    # 2. Spells
    elif cat == 'Spell':
        variations = [f"{term_base} 5e", f"{term_base} dnd"]
        
    # 3. Monster / NPC
    elif cat in ['Monster', 'NPC', 'Villain']:
        variations = [f"{term_base} 5e", f"{term_base} Dnd"]
        
    # 4. Items
    elif cat in ['Item', 'MagicItem']:
        variations = [f"{term_base} 5e", f"{term_base} dnd", f"{term_base} price"]
        
    # 5. Feat / Race / Background
    elif cat in ['Feat', 'Race', 'Background']:
        
        # Special Logic for "Monstrous Races" (Orc, Goblin, Kobold, etc.)
        # User Data: "Orc 5e" = Monster (100), "Orc Race 5e" = Race (25)
        # We must NOT generate "Orc 5e" for the Race category, or it collides/misleads.
        MONSTROUS_RACES = [
            'Orc', 'Goblin', 'Kobold', 'Bugbear', 'Hobgoblin', 
            'Lizardfolk', 'Yuan-Ti', 'Yuan-Ti Pureblood', 'Gnoll'
        ]
        
        if cat == 'Race' and term_base in MONSTROUS_RACES:
            variations = [f"{term_base} Race 5e", f"{term_base} Race Dnd"]
        else:
            variations = [f"{term_base} 5e", f"{term_base} 2024"]
            # For standard races, maybe add "Race" variant too?
            if cat == 'Race':
                variations.append(f"{term_base} Race 5e")
        
    # 6. Ancillary (Influencer, Art, Build, etc.)
    elif cat in ['Influencer', 'Youtube', 'Podcast', 'Website', 'Platform']:
        # Identity-based: use raw name + Dnd name
        variations = [term_base, f"{term_base} Dnd"]
        
    elif cat in ['Art', 'AI', 'Accessory', 'Slang', 'Terminology', 'Concept', 'Rule', 'Mechanic']:
        # Context-based: append Dnd/5e
        variations = [f"{term_base} Dnd", f"{term_base} 5e"]
        
    elif cat == 'Build':
        variations = [f"{term_base} 5e", f"{term_base} Build"]
        
    elif cat in ['Deity', 'Faction', 'Plane', 'Setting', 'Invocation', 'Eldritch Invocation']:
        # Lore-based
        variations = [f"{term_base} 5e", f"{term_base} Dnd"]
        
    elif cat in ['UA Content', 'Unearthed Arcana']:
        # Experimental content
        variations = [f"{term_base} UA", f"{term_base} 5e"]

    elif cat in ['Convention', 'Event']:
        # Real-world events
        variations = [term_base, f"{term_base} Dnd"]

    else:
        # Default Catch-All
        variations = [f"{term_base} Dnd"]
        
    # Deduplicate variations
    variations = sorted(list(set(variations)))
    
    created_at = datetime.datetime.now().isoformat()
    
    for v in variations:
        # Check Collision / Qualification Logic
        final_term = v
        rule = f"standard_{cat.lower()}"
        
        # Check if base term is in MCI (Collision)
        # Note: most variations already append '5e', so they are safe?
        # The user wanted to check generic terms.
        # e.g. "Warden" -> "Warden Dnd" (from refill map)
        
        # Priority: Refill Map (Manual Override from Phase 2.5)
        if term_base in refill_map:
            # If the base matches a refill target, we force the qualified version
            # But wait, refill map maps 'Warden' -> 'Warden Dnd'.
            # If we are generating 'Warden 5e', that's fine.
            # Use 'Warden Dnd' as a replacement for 'Warden build' maybe?
            pass

        # Disambiguation logic:
        # If the term DOES NOT have '5e' or 'dnd' or '2024' (unlikely given rules above, except old nicknames), append Dnd.
        # Actually simplest rule:
        # If term_base in MCI and "dnd" not in v.lower() and "5e" not in v.lower():
        #    Append " Dnd"
        
        # But wait, all my rules above append 5e/dnd/build.
        # "Warden build" -> "Warden Dnd build"? No, "Warden Dnd".
        
        if "build" in v and term_base.lower() in mci_set:
             # "Warden build" -> "Warden Dnd" per user instruction to kill ambiguous builds
             final_term = f"{term_base} Dnd"
             rule = "collision_fix"
        
        expansions.append({
            "term_id": str(uuid.uuid4()), # New unique ID for the search term row
            "original_keyword": term_base,
            "category": cat,
            "search_term": final_term,
            "expansion_rule": rule,
            "is_official": is_official,
            "created_at": created_at
            # is_pilot implicitly False for new ones
        })
        
    return expansions

def run_expansion():
    client = bigquery.Client()
    mci = load_mci()
    refill_map = load_refill_map()
    
    print("Fetching concepts...")
    query = f"SELECT * FROM `{SOURCE_TABLE}`"
    concepts = list(client.query(query).result())
    print(f"Loaded {len(concepts)} concepts.")
    
    # FETCH EXISTING TERMS TO PREVENT DUPLICATES
    print("Fetching existing terms...")
    existing_query = f"SELECT search_term FROM `{DEST_TABLE}`"
    existing_terms = set(row['search_term'] for row in client.query(existing_query).result())
    print(f"Index contains {len(existing_terms)} existing terms.")
    
    all_rows = []
    print("Generating terms...")
    for row in concepts:
        rows = generate_terms(row, mci, refill_map)
        for r in rows:
            if r['search_term'] not in existing_terms:
                all_rows.append(r)
                existing_terms.add(r['search_term']) # Add to local set to prevent dupes within this batch
        
    # Deduplicate by search_term (keep first?)
    # We want to avoid inserting duplicates into the table if they exist.
    # But checking DB is slow.
    # Strategy: Insert All into a Temp Table, then MERGE.
    
    print(f"Generatd {len(all_rows)} total search terms.")
    
    # Batch Insert
    BATCH_SIZE = 5000
    for i in range(0, len(all_rows), BATCH_SIZE):
        batch = all_rows[i:i+BATCH_SIZE]
        print(f"Inserting batch {i}...")
        
        # Insert raw (we rely on logic to be mostly correct, BQ dups are acceptable for now or we handle later)
        # Actually, let's just insert.
        errors = client.insert_rows_json(DEST_TABLE, batch)
        if errors:
            print(f"Errors: {errors[:5]}")
            
    print("Full Expansion Complete.")

if __name__ == "__main__":
    run_expansion()
ï *cascade08ïó*cascade08ó± *cascade08±¥*cascade08¥µ *cascade08µ∂*cascade08∂∑ *cascade08∑∏*cascade08∏º *cascade08ºΩ*cascade08Ωæ *cascade08æ¬*cascade08¬√ *cascade08√≈*cascade08≈∆ *cascade08∆«*cascade08«  *cascade08 À*cascade08ÀÃ *cascade08ÃÕ*cascade08ÕŒ *cascade08Œ—*cascade08—† *cascade08†£*cascade08££ *cascade08£Ï*cascade08Ï° *cascade08°º*cascade08ºŒ *cascade08Œ∏$ *cascade08∏$À&*cascade08À&®' *cascade08®'Å: *cascade08Å:≈<*cascade08≈<Õ= *cascade08Õ=•>*cascade08•>Æ> *cascade08Æ>…>*cascade08…>À> *cascade08À>Õ>*cascade08Õ>Œ> *cascade08Œ>œ>*cascade08œ>–> *cascade08–>⁄>*cascade08⁄>›> *cascade08›>ı>*cascade08ı>ˆ> *cascade08ˆ>í?*cascade08í?ì? *cascade08ì?ú?*cascade08ú?ù? *cascade08ù?£?*cascade08£?áF *cascade08"(5ea7c1b525e148ee9cb36b57c32852aa3816ac022-file:///C:/Users/Yorri/.gemini/expand_full.py:file:///C:/Users/Yorri/.gemini