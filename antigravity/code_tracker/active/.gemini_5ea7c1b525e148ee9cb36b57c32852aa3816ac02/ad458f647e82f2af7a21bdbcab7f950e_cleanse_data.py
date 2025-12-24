®import csv
import json

MCI_FILE = 'master_collision_index.json'
DATA_FILE = 'pilot_audit_full.csv'
OUTPUT_PURGE = 'purge_list.csv'
OUTPUT_REFILL = 'refill_list.csv'

def get_base_term(search_term):
    # Removes common suffixes to find the base concept for collision checking
    term = search_term.replace(' 5e', '').replace(' 2024', '').replace(' build', '').strip()
    return term.lower()

def run_cleanse():
    # 1. Load MCI
    with open(MCI_FILE, 'r', encoding='utf-8') as f:
        mci = json.load(f)
    print(f"Loaded MCI with {len(mci['all_combined'])} terms.")
    
    # Fast lookup set
    collision_set = set(mci['all_combined'])
    
    # 2. Process Data
    purge_list = []
    refill_list = []
    clean_count = 0
    
    with open(DATA_FILE, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            term = row['search_term'].strip()
            # BQ sometimes returns "Total Interest" as float or string
            try:
                interest = float(row['total_interest'])
            except:
                interest = 0
                
            base_lower = get_base_term(term)
            
            # Check Collision
            # We check if the BASE term is in the collision index
            # OR if the full term is simply "generic"
            is_collision = base_lower in collision_set
            
            # Also check for single-word generic terms that might not be in MCI (just in case)
            if len(base_lower.split()) == 1 and interest < 200:
                 # Aggressive check: if it's a single word and not explicitly whitelisted, call it a collision/risk
                 pass 

            if is_collision:
                if interest < 200:
                    # DROP
                    row['reason'] = f"Collision '{base_lower}' + Low Volume ({interest})"
                    purge_list.append(row)
                else:
                    # QUALIFY
                    row['reason'] = f"Collision '{base_lower}' + Hugh Volume ({interest})"
                    row['new_term'] = f"{base_lower.title()} Dnd" # Proposed new term
                    purge_list.append(row) # ensure ID is removed
                    refill_list.append(row) # add to refill queue
            else:
                clean_count += 1
                
    # 3. Write Outputs
    keys = ['search_term', 'total_interest', 'reason']
    with open(OUTPUT_PURGE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        for p in purge_list:
            writer.writerow({k: p.get(k, '') for k in keys})
            
    keys_refill = ['search_term', 'total_interest', 'reason', 'new_term']
    with open(OUTPUT_REFILL, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=keys_refill)
        writer.writeheader()
        for r in refill_list:
            writer.writerow({k: r.get(k, '') for k in keys_refill})
            
    print("-" * 30)
    print(f"Cleanse Analysis Complete")
    print(f"Total Terms: {clean_count + len(purge_list)}")
    print(f"Clean Terms: {clean_count}")
    print(f"Total Purge: {len(purge_list)} (Drops + Qualifies)")
    print(f"  -> To Drop: {len(purge_list) - len(refill_list)}")
    print(f"  -> To Refill (Qualify): {len(refill_list)}")

if __name__ == "__main__":
    run_cleanse()
®"(5ea7c1b525e148ee9cb36b57c32852aa3816ac022.file:///c:/Users/Yorri/.gemini/cleanse_data.py:file:///c:/Users/Yorri/.gemini