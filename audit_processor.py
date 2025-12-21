import csv

# Files
ALL_TERMS_FILE = 'pilot_audit_full.csv'
REVIEWED_FILE = 'pilot_audit_reviewed.csv'
OUTPUT_PROPOSAL = 'audit_proposals.csv'

# Classes for the Nickname glitch check (Safe to keep if not Alernate)
CLASSES = ['Cleric', 'Bard', 'Druid', 'Monk', 'Paladin', 'Wizard', 'Fighter', 'Rogue', 'Ranger', 'Barbarian', 'Sorcerer', 'Warlock', 'Artificer']

# Broad words based on user drops and inference
GENERIC_WORDS = {
    # User's direct drops
    'air', 'water', 'athlete', 'awakened',
    # Inferred generic professions/roles (likely too broad)
    'blacksmith', 'bomber', 'captain', 'celebrity', 'chieftain',
    'merchant', 'messenger', 'outlaw', 'physician', 'savage', 
    'scholar', 'scout', 'stalker', 'nomad', 'philosopher',
}

def get_base_term(search_term):
    # Removes common suffixes to find the base concept
    term = search_term.replace(' 5e', '').replace(' 2024', '').replace(' build', '').strip()
    return term

def run_audit():
    # 1. Load reviewed terms (to skip them)
    reviewed_terms = set()
    with open(REVIEWED_FILE, encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            reviewed_terms.add(row['search_term'].strip().lower())
    
    # 2. Process all terms
    proposals = []
    
    with open(ALL_TERMS_FILE, encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            term = row['search_term'].strip()
            term_lower = term.lower()
            
            if term_lower in reviewed_terms:
                continue
                
            base = get_base_term(term)
            base_lower = base.lower()
            
            status = "Proposed KEEP"
            reason = ""
            
            # RULE 1: Alternate Class Glitch
            if term.startswith("Alternate "):
                for c in CLASSES:
                    if term.startswith(f"Alternate {c}"):
                        status = "Proposed DROP"
                        reason = f"Glitch in Nickname algo: was to shorten Alternate {base.split()[-1]}"
                        break
            
            # RULE 2: Broad Terms
            if status == "Proposed KEEP" and base_lower in GENERIC_WORDS:
                status = "Proposed DROP"
                reason = f"Too broad: generic English/D&D word '{base}'"
            
            proposals.append({
                'search_term': term,
                'status': status,
                'reason': reason,
                'total_interest': row['total_interest'],
                'weeks': row['weeks']
            })
                
    # 3. Write outputs
    headers = ['search_term', 'status', 'reason', 'total_interest', 'weeks']
    
    with open(OUTPUT_PROPOSAL, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(proposals)
        
    print(f"Propsoal Generation Complete.")
    print(f"Skipped: {len(reviewed_terms)}")
    print(f"Total Proposed: {len(proposals)}")
    print(f"Drops: {len([p for p in proposals if 'DROP' in p['status']])}")

if __name__ == "__main__":
    run_audit()
