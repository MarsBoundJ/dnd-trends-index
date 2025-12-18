import csv

def parse_flee_mortals(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    keywords = []
    source = "Flee, Mortals!"
    world = "Generic"
    
    # List of lines to ignore (headers, non-content)
    ignore_list = [
        "Flee, Mortals!",
        "An epic bestiary containing rules",
        "View Cover Art",
        "Contents",
        "Foreword",
        "Introduction",
        "New Rules and Styles",
        "Action-Oriented Creatures",
        "Companion Creatures",
        "Retainers",
        "Minions",
        "The Timescape",
        "Using Creature Roles",
        "Encounter Building",
        "Creatures by CR",
        "Converting Ancestries",
        "Ch. 1: Creatures (A–D)",
        "Ch. 1: Creatures (E–K)",
        "Ch. 1: Creatures (L–S)",
        "Ch. 1: Creatures (T–Z)",
        "Ch. 2: Environments",
        "Environmental Features",
        "Ch. 3: Villain Parties",
        "Portraying Villainy",
        "Background",
        "Magic Items",
        "Villain Actions", # Partial match check later
        "Ch. 4: New Psionic Powers",
        "Credits",
        "Unique Monsters"
    ]

    # Categories mapping
    # Most are monsters.
    # Villain Parties are Factions or Groups? "Villain Party" isn't a standard category in the CSV yet.
    # CSV Categories: NPC, Villain, Location, Faction, Item, Mechanic, Species, Monster.
    
    # I'll treat "Villain Parties" names as "Faction" or "Villain"?
    # "Grave Order" -> Faction (or Villain Group)
    # "Anthracia..." -> Villain (Unique Monster)
    
    # State machine or context?
    # The list is mostly monsters.
    # "Ch. 3: Villain Parties" starts the villain parties section.
    # "Unique Monsters" starts the unique monsters section.
    
    current_category = "Monster"
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
            
        # Check ignore list
        if any(line.startswith(ignore) for ignore in ignore_list):
            continue
        if "Villain Actions" in line:
            continue
            
        # Context switching
        if line == "Ch. 3: Villain Parties":
            current_category = "Faction" # Or Villain?
            continue
        if line == "Unique Monsters":
            current_category = "Villain"
            continue
            
        # Special handling for Villain Parties section structure
        # The TOC lists: "Grave Order", "Background", "Magic Items", "Grave Order Villain Actions"
        # I filtered out Background, Magic Items, Villain Actions.
        # So "Grave Order" remains.
        
        # Special handling for Environments section
        # "Cave", "Fossil Cryptic", ...
        # "Cave" is a location type, but here it's a header for monsters found in caves.
        # I should probably ignore the environment headers if I can identify them.
        # Environment headers: Cave, Enchanted Forest, Graveyard and Tomb, Road, Ruined Keep, Sewer, Swamp, Underground.
        environments = [
            "Cave", "Enchanted Forest", "Graveyard and Tomb", "Road", 
            "Ruined Keep", "Sewer", "Swamp", "Underground"
        ]
        if line in environments:
            continue

        # Add keyword
        # If it's a Villain Party name, category is Faction?
        # Let's check the CSV for "Villain" vs "Faction".
        # "Cult of the Dead Three" -> Faction.
        # "Grave Order" -> Faction.
        
        # If it's a Unique Monster, category is Villain?
        # "Strahd von Zarovich" -> Villain.
        # "Anthracia..." -> Villain.
        
        # Default is Monster.
        
        # Refine Context
        # The script iterates top to bottom.
        # I need to detect when we are in "Villain Parties" section.
        # But I filtered out the header "Ch. 3...".
        # Wait, I put "Ch. 3..." in ignore list, so `line == "Ch. 3..."` check above will fail if I don't handle it before ignore check.
        # I should remove section headers from ignore list or handle them before.
        
        pass 

    # Let's rewrite the loop with better logic
    
    keywords_list = []
    
    mode = "Monster" # Default
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
            
        if line.startswith("Ch. 3: Villain Parties"):
            mode = "Faction"
            continue
        if line == "Unique Monsters":
            mode = "Villain"
            continue
        if line.startswith("Ch. 4:"):
            mode = "Skip"
            continue
        if line == "Credits":
            mode = "Skip"
            continue
            
        if mode == "Skip":
            continue
            
        # Ignore list checks
        is_ignored = False
        for ignore in ignore_list:
            if line == ignore or line.startswith(ignore): # Exact match or startswith for chapters
                is_ignored = True
                break
        if "Villain Actions" in line:
            is_ignored = True
            
        if line in ["Cave", "Enchanted Forest", "Graveyard and Tomb", "Road", "Ruined Keep", "Sewer", "Swamp", "Underground"]:
            is_ignored = True
            
        if is_ignored:
            continue
            
        # Add to list
        keywords_list.append([line, mode, world, source])
        
    return keywords_list

if __name__ == "__main__":
    results = parse_flee_mortals('c:\\Users\\Yorri\\.gemini\\antigravity\\scratch\\flee_mortals.txt')
    
    # Print first 10 and last 10 to verify
    print("First 10:")
    for r in results[:10]:
        print(r)
    print("\nLast 10:")
    for r in results[-10:]:
        print(r)
        
    # Write to CSV (append)
    # Actually, I'll just print them for now, or write to a temp csv to check.
    with open('c:\\Users\\Yorri\\.gemini\\antigravity\\scratch\\flee_mortals_extracted.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Keyword', 'Category', 'World', 'Source'])
        writer.writerows(results)
