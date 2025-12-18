import csv

def extract_batch7(text_file_path):
    keywords = []
    
    with open(text_file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
        
    current_book = None
    mode = None
    
    # Grim Hollow Player’s Guide
    # Grim Hollow Campaign Guide
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
            
        if line == "=== Grim Hollow Player’s Guide ===":
            current_book = "Grim Hollow Player’s Guide"
            mode = None
            continue
        if line == "=== Grim Hollow Campaign Guide ===":
            current_book = "Grim Hollow Campaign Guide"
            mode = None
            continue
            
        # --- Grim Hollow Player’s Guide ---
        if current_book == "Grim Hollow Player’s Guide":
            # Heritages (Races)
            if line == "Grim Hollow Heritages":
                mode = "Race"
                continue
            if mode == "Race":
                if "Chapter" in line:
                    mode = None
                    continue
                if line not in ["Common Heritages", "Rare Heritages", "Eldritch Heritages"]:
                     keywords.append([line, "Race", "Etharis", current_book, "2024"]) # User note: 2024 update
            
            # Classes
            if line == "Monster Hunter":
                keywords.append([line, "Class", "Etharis", current_book, "2024"])
                
            # Subclasses
            if "Guild" in line and "Monster Hunter" not in line:
                keywords.append([line, "Subclass", "Etharis", current_book, "2024"])
            if "Subclasses" in line and "Chapter" not in line:
                 # The TOC lists "Barbarian Subclasses" etc., but not the specific names. 
                 # I will add the generic category markers, but ideally I would have the names.
                 # Given the user provided TOC is high level, I will stick to what is there or known ones if mentioned.
                 # Actually, the user text has "Carver Guild", "Devourer Guild" etc. which are subclasses of Monster Hunter.
                 pass

            # Transformations
            if line in ["Aberrant Horror", "Fey", "Fiend", "Hag", "Lich", "Lycanthrope", "Ooze", "Primordial", "Seraph", "Shadowsteel Ghoul", "Specter", "Vampire"]:
                keywords.append([line, "Mechanic", "Etharis", current_book, "2024"]) # Transformations are a key mechanic
                
            # Realms (Locations)
            if line in ["The Bürach Empire", "The Ostoyan Empire", "The Charneault Kingdom", "The Castinellan Provinces", "The Valikan Clans", "The City of Liesech", "The City of Morencia"]:
                keywords.append([line, "Location", "Etharis", current_book, "2024"])
                
            # Factions
            if line in ["The Arcanist Inquisition", "The Augustine Trading Company", "The Company of Free Swords", "The Crimson Court", "The Ebon Syndicate", "Monster Hunter Guilds", "Morbus Doctore", "Order of Dawn", "The Prismatic Circle", "The Thaumaturge", "The Watchers of the Faithful"]:
                keywords.append([line, "Faction", "Etharis", current_book, "2024"])

        # --- Grim Hollow Campaign Guide ---
        if current_book == "Grim Hollow Campaign Guide":
            # Locations (More detailed here)
            if line in ["The Bürach Empire", "The City of Stehlenwald", "The Ostoyan Empire", "The City of Fallowheart", "The Free City of Malkovia", "The Charneault Kingdom", "Castle Lamesdhonneur", "Tol Leyemil", "The Castinellan Provinces", "Ember Cairn", "The Valikan Clans", "Cold Iron Keep", "The City of Liesech", "The City of Morencia"]:
                keywords.append([line, "Location", "Etharis", current_book, "2024"])
            
            # Folk (Races)
            if line in ["Dragonborn", "Dwarves", "Elves", "Gnomes", "Halflings", "Humans", "Dreamers", "Grudgels", "Laneshi", "Ogresh", "Accursed", "Arisen", "Dhampir", "Disembodied", "Downcast", "Wechselkind", "Wulven"]:
                keywords.append([line, "Race", "Etharis", current_book, "2024"])
                
            # Factions (Duplicates possible, but adding for coverage)
            if line in ["Augustine Trading Company", "Company of Free Swords", "Ebon Syndicate", "Thaumaturge", "Watchers of the Faithful", "The Arcanist Inquisition", "Crimson Court", "Monster Hunter Guilds", "Morbus Doctore", "Order of Dawn", "Prismatic Circle"]:
                keywords.append([line, "Faction", "Etharis", current_book, "2024"])
                
            # Divinity/Entities
            if line in ["Arch Seraphs", "Arch Daemons", "The Aether Kindred", "Primordials", "The Faerie Courts"]:
                keywords.append([line, "Deity", "Etharis", current_book, "2024"])
                
            # Items
            if line == "The Four Sacred Artifacts":
                keywords.append([line, "Item", "Etharis", current_book, "2024"])

    return keywords

if __name__ == "__main__":
    results = extract_batch7(r'c:\Users\Yorri\.gemini\antigravity\scratch\batch7_text.txt')
    
    with open(r'c:\Users\Yorri\.gemini\antigravity\scratch\batch7_extracted.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Keyword', 'Category', 'World', 'Source', 'Ruleset'])
        writer.writerows(results)
        
    print(f"Extracted {len(results)} keywords.")
