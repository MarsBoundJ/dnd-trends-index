import csv

def parse_wildemount(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    keywords = []
    source = "Explorer's Guide to Wildemount"
    world = "Exandria" # Wildemount is in Exandria
    
    mode = None
    
    # Categories mapping
    # Factions: Ch. 2
    # Locations: Ch. 3 and Maps section
    # Subclasses: Under Subclasses
    # Monsters: Ch. 7
    
    ignore_list = [
        "Contents", "Preface", "Welcome to Wildemount", "A New D&D Setting", "Nations of Wildemount",
        "What's in This Book", "War!", "Calendar and Time", "Moons of Exandria", "Daily Life in Wildemount",
        "Ch. 1: Story of Wildemount", "History of Wildemount", "Wildemount after the Calamity",
        "Pantheon of Exandria", "Prime Deities", "Betrayer Gods", "Lesser Idols",
        "Ch. 2: Factions and Societies", "Ch. 3: Wildemount Gazetteer", "Ch. 4: Character Options",
        "Races", "Dwarves | Elves | Halflings | Humans | Aarakocra | Aasimar | Dragonborn | Firbolgs | Genasi | Gnomes | Goblinkin | Goliaths | Half-Elves | Kenku | Orcs and Half-Orcs | Tabaxi | Tieflings | Tortles",
        "Subclasses", "Dunamancy Spells", "Heroic Chronicle", "Backgrounds", "Adapting Backgrounds",
        "Ch. 5: Adventures in Wildemount", "Using These Adventures", "Ch. 6: Wildemount Treasures",
        "Magic Items of Wildemount", "Vestiges of Divergence", "Arms of the Betrayers",
        "Ch. 7: Wildemount Bestiary", "Stat Blocks by Creature Type", "Wildemount NPCs",
        "Glossary", "Maps", "The Continent of Wildemount"
    ]
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
            
        # Mode switching
        if line == "Ch. 2: Factions and Societies":
            mode = "Faction"
            continue
        if line == "Ch. 3: Wildemount Gazetteer":
            mode = "Location"
            continue
        if line == "Subclasses":
            mode = "Subclass"
            continue
        if line == "Backgrounds":
            mode = "Mechanic" # Backgrounds
            continue
        if line == "Ch. 7: Wildemount Bestiary":
            mode = "Monster"
            continue
        if line == "Maps":
            mode = "Location"
            continue
            
        # Stop conditions
        if mode == "Faction" and "Ch. 3" in line:
            mode = None
        if mode == "Location" and "Ch. 4" in line:
            mode = None
        if mode == "Subclass" and "Dunamancy Spells" in line:
            mode = None
        if mode == "Mechanic" and "Ch. 5" in line:
            mode = None
        if mode == "Monster" and "Glossary" in line:
            mode = None
            
        # Check ignore list
        if line in ignore_list:
            continue
            
        # Maps section has "3.1: Menagerie Coast" format
        if mode == "Location" and ":" in line and any(c.isdigit() for c in line):
            # Extract name after colon
            parts = line.split(":", 1)
            if len(parts) > 1:
                name = parts[1].strip()
                keywords.append([name, "Location", world, source])
            continue
            
        # Add keywords based on mode
        if mode == "Faction":
            keywords.append([line, "Faction", world, source])
        elif mode == "Location":
            keywords.append([line, "Location", world, source])
        elif mode == "Subclass":
            # "Fighter: Echo Knight" -> "Echo Knight" (Subclass)
            if ":" in line:
                name = line.split(":")[1].strip()
                keywords.append([name, "Subclass", world, source])
            else:
                keywords.append([line, "Subclass", world, source])
        elif mode == "Mechanic":
            keywords.append([line, "Mechanic", world, source])
        elif mode == "Monster":
            keywords.append([line, "Monster", world, source])
            
    return keywords

if __name__ == "__main__":
    results = parse_wildemount(r'c:\Users\Yorri\.gemini\antigravity\scratch\wildemount.txt')
    
    with open(r'c:\Users\Yorri\.gemini\antigravity\scratch\wildemount_extracted.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Keyword', 'Category', 'World', 'Source'])
        writer.writerows(results)
        
    print(f"Extracted {len(results)} keywords.")
