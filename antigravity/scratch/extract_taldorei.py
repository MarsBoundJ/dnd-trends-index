import csv

def parse_taldorei(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    keywords = []
    source = "Tal’Dorei Campaign Setting Reborn"
    world = "Exandria" # Tal'Dorei is in Exandria
    
    mode = None
    
    # Categories:
    # Factions: Under "Factions and Societies"
    # Locations: Under "Ch. 3: Tal’Dorei Gazetteer"
    # Backgrounds: Under "Backgrounds"
    # Monsters/NPCs: Under "Allies and Adversaries"
    
    factions_section = False
    gazetteer_section = False
    backgrounds_section = False
    monsters_section = False
    
    ignore_list = [
        "Contents", "Preface", "Ch. 1", "Ch. 2", "Ch. 3", "Ch. 4", "Ch. 5", "Ch. 6",
        "Welcome to Tal’Dorei", "Allegiances of Tal’Dorei", "Tal’Dorei Gazetteer",
        "Character Options", "Game Master’s Toolkit", "Allies and Adversaries of Tal’Dorei",
        "Credits", "See below", "Nonplayer Creatures", "Playing in a Tal’Dorei Campaign",
        "Races and Cultures", "Subclasses", "Creating Adventures", "Tal’Dorei Treasures",
        "Magic Items", "Tools of the Ashari", "Vestiges of Divergence", "Optional Campaign Rules",
        "Pantheon of Exandria", "Prime Deities", "Betrayer Gods", "Lesser Idols",
        "Lands of Tal’Dorei", "What’s in This Book?", "Calendar, Time, and the Cosmos",
        "A History of Tal’Dorei", "Running a Tal’Dorei Campaign", "Hemocraft",
        "New Feats", "Supernatural Blessing: Fate-Touched", "Mixed Ancestry", "Other Races",
        "Dragonblood", "Dwarves", "Elemental Ancestry", "Elves", "Firbolgs", "Gnomes",
        "Goblinkin", "Half-Giants", "Halflings", "Humans", "Orcs", "Tieflings",
        "Barbarian", "Bard", "Cleric", "Druid", "Monk", "Paladin", "Sorcerer", "Wizard"
    ]
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
            
        # Mode switching
        if line == "Factions and Societies":
            mode = "Faction"
            continue
        if line == "Ch. 3: Tal’Dorei Gazetteer": # This line is in ignore list but I check it before ignore?
            # Actually I put "Ch. 3" in ignore list as partial match maybe?
            # Let's be careful.
            pass
            
        if "Ch. 3: Tal’Dorei Gazetteer" in line:
            mode = "Location"
            continue
        if line == "Backgrounds":
            mode = "Mechanic" # Backgrounds are mechanics? Or just "Background"
            continue
        if line == "Allies and Adversaries":
            mode = "Monster"
            continue
        if line == "Vox Machina":
            mode = "NPC" # These are specific NPCs
            continue
            
        # Stop conditions for sections
        if mode == "Faction" and "Ch. 3" in line:
            mode = None
        if mode == "Location" and "Ch. 4" in line:
            mode = None
        if mode == "Mechanic" and "New Feats" in line:
            mode = None
        
        # Check ignore list
        is_ignored = False
        if line in ignore_list:
            is_ignored = True
        if any(line.startswith(x) for x in ["Ch. ", "Step "]):
            is_ignored = True
            
        if is_ignored:
            continue
            
        # Add keywords based on mode
        if mode == "Faction":
            keywords.append([line, "Faction", world, source])
        elif mode == "Location":
            keywords.append([line, "Location", world, source])
        elif mode == "Mechanic":
            # "Ashari", "Clasp Member" -> These are backgrounds.
            # "Ashari" is also a faction.
            keywords.append([line, "Mechanic", world, source])
        elif mode == "Monster":
            # "Adranachs" -> Monster
            # "Jourrael, the Caedogeist" -> Villain?
            category = "Monster"
            if "Jourrael" in line or "Champion of Ravens" in line:
                category = "Villain"
            keywords.append([line, category, world, source])
        elif mode == "NPC":
            keywords.append([line, "NPC", world, source])
            
    return keywords

if __name__ == "__main__":
    results = parse_taldorei(r'c:\Users\Yorri\.gemini\antigravity\scratch\taldorei.txt')
    
    with open(r'c:\Users\Yorri\.gemini\antigravity\scratch\taldorei_extracted.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Keyword', 'Category', 'World', 'Source'])
        writer.writerows(results)
        
    print(f"Extracted {len(results)} keywords.")
