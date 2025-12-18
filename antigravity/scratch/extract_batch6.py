import csv

def extract_batch6(text_file_path):
    keywords = []
    
    with open(text_file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
        
    current_book = None
    mode = None
    
    # The Gunslinger Class: Valda's Spire of Secrets
    # The Crooked Moon Part One: Player Options & Campaign Setting
    # The Crooked Moon Part Two: Monsters & Adventure Campaign
    # Cthulhu by Torchlight
    # Abomination Vaults
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
            
        if line == "=== The Gunslinger Class: Valda's Spire of Secrets ===":
            current_book = "The Gunslinger Class"
            mode = None
            continue
        if line == "=== The Crooked Moon Part One: Player Options & Campaign Setting ===":
            current_book = "The Crooked Moon"
            mode = None
            continue
        if line == "=== The Crooked Moon Part Two: Monsters & Adventure Campaign ===":
            current_book = "The Crooked Moon" # Same book, different part
            mode = None
            continue
        if line == "=== Cthulhu by Torchlight ===":
            current_book = "Cthulhu by Torchlight"
            mode = None
            continue
        if line == "=== Abomination Vaults ===":
            current_book = "Abomination Vaults"
            mode = None
            continue
            
        # --- Gunslinger Class ---
        if current_book == "The Gunslinger Class":
            if line in ["Deadeye", "High Roller", "Secret Agent", "Spellslinger", "Trick Shot", "White Hat"]:
                keywords.append([line, "Subclass", "Generic", current_book, "2024"]) # User note: 2024 version

        # --- The Crooked Moon ---
        if current_book == "The Crooked Moon":
            # Locations
            if line in ["Druskenvald", "Ardengloom", "Astramar", "Bubonia", "Chernabos", "Edwardia", "Enoch", "Kalero", "Nerukhet", "Olmarsh", "Pholsense", "Picco", "Syndramas", "Zulrogg", "Wickermoor Hollow", "The Ghostlight Express", "Wickermoor Village", "The Crooked House", "Fields of the Crow", "The Crimson Monastery", "The Drowned Crossroads", "Skitterdeep Mine", "Memory’s Rest", "Hartsblight Forest", "Maidenmist Cemetery", "The Roving Rookery", "Fool’s Day", "The Crooked Nightmare", "Wicker’s Vigil", "The Crooked Tree"]:
                keywords.append([line, "Location", "Druskenvald", current_book, "2024"])
            
            # Races
            if line in ["Ashborn", "Azureborn", "Bogborn", "Curseborn", "Deepborn", "Gnarlborn", "Graveborn", "Harvestborn", "Plagueborn", "Relicborn", "Silkborn", "Stoneborn", "Threadborn"]:
                keywords.append([line, "Race", "Druskenvald", current_book, "2024"])
                
            # Subclasses
            if "Path of" in line or "College of" in line or "Domain" in line or "Circle of" in line or "Barrow Guard" in line or "Warrior of" in line or "Oath of" in line or "Grim Harbinger" in line or "Sinner" in line or "Crimson Sorcery" in line or "Patron" in line or "Occultist" in line or "Philosopher" in line:
                keywords.append([line, "Subclass", "Druskenvald", current_book, "2024"])
                
            # Monsters
            if line == "Appendix A: Bestiary" or line == "Bestiary":
                mode = "Monster"
                continue
            if mode == "Monster":
                if "Appendix" in line or "Credits" in line:
                    mode = None
                    continue
                keywords.append([line, "Monster", "Druskenvald", current_book, "2024"])

        # --- Cthulhu by Torchlight ---
        if current_book == "Cthulhu by Torchlight":
            if line == "(No TOC provided, searching web)":
                # Manual additions from research
                cthulhu_keywords = [
                    ["Apocalypse Domain", "Subclass", "Generic", current_book, "2024"],
                    ["Warrior of Cosmic Balance", "Subclass", "Generic", current_book, "2024"],
                    ["Oath of the Guardian", "Subclass", "Generic", current_book, "2024"],
                    ["Trail Warden", "Subclass", "Generic", current_book, "2024"],
                    ["Shadow Stalker", "Subclass", "Generic", current_book, "2024"],
                    ["Exalted Assembly of the Feline Court", "Subclass", "Generic", current_book, "2024"],
                    ["Bibliomancy", "Subclass", "Generic", current_book, "2024"],
                    ["Spell Scorned Barbarian", "Subclass", "Generic", current_book, "2024"],
                    ["Hungering Dark Sorcerer", "Subclass", "Generic", current_book, "2024"],
                    ["Breath of the Deep", "Spell", "Generic", current_book, "2024"],
                    ["Body Warping of Gorgoroth", "Spell", "Generic", current_book, "2024"],
                    ["Cthulhu", "Monster", "Generic", current_book, "2024"],
                    ["Mythos Tomes", "Item", "Generic", current_book, "2024"]
                ]
                keywords.extend(cthulhu_keywords)

        # --- Abomination Vaults ---
        if current_book == "Abomination Vaults":
            # Locations
            if line in ["Otari", "Gauntlight", "The Forgotten Dungeon", "Cult of the Canker", "Long Dream the Dead", "Into the Training Grounds", "Experiments in Flesh", "Soul Keepers", "Decaying Gardens", "On the Hunt", "To Draw the Baleful Glare"]:
                 keywords.append([line, "Location", "Golarion", current_book, "2024"]) # User note: 2024 update on DDB
            
            # Items
            if line in ["Drover’s Band", "Edge of Dissolution", "Folding Drums", "Ichthyosis Mutagen", "Lantern of Empty Light", "The Whispering Reeds", "Cooperative Blade", "Hunter’s Brooch", "Shadowed Thieves’ Tools", "Thresholds of Truth", "Crimson Fulcrum Lens", "Ebon Fulcrum Lens", "Emerald Fulcrum Lens", "Fulcrum Lattice", "Ochre Fulcrum Lens"]:
                keywords.append([line, "Item", "Golarion", current_book, "2024"])
                
            # Spells
            if line in ["Awaken Portal", "Daydreamer’s Curse", "Ectoplasmic Expulsion", "Outcast’s Curse", "Sage’s Curse", "Worm’s Repast"]:
                keywords.append([line, "Spell", "Golarion", current_book, "2024"])
                
            # Monsters/NPCs
            if line == "Monsters and NPCs":
                mode = "Monster"
                continue
            if mode == "Monster":
                if "(NPC)" in line:
                    keywords.append([line.replace(" (NPC)", ""), "NPC", "Golarion", current_book, "2024"])
                else:
                    keywords.append([line, "Monster", "Golarion", current_book, "2024"])

    return keywords

if __name__ == "__main__":
    results = extract_batch6(r'c:\Users\Yorri\.gemini\antigravity\scratch\batch6_text.txt')
    
    with open(r'c:\Users\Yorri\.gemini\antigravity\scratch\batch6_extracted.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Keyword', 'Category', 'World', 'Source', 'Ruleset'])
        writer.writerows(results)
        
    print(f"Extracted {len(results)} keywords.")
