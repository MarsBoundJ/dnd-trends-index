import csv

def extract_batch2(text_file_path):
    keywords = []
    
    with open(text_file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
        
    current_book = None
    mode = None
    
    # Grim Hollow: Lairs of Etharis
    # Dungeons of Drakkenheim
    # Humblewood Campaign Setting
    # Where Evil Lives
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
            
        if line == "=== Grim Hollow: Lairs of Etharis ===":
            current_book = "Grim Hollow: Lairs of Etharis"
            mode = None
            continue
        if line == "=== Dungeons of Drakkenheim ===":
            current_book = "Dungeons of Drakkenheim"
            mode = None
            continue
        if line == "=== Humblewood Campaign Setting ===":
            current_book = "Humblewood Campaign Setting"
            mode = None
            continue
        if line == "=== Where Evil Lives ===":
            current_book = "Where Evil Lives"
            mode = None
            continue
            
        # --- Grim Hollow: Lairs of Etharis ---
        if current_book == "Grim Hollow: Lairs of Etharis":
            # Lairs are numbered chapters 1-20
            if line[0].isdigit() and ":" in line:
                # "1: Bone House" -> "Bone House"
                name = line.split(":", 1)[1].strip()
                keywords.append([name, "Location", "Etharis", current_book, "2014"])
            
            # Monsters
            if line == "Etharis Creatures":
                mode = "Monster"
                continue
            if mode == "Monster":
                keywords.append([line, "Monster", "Etharis", current_book, "2014"])

        # --- Dungeons of Drakkenheim ---
        if current_book == "Dungeons of Drakkenheim":
            # Factions (Chapter 3)
            if line in ["The Amethyst Academy", "The Followers of the Falling Fire", "The Hooded Lanterns", "Knights of the Silver Order", "Queen’s Men"]:
                keywords.append([line, "Faction", "Drakkenheim", current_book, "2014"])
            
            # Locations (Chapters 4-9)
            if line in ["Emberwood Village", "Black Ivory Inn", "Buckledown Row", "Chapel of Saint Brenna", "Eckerman Mill", "Rat's Nest Tavern", "Reed Manor", "Shrine of Morrigan", "Smithy on the Scar", "Stick’s Ferry", "Cosmological Clocktower", "The Crater", "Kleinberg Estate", "Old Town Cistern", "Queen’s Park", "Royal Grotto", "Rose Theatre", "Saint Vitruvio’s Cathedral", "Slaughterstone Square", "Camp Dawn", "Court of Thieves", "Drakkenheim Garrison", "Inscrutable Tower", "Saint Selina’s Monastery", "Castle Drakken"]:
                keywords.append([line, "Location", "Drakkenheim", current_book, "2014"])
                
            # Monsters (Appendix A)
            if line == "Appendix A: Monsters":
                mode = "Monster"
                continue
            if line == "Appendix B: Nonplayer Characters":
                mode = "NPC"
                continue
            
            if mode == "Monster":
                if line in ["Mechanical Features", "Monster Modifications", "New Monsters", "Contaminated Elementals", "Legendary Creatures"]:
                    continue
                keywords.append([line, "Monster", "Drakkenheim", current_book, "2014"])
                
            if mode == "NPC":
                if line in ["Delerium, Magic & Spells"]:
                    mode = None
                    continue
                keywords.append([line, "NPC", "Drakkenheim", current_book, "2014"])

        # --- Humblewood ---
        if current_book == "Humblewood Campaign Setting":
            # Races
            if line in ["The Birdfolk", "The Humblefolk"]:
                # These are categories, but specific races are listed in Appendix B or implied.
                # The TOC lists "Races of the Wood" then "The Birdfolk", "The Humblefolk".
                # I'll add them as Races.
                keywords.append([line, "Race", "Humblewood", current_book, "2014"])
                
            # Locations (Chapter 3)
            if line in ["Alderheart", "Ashbarrow", "The Avium", "Brackenmill", "The Crest", "Marshview", "Meadowfen", "Mokk Fields", "Saltar’s Port", "Scorched Grove", "Talongrip Coast", "Winnowing Reach"]:
                keywords.append([line, "Location", "Humblewood", current_book, "2014"])
                
            # Monsters (Appendix A)
            if line == "Appendix A: Bestiary":
                mode = "Monster"
                continue
            if line == "Appendix B: Nonplayer Characters":
                mode = "NPC"
                continue
                
            if mode == "Monster":
                keywords.append([line, "Monster", "Humblewood", current_book, "2014"])
                
            if mode == "NPC":
                if line in ["Appendix C: Creating NPCs", "Adventure NPCs", "Birdfolk", "Cervan", "Corvum", "Gallus", "Hedge", "Jerbeen", "Luma", "Mapach", "Raptor", "Strig", "The Tenders", "Vulpin"]:
                    continue # Skip headers
                keywords.append([line, "NPC", "Humblewood", current_book, "2014"])

        # --- Where Evil Lives ---
        if current_book == "Where Evil Lives":
            # The TOC lists Lairs and Hoards.
            # "Jagged Edge Hideaway" -> Location
            # "Queen Bargnot’s Hoard" -> Implies "Queen Bargnot" is the boss (Villain/Monster)
            
            if "Hideaway" in line or "Tree" in line or "Hut" in line or "Mill" in line or "Camp" in line or "Library" in line or "Keep" in line or "Enclave" in line or "Ruins" in line or "Tower" in line or "Canyon" in line or "Tomb" in line or "Excrescence" in line or "Services" in line or "Shadowkeep" in line or "Rest" in line or "Hollow" in line or "Eyes" in line or "Cavern" in line or "Mount" in line or "Reliquary" in line or "Boughs" in line:
                 if "Story Hooks" not in line and "Features" not in line and "Areas" not in line and "Stat Blocks" not in line:
                    keywords.append([line, "Location", "Generic", current_book, "2014"])
            
            if "’s Hoard" in line or "'s Hoard" in line:
                # Extract name
                name = line.replace("’s Hoard", "").replace("'s Hoard", "").strip()
                keywords.append([name, "Villain", "Generic", current_book, "2014"])

    # --- Grim Hollow: The Players Guide (Manual List from Screenshot) ---
    gh_book = "Grim Hollow: The Players Guide"
    gh_keywords = [
        # Races
        ["Wechselkind", "Race", "Etharis", gh_book, "2014"],
        ["Laneshi", "Race", "Etharis", gh_book, "2014"],
        ["Ogresh", "Race", "Etharis", gh_book, "2014"],
        ["The Downcast", "Race", "Etharis", gh_book, "2014"],
        ["Dreamers", "Race", "Etharis", gh_book, "2014"],
        ["The Disembodied", "Race", "Etharis", gh_book, "2014"],
        # Subclasses
        ["Path of the Fractured", "Subclass", "Etharis", gh_book, "2014"],
        ["Path of the Primal Spirit", "Subclass", "Etharis", gh_book, "2014"],
        ["College of Adventurers", "Subclass", "Etharis", gh_book, "2014"],
        ["College of Requiems", "Subclass", "Etharis", gh_book, "2014"],
        ["Eldritch Domain", "Subclass", "Etharis", gh_book, "2014"],
        ["Inquisition Domain", "Subclass", "Etharis", gh_book, "2014"],
        ["Circle of Blood", "Subclass", "Etharis", gh_book, "2014"],
        ["Circle of Mutation", "Subclass", "Etharis", gh_book, "2014"],
        ["Bulwark Warrior", "Subclass", "Etharis", gh_book, "2014"],
        ["Living Crucible", "Subclass", "Etharis", gh_book, "2014"],
        ["Way of the Leaden Crown", "Subclass", "Etharis", gh_book, "2014"],
        ["Way of Pride", "Subclass", "Etharis", gh_book, "2014"],
        ["Oath of Pestilence", "Subclass", "Etharis", gh_book, "2014"],
        ["Oath of Zeal", "Subclass", "Etharis", gh_book, "2014"],
        ["Green Reaper", "Subclass", "Etharis", gh_book, "2014"],
        ["Vermin Lord", "Subclass", "Etharis", gh_book, "2014"],
        ["Highway Rider", "Subclass", "Etharis", gh_book, "2014"],
        ["Misfortune Bringer", "Subclass", "Etharis", gh_book, "2014"],
        ["Haunted", "Subclass", "Etharis", gh_book, "2014"],
        ["Wretched Bloodline", "Subclass", "Etharis", gh_book, "2014"],
        ["The First Vampire", "Subclass", "Etharis", gh_book, "2014"],
        ["The Parasite", "Subclass", "Etharis", gh_book, "2014"],
        ["Plague Doctor", "Subclass", "Etharis", gh_book, "2014"],
        ["Sangromancer", "Subclass", "Etharis", gh_book, "2014"],
        # Transformations
        ["Aberrant Horror", "Mechanic", "Etharis", gh_book, "2014"],
        ["Fiend", "Mechanic", "Etharis", gh_book, "2014"],
        ["Lich", "Mechanic", "Etharis", gh_book, "2014"],
        ["Lycanthrope", "Mechanic", "Etharis", gh_book, "2014"],
        ["Seraph", "Mechanic", "Etharis", gh_book, "2014"],
        ["Vampire", "Mechanic", "Etharis", gh_book, "2014"],
        ["Fey", "Mechanic", "Etharis", gh_book, "2014"],
        ["Primordial", "Mechanic", "Etharis", gh_book, "2014"],
        ["Specter", "Mechanic", "Etharis", gh_book, "2014"]
    ]
    keywords.extend(gh_keywords)
    
    return keywords

if __name__ == "__main__":
    results = extract_batch2(r'c:\Users\Yorri\.gemini\antigravity\scratch\batch2_text.txt')
    
    with open(r'c:\Users\Yorri\.gemini\antigravity\scratch\batch2_extracted.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Keyword', 'Category', 'World', 'Source', 'Ruleset'])
        writer.writerows(results)
        
    print(f"Extracted {len(results)} keywords.")
