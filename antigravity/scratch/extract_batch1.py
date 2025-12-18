import csv

def extract_batch1(text_file_path):
    keywords = []
    
    # 1. Parse Rick & Morty and Netherdeep from text file
    with open(text_file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
        
    current_book = None
    mode = None
    
    # Rick & Morty: "The Lost Dungeon of Rickedness" (Location), "Lycanthropickles" (Monster - from prev knowledge or text?)
    # Text has "App. D: Creatures" -> "Creature Stat Blocks"
    # It doesn't list the creatures in the TOC provided.
    # Wait, the TOC says "Creature Stat Blocks" but doesn't list them.
    # The user provided TOC for Rick & Morty is generic.
    # "The Lost Dungeon of Rickedness" is a location.
    # I might need to search for Rick & Morty monster list if TOC is insufficient.
    # But I'll extract what I can.
    
    # Netherdeep: Has "Appendix A: Creatures" with a list!
    # "Alyxian Aboleth", "Corrupted Giant Shark", etc.
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
            
        if line == "=== Dungeons & Dragons vs. Rick and Morty ===":
            current_book = "Dungeons & Dragons vs. Rick and Morty"
            mode = None
            continue
        if line == "=== Critical Role: Call of the Netherdeep ===":
            current_book = "Critical Role: Call of the Netherdeep"
            mode = None
            continue
            
        if current_book == "Dungeons & Dragons vs. Rick and Morty":
            if line == "The Lost Dungeon of Rickedness":
                keywords.append([line, "Location", "Forgotten Realms", current_book, "2014"])
            # The TOC doesn't list monsters. I'll have to rely on search or generic "Rick & Morty" keywords if any.
            # User provided TOC: "App. D: Creatures" -> "Creature Stat Blocks". No names.
            # I will add a placeholder or search result later.
            
        if current_book == "Critical Role: Call of the Netherdeep":
            # Locations
            if line in ["Jigow", "Bazzoxan", "Ank’Harel", "Cael Morrow", "The Netherdeep"]:
                keywords.append([line, "Location", "Exandria", current_book, "2014"])
            
            # Monsters in Appendix A
            # "Appendix A: Creatures" starts the section
            if line == "Appendix A: Creatures":
                mode = "Monster"
                continue
            if line == "Appendix B: Magic Items":
                mode = None
                continue
                
            if mode == "Monster":
                # Filter out headers
                if line in ["Using a Stat Block", "Adventuring Rivals", "Tier 1 Statblocks", "Tier 2 Statblocks", "Tier 3 Statblocks"]:
                    continue
                # Add monster
                keywords.append([line, "Monster", "Exandria", current_book, "2014"])

    # 2. Add Minecraft Creatures (Manual List from Search)
    minecraft_book = "Monstrous Compendium Vol. 3: Minecraft Creatures"
    minecraft_monsters = ["Blaze", "Creeper", "Ender Dragon", "Enderman", "Wolf of the Overworld"]
    for m in minecraft_monsters:
        keywords.append([m, "Monster", "Minecraft", minecraft_book, "2014"])
        
    # 3. Add Frozen Sick (Manual List from Search)
    frozen_sick_book = "Frozen Sick"
    frozen_sick_keywords = [
        ["Frozen Sick", "Location", "Exandria", frozen_sick_book, "2014"],
        ["Frigid Woe", "Mechanic", "Exandria", frozen_sick_book, "2014"],
        ["Palebank Village", "Location", "Exandria", frozen_sick_book, "2014"],
        ["Eiselcross", "Location", "Exandria", frozen_sick_book, "2014"],
        ["Salsvault", "Location", "Exandria", frozen_sick_book, "2014"],
        ["Croaker Cave", "Location", "Exandria", frozen_sick_book, "2014"],
        ["Urgon Wenth", "NPC", "Exandria", frozen_sick_book, "2014"],
        ["Verla Pelc", "NPC", "Exandria", frozen_sick_book, "2014"],
        ["Elro Aldataur", "NPC", "Exandria", frozen_sick_book, "2014"],
        ["Tulgi Lutan", "NPC", "Exandria", frozen_sick_book, "2014"],
        ["Hulil Lutan", "NPC", "Exandria", frozen_sick_book, "2014"],
        ["Irven Liel", "NPC", "Exandria", frozen_sick_book, "2014"],
        ["Orvo Mustave", "NPC", "Exandria", frozen_sick_book, "2014"],
        ["Ferol Sal", "NPC", "Exandria", frozen_sick_book, "2014"],
        ["Raegrin Mau", "NPC", "Exandria", frozen_sick_book, "2014"],
        ["The Buyer", "NPC", "Exandria", frozen_sick_book, "2014"],
        ["The Family at the Jolly Dwarf", "NPC", "Exandria", frozen_sick_book, "2014"]
    ]
    keywords.extend(frozen_sick_keywords)
    
    # 4. Add Lightning Keep (Manual List from Search)
    lk_book = "Lightning Keep"
    lk_keywords = [
        ["Lightning Keep", "Location", "Generic", lk_book, "2014"],
        ["Lightning Rods", "Item", "Generic", lk_book, "2014"],
        ["Lightning Shield", "Mechanic", "Generic", lk_book, "2014"],
        ["Marauding Dragon", "Villain", "Generic", lk_book, "2014"],
        ["Werewolf Knight", "NPC", "Generic", lk_book, "2014"],
        ["Blue Jade Rings", "Item", "Generic", lk_book, "2014"]
    ]
    keywords.extend(lk_keywords)
    
    return keywords

if __name__ == "__main__":
    results = extract_batch1(r'c:\Users\Yorri\.gemini\antigravity\scratch\batch1_text.txt')
    
    with open(r'c:\Users\Yorri\.gemini\antigravity\scratch\batch1_extracted.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Keyword', 'Category', 'World', 'Source', 'Ruleset'])
        writer.writerows(results)
        
    print(f"Extracted {len(results)} keywords.")
