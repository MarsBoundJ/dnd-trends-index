import csv

def extract_batch4(text_file_path):
    keywords = []
    
    with open(text_file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
        
    current_book = None
    mode = None
    
    # The Griffon’s Saddlebag Book Two
    # Humblewood Tales
    # Dungeons of Drakkenheim: In Search of the Smuggler's Secrets
    # Heliana’s Guide to Monster Hunting: Part 1
    # Obojima: Tales from the Tall Grass
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
            
        if line == "=== The Griffon’s Saddlebag Book Two ===":
            current_book = "The Griffon’s Saddlebag Book Two"
            mode = None
            continue
        if line == "=== Humblewood Tales ===":
            current_book = "Humblewood Tales"
            mode = None
            continue
        if line == "=== Dungeons of Drakkenheim: In Search of the Smuggler's Secrets ===":
            current_book = "Dungeons of Drakkenheim: In Search of the Smuggler's Secrets"
            mode = None
            continue
        if line == "=== Heliana’s Guide to Monster Hunting: Part 1 ===":
            current_book = "Heliana’s Guide to Monster Hunting: Part 1"
            mode = None
            continue
        if line == "=== Obojima: Tales from the Tall Grass ===":
            current_book = "Obojima: Tales from the Tall Grass"
            mode = None
            continue
            
        # --- Griffon's Saddlebag 2 ---
        if current_book == "The Griffon’s Saddlebag Book Two":
            # Items (Part 1)
            if line in ["Arista, Wand of the Spire", "Hatred, the Wrathful Edge", "Morath, Scepter of the Soul Vortex", "Nimbus, First Staff of the Thunderbirds", "Silverwind, the Cleansing Breeze"]:
                keywords.append([line, "Item", "Generic", current_book, "2014"])
            
            # Species (Part 2)
            if line in ["Etherean", "Geleton"]:
                keywords.append([line, "Race", "Generic", current_book, "2014"])
                
            # Subclasses
            if "Path of" in line or "College of" in line or "Domain" in line or "Circle of" in line or "Way of" in line or "Oath of" in line or "Grim Surgeon" in line or "Desert Soul" in line or "The Many" in line or "Wand Lore" in line or "Steel Hawk" in line or "Rocborne" in line:
                 if "Steel Hawk Captain" not in line and "Steel Hawk Soldier" not in line and "Rocborne Chief" not in line and "Rocborne Ancestors" not in line: # Avoid duplicates in stat blocks
                    keywords.append([line, "Subclass", "Generic", current_book, "2014"])

            # Locations (Part 3)
            if line in ["Antronec, the City of Artisans", "The Bloodmire", "Crest City", "Durheim Monastery", "Grymclover Forests", "Heavensteppe", "The H’rethi Desert", "The Infinite Animarum", "The Magmarath Caldera", "Mysteries of the Border Ethereal", "Orostead", "The Rocborne", "The Upheaval, a War Against Magic"]:
                keywords.append([line, "Location", "Generic", current_book, "2014"])
                
            # Monsters (Part 4)
            if line == "Part 4: Stat Block Appendices":
                mode = "Monster"
                continue
            if mode == "Monster":
                keywords.append([line, "Monster", "Generic", current_book, "2014"])

        # --- Humblewood Tales ---
        if current_book == "Humblewood Tales":
            # Locations/Adventures
            if line in ["The Wind-Touched", "The Wakewyrm’s Fury", "The Seahawk", "Hunt for the Loper", "Descent into the Dark", "Shoppes and Stores", "The Crimson Rose", "The Last Stand", "Zephyr & Co.", "Hannu’s Provisions", "Spleck Smandra’s Wagon of Worldly Wonders"]:
                keywords.append([line, "Location", "Humblewood", current_book, "2014"])
            
            # Subclasses
            if "Circle of" in line or "Patron:" in line or "Leyline Magic" in line:
                keywords.append([line, "Subclass", "Humblewood", current_book, "2014"])
                
            # Monsters (Appendix A)
            if line == "Appendix A: Bestiary":
                mode = "Monster"
                continue
            if line == "Appendix B: Non-Player Characters":
                mode = "NPC"
                continue
            if line == "Appendix C: Magic Items":
                mode = None
                continue
                
            if mode == "Monster":
                keywords.append([line, "Monster", "Humblewood", current_book, "2014"])
            if mode == "NPC":
                keywords.append([line, "NPC", "Humblewood", current_book, "2014"])

        # --- Smuggler's Secrets ---
        if current_book == "Dungeons of Drakkenheim: In Search of the Smuggler's Secrets":
            if line == "In Search of the Smuggler's Secrets":
                keywords.append([line, "Location", "Drakkenheim", current_book, "2024"]) # User note: 2024 design
            if line == "The Lamplighter":
                keywords.append([line, "Monster", "Drakkenheim", current_book, "2024"])

        # --- Heliana's Guide ---
        if current_book == "Heliana’s Guide to Monster Hunting: Part 1":
            # Adventures/Locations
            if line in ["Dread and Breakfast", "Reign of Iron", "Shadow of the Broodmother", "Dream Weaver"]:
                keywords.append([line, "Location", "Generic", current_book, "2014"])
            
            # Subclasses
            if "College of" in line or "Circle of" in line or "Oath of" in line or "School of" in line:
                keywords.append([line, "Subclass", "Generic", current_book, "2014"])
                
            # Monsters
            if line == "Appendix C: Creatures" or line == "Bestiary":
                mode = "Monster"
                continue
            if mode == "Monster":
                if "Appendix" in line:
                    mode = None
                    continue
                keywords.append([line, "Monster", "Generic", current_book, "2014"])

        # --- Obojima ---
        if current_book == "Obojima: Tales from the Tall Grass":
            # Locations
            if line in ["The Gift Of Shuritashi", "The Land Of Hot Water", "Mount Arbora", "The Gale Fields", "The Brackwater Wetlands", "The Coastal Highlands", "The Shallows"]:
                keywords.append([line, "Location", "Obojima", current_book, "2014"])
            
            # Subclasses
            if "Path of" in line or "College of" in line or "Circle of" in line or "The Spirit-Fused" in line or "Sheep Dragon Shepherd" in line or "Oath of" in line or "Corrupted Ranger" in line or "Waxwork Rogue" in line or "Oni Bloodline" in line or "The Lantern" in line or "Origami Mage" in line:
                keywords.append([line, "Subclass", "Obojima", current_book, "2014"])
                
            # Monsters
            if line == "Friends & Foes":
                mode = "Monster"
                continue
            if mode == "Monster":
                if "Adventures" in line or "Credits" in line:
                    mode = None
                    continue
                keywords.append([line, "Monster", "Obojima", current_book, "2014"])

    return keywords

if __name__ == "__main__":
    results = extract_batch4(r'c:\Users\Yorri\.gemini\antigravity\scratch\batch4_text.txt')
    
    with open(r'c:\Users\Yorri\.gemini\antigravity\scratch\batch4_extracted.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Keyword', 'Category', 'World', 'Source', 'Ruleset'])
        writer.writerows(results)
        
    print(f"Extracted {len(results)} keywords.")
