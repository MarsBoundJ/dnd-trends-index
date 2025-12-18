import csv

def extract_batch3(text_file_path):
    keywords = []
    
    with open(text_file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
        
    current_book = None
    mode = None
    
    # Book of Ebon Tides
    # Tales From The Shadows
    # The Illrigger Class
    # The Lord of the Rings Roleplaying
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
            
        if line == "=== Book of Ebon Tides ===":
            current_book = "Book of Ebon Tides"
            mode = None
            continue
        if line == "=== Tales From The Shadows ===":
            current_book = "Tales From The Shadows"
            mode = None
            continue
        if line == "=== The Illrigger Class ===":
            current_book = "The Illrigger Class"
            mode = None
            continue
        if line == "=== The Lord of the Rings Roleplaying ===":
            current_book = "The Lord of the Rings Roleplaying"
            mode = None
            continue
            
        # --- Book of Ebon Tides ---
        if current_book == "Book of Ebon Tides":
            # Races (Chapter 2)
            if line in ["Bearfolk", "Darakhul", "Shadow Fey", "Shadow Goblins", "Umbral Humans", "Quickstep", "Ratatosk", "Spiritfarer Erina", "Stygian Shade", "Sublime Ravenfolk", "Unbound Satarre", "Wyrd Gnome"]:
                keywords.append([line, "Race", "Shadow Realm", current_book, "2014"])
            
            # Subclasses (Chapter 3)
            if "Circle of" in line or "College of" in line or "Domain" in line or "Origin" in line or "Tradition" in line or "Gnawer" in line or "Binder" in line or "Way of" in line or "Mother of Sorrows" in line:
                keywords.append([line, "Subclass", "Shadow Realm", current_book, "2014"])
                
            # Locations (Chapter 6 & 7)
            if "Court of" in line or "City of" in line or "Tower of" in line or "Groves of" in line or "Moonlit Glades" in line or "Tenebrous Plain" in line or "Twilight Empire" in line or "Nightbrook" in line or "Dalliance" in line:
                keywords.append([line, "Location", "Shadow Realm", current_book, "2014"])
                
            # Monsters (Chapter 9)
            if line == "9. Monsters and NPCs":
                mode = "Monster"
                continue
            if mode == "Monster":
                keywords.append([line, "Monster", "Shadow Realm", current_book, "2014"])

        # --- Tales From The Shadows ---
        if current_book == "Tales From The Shadows":
            # Locations (Adventures)
            if "The Sable Forest" in line or "Tenebrous Plain" in line or "Hedge Maze" in line or "Wombweald" in line or "Blackwood" in line or "Brightsigh Tower" in line or "Village of Leer" in line or "Paletree Rise" in line or "Horn of Revels" in line or "Sheltered Market" in line or "Dark Archives" in line or "House of Reciprocities" in line or "Ticktock Tower" in line or "Corremel-in-Shadow" in line or "Oshragora" in line or "Kairious Complex" in line or "Impossible Vault" in line or "Rosy Casket Taverns" in line or "Gloaming Fens" in line or "Sunken House of Dawn" in line or "Shadow Forest" in line:
                keywords.append([line, "Location", "Shadow Realm", current_book, "2014"])
                
            # Monsters (Appendix)
            if line == "Monsters":
                mode = "Monster"
                continue
            if mode == "Monster":
                keywords.append([line, "Monster", "Shadow Realm", current_book, "2014"])
                
        # --- The Illrigger Class ---
        if current_book == "The Illrigger Class":
            # Subclasses
            if line in ["Painkiller", "Shadowmaster", "Architect of Ruin", "Hellspeaker", "Sanguine Knight"]:
                keywords.append([line, "Subclass", "Generic", current_book, "2014"])
            # Combat Styles
            if line in ["Treachery", "Bravado", "Schemes", "Lies"]:
                keywords.append([line, "Mechanic", "Generic", current_book, "2014"])

        # --- The Lord of the Rings Roleplaying ---
        if current_book == "The Lord of the Rings Roleplaying":
            # Cultures (Races)
            if line in ["Bardings", "Dwarves of Durin’s Folk", "Elves of Lindon", "Hobbits of the Shire", "Men of Bree", "Rangers of the North"]:
                keywords.append([line, "Race", "Middle-earth", current_book, "2014"])
            # Callings (Classes/Subclasses equivalent)
            if line in ["Captain", "Champion", "Messenger", "Scholar", "Treasure Hunter", "Warden"]:
                keywords.append([line, "Class", "Middle-earth", current_book, "2014"])
            # Locations (Chapter 9)
            if line in ["Eriador", "The Shire", "Lake Evendim", "The Bree-Land", "The Great East Road", "The Barrow-Downs", "The North Downs", "The South Downs", "The Weather Hills", "Angmar", "The Ettenmoors", "Mount Gram", "The Trollshaws", "Tharbad", "Lindon", "The Blue Mountains"]:
                keywords.append([line, "Location", "Middle-earth", current_book, "2014"])
            # Monsters (Stat Block Index)
            if line == "Stat Block Index":
                mode = "Monster"
                continue
            if mode == "Monster":
                keywords.append([line, "Monster", "Middle-earth", current_book, "2014"])

    # --- Illrigger Manual Additions (Spells & Items) ---
    illrigger_book = "The Illrigger Class"
    illrigger_extras = [
        # Spells
        ["Hellfire Blade", "Spell", "Generic", illrigger_book, "2014"],
        ["Mote of Hell", "Spell", "Generic", illrigger_book, "2014"],
        ["Single Combat", "Spell", "Generic", illrigger_book, "2014"],
        ["Aura of Desecration", "Spell", "Generic", illrigger_book, "2014"],
        ["Hell's Lash", "Spell", "Generic", illrigger_book, "2014"],
        # Items
        ["Acid Seal", "Item", "Generic", illrigger_book, "2014"],
        ["Cold Seal", "Item", "Generic", illrigger_book, "2014"],
        ["Fire Seal", "Item", "Generic", illrigger_book, "2014"],
        ["Lightning Seal", "Item", "Generic", illrigger_book, "2014"],
        ["Thunder Seal", "Item", "Generic", illrigger_book, "2014"],
        ["Poison Seal", "Item", "Generic", illrigger_book, "2014"],
        ["Psychic Seal", "Item", "Generic", illrigger_book, "2014"],
        ["Cross Skull and Bones", "Item", "Generic", illrigger_book, "2014"],
        ["Hellvet Cloak", "Item", "Generic", illrigger_book, "2014"],
        ["Infernal Calvary Medal", "Item", "Generic", illrigger_book, "2014"],
        ["Painstar", "Item", "Generic", illrigger_book, "2014"],
        ["Periapt of Hellish Vitality", "Item", "Generic", illrigger_book, "2014"],
        ["Shield of Horrors", "Item", "Generic", illrigger_book, "2014"]
    ]
    keywords.extend(illrigger_extras)
    
    return keywords

if __name__ == "__main__":
    results = extract_batch3(r'c:\Users\Yorri\.gemini\antigravity\scratch\batch3_text.txt')
    
    with open(r'c:\Users\Yorri\.gemini\antigravity\scratch\batch3_extracted.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Keyword', 'Category', 'World', 'Source', 'Ruleset'])
        writer.writerows(results)
        
    print(f"Extracted {len(results)} keywords.")
