import csv

def extract_batch5(text_file_path):
    keywords = []
    
    with open(text_file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
        
    current_book = None
    mode = None
    
    # The Malady of Minarrh
    # Valda's Spire of Secrets
    # Ruins of Symbaroum: Setting Handbook
    # One Shot Wonders
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
            
        if line == "=== The Malady of Minarrh ===":
            current_book = "The Malady of Minarrh"
            mode = None
            continue
        if line == "=== Valda's Spire of Secrets ===":
            current_book = "Valda's Spire of Secrets"
            mode = None
            continue
        if line == "=== Ruins of Symbaroum: Setting Handbook ===":
            current_book = "Ruins of Symbaroum: Setting Handbook"
            mode = None
            continue
        if line == "=== One Shot Wonders ===":
            current_book = "One Shot Wonders"
            mode = None
            continue
            
        # --- The Malady of Minarrh (Manual Additions) ---
        if current_book == "The Malady of Minarrh":
            # Based on search results
            if line == "(No TOC provided, searching web)":
                keywords.append(["Minarrh", "Location", "Generic", current_book, "2024"])
                keywords.append(["Anaya", "NPC", "Generic", current_book, "2024"])
                continue

        # --- Valda's Spire of Secrets ---
        if current_book == "Valda's Spire of Secrets":
            # Races (Chapter 1)
            if line in ["Geppettin", "Mandrake", "Mousefolk", "Spirithost", "Near-Human"]:
                keywords.append([line, "Race", "Generic", current_book, "2014"])
            
            # Classes (Chapter 2)
            if line in ["Alchemist", "Captain", "Craftsman", "Gunslinger", "Investigator", "Martyr", "Necromancer", "Warden", "Warmage", "Witch"]:
                keywords.append([line, "Class", "Generic", current_book, "2014"])
                
            # Subclasses (Chapter 3) - The TOC just lists class names, implying subclasses are inside.
            # I can't extract specific subclass names from the TOC provided in the screenshot as it only lists the class headers.
            # However, the user text mentioned "Muscle Wizard" (Barbarian) and "Arachnoid Stalker" (Rogue).
            # I will add these manually.
            pass

        # --- Ruins of Symbaroum ---
        if current_book == "Ruins of Symbaroum: Setting Handbook":
            # Locations
            if line in ["Davokar", "The Hunter’s Harbor", "Blackmoor", "The Explorer’s Haven", "Odaiova", "Baiaga", "Yndaros", "Yndarien", "Thistle Hold", "Karvosti", "The Underworld", "The Yonderworld", "The Spirit World", "Green as Copper", "A Blooming Vale", "Prisoners of the Death Crater", "Dawn of the Black Sun", "Lord of the Bog", "Blight Night", "Jakad’s Heart"]:
                keywords.append([line, "Location", "Symbaroum", current_book, "2014"])
                
            # Factions
            if line in ["Noble Houses", "The Noblest Blood", "Other Houses"]:
                 keywords.append([line, "Faction", "Symbaroum", current_book, "2014"])
                 
            # Races
            if line in ["Changelings", "Dwarves", "Elves", "Goblins", "Humans", "Ogres"]:
                keywords.append([line, "Race", "Symbaroum", current_book, "2014"])

        # --- One Shot Wonders ---
        if current_book == "One Shot Wonders":
            # Adventures (from Screenshots 3 & 4) - Manual List
            # I will add these manually in the list below
            pass

    # --- Manual Additions ---
    
    # Valda's Subclasses (from user text)
    valda_book = "Valda's Spire of Secrets"
    valda_extras = [
        ["Muscle Wizard", "Subclass", "Generic", valda_book, "2014"],
        ["Arachnoid Stalker", "Subclass", "Generic", valda_book, "2014"],
        ["Field Commander", "Feat", "Generic", valda_book, "2014"],
        ["Iron Grip", "Feat", "Generic", valda_book, "2014"]
    ]
    keywords.extend(valda_extras)
    
    # One Shot Wonders Adventures (from Screenshots)
    osw_book = "One Shot Wonders"
    osw_adventures = [
        "The Cat's Mother", "Students of Snow", "Flying Thief", "Save the Shelter", "Heir to the Lair", "Wave of Destruction", "Give it a Whirl", "Return the Favour", "The Sunken Crown", "Flower Power", "Hostage Hoax", "Rust and Ruin", "Crash Landing", "Bulette-Proof", "Light the Way", "Going Ape", "Boar Uproar", "Foes in the Foliage", "The Tiny War", "The Shrieker Garden", "Nightmare Glade", "Race to the Top", "Campaign Trail", "Plight of the Pegasus", "Dangerous Delivery", "Vrock Slide", "Forging a Future", "A Rude Awakening", "Sting Operation", "Warts and All", "Contract Terminated", "Cosmic Crossfire", "Creatures of the Deep", "Tarrasque of Terror",
        "Snow Angels", "Ice Trials", "Down at the Docks", "Cargo Chaos", "Wedding Crashers", "No-Horse Race", "Catch the Couriers", "Unhappy Birthday", "Shrub Scrub", "Hilltop Herd", "Big Babies", "The Wizard and Oz",
        "Golden Ticket", "Under New Owners", "Best Fiends Forever", "Dinner and a Duel", "Elemental Envoy", "State of the Art", "Making A Scene", "School Spirit", "Head in the Clouds",
        "Breaking the Ice", "Frozen Asset", "Secret to Bear", "Bigfoot Boasts", "Arctic Armaments", "Djinn in Their Sails", "Fishy Business", "Whale Delays", "Man in the Mirage", "Lure of the Lamia", "Constellation Prize", "Chasing Dreams", "Tree of Treasure", "Leader of the Pack", "Fool's Gold", "Study Break", "Minor Detour", "Hidden Gems", "Unravelled Plans", "Crying Wolf", "Night Owls", "Courtroom Drama",
        "Crossed Bones", "Frosty Reception", "In Too Deep", "Barnacle Booty", "Haunted Horizon", "Truth or Scare", "Blackout Bay", "The Tomb's Tome", "Law and Disorder", "Walk the Walk", "Welcome Home", "Off the Rails", "Cursed Chalet",
        "Ashes to Ashes", "Web Search", "Walking Undead", "The Last Resort", "Teacher's Pet", "Lich Hunt",
        "Trial and Error", "Flee or Freeze", "Troubled Waters", "Tide's Up", "Hatch and Release", "The Wild Side", "It's Mine Now", "On Shaky Ground", "On Burrowed Time", "Curtain Call", "Too Many Cooks", "Surprise Guests", "A Dog's Dinner", "Trouble Brewing", "Don't Look Down", "Come Home to Roost", "Up in Smoke", "Aerial Outlaws", "Total Eclipse", "Party Spirit", "Win or Ooze", "Fight at the Museum", "Spectator Sport", "Prized Procession", "The Baker's Dozen", "Just the Tonic", "Not Your Vault", "The Dragon's Cage", "Games of the Gods", "On the Menu"
    ]
    
    for adv in osw_adventures:
        keywords.append([adv, "Location", "Generic", osw_book, "2014"])

    return keywords

if __name__ == "__main__":
    results = extract_batch5(r'c:\Users\Yorri\.gemini\antigravity\scratch\batch5_text.txt')
    
    with open(r'c:\Users\Yorri\.gemini\antigravity\scratch\batch5_extracted.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Keyword', 'Category', 'World', 'Source', 'Ruleset'])
        writer.writerows(results)
        
    print(f"Extracted {len(results)} keywords.")
