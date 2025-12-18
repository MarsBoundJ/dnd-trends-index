import csv

def verify_one_shot_wonders(csv_path):
    extracted_adventures = set()
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) > 3 and row[3] == "One Shot Wonders":
                extracted_adventures.add(row[0])

    # List from Screenshots
    expected_adventures = [
        # Adventurous
        "The Cat's Mother", "Students of Snow", "Flying Thief", "Save the Shelter", "Heir to the Lair", 
        "Wave of Destruction", "Give it a Whirl", "Return the Favour", "The Sunken Crown", "Flower Power", 
        "Hostage Hoax", "Rust and Ruin", "Crash Landing", "Bulette-Proof", "Light the Way", "Going Ape", 
        "Boar Uproar", "Foes in the Foliage", "The Tiny War", "The Shrieker Garden", "Nightmare Glade", 
        "Race to the Top", "Campaign Trail", "Plight of the Pegasus", "Dangerous Delivery", "Vrock Slide", 
        "Forging a Future", "A Rude Awakening", "Sting Operation", "Warts and All", "Contract Terminated", 
        "Cosmic Crossfire", "Creatures of the Deep", "Tarrasque of Terror",
        # Adventurous (Col 2)
        "Golden Ticket", "Under New Owners", "Best Fiends Forever", "Dinner and a Duel", "Elemental Envoy", 
        "State of the Art", "Making A Scene", "School Spirit", "Head in the Clouds",
        # Mysterious
        "Breaking the Ice", "Frozen Asset", "Secret to Bear", "Bigfoot Boasts", "Arctic Armaments", 
        "Djinn in Their Sails", "Fishy Business", "Whale Delays", "Man in the Mirage", "Lure of the Lamia", 
        "Constellation Prize", "Chasing Dreams", "Tree of Treasure", "Leader of the Pack", "Fool's Gold", 
        "Study Break", "Minor Detour", "Hidden Gems", "Unravelled Plans", "Crying Wolf", "Night Owls", 
        "Courtroom Drama",
        # Light-Hearted
        "Snow Angels", "Ice Trials", "Down at the Docks", "Cargo Chaos", "Wedding Crashers", "No-Horse Race", 
        "Catch the Couriers", "Unhappy Birthday", "Shrub Scrub", "Hilltop Herd", "Big Babies", "The Wizard and Oz",
        # Spooky
        "Crossed Bones", "Frosty Reception", "In Too Deep", "Barnacle Booty", "Haunted Horizon", "Truth or Scare", 
        "Blackout Bay", "The Tomb's Tome", "Law and Disorder", "Walk the Walk", "Welcome Home", "Off the Rails", 
        "Cursed Chalet", "Ashes to Ashes", "Web Search", "Walking Undead", "The Last Resort", "Teacher's Pet", 
        "Lich Hunt",
        # Thrilling
        "Trial and Error", "Flee or Freeze", "Troubled Waters", "Tide's Up", "Hatch and Release", "The Wild Side", 
        "It's Mine Now", "On Shaky Ground", "On Burrowed Time", "Curtain Call", "Too Many Cooks", "Surprise Guests", 
        "A Dog's Dinner", "Trouble Brewing", "Don't Look Down", "Come Home to Roost", "Up in Smoke", "Aerial Outlaws", 
        "Total Eclipse", "Party Spirit", "Win or Ooze", "Fight at the Museum", "Spectator Sport", "Prized Procession", 
        "The Baker's Dozen", "Just the Tonic", "Not Your Vault", "The Dragon's Cage", "Games of the Gods", "On the Menu"
    ]

    missing = []
    for adv in expected_adventures:
        if adv not in extracted_adventures:
            missing.append(adv)
            
    print(f"Total Expected: {len(expected_adventures)}")
    print(f"Total Extracted: {len(extracted_adventures)}")
    print(f"Missing: {missing}")

if __name__ == "__main__":
    verify_one_shot_wonders(r'c:\Users\Yorri\.gemini\antigravity\scratch\dnd_keywords.csv')
