Ôimport csv

# Re-using the dictionary from build_mci.py for accurate reporting
EXTERNAL_COLLISIONS = {
    "games": ["Baldur's Gate 3", "BG3", "Diablo", "Skyrim", "Warhammer", "Pathfinder", "WoW", "World of Warcraft"],
    "generic": [
        "Water", "Fire", "Air", "Earth", 
        "Strength", "Dexterity", "Constitution", "Intelligence", "Wisdom", "Charisma",
        "Attack", "Defense", "Speed", "Level", "Class", "Race", "Background", "Feat", "Spell", "Item", "Monster", "NPC", 
        "Build", "Guide", "5e", "Dnd", "D&D"
    ],
    "professions": [
        "Captain", "Merchant", "Messenger", "Scholar", "Scout", "Stalker", "Nomad", "Philosopher", "Physician", 
        "Savage", "Blacksmith", "Bomber", "Celebrity", "Chieftain", "Outlaw", "Athlete", "Awakened"
    ],
    "ambiguous_entities": [
        "Champion", "Warden", "Raven Queen", "Telekinesis", "Hunter", "Beast", "Shadow", "Light", "Tempest", "Life", "Death"
    ]
}

OUTPUT_FILE = 'external_collisions_report.csv'

def export_report():
    rows = []
    for category, terms in EXTERNAL_COLLISIONS.items():
        for term in terms:
            rows.append({"Category": category, "Term": term})
            
    # Sort by Category then Term
    rows.sort(key=lambda x: (x['Category'], x['Term']))
    
    keys = ["Category", "Term"]
    try:
        with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
            writer.writerows(rows)
        print(f"Successfully exported {len(rows)} terms to {OUTPUT_FILE}")
    except Exception as e:
        print(f"Error writing CSV: {e}")

if __name__ == "__main__":
    export_report()
Ô*cascade08"(5ea7c1b525e148ee9cb36b57c32852aa3816ac0223file:///C:/Users/Yorri/.gemini/export_collisions.py:file:///C:/Users/Yorri/.gemini