ç5import json

# 1. Load BG3 Terms
bg3_terms = []
try:
    with open('bg3_additions.txt', 'r', encoding='utf-8') as f:
        # Skip header
        lines = f.readlines()[1:]
        for line in lines:
            parts = line.split(',')
            if parts:
                term = parts[0].strip()
                if term:
                    bg3_terms.append(term)
except Exception as e:
    print(f"Error reading BG3: {e}")
    bg3_terms = ["Berserker", "Wildheart"] # Fallback if file missing

bg3_terms = sorted(list(set(bg3_terms)))

# 2. Define All Collisions
EXTERNAL_COLLISIONS = {
    "Baldur's Gate 3": sorted(list(set(bg3_terms + [
        "Acolyte", "Charlatan", "Criminal", "Entertainer", "Folk Hero", "Guild Artisan", "Noble", "Outlander", "Sage", "Soldier", "Urchin", "Haunted One",
        "Ability Score Improvement", "Actor", "Alert", "Athlete", "Charger", "Crossbow Expert", "Defensive Duelist", "Dual Wielder", "Dungeon Delver", "Durable", "Elemental Adept", "Great Weapon Master", "Heavily Armoured", "Heavy Armor Master", "Lightly Armoured", "Lucky", "Mage Slayer", "Magic Initiate", "Martial Adept", "Medium Armour Master", "Mobile", "Moderately Armoured", "Performer", "Polearm Master", "Resilient", "Ritual Caster", "Savage Attacker", "Sentinel", "Sharpshooter", "Shield Master", "Skilled", "Spell Sniper", "Tavern Brawler", "Tough", "War Caster", "Weapon Master",
        "Human", "Elf", "Drow", "Half-Elf", "Half-Orc", "Halfling", "Dwarf", "Gnome", "Tiefling", "Githyanki", "Dragonborn",
        "High Elf", "Wood Elf", "Lolth-Sworn Drow", "Seldarine Drow", "High Half-Elf", "Wood Half-Elf", "Drow Half-Elf", "Lightfoot Halfling", "Strongheart Halfling", "Gold Dwarf", "Shield Dwarf", "Duergar", "Forest Gnome", "Deep Gnome", "Rock Gnome", "Asmodeus Tiefling", "Mephistopheles Tiefling", "Zariel Tiefling",
        "Black Dragonborn", "Blue Dragonborn", "Brass Dragonborn", "Bronze Dragonborn", "Copper Dragonborn", "Gold Dragonborn", "Green Dragonborn", "Red Dragonborn", "Silver Dragonborn", "White Dragonborn"
    ]))),
    "Diablo": [
        "Barbarian", "Sorcerer", "Sorceress", "Rogue", "Druid", "Necromancer", "Monk", "Paladin"
    ],
    "Pathfinder": [
        "Barbarian", "Bard", "Cleric", "Druid", "Fighter", "Monk", "Paladin", "Ranger", "Rogue", 
        "Sorcerer", "Wizard", "Alchemist", "Shaman", "Magus", "Gunslinger", "Summoner", "Bloodrager", 
        "Brawler", "Hunter", "Slayer", "Swashbuckler", "Investigator", "Skald"
    ],
    "Skyrim": [
        "Battlemage", "Spellsword", "Assassin", "Paladin", "Ranger"
    ],
    "Warhammer": [
        "Witch Hunter", "Assassin", "Wizard", "Warrior Priest", "Ranger", "Scout"
    ],
    "World of Warcraft": [
        "Paladin", "Warrior", "Monk", "Rogue", "Priest", "Mage", "Warlock", "Druid", "Hunter", 
        "Holy Paladin", "Protection Paladin", "Retribution Paladin", 
        "Brewmaster Monk", "Mistweaver Monk", "Windwalker Monk"
    ],
    "Generic": [
        "Water", "Fire", "Air", "Earth", 
        "Strength", "Dexterity", "Constitution", "Intelligence", "Wisdom", "Charisma",
        "Attack", "Defense", "Speed", "Level", "Class", "Race", "Background", "Feat", "Spell", "Item", "Monster", "NPC", 
        "Build", "Guide", "5e", "Dnd", "D&D"
    ],
    "Professions": [
        "Captain", "Merchant", "Messenger", "Scholar", "Scout", "Stalker", "Nomad", "Philosopher", "Physician", 
        "Savage", "Blacksmith", "Bomber", "Celebrity", "Chieftain", "Outlaw", "Athlete", "Awakened"
    ],
    "Ambiguous Entities": [
        "Champion", "Warden", "Raven Queen", "Telekinesis", "Hunter", "Beast", "Shadow", "Light", "Tempest", "Life", "Death"
    ]
}

# 3. Generate build_mci.py content
build_script_content = f'''import csv
import json
import os

INTERNAL_FILE = 'internal_collisions.csv'
OUTPUT_FILE = 'master_collision_index.json'

# Auto-Generated External Collisions
EXTERNAL_COLLISIONS = {json.dumps(EXTERNAL_COLLISIONS, indent=4)}

def build_index():
    mci = {{
        "internal": set(),
        "external": set()
    }}

    # 1. Load Internal Collisions from CSV
    print(f"Loading internal collisions from {{INTERNAL_FILE}}...")
    try:
        with open(INTERNAL_FILE, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader, None) # Skip header
            for row in reader:
                if row:
                    term = row[0].strip()
                    mci["internal"].add(term.lower())
    except Exception as e:
        print(f"Error reading internal file: {{e}}")

    # 2. Add External Collisions
    print("Adding external collisions...")
    for category, terms in EXTERNAL_COLLISIONS.items():
        for term in terms:
            mci["external"].add(term.lower())

    # 3. Convert sets to lists
    final_index = {{
        "internal": sorted(list(mci["internal"])),
        "external": sorted(list(mci["external"])),
        "all_combined": sorted(list(mci["internal"] | mci["external"]))
    }}

    # 4. Save to JSON
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(final_index, f, indent=2)

    print(f"MCI Build Complete.")
    print(f"Internal Terms: {{len(final_index['internal'])}}")
    print(f"External Terms: {{len(final_index['external'])}}")
    print(f"Total Unique Collisions: {{len(final_index['all_combined'])}}")

if __name__ == "__main__":
    build_index()
'''

# 4. Generate export_collisions.py content
export_script_content = f'''import csv

# Auto-Generated External Collisions
EXTERNAL_COLLISIONS = {json.dumps(EXTERNAL_COLLISIONS, indent=4)}

OUTPUT_FILE = 'external_collisions_report.csv'

def export_report():
    rows = []
    for category, terms in EXTERNAL_COLLISIONS.items():
        for term in terms:
            rows.append({{"Category": category, "Term": term}})
            
    # Sort by Category then Term
    rows.sort(key=lambda x: (x['Category'], x['Term']))
    
    keys = ["Category", "Term"]
    try:
        with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
            writer.writerows(rows)
        print(f"Successfully exported {{len(rows)}} terms to {{OUTPUT_FILE}}")
    except Exception as e:
        print(f"Error writing CSV: {{e}}")

if __name__ == "__main__":
    export_report()
'''

# 5. Write files
with open('build_mci.py', 'w', encoding='utf-8') as f:
    f.write(build_script_content)
    
with open('export_collisions.py', 'w', encoding='utf-8') as f:
    f.write(export_script_content)

print("Regenerated build_mci.py and export_collisions.py with full RPG data.")
˚ *cascade08˚ã*cascade08ãî *cascade08îÑ *cascade08Ñí*cascade08íú *cascade08úç5 *cascade08"(5ea7c1b525e148ee9cb36b57c32852aa3816ac0228file:///C:/Users/Yorri/.gemini/regenerate_mci_scripts.py:file:///C:/Users/Yorri/.gemini