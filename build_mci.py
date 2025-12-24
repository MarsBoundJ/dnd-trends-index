import csv
import json
import os

INTERNAL_FILE = 'internal_collisions.csv'
OUTPUT_FILE = 'master_collision_index.json'

# Auto-Generated External Collisions
EXTERNAL_COLLISIONS = {
    "Baldur's Gate 3": [
        "Ability Score Improvement",
        "Abjuration School",
        "Acolyte",
        "Actor",
        "Alert",
        "Arcane Archer",
        "Arcane Trickster",
        "Asmodeus Tiefling",
        "Assassin",
        "Athlete",
        "Battle Master",
        "Beast Master",
        "Berserker",
        "Black Dragonborn",
        "Bladesinging",
        "Blue Dragonborn",
        "Brass Dragonborn",
        "Bronze Dragonborn",
        "Champion",
        "Charger",
        "Charlatan",
        "Circle of Spores",
        "Circle of the Land",
        "Circle of the Moon",
        "Circle of the Stars",
        "College of Glamour",
        "College of Lore",
        "College of Swords",
        "College of Valour",
        "Conjuration School",
        "Copper Dragonborn",
        "Criminal",
        "Crossbow Expert",
        "Death Domain",
        "Deep Gnome",
        "Defensive Duelist",
        "Divination School",
        "Divine Soul",
        "Draconic Bloodline",
        "Dragonborn",
        "Drow",
        "Drow Half-Elf",
        "Dual Wielder",
        "Duergar",
        "Dungeon Delver",
        "Durable",
        "Dwarf",
        "Eldritch Knight",
        "Elemental Adept",
        "Elf",
        "Enchantment School",
        "Entertainer",
        "Evocation School",
        "Folk Hero",
        "Forest Gnome",
        "Githyanki",
        "Gloom Stalker",
        "Gnome",
        "Gold Dragonborn",
        "Gold Dwarf",
        "Great Weapon Master",
        "Green Dragonborn",
        "Guild Artisan",
        "Half-Elf",
        "Half-Orc",
        "Halfling",
        "Haunted One",
        "Heavily Armoured",
        "Heavy Armor Master",
        "High Elf",
        "High Half-Elf",
        "Human",
        "Hunter",
        "Illusion School",
        "Knowledge Domain",
        "Life Domain",
        "Light Domain",
        "Lightfoot Halfling",
        "Lightly Armoured",
        "Lolth-Sworn Drow",
        "Lucky",
        "Mage Slayer",
        "Magic Initiate",
        "Martial Adept",
        "Medium Armour Master",
        "Mephistopheles Tiefling",
        "Mobile",
        "Moderately Armoured",
        "Nature Domain",
        "Necromancy School",
        "Noble",
        "Oath of Devotion",
        "Oath of Vengeance",
        "Oath of the Ancients",
        "Oath of the Crown",
        "Oathbreaker",
        "Outlander",
        "Path of Giants",
        "Performer",
        "Polearm Master",
        "Red Dragonborn",
        "Resilient",
        "Ritual Caster",
        "Rock Gnome",
        "Sage",
        "Savage Attacker",
        "Seldarine Drow",
        "Sentinel",
        "Shadow Magic",
        "Sharpshooter",
        "Shield Dwarf",
        "Shield Master",
        "Silver Dragonborn",
        "Skilled",
        "Soldier",
        "Spell Sniper",
        "Storm Sorcery",
        "Strongheart Halfling",
        "Swarmkeeper",
        "Swashbuckler",
        "Tavern Brawler",
        "Tempest Domain",
        "The Archfey",
        "The Fiend",
        "The Great Old One",
        "The Hexblade",
        "Thief",
        "Tiefling",
        "Tough",
        "Transmutation School",
        "Trickery Domain",
        "Urchin",
        "War Caster",
        "War Domain",
        "Way of Shadow",
        "Way of the Drunken Master",
        "Way of the Four Elements",
        "Way of the Open Hand",
        "Weapon Master",
        "White Dragonborn",
        "Wild Magic",
        "Wildheart",
        "Wood Elf",
        "Wood Half-Elf",
        "Zariel Tiefling"
    ],
    "Diablo": [
        "Barbarian",
        "Sorcerer",
        "Sorceress",
        "Rogue",
        "Druid",
        "Necromancer",
        "Monk",
        "Paladin"
    ],
    "Pathfinder": [
        "Barbarian",
        "Bard",
        "Cleric",
        "Druid",
        "Fighter",
        "Monk",
        "Paladin",
        "Ranger",
        "Rogue",
        "Sorcerer",
        "Wizard",
        "Alchemist",
        "Shaman",
        "Magus",
        "Gunslinger",
        "Summoner",
        "Bloodrager",
        "Brawler",
        "Hunter",
        "Slayer",
        "Swashbuckler",
        "Investigator",
        "Skald"
    ],
    "Skyrim": [
        "Battlemage",
        "Spellsword",
        "Assassin",
        "Paladin",
        "Ranger"
    ],
    "Warhammer": [
        "Witch Hunter",
        "Assassin",
        "Wizard",
        "Warrior Priest",
        "Ranger",
        "Scout"
    ],
    "World of Warcraft": [
        "Paladin",
        "Warrior",
        "Monk",
        "Rogue",
        "Priest",
        "Mage",
        "Warlock",
        "Druid",
        "Hunter",
        "Holy Paladin",
        "Protection Paladin",
        "Retribution Paladin",
        "Brewmaster Monk",
        "Mistweaver Monk",
        "Windwalker Monk"
    ],
    "Generic": [
        "Water",
        "Fire",
        "Air",
        "Earth",
        "Strength",
        "Dexterity",
        "Constitution",
        "Intelligence",
        "Wisdom",
        "Charisma",
        "Attack",
        "Defense",
        "Speed",
        "Level",
        "Class",
        "Race",
        "Background",
        "Feat",
        "Spell",
        "Item",
        "Monster",
        "NPC",
        "Build",
        "Guide",
        "5e",
        "Dnd",
        "D&D"
    ],
    "Professions": [
        "Captain",
        "Merchant",
        "Messenger",
        "Scholar",
        "Scout",
        "Stalker",
        "Nomad",
        "Philosopher",
        "Physician",
        "Savage",
        "Blacksmith",
        "Bomber",
        "Celebrity",
        "Chieftain",
        "Outlaw",
        "Athlete",
        "Awakened"
    ],
    "Ambiguous Entities": [
        "Champion",
        "Warden",
        "Raven Queen",
        "Telekinesis",
        "Hunter",
        "Beast",
        "Shadow",
        "Light",
        "Tempest",
        "Life",
        "Death"
    ]
}

def build_index():
    mci = {
        "internal": set(),
        "external": set()
    }

    # 1. Load Internal Collisions from CSV
    print(f"Loading internal collisions from {INTERNAL_FILE}...")
    try:
        with open(INTERNAL_FILE, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader, None) # Skip header
            for row in reader:
                if row:
                    term = row[0].strip()
                    mci["internal"].add(term.lower())
    except Exception as e:
        print(f"Error reading internal file: {e}")

    # 2. Add External Collisions
    print("Adding external collisions...")
    for category, terms in EXTERNAL_COLLISIONS.items():
        for term in terms:
            mci["external"].add(term.lower())

    # 3. Convert sets to lists
    final_index = {
        "internal": sorted(list(mci["internal"])),
        "external": sorted(list(mci["external"])),
        "all_combined": sorted(list(mci["internal"] | mci["external"]))
    }

    # 4. Save to JSON
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(final_index, f, indent=2)

    print(f"MCI Build Complete.")
    print(f"Internal Terms: {len(final_index['internal'])}")
    print(f"External Terms: {len(final_index['external'])}")
    print(f"Total Unique Collisions: {len(final_index['all_combined'])}")

if __name__ == "__main__":
    build_index()
