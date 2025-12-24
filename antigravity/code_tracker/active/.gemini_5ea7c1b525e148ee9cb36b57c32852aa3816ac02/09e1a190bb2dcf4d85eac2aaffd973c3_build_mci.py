Žimport csv
import json
import os

INTERNAL_FILE = 'internal_collisions.csv'
OUTPUT_FILE = 'master_collision_index.json'

# Hardcoded External IPs and Generic Terms
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
                    # Add exact term
                    term = row[0].strip()
                    mci["internal"].add(term.lower())
    except Exception as e:
        print(f"Error reading internal file: {e}")

    # 2. Add External Collisions
    print("Adding external collisions...")
    for category, terms in EXTERNAL_COLLISIONS.items():
        for term in terms:
            mci["external"].add(term.lower())

    # 3. Convert sets to lists for JSON serialization
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
Ž*cascade08"(5ea7c1b525e148ee9cb36b57c32852aa3816ac022+file:///C:/Users/Yorri/.gemini/build_mci.py:file:///C:/Users/Yorri/.gemini