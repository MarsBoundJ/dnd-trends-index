import csv

def parse_drakkenheim(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    keywords = []
    source = "Monsters of Drakkenheim"
    world = "Drakkenheim"
    
    # Sections
    # Appendix B: Faction Agents -> Factions
    # Bestiary -> Monsters/NPCs
    
    in_factions = False
    in_bestiary = False
    
    factions = []
    
    # Pre-defined known villains/NPCs from the list to categorize correctly?
    # Or just default to Monster and let user refine?
    # "Lucretia Mathias", "Lord of the Feast", "Queen of Thieves" are definitely Villains/NPCs.
    # "Amethyst Academy" is a Faction.
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
            
        if line == "Appendix B: Faction Agents":
            in_factions = True
            continue
        if line.startswith("Appendix C:"):
            in_factions = False
            continue
            
        if line == "Bestiary":
            in_bestiary = True
            continue
            
        if in_factions:
            # These are factions
            # "Amethyst Academy", "Followers of the Falling Fire", etc.
            keywords.append([line, "Faction", world, source])
            factions.append(line)
            
        if in_bestiary:
            # Ignore single letters A-Z
            if len(line) == 1 and line.isalpha():
                continue
                
            # Ignore sub-headers if any?
            # The list seems flat mostly, but has some indentation in the original TOC which is lost here?
            # "Delerium Dragons" -> "Delerium Dragon Wyrmling"
            # "Elemental, Contaminated" -> "Animated Delerium Sludge"
            # I'll take them all as keywords.
            
            # Categorization logic
            category = "Monster"
            
            # Check for known NPC names or titles that sound like NPCs
            # This is heuristic.
            # "Captain", "Lord", "Lady", "Queen", "King", "Dr.", "Lieutenant"
            if any(title in line for title in ["Captain ", "Lord ", "Lady ", "Queen ", "King ", "Dr. ", "Lieutenant ", "High Flamekeeper", "Baron ", "Count ", "Countess "]):
                category = "Villain" # Or NPC
            
            # Specific named characters
            named_chars = [
                "Big Linda", "Bigger Linda", "Blackjack Mel", "Bojack", 
                "Chitter", "Crimson Countess", "Eldrick Runeweaver", 
                "He Who Laughs Last", "Kronen", "Lenore von Kessel", 
                "Lucretia Mathias", "Nathaniel Flint", "Ophelia Reed", 
                "River, Amethyst Academy Liaison", "Skretch", 
                "Vladimir von Drakken", "The Duchess", "The Pale Man", 
                "The Rat Prince", "The Rat King", "The World Ender"
            ]
            
            if line in named_chars or any(n in line for n in named_chars):
                category = "Villain"
                
            keywords.append([line, category, world, source])

    return keywords

if __name__ == "__main__":
    results = parse_drakkenheim(r'c:\Users\Yorri\.gemini\antigravity\scratch\drakkenheim.txt')
    
    # Write to CSV
    with open(r'c:\Users\Yorri\.gemini\antigravity\scratch\drakkenheim_extracted.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Keyword', 'Category', 'World', 'Source'])
        writer.writerows(results)
        
    print(f"Extracted {len(results)} keywords.")
