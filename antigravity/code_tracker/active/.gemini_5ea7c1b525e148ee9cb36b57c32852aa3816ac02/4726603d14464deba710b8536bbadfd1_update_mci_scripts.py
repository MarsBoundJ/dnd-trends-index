€import csv

BG3_FILE = 'bg3_additions.txt'
BUILD_SCRIPT = 'build_mci.py'
EXPORT_SCRIPT = 'export_collisions.py'

def get_bg3_terms():
    terms = set()
    with open(BG3_FILE, 'r', encoding='utf-8') as f:
        # Skip header
        lines = f.readlines()[1:]
        for line in lines:
            parts = line.split(',')
            if parts:
                term = parts[0].strip()
                if term:
                    terms.add(term)
    return sorted(list(terms))

def update_python_script(filename, bg3_terms):
    with open(filename, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # We want to replace the "games": [...] list or add to it.
    # But since it's hardcoded text, let's just find the EXTERNAL_COLLISIONS definition
    # and "smartly" replace the "games": [...] part? 
    # Or just simpler: Inject a new Key "bg3_classes".
    
    # Let's create a string for the new list
    formatted_list = ',\n        ' + ', '.join([f'"{t}"' for t in bg3_terms])
    
    # If the file already has "bg3_classes", we might duplicate.
    if "bg3_classes" in content:
        print(f"Skipping {filename}, already has bg3_classes.")
        return

    # Finding the insertion point.
    # Look for "games": [ ... ],
    marker = '"games": ['
    if marker not in content:
        print(f"Could not find games list in {filename}")
        return
        
    # We will insert "bg3_classes": [...], right after "games" list ends?
    # This is risky with regex.
    # Alternative: Rewrite the file completely since we know the structure from previous turn.
    
    # Let's just rewriting the whole variable is safer if we know the rest is static?
    # Or just replace the "games" list with "games" + bg3 terms?
    
    # Let's append to the 'games' list itself.
    # Find the closing bracket of "games": [ ... ]
    start_idx = content.find(marker)
    end_idx = content.find('],', start_idx)
    
    if start_idx == -1 or end_idx == -1:
        print(f"Error parsing {filename}")
        return
        
    current_list_content = content[start_idx + len(marker) : end_idx]
    
    # New content
    new_terms_str = ', '.join([f'"{t}"' for t in bg3_terms])
    new_list_content = current_list_content + ", " + new_terms_str
    
    new_content = content[:start_idx + len(marker)] + new_list_content + content[end_idx:]
    
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(new_content)
    print(f"Updated {filename}")

def run_update():
    new_terms = get_bg3_terms()
    print(f"Found {len(new_terms)} BG3 terms to add.")
    
    update_python_script(BUILD_SCRIPT, new_terms)
    update_python_script(EXPORT_SCRIPT, new_terms)

if __name__ == "__main__":
    run_update()
€*cascade08"(5ea7c1b525e148ee9cb36b57c32852aa3816ac0224file:///C:/Users/Yorri/.gemini/update_mci_scripts.py:file:///C:/Users/Yorri/.gemini