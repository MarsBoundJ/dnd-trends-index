import json
import os

def print_table(filename, title):
    if not os.path.exists(filename):
        print(f"File {filename} not found.")
        return
    
    with open(filename, 'r') as f:
        data = json.load(f)
    
    if not data:
        print(f"No data in {filename}")
        return

    print(f"### {title}")
    headers = data[0].keys()
    print("| " + " | ".join(headers) + " |")
    print("| " + " | ".join(["---"] * len(headers)) + " |")
    for row in data:
        print("| " + " | ".join(str(v).replace('\n', ' ') for v in row.values()) + " |")
    print("\n")

print_table("remove_sample.json", "Sample REMOVE Decisions (Limit 50)")
print_table("keep_sample.json", "Sample KEEP Decisions (Limit 30)")
print_table("sanity_check.json", "Specific Sanity Check Results")
