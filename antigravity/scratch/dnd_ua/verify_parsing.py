import csv
import sys
import os

# Mock logic from importer_ua_expanded.py to avoid dependency issues

def parse_ua_row(csv_row):
    rows_to_return = []
    try:
        article_title = csv_row.get('Title', 'Unknown UA')
        date = csv_row.get('Date', 'Unknown Date')
        contents_raw = csv_row.get('Contents', '')
        
        # Copied logic for verification
        year = date.split('-')[0] if date and '-' in date else "UnknownYear"

        if contents_raw:
            items = contents_raw.split(';')
            for item in items:
                clean_item = item.strip()
                if not clean_item:
                    continue
                
                lower_item = clean_item.lower()
                concept_name = f"{clean_item} (UA)"
                
                # Heuristics
                if "revised ranger" in lower_item or "the ranger, revised" in lower_item:
                    concept_name = f"Ranger (UA {year})"
                elif "mystic class" in lower_item or "psionics/mystic update" in lower_item:
                    concept_name = f"Mystic (UA {year})"
                elif "artificer class" in lower_item or "the artificer revisited" in lower_item:
                    concept_name = f"Artificer (UA {year})"
                elif "wizard (artificer)" in lower_item:
                    concept_name = f"Artificer (UA {year})"

                rows_to_return.append({
                    "concept_name": concept_name,
                    "date": date
                })
        return rows_to_return

    except Exception as e:
        print(f"WARN: Failed: {e}")
        return []

def main():
    csv_filename = "Dnd_UA.csv"
    print(f"Testing parsing on {csv_filename}...")
    
    with open(csv_filename, mode='r', encoding='utf-8-sig') as file:
        reader = csv.DictReader(file)
        for row in reader:
            concepts = parse_ua_row(row)
            for c in concepts:
                name = c['concept_name']
                # Check for our target classes
                if any(x in name for x in ["Ranger", "Artificer", "Mystic"]):
                     # Only print if it looks like a base class or our target normalized output
                     # or if it is still messy
                     if "class" in name.lower() or "(UA" in name:
                        print(f"[{c['date']}] {name}")


if __name__ == "__main__":
    main()
