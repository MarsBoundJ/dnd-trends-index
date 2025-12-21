import csv

keywords = ['raven queen', 'telekinesis', 'warden', 'water', 'champion', 'cleric', 'subclass', 'spell']
input_file = 'audit_proposals_final.csv'

with open(input_file, newline='', encoding='utf-8-sig') as f:
    reader = csv.DictReader(f)
    print(f"Headers: {reader.fieldnames}")
    for i, row in enumerate(reader):
        term = row.get('search_term', '').lower()
        # Find rows containing keywords OR where status was changed by user
        # (Assuming the user might have added a new column or used 'DROP'/'KEEP' differently)
        is_match = any(k in term for k in keywords)
        
        # Also check if the 'status' or 'reason' was modified by the user
        # My generated ones were 'Proposed KEEP' and 'Proposed DROP'
        # If the user changed it to just 'DROP' or 'KEEP' or added text, we want to see it
        status = row.get('status', '').upper()
        reason = row.get('reason', '')
        
        # Logic to find user-edited rows:
        # My script produced: 'Proposed KEEP' or 'Proposed DROP'
        # If it's something else, the user likely touched it.
        is_edited = status not in ['PROPOSED KEEP', 'PROPOSED DROP', ''] or (reason and 'Glitch' not in reason and 'Too broad' not in reason)

        if is_match or is_edited:
            print(f"Row {i+2}: {row['search_term']} | Status: {row.get('status')} | Reason: {row.get('reason')}")
