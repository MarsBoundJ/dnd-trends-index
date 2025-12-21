import csv

input_file = 'audit_proposals_final.csv'
output_file = 'user_audit_summary.txt'

with open(input_file, newline='', encoding='utf-8-sig') as f:
    reader = csv.DictReader(f)
    changes = []
    for i, row in enumerate(reader):
        status = row.get('status', '').upper()
        reason = row.get('reason', '')
        # The user might have overwritten my 'Proposed KEEP' with 'DROP' or added comments
        # Let's find anything where they changed the status OR added a reason
        if 'DROP' in status or (reason and reason != 'None' and 'Glitch' not in reason and 'Too broad' not in reason):
            changes.append({
                'row': i + 2,
                'term': row['search_term'],
                'status': status,
                'reason': reason
            })

with open(output_file, 'w', encoding='utf-8') as out:
    for c in changes:
        out.write(f"Row {c['row']}: {c['term']} | {c['status']} | {c['reason']}\n")
