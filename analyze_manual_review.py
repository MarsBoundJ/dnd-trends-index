import csv

input_file = 'audit_user_final.csv'
output_file = 'audit_feedback_analysis.txt'

with open(input_file, newline='', encoding='utf-8-sig') as f:
    # Some older spreadsheet exports might have messy headers or different encodings
    # Let's read it as a simple list first to be safe
    reader = csv.reader(f)
    headers = next(reader)
    
    # Try to find the correct indices for search_term, status, reason
    # Our generated headers were: search_term, status, reason, total_interest, weeks
    try:
        term_idx = headers.index('search_term')
        status_idx = headers.index('status')
        reason_idx = headers.index('reason')
    except ValueError:
        # Fallback to column indices if headers changed
        term_idx = 0
        status_idx = 1
        reason_idx = 2

    feedback = []
    for i, row in enumerate(reader):
        if len(row) < 3:
            continue
            
        term = row[term_idx]
        status = row[status_idx].upper()
        reason = row[reason_idx]
        
        # We want to find where the user ADDED text or changed status
        # My generated status: "PROPOSED KEEP" or "PROPOSED DROP"
        # My generated reason: "" or "Glitch/Too broad"
        
        is_user_drop = 'DROP' in status and 'PROPOSED' not in status
        is_user_keep = 'KEEP' in status and 'PROPOSED' not in status
        is_user_reason = reason and 'Too broad' not in reason and 'Glitch' not in reason and reason != 'None'
        
        if is_user_drop or is_user_keep or is_user_reason:
            feedback.append({
                'row': i + 2,
                'term': term,
                'status': status,
                'reason': reason
            })

with open(output_file, 'w', encoding='utf-8') as out:
    out.write(f"Found {len(feedback)} rows with manual user feedback.\n\n")
    for f in feedback:
        out.write(f"Row {f['row']}: {f['term']} | {f['status']} | {f['reason']}\n")
