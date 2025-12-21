import csv

input_file = 'audit_user_final.csv'
output_file = 'manual_review_final_analysis.txt'

with open(input_file, newline='', encoding='utf-8-sig') as f:
    reader = csv.reader(f)
    headers = next(reader)
    
    # Identify indices
    term_idx = 0
    status_idx = 1
    reason_idx = 2
    for i, h in enumerate(headers):
        if 'term' in h.lower(): term_idx = i
        if 'status' in h.lower(): status_idx = i
        if 'reason' in h.lower(): reason_idx = i

    feedback = []
    for i, row in enumerate(reader):
        if len(row) <= max(term_idx, status_idx, reason_idx):
            continue
            
        term = row[term_idx]
        status = row[status_idx].strip()
        reason = row[reason_idx].strip()
        
        # Capture:
        # 1. Any reason that isn't empty or the default ones I generated
        # 2. Any status that isn't 'Proposed KEEP' or 'Proposed DROP'
        
        is_manual_status = status and 'Proposed' not in status
        is_manual_reason = reason and 'Too broad' not in reason and 'Glitch' not in reason and reason != 'None'

        if is_manual_status or is_manual_reason:
            feedback.append({
                'row': i + 2,
                'term': term,
                'status': status,
                'reason': reason
            })

with open(output_file, 'w', encoding='utf-8') as out:
    out.write(f"Manual Audit Summary - {len(feedback)} User-Touched Rows\n")
    out.write("-" * 80 + "\n")
    for f in feedback:
        out.write(f"Row {f['row']:<5} | {f['term']:<40} | {f['status']:<15} | {f['reason']}\n")
