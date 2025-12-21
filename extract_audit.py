import csv

input_file = 'pilot_audit_reviewed.csv'
output_file = 'audit_summary_clean.txt'

with open(input_file, newline='', encoding='utf-8') as f:
    reader = csv.reader(f)
    headers = next(reader)
    
    with open(output_file, 'w', encoding='utf-8') as out:
        out.write(f"{'Row':<4} | {'Search Term':<40} | {'Status':<10} | {'Reason'}\n")
        out.write("-" * 100 + "\n")
        
        for i, row in enumerate(reader):
            if i >= 100:
                break
            
            # Extract columns based on headers found: search_term(0), Keep or Drop(4), Reason(5)
            # Need to be careful with indexing if columns moved
            term = row[0] if len(row) > 0 else ""
            status = row[4] if len(row) > 4 else ""
            reason = row[5] if len(row) > 5 else ""
            
            out.write(f"{i+2:<4} | {term:<40} | {status:<10} | {reason}\n")
