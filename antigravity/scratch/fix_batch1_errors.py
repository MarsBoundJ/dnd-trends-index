import csv
import os

def fix_csv_errors(file_path):
    temp_file = file_path + '.tmp'
    
    with open(file_path, 'r', encoding='utf-8', newline='') as f_in, \
         open(temp_file, 'w', encoding='utf-8', newline='') as f_out:
        
        reader = csv.reader(f_in)
        writer = csv.writer(f_out)
        
        try:
            header = next(reader)
            writer.writerow(header)
        except StopIteration:
            return

        seen_rows = set()
        # We need to preserve order, so we can't just use a set for everything if we want to keep it sorted-ish.
        # But we can check if the current row is exactly the same as the *previous* row (consecutive duplicates) 
        # or just track all seen rows if the file isn't huge. 1700 lines is small.
        
        previous_row = None
        
        for row in reader:
            # Fix 1: Remove "Frozen Sick" as Location
            # Row format: Keyword, Category, World, Source, Ruleset
            if len(row) >= 2 and row[0] == "Frozen Sick" and row[1] == "Location":
                continue
                
            # Fix 2: Remove duplicates
            # User said 1690 and 1691.
            # I'll just remove exact duplicates globally to be safe and clean.
            row_tuple = tuple(row)
            if row_tuple in seen_rows:
                continue
            seen_rows.add(row_tuple)
            
            writer.writerow(row)
            
    os.replace(temp_file, file_path)
    print("Fixed CSV errors.")

if __name__ == "__main__":
    fix_csv_errors(r'c:\Users\Yorri\.gemini\antigravity\scratch\dnd_keywords.csv')
