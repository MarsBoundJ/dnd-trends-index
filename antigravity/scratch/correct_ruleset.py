import csv
import os

def correct_ruleset(file_path):
    temp_file = file_path + '.tmp'
    
    with open(file_path, 'r', encoding='utf-8', newline='') as f_in, \
         open(temp_file, 'w', encoding='utf-8', newline='') as f_out:
        
        reader = csv.reader(f_in)
        writer = csv.writer(f_out)
        
        try:
            header = next(reader)
            writer.writerow(header)
        except StopIteration:
            return # Empty file
            
        # Header is line 1.
        # Loop starts at line 2.
        current_line = 2
        
        for row in reader:
            if 522 <= current_line <= 647:
                # Ensure row has enough columns, if not pad it (though it should)
                while len(row) < 5:
                    row.append('')
                row[4] = '2024'
            
            writer.writerow(row)
            current_line += 1
            
    os.replace(temp_file, file_path)
    print(f"Updated Ruleset to 2024 for lines 522-647.")

if __name__ == "__main__":
    correct_ruleset(r'c:\Users\Yorri\.gemini\antigravity\scratch\dnd_keywords.csv')
