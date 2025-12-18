import csv
import os

def add_ruleset_column(file_path):
    temp_file = file_path + '.tmp'
    
    with open(file_path, 'r', encoding='utf-8', newline='') as infile, \
         open(temp_file, 'w', encoding='utf-8', newline='') as outfile:
        
        reader = csv.DictReader(infile)
        fieldnames = reader.fieldnames + ['Ruleset']
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        
        writer.writeheader()
        
        for row in reader:
            source = row.get('Source', '')
            if 'Monsters of Drakkenheim' in source:
                row['Ruleset'] = '2024'
            else:
                row['Ruleset'] = '2014'
            writer.writerow(row)
            
    # Replace original file
    os.replace(temp_file, file_path)
    print(f"Updated {file_path} with Ruleset column.")

if __name__ == "__main__":
    add_ruleset_column(r'c:\Users\Yorri\.gemini\antigravity\scratch\dnd_keywords.csv')
