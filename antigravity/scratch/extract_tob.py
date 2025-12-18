import csv

def extract_tome_of_beasts(input_path, output_path):
    keywords = []
    
    # Check the header of the downloaded file first to know column names
    # But I'll assume standard CSV and inspect it if needed.
    # Based on search result: Name, Size, Type, Environment, HP, AC, Initiative, Alignment, CR, Source
    
    with open(input_path, 'r', encoding='utf-8', errors='replace') as f:
        reader = csv.DictReader(f)
        for row in reader:
            source = row.get('Source', '').strip()
            name = row.get('Name', '').strip()
            
            # Filter for Tome of Beasts
            # Source might be "Tome of Beasts", "Tome of Beasts 2", "TOB", etc.
            if 'Tome of Beasts' in source or 'TOB' in source.upper():
                # Clean up source name to be consistent
                if 'Tome of Beasts 2' in source:
                    clean_source = 'Tome of Beasts 2'
                elif 'Tome of Beasts 3' in source:
                    clean_source = 'Tome of Beasts 3'
                else:
                    clean_source = 'Tome of Beasts 1'
                
                # We only want Tome of Beasts 1 for now based on user request?
                # User listed "Tome of Beasts 1".
                # But if I have others, might as well add them?
                # User list included "Tome of Beasts 1".
                # I'll stick to TOB1 for now to match the specific request, or maybe all if easy.
                # Let's just do TOB1 for now to be safe, or label them correctly.
                
                if clean_source == 'Tome of Beasts 1':
                    keywords.append([name, 'Monster', 'Generic', clean_source])

    # Write to output
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        # writer.writerow(['Keyword', 'Category', 'World', 'Source']) # No header for append
        writer.writerows(keywords)
        
    return len(keywords)

if __name__ == "__main__":
    count = extract_tome_of_beasts(
        r'c:\Users\Yorri\.gemini\antigravity\scratch\github_monsters.csv',
        r'c:\Users\Yorri\.gemini\antigravity\scratch\tob_extracted.csv'
    )
    print(f"Extracted {count} monsters.")
