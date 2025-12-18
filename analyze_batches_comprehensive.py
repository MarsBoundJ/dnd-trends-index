
import pandas as pd
import glob
import os

# Path to the directory containing the CSVs
data_dir = r"c:\Users\Yorri\Downloads"

print(f"Scanning {data_dir} for batch test files...")

results = {}

# We look for files that likely contain our test data. 
# The user mentioned "test batch" in filenames, and we also have previous specific files.
for filename in os.listdir(data_dir):
    if not filename.endswith(".csv"):
        continue
        
    lower_name = filename.lower()
    # Filter for relevant files to avoid reading random CSVs in Downloads
    if "test batch" in lower_name or "wizard" in lower_name or "fighter" in lower_name or "rogue" in lower_name or "cleric" in lower_name or "sorcerer" in lower_name or "warlock" in lower_name or "paladin" in lower_name or "druid" in lower_name or "bard" in lower_name or "monk" in lower_name or "barbarian" in lower_name or "ranger" in lower_name or "artificer" in lower_name:
        filepath = os.path.join(data_dir, filename)
        
        try:
            # Inspection showed header usually at line 3 (skiprows=2)
            df = pd.read_csv(filepath, skiprows=2)
            
            # Simple validation: checks if first column looks like a date/week
            if df.empty or 'Week' not in str(df.columns[0]):
                continue

            file_stats = {}
            for col in df.columns[1:]: # Skip 'Week'
                # Clean up column name if it has extra google trends formatting like ": (United States)"
                clean_term = col.split(":")[0].strip()
                
                # Google trends uses "<1" for 0 sometimes
                total_hits = df[col].replace('<1', 0).astype(float).sum()
                file_stats[clean_term] = total_hits
            
            # Find the winner for this file
            if file_stats:
                sorted_stats = sorted(file_stats.items(), key=lambda x: x[1], reverse=True)
                results[filename] = sorted_stats

        except Exception as e:
            # print(f"Skipping {filename}: {e}")
            pass

# Print summary of winners
print("\n=== BATCH TEST RESULTS ===")
for filename, stats in sorted(results.items()):
    print(f"\nFile: {filename}")
    winner, win_hits = stats[0]
    print(f"  WINNER: '{winner}' ({int(win_hits)} hits)")
    
    # Print runner-ups to see the gap
    for term, hits in stats[1:]:
        if hits > 0:
            print(f"          '{term}' ({int(hits)} hits)")
