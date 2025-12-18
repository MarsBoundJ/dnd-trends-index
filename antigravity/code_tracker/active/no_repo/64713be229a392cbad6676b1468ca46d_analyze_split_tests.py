á
import pandas as pd
import glob
import os

# Path to the directory containing the CSVs
data_dir = r"c:\Users\Yorri\Downloads"

# potential expected files based on the request
expected_patterns = [
    "fighter 2014", "fighter 2024",
    "wizard 2014", "wizard 2024",
    "rogue 2014", "rogue 2024", 
    "counterspell", "cure wounds", "guidance"
]

results = {}

print(f"Scanning {data_dir} for expected files...")

found_files = []
for filename in os.listdir(data_dir):
    if not filename.endswith(".csv"):
        continue
    
    # Check if filename roughly matches one of our expected patterns
    # (Checking case insensitive match for simpler verification)
    lower_name = filename.lower()
    if any(pat in lower_name for pat in expected_patterns):
        found_files.append(os.path.join(data_dir, filename))

print(f"Found {len(found_files)} matching files.")

for filepath in found_files:
    try:
        # Google Trends CSVs often have the header on line 2 (index 1) or 3 (index 2)
        # Sample showed header on line 3, so skiprows=2
        df = pd.read_csv(filepath, skiprows=2) 
        
        # Identify data columns (skip 'Week')
        if 'Week' in df.columns[0]: 
             data_cols = df.columns[1:]
        else:
             # Fallback if format is slightly different 
             data_cols = df.columns[1:]

        print(f"\n--- {os.path.basename(filepath)} ---")
        file_stats = {}
        for col in data_cols:
            # Google trends uses "<1" for 0 sometimes
            total_hits = df[col].replace('<1', 0).astype(float).sum()
            file_stats[col] = total_hits
            
        # Sort and print stats for this file
        sorted_stats = sorted(file_stats.items(), key=lambda x: x[1], reverse=True)
        for term, hits in sorted_stats:
            print(f"  {term}: {int(hits)}")
            
    except Exception as e:
        print(f"Error processing {os.path.basename(filepath)}: {e}")
á*cascade0825file:///C:/Users/Yorri/.gemini/analyze_split_tests.py