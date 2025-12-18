
import pandas as pd
import glob
import os

# Path to the directory containing the CSVs
data_dir = r"c:\Users\Yorri\Downloads\sample dnd backgrounds"
all_files = glob.glob(os.path.join(data_dir, "*.csv"))

results = {}

for filename in all_files:
    try:
        df = pd.read_csv(filename, skiprows=2) # based on the inspection, header is on line 3 (index 2)
        # The first column is 'Week', the rest are data
        data_cols = df.columns[1:]
        
        for col in data_cols:
            # Clean column name to extracting the 'variation' part might be tricky if the term changes
            # But here we just want to see which specific *full string* got hits, or if we can detect a pattern.
            # Actually, let's just sum the column.
            total_hits = df[col].replace('<1', 0).sum() 
            results[col] = total_hits
            
    except Exception as e:
        print(f"Error processing {filename}: {e}")

# Display sorted results
sorted_results = sorted(results.items(), key=lambda x: x[1], reverse=True)
for term, hits in sorted_results:
    if hits > 0:
        print(f"{term}: {hits}")
