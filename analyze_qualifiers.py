import pandas as pd
import glob

files = ['champion.csv', 'warden.csv']

for f in files:
    try:
        print(f"\nAnalyzing {f}...")
        # Skip rows 0 and 1 (Google Trends header junk)
        df = pd.read_csv(f, skiprows=2)
        
        # Clean column names (remove ": (United States)")
        df.columns = [c.split(":")[0].strip() for c in df.columns]
        
        # Drop 'Week'
        if 'Week' in df.columns:
            df = df.drop(columns=['Week'])
            
        # Replace '<1' with 0 and sum
        sums = {}
        for col in df.columns:
            # clean non-numeric crap
            cleaned_series = df[col].astype(str).replace('<1', '0')
            # Coerce to numeric
            sums[col] = pd.to_numeric(cleaned_series, errors='coerce').sum()
            
        # Sort and print
        sorted_sums = sorted(sums.items(), key=lambda x: x[1], reverse=True)
        for term, val in sorted_sums:
            print(f"  {term}: {val}")
            
    except Exception as e:
        print(f"Error reading {f}: {e}")
