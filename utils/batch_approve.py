import csv
import json
import time
import sys
import argparse
import requests
import concurrent.futures
from typing import List, Dict

API_URL = "https://us-central1-dnd-trends-index.cloudfunctions.net/bouncer-api/system/suggestions/update"
CHUNK_SIZE = 5
PAUSE_BETWEEN_CHUNKS = 1.0

def process_row(row: Dict[str, str]) -> str:
    """
    Processes a single row and returns a status code: 'SUCCESS', 'ERROR', or 'SKIP'
    """
    concept_name = row.get('concept_name', '').strip()
    action = row.get('action', '').strip().upper()
    category = row.get('category', '').strip()

    if not concept_name or not action:
        return 'SKIP'

    payload = {
        "concept_name": concept_name,
        "action": action
    }
    
    if action == 'APPROVE' and category:
        payload["category"] = category
    elif action == 'APPROVE' and not category:
        # If action is APPROVE but no category provided, we skip or error? 
        # User spec says "omit category for ARCHIVE", implying it's needed for APPROVE.
        return 'ERROR'

    try:
        response = requests.post(API_URL, json=payload, timeout=15)
        if response.status_code == 200:
            return 'SUCCESS'
        else:
            print(f"Server error for {concept_name}: {response.status_code} - {response.text}")
            return 'ERROR'
    except Exception as e:
        print(f"Network error for {concept_name}: {str(e)}")
        return 'ERROR'

def main():
    parser = argparse.ArgumentParser(description="Batch approve/archive suggestions via Bouncer API")
    parser.add_argument("csv_path", help="Path to the input CSV file")
    args = parser.parse_args()

    rows = []
    try:
        with open(args.csv_path, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = [row for row in reader]
    except FileNotFoundError:
        print(f"Error: File {args.csv_path} not found.")
        sys.exit(1)

    total = len(rows)
    success_count = 0
    error_count = 0
    skip_count = 0

    print(f"🚀 Starting batch process for {total} rows...")

    for i in range(0, total, CHUNK_SIZE):
        chunk = rows[i:i + CHUNK_SIZE]
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=CHUNK_SIZE) as executor:
            results = list(executor.map(process_row, chunk))

        for res in results:
            if res == 'SUCCESS': success_count += 1
            elif res == 'ERROR': error_count += 1
            else: skip_count += 1

        print(f"✅ Approved: {success_count} / ❌ Error: {error_count} / ⏭ Skipped: {skip_count}")
        
        if i + CHUNK_SIZE < total:
            time.sleep(PAUSE_BETWEEN_CHUNKS)

    print("\n--- Final Counts ---")
    print(f"Total Processed: {total}")
    print(f"Success: {success_count}")
    print(f"Errors:  {error_count}")
    print(f"Skipped: {skip_count}")

if __name__ == "__main__":
    main()
