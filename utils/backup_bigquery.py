import subprocess
import datetime
import json
import sys

# CONFIGURATION
PROJECT_ID = "dnd-trends-index"
BUCKET_NAME = "gs://dnd-trends-backups"
DATASETS = [
    "commercial_data",
    "dnd_trends_categorized",
    "dnd_trends_raw",
    "gold_data",
    "silver_data",
    "social_data"
]

timestamp = datetime.datetime.now().strftime("%Y-%m-%d-%H%M")
print(f"🚀 Starting BigQuery Backup for {PROJECT_ID} at {timestamp}...")

for dataset in DATASETS:
    print(f"\n📂 Scanning Dataset: {dataset}...")
    
    # 1. List all tables
    try:
        list_cmd = ["bq", "ls", "--format=json", "--max_results=1000", f"{PROJECT_ID}:{dataset}"]
        result = subprocess.run(list_cmd, capture_output=True, text=True)
        
        if result.returncode != 0:
            print(f"   ⚠️ Error listing {dataset}: {result.stderr}")
            continue

        try:
            tables = json.loads(result.stdout)
        except json.JSONDecodeError:
            print("   ⚠️ No tables found or bad response.")
            continue
        
        # 2. Loop through tables
        for t in tables:
            table_id = t['tableReference']['tableId']
            table_type = t.get('type', 'TABLE')

            # SKIP VIEWS (Virtual tables don't need backup)
            if table_type == 'VIEW':
                print(f"   ⏭️ Skipping View: {table_id}")
                continue

            # Export REAL Tables
            full_table_path = f"{PROJECT_ID}:{dataset}.{table_id}"
            # Changing format to JSON to handle complex D&D data structures
            destination_uri = f"{BUCKET_NAME}/{timestamp}/{dataset}/{table_id}-*.json"
            
            print(f"   --> Exporting {table_id}...", end=" ", flush=True)
            
            extract_cmd = [
                "bq", "extract",
                "--destination_format=NEWLINE_DELIMITED_JSON",
                full_table_path,
                destination_uri
            ]
            
            extract_proc = subprocess.run(extract_cmd, capture_output=True, text=True)
            
            if extract_proc.returncode == 0:
                print("✅ Success")
            else:
                # Filter out the "Waiting..." noise to show the real error
                error_msg = extract_proc.stderr.replace("Waiting on bqjob", "").strip()
                print(f"\n      ❌ Failed. Details: {error_msg[:200]}...") # Show first 200 chars of error

    except Exception as e:
        print(f"   CRITICAL FAILURE on {dataset}: {str(e)}")

print("\n🎉 Backup Run Complete. Check your storage bucket.")
