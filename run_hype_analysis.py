from google.cloud import bigquery

def run_analysis():
    client = bigquery.Client()
    
    with open("backerkit_hype_analysis.sql", "r") as f:
        query = f.read()
        
    print("Running analysis query...")
    try:
        query_job = client.query(query)
        result = query_job.result() # Wait for completion
        print(f"Query executed successfully. Created table backerkit_hype_analysis.")
    except Exception as e:
        print(f"Query failed: {e}")

if __name__ == "__main__":
    run_analysis()
