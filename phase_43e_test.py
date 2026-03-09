from google.cloud import bigquery
import json

client = bigquery.Client()

def test_flow():
    # 1. Find an Uncategorized concept
    find_sql = "SELECT concept_name FROM `dnd-trends-index.dnd_trends_categorized.concept_library` WHERE category = 'Uncategorized' LIMIT 1"
    row = list(client.query(find_sql, location='US').result())
    if not row:
        print("No Uncategorized concepts found.")
        return
    
    concept = row[0].concept_name
    print(f"Targeting concept: {concept}")

    # 2. Update via the new logic (mimicking the POST route)
    update_sql = """
        UPDATE `dnd-trends-index.dnd_trends_categorized.concept_library`
        SET category = 'Subclass', is_active = TRUE
        WHERE concept_name = @name
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("name", "STRING", concept)
        ]
    )
    client.query(update_sql, job_config=job_config).result()
    print(f"Updated {concept} to Subclass.")

    # 3. Verify
    verify_sql = "SELECT concept_name, category FROM `dnd-trends-index.dnd_trends_categorized.concept_library` WHERE concept_name = @name"
    verify_row = list(client.query(verify_sql, job_config=job_config).result())
    print(f"Verification: {verify_row[0].concept_name} is now {verify_row[0].category}")

if __name__ == "__main__":
    test_flow()
