from google.cloud import bigquery
import json

client = bigquery.Client()

def final_test():
    # Update 'Me' back to Mechanic
    sql = """
        UPDATE `dnd-trends-index.dnd_trends_categorized.concept_library`
        SET category = 'Mechanic', is_active = TRUE
        WHERE concept_name = 'Me'
    """
    try:
        client.query(sql, location='US').result()
        print("Updated 'Me' to Mechanic.")
        
        # Verify
        verify_sql = "SELECT category, is_active FROM `dnd-trends-index.dnd_trends_categorized.concept_library` WHERE concept_name = 'Me'"
        row = list(client.query(verify_sql, location='US').result())[0]
        print(f"Verification: Category={row.category}, Active={row.is_active}")
        
        # Now Archive it back to be safe (as per previous scrub)
        archive_sql = """
            UPDATE `dnd-trends-index.dnd_trends_categorized.concept_library`
            SET category = 'Archive', is_active = FALSE
            WHERE concept_name = 'Me'
        """
        client.query(archive_sql, location='US').result()
        print("Archived 'Me' back.")
    except Exception as e:
        print(f"Test failed: {e}")

if __name__ == "__main__":
    final_test()
