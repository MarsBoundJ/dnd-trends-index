
from google.cloud import bigquery

def run_bulk_cleanup():
    client = bigquery.Client(project='dnd-trends-index')
    
    queries = [
        # 1. Fix leading-space garbled categories -> Equipment
        """
        UPDATE `dnd-trends-index.dnd_trends_categorized.concept_library`
        SET category = 'Equipment', last_processed_at = CURRENT_TIMESTAMP()
        WHERE category LIKE ' %' AND is_active = TRUE
        """,
        # 2. Fix spellcasting glossary terms -> Mechanic
        """
        UPDATE `dnd-trends-index.dnd_trends_categorized.concept_library`
        SET category = 'Mechanic', last_processed_at = CURRENT_TIMESTAMP()
        WHERE category = 'spellcasting' AND is_active = TRUE
        """,
        # 3. Fix specific bad categories -> Mechanic
        """
        UPDATE `dnd-trends-index.dnd_trends_categorized.concept_library`
        SET category = 'Mechanic', last_processed_at = CURRENT_TIMESTAMP()
        WHERE concept_name IN (
          'Size', 'Combat Encounters', 'Round Down',
          'Damage Threshold', 'Heroic Inspiration',
          'Improvised Weapon', 'Common Sign Language'
        ) AND is_active = TRUE
        """,
        # 4. Fix Stabling per day -> Equipment
        """
        UPDATE `dnd-trends-index.dnd_trends_categorized.concept_library`
        SET category = 'Equipment', last_processed_at = CURRENT_TIMESTAMP()
        WHERE concept_name = 'Stabling per day' AND is_active = TRUE
        """
    ]
    
    labels = [
        "Leading-space cleanup",
        "spellcasting -> Mechanic",
        "Specific bad categories -> Mechanic",
        "Stabling per day -> Equipment"
    ]
    
    for i, (q, label) in enumerate(zip(queries, labels), 1):
        print(f"Executing Query #{i} ({label})...")
        try:
            job = client.query(q)
            job.result()
            print(f"Query #{i} complete: {job.num_dml_affected_rows} rows affected.")
        except Exception as e:
            print(f"Error in Query #{i}: {e}")

if __name__ == "__main__":
    run_bulk_cleanup()
