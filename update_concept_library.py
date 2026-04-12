import csv
import sys
from google.cloud import bigquery

def run_update(batch_file):
    client = bigquery.Client(project='dnd-trends-index')
    try:
        with open(batch_file) as f:
            rows = list(csv.DictReader(f))
    except FileNotFoundError:
        print(f"Error: {batch_file} not found.")
        return
    except Exception as e:
        print(f"Error reading {batch_file}: {e}")
        return

    for row in rows:
        sql = """UPDATE `dnd-trends-index.dnd_trends_categorized.concept_library`
                 SET source_book = @sb, publisher = @pub
                 WHERE concept_name = @name"""
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("sb", "STRING", row['source_book']),
                bigquery.ScalarQueryParameter("pub", "STRING", row['publisher']),
                bigquery.ScalarQueryParameter("name", "STRING", row['concept_name']),
            ]
        )
        try:
            client.query(sql, job_config=job_config).result()
        except Exception as e:
            print(f"Error updating {row['concept_name']}: {e}")

    print(f"Done: {len(rows)} rows updated")

if __name__ == '__main__':
    if len(sys.argv) > 1:
        run_update(sys.argv[1])
    else:
        print("Usage: python3 update_concept_library.py <batch_file>")
