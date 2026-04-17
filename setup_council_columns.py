"""Add Council byline columns to gold_data.daily_articles.

One-off schema migration for Step 9a. Adds four nullable columns used by the
new Council writers in cloud_functions/daily_journalist/main.py:

  - author_name       STRING  e.g. "The Loremaster"
  - author_beat       STRING  e.g. "Industry history, financial autopsies..."
  - author_bio        STRING  one-line writer bio for the card
  - council_version   STRING  "v1" for Council rows, NULL for legacy rows

Legacy rows stay as-is (all four columns NULL). New Council rows set all four.
This lets the frontend filter cleanly (WHERE council_version = 'v1') and lets
us kill legacy with a single DELETE after parallel-run signoff.

Idempotent: ALTER TABLE ... ADD COLUMN IF NOT EXISTS is safe to re-run.

Run locally after authenticating with gcloud ADC:
    python setup_council_columns.py
"""

from google.cloud import bigquery

PROJECT_ID = "dnd-trends-index"
DATASET_ID = "gold_data"
TABLE_ID = "daily_articles"

ALTER_SQL = f"""
ALTER TABLE `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
    ADD COLUMN IF NOT EXISTS author_name STRING,
    ADD COLUMN IF NOT EXISTS author_beat STRING,
    ADD COLUMN IF NOT EXISTS author_bio STRING,
    ADD COLUMN IF NOT EXISTS council_version STRING
"""

VERIFY_SQL = f"""
SELECT column_name, data_type
FROM `{PROJECT_ID}.{DATASET_ID}.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = '{TABLE_ID}'
ORDER BY ordinal_position
"""


def run() -> None:
    client = bigquery.Client(project=PROJECT_ID)

    print(f"Altering {PROJECT_ID}.{DATASET_ID}.{TABLE_ID}...")
    client.query(ALTER_SQL).result()
    print("  Success.")

    print("Verifying schema:")
    rows = list(client.query(VERIFY_SQL).result())
    for row in rows:
        print(f"  - {row.column_name:20s} {row.data_type}")

    council_cols = {"author_name", "author_beat", "author_bio", "council_version"}
    actual = {row.column_name for row in rows}
    missing = council_cols - actual
    if missing:
        raise SystemExit(f"FAIL: columns missing after migration: {sorted(missing)}")
    print("\nAll Council columns present. Migration complete.")


if __name__ == "__main__":
    run()
