"""Create gold_data.freight_index_daily for the Freightos FBX harvester.

One-off schema setup for Step 9a. Stores a weekly snapshot of ocean-freight
container rates on the lanes that matter for TTRPG fulfillment. Feeds The
Quartermaster's articles. See cloud_functions/freight_index_harvester/main.py.

Idempotent via CREATE TABLE IF NOT EXISTS.

Run locally after authenticating with gcloud ADC:
    python setup_freight_index_daily.py
"""

from google.cloud import bigquery

PROJECT_ID = "dnd-trends-index"
DATASET_ID = "gold_data"
TABLE_ID = "freight_index_daily"
TABLE_REF = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

CREATE_SQL = f"""
CREATE TABLE IF NOT EXISTS `{TABLE_REF}` (
    date          DATE      NOT NULL,
    lane_code     STRING    NOT NULL,
    lane_name     STRING,
    index_value   FLOAT64,
    wow_delta_pct FLOAT64,
    source        STRING,
    raw_json      JSON
)
PARTITION BY date
CLUSTER BY lane_code
"""

VERIFY_SQL = f"""
SELECT column_name, data_type, is_nullable
FROM `{PROJECT_ID}.{DATASET_ID}.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = '{TABLE_ID}'
ORDER BY ordinal_position
"""


def run() -> None:
    client = bigquery.Client(project=PROJECT_ID)

    print(f"Creating {TABLE_REF} (if not exists)...")
    client.query(CREATE_SQL).result()
    print("  Success.")

    print("Verifying schema:")
    rows = list(client.query(VERIFY_SQL).result())
    for row in rows:
        nullable = "NULL" if row.is_nullable == "YES" else "NOT NULL"
        print(f"  - {row.column_name:15s} {row.data_type:10s} {nullable}")

    expected = {"date", "lane_code", "lane_name", "index_value", "wow_delta_pct", "source", "raw_json"}
    actual = {row.column_name for row in rows}
    missing = expected - actual
    if missing:
        raise SystemExit(f"FAIL: columns missing: {sorted(missing)}")
    print("\nTable ready.")


if __name__ == "__main__":
    run()
