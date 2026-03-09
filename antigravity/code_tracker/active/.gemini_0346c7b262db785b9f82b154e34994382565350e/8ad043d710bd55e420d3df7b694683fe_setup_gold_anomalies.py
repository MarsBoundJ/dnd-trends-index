È
from google.cloud import bigquery

PROJECT_ID = "dnd-trends-index"
DATASET_ID = "gold_data"

def setup_anomalies():
    client = bigquery.Client()
    
    # Anomaly Detection View
    # Detects if Today's Score is > Avg + 3*StdDev of last 30 days
    
    sql_anomalies = f"""
    CREATE OR REPLACE VIEW `{PROJECT_ID}.{DATASET_ID}.anomalies` AS
    WITH stats AS (
        SELECT 
            date,
            keyword,
            trend_score_raw,
            AVG(trend_score_raw) OVER (
                PARTITION BY keyword 
                ORDER BY date 
                ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
            ) as avg_30d,
            STDDEV(trend_score_raw) OVER (
                PARTITION BY keyword 
                ORDER BY date 
                ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
            ) as stddev_30d
        FROM `{PROJECT_ID}.gold_data.trend_scores`
    )
    SELECT
        date,
        keyword,
        trend_score_raw,
        avg_30d,
        stddev_30d,
        (avg_30d + (3 * stddev_30d)) as anomaly_threshold,
        CASE 
            WHEN trend_score_raw > (avg_30d + (3 * stddev_30d)) AND trend_score_raw > 10 THEN TRUE 
            ELSE FALSE 
        END as is_anomaly
    FROM stats
    WHERE date = CURRENT_DATE() -- Alerts for Today
    """

    print("Creating View: Anomalies...")
    try:
        client.query(sql_anomalies).result()
        print("Success.")
    except Exception as e:
        print(f"Error creating Anomalies View: {e}")

if __name__ == "__main__":
    setup_anomalies()
È"(0346c7b262db785b9f82b154e34994382565350e26file:///C:/Users/Yorri/.gemini/setup_gold_anomalies.py:file:///C:/Users/Yorri/.gemini