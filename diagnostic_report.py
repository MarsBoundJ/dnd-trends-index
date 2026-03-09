from google.cloud import bigquery
import json

def run_diagnostics():
    client = bigquery.Client()
    query = """
    SELECT video_id, channel_id, LENGTH(transcript_text) as transcript_len, transcript_available 
    FROM `dnd-trends-index.dnd_trends_raw.yt_video_index` 
    LIMIT 5
    """
    results = client.query(query).result()
    report = []
    for row in results:
        report.append(dict(row))
    print(json.dumps(report, indent=2))

if __name__ == "__main__":
    run_diagnostics()
