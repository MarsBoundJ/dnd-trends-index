from google.cloud import bigquery

PROJECT_ID = "dnd-trends-index"
client = bigquery.Client(project=PROJECT_ID)

def purge_failed():
    query = "DELETE FROM `dnd-trends-index.dnd_trends_raw.yt_video_index` WHERE transcript_available = FALSE"
    logger_query = client.query(query)
    logger_query.result()
    print("Successfully purged failed records from yt_video_index.")

if __name__ == "__main__":
    purge_failed()
