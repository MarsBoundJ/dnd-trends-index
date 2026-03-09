from google.cloud import bigquery
client = bigquery.Client(project='dnd-trends-index')
ddl = """
CREATE TABLE IF NOT EXISTS `dnd-trends-index.gold_data.chat_archives` (
    session_id STRING,
    user_email STRING,
    chat_history JSON,
    summary_text STRING,
    created_at TIMESTAMP
);
"""
client.query(ddl).result()
print('Table created successfully.')
