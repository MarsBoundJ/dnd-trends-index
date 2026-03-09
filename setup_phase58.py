from google.cloud import bigquery

def setup_phase58_tables():
    client = bigquery.Client(project='dnd-trends-index')
    
    queries = [
        """
        CREATE TABLE IF NOT EXISTS `dnd-trends-index.dnd_trends_raw.ai_suggestions` (
            concept_name STRING,
            suggested_category STRING,
            reason STRING,
            status STRING DEFAULT 'PENDING'
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS `dnd-trends-index.dnd_trends_raw.rpggeek_product_stats` (
            date DATE,
            concept_name STRING,
            rpggeek_id STRING,
            item_type STRING,
            owned_count INT64,
            rating_count INT64,
            average_rating FLOAT64,
            geek_rating FLOAT64,
            rank INT64,
            year_published INT64
        )
        """
    ]
    
    for q in queries:
        print(f"Executing: {q[:50]}...")
        client.query(q).result()
        
    print("Tables created successfully")

if __name__ == '__main__':
    setup_phase58_tables()
