import os
from google.cloud import bigquery

# Ensure we're authenticated correctly (given turbo mode constraints, this shouldn't be strictly necessary if docker has default creds, but good practice if dnd-key.json is present)
# If dnd-key.json exists we can use it, but user mentioned not to modify it. I'll just use default auth.

client = bigquery.Client(project="dnd-trends-index")

queries = [
    # itchio_products
    """
    CREATE TABLE IF NOT EXISTS `dnd-trends-index.dnd_trends_raw.itchio_products` (
        product_id STRING,
        title STRING,
        creator STRING,
        price FLOAT64,
        is_pwyw BOOLEAN,
        tags ARRAY<STRING>,
        aesthetic_clusters ARRAY<STRING>,
        list_type STRING,
        collected_date TIMESTAMP
    )
    """,
    # itchio_jams
    """
    CREATE TABLE IF NOT EXISTS `dnd-trends-index.dnd_trends_raw.itchio_jams` (
        jam_id STRING,
        jam_title STRING,
        theme_keywords ARRAY<STRING>,
        submission_count INT64,
        start_date TIMESTAMP,
        end_date TIMESTAMP
    )
    """,
    # itchio_price_history
    """
    CREATE TABLE IF NOT EXISTS `dnd-trends-index.dnd_trends_raw.itchio_price_history` (
        product_id STRING,
        price FLOAT64,
        is_pwyw BOOLEAN,
        observed_date TIMESTAMP
    )
    """,
    # aesthetic_clusters
    """
    CREATE TABLE IF NOT EXISTS `dnd-trends-index.dnd_trends_categorized.aesthetic_clusters` (
        cluster_name STRING,
        associated_tags ARRAY<STRING>
    )
    """,
    # Clear the table before seeding if it already existed
    """
    TRUNCATE TABLE `dnd-trends-index.dnd_trends_categorized.aesthetic_clusters`
    """,
    # Insert initial rows
    """
    INSERT INTO `dnd-trends-index.dnd_trends_categorized.aesthetic_clusters` (cluster_name, associated_tags)
    VALUES
        ('LO_FI_NOSTALGIA', ['8-bit', 'pixel-art', 'retro']),
        ('COZY_DOMESTIC', ['cozy', 'slice-of-life', 'cottagecore']),
        ('LIMINAL_DREAD', ['liminal-spaces', 'backrooms', 'scp']),
        ('ZINE_DIY', ['zine', 'hand-drawn', 'pamphlet']),
        ('QUEER_IDENTITY', ['lgbtq', 'coming-of-age', 'identity']),
        ('FOUND_FOOTAGE_Y2K', ['y2k', 'vhs', 'glitch']),
        ('COZY_HORROR', ['dark-fantasy', 'soft-horror']),
        ('SOLARPUNK_UTOPIA', ['solarpunk', 'ecology', 'hopepunk'])
    """
]

for query in queries:
    try:
        job = client.query(query)
        job.result()
        print("Success:", query.strip().split("\\n")[0][:50])
    except Exception as e:
        print("Error executing query:", e)
