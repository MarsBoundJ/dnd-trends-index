from google.cloud import bigquery
import json

client = bigquery.Client()

def check_wikipedia():
    print("--- Wikipedia Registry Check ---")
    tables = [
        "dnd-trends-index.social_data.wikipedia_article_registry",
        "dnd-trends-index.dnd_trends_categorized.wikipedia_article_registry"
    ]
    for table in tables:
        try:
            query = f"SELECT count(*) as count FROM `{table}`"
            # Explicitly set location to US
            results = client.query(query, location="US").to_dataframe()
            print(f"Table {table}: {results['count'][0]} rows")
        except Exception as e:
            print(f"Table {table}: Not found or error: {e}")

def seed_amazon():
    print("\n--- Amazon ASIN Seeding ---")
    asin_map = "dnd-trends-index.dnd_trends_categorized.amazon_asin_map"
    core_asins = [
        ("Player's Handbook", "0786969514"),
        ("Monster Manual", "0786965616"),
        ("Dungeon Master's Guide", "0786965624"),
        ("Tasha's Cauldron of Everything", "0786967023"),
        ("Xanathar's Guide to Everything", "0786966116")
    ]
    
    # Check if table exists
    try:
        client.get_table(asin_map)
        print(f"Table {asin_map} exists.")
    except Exception:
        print(f"Creating table {asin_map}...")
        schema = [
            bigquery.SchemaField("concept_name", "STRING"),
            bigquery.SchemaField("asin", "STRING")
        ]
        table = bigquery.Table(asin_map, schema=schema)
        client.create_table(table, location="US")

    for name, asin in core_asins:
        # Escape single quotes for SQL
        safe_name = name.replace("'", "''")
        query = f"SELECT count(*) as count FROM `{asin_map}` WHERE asin = '{asin}'"
        df = client.query(query, location="US").to_dataframe()
        exists = df['count'][0] > 0
        if not exists:
            print(f"Inserting {name} ({asin})...")
            dml = f"INSERT INTO `{asin_map}` (concept_name, asin) VALUES ('{safe_name}', '{asin}')"
            client.query(dml, location="US").result()
        else:
            print(f"{name} ({asin}) already exists.")

if __name__ == "__main__":
    check_wikipedia()
    seed_amazon()
