import os
import json
import time
from google.cloud import bigquery
import google.generativeai as genai

GENAI_API_KEY = "AIzaSyCIGyZyvf4m13f46pb0GAVGy4lsd88yQJ8"

PROJECT_ID = "dnd-trends-index"

def get_bq_client():
    return bigquery.Client(project=PROJECT_ID)

def load_clusters(client):
    query = "SELECT cluster_name, associated_tags FROM `dnd-trends-index.dnd_trends_categorized.aesthetic_clusters`"
    clusters = {}
    for row in client.query(query).result():
        # Clean tags to lower case for matching
        tags = [t.lower() for t in row['associated_tags']] if row['associated_tags'] else []
        clusters[row['cluster_name']] = tags
    return clusters

def get_untagged_products(client):
    query = """
    SELECT product_id, title, tags
    FROM `dnd-trends-index.dnd_trends_raw.itchio_products`
    WHERE ARRAY_LENGTH(aesthetic_clusters) = 0 OR aesthetic_clusters IS NULL
    """
    return list(client.query(query).result())

def update_product_clusters(client, product_id, clusters):
    table_ref = f"{PROJECT_ID}.dnd_trends_raw.itchio_products"
    
    # Use array conversion for parameterized queries with arrays
    query = f"""
    UPDATE `{table_ref}`
    SET aesthetic_clusters = UNNEST(JSON_EXTRACT_ARRAY(PARSE_JSON(@clusters_json)))
    WHERE product_id = @product_id
    """
    # actually UPDATE SET aesthetic_clusters = ARRAY(SELECT JSON_VALUE(x) FROM UNNEST(JSON_EXTRACT_ARRAY(PARSE_JSON(@clusters_json))) AS x)
    query = f"""
    UPDATE `{table_ref}`
    SET aesthetic_clusters = ARRAY(SELECT JSON_VALUE(x) FROM UNNEST(JSON_EXTRACT_ARRAY(PARSE_JSON(@clusters_json))) AS x)
    WHERE product_id = @product_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("clusters_json", "STRING", json.dumps(clusters)),
            bigquery.ScalarQueryParameter("product_id", "STRING", product_id)
        ]
    )
    client.query(query, job_config=job_config).result()

def run_tagger():
    client = get_bq_client()
    clusters = load_clusters(client)
    
    print(f"Loaded {len(clusters)} aesthetic clusters.")
    
    products = get_untagged_products(client)
    print(f"Found {len(products)} untagged products.")
    
    genai.configure(api_key=GENAI_API_KEY)
    model = genai.GenerativeModel('gemini-flash-latest')
    
    cluster_names = list(clusters.keys())
    
    for row in products:
        product_id = row['product_id']
        title = row['title']
        raw_tags = [t.lower() for t in row['tags']] if row['tags'] else []
        
        matched_clusters = set()
        
        # Part A: Exact Match
        for cluster_name, associated_tags in clusters.items():
            for tag in raw_tags:
                if tag in associated_tags:
                    matched_clusters.add(cluster_name)
                    
        # Part B: AI Inference Fallback
        if not matched_clusters:
            prompt = f"Based on this TTRPG title and description, assign it to one of these {len(cluster_names)} clusters: {', '.join(cluster_names)}. If it's a horror game about analog tech, use LIMINAL_DREAD. If it's a wholesome village game, use COZY_DOMESTIC. Title: {title}. Description: [Description not provided, rely on title]. Output ONLY the cluster name."
            
            try:
                response = model.generate_content(prompt)
                ai_cluster = response.text.strip()
                if ai_cluster in cluster_names:
                    matched_clusters.add(ai_cluster)
                    print(f"AI assigned '{title}' -> {ai_cluster}")
                else:
                    # try to see if it partial matches
                    for c_name in cluster_names:
                        if c_name in ai_cluster:
                            matched_clusters.add(c_name)
                            print(f"AI partial matched '{title}' -> {c_name}")
                            break
            except Exception as e:
                print(f"Gemini error for {product_id}: {e}")
                
        else:
            print(f"Exact match for '{title}' -> {matched_clusters}")
            
        if matched_clusters:
            update_product_clusters(client, product_id, list(matched_clusters))
            time.sleep(1) # rate limit prevention

if __name__ == "__main__":
    run_tagger()
