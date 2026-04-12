import os
from google.cloud import bigquery

PROJECT_ID = "dnd-trends-index"

def get_bq_client():
    return bigquery.Client(project=PROJECT_ID)

def create_market_whitespace_view(client):
    sql = """
    CREATE OR REPLACE VIEW `dnd-trends-index.gold_data.view_market_whitespace` AS
    WITH GoogleDemand AS (
        SELECT 
            mapped_cluster AS aesthetic_cluster,
            AVG(momentum_score) as avg_consumer_demand_momentum
        FROM `dnd-trends-index.gold_data.view_api_leaderboards`
        WHERE mapped_cluster IS NOT NULL
        GROUP BY 1
    ),
    FandomDemand AS (
        SELECT 
            predicted_cluster AS aesthetic_cluster,
            SUM(wiki_views) as total_fandom_interest
        FROM `dnd-trends-index.dnd_trends_raw.fandom_metrics`
        WHERE predicted_cluster IS NOT NULL AND DATE(timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
        GROUP BY 1
    ),
    ItchioSupply AS (
        SELECT 
            ac.cluster_id AS aesthetic_cluster,
            COUNT(DISTINCT j.jam_id) as upcoming_jams_count
        FROM `dnd-trends-index.dnd_trends_raw.itchio_jams` j
        CROSS JOIN UNNEST(j.theme_keywords) AS kw
        JOIN `dnd-trends-index.dnd_trends_categorized.aesthetic_clusters` ac 
            ON kw IN UNNEST(ac.tags)
        GROUP BY 1
    ),
    CurrentMarketSupply AS (
        SELECT 
            assigned_cluster AS aesthetic_cluster,
            COUNT(DISTINCT product_id) as current_catalog_supply
        FROM `dnd-trends-index.dnd_trends_categorized.core_dmsguild_view`
        WHERE assigned_cluster IS NOT NULL
        GROUP BY 1
    )
    SELECT 
        COALESCE(g.aesthetic_cluster, f.aesthetic_cluster, i.aesthetic_cluster, c.aesthetic_cluster) as aesthetic_cluster,
        COALESCE(g.avg_consumer_demand_momentum, 0) as consumer_demand,
        COALESCE(f.total_fandom_interest, 0) as dedicated_fandom_interest,
        COALESCE(i.upcoming_jams_count, 0) as upstream_creator_intent,
        COALESCE(c.current_catalog_supply, 0) as established_market_supply,
        
        -- The Gap Score: High Demand & Intent, but Low Established Supply
        (COALESCE(g.avg_consumer_demand_momentum, 0) * 0.4 + 
         LOG(COALESCE(f.total_fandom_interest, 1)) * 0.2 + 
         COALESCE(i.upcoming_jams_count, 0) * 0.4) 
         / NULLIF(COALESCE(c.current_catalog_supply, 1), 0) AS opportunity_gap_score

    FROM GoogleDemand g
    FULL OUTER JOIN FandomDemand f ON g.aesthetic_cluster = f.aesthetic_cluster
    FULL OUTER JOIN ItchioSupply i ON COALESCE(g.aesthetic_cluster, f.aesthetic_cluster) = i.aesthetic_cluster
    FULL OUTER JOIN CurrentMarketSupply c ON COALESCE(g.aesthetic_cluster, f.aesthetic_cluster, i.aesthetic_cluster) = c.aesthetic_cluster
    ORDER BY opportunity_gap_score DESC;
    """
    
    print("Creating view_market_whitespace in BigQuery...")
    client.query(sql).result()
    print("✅ Successfully created gold_data.view_market_whitespace.")

if __name__ == "__main__":
    client = get_bq_client()
    create_market_whitespace_view(client)
