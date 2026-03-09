from google.cloud import bigquery
import pandas as pd

client = bigquery.Client()

def audit_coverage():
    print("running coverage audit...")
    query = """
    WITH PilotTerms AS (
        SELECT term_id, search_term, original_keyword 
        FROM `dnd-trends-index.dnd_trends_categorized.expanded_search_terms`
        WHERE is_pilot = TRUE
    ),
    Trends AS (
        SELECT search_term, MAX(date) as last_trend_date, COUNT(*) as points
        FROM `dnd-trends-index.dnd_trends_categorized.trend_data_pilot`
        GROUP BY search_term
    ),
    Wiki AS (
        -- Simple match on original keyword to article title
        -- This is heuristic. Ideally we use a registry mapping.
        SELECT w.article_title, MAX(v.date) as last_wiki_date
        FROM `dnd-trends-index.social_data.wikipedia_daily_views` v
        JOIN `dnd-trends-index.social_data.wikipedia_article_registry` w
          ON v.article_title = w.article_title
        GROUP BY w.article_title
    ),
    Fandom AS (
        SELECT article_title, MAX(extraction_date) as last_fandom_date
        FROM `dnd-trends-index.dnd_trends_raw.fandom_daily_metrics`
        GROUP BY article_title
    )
    
    SELECT 
        p.search_term,
        p.original_keyword,
        t.last_trend_date,
        w.last_wiki_date,
        f.last_fandom_date,
        CASE WHEN t.last_trend_date IS NOT NULL THEN 'OK' ELSE 'MISSING' END as Status_Trends,
        CASE WHEN w.last_wiki_date IS NOT NULL THEN 'OK' ELSE 'MISSING' END as Status_Wiki,
        CASE WHEN f.last_fandom_date IS NOT NULL THEN 'OK' ELSE 'MISSING' END as Status_Fandom
    FROM PilotTerms p
    LEFT JOIN Trends t ON p.search_term = t.search_term
    LEFT JOIN Wiki w ON p.original_keyword = w.article_title -- Attempt direct match
    LEFT JOIN Fandom f ON p.original_keyword = f.article_title -- Attempt direct match
    ORDER BY p.search_term
    """
    
    df = client.query(query).to_dataframe()
    
    print(f"Total Pilot Terms: {len(df)}")
    
    missing_trends = df[df['Status_Trends'] == 'MISSING']
    missing_wiki = df[df['Status_Wiki'] == 'MISSING']
    missing_fandom = df[df['Status_Fandom'] == 'MISSING']
    
    print(f"Missing Google Trends: {len(missing_trends)}")
    print(f"Missing Wikipedia: {len(missing_wiki)}")
    print(f"Missing Fandom: {len(missing_fandom)}")
    
    if len(missing_trends) > 0:
        print("\nSample Missing Trends:")
        print(missing_trends.head(5)[['search_term']])
        
    # Validation of "Working 0-100"
    # We check if we have RECENT data (last 7 days)
    # df['last_trend_date'] = pd.to_datetime(df['last_trend_date'])
    # recent_cutoff = pd.Timestamp.now() - pd.Timedelta(days=7)
    # stale_trends = df[df['last_trend_date'] < recent_cutoff]
    # print(f"Stale Google Trends (<7d): {len(stale_trends)}")

if __name__ == "__main__":
    audit_coverage()
