from google.cloud import bigquery
import json
from datetime import datetime

def audit_all_streams():
    client = bigquery.Client()
    
    streams = [
        {
            "name": "Google Trends",
            "table_id": "dnd-trends-index.dnd_trends_categorized.trend_data_pilot",
            "date_col": "date",
            "keyword_col": "search_term"
        },
        {
            "name": "Reddit",
            "table_id": "dnd-trends-index.dnd_trends_categorized.reddit_daily_metrics",
            "date_col": "extraction_date",
            "keyword_col": "keyword"
        },
        {
            "name": "Wikipedia",
            "table_id": "dnd-trends-index.social_data.wikipedia_daily_views",
            "date_col": "date",
            "keyword_col": "article_title"
        },
        {
            "name": "Fandom",
            "table_id": "dnd-trends-index.social_data.fandom_trending",
            "date_col": "snapshot_date",
            "keyword_col": "article_title"
        },
        {
            "name": "YouTube",
            "table_id": "dnd-trends-index.social_data.youtube_videos",
            "date_col": "published_at",
            "keyword_col": "video_id"
        },
        {
            "name": "Roll20",
            "table_id": "dnd-trends-index.commercial_data.roll20_rankings",
            "date_col": "snapshot_date",
            "keyword_col": "title"
        },
        {
            "name": "Kickstarter",
            "table_id": "dnd-trends-index.commercial_data.kickstarter_projects",
            "date_col": "discovered_at",
            "keyword_col": "project_id"
        },
        {
            "name": "BackerKit",
            "table_id": "dnd-trends-index.commercial_data.backerkit_projects",
            "date_col": "scraped_at",
            "keyword_col": "title"
        }
    ]
    
    report = []
    
    for stream in streams:
        try:
            table = client.get_table(stream["table_id"])
            
            # Simple metadata count
            row_count = table.num_rows
            
            if row_count > 0:
                # Query for freshness and diversity
                query = f"""
                SELECT 
                    MAX({stream['date_col']}) as latest_date,
                    COUNT(DISTINCT {stream['keyword_col']}) as unique_entities
                FROM `{stream['table_id']}`
                """
                
                # Fetch results
                job = client.query(query, location=table.location)
                res = list(job)[0]
                
                report.append({
                    "Stream": stream["name"],
                    "Status": "Healthy",
                    "Rows": row_count,
                    "Latest Data": str(res.latest_date),
                    "Unique Entities": res.unique_entities
                })
            else:
                report.append({
                    "Stream": stream["name"],
                    "Status": "Warning/Empty",
                    "Rows": 0,
                    "Latest Data": "N/A",
                    "Unique Entities": 0
                })
            
        except Exception as e:
            report.append({
                "Stream": stream["name"],
                "Status": "Error",
                "Error": str(e)
            })

    # Add Article Newsroom Health
    try:
        articles_table = client.get_table("dnd-trends-index.gold_data.daily_articles")
        report.append({
            "Stream": "Daily Articles (AI)",
            "Status": "Healthy" if articles_table.num_rows > 0 else "Empty",
            "Rows": articles_table.num_rows,
            "Latest Data": "Active",
            "Unique Entities": "N/A"
        })
    except:
        pass

    print(json.dumps(report, indent=2))

if __name__ == "__main__":
    audit_all_streams()
