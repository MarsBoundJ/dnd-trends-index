from google.cloud import bigquery
import json

client = bigquery.Client()

def verify():
    report = {}
    
    # 1. Wiki Gold Count
    wiki_sql = "SELECT count(*) as count FROM `gold_data.view_wikipedia_leaderboards` WHERE average_score > 0"
    try:
        df = client.query(wiki_sql, location="US").to_dataframe()
        report['wiki_gold_count'] = int(df['count'][0])
    except Exception as e:
        report['wiki_gold_count'] = str(e)

    # 2. Archive Check
    archive_sql = "SELECT concept_name FROM `dnd_trends_categorized.concept_library` WHERE concept_name IN ('Me', 'AT') AND is_active = FALSE"
    try:
        df = client.query(archive_sql, location="US").to_dataframe()
        report['archived_check'] = list(df['concept_name'])
    except Exception as e:
        report['archived_check'] = str(e)

    print(json.dumps(report, indent=4))

if __name__ == "__main__":
    verify()
