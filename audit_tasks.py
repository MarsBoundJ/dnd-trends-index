from google.cloud import bigquery
import json

client = bigquery.Client(project='dnd-trends-index')

def hunt_fandom_ghosts():
    query = """
    SELECT article_title, MAX(hype_score) as max_score
    FROM `dnd-trends-index.dnd_trends_raw.fandom_daily_metrics`
    WHERE article_title NOT IN (SELECT concept_name FROM `dnd-trends-index.dnd_trends_categorized.concept_library`)
    GROUP BY article_title
    ORDER BY max_score DESC
    LIMIT 50
    """
    results = [dict(row) for row in client.query(query)]
    with open('fandom_ghosts.json', 'w') as f:
        json.dump(results, f, indent=2)
    print(f"Found {len(results)} Fandom ghosts.")

def hunt_anomalies():
    q1 = """
    SELECT concept_name, category FROM `dnd-trends-index.dnd_trends_categorized.concept_library` 
    WHERE category = 'Monster' AND (LOWER(concept_name) LIKE '%spell%' OR LOWER(concept_name) LIKE '%scroll%')
    """
    q2 = """
    SELECT concept_name, category FROM `dnd-trends-index.dnd_trends_categorized.concept_library` 
    WHERE category = 'Spell' AND (LOWER(concept_name) LIKE '%dragon%' OR LOWER(concept_name) LIKE '%giant%')
    """
    r1 = [dict(row) for row in client.query(q1)]
    r2 = [dict(row) for row in client.query(q2)]
    
    with open('anomalies.json', 'w') as f:
        json.dump({"monsters_with_spell": r1, "spells_with_dragon": r2}, f, indent=2)
    print(f"Found {len(r1) + len(r2)} anomalies.")

if __name__ == '__main__':
    hunt_fandom_ghosts()
    hunt_anomalies()
