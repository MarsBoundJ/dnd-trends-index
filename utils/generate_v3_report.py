
import os
from google.cloud import bigquery

# Configuration
PROJECT_ID = "dnd-trends-index"
OUTPUT_FILE = "trends_report_v3.md"

if not os.getenv("GOOGLE_APPLICATION_CREDENTIALS") and os.path.exists("/workspaces/dnd-trends/dnd-key.json"):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/workspaces/dnd-trends/dnd-key.json"

client = bigquery.Client(project=PROJECT_ID)

def generate_report():
    query = """
    WITH ranked_data AS (
        SELECT 
            display_name,
            category,
            score,
            ROW_NUMBER() OVER(PARTITION BY category ORDER BY score DESC) as rank
        FROM `dnd-trends-index.gold_data.view_leaderboard_clean`
        WHERE category NOT IN ('UA', 'Event', 'Archive', 'UA Content', 'Unearthed Arcana') OR category IS NULL
    )
    SELECT * FROM ranked_data WHERE rank <= 40 ORDER BY category, rank;
    """
    print("Fetching data from BigQuery...")
    query_job = client.query(query)
    rows = query_job.result()
    
    # Organize by category
    categories = {}
    for row in rows:
        cat = row.category or "Uncategorized"
        if cat not in categories:
            categories[cat] = []
        categories[cat].append(row)
    
    sorted_cats = sorted(categories.keys())
    
    report_lines = [
        "# Google Trends Deep Dive Report V3",
        "",
        "> [!NOTE]",
        "> This version features expanded leaderboards (Top 40 where available) and refined styling.",
        "",
        "## 1. Dynamic Category Leaderboards",
        ""
    ]
    
    for cat in sorted_cats:
        items = categories[cat]
        report_lines.append(f"### {cat} (Top {len(items)})")
        for i, item in enumerate(items):
            rank = i + 1
            name = item.display_name
            score = float(item.score)
            
            if rank == 1:
                report_lines.append(f"{rank}. **{name}** (Score {score:.2f})")
            else:
                report_lines.append(f"{rank}. {name} {score:.2f}")
        report_lines.append("")
        
    with open(OUTPUT_FILE, "w") as f:
        f.write("\n".join(report_lines))
    print(f"Report generated: {OUTPUT_FILE}")

if __name__ == "__main__":
    generate_report()
