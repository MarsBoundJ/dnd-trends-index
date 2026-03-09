
import os
from google.cloud import bigquery

# Configuration
PROJECT_ID = "dnd-trends-index"
OUTPUT_FILE = "TRENDS_REPORT_AUDIT_FINAL.md"

if not os.getenv("GOOGLE_APPLICATION_CREDENTIALS") and os.path.exists("/workspaces/dnd-trends/dnd-key.json"):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/workspaces/dnd-trends/dnd-key.json"

client = bigquery.Client(project=PROJECT_ID)

def generate_report():
    # 1. First, get the counts per category to determine Top N
    count_query = """
    SELECT category, COUNT(*) as total_count
    FROM `dnd-trends-index.gold_data.view_leaderboard_clean`
    WHERE category != 'Archive' OR category IS NULL
    GROUP BY 1
    """
    print("Calculating category sizes...")
    count_job = client.query(count_query)
    category_meta = {row.category or "Uncategorized": row.total_count for row in count_job.result()}

    # 2. Get the ranked data
    data_query = """
    SELECT 
        display_name,
        category,
        score,
        ROW_NUMBER() OVER(PARTITION BY category ORDER BY score DESC) as rank
    FROM `dnd-trends-index.gold_data.view_leaderboard_clean`
    WHERE category != 'Archive' OR category IS NULL
    ORDER BY category, rank
    """
    print("Fetching leaderboard data...")
    query_job = client.query(data_query)
    rows = query_job.result()
    
    # Organize by category
    categories = {}
    for row in rows:
        cat = row.category or "Uncategorized"
        if cat not in categories:
            categories[cat] = []
        
        # Apply the dynamic Limit: Top 40 if total > 200, else Top 20
        total = category_meta.get(cat, 0)
        limit = 40 if total > 200 else 20
        
        if row.rank <= limit:
            categories[cat].append(row)
    
    sorted_cats = sorted(categories.keys())
    
    report_lines = [
        "# Google Trends Deep Dive - Full Audit Report",
        "",
        "> [!IMPORTANT]",
        "> This report includes **ALL** categories found in the database for auditing purposes.",
        "> **Rules**: Top 40 for categories > 200 items, Top 20 for others. Max-Signal Aggregation on Averages.",
        "",
        "## 1. Dynamic Category Leaderboards",
        ""
    ]
    
    for cat in sorted_cats:
        items = categories[cat]
        total = category_meta.get(cat, 0)
        limit_used = 40 if total > 200 else 20
        
        report_lines.append(f"### {cat} (Total Items: {total}, Showing Top {len(items)})")
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
    print(f"Audit Report generated: {OUTPUT_FILE}")

if __name__ == "__main__":
    generate_report()
