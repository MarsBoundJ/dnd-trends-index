
import os
import argparse
import pandas as pd
import numpy as np
from google.cloud import bigquery
from datetime import datetime, timedelta

# Configuration
PROJECT_ID = "dnd-trends-index"
DATASET_ID = "dnd_trends_categorized"
TREND_TABLE = f"{PROJECT_ID}.{DATASET_ID}.trend_data_pilot"
TERMS_TABLE = f"{PROJECT_ID}.{DATASET_ID}.expanded_search_terms"
FANDOM_TABLE = f"{PROJECT_ID}.social_data.fandom_trending"
WIKI_TABLE = f"{PROJECT_ID}.silver_data.norm_wikipedia" # Changed to norm_wikipedia as registry might be empty
OUTPUT_FILE = "trends_deep_dive_report.md"

# Auto-load creds if local
CRED_PATH = "/app/dnd-key.json"
if os.path.exists(CRED_PATH):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CRED_PATH

def get_client():
    if os.path.exists(CRED_PATH):
        return bigquery.Client.from_service_account_json(CRED_PATH, project=PROJECT_ID)
    return bigquery.Client(project=PROJECT_ID)

def get_category_counts(client, test_limit=None):
    """
    Returns a DataFrame with category counts.
    """
    limit_clause = f"LIMIT {test_limit}" if test_limit else ""
    query = f"""
        SELECT category, COUNT(*) as count 
        FROM `{TERMS_TABLE}`
        GROUP BY category
        ORDER BY count DESC
        {limit_clause}
    """
    return client.query(query).to_dataframe()

def get_trend_stats(client, test_limit=None):
    """
    Aggregates trend data: Avg Interest, Std Dev, Seasonality
    """
    limit_clause = f"LIMIT {test_limit}" if test_limit else ""
    
    # We need to join with expanded_search_terms to get Category
    # Note: trend_data_pilot has search_term, terms table has search_term.
    
    query = f"""
        WITH stats AS (
            SELECT 
                t.search_term,
                e.category,
                AVG(t.interest) as avg_interest,
                STDDEV(t.interest) as std_dev_interest,
                COUNT(*) as data_points
            FROM `{TREND_TABLE}` t
            JOIN `{TERMS_TABLE}` e ON t.search_term = e.search_term
            GROUP BY 1, 2
        )
        SELECT * FROM stats
        ORDER BY avg_interest DESC
        {limit_clause}
    """
    return client.query(query).to_dataframe()

def get_seasonality(client):
    """
    Returns average interest by Day of Week
    """
    query = f"""
        SELECT 
            EXTRACT(DAYOFWEEK FROM date) as day_of_week_index,
            FORMAT_DATE('%A', date) as day_name,
            AVG(interest) as avg_interest
        FROM `{TREND_TABLE}`
        GROUP BY 1, 2
        ORDER BY 1
    """
    return client.query(query).to_dataframe()

def get_momentum(client):
    """
    Calculates 30-day Momentum (Current 30d Avg / Previous 30d Avg)
    """
    # Simple approximation: Compare last 30 days vs 30-60 days ago
    
    query = f"""
        WITH ranges AS (
            SELECT 
                search_term,
                date,
                interest,
                CASE 
                    WHEN date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) THEN 'current_30'
                    WHEN date >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY) AND date < DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) THEN 'prev_30'
                    ELSE 'older'
                END as period
            FROM `{TREND_TABLE}`
        ),
        aggregated AS (
            SELECT 
                search_term,
                period,
                AVG(interest) as avg_score
            FROM ranges
            WHERE period IN ('current_30', 'prev_30')
            GROUP BY 1, 2
        )
        SELECT 
            curr.search_term,
            curr.avg_score as score_current,
            prev.avg_score as score_prev,
            SAFE_DIVIDE(curr.avg_score - prev.avg_score, prev.avg_score) as growth_pct
        FROM aggregated curr
        JOIN aggregated prev ON curr.search_term = prev.search_term AND prev.period = 'prev_30'
        WHERE curr.period = 'current_30'
        ORDER BY growth_pct DESC
    """
    return client.query(query).to_dataframe()

def check_saturation(client, top_terms):
    """
    Checks if terms exist in Fandom or Wiki.
    top_terms is a list of search_terms.
    """
    # We'll just fetch all tracked articles for simplicity in Pilot
    fandom_q = f"SELECT DISTINCT article_title FROM `{FANDOM_TABLE}`"
    wiki_q = f"SELECT DISTINCT keyword as article_title FROM `{WIKI_TABLE}`"
    
    try:
        fandom_df = client.query(fandom_q).to_dataframe()
        wiki_df = client.query(wiki_q).to_dataframe()
        
        fandom_set = set(fandom_df['article_title'].str.lower())
        wiki_set = set(wiki_df['article_title'].str.lower())
        
        return fandom_set, wiki_set
    except Exception as e:
        print(f"Warning: Could not fetch saturation data: {e}")
        return set(), set()

def generate_report(category_counts, trend_stats, seasonality, momentum, saturation_sets):
    fandom_set, wiki_set = saturation_sets
    
    lines = []
    lines.append("# Deep Dive Trend Report")
    lines.append(f"Generated at: {datetime.now().isoformat()}")
    lines.append("")
    
    # 1. Dynamic Leaderboards
    lines.append("## 1. Dynamic Category Leaderboards")
    lines.append("Rule: Top 40 for Categories > 200 items, Top 20 for others.")
    
    # Join trend stats with category counts if not already
    # trend_stats has category
    
    processed_categories = set()
    
    # Group by category
    for cat in category_counts['category']:
        cat_count = category_counts[category_counts['category'] == cat]['count'].iloc[0]
        top_n = 40 if cat_count > 200 else 20
        
        subset = trend_stats[trend_stats['category'] == cat].sort_values(by='avg_interest', ascending=False).head(top_n)
        
        if subset.empty:
            continue
            
        lines.append(f"### {cat} (Total Items: {cat_count}) - Top {top_n}")
        for i, row in subset.iterrows():
            term = row['search_term']
            lines.append(f"{i+1}. **{term}** (Avg: {row['avg_interest']:.1f}, Std: {row['std_dev_interest']:.1f})")
        lines.append("")
        
    # 2. Volatility Analysis
    lines.append("## 2. Volatility Analysis")
    lines.append("| Term | Category | Volatility (StdDev) | Avg Interest | Type |")
    lines.append("|---|---|---|---|---|")
    
    # Classify
    # Use thresholds? 
    # Let's just list Top 10 Most Volatile (High StdDev) and Top 10 Most Stable (High Avg, Low StdDev)
    
    sorted_vol = trend_stats.sort_values(by='std_dev_interest', ascending=False).head(10)
    for i, row in sorted_vol.iterrows():
        lines.append(f"| {row['search_term']} | {row['category']} | {row['std_dev_interest']:.1f} | {row['avg_interest']:.1f} | Spikey/Volatile |")
        
    lines.append("")
    lines.append("**Top Stable Staples (High Interest, Low Vol)**")
    # Filter for high interest first (e.g. top 25%)
    high_interest_threshold = trend_stats['avg_interest'].quantile(0.75)
    stable_candidates = trend_stats[trend_stats['avg_interest'] >= high_interest_threshold].sort_values(by='std_dev_interest', ascending=True).head(10)
    
    for i, row in stable_candidates.iterrows():
        lines.append(f"| {row['search_term']} | {row['category']} | {row['std_dev_interest']:.1f} | {row['avg_interest']:.1f} | Stable |")
    
    lines.append("")
        
    # 3. Momentum
    lines.append("## 3. Momentum (30d Growth)")
    lines.append("| Term | Growth % | Current Avg | Prev Avg |")
    lines.append("|---|---|---|---|")
    
    for i, row in momentum.head(20).iterrows():
        lines.append(f"| {row['search_term']} | {row['growth_pct']:.1%} | {row['score_current']:.1f} | {row['score_prev']:.1f} |")
        
    lines.append("")
    
    # 4. Opportunity Matrix
    lines.append("## 4. Blue Ocean Opportunities (High Demand, Low Supply)")
    lines.append("Terms with High Interest but missing/low saturation in Fandom/Wiki.")
    
    # Filter trend_stats for high interest
    high_demand = trend_stats[trend_stats['avg_interest'] > 10] # Arbitrary threshold for pilot
    
    opportunities = []
    for i, row in high_demand.iterrows():
        term_clean = row['search_term'].lower()
        in_fandom = term_clean in fandom_set
        in_wiki = term_clean in wiki_set
        
        if not in_fandom and not in_wiki:
            opportunities.append(row)
            
    # Show top opportunities
    op_df = pd.DataFrame(opportunities)
    if not op_df.empty:
        op_df = op_df.sort_values(by='avg_interest', ascending=False).head(20)
        for i, row in op_df.iterrows():
            lines.append(f"* **{row['search_term']}** ({row['category']}) - Interest: {row['avg_interest']:.1f}")
    else:
        lines.append("No clear Blue Ocean opportunities found in this batch.")
        
    lines.append("")
            
    # 5. Seasonality
    lines.append("## 5. Seasonality (DM Prep Day)")
    lines.append("| Day | Avg Interest |")
    lines.append("|---|---|")
    for i, row in seasonality.iterrows():
        lines.append(f"| {row['day_name']} | {row['avg_interest']:.1f} |")
        
    return "\n".join(lines)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--test", action="store_true", help="Run in test mode with limited data")
    args = parser.parse_args()
    
    client = get_client()
    
    test_limit = 100 if args.test else None
    
    print("Fetching Category Counts...")
    cat_counts = get_category_counts(client, test_limit)
    
    print("Fetching Trend Stats...")
    trend_stats = get_trend_stats(client, test_limit)
    
    print("Fetching Seasonality...")
    seasonality = get_seasonality(client)
    
    print("Fetching Momentum...")
    momentum = get_momentum(client)
    
    print("Fetching Saturation Data...")
    # Get all terms from trend_stats for checking
    saturation = check_saturation(client, trend_stats['search_term'].tolist())
    
    print("Generating Report...")
    report = generate_report(cat_counts, trend_stats, seasonality, momentum, saturation)
    
    with open(OUTPUT_FILE, "w") as f:
        f.write(report)
        
    print(f"Report saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
