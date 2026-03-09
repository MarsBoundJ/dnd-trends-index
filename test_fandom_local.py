from fandom_scraper import fetch_wiki_trends
import json

def test_fetch():
    target_wiki = 'darksun'
    print(f"🧪 Testing fetch for '{target_wiki}'...")
    
    rows = fetch_wiki_trends(target_wiki)
    
    if not rows:
        print("❌ FAILED: No rows returned. Check network or API response.")
        return
    
    print(f"✅ SUCCESS: Retrieved {len(rows)} rows.")
    print("📋 Sample Row Data:")
    print(json.dumps(rows[0], indent=2))
    
    # Validation
    first_row = rows[0]
    expected_keys = ["extraction_date", "wiki_slug", "article_title", "rank_position", "hype_score", "view_count", "url_path"]
    missing = [k for k in expected_keys if k not in first_row]
    
    if missing:
        print(f"⚠️ WARNING: Missing keys in row: {missing}")
    else:
        print("✅ Schema Validation: Passed")

if __name__ == "__main__":
    test_fetch()
