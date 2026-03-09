import json
import os

def format_report():
    input_file = "bq_output_v3.json"
    if not os.path.exists(input_file):
        print(f"Error: {input_file} not found.")
        return

    with open(input_file, "r") as f:
        data = json.load(f)
    
    # Organize by category
    categories = {}
    for row in data:
        cat = row.get('category') or "Uncategorized"
        if cat not in categories:
            categories[cat] = []
        categories[cat].append(row)
    
    # Custom category order (optional, but let's stick to alphabet or appearance)
    sorted_cats = sorted(categories.keys())

    report_lines = [
        "# Google Trends Deep Dive Report V3",
        "",
        "> [!NOTE]",
        "> This version features expanded leaderboards (Top 40 where available) and refined styling for better readability.",
        "",
        "## 1. Dynamic Category Leaderboards",
        ""
    ]
    
    for cat in sorted_cats:
        items = categories[cat]
        report_lines.append(f"### {cat} (Top {len(items)})")
        for item in items:
            rank = item['rank']
            name = item['display_name']
            score = float(item['score'])
            
            if int(rank) == 1:
                report_lines.append(f"{rank}. **{name}** (Score {score:.2f})")
            else:
                report_lines.append(f"{rank}. {name} {score:.2f}")
        report_lines.append("")
        
    print("\n".join(report_lines))

if __name__ == "__main__":
    format_report()
