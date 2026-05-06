"""Extract OP + top comments from Reddit thread JSON for the live A1 audit."""
import json, os, glob, re

TEMP = r"C:\Users\Yorri\AppData\Local\Temp"
OUT = os.path.join(os.path.dirname(__file__), "ua_audit_comments.json")

THREADS = {
    "onednd_main": ("r/onednd", "Villainous Options 2 UA", "2026-04-23"),
    "dndnext_main": ("r/dndnext", "Unearthed Arcana: Villainous Options 2", "2026-04-23"),
    "lament_rate": ("r/onednd", "How are you going to rate... [Path of Lament]", "2026-04-27"),
    "venom_rate": ("r/onednd", "How are you going to rate... [Warrior of Venom]", "2026-04-28"),
    "primordial_rate": ("r/onednd", "How are you going to rate... [Primordial Patron]", "2026-04-29"),
    "two_cents": ("r/onednd", "Two Cents on the Villainous UA Part 2", "2026-04-25"),
    "bland_opinion": ("r/onednd", "Opinions on Villainous Options 2 - bland", "2026-04-23"),
    "bag_of_rats": ("r/dndnext", "Bag of Rats 2 - Homeopathic poison balance critique", "2026-04-23"),
    "venom_typo": ("r/onednd", "Venom Monk Sleeper Feature or Typo (?)", "2026-04-23"),
}

def walk_comments(node, depth=0, out=None):
    if out is None:
        out = []
    if depth > 2:
        return out
    if not isinstance(node, dict):
        return out
    kind = node.get("kind")
    if kind == "Listing":
        for c in node.get("data", {}).get("children", []):
            walk_comments(c, depth, out)
    elif kind == "t1":  # comment
        d = node.get("data", {})
        body = d.get("body", "")
        if body and body != "[deleted]" and body != "[removed]":
            out.append({
                "author": d.get("author", "?"),
                "body": body,
                "ups": d.get("ups", 0),
                "score": d.get("score", 0),
                "depth": depth,
            })
        replies = d.get("replies")
        if replies:
            walk_comments(replies, depth + 1, out)
    return out


extracted = {}
for key, (sub, title, date) in THREADS.items():
    path = os.path.join(TEMP, f"thread_{key}.json")
    if not os.path.exists(path):
        print(f"MISSING: {path}")
        continue
    with open(path, encoding="utf-8") as f:
        payload = json.load(f)
    # payload[0] is the OP listing, payload[1] is the comments listing
    op_data = payload[0]["data"]["children"][0]["data"]
    op_body = op_data.get("selftext", "")
    op_ups = op_data.get("ups", 0)
    op_comments = op_data.get("num_comments", 0)
    op_url = "https://www.reddit.com" + op_data.get("permalink", "")
    comments = walk_comments(payload[1])
    # Sort by score, take top 30
    comments.sort(key=lambda c: c["score"], reverse=True)
    extracted[key] = {
        "subreddit": sub,
        "title": title,
        "date": date,
        "url": op_url,
        "op_ups": op_ups,
        "op_num_comments": op_comments,
        "op_body": op_body,
        "top_comments": comments[:30],
        "total_extracted": len(comments),
    }
    print(f"{key:20s} | {sub:14s} | {op_ups:4d}u {op_comments:4d}c | {len(comments):3d} extracted")

with open(OUT, "w", encoding="utf-8") as f:
    json.dump(extracted, f, ensure_ascii=False, indent=2)
print(f"\nWrote {OUT}")
print(f"Total threads: {len(extracted)}")
print(f"Total comments extracted: {sum(t['total_extracted'] for t in extracted.values())}")
