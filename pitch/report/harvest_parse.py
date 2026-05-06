"""Parse Reddit harvest JSON for HotD + ORV due-diligence pass.

Strict filter:
  Post must contain BOTH an IP-specific term AND a D&D-vocabulary term
  in title or body. That filters tangential mentions out — what we want is
  posts genuinely about D&D-context conversion of the IP.
"""
import json, os, glob, re, sys, io
from datetime import datetime

# Force UTF-8 on stdout for Windows
if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

TEMP = r"C:\Users\Yorri\AppData\Local\Temp"

# IP-specific terms — post must contain at least one
IP_TERMS = {
    "hotd": re.compile(
        r"\b(House of the Dragon|HotD|Targaryen|Westeros|Game of Thrones|"
        r"GoT|Daemon|Rhaenyra|Aegon|Visenya|dragonseed|fire and blood|"
        r"red keep|king'?s landing|dragonstone|Iron Throne)\b",
        re.IGNORECASE,
    ),
    "orv": re.compile(
        r"\b(Omniscient Reader|ORV|Kim Dokja|Yoo Joonghyuk|Han Sooyoung|"
        r"Lee Hyunsung|Yoo Sangah|Three Ways to Survive|TWSA|"
        r"singshong|Star Stream|disaster scenarios)\b",
        re.IGNORECASE,
    ),
}

# D&D-vocabulary — post must contain at least one
DND_VOCAB = re.compile(
    r"\b(5e|D&D|DnD|Dungeons.?and.?Dragons|"
    r"5\.5e|fifth edition|tabletop RPG|TTRPG|"
    r"homebrew|subclass|species(?:.?race)?|spell.?slot|"
    r"player.?s.?handbook|player handbook|adventurer.?s.?league|"
    r"\bDM\b|dungeon master|Wizards of the Coast|WoTC|"
    r"\b5e campaign|d&d campaign|dnd campaign|tabletop campaign|"
    r"Critical Role|matt mercer|matthew mercer|"
    r"BG3|baldur.?s.?gate)\b",
    re.IGNORECASE,
)


def is_ip_specific_dnd(post_data, ip_key):
    text = (post_data.get("title", "") + " " + post_data.get("selftext", ""))
    has_ip = bool(IP_TERMS[ip_key].search(text))
    has_dnd = bool(DND_VOCAB.search(text))
    return has_ip and has_dnd, has_ip, has_dnd


def parse_harvest(prefix, ip_key):
    """Parse all JSON files matching the prefix. Return classified hits."""
    seen = set()
    all_records = []
    for path in glob.glob(os.path.join(TEMP, f"harvest_{prefix}_*.json")):
        try:
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
        except (json.JSONDecodeError, FileNotFoundError):
            continue
        children = data.get("data", {}).get("children", [])
        for c in children:
            d = c.get("data", {})
            pid = d.get("id", "")
            if not pid or pid in seen:
                continue
            created = d.get("created_utc", 0)
            if created < 1714521600:
                continue
            both, has_ip, has_dnd = is_ip_specific_dnd(d, ip_key)
            seen.add(pid)
            all_records.append({
                "id": pid,
                "subreddit": d.get("subreddit", ""),
                "title": d.get("title", "")[:200],
                "ups": d.get("ups", 0),
                "comments": d.get("num_comments", 0),
                "created": datetime.fromtimestamp(created).strftime("%Y-%m-%d"),
                "url": "https://www.reddit.com" + d.get("permalink", ""),
                "has_ip_term": has_ip,
                "has_dnd_vocab": has_dnd,
                "is_match": both,
                "selftext_excerpt": d.get("selftext", "")[:400].replace("\n", " "),
            })
    return all_records


def report(label, records, ip_key):
    print("=" * 80)
    print(f"{label} — Reddit harvest (strict IP+D&D filter)")
    print("=" * 80)
    matches = [r for r in records if r["is_match"]]
    has_ip_only = [r for r in records if r["has_ip_term"] and not r["has_dnd_vocab"]]
    has_dnd_only = [r for r in records if r["has_dnd_vocab"] and not r["has_ip_term"]]
    neither = [r for r in records if not r["has_ip_term"] and not r["has_dnd_vocab"]]
    print(f"Total unique posts surfaced by search: {len(records)}")
    print(f"  ✓ Posts with IP term + D&D vocab (real signal): {len(matches)}")
    print(f"    Posts with IP term only (not D&D-context): {len(has_ip_only)}")
    print(f"    Posts with D&D vocab only (not IP-specific): {len(has_dnd_only)}")
    print(f"    Posts with neither (search noise): {len(neither)}")
    print()
    if matches:
        print(f"Real-signal posts ({len(matches)}, sorted by ups):")
        matches.sort(key=lambda r: r["ups"], reverse=True)
        for m in matches[:30]:
            print(f"  [{m['created']}] r/{m['subreddit']} | {m['ups']}u {m['comments']}c | {m['title']}")
            if m["selftext_excerpt"].strip():
                print(f"     body: {m['selftext_excerpt'][:250]}")
            print(f"     URL: {m['url']}")
    else:
        print("→ Zero real-signal posts after strict IP+D&D filter. The search noise was substantial; the actual D&D-context IP conversation is not happening at meaningful volume.")
    print()


def main():
    hotd = parse_harvest("hotd", "hotd")
    report("HOUSE OF THE DRAGON", hotd, "hotd")

    orv = parse_harvest("orv", "orv")
    report("OMNISCIENT READER'S VIEWPOINT", orv, "orv")

    out = os.path.join(os.path.dirname(__file__), "harvest_results.json")
    with open(out, "w", encoding="utf-8") as f:
        json.dump({
            "hotd_total": len(hotd),
            "hotd_real_signal": [r for r in hotd if r["is_match"]],
            "orv_total": len(orv),
            "orv_real_signal": [r for r in orv if r["is_match"]],
        }, f, ensure_ascii=False, indent=2, default=str)
    print(f"Saved structured results to {out}")


if __name__ == "__main__":
    main()
