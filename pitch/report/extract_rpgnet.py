"""Extract RPG.net thread posts about UA Villainous Options (and 2)."""
import os, re, html, json
from html.parser import HTMLParser

PAGES = [
    r"C:\Users\Yorri\AppData\Local\Temp\rpgnet_ua.html",
    r"C:\Users\Yorri\AppData\Local\Temp\rpgnet_ua_p2.html",
]

# Each post is an <article class="message ..."> with data-author attribute.
# The post body is in <div class="bbWrapper"> inside.
POST_HEADER = re.compile(
    r'<article class="message[^"]*message--post[^"]*"\s+data-author="([^"]+)"[^>]*?id="(js-post-\d+)"',
    re.DOTALL,
)
DATETIME = re.compile(r'datetime="([^"]+)"')
BBWRAPPER = re.compile(r'<div class="bbWrapper">(.*?)<div class="js-selectToQuoteEnd">', re.DOTALL)

class TagStripper(HTMLParser):
    def __init__(self):
        super().__init__()
        self.parts = []
        self.skip_depth = 0
    def handle_starttag(self, tag, attrs):
        attrd = dict(attrs)
        cls = attrd.get("class", "")
        if tag == "blockquote" or "bbCodeBlock" in cls:
            self.skip_depth += 1
            return
        if self.skip_depth > 0:
            return
        if tag in ("br", "p", "li"):
            self.parts.append("\n")
    def handle_endtag(self, tag):
        if tag == "blockquote" and self.skip_depth > 0:
            self.skip_depth -= 1
            return
        if self.skip_depth > 0:
            return
        if tag in ("p", "li"):
            self.parts.append("\n")
    def handle_data(self, data):
        if self.skip_depth > 0:
            return
        self.parts.append(data)
    def get_text(self):
        return "".join(self.parts).strip()

def strip_html(s):
    s = re.sub(r"<blockquote[^>]*>.+?</blockquote>", "", s, flags=re.DOTALL)
    p = TagStripper()
    p.feed(s)
    txt = html.unescape(p.get_text())
    txt = re.sub(r"\n+", "\n", txt)
    txt = re.sub(r"[ \t]+", " ", txt)
    return txt.strip()

def split_posts(html_content):
    """Find each <article ...message--post... data-author=...> and the body inside."""
    out = []
    headers = list(POST_HEADER.finditer(html_content))
    for i, h in enumerate(headers):
        author = h.group(1)
        post_id = h.group(2)
        start = h.start()
        end = headers[i + 1].start() if i + 1 < len(headers) else len(html_content)
        chunk = html_content[start:end]
        date_m = DATETIME.search(chunk)
        date = date_m.group(1) if date_m else ""
        body_m = BBWRAPPER.search(chunk)
        body = strip_html(body_m.group(1)) if body_m else ""
        out.append({
            "author": author,
            "post_id": post_id,
            "date": date,
            "body": body,
            "body_chars": len(body),
        })
    return out


all_posts = []
for path in PAGES:
    if not os.path.exists(path):
        print(f"MISSING: {path}")
        continue
    with open(path, encoding="utf-8") as f:
        html_content = f.read()
    posts = split_posts(html_content)
    print(f"{os.path.basename(path)}: {len(posts)} posts extracted")
    all_posts.extend(posts)

print(f"\nTotal posts on thread: {len(all_posts)}")

# Filter to VO2 window
vo2_posts = []
for p in all_posts:
    is_after_vo2 = p["date"] >= "2026-04-23"
    body_lower = p["body"].lower()
    mentions_vo2 = any(kw in body_lower for kw in [
        "lament", "venom", "primordial", "banshee", "envenom", "elemental node",
        "villainous options 2", "vo2", "vo 2",
    ])
    if is_after_vo2 or mentions_vo2:
        p["is_after_vo2"] = is_after_vo2
        p["mentions_vo2_specific"] = mentions_vo2
        vo2_posts.append(p)

print(f"\nPosts in VO2 window: {len(vo2_posts)}")
print(f"  After Apr 23: {sum(1 for p in vo2_posts if p['is_after_vo2'])}")
print(f"  VO2-keyword mentions: {sum(1 for p in vo2_posts if p['mentions_vo2_specific'])}")
print()

for p in vo2_posts:
    if p["is_after_vo2"]:
        body_preview = p["body"][:400].replace("\n", " | ")
        print(f"  [{p['date'][:10]}] {p['author']:20s} ({p['body_chars']}c)")
        print(f"    {body_preview}")
        print()

OUT = os.path.join(os.path.dirname(__file__), "rpgnet_vo2_posts.json")
with open(OUT, "w", encoding="utf-8") as f:
    json.dump(vo2_posts, f, ensure_ascii=False, indent=2)
print(f"Wrote {OUT}")
