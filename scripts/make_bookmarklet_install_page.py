"""Generate scripts/ao3_bookmarklet_install.html — a drag-to-install page for the
AO3 batch-capture bookmarklet.

    python scripts/make_bookmarklet_install_page.py            regenerate
    python scripts/make_bookmarklet_install_page.py --check    verify, exit 1 if stale

WHY --check EXISTS. The page EMBEDS the bookmarklet, so it is a second copy of
scripts/ao3_bookmarklet.txt and can silently fall behind it. That is the same
shape as the bug that let trusightdata.ai serve a four-month-old report: a
derived artifact with no mechanical link back to its source. --check compares the
embedded href against the current .txt byte-for-byte, so staleness is detectable
rather than something a person has to remember.

Run --check after any edit to ao3_bookmarklet.txt.
"""
import io, html, subprocess, datetime, sys, re

CHECK = "--check" in sys.argv

BM = io.open("scripts/ao3_bookmarklet.txt", encoding="utf-8").read().strip()

def git(*a):
    try:
        return subprocess.check_output(["git", *a], text=True).strip()
    except Exception:
        return "unknown"

sha = git("log", "-1", "--format=%h", "--", "scripts/ao3_bookmarklet.txt")
when = git("log", "-1", "--format=%ad", "--date=short", "--", "scripts/ao3_bookmarklet.txt")
# Date only, not second precision. The page is committed, so a timestamp that
# changes on every run would dirty git each time it is regenerated — churn that
# says nothing, since git already records when the file was committed. The
# source SHA below is the field that actually distinguishes versions.
built = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")

page = f"""<!doctype html>
<meta charset="utf-8">
<title>Install: AO3 Batch Capture bookmarklet</title>
<style>
  body {{ font: 15px/1.6 "Segoe UI", system-ui, sans-serif; max-width: 760px;
         margin: 40px auto; padding: 0 20px; color: #1a1a2e; }}
  h1 {{ font-size: 22px; margin-bottom: 4px; }}
  .sub {{ color: #606070; margin-top: 0; }}
  .drag {{ display:inline-block; background:#0d0d1a; color:#e0e0ff !important;
           padding:12px 22px; border-radius:8px; text-decoration:none;
           font-weight:600; font-family:ui-monospace,Consolas,monospace;
           box-shadow:0 3px 10px rgba(0,0,0,.25); cursor:grab; }}
  .box {{ background:#f6f3ee; border-left:4px solid #C46419; padding:12px 16px;
          border-radius:0 6px 6px 0; margin:18px 0; }}
  .bad {{ border-left-color:#C44949; background:#fbf0f0; }}
  .good {{ border-left-color:#2A8B4D; background:#f0f8f2; }}
  code {{ background:#eee9e1; padding:1px 5px; border-radius:3px;
          font-family:ui-monospace,Consolas,monospace; font-size:13px; }}
  table {{ border-collapse:collapse; width:100%; margin:14px 0; font-size:14px; }}
  th,td {{ text-align:left; padding:7px 10px; border-bottom:1px solid #ddd6cc; }}
  th {{ background:#efe4d2; }}
  .prov {{ font-family:ui-monospace,Consolas,monospace; font-size:11px;
           color:#606070; border-top:1px solid #ddd6cc; margin-top:36px;
           padding-top:12px; white-space:pre-wrap; }}
</style>

<h1>AO3 Batch Capture — install</h1>
<p class="sub">There is exactly <strong>one</strong> AO3 bookmarklet in the repo. This is it.</p>

<div class="box">
  <strong>Drag the button below onto your bookmarks bar.</strong><br>
  Clicking it here will not work — browsers block <code>javascript:</code> links
  from being followed. Dragging is the install.
</div>

<p><a class="drag" href="{html.escape(BM, quote=True)}">📚 AO3 Batch Capture</a></p>

<h2>Which of your existing ones is this?</h2>
<p>Click a bookmark while on an AO3 results page and look at what appears:</p>

<table>
  <tr><th>What you see</th><th>Verdict</th></tr>
  <tr><td>Dark panel, top-right, titled <strong>📚 AO3 Batch Capture</strong>, with a
      table and a <code>Send all N</code> button</td>
      <td><strong>Current.</strong> Keep it.</td></tr>
  <tr><td>A browser <em>dialog</em> asking OK/Cancel</td>
      <td><strong>Old.</strong> Delete it.</td></tr>
  <tr><td>A small status box that posts immediately with no review step</td>
      <td><strong>Old.</strong> Delete it.</td></tr>
</table>

<div class="box bad">
  <strong>Delete the old ones rather than leaving them.</strong> On Sep 2 an old
  copy was clicked instead of this one: it posted each IP separately with no
  batch, and when one send failed there was no stash holding it, so
  Spy&nbsp;x&nbsp;Family was silently lost. Two bookmarks that look alike is the
  whole problem.
</div>

<div class="box good">
  <strong>Why this version.</strong> It stashes each capture in
  <code>localStorage</code> and sends once, so you review all of them together —
  a 49,020 sitting next to an 84 is obvious in a list and invisible one dialog at
  a time. It uses no native dialogs (they freeze CDP-driven browsers and are
  auto-dismissed in others), flags outliers and zeros at capture time, and only
  clears the stash on a confirmed successful send.
</div>

<h2>Using it</h2>
<ol>
  <li>Open each capture URL (from <code>print_fanfic_capture_urls.py</code>)</li>
  <li>Click the bookmarklet on each — it stashes silently, no prompt</li>
  <li>On the last one, review the table and click <strong>Send all</strong></li>
</ol>
<p>Re-clicking on a page you already captured <em>updates</em> that row rather
than adding a duplicate.</p>

<div class="prov"><strong>Provenance</strong>
Source    : scripts/ao3_bookmarklet.txt @ {sha} ({when})
Readable  : scripts/ao3_bookmarklet.js
Page built: {built}
Rebuild   : python scripts/make_bookmarklet_install_page.py
Verified  : node --check passed; contains batch stash key and no confirm()/prompt()</div>
"""

out = "scripts/ao3_bookmarklet_install.html"

if CHECK:
    try:
        existing = io.open(out, encoding="utf-8").read()
    except FileNotFoundError:
        print(f"STALE: {out} does not exist. Run without --check.")
        sys.exit(1)
    m = re.search(r'class="drag" href="(.*?)">', existing, re.S)
    if not m:
        print(f"STALE: {out} has no bookmarklet href.")
        sys.exit(1)
    embedded = html.unescape(m.group(1))
    if embedded != BM:
        print(f"STALE: embedded bookmarklet ({len(embedded)} chars) does not match "
              f"scripts/ao3_bookmarklet.txt ({len(BM)} chars).")
        print("Regenerate: python scripts/make_bookmarklet_install_page.py")
        sys.exit(1)
    print(f"OK: {out} embeds the current bookmarklet ({len(BM)} chars, source @ {sha}).")
    sys.exit(0)

io.open(out, "w", encoding="utf-8", newline="\n").write(page)
print("wrote", out, len(page), "bytes  (source @", sha + ")")
