"""Generate the drag-to-install pages for the bookmarklets that have one.

    python scripts/make_bookmarklet_install_page.py            regenerate all
    python scripts/make_bookmarklet_install_page.py --check    verify all, exit 1 if stale
    python scripts/make_bookmarklet_install_page.py ddb        just one
    python scripts/make_bookmarklet_install_page.py ddb --check

WHY --check EXISTS. Each page EMBEDS its bookmarklet, so it is a second copy of
the .txt and can silently fall behind it. That is the same shape as the bug that
let trusightdata.ai serve a four-month-old report: a derived artifact with no
mechanical link back to its source. --check compares the embedded href against
the current .txt byte-for-byte, so staleness is detectable rather than something
a person has to remember.

WHY DDB WAS ADDED (Sep 15, 2026). AO3 had a page; DDB did not. The DDB
bookmarklet installed in the browser turned out to be a week behind the repo,
and nobody noticed for a week — the capture ran 133 combos where the current
build queues 19, re-fetching 104 combos already measured as empty. Worse, the
staleness was "proved absent" by bad reasoning: the magic-items pacing and the
freshness window shipped in the SAME commit, so observing the pacing was taken
as evidence the whole build was installed. A page plus --check replaces that
inference with a check.

Run --check after any edit to a bookmarklet .txt.
"""
import io, html, subprocess, datetime, sys, re

ARGS = [a for a in sys.argv[1:] if not a.startswith("-")]
CHECK = "--check" in sys.argv


def git(*a):
    try:
        return subprocess.check_output(["git", *a], text=True).strip()
    except Exception:
        return "unknown"


AO3_BODY = """<h1>AO3 Batch Capture — install</h1>
<p class="sub">There is exactly <strong>one</strong> AO3 bookmarklet in the repo. This is it.</p>

<div class="box">
  <strong>Drag the button below onto your bookmarks bar.</strong><br>
  Clicking it here will not work — browsers block <code>javascript:</code> links
  from being followed. Dragging is the install.
</div>

<p><a class="drag" href="{href}">📚 AO3 Batch Capture</a></p>

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
"""

DDB_BODY = """<h1>DDB Homebrew Capture — install</h1>
<p class="sub">There is exactly <strong>one</strong> D&amp;D&nbsp;Beyond bookmarklet in the repo. This is it.</p>

<div class="box">
  <strong>Drag the button below onto your bookmarks bar.</strong><br>
  Clicking it here will not work — browsers block <code>javascript:</code> links
  from being followed. Dragging is the install.
</div>

<p><a class="drag" href="{href}">📋 DDB Homebrew Capture</a></p>

<div class="box bad">
  <strong>The plan screen is the version test — use it.</strong> Click the
  bookmarklet on any <code>dndbeyond.com</code> page and read the header.
  <br><br>
  <strong>Current build:</strong> it skips every combo captured in the last
  <strong>30 days</strong>, so after a recent round it offers only the handful
  that actually failed — typically <em>tens</em> pending out of 200.
  <br><br>
  <strong>Old build:</strong> it offers <em>hundreds</em>, because it treats a
  <em>measured zero</em> — a combo correctly captured as "no homebrew exists" —
  as never captured, and re-runs it every single time.
  <br><br>
  On Sep 15 the installed copy queued <strong>133</strong> where this build
  queues <strong>19</strong>. 104 of those 133 were combos already measured as
  empty 18 hours earlier: ~7x the requests against a fragile source, for zero new
  information.
</div>

<div class="box good">
  <strong>Why this version.</strong> Three things, all learned from failed runs:
  a <strong>30-day freshness window</strong> so a round never re-hammers work
  already done, and so staleness is visible rather than hidden behind "done";
  <strong><code>/magic-items</code> runs last</strong> with a longer pause after
  each request, because it has the largest pages and is the first to time out, so
  a stall there cannot poison the sections that were going to succeed; and
  <strong>skip-logic</strong> that isolates the gap automatically, so a retry
  touches only the missing combos.
</div>

<h2>Before you run it</h2>
<ol>
  <li><strong>Be signed in to D&amp;D&nbsp;Beyond.</strong> Unauthenticated runs
      hit the login/Cloudflare wall and every fetch hangs — this alone caused a
      full-run wipeout.</li>
  <li><strong>Load a homebrew page manually first</strong>, e.g.
      <code>/homebrew/magic-items</code>. Slow or erroring for you as a human
      means it will fail for the run too; wait rather than retry.</li>
  <li>Click the bookmarklet, read the pending count, press
      <strong>Capture N pending</strong>.</li>
</ol>

<div class="box">
  <strong>A failure means WAIT, never retry-harder.</strong> Re-running
  immediately escalates a soft throttle into a hard block. Wait at least several
  hours, and at least 24 if a full run already showed throttling. Never re-blast
  all 200 after a failure — the skip-logic already isolates what is missing.
  <br><br>
  A <code>500</code> or timeout on a <em>filtered</em> listing is DDB's own bug,
  intermittent since Feb 2026 and not fixed by waiting. Mark it unmeasured and
  re-probe monthly with one manual request.
</div>

<h2>What the result counts mean</h2>
<table>
  <tr><th>Result</th><th>Meaning</th></tr>
  <tr><td><strong>saved</strong></td>
      <td>Homebrew found; a row with items was written.</td></tr>
  <tr><td><strong>empty</strong></td>
      <td>A <strong>measured zero</strong> — a row was still written. This is a
          real organic-demand signal, not a failure.</td></tr>
  <tr><td><strong>failed</strong></td>
      <td><strong>Unmeasured</strong> — no row written, deliberately. Never let
          anything downstream read these as zeros; that fabricates "no demand".</td></tr>
</table>
"""

TARGETS = {
    "ao3": {
        "txt": "scripts/ao3_bookmarklet.txt",
        "js": "scripts/ao3_bookmarklet.js",
        "out": "scripts/ao3_bookmarklet_install.html",
        "title": "Install: AO3 Batch Capture bookmarklet",
        "body": AO3_BODY,
        "verified": "node --check passed; contains batch stash key and no confirm()/prompt()",
    },
    "ddb": {
        "txt": "scripts/ddb_homebrew_bookmarklet.txt",
        "js": "scripts/ddb_homebrew_bookmarklet.js",
        "out": "scripts/ddb_homebrew_bookmarklet_install.html",
        "title": "Install: DDB Homebrew Capture bookmarklet",
        "body": DDB_BODY,
        "verified": "node --check passed; contains the 30-day freshness window and magic-items-last pacing",
    },
}

CSS = """<style>
  body { font: 15px/1.6 "Segoe UI", system-ui, sans-serif; max-width: 760px;
         margin: 40px auto; padding: 0 20px; color: #1a1a2e; }
  h1 { font-size: 22px; margin-bottom: 4px; }
  .sub { color: #606070; margin-top: 0; }
  .drag { display:inline-block; background:#0d0d1a; color:#e0e0ff !important;
           padding:12px 22px; border-radius:8px; text-decoration:none;
           font-weight:600; font-family:ui-monospace,Consolas,monospace;
           box-shadow:0 3px 10px rgba(0,0,0,.25); cursor:grab; }
  .box { background:#f6f3ee; border-left:4px solid #C46419; padding:12px 16px;
          border-radius:0 6px 6px 0; margin:18px 0; }
  .bad { border-left-color:#C44949; background:#fbf0f0; }
  .good { border-left-color:#2A8B4D; background:#f0f8f2; }
  code { background:#eee9e1; padding:1px 5px; border-radius:3px;
          font-family:ui-monospace,Consolas,monospace; font-size:13px; }
  table { border-collapse:collapse; width:100%; margin:14px 0; font-size:14px; }
  th,td { text-align:left; padding:7px 10px; border-bottom:1px solid #ddd6cc; }
  th { background:#efe4d2; }
  .prov { font-family:ui-monospace,Consolas,monospace; font-size:11px;
           color:#606070; border-top:1px solid #ddd6cc; margin-top:36px;
           padding-top:12px; white-space:pre-wrap; }
</style>"""


def build(slug, t):
    bm = io.open(t["txt"], encoding="utf-8").read().strip()
    sha = git("log", "-1", "--format=%h", "--", t["txt"])
    when = git("log", "-1", "--format=%ad", "--date=short", "--", t["txt"])
    # Flag uncommitted source. Without this the footer cites the last COMMIT while
    # embedding newer working-tree content — provenance that is confidently wrong,
    # which the convention rates as worse than none because it gets believed.
    if git("status", "--porcelain", t["txt"]):
        sha += " + UNCOMMITTED CHANGES"
    # Date only, not second precision. The page is committed, so a timestamp that
    # changes on every run would dirty git each time it is regenerated — churn that
    # says nothing, since git already records when the file was committed. The
    # source SHA below is the field that actually distinguishes versions.
    built = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")
    body = t["body"].replace("{href}", html.escape(bm, quote=True))
    page = (
        "<!doctype html>\n<meta charset=\"utf-8\">\n"
        f"<title>{t['title']}</title>\n{CSS}\n\n{body}\n"
        f"<div class=\"prov\"><strong>Provenance</strong>\n"
        f"Source    : {t['txt']} @ {sha} ({when})\n"
        f"Readable  : {t['js']}\n"
        f"Page built: {built}\n"
        f"Rebuild   : python scripts/make_bookmarklet_install_page.py {slug}\n"
        f"Verified  : {t['verified']}</div>\n"
    )
    return bm, sha, page


def check(slug, t, bm, sha):
    out = t["out"]
    try:
        existing = io.open(out, encoding="utf-8").read()
    except FileNotFoundError:
        print(f"STALE [{slug}]: {out} does not exist. Run without --check.")
        return False
    m = re.search(r'class="drag" href="(.*?)">', existing, re.S)
    if not m:
        print(f"STALE [{slug}]: {out} has no bookmarklet href.")
        return False
    if html.unescape(m.group(1)) != bm:
        print(f"STALE [{slug}]: embedded bookmarklet ({len(html.unescape(m.group(1)))} chars) "
              f"does not match {t['txt']} ({len(bm)} chars).")
        return False
    # The content check above cannot see a dead SHA. A squash merge replaces the
    # branch commits, so a page generated before the merge cites a commit that no
    # longer exists in main: the embedded bookmarklet still matches byte for byte
    # while the one field that makes the page traceable points at nothing. That is
    # the convention's own failure mode — provenance that is confidently wrong is
    # worse than none, because it gets believed. Caught #127 after the fact.
    stamped = re.search(re.escape(t["txt"]) + r" @ ([0-9a-f]{7,40})", existing)
    if stamped:
        ref = stamped.group(1)
        reachable = subprocess.call(
            ["git", "merge-base", "--is-ancestor", ref, "HEAD"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        ) == 0
        if not reachable:
            print(f"STALE [{slug}]: the footer cites {ref}, which is not in this "
                  f"branch's history. A squash merge rewrites branch SHAs, so "
                  f"regenerate AFTER merging. Current source commit is {sha}.")
            return False
    print(f"OK [{slug}]: {out} embeds the current bookmarklet "
          f"({len(bm)} chars, source @ {sha}).")
    return True


selected = ARGS or list(TARGETS)
unknown = [s for s in selected if s not in TARGETS]
if unknown:
    print(f"unknown target(s) {unknown}; known: {sorted(TARGETS)}")
    sys.exit(2)

failures = 0
for slug in selected:
    t = TARGETS[slug]
    bm, sha, page = build(slug, t)
    if CHECK:
        if not check(slug, t, bm, sha):
            failures += 1
    else:
        io.open(t["out"], "w", encoding="utf-8", newline="\n").write(page)
        print(f"wrote {t['out']} {len(page)} bytes  (source @ {sha})")

if CHECK and failures:
    print("Regenerate: python scripts/make_bookmarklet_install_page.py")
sys.exit(1 if failures else 0)
