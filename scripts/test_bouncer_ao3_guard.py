"""Tests for the Bouncer's AO3 ingest guard rails.

    python scripts/test_bouncer_ao3_guard.py

The function under test is extracted from bouncer/main.py rather than imported,
because importing that module pulls in functions_framework and vertexai, which
need not be installed to reason about this logic. It is extracted rather than
copied for the same reason the JS tests are: a copy drifts, and a test that no
longer matches deployed code reports green while the server does something else.

_ao3_rejections is deliberately pure so this is possible at all. Every real case
below is a row that actually reached, or actually belongs in, the production
table.
"""

from __future__ import annotations

import ast
import io
import pathlib
import sys

SRC = pathlib.Path(__file__).resolve().parent.parent / "bouncer" / "main.py"
WANTED = "_ao3_rejections"


def load_fn():
    """Extract the function plus the module-level CONSTANTS it closes over, so
    the threshold under test is the deployed one rather than a number retyped
    here. Retyping it is how a test ends up passing against a value the server
    does not use."""
    tree = ast.parse(io.open(SRC, encoding="utf-8").read())
    wanted = [
        n for n in tree.body
        if isinstance(n, ast.Assign)
        and any(isinstance(t, ast.Name) and t.id.isupper() and t.id.startswith("AO3_")
                for t in n.targets)
    ]
    fn = next((n for n in tree.body
               if isinstance(n, ast.FunctionDef) and n.name == WANTED), None)
    if fn is None:
        raise SystemExit(
            f"{WANTED} not found in {SRC}. If it was renamed, update this test "
            f"rather than deleting it."
        )
    ns: dict = {}
    exec(compile(ast.Module(wanted + [fn], []), str(SRC), "exec"), ns)
    return ns[WANTED]


rejections = load_fn()

# Real capture URLs, trimmed. The filtered one is what the generator emits.
FILTERED = (
    "https://archiveofourown.org/works?tag_id=Dungeons+*a*+Dragons"
    "+%28Roleplaying+Game%29&work_search%5Bother_tag_names%5D=X"
    "&commit=Sort+and+Filter&_arcane_ip=X"
)
UNFILTERED = (
    "https://archiveofourown.org/works?tag_id=Dungeons+*a*+Dragons"
    "+%28Roleplaying+Game%29&commit=Sort+and+Filter"
)

# Live totals, Sep 14 2026.
TOTALS = {
    "Baldur's Gate (Video Games)": 49615,
    "The Lord of the Rings - All Media Types": 53634,
    "Avatar: The Last Airbender & Related Fandoms": 64874,
    "Stranger Things (TV 2016)": 138509,
}


def row(ip, tag, count, url=FILTERED, platform="ao3"):
    return {
        "ip_name": ip, "platform": platform, "platform_canonical": tag,
        "work_count": count, "source_url": url,
    }


passed = failed = 0


def check(name, got, want):
    global passed, failed
    if got == want:
        passed += 1
        print(f"ok    {name}")
    else:
        failed += 1
        print(f"FAIL  {name}\n        got={got!r}  want={want!r}")


# ── The row this guard exists for ────────────────────────────────────────────
# Sep 14, fourth occurrence. 99.3% of its own fandom.
bg3 = row("Baldur's Gate 3", "Baldur's Gate (Video Games)", 49245)
out = rejections([bg3], TOTALS)
check("the BG3 metatag artifact is rejected", len(out), 1)
check("  ...and the reason names the percentage", "99.3%" in out[0], True)
check("  ...and explains the metatag cause", "metatag" in out[0], True)

# ── Real captures must sail through untouched ────────────────────────────────
# A rejection that could plausibly fire on good data would be worse than none.
good = [
    row("The Lord of the Rings", "The Lord of the Rings - All Media Types", 84),
    row("Avatar: The Last Airbender",
        "Avatar: The Last Airbender & Related Fandoms", 60),
    row("Stranger Things", "Stranger Things (TV 2016)", 83),
]
check("a real 25-IP round is untouched", rejections(good, TOTALS), [])
check("the whole batch together is untouched",
      rejections(good + good, TOTALS), [])

# ── The Sep 1 unfiltered page ────────────────────────────────────────────────
# 10,886 = every D&D crossover on AO3, stored as one IP's count. The filter IS
# the measurement, and AO3 silently ignores a missing one.
unf = row("Some IP", "Some Tag", 10886, url=UNFILTERED)
out = rejections([unf], TOTALS)
check("an unfiltered capture is rejected", len(out), 1)
check("  ...and says the filter is missing",
      "other_tag_names" in out[0], True)
check("  ...even when the count looks modest",
      len(rejections([row("X", "Y", 3, url=UNFILTERED)], TOTALS)), 1)

# ── Scope ────────────────────────────────────────────────────────────────────
check("FFN rows are not judged by AO3 rules",
      rejections([row("Baldur's Gate 3", "Baldur's Gate (FFN id 917/1116)", 4,
                      url="https://www.fanfiction.net/crossovers/", platform="ffn")],
                 TOTALS), [])

# A tag we have no total for cannot be checked for inflation. Declining to judge
# is correct: guessing would be worse, and the capture guard view still raises
# NO_FANDOM_TOTAL downstream.
check("an unknown tag is not judged on inflation",
      rejections([row("New IP", "Some Brand New Canonical", 99999)], TOTALS), [])

# ── The boundary ─────────────────────────────────────────────────────────────
# 50% of a 100-work fandom. Real crossover rates top out near 0.4%.
check("just under the threshold passes",
      rejections([row("X", "T", 50, )], {"T": 100}), [])
check("just over the threshold is rejected",
      len(rejections([row("X", "T", 51)], {"T": 100})), 1)

# ── One bad row is reported alongside the good ones it travelled with ────────
mixed = rejections(good + [bg3], TOTALS)
check("one bad row in a good batch is caught", len(mixed), 1)
check("  ...and it is the right one", "Baldur's Gate 3" in mixed[0], True)

print(f"\n{passed} passed, {failed} failed")
sys.exit(1 if failed else 0)
