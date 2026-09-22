"""
Validator for the storefront facet trees in taxonomy/.

    python scripts/test_taxonomy_facets.py

WHY THIS EXISTS. These files are reference data, not code, which makes them easy
to edit carelessly and impossible to notice when someone does. They are also the
join keys for every classified row, so a duplicate id or a silently renamed axis
corrupts analysis rather than crashing anything.

The check that earns its keep is the cross-store one. Both storefronts draw
facet ids from the same numeric ranges (44xxx, 45xxx, 100xxx, 1000xxx), and
neither capture is complete. The moment the same number means two different
things in two files, anything joining on id alone starts silently mixing
DMs Guild's Urban with whatever DriveThruRPG gave that number. No error, no
crash - just a category that quietly counts the wrong products. This fails the
build the day that becomes possible.
"""

import json
import pathlib
import sys
from collections import Counter

REPO = pathlib.Path(__file__).resolve().parent.parent
TAXONOMY = REPO / "taxonomy"

REQUIRED_TOP = ["taxonomy_version", "captured_at", "source", "capture_complete", "axes"]
REQUIRED_VALUE = ["id", "slug", "label"]

# Buckets that are not real values. Analysis must never rank these alongside
# genuine ones, so each has to stay flagged with a note in the data.
CATCH_ALLS = {"more-settings", "previous-storylines", "miscellaneous", "other"}

passed = 0
failed = 0


def check(name, ok, detail=""):
    global passed, failed
    if ok:
        passed += 1
        print(f"  ok   {name}")
    else:
        failed += 1
        print(f"  FAIL {name}")
        if detail:
            print(f"         {detail}")


def load_all():
    files = sorted(TAXONOMY.glob("*_facets_v*.json"))
    if not files:
        print("No facet files found in taxonomy/ — nothing to validate.")
        sys.exit(1)
    return {f.stem: json.loads(f.read_text(encoding="utf-8")) for f in files}


docs = load_all()
print(f"\nFound {len(docs)} facet file(s): {', '.join(docs)}\n")

# ── Per-file structure ──────────────────────────────────────────────────────

for name, doc in docs.items():
    print(f"── {name} ──")

    for key in REQUIRED_TOP:
        check(f"has {key}", key in doc)

    check("capture_complete is a bool", isinstance(doc.get("capture_complete"), bool))

    ids, slugs_by_axis = [], {}
    for axis, spec in doc.get("axes", {}).items():
        check(f"{axis} declares cardinality or slot",
              "cardinality" in spec or "slot" in spec)
        vals = spec.get("values", [])
        check(f"{axis} has values", len(vals) > 0)

        missing = [v for v in vals if not all(k in v for k in REQUIRED_VALUE)]
        check(f"{axis}: every value has id/slug/label", not missing,
              f"{len(missing)} incomplete")

        non_numeric = [v["id"] for v in vals if not str(v.get("id", "")).isdigit()]
        check(f"{axis}: ids are numeric strings", not non_numeric, str(non_numeric))

        ids += [v["id"] for v in vals]
        slugs_by_axis[axis] = [v["slug"] for v in vals]

    dupe_ids = [i for i, n in Counter(ids).items() if n > 1]
    check("no duplicate ids within the store", not dupe_ids, str(dupe_ids))

    for axis, slugs in slugs_by_axis.items():
        d = [s for s, n in Counter(slugs).items() if n > 1]
        check(f"{axis}: no duplicate slugs", not d, str(d))

    # A catch-all ranked as a peer is a real analysis bug, so the data has to
    # say so out loud rather than relying on whoever writes the query knowing.
    unflagged = []
    for axis, spec in doc.get("axes", {}).items():
        for v in spec.get("values", []):
            if v["slug"] in CATCH_ALLS and "note" not in v:
                unflagged.append(f"{axis}.{v['slug']}")
    check("catch-all buckets carry a warning note", not unflagged, str(unflagged))
    print()

# ── Cross-store ─────────────────────────────────────────────────────────────

print("── across stores ──")

flat = {}
for name, doc in docs.items():
    for axis, spec in doc.get("axes", {}).items():
        for v in spec.get("values", []):
            flat.setdefault(v["id"], []).append((name, axis, v["label"]))

collisions = {i: rows for i, rows in flat.items() if len(rows) > 1}

def same_concept(rows):
    # Labels often differ only by their non-Latin half ("Japanese 日本語").
    heads = {r[2].split("/")[0].split("(")[0].strip().lower() for r in rows}
    return len(heads) == 1

real = {i: rows for i, rows in collisions.items() if not same_concept(rows)}

check("no id means different things in different stores", not real,
      "; ".join(f"{i}: " + " vs ".join(f"{s}.{a}={l!r}" for s, a, l in rows)
                for i, rows in real.items()))

if collisions and not real:
    for i, rows in collisions.items():
        print(f"         note: {i} appears in {len(rows)} stores, same concept — "
              f"still not a join key")

check("every store declares a distinct taxonomy_version",
      len({d["taxonomy_version"] for d in docs.values()}) == len(docs))

incomplete = [n for n, d in docs.items() if not d.get("capture_complete")]
if incomplete:
    print(f"\n  note: {len(incomplete)} capture(s) flagged incomplete "
          f"({', '.join(incomplete)}) — absence of a value proves nothing there")

print(f"\n{passed} passed, {failed} failed\n")
sys.exit(1 if failed else 0)
