"""
Validator for the generated facet trees in taxonomy/.

    python scripts/test_taxonomy_facets.py

WHY THIS EXISTS. These files are the join keys for every classified product, so
a broken parent link or a stale annotation corrupts analysis rather than
crashing anything. Nothing here errors at runtime; it just quietly counts the
wrong products.

The v1 version of this validator enforced a misreading. It carried a hard-coded
CATCH_ALLS list containing "more-settings" and FAILED the build unless that
value was flagged as an other-bucket with a warning note. The API later showed
"More Settings" is the parent of twenty real settings. So the test was not
merely silent about the error - it required it, and would have kept requiring it
while twenty settings were filed under "other".

The lesson shaping this rewrite: assertions about the data belong in the data.
Catch-all status is now COMPUTED by the generator (leaf + name shape) and this
file only checks the computation is self-consistent. Judgement lives in
annotations.json, and the checks below make sure it stays attached to values
that actually exist.
"""

import json
import pathlib
import sys
from collections import Counter

REPO = pathlib.Path(__file__).resolve().parent.parent
TAXONOMY = REPO / "taxonomy"

passed = failed = 0


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


docs = {p.stem: json.loads(p.read_text(encoding="utf-8"))
        for p in sorted(TAXONOMY.glob("*_facets_v2.json"))}
if not docs:
    print("No generated facet files found — run scripts/build_facet_tree.py")
    sys.exit(1)

ann_path = TAXONOMY / "annotations.json"
annotations = json.loads(ann_path.read_text(encoding="utf-8")) if ann_path.exists() else {}

print(f"\nFound {len(docs)} generated file(s): {', '.join(docs)}\n")

for name, doc in docs.items():
    print(f"── {name} ──")
    store = doc.get("store")

    for key in ("taxonomy_version", "store", "captured_at", "source",
                "generated_by", "axes", "orphans"):
        check(f"has {key}", key in doc)

    every = [v for ax in doc["axes"].values() for v in ax["values"]]
    ids = {v["id"] for v in every} | {ax["root_id"] for ax in doc["axes"].values()}

    dupes = [i for i, n in Counter(
        [v["id"] for v in every] + [ax["root_id"] for ax in doc["axes"].values()]).items() if n > 1]
    check("no id appears twice", not dupes, str(dupes))

    # Every parent must resolve, or the value is misfiled: a dangling parent
    # means its whole subtree hangs off nothing and silently leaves the axis.
    dangling = [v["id"] for v in every if v["parent"] not in ids]
    check("every parent resolves within the store", not dangling, str(dangling[:8]))

    # Depth must be consistent with the parent chain, since depth is what
    # rollups use. An off-by-one here would roll a child to the wrong ancestor.
    #
    # CONVENTION: the root is depth 0 and is not itself a value; a direct child
    # of the root is depth 1. This check disagreed with the generator on first
    # run — neither was wrong, the convention simply had not been written down
    # anywhere, which is exactly how an off-by-one gets into a rollup later.
    roots = {ax["root_id"] for ax in doc["axes"].values()}
    by_id = {v["id"]: v for v in every}
    bad_depth = []
    for v in every:
        expected = 1 if v["parent"] in roots \
            else by_id.get(v["parent"], {}).get("depth", -99) + 1
        if v["depth"] != expected:
            bad_depth.append(f"{v['id']}({v['label']}) depth={v['depth']} expected={expected}")
    check("depth matches the parent chain", not bad_depth, "; ".join(bad_depth[:4]))

    # THE CHECK THAT WOULD HAVE CAUGHT THE v1 MISREADING.
    wrong = [f"{v['id']} {v['label']} ({v['child_count']} children)"
             for v in every if v.get("catch_all") and v["child_count"] > 0]
    check("no catch-all has children", not wrong, "; ".join(wrong))

    check("leaf flag matches child count",
          all(v["is_leaf"] == (v["child_count"] == 0) for v in every))

    # A stale annotation is worse than none: it reads as current judgement about
    # a value that no longer exists, and nothing else would ever reveal it.
    stale = [k for k in annotations.get(store, {}) if k not in ids]
    check("every annotation points at a live id", not stale, str(stale))

    attached = sum(1 for v in every if "note" in v)
    check("annotations reached the generated file",
          attached == len([k for k in annotations.get(store, {}) if k in ids]),
          f"{attached} attached")

    print(f"       {len(every)} values · {len(doc['axes'])} axes · "
          f"{len(doc['orphans'])} orphans · max depth "
          f"{max(ax['max_depth'] for ax in doc['axes'].values())}")
    print()

# ── Cross-store ─────────────────────────────────────────────────────────────
# Both stores draw ids from the same ranges. The day one number means two
# things, anything joining on id alone starts mixing them with no error.

print("── across stores ──")
flat = {}
for name, doc in docs.items():
    for ax in doc["axes"].values():
        for v in ax["values"]:
            flat.setdefault(v["id"], []).append((doc["store"], ax["label"], v["label"]))

real = {i: rows for i, rows in flat.items()
        if len({r[2].split("/")[0].split("(")[0].strip().lower() for r in rows}) > 1}
check("no id means different things in different stores", not real,
      "; ".join(f"{i}: " + " vs ".join(f"{s}.{a}={l!r}" for s, a, l in rows)
                for i, rows in list(real.items())[:4]))

check("each store has its own taxonomy_version",
      len({d["taxonomy_version"] for d in docs.values()}) == len(docs))

print(f"\n{passed} passed, {failed} failed\n")
sys.exit(1 if failed else 0)
