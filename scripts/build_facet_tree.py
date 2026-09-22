"""
Generate taxonomy/<store>_facets_v2.json from a captured filter dump.

    python scripts/build_facet_tree.py

WHY THIS EXISTS. The v1 facet files were hand-captured from browse sidebars and
popups. Measured against the storefront's own /api/vBeta/filters endpoint, the
DMs Guild file held 80 of 197 values - 41%, and 11 of 66 product types. Not
because the capture was careless, but because a browse list shows top-level
nodes and the vocabulary is a TREE. No amount of pasting sidebars was ever going
to reach the leaves.

So the tree is no longer transcribed. It is dumped from the API into
taxonomy/raw/<store>_filters.txt and this script builds the JSON. Re-capturing
is a console paste; regenerating is one command; nothing is retyped.

Hand-written judgement - hazards, catch-all warnings, cross-store mapping notes
- lives in taxonomy/annotations.json keyed by (store, id) and is merged in here,
so regenerating never destroys it.

WHAT THE SCRIPT DECIDES, AND WHY IT MATTERS. A node with children is a PARENT,
never a catch-all. "More Settings" reads like an other-bucket and was recorded
as one, with a note saying to treat it as such; it is in fact the parent of
twenty real settings - Al-Qadim, Birthright, Kara-Tur, Maztica, Mystara. Filing
those under "other" would have erased a tier of the market. Catch-all status is
therefore computed from the tree (leaf + matching name), never asserted by hand.
"""

import json
import pathlib
import re
import sys
from collections import defaultdict

REPO = pathlib.Path(__file__).resolve().parent.parent
TAXONOMY = REPO / "taxonomy"
RAW = TAXONOMY / "raw"

# A leaf whose name reads like a bucket. A node with children never qualifies,
# however it is named — see the docstring.
CATCH_ALL_NAMES = re.compile(r"^(other|miscellaneous|more .*|.*\bother\b)$", re.I)

# Parents referenced by children but absent from the dump. 999999 collects the
# storefront's promotional rails (Staff Picks, seasonal sales) — merchandising
# rather than taxonomy, and deliberately not a real root.
PHANTOM_PARENTS = {"999999"}


def slugify(name):
    s = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")
    return s or "unnamed"


def load_raw(path):
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split("|", 2)
        if len(parts) != 3:
            raise SystemExit(f"{path.name}: malformed line: {line!r}")
        fid, pid, name = (p.strip() for p in parts)
        rows.append({"id": fid, "parent": pid, "name": name})
    return rows


def build(store, rows, annotations):
    by_id = {r["id"]: r for r in rows}
    children = defaultdict(list)
    for r in rows:
        children[r["parent"]].append(r["id"])

    def climb(fid):
        """Walk to the root. Returns (root_id, ancestor_path)."""
        path, seen, cur = [], set(), fid
        while True:
            if cur in seen:                      # a cycle would hang the walk
                raise SystemExit(f"{store}: cycle in ancestry at {cur}")
            seen.add(cur)
            node = by_id.get(cur)
            if node is None or node["parent"] in ("0", ""):
                return cur, path
            if node["parent"] in PHANTOM_PARENTS or node["parent"] not in by_id:
                return cur, path                 # orphan: it is its own root
            path.append(node["parent"])
            cur = node["parent"]

    roots, orphans = [], []
    for r in rows:
        if r["parent"] == "0":
            roots.append(r["id"])
        elif r["parent"] in PHANTOM_PARENTS:
            orphans.append((r["id"], "promotional rail, parent not a real facet"))
        elif r["parent"] not in by_id:
            orphans.append((r["id"], f"parent {r['parent']} absent from the dump"))

    axes = {}
    for root_id in sorted(roots, key=int):
        root = by_id[root_id]
        members = []
        for r in rows:
            if r["id"] == root_id:
                continue
            rid, path = climb(r["id"])
            if rid != root_id:
                continue
            # depth: root is 0 and is not a value; its direct children are 1.
            entry = {
                "id": r["id"],
                "slug": slugify(r["name"]),
                "label": r["name"],
                "parent": r["parent"],
                "depth": len(path),
                "is_leaf": not children[r["id"]],
                "child_count": len(children[r["id"]]),
            }
            if entry["is_leaf"] and CATCH_ALL_NAMES.match(r["name"]):
                entry["catch_all"] = True
            note = annotations.get(store, {}).get(r["id"])
            if note:
                entry["note"] = note
            members.append(entry)

        members.sort(key=lambda e: (e["depth"], int(e["id"])))
        axes[slugify(root["name"])] = {
            "root_id": root_id,
            "label": root["name"],
            "cardinality": "many",
            "max_depth": max((e["depth"] for e in members), default=0),
            "value_count": len(members),
            "values": members,
        }

    return {
        "taxonomy_version": f"{store}-v2",
        "store": store,
        "captured_at": "2026-09-22",
        "source": f"GET https://api.{store}.com/api/vBeta/filters (paged, groupId+siteId set)",
        "generated_by": "scripts/build_facet_tree.py",
        "capture_complete": True,
        "_comment": [
            "GENERATED — do not hand-edit. Re-capture into taxonomy/raw/ and rerun the script.",
            "Judgement belongs in taxonomy/annotations.json, which is merged in by id.",
            "",
            "cardinality is 'many' on every axis: a single product carries three product",
            "types and two editions (DMs Guild 457996), so none of these is one-per-product.",
            "",
            "ALWAYS send groupId and siteId. The host does not decide the vocabulary — a",
            "bare call to api.dmsguild.com returns DriveThruRPG's tree with HTTP 200 and no",
            "warning.",
        ],
        "total_values": sum(a["value_count"] for a in axes.values()) + len(axes),
        "orphans": [
            {"id": i, "reason": why, "label": by_id[i]["name"]} for i, why in orphans
        ],
        "axes": axes,
    }


def main():
    ann_path = TAXONOMY / "annotations.json"
    annotations = json.loads(ann_path.read_text(encoding="utf-8")) if ann_path.exists() else {}

    dumps = sorted(RAW.glob("*_filters.txt"))
    if not dumps:
        raise SystemExit("No dumps in taxonomy/raw/ — capture one first.")

    for dump in dumps:
        store = dump.stem.replace("_filters", "")
        rows = load_raw(dump)
        doc = build(store, rows, annotations)
        out = TAXONOMY / f"{store}_facets_v2.json"
        out.write_text(json.dumps(doc, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

        print(f"\n{out.name}  ({len(rows)} rows in, {doc['total_values']} values out)")
        for name, ax in doc["axes"].items():
            print(f"  {ax['label']:<22} root={ax['root_id']:<9} "
                  f"{ax['value_count']:>3} values, depth {ax['max_depth']}")
        if doc["orphans"]:
            print(f"  orphans: {len(doc['orphans'])} "
                  f"({', '.join(o['label'] for o in doc['orphans'][:4])}…)")


if __name__ == "__main__":
    sys.exit(main())
