"""Tests for timestamp generation in bouncer/main.py.

    python scripts/test_bouncer_timestamps.py

Guards the trap that made the utcnow migration a per-site audit rather than a
find-and-replace (see docs/bouncer_utcnow_migration.md):

    naive  ->  2026-09-15T11:22:12.000876          + 'Z'  ->  ...876Z      valid
    aware  ->  2026-09-15T11:22:12.000876+00:00    + 'Z'  ->  ...+00:00Z   INVALID

`_utc_now_iso()` returns the AWARE form, so appending 'Z' to it writes a
malformed timestamp straight into BigQuery — on the ingest routes every
bookmarklet posts to. The last two checks below are source-level: they fail if
anyone reintroduces the naive call or re-appends the 'Z'.

The function is extracted from the module rather than imported, because
importing bouncer/main.py pulls in functions_framework and vertexai. Same
pattern, and same reason, as test_bouncer_ao3_guard.py.
"""

from __future__ import annotations

import ast
import datetime
import io
import pathlib
import sys

SRC = pathlib.Path(__file__).resolve().parent.parent / "bouncer" / "main.py"
SOURCE = io.open(SRC, encoding="utf-8").read()
WANTED = "_utc_now_iso"


def load_fn():
    tree = ast.parse(SOURCE)
    fn = next((n for n in tree.body
               if isinstance(n, ast.FunctionDef) and n.name == WANTED), None)
    if fn is None:
        raise SystemExit(
            f"{WANTED} not found in {SRC}. If it was renamed, update this test "
            f"rather than deleting it."
        )
    ns: dict = {"datetime": datetime}
    exec(compile(ast.Module([fn], []), str(SRC), "exec"), ns)
    return ns[WANTED]


utc_now_iso = load_fn()

passed = failed = 0


def check(name, got, want):
    global passed, failed
    if got == want:
        passed += 1
        print(f"ok    {name}")
    else:
        failed += 1
        print(f"FAIL  {name}\n        got={got!r}  want={want!r}")


value = utc_now_iso()

# ── The contract ─────────────────────────────────────────────────────────────
check("returns a string", isinstance(value, str), True)
check("does NOT end in 'Z' — appending one is the trap",
      value.endswith("Z"), False)
check("never contains the malformed '+00:00Z'", "+00:00Z" in value, False)

parsed = datetime.datetime.fromisoformat(value)
check("round-trips through fromisoformat", parsed.isoformat(), value)
check("is timezone-aware", parsed.tzinfo is not None, True)
check("offset is exactly UTC", parsed.utcoffset(), datetime.timedelta(0))

# Loose, so a slow machine or a clock skew of a few seconds cannot fail the
# suite. The point is "roughly now", not stopwatch accuracy.
now = datetime.datetime.now(datetime.timezone.utc)
check("is within 60s of now", abs((now - parsed).total_seconds()) < 60, True)

# ── Source-level regression guards ───────────────────────────────────────────
# These are the ones that actually matter long-term. The value contract above
# can stay green while a new call site reintroduces the bug beside it.
check("no naive utcnow() call survives in the module",
      "datetime.datetime.utcnow(" in SOURCE, False)
check("nothing appends 'Z' to the helper",
      "_utc_now_iso() +" in SOURCE, False)

print(f"\n{passed} passed, {failed} failed")
sys.exit(1 if failed else 0)
