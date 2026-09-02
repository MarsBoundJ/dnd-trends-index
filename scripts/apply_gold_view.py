"""
apply_gold_view — deploy a view definition from gold_views/ to BigQuery.

Generalises the one-off pattern in apply_wikipedia_view.py. The .sql file stays
the source of truth rather than SQL being duplicated inside a Python string, so
what is reviewed in the repo is exactly what is deployed.

Runs as YOU (application default credentials). The MCP BigQuery connector used
by tooling is read-only — it lacks bigquery.tables.create — so view deploys go
through this path.

Usage:
    python scripts/apply_gold_view.py gold_views/fanfic_capture_guard.sql
    python scripts/apply_gold_view.py gold_views/foo.sql --dry-run

After a successful deploy it runs a row count against the new view, which for
guard-style views is the useful signal: 0 rows means nothing is currently
flagged.
"""

from __future__ import annotations

import argparse
import pathlib
import re
import sys

VIEW_RE = re.compile(
    r"CREATE\s+(?:OR\s+REPLACE\s+)?VIEW\s+`([^`]+)`", re.IGNORECASE
)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("sql_file")
    ap.add_argument("--dry-run", action="store_true",
                    help="print the target view and exit without deploying")
    args = ap.parse_args()

    path = pathlib.Path(args.sql_file)
    if not path.exists():
        print(f"error: {path} not found", file=sys.stderr)
        return 1

    sql = path.read_text(encoding="utf-8")
    m = VIEW_RE.search(sql)
    if not m:
        print(f"error: {path} contains no CREATE [OR REPLACE] VIEW statement.\n"
              f"       This script deploys views only — it will not run arbitrary DML.",
              file=sys.stderr)
        return 1

    view = m.group(1)
    print(f"file : {path}")
    print(f"view : {view}")
    print(f"bytes: {len(sql):,}")

    if args.dry_run:
        print("\n--dry-run: not deploying.")
        return 0

    from google.cloud import bigquery  # imported late so --dry-run needs no deps
    from google.auth import exceptions as auth_exc

    project = view.split(".")[0]

    # Expired ADC surfaces as a ~60-line traceback whose only useful content is
    # the final line. Catch it and say the one thing that matters — including
    # that ADC is refreshed by a DIFFERENT command than the gcloud/bq
    # credential, which is an easy hour to lose.
    try:
        client = bigquery.Client(project=project)
        print("\ndeploying …")
        client.query(sql).result()
    except (auth_exc.RefreshError, auth_exc.DefaultCredentialsError) as e:
        print(f"\nAUTH FAILED: {type(e).__name__}: {e}\n", file=sys.stderr)
        print("Your Application Default Credentials are missing or expired.\n"
              "ADC is what Python client libraries use, and it is refreshed by a\n"
              "DIFFERENT command than the gcloud CLI / bq credential:\n\n"
              "    gcloud auth application-default login\n\n"
              "  (`gcloud auth login` alone authenticates the CLI, NOT ADC.)\n\n"
              "Alternative that needs no CLI auth: paste the .sql into the\n"
              "BigQuery console and run it there.", file=sys.stderr)
        return 2
    except Exception as e:
        print(f"\nDEPLOY FAILED: {type(e).__name__}: {e}", file=sys.stderr)
        return 1

    print("view created.")

    # Row count is the meaningful check for a guard view: 0 == nothing flagged.
    n = list(client.query(f"SELECT COUNT(*) AS n FROM `{view}`").result())[0].n
    print(f"\nrows currently returned: {n}")
    if n == 0:
        print("  clean — no findings against current data.")
    else:
        print(f"  {n} finding(s). Inspect with:")
        print(f"    SELECT * FROM `{view}` ORDER BY severity, work_count DESC")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
