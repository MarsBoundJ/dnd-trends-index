# Handover — replacing `datetime.utcnow()` in `bouncer/main.py`

**Written Sep 14, 2026, to be picked up Sep 15.** Everything needed to start is
here; nothing below needs re-deriving.

## Why

`bouncer-api` moved to **python312** on Sep 14. `datetime.datetime.utcnow()` is
**deprecated** there. It still works, so this is debt, not an outage — there is
no deadline and no user-visible symptom today.

## Why it is not a find-and-replace

`utcnow()` returns a **naive** datetime. `datetime.now(timezone.utc)` returns an
**aware** one. `.isoformat()` then appends an offset:

```
naive  ->  2026-09-14T23:45:00.123456          + 'Z'  ->  ...123456Z     valid
aware  ->  2026-09-14T23:45:00.123456+00:00    + 'Z'  ->  ...+00:00Z     INVALID
```

Five of the ten sites append a literal `'Z'`. Swapping the call without removing
that `+ 'Z'` writes a malformed timestamp **straight into BigQuery**, on the
ingest routes every bookmarklet posts to. That is the whole reason this was kept
out of the runtime-upgrade commit.

## The ten sites, already audited

Destination column types were checked — **every one is `TIMESTAMP`**, never
`DATETIME`. That matters: `TIMESTAMP` accepts an offset, so the aware form is
safe wherever the stray `'Z'` is removed. (A `DATETIME` column would reject
`+00:00` and would have needed `.replace(tzinfo=None)` instead. None here.)

| Line | Current shape | Writes to | Change needed |
|---|---|---|---|
| 1248 | `.strftime('%Y-%m-%d %H:%M:%S')` | `gold_data.chat_archives.created_at` | **swap only** — the format string has no `%z`, so output is byte-identical |
| 1449 | `.date().isoformat()` | dedup guard, `DATE(discovered_at) = '…'` | **swap only** — `.date()` is the same UTC date either way |
| 1450 | `.isoformat() + 'Z'` | `commercial_data.kickstarter_projects.discovered_at` | **swap + DROP `+ 'Z'`** |
| 1489 | `.date().isoformat()` | dedup guard, `DATE(scraped_at) = '…'` | **swap only** |
| 1490 | `.isoformat() + 'Z'` | `commercial_data.backerkit_projects.scraped_at` (NOT NULL) | **swap + DROP `+ 'Z'`** |
| 1556 | `.isoformat() + 'Z'` | `dnd_trends_raw.fanfic_crossover_counts.scraped_at` | **swap + DROP `+ 'Z'`** |
| 1756 | `.isoformat() + 'Z'` | `dnd_trends_raw.ddb_homebrew_counts.scraped_at` (NOT NULL) | **swap + DROP `+ 'Z'`** |
| 1914 | `.isoformat() + 'Z'` | `dnd_trends_raw.forum_thread_bodies.scraped_at` | **swap + DROP `+ 'Z'`** |
| 2061 | `.isoformat()` (bare) | `review_queue.reviewed_at`, via a SQL string literal | **swap only** — TIMESTAMP parses `+00:00` |
| 2229 | `.isoformat()` (bare) | `seeds.added_at` (NOT NULL), via `insert_rows_json` | **swap only** |

**Five swap-only, five that must also lose the `+ 'Z'`.**

## Recommended approach: one helper, not ten edits

Add near the top of `bouncer/main.py`:

```python
def _utc_now_iso() -> str:
    """UTC now as an RFC3339 string, for BigQuery TIMESTAMP columns.

    Do NOT append 'Z' to this. It is timezone-aware, so isoformat() already
    ends in '+00:00'; adding 'Z' produces '+00:00Z', which BigQuery rejects.
    That trap is why this helper exists instead of ten inline call sites.
    """
    return datetime.datetime.now(datetime.timezone.utc).isoformat()
```

Then `now_ts = _utc_now_iso()` at the five `+ 'Z'` sites and the two bare ones.
Lines 1248, 1449 and 1489 want the datetime rather than the string, so give them
`datetime.datetime.now(datetime.timezone.utc)` directly and keep their existing
`.strftime(...)` / `.date().isoformat()` tails unchanged.

One helper means the aware/naive decision lives in one documented place, and the
next person adding a timestamp cannot reintroduce the `'Z'` bug.

## Output format change, and why it is acceptable

Old: `2026-09-14T23:45:00.123456Z`. New: `2026-09-14T23:45:00.123456+00:00`.
Both are valid RFC3339 and BigQuery stores the **same instant** from either.

If byte-identical output is wanted, append `.replace('+00:00', 'Z')` inside the
helper. **Check first** whether any response body or dashboard string-matches on
a trailing `Z` — the ingest routes return only `{"inserted": N}`, so this is
unlikely, but it is the one thing worth grepping before choosing.

## Verification plan

No unit-test harness covers `bouncer/main.py`. Verify by exercising the routes,
after deploy, the way the AO3 guard was verified:

1. **fanfic (1556)** — POST one row to `/system/fanfic/ingest-crossover-count`,
   then read it back and confirm `scraped_at` parses and is ~now. Use a real
   capture, or accept that a test row must then be quarantined.
2. **The other four `'Z'` sites** are the same code shape. If fanfic round-trips
   correctly, the shared risk is retired; a malformed timestamp fails loudly at
   insert (`insert_rows_json` returns errors) rather than silently.
3. **Grep afterwards** for `utcnow` — expect zero hits.
4. Smoke-test `/system/health` to confirm the module still imports.

## Guardrails that apply

- **Deploying is blocked for Claude** by the auto-mode classifier. Prepare,
  verify, then hand Phil the command. Write it in **PowerShell** form with no
  `/c/...` path. The deploy is the same one used on Sep 14:

  ```
  gcloud functions deploy bouncer-api --gen2 --region=us-central1 --project=dnd-trends-index --runtime=python312 --entry-point=bouncer_api --source=bouncer --trigger-http --memory=512M --timeout=300
  ```

- **Confirm the deploy with `describe`, comparing `updateTime`** — on Sep 14 a
  pasted command failed at a bad `cd` and never ran `gcloud`, while the previous
  deploy's output scrolling past looked like success.
- `bouncer-api` serves **every** bookmarklet ingest, not just AO3. A bad deploy
  stops all capture until rolled back.

## State as of handover

`main` at `8d394ef`. `bouncer/main.py` last changed by #137 (the AO3 guard
failing open). Nothing in flight, working tree clean apart from two untracked
PC-builder scripts that are deliberately left alone.
