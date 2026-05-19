# Data-Scrape Scheduling Policy

**Status:** Canonical. Supersedes the retired "Caldean" system. Established 2026-05-19 (Phil).

## TL;DR

Every data stream runs on **one fixed UTC time daily, Sunday–Friday (Saturday skipped), plus one Sunday post-Shabbat catch-up.** No variable timing. Shabbat is never scraped.

## Background: Caldean is retired

The old **Caldean** system scraped during the Kabbalistic **Mercury planetary hour** — a clock time that *changed every day* — implemented as many pre-generated one-time Cloud Scheduler jobs. With the number of streams we now run, this is too complex to track and maintain. It is **scrapped**. Do **not** reintroduce Mercury-hour or any per-day-variable timing. `cloud_functions/caldean_calculator/` is dead code (remove when convenient; not urgent).

The **Shabbat blackout is kept** — it is a hard, non-negotiable requirement. Only the variable-time complexity was removed.

## The Shabbat blackout window (authoritative)

Defined and enforced at runtime by [`cloud_functions/shared/shabbat_gate.py`](../cloud_functions/shared/shabbat_gate.py) — keep that gate wired into every harvester as defense-in-depth even though schedulers are designed to avoid the window.

```
BLACKOUT  =  Friday 21:30 UTC  →  Sunday 03:45 UTC
```

- `Friday 21:30 UTC` = the **earliest possible** Friday Halachic sundown across the whole year.
- `Sunday 03:45 UTC` = the **latest possible** Saturday-night Halachic twilight across the whole year.
- Fixed worst-case bounds so no per-week sundown calculation is ever needed.
- **Location caveat:** computed for Iowa (Council Bluffs / Cedar Rapids). Iowa is north of Kansas, so its seasonal daylight swing is larger and the window is *more conservative* than Kansas requires (safe). **OPEN ITEM:** confirm the intended location/coordinates; re-verify bounds if it should be Kansas-specific.

## Canonical cron pattern (apply to EVERY stream)

Two recurring Cloud Scheduler jobs per stream, both in UTC:

| Job | Cron | Fires | Purpose |
|-----|------|-------|---------|
| Daily | `M H * * 0-5` | Sun–Fri at `H:M` (cron dow `6`=Sat excluded) | Normal daily scrape |
| Post-Shabbat catch-up | `0 4 * * 0` | Sunday 04:00 UTC | Covers the full Shabbat gap |

Rules:

1. **`H:M` must be before 21:30 UTC.** This guarantees Friday's run is always pre-Shabbat *and* every Sunday/weekday run is well clear of the window. **Proposed default: `15:00 UTC`** (already used by several streams; Fri 15:00 ≪ 21:30 ✓, Sun 15:00 ≫ 03:45 ✓). Pick one time and use it for all streams.
2. **Saturday is never scheduled** (excluded by `0-5`).
3. **The Sunday 04:00 UTC catch-up** runs just after the blackout ends (03:45). In Phil's Central time this is "late Saturday night." It backfills everything since Friday's run: with the 15:00 default, Fri 15:00 → Sun 04:00 ≈ **37 hours** (the required ">24h, ~30h" catch-up). Sunday also gets its normal 15:00 run — a harmless second pass for watermark-based streams.

**Hard requirement for the catch-up to actually catch up:** each stream must be either
- **(a) watermark-based** — resumes from its last successful timestamp, so it auto-covers any gap (preferred), or
- **(b) fixed-lookback** with a lookback ≥ **~40 hours** on the Sunday catch-up run.

Verify this per stream as it is migrated. A stream with a 24h lookback would silently lose Saturday's data.

## Migration plan

Move **all** streams onto this pattern incrementally.

- **Pilot: Wikipedia** (also currently broken — watermark None-crash + missing scheduler; see `project_data_streams_status.md` memory). Fixing it = first application of this policy.
- Subsequent streams: TBD as reached.
- **Not every stream needs daily cadence.** Some (crowdfunding, catalog supply, etc.) may run weekly or Mon/Wed/Fri — that is fine, but they still use the **same Shabbat-safe time bounds** (a time before 21:30 UTC, never Saturday, catch-up after Sun 03:45 UTC).
- Each scheduler create/update is a GCP state change → **pause-and-ask** before applying.

## How to apply (checklist for any new/audited stream)

1. Schedule fires strictly **outside** `[Fri 21:30 UTC, Sun 03:45 UTC]`.
2. Use the 2-cron canonical pattern (daily `M H * * 0-5` + `0 4 * * 0`), or a reduced cadence on the same safe bounds.
3. Confirm the stream is watermark-based **or** has a ≥40h lookback for the Sunday catch-up.
4. Keep `shabbat_gate.py` wired in the function as runtime defense-in-depth.
5. Never variable/Mercury-hour timing.
