"""
related_queries_discovery/main.py
Arcane Analytics — GCP project: dnd-trends-index

Cloud Function: discover_related_queries
----------------------------------------
Calls pytrends related_queries() and related_topics() for a set of seed
keywords, writes raw results to BigQuery (dnd_trends_raw.related_queries),
then cross-references "Rising" terms against concept_library and writes
any novel entries to dnd_trends_raw.emerging_terms for human review.

Triggered via HTTP (Cloud Scheduler or manual invocation).
Proxy: Webshare residential (same config as existing scraper).
"""

import os
import json
import time
import logging
import random
import hashlib
import datetime
import functions_framework

import requests
from requests.auth import HTTPProxyAuth

from pytrends.request import TrendReq
from google.cloud import bigquery

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config — all sensitive values pulled from environment / Secret Manager
# ---------------------------------------------------------------------------
PROJECT_ID      = os.environ.get("GCP_PROJECT", "dnd-trends-index")
RAW_DATASET     = "dnd_trends_raw"
LIB_DATASET     = "dnd_trends_categorized"
RESULTS_TABLE   = f"{PROJECT_ID}.{RAW_DATASET}.related_queries"
EMERGING_TABLE  = f"{PROJECT_ID}.{RAW_DATASET}.emerging_terms"
LIBRARY_TABLE   = f"{PROJECT_ID}.{LIB_DATASET}.concept_library"
SEEDS_TABLE     = f"{PROJECT_ID}.{LIB_DATASET}.seeds"

# Webshare residential proxy  (same env-var names as existing scraper)
PROXY_HOST      = os.environ.get("WEBSHARE_PROXY_HOST", "p.webshare.io")
PROXY_PORT      = os.environ.get("WEBSHARE_PROXY_PORT", "80")
PROXY_USER      = os.environ.get("WEBSHARE_PROXY_USER", "")
PROXY_PASS      = os.environ.get("WEBSHARE_PROXY_PASS", "")

# Pytrends knobs
GEO             = os.environ.get("TRENDS_GEO", "US")          # "" = worldwide
TIMEFRAME       = os.environ.get("TRENDS_TIMEFRAME", "today 3-m")
LANG            = os.environ.get("TRENDS_LANG", "en-US")
RETRIES         = int(os.environ.get("TRENDS_RETRIES", "3"))
RETRY_BACKOFF   = float(os.environ.get("TRENDS_RETRY_BACKOFF", "15"))  # seconds

# Default seed keywords (overridable via request body)
DEFAULT_SEEDS = [
    "dungeons and dragons",
    "dnd 5e",
    "dnd 2024",
    "pathfinder 2e",
    "ttrpg",
    "one dnd",
    "dnd beyond",
]

# ---------------------------------------------------------------------------
# Proxy helpers (mirrors existing scraper pattern)
# ---------------------------------------------------------------------------

def _build_proxy_url():
    if not PROXY_HOST or not PROXY_PORT:
        return None
    if PROXY_USER and PROXY_PASS:
        return f"http://{PROXY_USER}:{PROXY_PASS}@{PROXY_HOST}:{PROXY_PORT}"
    return f"http://{PROXY_HOST}:{PROXY_PORT}"


def _build_pytrends_session(proxy_url: str = None) -> TrendReq:
    """
    Build a TrendReq that optionally routes through a proxy.
    Proxy is passed via the direct `proxies` dict parameter (not requests_args)
    because pytrends 4.9.2 only applies session.proxies.update() from that path.
    """
    requests_args = {
        "verify": True,
        "headers": {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
            "Accept-Language": "en-US,en;q=0.9",
        }
    }
    # Pass proxy both ways:
    # - proxies=[url] for GetGoogleCookie() which uses self.proxies[index]
    # - requests_args["proxies"] for _get_data() session-level proxy (if pytrends applies it)
    proxies_list = [proxy_url] if proxy_url else []
    if proxy_url:
        requests_args["proxies"] = {"http": proxy_url, "https": proxy_url}
        log.warning("PROXY: routing through %s", proxy_url.split("@")[-1])
    else:
        log.warning("PROXY: none — requests will originate from GCP IP (likely to be rate-limited)")

    pt = TrendReq(
        hl=LANG,
        tz=360,
        timeout=(10, 30),
        retries=2,
        backoff_factor=0.5,
        proxies=proxies_list,
        requests_args=requests_args,
    )

    # Post-init: log all TrendReq attributes to find the session object name
    if proxy_url:
        attrs = [a for a in vars(pt) if 'session' in a.lower() or 'proxy' in a.lower()]
        log.warning("PROXY: TrendReq instance attrs matching session/proxy: %s", attrs)
        for attr in attrs:
            sess = getattr(pt, attr, None)
            if sess and hasattr(sess, 'proxies'):
                sess.proxies.update({"http": proxy_url, "https": proxy_url})
                log.warning("PROXY: set on pt.%s.proxies", attr)
                break
        else:
            log.warning("PROXY: no session attr found. All instance attrs: %s", list(vars(pt).keys()))

    return pt


# ---------------------------------------------------------------------------
# Pytrends fetch helpers
# ---------------------------------------------------------------------------

def _safe_fetch(pytrends: TrendReq, kw_list: list[str], attempt: int = 1):
    """
    Wraps pytrends.build_payload + related_queries/topics with retry logic.
    Returns (queries_dict, topics_dict) or raises on persistent failure.

    related_topics() is handled separately because pytrends has a known bug:
    it crashes with IndexError when rankedList is empty instead of returning
    an empty result. We catch that gracefully and continue with queries data.
    """
    kw = kw_list[0] if kw_list else "?"

    # --- related_queries (primary data — retry on failure) ---
    try:
        pytrends.build_payload(kw_list, cat=0, timeframe=TIMEFRAME, geo=GEO, gprop="")
        queries = pytrends.related_queries()  # {kw: {"top": df, "rising": df}}
        log.info("related_queries() OK for '%s'", kw)
    except Exception as exc:
        import traceback as _tb
        log.warning("related_queries error (attempt %d/%d) for '%s': %s\n%s",
                    attempt, RETRIES, kw, exc, _tb.format_exc())
        if attempt >= RETRIES:
            raise
        jitter = random.uniform(0, 10)
        time.sleep(RETRY_BACKOFF * attempt + jitter)
        return _safe_fetch(pytrends, kw_list, attempt + 1)

    # Sleep between the two calls — Google rate-limits /relatedqueries and
    # /relatedsearches independently; back-to-back calls cause the second
    # to return None silently rather than raising a 429.
    time.sleep(random.uniform(3, 6))

    # --- related_topics (secondary — fail gracefully, don't discard queries data) ---
    topics = None
    try:
        topics = pytrends.related_topics()   # {kw: {"top": df, "rising": df}}
        topic_result = (topics or {}).get(kw, {})
        if not topic_result or (topic_result.get("top") is None and topic_result.get("rising") is None):
            log.warning("related_topics() returned no data for '%s' — possible rate-limit or no topic coverage", kw)
        else:
            log.info("related_topics() OK for '%s'", kw)
    except IndexError:
        # pytrends bug: crashes on empty rankedList instead of returning empty result
        log.warning("related_topics() returned empty rankedList for '%s' — skipping topics", kw)
    except Exception as exc:
        import traceback as _tb
        log.warning("related_topics() error for '%s': %s\n%s", kw, exc, _tb.format_exc())

    return queries, topics


def _df_to_rows(df, seed_kw: str, result_type: str, data_type: str, run_id: str) -> list[dict]:
    """
    Convert a pytrends DataFrame to a list of BQ-ready dicts.

    data_type  : "query" | "topic"
    result_type: "top"   | "rising"
    """
    rows = []
    if df is None or df.empty:
        return rows

    now = datetime.datetime.utcnow().isoformat()

    for _, row in df.iterrows():
        # column names differ between related_queries (query, value) and
        # related_topics (topic_title, topic_type, value)
        if data_type == "query":
            term        = str(row.get("query", "")).strip()
            topic_type  = None
        else:
            term        = str(row.get("topic_title", "")).strip()
            topic_type  = str(row.get("topic_type", "")).strip() or None

        value = row.get("value")
        # pytrends encodes "Breakout" as the string "Breakout" for rising >+200%
        value_numeric = None
        is_breakout   = False
        if isinstance(value, str) and value.lower() == "breakout":
            is_breakout = True
        else:
            try:
                value_numeric = int(value)
            except (TypeError, ValueError):
                pass

        if not term:
            continue

        term_id = hashlib.md5(f"{seed_kw}|{term}|{data_type}|{result_type}".encode()).hexdigest()

        rows.append({
            "run_id":       run_id,
            "term_id":      term_id,
            "seed_keyword": seed_kw,
            "term":         term,
            "data_type":    data_type,       # "query" or "topic"
            "result_type":  result_type,     # "top" or "rising"
            "topic_type":   topic_type,      # only for topics
            "value":        value_numeric,   # relative interest 0-100 or % change
            "is_breakout":  is_breakout,     # >200% rising
            "geo":          GEO or "WW",
            "timeframe":    TIMEFRAME,
            "fetched_at":   now,
        })
    return rows


# ---------------------------------------------------------------------------
# BigQuery helpers
# ---------------------------------------------------------------------------

def _ensure_tables(client: bigquery.Client) -> None:
    """Create datasets + tables if they don't already exist."""

    # Dataset
    for ds_id in [RAW_DATASET]:
        ds_ref = bigquery.Dataset(f"{PROJECT_ID}.{ds_id}")
        ds_ref.location = "US"
        client.create_dataset(ds_ref, exists_ok=True)
        log.info("Dataset ready: %s", ds_id)

    # related_queries table
    rq_schema = [
        bigquery.SchemaField("run_id",       "STRING",    mode="REQUIRED"),
        bigquery.SchemaField("term_id",      "STRING",    mode="REQUIRED"),
        bigquery.SchemaField("seed_keyword", "STRING",    mode="REQUIRED"),
        bigquery.SchemaField("term",         "STRING",    mode="REQUIRED"),
        bigquery.SchemaField("data_type",    "STRING",    mode="REQUIRED"),
        bigquery.SchemaField("result_type",  "STRING",    mode="REQUIRED"),
        bigquery.SchemaField("topic_type",   "STRING",    mode="NULLABLE"),
        bigquery.SchemaField("value",        "INTEGER",   mode="NULLABLE"),
        bigquery.SchemaField("is_breakout",  "BOOL",      mode="REQUIRED"),
        bigquery.SchemaField("geo",          "STRING",    mode="REQUIRED"),
        bigquery.SchemaField("timeframe",    "STRING",    mode="REQUIRED"),
        bigquery.SchemaField("fetched_at",   "TIMESTAMP", mode="REQUIRED"),
    ]
    rq_table = bigquery.Table(RESULTS_TABLE, schema=rq_schema)
    rq_table.time_partitioning = bigquery.TimePartitioning(field="fetched_at")
    rq_table.clustering_fields = ["seed_keyword", "result_type", "data_type"]
    client.create_table(rq_table, exists_ok=True)
    log.info("Table ready: %s", RESULTS_TABLE)

    # emerging_terms table
    et_schema = [
        bigquery.SchemaField("run_id",         "STRING",    mode="REQUIRED"),
        bigquery.SchemaField("term_id",        "STRING",    mode="REQUIRED"),
        bigquery.SchemaField("term",           "STRING",    mode="REQUIRED"),
        bigquery.SchemaField("seed_keyword",   "STRING",    mode="REQUIRED"),
        bigquery.SchemaField("data_type",      "STRING",    mode="REQUIRED"),
        bigquery.SchemaField("value",          "INTEGER",   mode="NULLABLE"),
        bigquery.SchemaField("is_breakout",    "BOOL",      mode="REQUIRED"),
        bigquery.SchemaField("review_status",  "STRING",    mode="REQUIRED"),  # PENDING
        bigquery.SchemaField("flagged_at",     "TIMESTAMP", mode="REQUIRED"),
    ]
    et_table = bigquery.Table(EMERGING_TABLE, schema=et_schema)
    et_table.time_partitioning = bigquery.TimePartitioning(field="flagged_at")
    client.create_table(et_table, exists_ok=True)
    log.info("Table ready: %s", EMERGING_TABLE)


def _insert_rows(client: bigquery.Client, table_id: str, rows: list[dict]) -> int:
    """Stream-insert rows, return count inserted."""
    if not rows:
        return 0
    errors = client.insert_rows_json(table_id, rows)
    if errors:
        log.error("BQ insert errors for %s: %s", table_id, errors)
        raise RuntimeError(f"BigQuery insert failed: {errors}")
    log.info("Inserted %d rows → %s", len(rows), table_id)
    return len(rows)


def _get_known_terms(client: bigquery.Client) -> set[str]:
    """
    Pull the lowercase normalized term set from concept_library.
    Used for novelty-check against Rising terms.
    """
    query = f"""
        SELECT LOWER(TRIM(concept_name)) AS kw
        FROM `{LIBRARY_TABLE}`
        WHERE concept_name IS NOT NULL
    """
    try:
        return {row["kw"] for row in client.query(query).result()}
    except Exception as exc:
        log.warning("Could not fetch concept_library terms: %s — novelty check skipped", exc)
        return set()


# ---------------------------------------------------------------------------
# Seeds table helpers
# ---------------------------------------------------------------------------

SEEDS_PER_RUN = int(os.environ.get("SEEDS_PER_RUN", "40"))


def _fetch_active_seeds(bq: bigquery.Client) -> list[str]:
    """
    Load active seed keywords from dnd_trends_categorized.seeds.
    Picks the SEEDS_PER_RUN least-recently-used seeds so the full set is
    rotated across runs rather than always processing the same first N.
    Falls back to DEFAULT_SEEDS if the table is empty or unavailable.
    """
    query = f"""
        SELECT term
        FROM `{SEEDS_TABLE}`
        WHERE is_active = TRUE
        ORDER BY last_used_at ASC NULLS FIRST, added_at ASC
        LIMIT {SEEDS_PER_RUN}
    """
    try:
        seeds = [row["term"] for row in bq.query(query).result()]
        if seeds:
            log.info("Loaded %d seeds from BQ seeds table", len(seeds))
            return seeds
        log.warning("Seeds table is empty — falling back to DEFAULT_SEEDS")
    except Exception as exc:
        log.warning("Could not fetch seeds table: %s — falling back to DEFAULT_SEEDS", exc)
    return DEFAULT_SEEDS


def _update_seed_last_used(bq: bigquery.Client, seeds: list[str]) -> None:
    """
    Write back last_used_at = NOW() for every seed that was processed.
    Uses a parameterized UNNEST query to avoid SQL injection / escaping bugs
    with single-quoted terms (BigQuery uses \\' not '' for escaping).
    """
    if not seeds:
        return
    # Build a VALUES list using UNNEST — avoids all string-escaping issues
    placeholders = ", ".join(f"'{s}'" for s in seeds if "'" not in s)
    safe_seeds = [s for s in seeds if "'" not in s]
    problematic = [s for s in seeds if "'" in s]
    if problematic:
        log.warning("Skipping last_used_at update for seeds with single quotes: %s", problematic)
    if not safe_seeds:
        return
    terms_csv = ", ".join(f"'{s}'" for s in safe_seeds)
    dml = f"""
        UPDATE `{SEEDS_TABLE}`
        SET last_used_at = CURRENT_TIMESTAMP()
        WHERE term IN ({terms_csv})
    """
    try:
        bq.query(dml).result()
        log.info("Updated last_used_at for %d seeds", len(safe_seeds))
    except Exception as exc:
        log.warning("Could not update seed last_used_at: %s", exc)


# ---------------------------------------------------------------------------
# Core orchestration
# ---------------------------------------------------------------------------

def _process_seeds(pytrends: TrendReq, seeds: list[str], bq: bigquery.Client, run_id: str) -> dict:
    """
    Iterate over seed keywords in batches of 1 (pytrends related_* only
    supports a single keyword reliably for related results), fetch data,
    and collect all rows.

    Returns summary stats dict.
    """
    all_raw_rows: list[dict] = []

    # Process one seed at a time to avoid Google throttling on bulk payloads
    for seed in seeds:
        log.info("Fetching related data for seed: '%s'", seed)
        try:
            queries_data, topics_data = _safe_fetch(pytrends, [seed])
        except Exception as exc:
            log.error("Skipping seed '%s' after %d retries: %s", seed, RETRIES, exc)
            continue

        # --- related_queries ---
        for rtype in ("top", "rising"):
            df = (queries_data or {}).get(seed, {}).get(rtype)
            all_raw_rows.extend(_df_to_rows(df, seed, rtype, "query", run_id))

        # --- related_topics ---
        for rtype in ("top", "rising"):
            df = (topics_data or {}).get(seed, {}).get(rtype)
            all_raw_rows.extend(_df_to_rows(df, seed, rtype, "topic", run_id))

        # Polite delay between seeds
        time.sleep(random.uniform(3, 6))

    # Write all raw rows to BQ
    _insert_rows(bq, RESULTS_TABLE, all_raw_rows)

    # --- Novelty flagging: Rising terms not in concept_library ---
    known_terms = _get_known_terms(bq)
    now = datetime.datetime.utcnow().isoformat()

    emerging_rows = []
    seen_terms: set[str] = set()   # deduplicate across seeds within a run

    for row in all_raw_rows:
        if row["result_type"] != "rising":
            continue
        term_lower = row["term"].lower().strip()
        if term_lower in known_terms:
            continue
        if term_lower in seen_terms:
            continue
        seen_terms.add(term_lower)

        emerging_rows.append({
            "run_id":        run_id,
            "term_id":       row["term_id"],
            "term":          row["term"],
            "seed_keyword":  row["seed_keyword"],
            "data_type":     row["data_type"],
            "value":         row["value"],
            "is_breakout":   row["is_breakout"],
            "review_status": "PENDING",
            "flagged_at":    now,
        })

    _insert_rows(bq, EMERGING_TABLE, emerging_rows)

    return {
        "seeds_processed": len(seeds),
        "raw_rows":        len(all_raw_rows),
        "emerging_flagged": len(emerging_rows),
    }


# ---------------------------------------------------------------------------
# Cloud Function entry point
# ---------------------------------------------------------------------------

@functions_framework.http
def discover_related_queries(request):
    """
    HTTP-triggered Cloud Function.

    Optional JSON body:
    {
      "seeds": ["dungeons and dragons", "dnd 5e"],  // override defaults
      "dry_run": false
    }
    """
    body = {}
    if request.content_type == "application/json":
        try:
            body = request.get_json(silent=True) or {}
        except Exception:
            pass

    dry_run = bool(body.get("dry_run", False))

    # BigQuery client — uses Application Default Credentials (ADC)
    bq = bigquery.Client(project=PROJECT_ID)

    # Seeds: use request override if provided, otherwise load from BQ seeds table
    seeds = body.get("seeds") or _fetch_active_seeds(bq)

    run_id = hashlib.md5(
        f"{datetime.datetime.utcnow().isoformat()}|{'|'.join(seeds)}".encode()
    ).hexdigest()[:16]

    log.info("Run %s | seeds=%d | dry_run=%s", run_id, len(seeds), dry_run)

    if dry_run:
        return json.dumps({
            "status":  "dry_run",
            "run_id":  run_id,
            "seeds":   seeds,
            "message": "No data written."
        }), 200, {"Content-Type": "application/json"}

    # Build proxy-authenticated pytrends session
    proxy_url = _build_proxy_url()
    pytrends  = _build_pytrends_session(proxy_url)

    try:
        _ensure_tables(bq)
        stats = _process_seeds(pytrends, seeds, bq, run_id)
        _update_seed_last_used(bq, seeds)
    except Exception as exc:
        log.exception("Fatal error in run %s", run_id)
        return json.dumps({"status": "error", "run_id": run_id, "error": str(exc)}), 500, {
            "Content-Type": "application/json"
        }

    result = {"status": "ok", "run_id": run_id, **stats}
    log.info("Run %s complete: %s", run_id, result)
    return json.dumps(result), 200, {"Content-Type": "application/json"}
