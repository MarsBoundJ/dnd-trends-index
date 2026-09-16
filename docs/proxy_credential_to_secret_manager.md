# Moving the Webshare proxy credential into Secret Manager

**Written Sep 15, 2026.** All findings below were measured, not assumed.

## What is exposed, and where

The Webshare proxy credentials sit in **four live resources**, in **two
different shapes**. All four were found by parsing config directly; a first
sweep using a `--format` path returned "none" everywhere including two
known-positive jobs, so it was discarded. **Any sweep here needs a control: run
it against a resource you already know holds the value, and distrust the result
if that comes back clean.**

| Resource | Kind | Shape | Service account |
|---|---|---|---|
| `google-trends-job` | Cloud Run job, daily ~02:2x | `PROXY_URL` | default compute |
| `itchio-rss-harvester` | Cloud Run job, daily ~04:01 | `PROXY_URL` | `antigravity-turbo-agent@` |
| `bgg-harvester` | Cloud Function, request-driven | `PROXY_URL` | default compute |
| `discover-related-queries` | Cloud Function, request-driven | **4 separate vars** | `antigravity-turbo-agent@` |

The three `PROXY_URL` holders carry **byte-identical values** (compared by
SHA-256 digest, never printing them). Both functions are live — each served
requests within the last 30 days.

The `google-trends-scraper` Cloud Function, a fifth holder, was deleted Sep 14.

### The fourth one is shaped differently and needs its own decision

`discover-related-queries` does not store a URL. It stores
`WEBSHARE_PROXY_HOST`, `WEBSHARE_PROXY_PORT`, `WEBSHARE_PROXY_USER` and
`WEBSHARE_PROXY_PASS` as four separate variables. A single `PROXY_URL` secret
cannot be swapped in without a code change.

**DECIDED Sep 16, 2026 by Phil: four secrets.** No code change, so the migration
stays purely about *where* the credential lives. Five secret objects in total:
one `webshare-proxy-url` for the three URL holders, plus four components for
this function.

(The rejected alternative was one secret plus a code change to parse the URL.
Fewer objects to rotate, but it mixes a behaviour change into a live function
during a credential migration.)

Only `WEBSHARE_PROXY_PASS` is truly sensitive. Host, port and user are low-risk
on their own but identify the account, so they are worth moving together.

### Rotation now has a blast radius

Rotating at webshare.io **invalidates the credential for all four at once.**
Update all four in the same sitting, or the stragglers break.

Timing matters because two are scheduled: Trends at ~02:2x UTC and itch.io at
~04:01 UTC. **Do this well clear of that window** — ideally just after both have
run — so a half-finished migration cannot collide with a scheduled execution.

## There is already a secret, and it is stale

`pytrends-proxy-creds` exists, created **2025-11-02**. Do not assume it is the
same credential — it is not:

- Compared by SHA-256 digest (never printing either value): **they differ.**
- `get-iam-policy` returns no bindings, so nothing has been granted access to it.
- The name is now a misnomer twice over: pytrends is no longer the collector
  (Playwright is), and `itchio-rss-harvester` uses the same proxy.

So it is stale, unused, and misleadingly named. Replace rather than reuse.

## Rotate first, then migrate

The live value was printed into a session transcript on Sep 14 while reading the
function config. Treat it as disclosed.

Rotating **first** is also what makes the migration clean: the new credential
goes straight into Secret Manager and never exists as a plaintext env var at any
point. Migrating first and rotating later would mean handling the exposed value
twice for no benefit.

## No code change for three of the four

Cloud Run and Cloud Functions both inject a secret as an ordinary environment
variable, so `os.environ.get("PROXY_URL")` keeps working untouched in
`browser_trends.py`, the itch.io harvester and `bgg-harvester`.

`discover-related-queries` is the exception — it reads four separate variables,
so whether it needs a code change depends on which option above is chosen.

**The two failure modes are OPPOSITE, and an earlier version of this document
got one of them wrong.**

`browser_trends.py` refuses to start when `PROXY_URL` is unset — "Refusing to
run unproxied". A misconfigured secret there fails **loudly** on the next
scheduled run. That is the failure mode you want.

`discover-related-queries` does the reverse. `main.py:49-52` gives every proxy
variable a **default** (`p.webshare.io`, port `80`, empty user and pass), and
line 79 builds a proxy URL only `if PROXY_USER and PROXY_PASS`. With those
empty, `proxies_list` is `[]`, no proxy is attached, and the function **scrapes
Google Trends directly from the raw GCP IP without raising anything.**

So a botched secret mount on this one is **silent**. Do not read the absence of
errors as success — verify the config explicitly (step 5).

## Steps

Run from anywhere; these need no particular working directory. **Claude cannot
run these** — `gcloud` mutations are blocked by the auto-mode classifier.

**1. Rotate at webshare.io.** Generate new proxy credentials and keep the new
URL on the clipboard. Do not paste it into a shell as an argument.

**2. Create the five secrets:** `webshare-proxy-url` (the full
`http://USER:PASS@HOST:PORT`), plus `webshare-proxy-host`, `-port`, `-user` and
`-pass`.

**Preferred: the Cloud Console**, Secret Manager -> Create Secret. The value is
typed into a form field, so it never reaches shell history, scrollback or a temp
file — cleaner than any terminal route.

By CLI instead, read from stdin so the value is never an argument:

```
gcloud secrets create webshare-proxy-url --project=dnd-trends-index --replication-policy=automatic --data-file=-
```

Paste the value, press **Enter**, then **Ctrl+Z** and **Enter** again to close
stdin on Windows. (A trailing newline is fine; the readers strip it.)

**3. Grant read access.** `webshare-proxy-url` is read by BOTH service accounts
(compute runs google-trends-job and bgg-harvester; antigravity-turbo-agent runs
itchio-rss-harvester). The four component secrets are read ONLY by
antigravity-turbo-agent, so scope them narrowly — PowerShell:

```
foreach ($s in "host","port","user","pass") { gcloud secrets add-iam-policy-binding "webshare-proxy-$s" --project=dnd-trends-index --member="serviceAccount:antigravity-turbo-agent@dnd-trends-index.iam.gserviceaccount.com" --role="roles/secretmanager.secretAccessor" }
```

And for the URL secret, two separate grants:

```
gcloud secrets add-iam-policy-binding webshare-proxy-url --project=dnd-trends-index --member="serviceAccount:187467566422-compute@developer.gserviceaccount.com" --role="roles/secretmanager.secretAccessor"
```

```
gcloud secrets add-iam-policy-binding webshare-proxy-url --project=dnd-trends-index --member="serviceAccount:antigravity-turbo-agent@dnd-trends-index.iam.gserviceaccount.com" --role="roles/secretmanager.secretAccessor"
```

**4. Point each resource at the secret and drop the plaintext variable.**

The two Cloud Run jobs:

```
gcloud run jobs update google-trends-job --region=us-central1 --project=dnd-trends-index --remove-env-vars=PROXY_URL --update-secrets=PROXY_URL=webshare-proxy-url:latest
```

```
gcloud run jobs update itchio-rss-harvester --region=us-central1 --project=dnd-trends-index --remove-env-vars=PROXY_URL --update-secrets=PROXY_URL=webshare-proxy-url:latest
```

The two Cloud Functions. **Do not use `gcloud functions deploy` here.**

Both are GEN_2, so each is backed by a Cloud Run service of the same name, and
`gcloud run services update` rewires the environment **without a rebuild** —
instant, and rollback is a revision swap.

`gcloud functions deploy` would rebuild from source, and its `--source` default
is a trap. From gcloud's own help: *"If you do not specify the --source flag ...
if the function was previously deployed using a local filesystem path, then the
function's source code will be updated using the current directory."* Run from
the repo root, that replaces the function's code with the repo root. Same family
as the worktree trap in [[feedback_gcloud_deploy_cwd]]. Avoiding the rebuild
matters independently: re-resolving dependencies is exactly how #138 broke the
live Trends stream.

```
gcloud run services update bgg-harvester --region=us-central1 --project=dnd-trends-index --remove-env-vars=PROXY_URL --update-secrets=PROXY_URL=webshare-proxy-url:latest
```

```
gcloud run services update discover-related-queries --region=us-central1 --project=dnd-trends-index --remove-env-vars=WEBSHARE_PROXY_HOST,WEBSHARE_PROXY_PORT,WEBSHARE_PROXY_USER,WEBSHARE_PROXY_PASS --update-secrets=WEBSHARE_PROXY_HOST=webshare-proxy-host:latest,WEBSHARE_PROXY_PORT=webshare-proxy-port:latest,WEBSHARE_PROXY_USER=webshare-proxy-user:latest,WEBSHARE_PROXY_PASS=webshare-proxy-pass:latest
```

**Rollback targets, captured Sep 16 18:35 UTC before any change:**
`bgg-harvester-00005-897` and `discover-related-queries-00054-gam`. Roll back with
`gcloud run services update-traffic <service> --region=us-central1 --to-revisions=<revision>=100`.

If gcloud objects to removing and setting the same key in one call, split it:
run with `--remove-env-vars=PROXY_URL` first, then again with
`--update-secrets=...`.

**5. Verify the wiring**, which Claude *can* do — `describe` is read-only. The
env entry should report as a secret reference rather than a literal value.

Then let the scheduled runs confirm it functionally: Trends at ~02:2x, itch.io at
~04:01. Both write to BigQuery, so a successful run is visible in the data.

## After it works

Delete the stale secret — destructive, so it is a deliberate step:

```
gcloud secrets delete pytrends-proxy-creds --project=dnd-trends-index
```

Confirm first that nothing references it. It had no IAM bindings as of Sep 15,
which is strong evidence nothing can read it, let alone does.
