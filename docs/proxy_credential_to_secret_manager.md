# Moving the Webshare proxy credential into Secret Manager

**Written Sep 15, 2026.** All findings below were measured, not assumed.

## What is exposed

`PROXY_URL` holds a Webshare proxy URL with an embedded **username and password
in plaintext**, readable by anyone who can run `describe` on the resource. It
sits in **two live Cloud Run jobs**:

| Job | Runs | Service account |
|---|---|---|
| `google-trends-job` | daily ~02:2x UTC | `187467566422-compute@developer.gserviceaccount.com` (default) |
| `itchio-rss-harvester` | daily ~04:01 UTC | `antigravity-turbo-agent@dnd-trends-index.iam.gserviceaccount.com` |

**Note the service accounts differ** — the grant has to be made twice.

A third holder, the `google-trends-scraper` Cloud Function, was deleted Sep 14,
so it is no longer a concern.

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

## No code change is required

Cloud Run injects a secret as an ordinary environment variable, so
`os.environ.get("PROXY_URL")` in `browser_trends.py` keeps working untouched.

**Safety property worth knowing:** `browser_trends.py` refuses to start when
`PROXY_URL` is unset — "Refusing to run unproxied". So a misconfigured secret
fails **loudly** on the next scheduled run rather than silently scraping from the
raw GCP IP. That is the failure mode you want, and it means a mistake here is
noisy rather than quiet.

## Steps

Run from anywhere; these need no particular working directory. **Claude cannot
run these** — `gcloud` mutations are blocked by the auto-mode classifier.

**1. Rotate at webshare.io.** Generate new proxy credentials and keep the new
URL on the clipboard. Do not paste it into a shell as an argument.

**2. Create the secret, reading the value from stdin** so it never enters shell
history:

```
gcloud secrets create webshare-proxy-url --project=dnd-trends-index --replication-policy=automatic --data-file=-
```

Paste the URL, press **Enter**, then **Ctrl+Z** and **Enter** again to close
stdin on Windows. (A trailing newline is fine; the readers strip it.)

**3. Grant read access to both service accounts** — two separate grants:

```
gcloud secrets add-iam-policy-binding webshare-proxy-url --project=dnd-trends-index --member="serviceAccount:187467566422-compute@developer.gserviceaccount.com" --role="roles/secretmanager.secretAccessor"
```

```
gcloud secrets add-iam-policy-binding webshare-proxy-url --project=dnd-trends-index --member="serviceAccount:antigravity-turbo-agent@dnd-trends-index.iam.gserviceaccount.com" --role="roles/secretmanager.secretAccessor"
```

**4. Point each job at the secret and drop the plaintext variable:**

```
gcloud run jobs update google-trends-job --region=us-central1 --project=dnd-trends-index --remove-env-vars=PROXY_URL --update-secrets=PROXY_URL=webshare-proxy-url:latest
```

```
gcloud run jobs update itchio-rss-harvester --region=us-central1 --project=dnd-trends-index --remove-env-vars=PROXY_URL --update-secrets=PROXY_URL=webshare-proxy-url:latest
```

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
