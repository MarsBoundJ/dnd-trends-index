# D&D Trends Index - Project Architecture

## 1. Environment & Infrastructure
*   **IDE:** Antigravity (VS Code) running in **Turbo Mode**.
*   **Container:** Docker DevContainer (Python 3.11).
    *   **OS:** Debian Trixie (Slim).
    *   **Tools:** `gcloud` CLI, `bq` (BigQuery) CLI, Python, Git.
*   **Security:** 
    *   **Service Account:** Authenticated via `dnd-key.json` (Scoped to `dnd-trends-index`).
    *   **Git:** Code is version controlled.
    *   **Data:** Backups via `utils/backup_bigquery.py`.

## 2. Cloud Resources (GCP)
*   **Project ID:** `dnd-trends-index`
*   **Data Warehouse:** BigQuery
    *   **Datasets:** `commercial_data`, `gold_data`, `silver_data`, `dnd_trends_raw`, `dnd_trends_categorized`, `social_data`.
    *   **Note:** `gold_data` and `silver_data` contain Views (Virtual Tables) which cannot be exported directly to CSV.

## 3. Key Scripts
| Script | Location | Purpose |
| :--- | :--- | :--- |
| **Backup Script** | `utils/backup_bigquery.py` | Exports all real tables to Cloud Storage (JSON format). Ignores Views. |
| **Dev Config** | `.devcontainer/devcontainer.json` | Defines the Docker build, installs `gcloud`, and auto-authenticates on launch. |

## 4. Operational Protocols (Turbo Mode)
*   **Before Destructive Actions:** Always run `python3 utils/backup_bigquery.py`.
*   **Credential Path:** In the container, use `/app/dnd-key.json` (mapped from host).
*   **Permissions:** If report generation fails with `Permission denied`, run `chmod 666` as root.
*   **Data Safety:** BigQuery "Time Travel" allows restoring tables deleted within 7 days.
*   **Repo Safety:** `.gitignore` must always include `dnd-key.json` and `.env`.

## 5. Current Workflow
1.  Launch Antigravity -> "Reopen in Container".
2.  Verify Hostname (ensure it's not `LAPTOP-XXX`).
3.  Agent has "Always Proceed" permission for coding and terminal commands.

## 6. Taxonomy Definitions: Hot Concepts
*   **Purpose:** To track high-volume D&D search terms that do not fit strictly within standard game categories (Class, Monster, etc.).
*   **Witch (Hot Concepts):** Uses the `Witch Dnd` variation to capture broader community interest in the archetype, separate from specific homebrew class builds.
*   **Active Search:** We periodically scan for and add trending concepts outside the core taxonomy to this category to maintain a baseline for cultural relevance.
