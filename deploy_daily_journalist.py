"""Deploy the Daily Journalist (Council edition) Cloud Function.

Redeploys the existing `dnd-daily-journalist` gen2 function with the Council
refactor. The function URL is preserved so the existing scheduler job
(`trigger-daily-journalist`) keeps firing against it.

Request body controls mode:
    {}                        -> {"mode": "council"}  (default, single article)
    {"mode": "legacy"}        -> legacy trio
    {"mode": "both"}          -> Council + legacy trio (parallel-run window)
    {"mode": "council", "writer": "bursar"}  -> force a specific Council member
"""

import subprocess

PROJECT_ID = "dnd-trends-index"
REGION = "us-central1"
FUNCTION_NAME = "dnd-daily-journalist"
SOURCE_DIR = "./cloud_functions/daily_journalist"
ENTRY_POINT = "generate_article"


def deploy() -> None:
    print(f"Deploying Cloud Function: {FUNCTION_NAME}...")
    cmd = (
        f"gcloud functions deploy {FUNCTION_NAME} "
        f"--gen2 "
        f"--runtime=python311 "
        f"--region={REGION} "
        f"--source={SOURCE_DIR} "
        f"--entry-point={ENTRY_POINT} "
        f"--trigger-http "
        f"--allow-unauthenticated "
        f"--memory=512Mi "
        f"--timeout=540s "
        f"--project={PROJECT_ID}"
    )
    print(f"Running: {cmd}")
    subprocess.run(cmd, shell=True, check=True)

    url_cmd = (
        f"gcloud functions describe {FUNCTION_NAME} --gen2 "
        f"--region={REGION} --format='value(serviceConfig.uri)' "
        f"--project={PROJECT_ID}"
    )
    result = subprocess.run(url_cmd, shell=True, capture_output=True, text=True)
    print(f"Function URL: {result.stdout.strip()}")


if __name__ == "__main__":
    deploy()
