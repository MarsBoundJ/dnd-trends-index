"""Deploy the Freightos FBX harvester Cloud Function.

First-time deploy. After success, wire Cloud Scheduler to POST this URL
weekly on Saturday 22:00 America/Chicago (see step 9a plan).
"""

import subprocess

PROJECT_ID = "dnd-trends-index"
REGION = "us-central1"
FUNCTION_NAME = "freight-index-harvester"
SOURCE_DIR = "./cloud_functions/freight_index_harvester"
ENTRY_POINT = "freight_index_harvester"


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
        f"--memory=256Mi "
        f"--timeout=120s "
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
