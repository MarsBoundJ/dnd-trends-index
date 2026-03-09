from google.cloud import scheduler_v1
import os

PROJECT_ID = "dnd-trends-index"
LOCATION = "us-central1"
SERVICE_ACCOUNT_TRIGGER = "antigravity-turbo-agent@dnd-trends-index.iam.gserviceaccount.com"
SERVICE_ACCOUNT_WORKFLOW = "antigravity-turbo-agent@dnd-trends-index.iam.gserviceaccount.com"

client = scheduler_v1.CloudSchedulerClient()
parent = f"projects/{PROJECT_ID}/locations/{LOCATION}"

def create_or_update_job(job_name, schedule, uri, sa_email):
    full_path = f"{parent}/jobs/{job_name}"
    job = {
        "name": full_path,
        "http_target": {
            "uri": uri,
            "http_method": scheduler_v1.HttpMethod.POST,
            "oidc_token": {
                "service_account_email": sa_email
            },
            "body": b'{"argument": "null"}'
        },
        "schedule": schedule,
        "time_zone": "Etc/UTC"
    }
    
    try:
        try:
            client.get_job(name=full_path)
            client.update_job(job=job)
            print(f"✅ Updated: {job_name}")
        except Exception as e:
            if "not found" in str(e).lower():
                client.create_job(parent=parent, job=job)
                print(f"✅ Created: {job_name}")
            else:
                raise e
    except Exception as e:
        print(f"❌ Failed {job_name}: {e}")

if __name__ == "__main__":
    # Job A: Clockmaker
    create_or_update_job(
        "caldean-daily-updater",
        "0 0 * * *",
        "https://caldean-calculator-187467566422.us-central1.run.app",
        SERVICE_ACCOUNT_TRIGGER
    )
    
    # Job B: Master Trigger
    create_or_update_job(
        "caldean-master-trigger",
        "0 0 1 1 *",
        "https://workflowexecutions.googleapis.com/v1/projects/dnd-trends-index/locations/us-central1/workflows/dnd-fast-lane/executions",
        SERVICE_ACCOUNT_WORKFLOW
    )
