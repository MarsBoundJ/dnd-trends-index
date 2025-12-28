Á
import subprocess
import os

PROJECT_ID = "dnd-trends-index"
REGION = "us-central1"

WORKFLOWS = {
    "dnd-fast-lane": {
        "source": "workflow_fast_lane.yaml",
        "schedule": "0 6 * * *", # Daily @ 6 AM UTC
        "desc": "Daily D&D Trends Scrapers (Reddit, Fandom, YouTube)"
    },
    "dnd-slow-lane": {
        "source": "workflow_slow_lane.yaml",
        "schedule": "0 0 * * 0", # Sundays @ Midnight UTC
        "desc": "Weekly Commercial Scrapers (Kickstarter, Roll20, RPGGeek)"
    }
}

def run_cmd(cmd):
    print(f"Running: {cmd}")
    try:
        subprocess.run(cmd, shell=True, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error: {e}")

def deploy():
    print(f"Deploying Orchestration to Project: {PROJECT_ID}")
    
    for name, config in WORKFLOWS.items():
        print(f"\n--- Deploying {name} ---")
        
        # 1. Deploy Workflow
        cmd_deploy = f"gcloud workflows deploy {name} --source={config['source']} --location={REGION} --description='{config['desc']}'"
        run_cmd(cmd_deploy)
        
        # 2. Creates Cloud Scheduler Job
        # Note: We assume the Service Account exists
        job_name = f"{name}-trigger"
        cmd_schedule = f"gcloud scheduler jobs create http {job_name} --schedule='{config['schedule']}' --uri='https://workflowexecutions.googleapis.com/v1/projects/{PROJECT_ID}/locations/{REGION}/workflows/{name}/executions' --oauth-service-account-email='workflows-sa@{PROJECT_ID}.iam.gserviceaccount.com' --location={REGION}"
        
        # Try to create, if exists, update? (Scheduler create fails if exists)
        # We'll just run create and catch error visually
        print(f"Creating Schedule: {config['schedule']}")
        run_cmd(cmd_schedule)

if __name__ == "__main__":
    deploy()
Á*cascade08"(f973a887b13d985b6bb9f24c433c2ee4d5a88d0426file:///C:/Users/Yorri/.gemini/deploy_orchestration.py:file:///C:/Users/Yorri/.gemini