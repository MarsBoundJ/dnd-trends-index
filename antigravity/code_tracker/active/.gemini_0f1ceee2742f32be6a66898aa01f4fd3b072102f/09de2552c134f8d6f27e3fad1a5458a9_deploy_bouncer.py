Ü
import subprocess
import os

PROJECT_ID = "dnd-trends-index"
REGION = "us-central1"
FUNCTION_NAME = "get_trend_data"
SOURCE_DIR = "./bouncer"
ENTRY_POINT = "get_daily_trends"

def deploy():
    print(f"Deploying Cloud Function: {FUNCTION_NAME}...")
    
    # 2nd Gen Function Deployment
    # --allow-unauthenticated: Needed for GitHub Pages to call it from client-side JS
    cmd = (
        f"gcloud functions deploy {FUNCTION_NAME} "
        f"--gen2 "
        f"--runtime=python310 "
        f"--region={REGION} "
        f"--source={SOURCE_DIR} "
        f"--entry-point={ENTRY_POINT} "
        f"--trigger-http "
        f"--allow-unauthenticated "
        f"--project={PROJECT_ID}"
    )
    
    print(f"Running: {cmd}")
    try:
        subprocess.run(cmd, shell=True, check=True)
        print("Deployment successful!")
        
        # Get the URL
        cmd_url = f"gcloud functions describe {FUNCTION_NAME} --gen2 --region={REGION} --format='value(serviceConfig.uri)' --project={PROJECT_ID}"
        result = subprocess.run(cmd_url, shell=True, capture_output=True, text=True)
        url = result.stdout.strip()
        print(f"Function URL: {url}")
        
        # Save URL to a file for the React App to use
        with open("bouncer_url.txt", "w") as f:
            f.write(url)
            
    except subprocess.CalledProcessError as e:
        print(f"Error during deployment: {e}")

if __name__ == "__main__":
    deploy()
Ü"(0f1ceee2742f32be6a66898aa01f4fd3b072102f20file:///C:/Users/Yorri/.gemini/deploy_bouncer.py:file:///C:/Users/Yorri/.gemini