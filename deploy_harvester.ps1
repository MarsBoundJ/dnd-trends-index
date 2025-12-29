# deploy_harvester.ps1
# Automates the deployment of the Reddit Harvester to Google Cloud Run

$PROJECT_ID = "dnd-trends-index"
$REGION = "us-central1"
$IMAGE_NAME = "gcr.io/$PROJECT_ID/reddit-harvester"
$JOB_NAME = "reddit-harvester-job"

Write-Host "🚀 Starting Deployment for $JOB_NAME (Project: $PROJECT_ID)..." -ForegroundColor Cyan

# 0. Check Auth
Write-Host "Checking gcloud auth..."
gcloud auth list --filter=status:ACTIVE --format="value(account)"

# 1. Build Container (Cloud Build)
Write-Host "`n📦 Building Container Image..." -ForegroundColor Yellow
$buildCmd = "gcloud builds submit --tag $IMAGE_NAME --project $PROJECT_ID"
Invoke-Expression $buildCmd
if ($LASTEXITCODE -ne 0) { Write-Error "Build failed!"; exit 1 }

# 2. Deploy Cloud Run Job
Write-Host "`n☁️  Deploying Cloud Run Job..." -ForegroundColor Yellow

$REDDIT_CLIENT_ID = "HGo2aTjVymaMre8kFMs9PA"
$REDDIT_CLIENT_SECRET = "I-n58I_Ai3FWr08VK1TElErMLep6Sg"
$REDDIT_USER_AGENT = "win:dnd-trends-tracker:v1.0 (by /u/Short_Let7420)"

# Constructing the Env Vars string carefully
$ENV_VARS = "REDDIT_CLIENT_ID=$REDDIT_CLIENT_ID,REDDIT_CLIENT_SECRET=$REDDIT_CLIENT_SECRET,REDDIT_USER_AGENT=$REDDIT_USER_AGENT"

# Try Create First
Write-Host "Attempting to create job..."
gcloud run jobs create $JOB_NAME --image $IMAGE_NAME --region $REGION --project $PROJECT_ID --set-env-vars $ENV_VARS --max-retries 0 --task-timeout 5m

if ($LASTEXITCODE -ne 0) {
    Write-Host "Job might already exist. Updating..." -ForegroundColor Yellow
    gcloud run jobs update $JOB_NAME --image $IMAGE_NAME --region $REGION --set-env-vars $ENV_VARS
}

Write-Host "`n✅ Deployment Complete!" -ForegroundColor Green
Write-Host "To run the job manually: gcloud run jobs execute $JOB_NAME --region $REGION"
