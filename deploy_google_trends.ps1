# deploy_google_trends.ps1
# Automates the deployment of the Google Trends Scraper to Google Cloud Run Jobs

$PROJECT_ID = "dnd-trends-index"
$REGION = "us-central1"
$IMAGE_NAME = "gcr.io/$PROJECT_ID/google-trends-scraper"
$JOB_NAME = "google-trends-job"
$SOURCE_DIR = "cloud_functions/google_trends_scraper"

Write-Host "🚀 Starting Deployment for $JOB_NAME (Project: $PROJECT_ID)..." -ForegroundColor Cyan

# 1. Build Container (Cloud Build)
Write-Host "`n📦 Building Container Image..." -ForegroundColor Yellow
$buildCmd = "gcloud builds submit $SOURCE_DIR --tag $IMAGE_NAME --project $PROJECT_ID"
Invoke-Expression $buildCmd
if ($LASTEXITCODE -ne 0) { Write-Error "Build failed!"; exit 1 }

# 2. Deploy Cloud Run Job
Write-Host "`n☁️  Deploying Cloud Run Job..." -ForegroundColor Yellow

# Try Create First
Write-Host "Attempting to create job..."
gcloud run jobs create $JOB_NAME --image $IMAGE_NAME --region $REGION --project $PROJECT_ID --max-retries 0 --task-timeout 2h --memory 2Gi

if ($LASTEXITCODE -ne 0) {
    Write-Host "Job might already exist. Updating..." -ForegroundColor Yellow
    gcloud run jobs update $JOB_NAME --image $IMAGE_NAME --region $REGION --task-timeout 2h --memory 2Gi
}

Write-Host "`n✅ Deployment Complete!" -ForegroundColor Green
Write-Host "To run the job manually: gcloud run jobs execute $JOB_NAME --region $REGION"
