# deploy_itchio.ps1
$PROJECT_ID = "dnd-trends-index"
$REGION = "us-central1"
$IMAGE_NAME = "gcr.io/$PROJECT_ID/itchio-rss-harvester"
$JOB_NAME = "itchio-rss-harvester"
$SERVICE_ACCOUNT = "antigravity-turbo-agent@dnd-trends-index.iam.gserviceaccount.com"

Write-Host "🚀 Starting Deployment for $JOB_NAME (Project: $PROJECT_ID)..." -ForegroundColor Cyan

# 1. Build Container (Cloud Build)
Write-Host "`n📦 Building Container Image..." -ForegroundColor Yellow
$buildCmd = "gcloud builds submit --tag $IMAGE_NAME harvesters/itchio_rss --project $PROJECT_ID"
Invoke-Expression $buildCmd
if ($LASTEXITCODE -ne 0) { Write-Error "Build failed!"; exit 1 }

# 2. Deploy Cloud Run Job
Write-Host "`n☁️  Deploying Cloud Run Job..." -ForegroundColor Yellow
gcloud run jobs create $JOB_NAME --image $IMAGE_NAME --region $REGION --project $PROJECT_ID --max-retries 0 --service-account $SERVICE_ACCOUNT
if ($LASTEXITCODE -ne 0) {
    Write-Host "Job might already exist. Updating..." -ForegroundColor Yellow
    gcloud run jobs update $JOB_NAME --image $IMAGE_NAME --region $REGION
}

Write-Host "`n✅ Deployment Complete!" -ForegroundColor Green
