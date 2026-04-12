$TOKEN = gcloud auth print-identity-token
curl.exe -s -X POST "https://discover-related-queries-kfh5mgjgiq-uc.a.run.app" `
  -H "Authorization: Bearer $TOKEN" `
  -H "Content-Type: application/json" `
  -d '{\"seeds\": [\"dungeons and dragons\"]}' `
  --max-time 120

gcloud functions logs read discover-related-queries --region=us-central1 --project=dnd-trends-index --limit=5 --gen2
