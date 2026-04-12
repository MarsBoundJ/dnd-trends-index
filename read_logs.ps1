$now = (Get-Date).ToUniversalTime().AddMinutes(-20).ToString("yyyy-MM-ddTHH:mm:ssZ")
$filter = "resource.type=`"cloud_run_revision`" AND timestamp >= `"$now`" AND resource.labels.service_name=`"discover-related-queries`""
gcloud logging read $filter --limit=50 --project=dnd-trends-index --format="value(textPayload, severity, timestamp)"
