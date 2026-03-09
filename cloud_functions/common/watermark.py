from google.cloud import storage
import datetime
import json
import os

BUCKET_NAME = "antigravity-metadata-dnd-trends"

class HighWatermark:
    def __init__(self, job_name):
        self.job_name = job_name
        self.storage_client = storage.Client()
        self.bucket = self.storage_client.bucket(BUCKET_NAME)
        self.blob_name = f"last_scrape_marker_{job_name}.json"
        self.blob = self.bucket.blob(self.blob_name)

    def get_range(self, default_lookback_hours=24):
        """
        Returns (start_time_iso, end_time_iso)
        start_time_iso: The last successful run time OR (now - default_lookback)
        end_time_iso: Current UTC time
        """
        end_time = datetime.datetime.now(datetime.timezone.utc)
        
        try:
            if self.blob.exists():
                data = json.loads(self.blob.download_as_text())
                start_time_str = data.get('last_successful_timestamp')
                # Parse to ensure valid, but return string for ISO compatibility
                # start_time = datetime.datetime.fromisoformat(start_time_str)
                start_time = start_time_str
                print(f"🌊 High Watermark Found: Resuming from {start_time}")
            else:
                raise FileNotFoundError("Marker not found")
        except Exception as e:
            print(f"⚠️ No watermark found ({e}). Using default lookback of {default_lookback_hours}h.")
            start_dt = end_time - datetime.timedelta(hours=default_lookback_hours)
            start_time = start_dt.isoformat()

        return start_time, end_time.isoformat()

    def update_marker(self, timestamp_iso):
        """
        Updates the marker with the successful completion timestamp.
        """
        try:
            payload = json.dumps({
                "last_successful_timestamp": timestamp_iso,
                "updated_at": datetime.datetime.now(datetime.timezone.utc).isoformat()
            })
            self.blob.upload_from_string(payload)
            print(f"✅ High Watermark Updated to {timestamp_iso}")
        except Exception as e:
            print(f"❌ Failed to update watermark: {e}")
