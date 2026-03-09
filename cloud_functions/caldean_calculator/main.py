import os
import datetime
import json
import requests
import dateutil.parser
from flask import Flask, request
from google.cloud import scheduler_v1
from google.protobuf import field_mask_pb2

app = Flask(__name__)

# Configuration
ZIP_CODE = "51501" # Council Bluffs, Iowa
PROJECT_ID = "dnd-trends-index"
LOCATION = "us-central1"
WORKFLOW_NAME = "dnd-fast-lane"
MASTER_JOB_NAME = "caldean-master-trigger"
URI = f"https://workflowexecutions.googleapis.com/v1/projects/{PROJECT_ID}/locations/{LOCATION}/workflows/{WORKFLOW_NAME}/executions"
SERVICE_ACCOUNT = "antigravity-turbo-agent@dnd-trends-index.iam.gserviceaccount.com"

# Mercury Hour Mapping (1-indexed hour of the day)
MERCURY_MAP = {
    6: 4, # Sunday
    0: 7, # Monday
    1: 3, # Tuesday
    2: 1, # Wednesday
    3: 5, # Thursday
    4: 2  # Friday
}

def get_zmanim_data(target_date_str):
    url = f"https://www.hebcal.com/zmanim?cfg=json&zip={ZIP_CODE}&date={target_date_str}&havdalah=8.5"
    r = requests.get(url)
    r.raise_for_status()
    data = r.json()
    return data.get('times', data)

@app.route("/", methods=["POST"])
def main_handler():
    """
    Singleton Clockmaker: Updates the master trigger for Today's specific esoteric hour.
    """
    print("🚀 Execution Started: Flask main_handler")
    
    client = scheduler_v1.CloudSchedulerClient()
    parent = f"projects/{PROJECT_ID}/locations/{LOCATION}"
    full_job_path = f"{parent}/jobs/{MASTER_JOB_NAME}"
    
    today = datetime.date.today()
    date_str = today.isoformat()
    weekday = today.weekday()
    
    try:
        day_data = get_zmanim_data(date_str)
        sunrise_str = day_data.get('sunrise') or day_data.get('hanetzMishor')
        sunset_str = day_data.get('sunset') or day_data.get('shkiah')
        
        if not sunrise_str or not sunset_str:
            return f"Error: Missing sunrise/sunset for {date_str}", 500
            
        sunrise = dateutil.parser.parse(sunrise_str)
        sunset = dateutil.parser.parse(sunset_str)
        
        trigger_dt = None
        reason = "mercury_hour"
        
        if weekday == 5: # Saturday
            if 'havdalah' in day_data:
                trigger_dt = dateutil.parser.parse(day_data['havdalah'])
                reason = "shabbat_havdalah"
        else:
            day_len_seconds = (sunset - sunrise).total_seconds()
            shaah_zmanit = day_len_seconds / 12
            target_hour_index = MERCURY_MAP.get(weekday)
            if target_hour_index:
                offset_seconds = (target_hour_index - 1) * shaah_zmanit
                trigger_dt = sunrise + datetime.timedelta(seconds=offset_seconds)
        
        if not trigger_dt:
            return f"Error: Could not calculate trigger for {date_str}", 500

        # Convert to UTC Cron
        dt_utc = trigger_dt.astimezone(datetime.timezone.utc)
        cron_expr = f"{dt_utc.minute} {dt_utc.hour} {dt_utc.day} {dt_utc.month} *"
        
        job = {
            "name": full_job_path,
            "http_target": {
                "uri": URI,
                "http_method": scheduler_v1.HttpMethod.POST,
                "oidc_token": {
                    "service_account_email": SERVICE_ACCOUNT
                },
                "body": b'{"argument": "null"}'
            },
            "schedule": cron_expr,
            "time_zone": "Etc/UTC"
        }
        
        # Update Singleton
        mask = field_mask_pb2.FieldMask(paths=['schedule', 'http_target'])
        client.update_job(job=job, update_mask=mask)
        
        audit_log = {
            "event": "schedule_update",
            "date": date_str,
            "new_cron": cron_expr,
            "trigger_utc": dt_utc.isoformat(),
            "reason": reason
        }
        print(json.dumps(audit_log))
        
        return json.dumps(audit_log), 200

    except Exception as e:
        error_msg = f"CRITICAL ERROR: {str(e)}"
        print(error_msg)
        return error_msg, 500

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
