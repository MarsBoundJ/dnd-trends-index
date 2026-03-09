import requests
import datetime
import dateutil.parser
import subprocess
import sys

# Configuration
ZIP_CODE = "51501" # Council Bluffs, Iowa
URI = "https://workflowexecutions.googleapis.com/v1/projects/dnd-trends-index/locations/us-central1/workflows/workflow-fast-lane/executions"
SERVICE_ACCOUNT = "antigravity-turbo-agent@dnd-trends-index.iam.gserviceaccount.com"

# Python weekday: 0=Mon, 1=Tue... 6=Sun
# Mercury Hour Mapping (1-indexed hour of the day)
MERCURY_MAP = {
    6: 4, # Sunday
    0: 7, # Monday
    1: 3, # Tuesday
    2: 1, # Wednesday (Use 1st hour)
    3: 5, # Thursday
    4: 2  # Friday
}

def get_zmanim_data(days=30):
    """
    Fetches Zmanim for the next N days.
    """
    now = datetime.datetime.now()
    manifest_times = {}
    
    for i in range(days):
        target_date = now + datetime.timedelta(days=i)
        date_str = target_date.strftime("%Y-%m-%d")
        
        # HebCal Zmanim API for specific date
        url = f"https://www.hebcal.com/zmanim?cfg=json&zip={ZIP_CODE}&date={date_str}&havdalah=8.5"
        
        print(f"Fetching Zmanim for {date_str}...")
        try:
            r = requests.get(url)
            r.raise_for_status()
            data = r.json()
            # If the response has a 'times' key, use it. If it IS the time object, use it.
            # Usually zmanim JSON has root keys like 'times', 'location', 'date'.
            if 'times' in data:
                manifest_times[date_str] = data['times']
            else:
                 # Fallback if structure is flat
                 manifest_times[date_str] = data
        except Exception as e:
            print(f"HebCal API Error for {date_str}: {e}")
            
    # Return structure matching what calculate_schedule expects: {'times': {...}}
    return {'times': manifest_times}

def calculate_schedule(manifest, days_to_provision=30):
    """
    Calculates specific trigger times for the next N days.
    """
    triggers = []
    now = datetime.datetime.now()

    # The manifest structure for 'zmanim' endpoint usually has "times": {"YYYY-MM-DD": {...}}
    if not manifest.get('times'):
        print(f"Debug: 'times' key missing. Keys found: {list(manifest.keys())}")
        return []
        
    times_map = manifest['times']
    print(f"Debug: Found {len(times_map)} days in manifest. Date range example: {list(times_map.keys())[:3]}")

    for i in range(days_to_provision):
        target_date = now + datetime.timedelta(days=i)
        date_str = target_date.strftime("%Y-%m-%d")
        
        if date_str not in times_map:
            print(f"Debug: {date_str} not found in manifest.")
            continue
            
        day_data = times_map[date_str]
        
        # KEY TIMES (ISO Strings)
        # Debug keys
        # print(f"Keys for {date_str}: {list(day_data.keys())}")
        
        try:
            # Try English keys first, then Hebrew
            sunrise_str = day_data.get('sunrise') or day_data.get('hanetzMishor') or day_data.get('hanetzHachamah')
            sunset_str = day_data.get('sunset') or day_data.get('shkiah')
            
            if not sunrise_str or not sunset_str:
                print(f"Skipping {date_str}: keys missing. Available: {list(day_data.keys())}")
                continue
                
            sunrise = dateutil.parser.parse(sunrise_str)
            sunset = dateutil.parser.parse(sunset_str)
        except Exception as e:
            print(f"Skipping {date_str}: Error parsing {e}")
            continue

        weekday = target_date.weekday()
        
        trigger_dt = None
        job_type = "mercury"
        
        if weekday == 5: # Saturday
            # Shabbat Logic: Run at Havdalah
            if 'havdalah' in day_data:
                 trigger_dt = dateutil.parser.parse(day_data['havdalah'])
                 job_type = "shabbat-catchup"
                 print(f"[{date_str}] Shabbat: Havdalah at {trigger_dt.time()}")
            else:
                 print(f"[{date_str}] Shabbat: No Havdalah found! Skipping.")
        else:
            # Chaldean Logic: Hour of Mercury
            # Calculate Sha'ah Zmanit (Proportional Hour)
            # Total daylight seconds / 12
            day_len_seconds = (sunset - sunrise).total_seconds()
            shaah_zmanit = day_len_seconds / 12
            
            target_hour_index = MERCURY_MAP.get(weekday)
            if target_hour_index:
                # 1st Hour is (Index 1) => Sunrise + 0 * shaah
                # Nth Hour is => Sunrise + (N-1) * shaah
                offset_seconds = (target_hour_index - 1) * shaah_zmanit
                trigger_dt = sunrise + datetime.timedelta(seconds=offset_seconds)
                print(f"[{date_str}] {target_date.strftime('%A')}: Mercury Hour ({target_hour_index}) at {trigger_dt.time()}")
        
        if trigger_dt:
            # Check if this time is in the past (if running today)
            # if trigger_dt < datetime.datetime.now(datetime.timezone.utc):
            #     continue # Skip past
            
            triggers.append({
                "date_str": date_str,
                "trigger_dt": trigger_dt,
                "type": job_type
            })
            
    return triggers

def provision_cloud_scheduler(triggers):
    """
    Creates/Updates Cloud Scheduler jobs for each trigger.
    Naming convention: scrape-YYYY-MM-DD
    """
    print(f"\nProvisioning {len(triggers)} jobs...")
    
    for t in triggers:
        dt = t['trigger_dt']
        job_name = f"scrape-{t['date_str']}"
        
        # Cron: Minute Hour Day Month *
        # Note: dt is timezone aware (HebCal returns ISO with offset).
        # Cloud Scheduler 'schedule' flag expects time in the specified --time-zone.
        # If we use UTC, we must convert.
        dt_utc = dt.astimezone(datetime.timezone.utc)
        cron_expr = f"{dt_utc.minute} {dt_utc.hour} {dt_utc.day} {dt_utc.month} *"
        
        # Payload: Include 'lookback' for Shabbat catch-up
        is_catchup = (t['type'] == "shabbat-catchup")
        # Workflow expects {"argument": "..."}. We can pass JSON string.
        # Actually our workflow ignores args currently but HighWatermark handles it. 
        # But user requested payload: {"lookback": "26h"}
        
        cmd = [
            "gcloud", "scheduler", "jobs", "create", "http", job_name,
            f"--schedule={cron_expr}",
            f"--uri={URI}",
            "--message-body={\"argument\": \"null\"}",
            f"--oauth-service-account-email={SERVICE_ACCOUNT}",
            "--time-zone=Etc/UTC",
            "--location=us-central1"
        ]
        
        # Try Create, if exists Update
        try:
            # Check existence first to act cleaner? 
            # Or just run update if create fails.
            # 'create' is noisy if exists.
            
            # Let's try UPDATE first (if exists), else CREATE.
            # But UPDATE requires the job to exist.
            # Reverse: Try CREATE, catch AlreadyExists, then UPDATE.
            subprocess.run(cmd, check=True, stderr=subprocess.PIPE)
            print(f"✅ Created {job_name} for {dt_utc}")
        except subprocess.CalledProcessError as e:
            if b"ALREADY_EXISTS" in e.stderr:
                # Update logic
                cmd[3] = "update"
                subprocess.run(cmd, check=True)
                print(f"🔄 Updated {job_name} for {dt_utc}")
            else:
                print(f"❌ Error {job_name}: {e.stderr.decode()}")

if __name__ == "__main__":
    print("🌟 Cosmic Architect: Initializing Mercury Schedule 🌟")
    manifest = get_zmanim_data(days=30)
    if manifest:
        triggers = calculate_schedule(manifest, days_to_provision=30)
        provision_cloud_scheduler(triggers)

