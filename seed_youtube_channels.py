import os
from googleapiclient.discovery import build
from google.cloud import bigquery
import datetime

# Using the API Key provided by user
API_KEY = "AIzaSyCIGyZyvf4m13f46pb0GAVGy4lsd88yQJ8"

CHANNELS = [
    # Tier 1
    ("@criticalrole", 1), ("@dimension20show", 1), ("@DNDWizards", 1), ("@DungeonDudes", 1), ("@GinnyDi", 1), 
    ("@XPtoLevel3", 1), ("@mcolville", 1), ("@TheDungeoncast", 1), ("@DnDBeyond", 1),
    # Tier 2
    ("@TreantmonksTemple", 2), ("@PackTactics", 2), ("@DnDDeepDive", 2), ("@TuloktheBarbrarian", 2), 
    ("@MinMaxMunchkin", 2), ("@TheDDLogs", 2),
    # Tier 3
    ("@Jorphdan", 3), ("@MrRhexx", 3), ("@AJPickett", 3), ("@PuffinForest", 3), ("@zeebashew", 3), 
    ("@DingoDoodles", 3), ("@Runesmith", 3), ("@PointyHat", 3), ("@DeerstalkerPictures", 3), 
    ("@VivaLaDirtLeagueDnD", 3),
    # Tier 4
    ("@SlyFlourish", 4), ("@theDMLair", 4), ("@BobWorldBuilder", 4), ("@WebDM", 4), ("@Taking20", 4), 
    ("@Nerdarchy", 4), ("@NerdImmersion", 4), ("@DUNGEONCRAFT1", 4), ("@MonarchsFactory", 4), ("@Edariad", 4),
    # Tier 5
    ("@HighRollersDnD", 5), ("@StinkyDragonPod", 5), ("@LegendsofAvantris", 5), ("@JustRollWithIt", 5), 
    ("@WorldofIo", 5), ("@OneShotQuesters", 5), ("@DnDShorts", 5), ("@MrRipper", 5), ("@CritCrab", 5), 
    ("@AllThingsDnD", 5), ("@WASD20", 5), ("@EspertheBard", 5), ("@DontStopThinking", 5), ("@RealmSmith", 5),
    # Tier 6
    ("@the_twig131", 6), ("@GMPhilosophy", 6), ("@EmergentGM", 6), ("@allseeingeyetrpg", 6), 
    ("@TabletopTheory", 6), ("@DndUnoptimized", 6), ("@DungeonsAndDrams", 6), ("@WintryRPG", 6), 
    ("@FryMinis", 6), ("@ZipperonDisney", 6), ("@ZachTheBold", 6), ("@TheClericCorner", 6)
]

def seed_registry():
    print(f"Initializing YouTube API with key ending in ...{API_KEY[-4:]}")
    try:
        youtube = build("youtube", "v3", developerKey=API_KEY)
    except Exception as e:
        print(f"Failed to build service: {e}")
        return

    rows_to_insert = []
    
    print(f"Resolving {len(CHANNELS)} channels...")
    for handle, tier in CHANNELS:
        try:
            req = youtube.channels().list(forHandle=handle, part="id,snippet,contentDetails")
            res = req.execute()
            
            if not res.get("items"):
                print(f"  -> Not found: {handle}")
                continue
                
            item = res["items"][0]
            c_id = item["id"]
            title = item["snippet"]["title"]
            # Optimization: Convert UC to UU to get uploads playlist without extra call
            uploads_id = c_id.replace("UC", "UU", 1)
            
            rows_to_insert.append({
                "channel_id": c_id,
                "channel_name": title,
                "handle": handle,
                "uploads_playlist_id": uploads_id,
                "tier": tier,
                "last_scanned_at": None
            })
            # print(f"  -> Found: {title}") # Reduce spam
            
        except Exception as e:
            print(f"  -> Error for {handle}: {e}")

    if rows_to_insert:
        client = bigquery.Client()
        table_id = "dnd-trends-index.social_data.youtube_channel_registry"
        
        # Deduplication check could go here, but BQ append is fine for now, user can clean later
        try:
            print(f"Inserting {len(rows_to_insert)} rows to BigQuery...")
            errors = client.insert_rows_json(table_id, rows_to_insert)
            if errors:
                print(f"BQ Errors: {errors}")
            else:
                print("Seeding Complete.")
        except Exception as e:
            print(f"BQ Insert Failed: {e}")
    else:
        print("No channels resolved.")

if __name__ == "__main__":
    seed_registry()
