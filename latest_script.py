#!/usr/bin/env python3
"""
Improved transcript downloader with better error handling and logging
"""
import os
import re
import time
import json
import random
import traceback
from datetime import datetime, timezone
import requests
import urllib3
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from youtube_transcript_api import YouTubeTranscriptApi
from youtube_transcript_api._errors import TranscriptsDisabled, NoTranscriptFound

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# === CONFIGURATION ===
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROXY_FILE = os.path.join(BASE_DIR, "proxies.txt")
VIDEO_JSON_FILE = os.path.join(BASE_DIR, "treantmonk_videos.json")
TEST_VIDEO_FILE = os.path.join(BASE_DIR, "test_videos.json")  # For testing
USE_TEST_FILE = False  # PRODUCTION MODE - process all videos
OUTPUT_FOLDER = os.path.join(BASE_DIR, "treantmonk_transcripts")
FAILED_LOG_FILE = os.path.join(BASE_DIR, "failed_videos.txt")
PROGRESS_LOG_FILE = os.path.join(BASE_DIR, "download_progress.log")

REQUEST_DELAY_MIN = 60    # 1-3 minute delays to avoid IP bans
REQUEST_DELAY_MAX = 180
MAX_ATTEMPTS_PER_VIDEO = 3  # Try each video up to 3 times before giving up

# === UTILITIES ===
def timestamp():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

def log_progress(message):
    """Log to both console and file"""
    print(message)
    with open(PROGRESS_LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(f"{message}\n")

def clean_filename(title: str) -> str:
    title = title.strip().replace('/', '-').replace(' ', '_')
    return re.sub(r'[^a-zA-Z0-9_-]', '', title)[:120]

def format_transcript(transcript):
    # Handle both dict format and FetchedTranscriptSnippet objects
    if hasattr(transcript, '__iter__'):
        # Convert to list of text strings
        text_parts = []
        for part in transcript:
            if hasattr(part, 'text'):
                text_parts.append(part.text)
            elif isinstance(part, dict) and 'text' in part:
                text_parts.append(part['text'])
            else:
                text_parts.append(str(part))
        full_text = ' '.join(text_parts).replace('\n', ' ')
    else:
        full_text = str(transcript)
    
    sentences = re.split(r'(?<=[.?!])\s+', full_text)
    paragraphs = [' '.join(sentences[i:i+4]) for i in range(0, len(sentences), 4)]
    return "\n\n".join(paragraphs)

def load_proxy():
    """Load the single rotating proxy"""
    if not os.path.exists(PROXY_FILE):
        raise FileNotFoundError(f"Proxy file not found: {PROXY_FILE}")
    
    with open(PROXY_FILE, 'r', encoding='utf-8') as f:
        line = f.readline().strip()
        if ':' in line:
            parts = line.split(':')
            if len(parts) == 2:
                host, port = parts
                return {
                    "http": f"http://{host}:{port}",
                    "https": f"http://{host}:{port}",
                    "host": host,
                    "port": port
                }
    raise RuntimeError("No valid proxy found in proxies.txt")

def get_transcript_with_proxy(video_id, proxy):
    """Get transcript using proxy"""
    # Monkey-patch requests to use proxy
    original_get = requests.get
    original_session_get = requests.Session.get
    
    def patched_get(url, **kwargs):
        kwargs['proxies'] = {'http': proxy['http'], 'https': proxy['https']}
        kwargs['verify'] = False
        kwargs['timeout'] = kwargs.get('timeout', 15)
        return original_get(url, **kwargs)
    
    def patched_session_get(self, url, **kwargs):
        kwargs['proxies'] = {'http': proxy['http'], 'https': proxy['https']}
        kwargs['verify'] = False
        kwargs['timeout'] = kwargs.get('timeout', 15)
        return original_session_get(self, url, **kwargs)
    
    requests.get = patched_get
    requests.Session.get = patched_session_get
    
    try:
        # Use the fetch method
        api = YouTubeTranscriptApi()
        transcript = api.fetch(video_id, languages=['en'])
        return transcript
    finally:
        requests.get = original_get
        requests.Session.get = original_session_get

def extract_video_id(url_or_id):
    """Extract video ID from various YouTube URL formats"""
    if not url_or_id:
        return None
    
    if len(url_or_id) == 11 and re.match(r'^[0-9A-Za-z_-]{11}$', url_or_id):
        return url_or_id
    
    patterns = [
        r'(?:v=|/)([0-9A-Za-z_-]{11}).*',
        r'(?:embed/)([0-9A-Za-z_-]{11})',
        r'(?:watch\?v=)([0-9A-Za-z_-]{11})'
    ]
    
    for pattern in patterns:
        match = re.search(pattern, url_or_id)
        if match:
            return match.group(1)
    
    return None

def load_video_list(path):
    if not os.path.exists(path):
        raise FileNotFoundError(f"Video list not found: {path}")
    with open(path, 'r', encoding='utf-8') as f:
        videos = json.load(f)
    return videos

def save_failed_list(failed_list, path=FAILED_LOG_FILE):
    with open(path, 'w', encoding='utf-8') as outf:
        for item in failed_list:
            outf.write(f"{item.get('url','')}|{item.get('title','')}|{item.get('reason','')}\n")

# === MAIN EXECUTION ===
def main():
    log_progress("="*70)
    log_progress("TREANTMONK TRANSCRIPT DOWNLOADER - IMPROVED VERSION")
    log_progress("="*70)
    log_progress(f"Started at: {timestamp()}")
    log_progress(f"Delay between requests: {REQUEST_DELAY_MIN}-{REQUEST_DELAY_MAX} seconds")
    log_progress(f"Max attempts per video: {MAX_ATTEMPTS_PER_VIDEO}")
    log_progress("="*70)
    
    # Create output folder
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    
    # Load proxy
    try:
        proxy = load_proxy()
        log_progress(f"Loaded proxy: {proxy['host']}:{proxy['port']}")
    except Exception as e:
        log_progress(f"ERROR loading proxy: {e}")
        return
    
    # Load video list
    try:
        video_file = TEST_VIDEO_FILE if USE_TEST_FILE else VIDEO_JSON_FILE
        videos = load_video_list(video_file)
        log_progress(f"Loaded {len(videos)} videos from {'TEST' if USE_TEST_FILE else 'FULL'} list")
    except Exception as e:
        log_progress(f"ERROR loading video list: {e}")
        return
    
    # Check what's already downloaded
    existing_files = set(os.listdir(OUTPUT_FOLDER))
    
    def title_to_filename(title):
        return clean_filename(title) + ".txt"
    
    pending = [v for v in videos if title_to_filename(v['title']) not in existing_files]
    
    log_progress(f"Already downloaded: {len(videos) - len(pending)}")
    log_progress(f"Remaining to download: {len(pending)}")
    log_progress("="*70)
    
    if not pending:
        log_progress("All transcripts already downloaded!")
        return
    
    # Track attempts per video
    attempt_count = {}
    failed_permanent = []
    successful_count = 0
    
    # Process each video
    for i, video in enumerate(pending, 1):
        video_id = extract_video_id(video.get('id', '') or video.get('url', ''))
        title = video.get('title', 'video')
        url = video.get('url', f"https://www.youtube.com/watch?v={video_id}")
        
        if not video_id:
            log_progress(f"[{i}/{len(pending)}] ERROR: Could not extract video ID for '{title}'")
            failed_permanent.append({**video, 'reason': 'Invalid video ID'})
            continue
        
        # Check attempt count
        if video_id not in attempt_count:
            attempt_count[video_id] = 0
        
        if attempt_count[video_id] >= MAX_ATTEMPTS_PER_VIDEO:
            log_progress(f"[{i}/{len(pending)}] SKIP: '{title}' (max attempts reached)")
            continue
        
        attempt_count[video_id] += 1
        log_progress(f"[{i}/{len(pending)}] Attempting '{title}' (ID: {video_id}, attempt {attempt_count[video_id]}/{MAX_ATTEMPTS_PER_VIDEO})")
        
        try:
            transcript = get_transcript_with_proxy(video_id, proxy)
            text = format_transcript(transcript)
            out_file = os.path.join(OUTPUT_FOLDER, title_to_filename(title))
            
            with open(out_file, 'w', encoding='utf-8') as f:
                f.write(f"{title}\n{url}\n\n{text}")
            
            successful_count += 1
            log_progress(f"    ✓ SUCCESS! Saved {len(transcript)} segments. Total downloaded: {successful_count}")
            
        except (TranscriptsDisabled, NoTranscriptFound) as e:
            log_progress(f"    ✗ No transcript available")
            failed_permanent.append({**video, 'reason': 'No transcript available'})
            
        except Exception as e:
            error_msg = str(e)[:100]
            log_progress(f"    ⚠ ERROR: {error_msg}")
            if attempt_count[video_id] >= MAX_ATTEMPTS_PER_VIDEO:
                failed_permanent.append({**video, 'reason': f'Failed after {MAX_ATTEMPTS_PER_VIDEO} attempts'})
        
        # Delay before next request (except for last video)
        if i < len(pending):
            delay = random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX)
            log_progress(f"    Sleeping {delay:.1f}s before next request...")
            time.sleep(delay)
    
    # Save failed videos
    if failed_permanent:
        save_failed_list(failed_permanent)
        log_progress(f"\nSaved {len(failed_permanent)} failed videos to {FAILED_LOG_FILE}")
    
    log_progress("\n" + "="*70)
    log_progress("DOWNLOAD COMPLETE")
    log_progress("="*70)
    log_progress(f"Total videos processed: {len(pending)}")
    log_progress(f"Successfully downloaded: {successful_count}")
    log_progress(f"Failed (no transcript): {len(failed_permanent)}")
    log_progress(f"Finished at: {timestamp()}")
    log_progress("="*70)

if __name__ == "__main__":
    main()
