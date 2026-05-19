"""
YT Transcript POC — puller (the #1 acquisition risk; prove this first).

Hybrid acquisition (runs from the operator's RESIDENTIAL machine — not a
GCP datacenter — which is what makes this viable; datacenter IPs get
blocked, see project_yt_transcript_poc.md "Acquisition reality"):

  - yt-dlp        -> channel video list + per-video metadata
                     (title, description, chapters, view_count,
                      upload_date, duration). Uploader-typed = CLEAN.
  - youtube-transcript-api -> the transcript, with the is_generated
                     flag (manual captions are far cleaner than auto;
                     we record which, per the denoising design).

Writes one self-contained {video_id}.json per video into RAW_DIR
(OUTSIDE the repo). Idempotent: skips a video already pulled unless
--force.

Usage:
  python -m scripts.yt_transcript_poc.pull            # N from config
  python scripts/yt_transcript_poc/pull.py --n 3      # smoke
  python scripts/yt_transcript_poc/pull.py --force
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import random
import sys
import time
import traceback

# Gentle pacing between transcript fetches. YouTube's transcript
# endpoint IP-blocks after a burst (~15 rapid fetches — observed
# 2026-05-19, identical to the BackerKit/DDB "burst then block"
# pattern). Discipline (see feedback_ddb_homebrew_cadence.md +
# context_docs Harvester rules): pace requests AND on the first hard
# block STOP — never retry-harder in-run. Re-run after a cooldown;
# cached videos skip, only the blocked ones are retried.
PACE_MIN_SEC = float(os.environ.get("YT_POC_PACE_MIN", "3"))
PACE_MAX_SEC = float(os.environ.get("YT_POC_PACE_MAX", "7"))

try:  # tolerate being run as a file or a module
    from . import config
except ImportError:  # pragma: no cover
    import config  # type: ignore


def _list_latest_video_ids(channel_url: str, n: int) -> list[dict]:
    """Flat (cheap, no per-video fetch) list of the newest n uploads."""
    import yt_dlp

    opts = {
        "quiet": True,
        "skip_download": True,
        "extract_flat": "in_playlist",
        "playlistend": n,
        "ignoreerrors": True,
    }
    with yt_dlp.YoutubeDL(opts) as ydl:
        info = ydl.extract_info(channel_url, download=False)
    entries = [e for e in (info or {}).get("entries", []) if e and e.get("id")]
    return [
        {"video_id": e["id"], "title": e.get("title", ""), "url": e.get("url", "")}
        for e in entries[:n]
    ]


def _full_metadata(video_id: str) -> dict:
    """Per-video yt-dlp metadata (description/chapters/stats are NOT in
    the flat list). Uploader-typed -> the clean entity spine."""
    import yt_dlp

    url = f"https://www.youtube.com/watch?v={video_id}"
    opts = {"quiet": True, "skip_download": True, "ignoreerrors": True}
    with yt_dlp.YoutubeDL(opts) as ydl:
        info = ydl.extract_info(url, download=False) or {}
    return {
        "video_id": video_id,
        "title": info.get("title", ""),
        "description": info.get("description", "") or "",
        "chapters": [
            {"title": c.get("title", ""), "start": c.get("start_time")}
            for c in (info.get("chapters") or [])
        ],
        "upload_date": info.get("upload_date", ""),  # YYYYMMDD
        "duration_sec": info.get("duration"),
        "view_count": info.get("view_count"),
        "like_count": info.get("like_count"),
        "comment_count": info.get("comment_count"),
        "channel": info.get("channel", ""),
        "channel_id": info.get("channel_id", ""),
        "webpage_url": info.get("webpage_url", url),
        "tags": info.get("tags") or [],
    }


def _fetch_transcript(video_id: str) -> dict:
    """Transcript via youtube-transcript-api v1.x (instance .list()/.fetch()).
    Prefer a MANUALLY-created English track (clean) over auto-generated
    (noisy) — the denoising funnel routes on `caption_source`.

    `error_kind` taxonomy is load-bearing for the acquisition-risk read:
      blocked  -> YouTube IP/request block (the datacenter-block reality;
                  would force the ASR-on-audio fallback even residentially)
      disabled -> creator disabled transcripts (rare for D&D advice)
      none     -> no track in our languages
      other    -> anything else
    """
    from youtube_transcript_api import (
        YouTubeTranscriptApi,
        IpBlocked,
        RequestBlocked,
        TranscriptsDisabled,
        NoTranscriptFound,
    )

    out = {
        "caption_source": None,   # "manual" | "auto" | None
        "language": None,
        "segments": [],           # [{text,start,duration}]
        "error": None,
        "error_kind": None,       # blocked | disabled | none | other
    }
    langs = ["en", "en-US", "en-GB"]
    try:
        ytt = YouTubeTranscriptApi()
        tlist = ytt.list(video_id)
        chosen = None
        try:
            chosen = tlist.find_manually_created_transcript(langs)
            out["caption_source"] = "manual"
        except Exception:
            try:
                chosen = tlist.find_generated_transcript(langs)
                out["caption_source"] = "auto"
            except Exception:
                for t in tlist:  # whatever exists, any language
                    chosen = t
                    out["caption_source"] = "auto" if getattr(
                        t, "is_generated", True) else "manual"
                    break
        if chosen is None:
            out["error"], out["error_kind"] = "no transcript track", "none"
            return out
        out["language"] = getattr(chosen, "language_code", None)
        fetched = chosen.fetch()
        out["segments"] = [
            {"text": s["text"], "start": s["start"], "duration": s["duration"]}
            for s in fetched.to_raw_data()
        ]
    except (IpBlocked, RequestBlocked) as e:
        out["error"], out["error_kind"] = type(e).__name__, "blocked"
    except TranscriptsDisabled:
        out["error"], out["error_kind"] = "TranscriptsDisabled", "disabled"
    except NoTranscriptFound:
        out["error"], out["error_kind"] = "NoTranscriptFound", "none"
    except Exception as e:
        out["error"], out["error_kind"] = f"{type(e).__name__}: {e}", "other"
    return out


def pull(n: int | None = None, force: bool = False) -> dict:
    config.ensure_dirs()
    n = n or config.N_VIDEOS
    print(f"[pull] channel={config.CHANNEL_URL} n={n} -> {config.RAW_DIR}")

    try:
        vids = _list_latest_video_ids(config.CHANNEL_URL, n)
    except Exception as e:
        print(f"[pull] FATAL: could not list channel videos: {e}")
        traceback.print_exc()
        return {"ok": False, "error": str(e)}

    print(f"[pull] found {len(vids)} videos")
    summary = {"ok": True, "channel": config.CHANNEL_LABEL, "pulled": [],
               "skipped": [], "transcript_failed": [], "blocked_stop": False}

    fetched_count = 0
    for i, v in enumerate(vids, 1):
        vid = v["video_id"]
        dest = config.RAW_DIR / f"{vid}.json"
        # Only skip when cached AND the transcript actually succeeded
        # last time. A cached-but-blocked entry (segments empty) should
        # auto-retry on a later cooldown run without needing --force
        # (which would refetch everything). This is what makes the
        # gentle-cadence + cooldown loop converge over time.
        if dest.exists() and not force:
            try:
                prev_tr = json.loads(dest.read_text(encoding="utf-8")).get(
                    "transcript", {}) or {}
            except Exception:
                prev_tr = {}
            if prev_tr.get("segments"):
                summary["skipped"].append(vid)
                print(f"  [{i}/{len(vids)}] {vid}  (cached+ok, skip)")
                continue
            # else: cached but blocked/empty -> retry below
        if fetched_count > 0:  # pace between live fetches, not the first
            time.sleep(random.uniform(PACE_MIN_SEC, PACE_MAX_SEC))
        fetched_count += 1
        try:
            meta = _full_metadata(vid)
            tr = _fetch_transcript(vid)
        except Exception as e:
            print(f"  [{i}/{len(vids)}] {vid}  ERROR {e}")
            continue
        rec = {
            "video_id": vid,
            "pulled_at": dt.datetime.now(dt.timezone.utc).isoformat(),
            "channel_label": config.CHANNEL_LABEL,
            "channel_dialect": config.CHANNEL_DIALECT,
            "metadata": meta,
            "transcript": tr,
        }
        dest.write_text(json.dumps(rec, ensure_ascii=False, indent=2), encoding="utf-8")
        cap = tr["caption_source"] or "NONE"
        nseg = len(tr["segments"])
        if tr["error"]:
            summary["transcript_failed"].append(
                {"video_id": vid, "error": tr["error"],
                 "error_kind": tr.get("error_kind")})
        summary["pulled"].append(vid)
        print(f"  [{i}/{len(vids)}] {vid}  cap={cap} seg={nseg}  "
              f"\"{meta['title'][:60]}\"")
        if tr.get("error_kind") == "blocked":
            # Discipline: first hard block -> STOP. Hammering only
            # deepens it (the BackerKit/DDB lesson). Metadata + the
            # block marker are saved; a later re-run skips cached and
            # retries just the blocked ones after the cooldown.
            summary["blocked_stop"] = True
            print(f"[pull] IP-BLOCK hit at video {i} — STOPPING (not "
                  f"retrying-harder). Re-run after a cooldown; the "
                  f"{len(summary['pulled'])-1} good ones will skip, "
                  f"blocked ones retry.")
            break

    print(f"[pull] done: pulled={len(summary['pulled'])} "
          f"skipped={len(summary['skipped'])} "
          f"transcript_failed={len(summary['transcript_failed'])}")
    return summary


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=None)
    ap.add_argument("--force", action="store_true")
    a = ap.parse_args()
    r = pull(n=a.n, force=a.force)
    sys.exit(0 if r.get("ok") else 1)
