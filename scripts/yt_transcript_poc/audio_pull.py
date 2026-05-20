"""
YT Transcript POC — Option D: audio puller (the production-clean path).

Replaces the IpBlocked-prone YouTube captions endpoint with audio
download + own-ASR (transcribe.py). yt-dlp pulls only the audio stream
(no video re-encoding, no ffmpeg dependency: we ask yt-dlp for
native-m4a/AAC which Gemini accepts directly).

Storage:
  ~/yt_poc_data/treantmonk/audio/{video_id}.m4a   (OUTSIDE repo;
  copyrighted creator audio; gitignored; same derive-and-discard rule
  as transcripts per project_yt_transcript_poc.md).

Metadata is still pulled via pull.py's helpers (uploader-typed = clean,
not affected by the YT captions rate-limit) and written to
RAW_DIR/{video_id}.json — with the `transcript` field LEFT EMPTY here.
transcribe.py fills `transcript` after the audio is processed by Gemini.

Idempotent: skips if {video_id}.m4a already exists locally.

Usage:
  python -m scripts.yt_transcript_poc.audio_pull         # N from config
  python scripts/yt_transcript_poc/audio_pull.py --n 3   # smoke
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import sys

try:
    from . import config, pull  # reuse existing metadata helpers
except ImportError:  # pragma: no cover
    import config       # type: ignore
    import pull         # type: ignore


def _download_audio(video_id: str) -> tuple[bool, str | None, str | None]:
    """Returns (ok, audio_path, error). Prefer native m4a (no ffmpeg
    re-encode needed); fall back to whatever bestaudio offers. yt-dlp
    handles network retries internally."""
    import yt_dlp

    url = f"https://www.youtube.com/watch?v={video_id}"
    out_tmpl = str(config.AUDIO_DIR / "%(id)s.%(ext)s")
    opts = {
        # m4a/AAC is what Gemini ingests cleanly with no transcode.
        "format": "bestaudio[ext=m4a]/bestaudio[acodec=aac]/bestaudio",
        "outtmpl": out_tmpl,
        "quiet": True,
        "noprogress": True,
        "no_warnings": True,
        "ignoreerrors": True,
    }
    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(url, download=True)
        if not info:
            return False, None, "yt-dlp returned no info"
        # Discover the actual on-disk filename (extension varies).
        for ext in ("m4a", "mp4", "webm", "opus", "ogg", "mp3"):
            cand = config.AUDIO_DIR / f"{video_id}.{ext}"
            if cand.exists():
                return True, str(cand), None
        return False, None, "audio file not found on disk after download"
    except Exception as e:
        return False, None, f"{type(e).__name__}: {e}"


def _existing_audio_path(video_id: str) -> str | None:
    for ext in ("m4a", "mp4", "webm", "opus", "ogg", "mp3"):
        cand = config.AUDIO_DIR / f"{video_id}.{ext}"
        if cand.exists():
            return str(cand)
    return None


def pull_audio(n: int | None = None, force: bool = False) -> dict:
    config.ensure_dirs()
    n = n or config.N_VIDEOS
    print(f"[audio_pull] channel={config.CHANNEL_URL} n={n} -> {config.AUDIO_DIR}")

    try:
        vids = pull._list_latest_video_ids(config.CHANNEL_URL, n)
    except Exception as e:
        print(f"[audio_pull] FATAL: could not list channel videos: {e}")
        return {"ok": False, "error": str(e)}

    print(f"[audio_pull] found {len(vids)} videos")
    summary = {"ok": True, "channel": config.CHANNEL_LABEL,
               "downloaded": [], "skipped": [], "failed": []}

    for i, v in enumerate(vids, 1):
        vid = v["video_id"]
        existing = _existing_audio_path(vid)
        if existing and not force:
            summary["skipped"].append({"video_id": vid, "path": existing})
            print(f"  [{i}/{len(vids)}] {vid}  (audio cached, skip)")
            continue

        # Always ensure metadata is captured (matches pull.py schema).
        try:
            meta = pull._full_metadata(vid)
        except Exception as e:
            print(f"  [{i}/{len(vids)}] {vid}  ERROR metadata: {e}")
            summary["failed"].append({"video_id": vid, "error": f"metadata: {e}"})
            continue

        # Download audio.
        ok, audio_path, err = _download_audio(vid)
        if not ok:
            print(f"  [{i}/{len(vids)}] {vid}  ERROR audio: {err}")
            summary["failed"].append({"video_id": vid, "error": f"audio: {err}"})
            continue

        # Write the raw record with transcript LEFT EMPTY (transcribe.py
        # fills it). Same shape as pull.py output so normalize.py et al.
        # work unchanged.
        rec = {
            "video_id": vid,
            "pulled_at": dt.datetime.now(dt.timezone.utc).isoformat(),
            "channel_label": config.CHANNEL_LABEL,
            "channel_dialect": config.CHANNEL_DIALECT,
            "metadata": meta,
            "audio_path": audio_path,
            "transcript": {
                "caption_source": None,   # set by transcribe.py
                "language": None,
                "segments": [],
                "error": "audio pulled; transcript pending — run transcribe.py",
                "error_kind": "pending",
            },
        }
        dest = config.RAW_DIR / f"{vid}.json"
        # If a previous YT-captions record exists, preserve its
        # transcript under acquisition_history (provenance) instead of
        # silently overwriting.
        if dest.exists():
            try:
                prev = json.loads(dest.read_text(encoding="utf-8"))
                hist = prev.get("acquisition_history", [])
                if prev.get("transcript", {}).get("caption_source"):
                    hist.append({
                        "ts": prev.get("pulled_at"),
                        "transcript": prev["transcript"],
                    })
                rec["acquisition_history"] = hist
            except Exception:
                pass
        dest.write_text(json.dumps(rec, ensure_ascii=False, indent=2),
                        encoding="utf-8")
        summary["downloaded"].append({"video_id": vid, "path": audio_path})
        print(f"  [{i}/{len(vids)}] {vid}  audio={audio_path.split('/')[-1]}  "
              f"\"{meta['title'][:55]}\"")

    print(f"[audio_pull] done: downloaded={len(summary['downloaded'])} "
          f"skipped={len(summary['skipped'])} failed={len(summary['failed'])}")
    return summary


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=None)
    ap.add_argument("--force", action="store_true")
    a = ap.parse_args()
    r = pull_audio(n=a.n, force=a.force)
    sys.exit(0 if r.get("ok") else 1)
