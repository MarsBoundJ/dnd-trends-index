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
    """Returns (ok, audio_path, error). Robust against YouTube SABR:
    YT now serves combined audio+video streams to many clients and
    hides clean audio-only streams (verified during smoke test —
    bestaudio[ext=m4a] failed with 'Requested format is not available'
    on a recent Treantmonk video). Strategy: prefer audio-only m4a;
    if SABR forces combined, FFmpegExtractAudio (ffmpeg required, we
    have it) pulls the audio track to m4a. Always yields {id}.m4a
    that Gemini ingests directly."""
    import yt_dlp

    url = f"https://www.youtube.com/watch?v={video_id}"
    out_tmpl = str(config.AUDIO_DIR / "%(id)s.%(ext)s")
    opts = {
        # Try audio-only first; fall through to combined ("best") if
        # SABR has hidden the audio-only streams from our client.
        "format": "bestaudio[ext=m4a]/bestaudio/best",
        "outtmpl": out_tmpl,
        "postprocessors": [{
            "key": "FFmpegExtractAudio",
            "preferredcodec": "m4a",
            "preferredquality": "0",
        }],
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
        # After FFmpegExtractAudio runs, the m4a is the canonical output.
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


def _list_videos_since(channel_url: str, since_yyyymmdd: str,
                       n_max: int = 400) -> list[dict]:
    """Walk channel newest-first; per-video metadata fetch to get
    upload_date; STOP early when a video is older than `since` (since
    /videos is sorted newest-first, everything after is older too).

    Returns [{video_id, title, upload_date}] sorted newest-first.
    Per-video extraction is sequential — yt-dlp metadata calls aren't
    rate-limited the same way the captions endpoint is, but we still
    pace lightly to be kind.
    """
    import time as _time
    flat = pull._list_latest_video_ids(channel_url, n_max)
    out: list[dict] = []
    print(f"[audio_pull] walking {len(flat)} flat entries; filter "
          f"upload_date >= {since_yyyymmdd}")
    for i, v in enumerate(flat, 1):
        try:
            meta = pull._full_metadata(v["video_id"])
        except Exception:
            continue
        ud = meta.get("upload_date", "") or ""
        if not ud:
            continue
        if ud < since_yyyymmdd:
            print(f"[audio_pull]   stopped at #{i}: {v['video_id']} "
                  f"upload_date={ud} < {since_yyyymmdd}")
            break
        out.append({"video_id": v["video_id"], "title": meta["title"],
                    "upload_date": ud})
        if i % 25 == 0:
            print(f"[audio_pull]   ... {i} scanned, {len(out)} matched")
        _time.sleep(0.4)  # gentle pace; metadata endpoint isn't the
                          # transcript-endpoint rate-limiter
    return out


def _stratified_pick(sorted_list: list[dict], n: int) -> list[dict]:
    """Pick n items evenly spaced by index from a sorted list. Used to
    smoke-test ASR/extract quality across the corpus's full date span
    cheaply before committing to the full batch."""
    if len(sorted_list) <= n:
        return sorted_list
    step = (len(sorted_list) - 1) / (n - 1)
    return [sorted_list[int(round(i * step))] for i in range(n)]


def pull_audio(n: int | None = None, force: bool = False,
               since: str | None = None,
               stratified_sample: int | None = None,
               dry_run: bool = False) -> dict:
    config.ensure_dirs()
    n_max = n or (400 if since else config.N_VIDEOS)
    print(f"[audio_pull] channel={config.CHANNEL_URL} n_max={n_max}"
          f"{' since=' + since if since else ''}"
          f"{' stratified=' + str(stratified_sample) if stratified_sample else ''}"
          f"{' DRY-RUN' if dry_run else ''}"
          f" -> {config.AUDIO_DIR}")

    if since:
        candidates = _list_videos_since(config.CHANNEL_URL, since, n_max)
        print(f"[audio_pull] {len(candidates)} videos with upload_date >= {since}")
        if stratified_sample:
            vids = _stratified_pick(candidates, stratified_sample)
            print(f"[audio_pull] stratified sample of {len(vids)}:")
            for v in vids:
                print(f"  {v['upload_date']}  {v['video_id']}  "
                      f"\"{v['title'][:55]}\"")
        else:
            vids = candidates
    else:
        try:
            vids = pull._list_latest_video_ids(config.CHANNEL_URL, n_max)
        except Exception as e:
            print(f"[audio_pull] FATAL: could not list channel videos: {e}")
            return {"ok": False, "error": str(e)}
        print(f"[audio_pull] found {len(vids)} videos")

    if dry_run:
        print(f"[audio_pull] DRY-RUN — no downloads.")
        return {"ok": True, "dry_run": True, "selected": vids}
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
    ap.add_argument("--n", type=int, default=None,
                    help="Cap on flat-listing scan. Default 400 with --since, "
                         "or N_VIDEOS from config without.")
    ap.add_argument("--force", action="store_true",
                    help="Re-download even if audio is already cached.")
    ap.add_argument("--since", type=str, default=None,
                    help="YYYYMMDD — only include videos uploaded on/after "
                         "this date. e.g. --since 20240618 (anchor: 2024 PHB "
                         "Weapon Mastery official rules).")
    ap.add_argument("--stratified-sample", type=int, default=None,
                    help="Pick N videos evenly spaced by upload_date across "
                         "the --since-filtered set. Used to smoke-test "
                         "quality across the date span cheaply before "
                         "committing to the full batch.")
    ap.add_argument("--dry-run", action="store_true",
                    help="List the selection without downloading audio.")
    a = ap.parse_args()
    r = pull_audio(n=a.n, force=a.force, since=a.since,
                   stratified_sample=a.stratified_sample, dry_run=a.dry_run)
    sys.exit(0 if r.get("ok") else 1)
