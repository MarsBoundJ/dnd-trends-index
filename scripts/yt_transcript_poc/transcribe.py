"""
YT Transcript POC — Option D: Gemini 2.5 Flash-Lite audio ASR.

This is the production-clean replacement for the rate-limited YouTube
captions endpoint. Takes the audio files downloaded by audio_pull.py
and produces a clean transcript via Gemini, with a DYNAMIC MICRO-
GLOSSARY in the system instruction so D&D proper nouns (spells,
subclasses, classes, mechanics) get the canonical spelling at the
source — exactly the red-team-hardened design's "glossary-biased ASR"
(see project_yt_transcript_poc.md).

Verified pricing (Phil, May 2026): Gemini 2.5 Flash-Lite at $0.30/1M
audio input + $0.40/1M text output ≈ $1.38 for the full 75 × 35 min
Treantmonk batch (incl. metadata pass). Token cost logged per video.

Requires GEMINI_API_KEY (env var or local .env). The .env file is
gitignored — never committed.
"""

from __future__ import annotations

import argparse
import datetime as dt
import glob
import json
import os
import subprocess
import sys
import time
import traceback
from pathlib import Path

try:
    from . import config
except ImportError:  # pragma: no cover
    import config  # type: ignore

# Long-audio chunking. Gemini 2.5 Flash-Lite enters soft-repetition
# loops on long-form transcription — first observed on Treantmonk's
# "Wizard Tier 3" (50 min) → output cap 65k tokens. 15-min chunks
# helped some chunks but the middle chunk still looped 9× on a
# repetitive-discourse pattern. Two-part fix:
#   1) smaller chunks (8 min) — lowers probability of latching onto
#      a loop pattern in the first place;
#   2) per-call max_output_tokens proportional to chunk duration —
#      hard ceiling that stops runaway loops WITHOUT losing audio
#      content (the next chunk's boundary determines coverage).
# ffmpeg -c copy is lossless + fast (no transcode).
CHUNK_MAX_SEC = int(os.environ.get("YT_POC_CHUNK_MAX_SEC", "480"))  # 8 min
# Output cap formula: normal English speech is ~4 output tokens/sec
# of audio (200 wpm × ~1.3 tok/word ÷ 60s). 25 tok/sec is ~6x normal
# headroom — generous for verbose-speech videos but TIGHT enough that
# a loop hits the cap within ~5x the clean transcript length, not
# the model's 65k natural ceiling. Result: clean transcripts cost
# nothing extra; loops are bounded.
OUTPUT_TOKEN_BUDGET_PER_SEC = 25
MAX_OUTPUT_TOKENS_CEILING = 20000  # hard ceiling regardless of duration


# ── Minimal .env loader (no python-dotenv dependency) ───────────────────────
def _load_dotenv() -> None:
    """Load KEY=VALUE pairs from <repo>/scripts/yt_transcript_poc/.env
    into os.environ (without overwriting existing values)."""
    here = Path(__file__).resolve().parent
    for candidate in (here / ".env", here.parent.parent / ".env"):
        if candidate.exists():
            for line in candidate.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, _, v = line.partition("=")
                k = k.strip()
                v = v.strip().strip("'").strip('"')
                if k and k not in os.environ:
                    os.environ[k] = v
            return


# ── Dynamic micro-glossary per video ─────────────────────────────────────────
def _per_video_glossary(video_id: str, meta: dict) -> list[str]:
    """Defaults + this video's primary entities (already extracted in
    derived/{id}.entities.json by extract.py over the clean uploader
    text) + title/tag tokens. Capped at ~80 terms — the red-team
    sweet spot."""
    terms: list[str] = list(config.DEFAULT_GLOSSARY_SEED)

    # Pull primary entities from the deterministic extractor if cached.
    ent_path = config.DERIVED_DIR / f"{video_id}.entities.json"
    if ent_path.exists():
        try:
            ents = json.loads(ent_path.read_text(encoding="utf-8"))
            for x in ents.get("primary_entities", []):
                c = x.get("canonical")
                if c and c not in terms:
                    terms.append(c)
        except Exception:
            pass

    # Title / chapter titles are uploader-typed, always relevant.
    title_terms = [t for t in (meta.get("title", "") or "").split()
                   if len(t) >= 4 and t[0].isupper()]
    for t in title_terms:
        if t not in terms:
            terms.append(t)
    for ch in meta.get("chapters", []) or []:
        ct = ch.get("title", "") or ""
        for t in ct.split():
            if len(t) >= 4 and t[0].isupper() and t not in terms:
                terms.append(t)

    return terms[:80]


# ── Gemini call ──────────────────────────────────────────────────────────────
SYSTEM_INSTRUCTION = (
    "You are a careful transcriber of D&D YouTube content. Produce a "
    "VERBATIM transcript of the audio. When you hear the D&D terms listed "
    "below (proper nouns of spells, subclasses, classes, monsters, "
    "mechanics), use the CANONICAL spelling provided — do not paraphrase, "
    "do not auto-correct away from these spellings. Output ONLY the "
    "transcript text in natural sentences with punctuation. No "
    "timestamps. No summaries. No commentary. No section headers."
)


def _ffmpeg_chunk(input_path: str, start_sec: float, dur_sec: float,
                  output_path: str) -> None:
    """Lossless audio slice via ffmpeg -c copy — fast, no re-encode.
    Required to keep long videos under Gemini's clean-output window."""
    cmd = [
        "ffmpeg", "-y", "-loglevel", "error",
        "-ss", str(start_sec), "-i", input_path,
        "-t", str(dur_sec), "-c", "copy", output_path,
    ]
    subprocess.run(cmd, check=True)


def _transcribe_long(client, audio_path: str, duration_sec: float,
                     glossary: list[str]) -> dict:
    """Chunk + transcribe + stitch for videos > CHUNK_MAX_SEC. Returns
    the same shape as _transcribe_one (text/usage/cost merged across
    chunks)."""
    chunks_dir = Path(audio_path).parent
    base = Path(audio_path).stem
    parts: list[str] = []
    usage_total = {"prompt_tokens": 0, "candidates_tokens": 0,
                   "total_tokens": 0}
    errors: list[str] = []

    start = 0.0
    idx = 0
    while start < duration_sec:
        dur = min(CHUNK_MAX_SEC, duration_sec - start)
        chunk_path = str(chunks_dir / f"{base}.chunk{idx:02d}.m4a")
        try:
            _ffmpeg_chunk(audio_path, start, dur, chunk_path)
        except subprocess.CalledProcessError as e:
            return {"ok": False, "text": "",
                    "error": f"ffmpeg chunk failed: {e}", "usage": None}
        sub = _transcribe_one(client, chunk_path, glossary,
                              max_output_tokens=_budget_for_duration(dur))
        # Cleanup chunk file regardless of result (keep disk clean).
        try:
            os.unlink(chunk_path)
        except OSError:
            pass
        if not sub["ok"]:
            errors.append(f"chunk {idx}: {sub.get('error')}")
            # Continue; partial transcript is better than nothing.
        else:
            parts.append(sub["text"])
        if sub.get("usage"):
            for k in usage_total:
                usage_total[k] += sub["usage"].get(k) or 0
        start += dur
        idx += 1

    full_text = "\n\n".join(parts)
    return {
        "ok": bool(full_text),
        "text": full_text,
        "error": "; ".join(errors) if errors else None,
        "usage": usage_total if any(usage_total.values()) else None,
    }


def _budget_for_duration(duration_sec: float) -> int:
    """Per-call max_output_tokens cap proportional to audio duration."""
    return min(int(duration_sec * OUTPUT_TOKEN_BUDGET_PER_SEC) + 500,
               MAX_OUTPUT_TOKENS_CEILING)


def _transcribe_one(client, audio_path: str, glossary: list[str],
                    max_output_tokens: int | None = None) -> dict:
    """Returns {ok, text, error, usage}. Caller handles retries."""
    out = {"ok": False, "text": "", "error": None, "usage": None}
    try:
        # New SDK (google-genai 1.x) Files API — robust for audio.
        audio_file = client.files.upload(file=audio_path)
        user_prompt = (
            "Transcribe this D&D YouTube video. "
            f"Canonical D&D terms to use when heard: {', '.join(glossary)}."
        )
        gen_config = {
            "system_instruction": SYSTEM_INSTRUCTION,
            "temperature": 0.0,
        }
        if max_output_tokens:
            gen_config["max_output_tokens"] = max_output_tokens
        resp = client.models.generate_content(
            model=config.ASR_MODEL,
            contents=[audio_file, user_prompt],
            config=gen_config,
        )
        out["text"] = (resp.text or "").strip()
        out["ok"] = bool(out["text"])
        # Token usage for cost-tracking.
        if hasattr(resp, "usage_metadata") and resp.usage_metadata:
            um = resp.usage_metadata
            out["usage"] = {
                "prompt_tokens": getattr(um, "prompt_token_count", None),
                "candidates_tokens": getattr(um, "candidates_token_count", None),
                "total_tokens": getattr(um, "total_token_count", None),
            }
        # Best-effort cleanup of the uploaded file (Files API auto-expires
        # in 48h anyway, but be polite).
        try:
            client.files.delete(name=audio_file.name)
        except Exception:
            pass
    except Exception as e:
        out["error"] = f"{type(e).__name__}: {e}"
    return out


# ── Cost estimator (live; matches the verified pricing) ─────────────────────
PRICE_AUDIO_IN_PER_MTOK = 0.30   # Flash-Lite audio input ($/1M tokens)
PRICE_TEXT_OUT_PER_MTOK = 0.40   # Flash-Lite text output ($/1M tokens)


def _estimate_cost(usage: dict | None) -> float | None:
    if not usage:
        return None
    pin = (usage.get("prompt_tokens") or 0) / 1_000_000
    pout = (usage.get("candidates_tokens") or 0) / 1_000_000
    return round(pin * PRICE_AUDIO_IN_PER_MTOK + pout * PRICE_TEXT_OUT_PER_MTOK, 6)


# ── Main driver ──────────────────────────────────────────────────────────────
def _looks_already_transcribed(rec: dict) -> bool:
    tr = rec.get("transcript") or {}
    return (tr.get("caption_source") == "gemini_flash_lite"
            and bool(tr.get("segments")))


def run(force: bool = False, max_videos: int | None = None) -> dict:
    _load_dotenv()
    config.ensure_dirs()

    key = os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")
    if not key:
        msg = ("GEMINI_API_KEY (or GOOGLE_API_KEY) is not set. Add it to "
               "scripts/yt_transcript_poc/.env (gitignored) or export it.")
        print(f"[transcribe] {msg}")
        return {"ok": False, "error": "no API key", "message": msg}

    try:
        from google import genai
    except ImportError as e:
        print(f"[transcribe] google-genai not installed: {e}")
        return {"ok": False, "error": "google-genai missing"}

    client = genai.Client(api_key=key)

    targets: list[Path] = []
    for p in sorted(config.RAW_DIR.glob("*.json")):
        try:
            rec = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not rec.get("audio_path"):
            continue
        if _looks_already_transcribed(rec) and not force:
            continue
        targets.append(p)
        if max_videos and len(targets) >= max_videos:
            break

    print(f"[transcribe] {len(targets)} video(s) to transcribe via "
          f"{config.ASR_MODEL}")
    summary = {"ok": True, "transcribed": [], "failed": [], "total_cost_usd": 0.0}

    for i, p in enumerate(targets, 1):
        rec = json.loads(p.read_text(encoding="utf-8"))
        vid = rec["video_id"]
        meta = rec.get("metadata", {})
        audio_path = rec.get("audio_path")
        if not audio_path or not Path(audio_path).exists():
            summary["failed"].append({"video_id": vid,
                                       "error": "audio file missing"})
            print(f"  [{i}/{len(targets)}] {vid}  ERROR audio missing")
            continue

        glossary = _per_video_glossary(vid, meta)
        duration_sec = float(meta.get("duration_sec") or 0.0)
        t0 = time.time()
        if duration_sec > CHUNK_MAX_SEC:
            n_chunks = int((duration_sec + CHUNK_MAX_SEC - 1) // CHUNK_MAX_SEC)
            print(f"  [{i}/{len(targets)}] {vid}  long audio "
                  f"({duration_sec/60:.1f} min) -> {n_chunks}-chunk")
            result = _transcribe_long(client, audio_path, duration_sec, glossary)
        else:
            result = _transcribe_one(client, audio_path, glossary,
                                     max_output_tokens=_budget_for_duration(
                                         duration_sec))
        dt_s = time.time() - t0

        if not result["ok"]:
            summary["failed"].append({"video_id": vid, "error": result["error"]})
            print(f"  [{i}/{len(targets)}] {vid}  ERROR {result['error']}  "
                  f"({dt_s:.1f}s)")
            continue

        # Single-segment shape preserves downstream schema compatibility
        # (normalize / extract / parse_llm read transcript.segments[*].text).
        text = result["text"]
        rec["transcript"] = {
            "caption_source": "gemini_flash_lite",
            "language": "en",
            "segments": [{
                "text": text,
                "start": 0.0,
                "duration": float(meta.get("duration_sec") or 0.0),
            }],
            "error": None,
            "error_kind": None,
            "glossary_size": len(glossary),
            "asr_model": config.ASR_MODEL,
            "asr_usage": result["usage"],
            "asr_cost_usd": _estimate_cost(result["usage"]),
            "asr_elapsed_sec": round(dt_s, 1),
            "asr_transcribed_at": dt.datetime.now(
                dt.timezone.utc).isoformat(),
        }
        p.write_text(json.dumps(rec, ensure_ascii=False, indent=2),
                     encoding="utf-8")

        cost = rec["transcript"]["asr_cost_usd"] or 0.0
        summary["total_cost_usd"] = round(summary["total_cost_usd"] + cost, 6)
        summary["transcribed"].append({"video_id": vid,
                                         "cost_usd": cost,
                                         "elapsed_sec": dt_s})
        print(f"  [{i}/{len(targets)}] {vid}  ok len={len(text)} chars  "
              f"cost=${cost:.4f}  ({dt_s:.1f}s)  \"{meta.get('title','')[:50]}\"")

    print(f"[transcribe] done: transcribed={len(summary['transcribed'])} "
          f"failed={len(summary['failed'])} "
          f"total_cost=${summary['total_cost_usd']:.4f}")
    return summary


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--force", action="store_true",
                    help="Re-transcribe even if already done.")
    ap.add_argument("--max", type=int, default=None,
                    help="Cap the number of videos transcribed this run "
                         "(useful for smoke-testing on 1-2 videos before "
                         "committing to the full batch).")
    a = ap.parse_args()
    r = run(force=a.force, max_videos=a.max)
    sys.exit(0 if r.get("ok") else 1)
