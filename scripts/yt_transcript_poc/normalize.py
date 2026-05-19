"""
YT Transcript POC — normalizer.

Splits each raw pull into the two layers the denoising design depends on:
  - clean_text : uploader-typed (title + description + chapter titles).
                 Precision-critical entity coverage comes from HERE.
  - transcript : the (auto, noisy) spoken text + timestamped segments.
                 Stance + speech-only relational entities come from here.

Writes DERIVED_DIR/{video_id}.norm.json. No network, no API.
"""

from __future__ import annotations

import glob
import json
import os
import re

try:
    from . import config
except ImportError:  # pragma: no cover
    import config  # type: ignore


def _clean(s: str) -> str:
    return re.sub(r"[ \t]+", " ", (s or "")).strip()


def normalize_one(raw: dict) -> dict:
    m = raw.get("metadata", {})
    t = raw.get("transcript", {})
    chapter_titles = [c.get("title", "") for c in m.get("chapters", [])]
    clean_text = "\n".join(
        [m.get("title", ""), m.get("description", "")] + chapter_titles
    )
    segs = t.get("segments", []) or []
    transcript_text = _clean(" ".join(s.get("text", "") for s in segs))
    return {
        "video_id": raw.get("video_id"),
        "title": m.get("title", ""),
        "upload_date": m.get("upload_date", ""),
        "view_count": m.get("view_count"),
        "like_count": m.get("like_count"),
        "duration_sec": m.get("duration_sec"),
        "caption_source": t.get("caption_source"),
        "transcript_ok": bool(segs),
        "transcript_error_kind": t.get("error_kind"),
        # the two layers:
        "clean_text": _clean(clean_text),         # uploader-typed
        "chapter_titles": chapter_titles,
        "transcript_text": transcript_text,       # noisy ASR
        "transcript_segments": segs,
        "transcript_char_len": len(transcript_text),
    }


def run() -> list[dict]:
    config.ensure_dirs()
    out = []
    for f in sorted(glob.glob(str(config.RAW_DIR / "*.json"))):
        raw = json.loads(open(f, encoding="utf-8").read())
        norm = normalize_one(raw)
        dest = config.DERIVED_DIR / f"{norm['video_id']}.norm.json"
        dest.write_text(json.dumps(norm, ensure_ascii=False, indent=2),
                        encoding="utf-8")
        out.append(norm)
    print(f"[normalize] {len(out)} videos -> {config.DERIVED_DIR}")
    return out


if __name__ == "__main__":
    run()
