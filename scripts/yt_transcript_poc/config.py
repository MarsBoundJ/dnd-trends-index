"""
YT Transcript POC — config.

Pilot: Treantmonk's Temple (optimizer / rules-analysis dialect — ideal for
the sim/validation-oracle + expert mechanics opinion). Single channel,
single dialect: this is a PROOF-OF-CONCEPT, not the stratified accuracy
gate (that needs the multi-dialect set per project_yt_transcript_poc.md).

LEGAL POSTURE (locked): transcripts are the creator's copyrighted speech.
Derive-and-discard, internal-only, NEVER commit raw transcripts to git,
NEVER publish verbatim. Raw pulls go OUTSIDE the repo (DATA_DIR below).
"""

from __future__ import annotations

import os
from pathlib import Path

# Treantmonk's Temple. Channel-ID URL (stable; handles can change).
# Verified 2026-05-19: handle @TreantmonksTemple, id UCr7N1om5-7UemjK_stlAuTw.
# /videos = uploads, newest first.
CHANNEL_URL = "https://www.youtube.com/channel/UCr7N1om5-7UemjK_stlAuTw/videos"
CHANNEL_LABEL = "treantmonk"
CHANNEL_DIALECT = "optimizer_rules_analysis"

# How many of the latest videos to pull. 20 chosen over 10: same yt-dlp
# cost, better recent-topic coverage (2024-PHB-era subclass takes, tier
# lists) for the divergence signal. Override with YT_POC_N.
N_VIDEOS = int(os.environ.get("YT_POC_N", "20"))

# Raw data lands OUTSIDE the repo (copyrighted creator content; never
# committed). Override with YT_POC_DATA_DIR.
DATA_DIR = Path(
    os.environ.get("YT_POC_DATA_DIR", str(Path.home() / "yt_poc_data"))
).resolve()
RAW_DIR = DATA_DIR / CHANNEL_LABEL / "raw"          # per-video {id}.json
DERIVED_DIR = DATA_DIR / CHANNEL_LABEL / "derived"  # normalized + extracted
GLOSSARY_CACHE = DATA_DIR / "glossary_cache.json"

# BQ project for the glossary pull (read-only).
BQ_PROJECT = "dnd-trends-index"

# Gemini model for the (separate, gated) LLM parse step.
LLM_MODEL = os.environ.get("YT_POC_LLM_MODEL", "gemini-2.5-flash")


def ensure_dirs() -> None:
    for d in (RAW_DIR, DERIVED_DIR):
        d.mkdir(parents=True, exist_ok=True)
