"""
Arcane Analytics — Frame collection bootstrap (Steps 9.5 + 9.7).

One-time idempotent script that seeds the `frames/` Firestore collection:
  - `pure-data` (9.5) — the null-frame baseline, used as the default active
    frame. Empty worldview = transparent to article generation.
  - `players-eye` (9.7) — the demand-side frame paired with Gamer Gary.
    Seeded inactive; admin can switch via /admin/frames.

Future frames (hasbro-2026, industry-fundamentals, paizo-2026, …) land in
later steps or via manual console edits.

Run:
    python setup_frames_collection.py

Safety:
  - Idempotent via set(merge=True). Re-running won't clobber manual edits.
  - Active pointer (`frames/_meta`) is only written if no active frame is
    already set, so re-running won't flip a production active frame back.
  - Requires Application Default Credentials: `gcloud auth application-default login`.
"""

from google.cloud import firestore

PROJECT_ID = "dnd-trends-index"
FRAMES_COLLECTION = "frames"
ACTIVE_META_DOC = "_meta"

PURE_DATA_FRAME = {
    "frame_id": "pure-data",
    "label": "Pure Data (no corporate-strategy frame)",
    "source_docs": [],
    "worldview_summary": "",
    "strategic_building_blocks": [],
    "portfolio_taxonomy": [],
    "structural_taxonomy": [],
    "named_grow_brands": [],
    "named_gem2_franchises": [],
    "benchmarks": {},
    "risks_on_watch": [],
    "active_universes_beyond_pipeline": [],
    "active_partnership_pipeline": [],
    "in_house_digital_slate": [],
    "tone_distribution": {"deck_ready": 6, "sharp": 1},
    "guardrails": [],
}

# Step 9.7 — the demand-side frame paired with Gamer Gary. Read signals
# through "what's happening at actual tables" rather than "what corporate
# strategy is executing." See project_tracks_frames_roadmap.md and the
# Gary voice notes in council.py.
PLAYERS_EYE_FRAME = {
    "frame_id": "players-eye",
    "label": "Player's-Eye (demand-side perspective)",
    "source_docs": [
        # Internal style studies — Gemini extractions:
        # ginny_di_player_analysis.md (radical empathy + social contract)
        # mike_shea_player_analysis.md (pragmatic protector + Lazy DM)
    ],
    "worldview_summary": (
        "Read every signal through what's happening at actual D&D tables — "
        "Friday night home games, convention one-shots, organized play "
        "stores. Players and DMs are the demand side of every supply-side "
        "move a publisher makes; their felt experience is the ground truth "
        "the industry's numbers reflect. Surface goodwill erosion before it "
        "becomes revenue erosion."
    ),
    "strategic_building_blocks": [
        {
            "id": "actually_played",
            "label": "Actually Played at Tables",
            "rubric": (
                "Does VTT/Roll20/Foundry data show real sessions running "
                "this, or is it just talk?"
            ),
        },
        {
            "id": "table_sentiment",
            "label": "Table Sentiment",
            "rubric": (
                "Are players enjoying this? Reddit, BGG reviews, YouTube "
                "comment tone."
            ),
        },
        {
            "id": "watch_vs_play_split",
            "label": "Watching vs Playing",
            "rubric": (
                "Actual-play YouTube can spike without tables picking it "
                "up. Which is which?"
            ),
        },
        {
            "id": "optimization_patterns",
            "label": "What Players Optimize For",
            "rubric": (
                "Class build-advice searches reveal which knobs players "
                "actually turn."
            ),
        },
        {
            "id": "community_health",
            "label": "Community Health",
            "rubric": (
                "Subreddit active-user deltas, Discord growth, organized-"
                "play attendance."
            ),
        },
    ],
    "portfolio_taxonomy": [],
    "structural_taxonomy": [],
    "named_grow_brands": [],
    "named_gem2_franchises": [],
    "benchmarks": {},
    "risks_on_watch": [],
    "active_universes_beyond_pipeline": [],
    "active_partnership_pipeline": [],
    "in_house_digital_slate": [],
    # Tuned looser than Hasbro's 6:1 — Gary is more freeform by design.
    "tone_distribution": {"deck_ready": 4, "sharp": 3},
    "guardrails": [
        "table_grounded",  # Every observation must tie to a concrete
                           # table-level consequence. No abstractions.
        "emotion_from_data",  # Emotional register follows the signal. Don't
                              # invent sting; don't hide sting.
        "constructive_not_adversarial",  # Critique specifies practice +
                                         # who it hurts + what would fix it.
    ],
}


def main() -> None:
    db = firestore.Client(project=PROJECT_ID)
    frames_ref = db.collection(FRAMES_COLLECTION)

    # Seed pure-data + players-eye frames (merge=True keeps any manual
    # additions intact).
    for frame in (PURE_DATA_FRAME, PLAYERS_EYE_FRAME):
        frames_ref.document(frame["frame_id"]).set(frame, merge=True)
        print(f"[OK] Wrote frames/{frame['frame_id']}")

    # Set pure-data as active ONLY if no active frame is already set.
    # players-eye is seeded inactive — admin flips via /admin/frames when
    # they want demand-side reads on Track D.
    meta_ref = frames_ref.document(ACTIVE_META_DOC)
    meta_snap = meta_ref.get()
    if meta_snap.exists and (meta_snap.to_dict() or {}).get("activeFrameId"):
        print(
            f"[OK] frames/_meta already points at "
            f"'{(meta_snap.to_dict() or {}).get('activeFrameId')}' — leaving as-is"
        )
    else:
        meta_ref.set({"activeFrameId": PURE_DATA_FRAME["frame_id"]})
        print(f"[OK] Set frames/_meta.activeFrameId = {PURE_DATA_FRAME['frame_id']}")

    print("Done.")


if __name__ == "__main__":
    main()
