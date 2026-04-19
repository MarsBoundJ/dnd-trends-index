"""
Arcane Analytics — Frame collection bootstrap (Step 9.5).

One-time idempotent script that seeds the `frames/` Firestore collection
with a single baseline frame (`pure-data`) and sets it as active. Future
frames (hasbro-2026, players-eye, industry-fundamentals, paizo-2026, …)
land in later steps or via manual console edits.

Why `pure-data` is the baseline:
  - It's the null-frame. empty worldview_summary + empty
    strategic_building_blocks → Track D generation is effectively off and
    the Council writes Track B articles using their own-expertise default.
  - Gives the app a safe "no active corporate strategy" state while we
    build out the real frames.
  - Lets the /admin/frames panel always have *something* to show, even
    before the Hasbro frame is ingested in Step 9.8.

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


def main() -> None:
    db = firestore.Client(project=PROJECT_ID)
    frames_ref = db.collection(FRAMES_COLLECTION)

    # Seed pure-data frame (merge=True keeps any manual additions intact).
    frames_ref.document(PURE_DATA_FRAME["frame_id"]).set(
        PURE_DATA_FRAME, merge=True
    )
    print(f"[OK] Wrote frames/{PURE_DATA_FRAME['frame_id']}")

    # Set pure-data as active ONLY if no active frame is already set.
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
