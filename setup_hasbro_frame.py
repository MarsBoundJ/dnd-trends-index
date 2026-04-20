"""
Arcane Analytics — Hasbro 2026 frame seed (Step 9.8, Chunk A).

Idempotent seed for `frames/hasbro-2026` — the first corporate-strategy
frame. All strategic priors ride Hasbro's FY2025 Annual Report + 10-K +
"Playing to Win" press release, filtered through the pitch spine
documented in project_hasbro_pitch_problems_solutions.md.

Seeded INACTIVE. The active-frame pointer at frames/_meta stays at
`pure-data` until an admin flips it via /admin/frames (or this script is
re-run with --activate, which is deliberately not implemented here —
activation is a one-click admin action, not a script invocation).

Run:
    python setup_hasbro_frame.py

Safety:
  - Idempotent via set(merge=True). Re-running won't clobber field-level
    edits made in the Firestore console or admin panel.
  - Does NOT touch frames/_meta.activeFrameId.
  - Requires ADC: `gcloud auth application-default login`.

Shape mirrors PURE_DATA_FRAME / PLAYERS_EYE_FRAME from
setup_frames_collection.py so the FrameSchema in arcane/src/lib/frames.ts
validates without surprises.
"""

from google.cloud import firestore

PROJECT_ID = "dnd-trends-index"
FRAMES_COLLECTION = "frames"


# The "why every conversation with WotC starts here" single fact —
# embedded in worldview_summary so Sage cites it unprompted when the
# frame is active and a user asks about the D&D outlook.
HASBRO_2026_FRAME = {
    "frame_id": "hasbro-2026",
    "label": "Hasbro / Wizards of the Coast — Playing to Win (FY26)",
    "source_docs": [
        # Four corporate documents ingested as corpus chunks in
        # frames/hasbro-2026/corpus/ by setup_hasbro_frame_corpus.py.
        "Hasbro 2025 Annual Report (FINAL 04172026)",
        "Hasbro Q4 2025 Earnings — Fourth Quarter and Full Year 2025 Financial Results",
        "Hasbro Form 10-K (Feb 2026)",
        "Hasbro Unveils New Strategy — Playing to Win (press release)",
    ],
    "worldview_summary": (
        "Read every signal through Hasbro's Playing to Win strategy (2025–2027). "
        "In FY25 the Grow Brand tier grew +24.4% ($2.8B → $3.5B), led by Magic: The "
        "Gathering (+$641.5M, +59% YoY) on the strength of Universes Beyond sets "
        "(Final Fantasy, Avatar: The Last Airbender, Marvel's Spider-Man, Edges of "
        "Eternities). Two Grow Brands underperformed the tier: PLAY-DOH and DUNGEONS "
        "& DRAGONS. D&D is the Grow Brand that didn't grow — the single fact that "
        "anchors every Track D article about D&D. Hasbro's five Playing-to-Win pillars "
        "(Aging Up, Digital & Direct, Universes Beyond, Partner Scale, Operational "
        "Excellence) structure which signals matter; the Grow/Optimize/Reinvent "
        "portfolio taxonomy and GEM² structural taxonomy (Gamified / Entertainment-"
        "driven / Multi-purchase / Multi-generational) structure how to weight them. "
        "Wizards + Digital Gaming segment runs at 46% adjusted operating margin; "
        "Entertainment runs at 51.4% on $77M of revenue — licensing is the highest-"
        "margin growth vector. Concentration risks: Amazon 11% + Walmart 9% = 20% of "
        "global revenue; top-5 customers = 35%. Tariffs triggered a $1.02B Consumer "
        "Products goodwill impairment in Q2 2025 with $44.9M in direct costs. "
        "In-house digital slate: EXODUS (sci-fi RPG) and WARLOCK: DUNGEONS & DRAGONS "
        "(third-person action, 100M+ trailer views). Stated transformation: "
        "'digital-first play and IP company' — recurring-revenue + high-margin "
        "digital + licensing weight more than physical toys."
    ),
    "strategic_building_blocks": [
        {
            "id": "aging_up",
            "label": "Aging Up (13+ collector economy)",
            "rubric": (
                "Does this signal skew toward 13+ collectors vs. kid-gift "
                "buyers? Signals from r/DnD, r/DMAcademy, r/magicTCG, BGG "
                "owned-counts, premium Amazon SKU rankings. Playing-to-Win "
                "pillar: 13+ consumers gaining purchase share."
            ),
        },
        {
            "id": "digital_and_direct",
            "label": "Digital & Direct (digital-first + D2C)",
            "rubric": (
                "Does this signal validate a digital-platform investment or "
                "a D2C channel (Hasbro PULSE, Secret Lair, D&D Beyond)? "
                "Watch: BG3 halo, MONOPOLY GO! ($168M FY25 via Scopely, up "
                "from $112M), EXODUS, WARLOCK: D&D, DMsGuild mithral-tier."
            ),
        },
        {
            "id": "universes_beyond",
            "label": "Universes Beyond (licensing pipeline)",
            "rubric": (
                "Non-D&D/non-MTG IP surging across Fandom / Reddit / YouTube "
                "/ Steam — ranked for Magic-crossover licensing fit. The "
                "flagship Arcane feature for Hasbro (Step 9.9 — UB Matrix). "
                "Magic's +59% was driven by this pipeline."
            ),
        },
        {
            "id": "partner_scale",
            "label": "Partner Scale (inbound/outbound licensing)",
            "rubric": (
                "Entertainment segment runs at 51.4% adjusted operating "
                "margin. Score signals for cross-IP resonance: HBO Baldur's "
                "Gate series, KPop Demon Hunters (Netflix), Harry Potter "
                "primary-toy, Voltron, Street Fighter are the current "
                "pipeline."
            ),
        },
        {
            "id": "operational_excellence",
            "label": "Operational Excellence ($1B cost savings by 2027)",
            "rubric": (
                "Not a direct signal input, but a routing prior: when the "
                "frame recommends reinvestment, benchmark against Hasbro's "
                "own winners (Wizards 46% adj op margin; Magic +59%). "
                "Approximately half of the $1B drops to bottom line."
            ),
        },
    ],
    "portfolio_taxonomy": ["Grow", "Optimize", "Reinvent"],
    "structural_taxonomy": [
        "Gamified",
        "Entertainment-driven",
        "Multi-purchase",
        "Multi-generational",
    ],
    # Conservative spine pending corpus-validated expansion. The four
    # that are load-bearing for the pitch are explicit in the Annual
    # Report's MD&A; others exist but don't appear in the pitch spine.
    "named_grow_brands": [
        "MAGIC: THE GATHERING",
        "DUNGEONS & DRAGONS",
        "MONOPOLY",
        "NERF",
        "TRANSFORMERS",
        "PLAY-DOH",
    ],
    "named_gem2_franchises": [
        "MAGIC: THE GATHERING",
        "DUNGEONS & DRAGONS",
        "TRANSFORMERS",
        "MONOPOLY",
    ],
    "benchmarks": {
        # Margin & growth reference numbers — "what good looks like"
        "wizards_digital_adj_op_margin_pct": 46.0,
        "entertainment_adj_op_margin_pct": 51.4,
        "magic_fy25_yoy_pct": 59.0,
        "grow_brand_tier_fy25_yoy_pct": 24.4,
        "magic_fy25_revenue_add_musd": 641.5,
        "entertainment_fy25_revenue_musd": 77.0,
        "monopoly_go_fy25_revenue_musd": 168.0,
        "monopoly_go_fy24_revenue_musd": 112.0,
        # Concentration & risk reference numbers
        "amazon_revenue_share_pct": 11.0,
        "walmart_revenue_share_pct": 9.0,
        "top5_customer_revenue_share_pct": 35.0,
        "consumer_products_goodwill_impairment_fy25_musd": 1020.0,
        "direct_tariff_cost_fy25_musd": 44.9,
        # MTG ecosystem reach
        "mtg_org_play_unique_players_2025": 1_000_000,
        "wpn_stores_2025": 10_000,
        # Playing-to-Win guidance (2025-2027)
        "ptw_revenue_cagr_target_pct_low": 4.0,
        "ptw_revenue_cagr_target_pct_high": 6.0,
        "ptw_adj_op_margin_expansion_bps_low": 50.0,
        "ptw_adj_op_margin_expansion_bps_high": 100.0,
        "ptw_gross_cost_savings_by_2027_musd": 1000.0,
        "ptw_gross_debt_ebitda_ratio_target_by_2026": 2.5,
    },
    "risks_on_watch": [
        {
            "id": "tariffs",
            "fact": (
                "Q2 2025 tariff implementation triggered a $1.02B "
                "Consumer Products goodwill impairment; $44.9M direct "
                "cost in FY25. Hasbro manufactures heavily in China — "
                "FBX01 (China→NAWC) and FBX03 (China→NAEC) freight "
                "indexes are the early-warning layer."
            ),
        },
        {
            "id": "concentration",
            "fact": (
                "Amazon 11% + Walmart 9% = 20% of global net revenue; "
                "top-5 customers = 35%. D2C push (PULSE, Secret Lair) is "
                "the structural response."
            ),
        },
        {
            "id": "dnd_decline",
            "fact": (
                "D&D underperformed the Grow Brand tier in FY25 despite "
                "being classified as one. Named explicitly in MD&A as a "
                "revenue-decline contributor alongside PLAY-DOH."
            ),
        },
        {
            "id": "ub_saturation",
            "fact": (
                "Universes Beyond is Magic's growth engine and the "
                "single-largest fan-backlash risk surface. Watch "
                "r/magicTCG, r/MTG, r/mtgfinance, r/EDH sentiment "
                "deltas — goodwill erosion precedes revenue erosion."
            ),
        },
        {
            "id": "digital_execution",
            "fact": (
                "EXODUS + WARLOCK: D&D are the in-house digital slate. "
                "Both are pre-launch; execution risk on trailer-to-"
                "revenue conversion is real. MONOPOLY GO! is the proof "
                "point Hasbro cites."
            ),
        },
        {
            "id": "cost_savings_execution",
            "fact": (
                "$1B gross cost savings by 2027 is Operational Excellence "
                "pillar. Approximately half drops to bottom line. "
                "Execution risk is internal — not an Arcane signal — but "
                "reinvestment routing is where our analytics lands."
            ),
        },
    ],
    "active_universes_beyond_pipeline": [
        "Final Fantasy",
        "Avatar: The Last Airbender",
        "Marvel's Spider-Man",
        "Edges of Eternities",
    ],
    "active_partnership_pipeline": [
        "HBO Baldur's Gate series",
        "KPop Demon Hunters (Netflix)",
        "Harry Potter — primary-toy license",
        "Voltron",
        "Street Fighter",
    ],
    "in_house_digital_slate": [
        "EXODUS",
        "WARLOCK: DUNGEONS & DRAGONS",
    ],
    # 6:1 default — most Track D articles are deck-ready (exec-safe,
    # McKinsey-register); one in seven is sharp (the ones that close
    # the sale, per project_hasbro_pitch_problems_solutions.md
    # § Tone calibration).
    "tone_distribution": {"deck_ready": 6, "sharp": 1},
    "guardrails": [
        # Every Track D article must treat players and shareholders as
        # distinct constituencies. Bursar speaks to shareholders;
        # Gamer Gary speaks to players; Sage synthesizes. Never blur.
        "fan_vs_shareholder",
        # Corporate strategy is observed + interpreted, never predicted.
        # "Watch this, because X" — not "this will cause Y."
        "strategy_not_prophecy",
    ],
}


def main() -> None:
    db = firestore.Client(project=PROJECT_ID)
    frames_ref = db.collection(FRAMES_COLLECTION)

    frames_ref.document(HASBRO_2026_FRAME["frame_id"]).set(
        HASBRO_2026_FRAME, merge=True
    )
    print(f"[OK] Wrote frames/{HASBRO_2026_FRAME['frame_id']}")

    # Deliberately do NOT touch frames/_meta.activeFrameId. Admin flips
    # via /admin/frames when ready for Track D to go live.
    print(
        "[OK] frames/_meta.activeFrameId left as-is — "
        "hasbro-2026 is seeded INACTIVE by design."
    )
    print("Done.")


if __name__ == "__main__":
    main()
