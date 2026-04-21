"""
Arcane Analytics — Industry Fundamentals frame seed (Step 9.10).

Idempotent seed for `frames/industry-fundamentals` — the non-aligned
academic-observer frame paired with The Dean. Strategic priors are
the generic MBA-for-publishing playbook: unit economics, competitive
structure (Porter's Five Forces), disruption posture (Christensen),
platform dynamics (Thompson), market-boundary framing (Kim's Blue
Ocean). No buyer-specific worldview — that's what distinguishes this
frame from corporate-strategy frames like hasbro-2026.

Architecture: `is_corporate_strategy_frame()` gates on non-empty
portfolio_taxonomy; `is_industry_fundamentals_frame()` gates on
frame_id match. Keeping portfolio_taxonomy EMPTY here is load-bearing
— it's how the journalist distinguishes "Bursar writes Track D" from
"Dean writes Track C."

Seeded INACTIVE. The active-frame pointer at frames/_meta stays
wherever it is (pure-data by default) until an admin flips via
/admin/frames.

Run:
    python setup_industry_fundamentals_frame.py

Safety:
  - Idempotent via set(merge=True).
  - Does NOT touch frames/_meta.activeFrameId.
  - Requires ADC: `gcloud auth application-default login`.

Shape mirrors setup_hasbro_frame.py + setup_frames_collection.py so
the FrameSchema in arcane/src/lib/frames.ts validates without
surprises.
"""

from google.cloud import firestore

PROJECT_ID = "dnd-trends-index"
FRAMES_COLLECTION = "frames"


INDUSTRY_FUNDAMENTALS_FRAME = {
    "frame_id": "industry-fundamentals",
    "label": "Industry Fundamentals (non-aligned, academic register)",
    "source_docs": [
        # Generic canon descriptors — NOT named-individual attribution.
        # The specific luminary bench that drives The Dean's register
        # lives in council.py (internal prompt only). Naming those
        # individuals here, where this field is stored in Firestore
        # and could surface via admin UI, creates an unnecessary
        # false-endorsement surface. See 9.10.1 luminary-anonymity
        # rule in COUNCIL_HOUSE_RULES.
        "Classic corporate-strategy canon (Five Forces + Generic Strategies)",
        "Disruption-theory canon (Sustaining vs Disruptive Innovation, Jobs-to-be-done)",
        "Blue Ocean strategy canon (Value Innovation)",
        "Modern platform-economics analysis (Aggregation Theory, Zero Marginal Cost)",
        "Provocateur-economist register (Rundle, Tollbooth, Brand Moat)",
    ],
    "worldview_summary": (
        "Read every signal through the lens of structural industry "
        "analysis — the academic observer's perspective, not the "
        "shareholder's (carried by the Bursar) or the player's "
        "(carried by Gamer Gary). What market structure is the TTRPG "
        "segment actually inhabiting? Which of Porter's Five Forces "
        "is tightest — substitutes (open rule systems like Pathfinder "
        "ORC, LitRPG crossover), new entrants (indie VTTs, foreign-"
        "licensed TTRPG adaptations), buyer power (distributor "
        "concentration at Amazon + Walmart), supplier power (creators "
        "leaving incumbents for indie publishing), rivalry (Paizo, "
        "Free League, indie-first alternatives)? Which players are in "
        "the Sustaining-Innovation trap with legacy cash cows? Which "
        "Disruptive alternatives are climbing from below toward "
        "'good enough' for the mainstream? Where are aggregator plays "
        "forming (D&D Beyond, Roll20, Foundry as potential Aggregation-"
        "Theory-style aggregators of demand)? Which business models "
        "are transitioning from episodic transactions to recurring "
        "revenue (Rundle economics)? Name the framework you apply "
        "when you apply it. Observation over prediction. Structural "
        "over sentimental."
    ),
    "strategic_building_blocks": [
        {
            "id": "unit_economics",
            "label": "Unit Economics (CAC / LTV / Gross Margin)",
            "rubric": (
                "Does the signal speak to cost-of-acquisition, "
                "lifetime-value, gross vs contribution margin, or "
                "net revenue retention? Best-in-class SaaS benchmarks: "
                "LTV/CAC > 3, gross margin > 70%, NRR > 100%. TTRPG "
                "analogs: per-table book purchase frequency, "
                "DDB Master Tier subscription retention, FLGS "
                "margin compression."
            ),
        },
        {
            "id": "competitive_structure",
            "label": "Competitive Structure (Porter's Five Forces)",
            "rubric": (
                "Which force is tightest on the signal? Threat of "
                "substitutes (indie RPG systems, video-game "
                "alternatives), new entrants (first-time creators, "
                "Kickstarter), buyer power (retail concentration), "
                "supplier power (creator-talent leaving), rivalry "
                "among incumbents (WotC vs Paizo vs Free League). "
                "Name the dominant force."
            ),
        },
        {
            "id": "disruption_posture",
            "label": "Disruption Posture (Christensen)",
            "rubric": (
                "Is this a Sustaining-Innovation move (richer version "
                "of the existing product for the richest customers) "
                "or a Disruptive-Innovation move (scrappier, lower-"
                "margin, entering from below)? Which incumbents are "
                "trapped; which entrants are climbing? What "
                "job-to-be-done is the customer hiring the product "
                "to do?"
            ),
        },
        {
            "id": "platform_dynamics",
            "label": "Platform Dynamics (Thompson: Aggregation + Zero Marginal Cost)",
            "rubric": (
                "Is there an aggregator forming? Who owns the end-"
                "customer relationship in this distribution chain? "
                "Who is being commoditized? Which platforms have "
                "zero-marginal-cost leverage vs which still fight "
                "physical-world economics? The Smiling Curve valley-"
                "of-death: who is in the middle getting crushed?"
            ),
        },
        {
            "id": "market_boundary",
            "label": "Market Boundary (Blue Ocean vs Red Ocean)",
            "rubric": (
                "Is the signal about fighting inside a known, "
                "saturated market (Red Ocean — e.g., yet another 5e-"
                "compatible fantasy retro-clone) or redefining the "
                "boundary (Blue Ocean — e.g., the BG3-to-tabletop "
                "pipeline creating new non-consumer entry points)? "
                "Value Innovation asks: which axes are we competing "
                "on that customers don't actually value?"
            ),
        },
    ],
    # Deliberately empty — this frame is non-aligned, not tied to any
    # single buyer's brand portfolio. Corporate-strategy frames
    # (hasbro-2026) fill these; industry-fundamentals leaves them empty.
    "portfolio_taxonomy": [],
    "structural_taxonomy": [],
    "named_grow_brands": [],
    "named_gem2_franchises": [],
    "benchmarks": {
        # Illustrative anchors for "what good looks like" — Dean cites
        # these when the signal calls for calibration. Small set;
        # expand via Firestore edit as patterns emerge.
        "ltv_cac_ratio_bestinclass_saas": 3.0,
        "gross_margin_pct_bestinclass_saas": 70.0,
        "net_revenue_retention_pct_bestinclass_saas": 110.0,
        # TTRPG-adjacent benchmarks drawn from the Hasbro corpus when
        # relevant cross-pollination is useful.
        "wizards_digital_adj_op_margin_pct": 46.0,
        "entertainment_licensing_adj_op_margin_pct": 51.4,
    },
    "risks_on_watch": [],
    "active_universes_beyond_pipeline": [],
    "active_partnership_pipeline": [],
    "in_house_digital_slate": [],
    # 5:2 — academic register benefits from more provocation than
    # Hasbro's 6:1. The "sharp" articles in a Dean context are where
    # a structural insight lands uncomfortably — the framework names
    # something the industry hasn't said aloud. Worth one-in-three-ish.
    "tone_distribution": {"deck_ready": 5, "sharp": 2},
    "guardrails": [
        # Same as Hasbro frame — analyze, don't predict.
        "strategy_not_prophecy",
        # Dean-specific: when you apply a framework, name it. No
        # hand-waving analytic gestures ("it's a matter of competitive
        # dynamics"); name the specific framework ("Porter's rivalry-
        # among-competitors force is at its tightest here").
        "name_the_framework",
    ],
}


def main() -> None:
    db = firestore.Client(project=PROJECT_ID)
    frames_ref = db.collection(FRAMES_COLLECTION)

    frames_ref.document(INDUSTRY_FUNDAMENTALS_FRAME["frame_id"]).set(
        INDUSTRY_FUNDAMENTALS_FRAME, merge=True
    )
    print(f"[OK] Wrote frames/{INDUSTRY_FUNDAMENTALS_FRAME['frame_id']}")

    # Deliberately do NOT touch frames/_meta.activeFrameId. Admin flips
    # via /admin/frames when ready for Track C to go live.
    print(
        "[OK] frames/_meta.activeFrameId left as-is — "
        "industry-fundamentals is seeded INACTIVE by design."
    )
    print("Done.")


if __name__ == "__main__":
    main()
