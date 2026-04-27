"""
Arcane Analytics — MTG Universes Beyond historical precedent database
(Stage 2a of the community_reception multi-source composite, Apr 27, 2026).

Hand-curated reception scoring for past Magic: The Gathering Universes
Beyond shipments. Each entry represents one IP × major release event.

Used as analogical input to the community_reception score: when scoring
a new IP candidate, the matcher finds the closest MTG UB precedents
(by IP attributes — medium, genre, fanbase profile) and uses their
reception as a leading indicator. This dataset speaks WotC's own
language — they lived through Walking Dead, they shipped LotR, they
saw Final Fantasy break records.

Companion table: dnd_crossover_precedents (seed_dnd_crossover_precedents.py)
covers D&D's own forward-direction crossover history (~6-8 entries).
The matcher weights D&D-precedent matches heavier (~0.65) than
MTG-UB-precedent matches (~0.35) when both apply, because same-community
signal is stronger than adjacent-community signal.

Curation note: reception_score uses the SAME [0.0, 1.0] scale as
community_reception (high = better). commercial_signal is intentionally
SEPARATE from reception_score — Walking Dead sold gangbusters but the
community reception was a revolt. The two dimensions need to be
independently surfaceable in the data trail.

Source notes for each entry cite where the reception assessment came
from (Reddit threads, financial reports, Hipsters of the Coast articles,
WotC follow-up product decisions, secondary-market price stability).
This is not bulletproof market research — it's curated analyst judgment,
labeled as such. Phil to spot-review entries before BQ load.

Run standalone to print summary stats:
    python scripts/seed_mtg_ub_precedents.py
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

ProductFormat = Literal[
    "full_set",          # Standard-legal set (LotR, FF, Avatar, Marvel)
    "secret_lair",       # Secret Lair drop (Walking Dead, Stranger Things, etc.)
    "commander_decks",   # IP-only Commander preconstructed launch (40K, Doctor Who, Fallout)
    "beyond_booster",    # The 2024 Beyond Booster format (Assassin's Creed)
    "standalone",        # Other (Edges of Eternities-class in-house UB analogs)
]

CommercialSignal = Literal[
    "boom",     # Record-breaking or genre-defining sales
    "strong",   # Above-typical UB performance
    "soft",     # Below expectations but not disaster
    "flop",     # Material commercial disappointment
    "unknown",  # Not enough public signal to score
]


@dataclass(frozen=True)
class MTGUBPrecedent:
    precedent_id: str           # Slug, e.g. "lotr_tales_2023"
    ip_name: str                # Joinable to ub_candidate_seeds when applicable
    set_name: str               # Display name of the actual product
    release_year: int
    product_format: ProductFormat
    reception_score: float      # [0.0, 1.0], same scale as community_reception
    commercial_signal: CommercialSignal
    community_signal_summary: str  # 1-2 sentences on the community reaction
    notable_controversies: str  # Comma-separated tags, "" if none
    source_notes: str           # Where the assessment came from
    curator_notes: str = ""     # Internal flags (uncertainty, conflicting signals)


# ════════════════════════════════════════════════════════════════════════
# THE CURATED PRECEDENTS
# ════════════════════════════════════════════════════════════════════════
#
# Organized roughly by reception spectrum (celebrated → divisive → revolt)
# within each product format, so spot-checking the calibration is easy.
# ════════════════════════════════════════════════════════════════════════

PRECEDENTS: list[MTGUBPrecedent] = [

    # ─── Full sets — the flagship UB products ─────────────────────────────

    MTGUBPrecedent(
        precedent_id="lotr_tales_2023",
        ip_name="Lord of the Rings",
        set_name="The Lord of the Rings: Tales of Middle-earth",
        release_year=2023,
        product_format="full_set",
        reception_score=0.95,
        commercial_signal="boom",
        community_signal_summary=(
            "The 'how UB should be done' precedent. Massive commercial "
            "success, deep flavor execution, even MTG purists conceded the "
            "set was lovingly made. The serialized 1-of-1 One Ring became "
            "a cultural moment beyond MTG."
        ),
        notable_controversies="serialized_unique_card_meta",
        source_notes=(
            "WotC quarterly disclosures, MTG community sentiment across "
            "r/magicTCG / r/EDH, Hipsters/StarCityGames coverage."
        ),
    ),

    MTGUBPrecedent(
        precedent_id="final_fantasy_2025",
        ip_name="Final Fantasy",
        set_name="Final Fantasy",
        release_year=2025,
        product_format="full_set",
        reception_score=0.90,
        commercial_signal="boom",
        community_signal_summary=(
            "Record sales per Hasbro's FY25 disclosures. JRPG audience "
            "overlap with Magic players proved as deep as forecast. "
            "Mechanical execution well-received; lore-faithful."
        ),
        notable_controversies="",
        source_notes=(
            "Hasbro FY2025 Annual Report MD&A explicitly cites FF as a "
            "growth driver. Community reception via r/magicTCG launch threads."
        ),
    ),

    MTGUBPrecedent(
        precedent_id="marvel_spiderman_2025",
        ip_name="Marvel",
        set_name="Marvel's Spider-Man",
        release_year=2025,
        product_format="full_set",
        reception_score=0.85,
        commercial_signal="strong",
        community_signal_summary=(
            "First Marvel UB set, broad mainstream pull. Reception strong "
            "but not euphoric — Marvel-fatigue concerns from non-Marvel "
            "MTG players surfaced. Commercial signal solid."
        ),
        notable_controversies="marvel_fatigue_discourse",
        source_notes=(
            "Hasbro FY25 Annual Report names it as 2025 UB driver. "
            "Reddit sentiment mixed-positive."
        ),
    ),

    MTGUBPrecedent(
        precedent_id="avatar_tla_2025",
        ip_name="Avatar: The Last Airbender",
        set_name="Avatar: The Last Airbender",
        release_year=2025,
        product_format="full_set",
        reception_score=0.88,
        commercial_signal="strong",
        community_signal_summary=(
            "Beloved property meeting beloved game. Avatar's clear faction "
            "structure (four nations) mapped naturally to MTG color pie. "
            "Strong reception across both fandoms."
        ),
        notable_controversies="",
        source_notes=(
            "Hasbro FY25 references; MTG community launch reception strong."
        ),
    ),

    MTGUBPrecedent(
        precedent_id="edges_of_eternities_2025",
        ip_name="Edges of Eternities",
        set_name="Edges of Eternities",
        release_year=2025,
        product_format="standalone",
        reception_score=0.75,
        commercial_signal="strong",
        community_signal_summary=(
            "In-house Hasbro IP that ACTS like UB structurally — sci-fi "
            "western space opera deliberately bridging UB-style audiences "
            "to Magic's owned worldbuilding. Reception positive; precedent "
            "for Hasbro-owned UB-shaped products."
        ),
        notable_controversies="ub_definition_drift",
        source_notes="Hasbro FY25 Annual Report; r/magicTCG launch coverage.",
        curator_notes=(
            "Edge case for the precedent DB — technically not third-party "
            "UB but functionally similar product positioning. Included for "
            "the 'Hasbro doing UB shape with own IP' signal."
        ),
    ),

    # ─── Commander deck suites — the IP-only Commander launches ──────────

    MTGUBPrecedent(
        precedent_id="warhammer_40k_commander_2022",
        ip_name="Warhammer 40K",
        set_name="Warhammer 40,000 Commander Decks",
        release_year=2022,
        product_format="commander_decks",
        reception_score=0.92,
        commercial_signal="boom",
        community_signal_summary=(
            "The canonical 'UB done right for a niche-but-deep fanbase' "
            "precedent. Four faction decks (Necron, Tyranid, Imperium, "
            "Chaos) sold out repeatedly; 40K community embraced the "
            "treatment. WotC's go-to reference for serving devoted niches."
        ),
        notable_controversies="reprint_scarcity_complaints",
        source_notes=(
            "Sustained reprint demand 2022-2024; r/Warhammer40k positive "
            "reception threads; Goonhammer coverage."
        ),
    ),

    MTGUBPrecedent(
        precedent_id="doctor_who_commander_2023",
        ip_name="Doctor Who",
        set_name="Doctor Who Commander Decks",
        release_year=2023,
        product_format="commander_decks",
        reception_score=0.78,
        commercial_signal="strong",
        community_signal_summary=(
            "Whovian community embraced strongly; broader MTG reception "
            "mid-positive — narrow niche IP played well to its base "
            "without dominating broader discourse. 'Time travel' mechanics "
            "novel and well-executed."
        ),
        notable_controversies="",
        source_notes=(
            "r/doctorwho positive reception; r/magicTCG niche-positive; "
            "secondary market price stability solid for niche product."
        ),
    ),

    MTGUBPrecedent(
        precedent_id="fallout_commander_2024",
        ip_name="Fallout",
        set_name="Fallout Commander Decks",
        release_year=2024,
        product_format="commander_decks",
        reception_score=0.80,
        commercial_signal="strong",
        community_signal_summary=(
            "Strong sales, well-executed flavor (radiation/rust/wasteland "
            "mechanics felt right). Bethesda fans engaged; MTG community "
            "broadly receptive. Sister-property dynamic (Hasbro adjacent "
            "to Bethesda licensing) didn't hurt."
        ),
        notable_controversies="",
        source_notes=(
            "Sustained reprint 2024; r/Fallout positive; r/magicTCG positive."
        ),
    ),

    MTGUBPrecedent(
        precedent_id="assassins_creed_2024",
        ip_name="Assassin's Creed",
        set_name="Assassin's Creed (Beyond Booster + Commander)",
        release_year=2024,
        product_format="beyond_booster",
        reception_score=0.45,
        commercial_signal="soft",
        community_signal_summary=(
            "The 'what is this product line' precedent. New Beyond Booster "
            "format confused players, power-level concerns, mid-level IP "
            "couldn't carry the format experiment. Community reception "
            "divisive; product-line discourse louder than IP discourse."
        ),
        notable_controversies="beyond_booster_format_confusion,power_level_concerns",
        source_notes=(
            "r/magicTCG critical threads on Beyond Booster format; "
            "soft secondary-market response; WotC reportedly reconsidering "
            "Beyond Booster format for future products."
        ),
    ),

    # ─── Secret Lair drops — the divisive testbed format ─────────────────

    MTGUBPrecedent(
        precedent_id="walking_dead_secret_lair_2020",
        ip_name="The Walking Dead",
        set_name="The Walking Dead Secret Lair",
        release_year=2020,
        product_format="secret_lair",
        reception_score=0.10,
        commercial_signal="boom",
        community_signal_summary=(
            "THE canonical UB community revolt. Mechanically-unique cards "
            "in a Secret Lair (i.e., not reprintable, must-buy for "
            "competitive players) kicked off the existential UB debate. "
            "WotC publicly committed to never repeat the mechanically-"
            "unique-in-Secret-Lair structure. Sales were strong but the "
            "community trust cost was material and lasting."
        ),
        notable_controversies=(
            "mechanically_unique_in_secret_lair,ub_legitimacy_debate,"
            "competitive_must_buy"
        ),
        source_notes=(
            "WotC public response committing to reprint pathway; "
            "extensive r/magicTCG / r/spikes contemporaneous threads; "
            "MaRo's Blogatog public engagement on the controversy."
        ),
    ),

    MTGUBPrecedent(
        precedent_id="stranger_things_secret_lair_2021",
        ip_name="Stranger Things",
        set_name="Stranger Things Secret Lair",
        release_year=2021,
        product_format="secret_lair",
        reception_score=0.45,
        commercial_signal="strong",
        community_signal_summary=(
            "Established the 'IP-themed Secret Lair' template post-Walking "
            "Dead controversy. Reception divisive — strong sales, but the "
            "'is MTG becoming a crossover game' anxiety surfaced again. "
            "Less revolt-tier than Walking Dead because cards were "
            "reprinted in mechanical form on standard cards."
        ),
        notable_controversies="ub_creep_anxiety",
        source_notes=(
            "Strong commercial reception; mixed-to-divisive r/magicTCG "
            "discourse; followed by the formal UB → Standard policy clarification."
        ),
    ),

    MTGUBPrecedent(
        precedent_id="fortnite_secret_lair_2023",
        ip_name="Fortnite",
        set_name="Fortnite Secret Lair",
        release_year=2023,
        product_format="secret_lair",
        reception_score=0.35,
        commercial_signal="soft",
        community_signal_summary=(
            "Tonal mismatch concerns dominated discourse. Fortnite's "
            "kid-skewing colorful aesthetic clashed with MTG's older-skewing "
            "audience self-image. Sales soft for a Secret Lair drop."
        ),
        notable_controversies="tonal_mismatch,age_demographic_mismatch",
        source_notes="r/magicTCG critical threads; soft secondary-market.",
    ),

    MTGUBPrecedent(
        precedent_id="street_fighter_secret_lair_2022",
        ip_name="Street Fighter",
        set_name="Street Fighter Secret Lair",
        release_year=2022,
        product_format="secret_lair",
        reception_score=0.65,
        commercial_signal="strong",
        community_signal_summary=(
            "Niche celebration. Fighting-game community modest but "
            "passionate; MTG players appreciated the clean execution and "
            "tournament-ladder lore mapping onto MTG's color pie."
        ),
        notable_controversies="",
        source_notes=(
            "r/StreetFighter positive; r/magicTCG niche-positive; "
            "secondary-market price stability above average for Secret Lair."
        ),
    ),

    MTGUBPrecedent(
        precedent_id="spongebob_secret_lair_2024",
        ip_name="SpongeBob SquarePants",
        set_name="SpongeBob SquarePants Secret Lair",
        release_year=2024,
        product_format="secret_lair",
        reception_score=0.30,
        commercial_signal="soft",
        community_signal_summary=(
            "Divisive. Cited frequently in 'UB has gone too far' "
            "discourse — SpongeBob in MTG read to many as the boundary "
            "case where the brand integrity argument finally bit. "
            "Some players defended the joke; many didn't."
        ),
        notable_controversies="ub_brand_integrity_threshold,tonal_mismatch",
        source_notes=(
            "r/magicTCG mixed-negative; cited in Hipsters of the Coast "
            "'where does UB stop' meta-discussion."
        ),
    ),

    MTGUBPrecedent(
        precedent_id="hatsune_miku_secret_lair_2024",
        ip_name="Hatsune Miku",
        set_name="Hatsune Miku Secret Lair",
        release_year=2024,
        product_format="secret_lair",
        reception_score=0.50,
        commercial_signal="strong",
        community_signal_summary=(
            "Vocaloid fanbase delivered strong sales; broader MTG reception "
            "split between 'fun, harmless' and 'this is what UB-creep "
            "looks like.' Secondary market firm."
        ),
        notable_controversies="anime_aesthetic_pushback",
        source_notes=(
            "Strong Vocaloid community engagement; mixed r/magicTCG; "
            "secondary-market price stability above-average for the format."
        ),
    ),

    MTGUBPrecedent(
        precedent_id="princess_bride_secret_lair_2024",
        ip_name="The Princess Bride",
        set_name="The Princess Bride Secret Lair",
        release_year=2024,
        product_format="secret_lair",
        reception_score=0.78,
        commercial_signal="strong",
        community_signal_summary=(
            "Beloved property meeting tabletop-friendly demographic. "
            "MTG community responded warmly — demographic alignment "
            "(literary, '80s nostalgia, board-gaming households) "
            "produced a near-universal positive read."
        ),
        notable_controversies="",
        source_notes=(
            "Strong sales; r/magicTCG positive; secondary-market firm."
        ),
    ),

    MTGUBPrecedent(
        precedent_id="jurassic_world_secret_lair_2022",
        ip_name="Jurassic World",
        set_name="Jurassic World Secret Lair",
        release_year=2022,
        product_format="secret_lair",
        reception_score=0.55,
        commercial_signal="strong",
        community_signal_summary=(
            "Mainstream pull strong; MTG-community-pure reception modest. "
            "Dinosaurs as a creature type already loved in MTG, so the "
            "thematic translation worked even if the broader IP "
            "association was lukewarm."
        ),
        notable_controversies="",
        source_notes="r/magicTCG mixed-positive; commercial signal solid.",
    ),

    MTGUBPrecedent(
        precedent_id="lotr_holiday_secret_lair_2023",
        ip_name="Lord of the Rings",
        set_name="The Lord of the Rings Holiday Release Secret Lair",
        release_year=2023,
        product_format="secret_lair",
        reception_score=0.85,
        commercial_signal="strong",
        community_signal_summary=(
            "Companion piece to Tales of Middle-earth full set. Reception "
            "celebrated — riding the same goodwill, complementary art "
            "treatments rather than competing with the full set."
        ),
        notable_controversies="",
        source_notes="r/magicTCG positive; secondary-market firm.",
    ),

    MTGUBPrecedent(
        precedent_id="transformers_brothers_war_2022",
        ip_name="Transformers",
        set_name="Transformers (in The Brothers' War)",
        release_year=2022,
        product_format="secret_lair",
        reception_score=0.70,
        commercial_signal="strong",
        community_signal_summary=(
            "Sister-property crossover (Hasbro IP within MTG) — "
            "Transformers cards bonus-sheet inserted into Brothers' War. "
            "Reception positive — well-executed double-faced mechanical "
            "design (robot ↔ alt-mode), Hasbro-internal synergy felt natural."
        ),
        notable_controversies="hasbro_internal_synergy_optics",
        source_notes=(
            "Bonus-sheet pricing held; r/magicTCG positive; cited as "
            "the 'right way' to do crossovers vs. Walking Dead."
        ),
    ),

    # ─── Other notable UB events ─────────────────────────────────────────

    MTGUBPrecedent(
        precedent_id="warhammer_40k_commander_followups_2024",
        ip_name="Warhammer 40K",
        set_name="Warhammer 40K follow-up Commander products",
        release_year=2024,
        product_format="commander_decks",
        reception_score=0.85,
        commercial_signal="strong",
        community_signal_summary=(
            "Sustained second wave of 40K Commander products — proves "
            "the original 2022 launch wasn't a one-off. 40K → MTG "
            "audience pipeline continues to deliver."
        ),
        notable_controversies="",
        source_notes="WotC follow-up product decisions; market signal sustained.",
    ),

    MTGUBPrecedent(
        precedent_id="doctor_who_followups_2024",
        ip_name="Doctor Who",
        set_name="Doctor Who follow-up Commander products",
        release_year=2024,
        product_format="commander_decks",
        reception_score=0.72,
        commercial_signal="strong",
        community_signal_summary=(
            "Follow-up products launched; modest but sustained Whovian "
            "engagement. Less euphoric than 40K follow-ups but the "
            "audience returned."
        ),
        notable_controversies="",
        source_notes="WotC follow-up product decisions; secondary-market stable.",
    ),

    MTGUBPrecedent(
        precedent_id="hp_tba",
        ip_name="Harry Potter",
        set_name="(announced; release TBD)",
        release_year=2026,
        product_format="full_set",
        reception_score=0.50,
        commercial_signal="unknown",
        community_signal_summary=(
            "Announced as forthcoming UB. Pre-release sentiment heavily "
            "polarized by author-association controversy — strong "
            "commercial pull expected, but community reception "
            "pre-pollution unusually high. Curator placeholder; "
            "revisit post-release."
        ),
        notable_controversies="author_controversy_polarization",
        source_notes=(
            "Pre-release Reddit sentiment threads; curated as placeholder."
        ),
        curator_notes=(
            "Speculative entry — assessment to be replaced with actual "
            "reception data on release. Currently used as 'high commercial "
            "/ uncertain reception' anchor for the matcher."
        ),
    ),

    MTGUBPrecedent(
        precedent_id="hasbro_pulse_premium_2023",
        ip_name="Hasbro PULSE Premium UB Lairs",
        set_name="PULSE-exclusive UB Secret Lair drops",
        release_year=2023,
        product_format="secret_lair",
        reception_score=0.40,
        commercial_signal="soft",
        community_signal_summary=(
            "PULSE-exclusive distribution model received critically — "
            "'pay to access UB' framing surfaced. Whatever IP was attached "
            "got pulled into the distribution-model controversy."
        ),
        notable_controversies="distribution_model_pushback",
        source_notes="r/magicTCG threads on PULSE distribution model.",
        curator_notes=(
            "Aggregate entry — multiple PULSE-exclusive drops shared the "
            "same reception headwind. Treat as 'distribution-model "
            "precedent' rather than IP-precedent."
        ),
    ),

    MTGUBPrecedent(
        precedent_id="aliens_secret_lair_2024",
        ip_name="Aliens",
        set_name="Aliens Secret Lair",
        release_year=2024,
        product_format="secret_lair",
        reception_score=0.62,
        commercial_signal="strong",
        community_signal_summary=(
            "Sci-fi horror translation worked — Xenomorph as creature "
            "type fit MTG's existing horror-tribal architecture. Niche "
            "celebration with broader-MTG mild approval."
        ),
        notable_controversies="",
        source_notes=(
            "r/LV426 positive; r/magicTCG niche-positive; secondary firm."
        ),
    ),

    MTGUBPrecedent(
        precedent_id="godzilla_ikoria_2020",
        ip_name="Godzilla",
        set_name="Godzilla Series Monsters (Ikoria)",
        release_year=2020,
        product_format="standalone",
        reception_score=0.72,
        commercial_signal="strong",
        community_signal_summary=(
            "First major IP crossover of the modern UB era — Godzilla "
            "monsters as alt-art treatments on Ikoria's existing kaiju. "
            "Reception positive — light-touch crossover (alt-art only, "
            "not mechanically distinct cards) avoided the Walking Dead trap."
        ),
        notable_controversies="",
        source_notes=(
            "r/magicTCG positive launch reception; precursor signal that "
            "alt-art-only crossovers carried less community-trust risk."
        ),
        curator_notes=(
            "Pre-formal-UB but considered the historical precedent for "
            "the program. Included for the 'how WotC learned UB worked' "
            "signal."
        ),
    ),

    MTGUBPrecedent(
        precedent_id="critical_role_secret_lair_2023",
        ip_name="Critical Role",
        set_name="Critical Role Secret Lair",
        release_year=2023,
        product_format="secret_lair",
        reception_score=0.80,
        commercial_signal="strong",
        community_signal_summary=(
            "Tabletop-adjacent IP in MTG — strong cross-fanbase pull "
            "(CR audience overlaps heavily with MTG/EDH players). "
            "Reception celebrated within EDH community especially."
        ),
        notable_controversies="",
        source_notes=(
            "r/EDH strongly positive; r/magicTCG positive; CR fanbase "
            "engagement sustained."
        ),
    ),
]


# ════════════════════════════════════════════════════════════════════════
# Distribution diagnostics
# ════════════════════════════════════════════════════════════════════════

def _summarize() -> None:
    print(f"Total precedents: {len(PRECEDENTS)}")
    print()

    print("By product_format:")
    fmt_counts: dict[str, int] = {}
    for p in PRECEDENTS:
        fmt_counts[p.product_format] = fmt_counts.get(p.product_format, 0) + 1
    for fmt, n in sorted(fmt_counts.items(), key=lambda kv: -kv[1]):
        print(f"  {fmt:20s}  {n:3d}")
    print()

    print("By reception band:")
    bands = [
        ("celebrated 0.90-1.0", lambda r: r >= 0.90),
        ("welcomed   0.70-0.89", lambda r: 0.70 <= r < 0.90),
        ("divisive   0.40-0.69", lambda r: 0.40 <= r < 0.70),
        ("rejected   0.00-0.39", lambda r: r < 0.40),
    ]
    for label, predicate in bands:
        n = sum(1 for p in PRECEDENTS if predicate(p.reception_score))
        print(f"  {label:25s}  {n:3d}")
    print()

    print("By commercial_signal:")
    cs_counts: dict[str, int] = {}
    for p in PRECEDENTS:
        cs_counts[p.commercial_signal] = cs_counts.get(p.commercial_signal, 0) + 1
    for cs, n in sorted(cs_counts.items(), key=lambda kv: -kv[1]):
        print(f"  {cs:10s}  {n:3d}")
    print()

    # The interesting cases — high commercial / low reception
    print("Strategic cases (commercial != reception):")
    for p in PRECEDENTS:
        if (p.commercial_signal in ("boom", "strong")
                and p.reception_score < 0.5):
            print(
                f"  HIGH COMMERCIAL / LOW RECEPTION  "
                f"{p.ip_name:30s}  reception={p.reception_score:.2f}  ({p.precedent_id})"
            )
    for p in PRECEDENTS:
        if p.commercial_signal == "soft" and p.reception_score >= 0.7:
            print(
                f"  SOFT COMMERCIAL / HIGH RECEPTION "
                f"{p.ip_name:30s}  reception={p.reception_score:.2f}  ({p.precedent_id})"
            )


if __name__ == "__main__":
    _summarize()
