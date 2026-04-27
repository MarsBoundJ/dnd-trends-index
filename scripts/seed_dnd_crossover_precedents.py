"""
Arcane Analytics — D&D crossover precedent database (Stage 2b of the
community_reception multi-source composite, Apr 27, 2026).

Hand-curated reception scoring for D&D's own forward-direction crossover
products — IPs licensed INTO official 5e content. This is the
high-purity sibling of seed_mtg_ub_precedents.py: much sparser dataset,
but each entry is gold-standard signal because it's the SAME community
judging the SAME product line we're predicting reception for.

Companion table: ub_mtg_precedents (seed_mtg_ub_precedents.py) covers
~26 MTG Universes Beyond shipments. The matcher weights D&D-precedent
matches HEAVIER (~0.65) than MTG-UB-precedent matches (~0.35) when both
apply, with per-IP renormalization when only one type matches.

Phil's Apr 27 expansion ("can we get something similar with D&D
crossovers? Stranger Things, Rick and Morty?") — yes, and same-community
signal is strictly stronger than adjacent-community signal for our
specific question.

NOT IN SCOPE (separate feature for separate conversation):
- Reverse-direction D&D licensing (D&D licensed OUT to BG3, the HBO
  series, Honor Among Thieves movie, novels, board games, video games).
  That's a "how does D&D travel as IP" question for the broader Hasbro
  pitch — different feature.
- D&D-internal IP revivals (Dragonlance, Spelljammer 2022, Planescape
  2023). These aren't crossovers — they're WotC's own IP being
  re-shipped. Different signal entirely.

Curation note: the dataset is DELIBERATELY SPARSE. D&D has shipped
roughly 6-8 forward-direction crossovers in the past decade — that's
the entire population, not a sample. Adding lower-quality entries to
hit a count would dilute signal value.

Run standalone to print summary stats:
    python scripts/seed_dnd_crossover_precedents.py
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

ProductFormat = Literal[
    "boxed_starter_set",   # Stranger Things D&D Starter Set, Rick and Morty boxed
    "adventure_module",    # Critical Role: Call of the Netherdeep
    "setting_book",        # Critical Role: Explorer's Guide to Wildemount
    "supplement",          # Strixhaven: A Curriculum of Chaos
    "tie_in_companion",    # Movie/show tie-in books (cookbooks, lore guides)
    "comic_crossover",     # Rick and Morty vs. D&D comic series
]

CommercialSignal = Literal[
    "boom",      # Reprint waves, sustained sales years post-release
    "strong",    # Above-typical D&D supplement performance
    "soft",      # Below expectations
    "flop",      # Material disappointment
    "unknown",   # Limited-edition or hard-to-assess
]


@dataclass(frozen=True)
class DnDCrossoverPrecedent:
    precedent_id: str
    ip_name: str                # Joinable to ub_candidate_seeds when applicable
    product_name: str           # Display name of the actual product
    release_year: int
    product_format: ProductFormat
    reception_score: float      # [0.0, 1.0], same scale as community_reception
    commercial_signal: CommercialSignal
    community_signal_summary: str  # 1-2 sentences on the D&D community reaction
    notable_controversies: str  # Comma-separated tags, "" if none
    source_notes: str
    curator_notes: str = ""


# ════════════════════════════════════════════════════════════════════════
# THE CURATED PRECEDENTS
# ════════════════════════════════════════════════════════════════════════
#
# This is roughly the COMPLETE forward-direction D&D crossover catalog
# of the past decade, not a sample. Adding products below the "true
# crossover" bar would dilute the signal.
# ════════════════════════════════════════════════════════════════════════

PRECEDENTS: list[DnDCrossoverPrecedent] = [

    DnDCrossoverPrecedent(
        precedent_id="stranger_things_starter_2019",
        ip_name="Stranger Things",
        product_name="Stranger Things D&D Starter Set",
        release_year=2019,
        product_format="boxed_starter_set",
        reception_score=0.85,
        commercial_signal="strong",
        community_signal_summary=(
            "Celebrated. Show's existing D&D-DNA (the kids literally play "
            "D&D in the show) made the crossover feel earned rather than "
            "imposed. Demogorgon mini included; Hawkins-themed adventure "
            "module hit the right tonal register. Halo from Season 3 of "
            "the show drove sustained interest into D&D for casual fans."
        ),
        notable_controversies="",
        source_notes=(
            "r/DnD positive launch threads; sustained Reddit references "
            "for years post-release; cited by WotC as positive crossover "
            "precedent in subsequent press."
        ),
    ),

    DnDCrossoverPrecedent(
        precedent_id="rick_and_morty_vs_dnd_2019",
        ip_name="Rick and Morty",
        product_name="Dungeons & Dragons vs. Rick and Morty",
        release_year=2019,
        product_format="boxed_starter_set",
        reception_score=0.50,
        commercial_signal="strong",
        community_signal_summary=(
            "Divisive. Adult-irreverent humor of the show clashed with "
            "D&D's heroic-fantasy default register; some embraced the "
            "subversion, others read it as 'WotC pandering to crossover "
            "fans.' Boxed product sold well commercially but community "
            "reception split sharply along 'tone fit' lines."
        ),
        notable_controversies="adult_humor_tonal_mismatch,pandering_concerns",
        source_notes=(
            "r/DnD launch reception split; Critical Role and adjacent "
            "podcaster reactions mixed; commercial signal solid but "
            "community trust cost real."
        ),
    ),

    DnDCrossoverPrecedent(
        precedent_id="cr_wildemount_2020",
        ip_name="Critical Role",
        product_name="Critical Role: Explorer's Guide to Wildemount",
        release_year=2020,
        product_format="setting_book",
        reception_score=0.92,
        commercial_signal="boom",
        community_signal_summary=(
            "Celebrated. Critical Role's audience overlaps D&D's audience "
            "more deeply than any other IP — this was a homecoming, not "
            "a crossover. Dungeon Master Matthew Mercer's setting work "
            "received as legitimate D&D worldbuilding. Best-selling D&D "
            "setting book of its era; strong sustained engagement."
        ),
        notable_controversies="",
        source_notes=(
            "Bestseller list charting; r/DnD and r/criticalrole strongly "
            "positive; cited by WotC as model for creator-IP crossovers."
        ),
    ),

    DnDCrossoverPrecedent(
        precedent_id="cr_netherdeep_2022",
        ip_name="Critical Role",
        product_name="Critical Role: Call of the Netherdeep",
        release_year=2022,
        product_format="adventure_module",
        reception_score=0.85,
        commercial_signal="strong",
        community_signal_summary=(
            "Well-received follow-up to Wildemount. Adventure-module "
            "format reached the more typical D&D-buyer demographic "
            "(many DMs run published adventures; fewer buy setting books). "
            "Reception positive though slightly less euphoric than "
            "Wildemount — sequel-vs-original effect."
        ),
        notable_controversies="",
        source_notes=(
            "Strong sales; r/DnD and r/criticalrole positive; "
            "DM's Guild and DriveThruRPG ratings firm."
        ),
    ),

    DnDCrossoverPrecedent(
        precedent_id="strixhaven_2021",
        ip_name="Magic: The Gathering",
        product_name="Strixhaven: A Curriculum of Chaos",
        release_year=2021,
        product_format="supplement",
        reception_score=0.45,
        commercial_signal="strong",
        community_signal_summary=(
            "MTG plane (Strixhaven, the magical college setting) ported "
            "into D&D supplement. Divisive — D&D community reaction "
            "split on 'D&D doesn't need MTG settings' lines. Mechanical "
            "execution received decently; the existence of the product "
            "itself was the polarizing variable. The reverse-direction "
            "(MTG plane in D&D) inverts the typical crossover dynamic."
        ),
        notable_controversies=(
            "mtg_in_dnd_pushback,setting_proliferation_concerns,"
            "harry_potter_resemblance_discourse"
        ),
        source_notes=(
            "r/DnD mixed-negative launch threads; r/magicTCG mostly "
            "neutral (it was Magic players who didn't have to live with "
            "it as their game's content)."
        ),
        curator_notes=(
            "Reverse-direction crossover (MTG → D&D rather than third-"
            "party IP → D&D). Relevant precedent for any 'sister-property' "
            "IP scoring (e.g., Hasbro-internal crossovers like "
            "Transformers-flavored content into D&D would face similar "
            "discourse dynamics)."
        ),
    ),

    DnDCrossoverPrecedent(
        precedent_id="honor_among_thieves_tie_ins_2023",
        ip_name="Honor Among Thieves (D&D film)",
        product_name="Honor Among Thieves movie tie-in companion books",
        release_year=2023,
        product_format="tie_in_companion",
        reception_score=0.75,
        commercial_signal="strong",
        community_signal_summary=(
            "Heroes' Feast cookbook, lore companions, character guides. "
            "Well-received — film itself surprisingly well-received by "
            "D&D community, halo carried into companion products. "
            "Quality of writing and design above typical movie tie-in "
            "fare. Some pure-D&D-content purists ambivalent on the "
            "'tie-in' framing."
        ),
        notable_controversies="movie_tie_in_framing",
        source_notes=(
            "r/DnD positive on film and tie-in products; companion "
            "books rated firm on Amazon and Goodreads."
        ),
        curator_notes=(
            "Edge case — Honor Among Thieves is technically D&D's own "
            "IP being adapted to film, then back-licensed to companion "
            "products. Counts as crossover precedent because community "
            "reception dynamics (cross-media halo, tie-in skepticism) "
            "match third-party IP crossover patterns more than they "
            "match D&D-internal IP revivals."
        ),
    ),

    DnDCrossoverPrecedent(
        precedent_id="rick_and_morty_character_pack_2019",
        ip_name="Rick and Morty",
        product_name="Rick and Morty Character Sheet Pack",
        release_year=2019,
        product_format="tie_in_companion",
        reception_score=0.55,
        commercial_signal="soft",
        community_signal_summary=(
            "Companion to the boxed adventure. Character sheets featuring "
            "Rick, Morty, Summer, etc. as 5e PCs. Mostly novelty product "
            "for the boxed-set buyer; reception slightly more positive "
            "than the main boxed set because it didn't carry the "
            "'official adventure' weight."
        ),
        notable_controversies="",
        source_notes=(
            "Soft sales (limited-utility product); r/DnD niche-positive."
        ),
        curator_notes=(
            "Same-IP precedent as the boxed adventure — included to give "
            "the matcher TWO Rick and Morty data points showing the "
            "'mid-divisive' reception range."
        ),
    ),
]


# ════════════════════════════════════════════════════════════════════════
# Distribution diagnostics
# ════════════════════════════════════════════════════════════════════════

def _summarize() -> None:
    print(f"Total D&D crossover precedents: {len(PRECEDENTS)}")
    print()

    print("By product_format:")
    fmt_counts: dict[str, int] = {}
    for p in PRECEDENTS:
        fmt_counts[p.product_format] = fmt_counts.get(p.product_format, 0) + 1
    for fmt, n in sorted(fmt_counts.items(), key=lambda kv: -kv[1]):
        print(f"  {fmt:25s}  {n:3d}")
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

    print("By IP (for matcher cross-reference):")
    ip_counts: dict[str, int] = {}
    for p in PRECEDENTS:
        ip_counts[p.ip_name] = ip_counts.get(p.ip_name, 0) + 1
    for ip, n in sorted(ip_counts.items(), key=lambda kv: -kv[1]):
        marker = "  (multiple data points)" if n > 1 else ""
        print(f"  {ip:35s}  {n:3d}{marker}")


if __name__ == "__main__":
    _summarize()
