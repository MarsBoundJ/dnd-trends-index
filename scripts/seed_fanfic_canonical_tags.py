"""
Arcane Analytics — UB candidate IP -> AO3 / FFN canonical tag mappings
(Stage 4 of the community_reception multi-source composite, Apr 27, 2026).

For each high-priority UB IP, the canonical tag name AO3 uses (community-
maintained tag wrangling) and the FFN fandom ID (numeric, used in
crossover URLs like /[A]-and-[B]-Crossovers/[ID_A]/[ID_B]/).

Used by:
- The static install page (arcane/.../bookmarklets.html) — to render
  per-IP deep-link buttons that pre-fill the AO3 search URL with the
  right canonical tag and FFN crossover URL with the right fandom ID.
- The gold view consumer side — to join bookmarklet-captured counts
  back to the seed-list IP names.

Coverage: top ~25 IPs in the seed list, prioritized by demo relevance
to the Hasbro pitch. The other 117 IPs in the seed list can be
captured ad-hoc via the bookmarklet's prompt() fallback (Phil types
the IP name when there's no URL marker).

Curation note: AO3 tags are MAINTAINED by community tag wranglers and
can be reorganized over time. The mappings below are best-effort as of
Apr 27, 2026. Phil should re-verify any specific tag if the
bookmarklet's auto-detected canonical doesn't match expectations.

FFN fandom IDs are stable integers from FFN's URL routing. D&D = 1116.

Run standalone to print summary stats:
    python scripts/seed_fanfic_canonical_tags.py
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class FanficCanonical:
    ip_name: str               # MUST match seed_ub_candidate_ips.py
    ao3_tag: str               # AO3 canonical tag name (URL-encoded form)
    ffn_id: int | None = None  # FFN fandom ID (int) or None if unknown
    notes: str = ""


# ────────────────────────────────────────────────────────────────────────
# THE CURATED MAPPINGS (top ~25 demo-relevant IPs)
# ────────────────────────────────────────────────────────────────────────

D_AND_D_AO3 = "Dungeons & Dragons (Roleplaying Game)"
D_AND_D_FFN_ID = 1116

MAPPINGS: list[FanficCanonical] = [
    # ─── TV / film (high mainstream pull, big crossover fandoms) ──────────
    FanficCanonical(
        ip_name="Stranger Things",
        ao3_tag="Stranger Things (TV 2016)",
        ffn_id=11014103,  # FFN ID for the show — verify on first use
        notes="Show literally features kids playing D&D — high crossover expected.",
    ),
    FanficCanonical(
        ip_name="The Lord of the Rings",
        ao3_tag="The Lord of the Rings - All Media Types",
        ffn_id=382,
        notes="Largest fantasy literature fandom. Confirmed FFN id 382 from research.",
    ),
    FanficCanonical(
        ip_name="Avatar: The Last Airbender",
        ao3_tag="Avatar: The Last Airbender",
        ffn_id=4514,
        notes="Strong YA fantasy crossover potential.",
    ),
    FanficCanonical(
        ip_name="The Mandalorian",
        ao3_tag="The Mandalorian (TV)",
        ffn_id=None,
        notes="Star Wars universe — FFN id needs lookup.",
    ),
    FanficCanonical(
        ip_name="Doctor Who",
        ao3_tag="Doctor Who (2005)",
        ffn_id=None,
        notes="Multiple Doctor Who eras — using new-Who as primary.",
    ),
    FanficCanonical(
        ip_name="House of the Dragon",
        ao3_tag="House of the Dragon (TV)",
        ffn_id=None,
        notes="Recent show, GoT spinoff.",
    ),
    FanficCanonical(
        ip_name="Severance",
        ao3_tag="Severance (TV 2022)",
        ffn_id=None,
        notes="Apple TV+; AO3 fandom is small but present.",
    ),

    # ─── Video games (CRPG / FromSoft / mainstream gaming) ────────────────
    FanficCanonical(
        ip_name="Baldur's Gate 3",
        ao3_tag="Baldur's Gate (Video Games)",
        ffn_id=None,
        notes="AO3 tag covers the BG series broadly; BG3-specific tag may also exist.",
    ),
    FanficCanonical(
        ip_name="Cyberpunk 2077",
        ao3_tag="Cyberpunk 2077 (Video Game)",
        ffn_id=None,
        notes="",
    ),
    FanficCanonical(
        ip_name="The Witcher",
        ao3_tag="The Witcher (Video Games)",
        ffn_id=None,
        notes="Three umbrella tags exist (books, Netflix, video games).",
    ),
    FanficCanonical(
        ip_name="Dark Souls",
        ao3_tag="Dark Souls (Video Games)",
        ffn_id=None,
        notes="",
    ),
    FanficCanonical(
        ip_name="Bloodborne",
        ao3_tag="Bloodborne (Video Game)",
        ffn_id=None,
        notes="",
    ),
    FanficCanonical(
        ip_name="Elden Ring",
        ao3_tag="Elden Ring (Video Game)",
        ffn_id=None,
        notes="",
    ),
    FanficCanonical(
        ip_name="Final Fantasy XIV",
        ao3_tag="Final Fantasy XIV",
        ffn_id=None,
        notes="",
    ),
    FanficCanonical(
        ip_name="Persona 5 Royal",
        ao3_tag="Persona 5",
        ffn_id=None,
        notes="P5 Royal often crossover-tagged with main P5.",
    ),
    FanficCanonical(
        ip_name="Hades",
        ao3_tag="Hades (Video Game 2018)",
        ffn_id=None,
        notes="Disambiguates from Hades the Greek god.",
    ),

    # ─── Anime / manga (HUGE fanfic communities) ──────────────────────────
    FanficCanonical(
        ip_name="Attack on Titan",
        ao3_tag="Shingeki no Kyojin | Attack on Titan",
        ffn_id=None,
        notes="AO3 uses the original title with English translation appended.",
    ),
    FanficCanonical(
        ip_name="Jujutsu Kaisen",
        ao3_tag="Jujutsu Kaisen (Manga)",
        ffn_id=None,
        notes="",
    ),
    FanficCanonical(
        ip_name="Demon Slayer",
        ao3_tag="Kimetsu no Yaiba | Demon Slayer",
        ffn_id=None,
        notes="",
    ),
    FanficCanonical(
        ip_name="One Piece",
        ao3_tag="One Piece",
        ffn_id=None,
        notes="",
    ),
    FanficCanonical(
        ip_name="Spy x Family",
        ao3_tag="SPY x FAMILY",
        ffn_id=None,
        notes="AO3 uses the all-caps stylization.",
    ),

    # ─── Literature (smaller AO3 fandoms but signal-rich) ─────────────────
    FanficCanonical(
        ip_name="The Stormlight Archive",
        ao3_tag="The Stormlight Archive - Brandon Sanderson",
        ffn_id=None,
        notes="Author name appended for disambiguation.",
    ),
    FanficCanonical(
        ip_name="Mistborn",
        ao3_tag="Mistborn - Brandon Sanderson",
        ffn_id=None,
        notes="",
    ),
    FanficCanonical(
        ip_name="Dune",
        ao3_tag="Dune - All Media Types",
        ffn_id=None,
        notes="Covers Herbert books + 2021 film + earlier media.",
    ),
    FanficCanonical(
        ip_name="The Murderbot Diaries",
        ao3_tag="The Murderbot Diaries - Martha Wells",
        ffn_id=None,
        notes="",
    ),
    FanficCanonical(
        ip_name="Percy Jackson and the Olympians",
        ao3_tag="Percy Jackson and the Olympians & Related Fandoms - All Media Types",
        ffn_id=None,
        notes="Massive crossover fandom on AO3 — huge mythology overlap with D&D.",
    ),
]


def _summarize() -> None:
    print(f"Total fanfic canonical mappings: {len(MAPPINGS)}")
    print()
    print(f"D&D AO3 tag: '{D_AND_D_AO3}'")
    print(f"D&D FFN ID:  {D_AND_D_FFN_ID}")
    print()
    print(f"FFN ids known: {sum(1 for m in MAPPINGS if m.ffn_id)}")
    print(f"FFN ids TODO:  {sum(1 for m in MAPPINGS if not m.ffn_id)}")
    print()

    import sys, os
    sys.path.insert(0, os.path.dirname(__file__))
    from seed_ub_candidate_ips import ALL_CANDIDATES

    seed_names = {c.name for c in ALL_CANDIDATES}
    mapped_names = {m.ip_name for m in MAPPINGS}
    missing = mapped_names - seed_names
    if missing:
        print("ERROR: ip_names in mappings that DO NOT match seed list:")
        for n in sorted(missing):
            print(f"  - {n}")
    else:
        print("All ip_names matched seed list. OK")


if __name__ == "__main__":
    _summarize()
