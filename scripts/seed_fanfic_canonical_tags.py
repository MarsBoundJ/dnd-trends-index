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
    ao3_tag: str               # AO3 CANONICAL tag — see warning below
    ffn_id: int | None = None  # FFN fandom ID (int) or None if unknown
    notes: str = ""
    # False when AO3 cannot filter on this tag at all (tag exists but is not
    # marked "common"). Such an IP is UNMEASURABLE, not zero — the generator
    # skips it so it can never be recorded as a measured zero.
    ao3_filterable: bool = True
    # Date the ao3_tag was last confirmed canonical + filterable on AO3.
    ao3_verified_on: str = ""


# ────────────────────────────────────────────────────────────────────────
# ⚠️  ao3_tag MUST be AO3's CANONICAL tag, never a synonym.
#
# AO3 merges tags over time. A synonym still resolves when you *browse*
# /tags/<name>, so it looks perfectly healthy — but passed to
# work_search[other_tag_names] it matches nothing and the filter returns
# 0 results with no error. A stale tag is therefore indistinguishable
# from "this IP has no D&D crossover fic".
#
# That is not theoretical. On Sep 2, 2026, 4 of 26 seed tags (15%) had
# become synonyms and were all silently reporting 0:
#     The Witcher    0 -> 48
#     Jujutsu Kaisen 0 -> 54
#     Demon Slayer   0 -> 24
#     Spy x Family   0 ->  2
# Every failure was an English-only name whose canonical carries the
# original-language title (Wiedźmin, 呪術廻戦, 鬼滅の刃) or a suffix.
#
# To verify a tag, open https://archiveofourown.org/tags/<tag> and look for:
#   - a "Mergers" section  -> it is a SYNONYM; use the canonical it names
#   - "has not been marked common and can't be filtered on" -> set
#     ao3_filterable=False; the IP cannot be measured at all
#   - neither          -> canonical and filterable; stamp ao3_verified_on
#
# Treat any 0 from a capture as UNVERIFIED until the tag is re-checked.
# ────────────────────────────────────────────────────────────────────────


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
        ao3_tag="Avatar: The Last Airbender & Related Fandoms",
        ffn_id=4514,
        ao3_verified_on="2026-09-02",
        notes="Strong YA fantasy crossover potential. Bare 'Avatar: The Last "
              "Airbender' was NOT the canonical — corrected Sep 2 2026 from the "
              "AO3 fandom listing (64,589 works). Broadest Avatar tag available; "
              "no separate '- All Media Types' umbrella exists.",
    ),
    FanficCanonical(
        ip_name="The Mandalorian",
        ao3_tag="The Mandalorian (TV)",
        ffn_id=None,
        notes="Star Wars universe — FFN id needs lookup.",
    ),
    FanficCanonical(
        ip_name="Doctor Who",
        ao3_tag="Doctor Who & Related Fandoms",
        ffn_id=None,
        ao3_verified_on="2026-09-02",
        notes="LEVEL SWITCH Sep 2 2026 (work item D): was 'Doctor Who (2005)' "
              "(61,401 works, new-Who only). The umbrella carries 109,819 — it "
              "includes Classic Who and the spin-offs, which is the entity a "
              "licence would actually cover. This case was INVISIBLE until the "
              "same day, because is_umbrella only recognised '- All Media Types' "
              "and missed the '& Related Fandoms' form entirely.",
    ),
    FanficCanonical(
        ip_name="House of the Dragon",
        ao3_tag="House of the Dragon (TV)",
        ffn_id=None,
        notes="Recent show, GoT spinoff.",
    ),
    FanficCanonical(
        ip_name="Severance",
        ao3_tag="Severance (TV)",
        ffn_id=None,
        ao3_verified_on="2026-09-02",
        notes="MEASURABLE after all — ao3_filterable=False REVERTED Sep 2 2026. "
              "The earlier 'not marked common, can't be filtered on' reading was "
              "correct about the tag we were asking for ('Severance (TV 2022)') "
              "but that was never AO3's canonical. The canonical is "
              "'Severance (TV)' — listed, filterable, 2,361 works. The "
              "unmeasurable verdict was itself a stale-tag casualty; a wrong tag "
              "and an unfilterable tag produce the identical silent 0. "
              "Distinguish them by checking the fandom listing, not the tag page.",
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
        ao3_tag="Wiedźmin | The Witcher - All Media Types",
        ffn_id=None,
        ao3_verified_on="2026-09-02",
        notes="Three umbrella tags exist (books, Netflix, video games). "
              "'The Witcher (Video Games)' is a SYNONYM of this canonical and "
              "silently returned 0; corrected Sep 2 2026 -> 24 works (NOT 48 — "
              "that was April's value under the older, broader tag AO3 has since "
              "re-wrangled). Note the canonical is singular 'Video Game'. "
              "LEVEL SWITCH Sep 2 2026 (work item D): was "
              "'Wiedźmin | The Witcher (Video Game)' at 10,538 works — a QUARTER "
              "of the 42,482-work franchise. The largest level gap in the set.",
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
        ao3_tag="Hades (Supergiant Games Video Games)",
        ffn_id=None,
        ao3_verified_on="2026-09-02",
        notes="Disambiguates from Hades the Greek god. AO3 renamed this tag — "
              "'Hades (Video Game 2018)' was NOT the canonical; corrected Sep 2 "
              "2026 from the fandom listing (8,495 works).",
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
        ao3_tag="呪術廻戦 | Jujutsu Kaisen (Anime & Manga)",
        ffn_id=None,
        ao3_verified_on="2026-09-02",
        notes="'Jujutsu Kaisen (Manga)' is a SYNONYM of this canonical and "
              "silently returned 0; corrected Sep 2 2026 -> 54 works.",
    ),
    FanficCanonical(
        ip_name="Demon Slayer",
        ao3_tag="鬼滅の刃 | Demon Slayer: Kimetsu no Yaiba (Anime & Manga)",
        ffn_id=None,
        ao3_verified_on="2026-09-02",
        notes="'Kimetsu no Yaiba | Demon Slayer' is a SYNONYM of this canonical "
              "and silently returned 0; corrected Sep 2 2026 -> 24 works. The "
              "'&' encodes as %26 in other_tag_names (the *a* form is tag_id-only).",
    ),
    FanficCanonical(
        ip_name="One Piece",
        ao3_tag="One Piece - All Media Types",
        ffn_id=None,
        ao3_verified_on="2026-09-02",
        notes="Bare 'One Piece' was NOT the canonical. LEVEL SWITCH Sep 2 2026 "
              "(work item D): was 'One Piece (Anime & Manga)' at 99,968. The "
              "umbrella is 101,017 — only +1.0%, so this switch changes almost "
              "nothing numerically. It is made anyway, because the rule is "
              "'umbrella where AO3 provides one' and applying it only where the "
              "gap looks large would make the level a post-hoc judgement call "
              "rather than a rule.",
    ),
    FanficCanonical(
        ip_name="Spy x Family",
        ao3_tag="SPY x FAMILY - All Media Types",
        ffn_id=None,
        ao3_verified_on="2026-09-02",
        notes="AO3 uses the all-caps stylization. Bare 'SPY x FAMILY' is a "
              "SYNONYM of this canonical and silently returned 0; corrected "
              "Sep 2 2026 -> 2 works. LEVEL SWITCH same day (work item D): was "
              "'SPY x FAMILY (Manga)' at 8,053; umbrella is 8,899 (+10.5%).",
    ),

    # ─── Literature (smaller AO3 fandoms but signal-rich) ─────────────────
    FanficCanonical(
        ip_name="The Stormlight Archive",
        ao3_tag="Stormlight Archive - Brandon Sanderson",
        ffn_id=None,
        ao3_verified_on="2026-09-02",
        notes="Author name appended for disambiguation. AO3's canonical has NO "
              "leading 'The' — corrected Sep 2 2026 from the fandom listing "
              "(2,654 works). A one-word difference produced the same silent "
              "failure class as a full rename.",
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
