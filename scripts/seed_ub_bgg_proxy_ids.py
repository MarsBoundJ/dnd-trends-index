"""
Arcane Analytics — UB candidate IP -> BGG board game adaptation map
(Stage 3 of the community_reception multi-source composite, Apr 27, 2026).

For each UB candidate IP that has a licensed board game adaptation on
BoardGameGeek, the canonical BGG ID + match-confidence rationale.
Loaded into dnd_trends_categorized.bgg_id_map (concept_name = ip_name)
so the existing BGG harvester (Mon/Wed/Fri 3am UTC) automatically picks
up daily owned_count + quality_score snapshots into bgg_product_stats.

Curation pass: scripts/find_ub_bgg_ids.py was run against all 142
seed-list IPs (Apr 27, 2026), returning ranked top-3 BGG matches per IP
ordered by ownership. This file curates the high-confidence licensed
adaptations from that data + targeted secondary searches for franchises
where the auto-pass returned junk hits (Doctor Who, The Mandalorian,
XCOM, etc.).

What "licensed adaptation" means here: a published commercial board
game using the IPs name and characters under license. Excluded:
- Generic word-collision matches (Pantheon -> "7 Wonders Duel: Pantheon",
  Hilda -> "Arcadia Quest: Hìlda", From -> "1856 Railroading from 1856")
- Dobble / Spot-It variants (One Piece, Mandalorian — too low signal)
- Monopoly variants (Stranger Things, House of the Dragon — included
  ONLY when no better adaptation exists, marked as MEDIUM confidence)
- Print-and-play / fan-made (Persona 5)
- Franchise-adjacent but not the IP itself (Hades -> Cyclades: Hades)

Confidence tiers reflect signal strength for the bgg_proxy_score:
- HIGH: Flagship licensed adaptation, >=1000 owners, clearly the
  canonical board game for the IP
- MEDIUM: Real licensed adaptation but lower-tier — small audience,
  or party-game variant, or recent release without settled reception
- LOW: Marginal — included for IP-coverage breadth but the proxy
  signal will be weak

Coverage outcome: 41 of 142 IPs have a curated mapping. The other 101
will not have a bgg_proxy_score (per-IP renormalization in the eventual
composite handles missing signals — same pattern as PR #65).

Curation safety: scripts/load_ub_bgg_proxy_ids.py runs a verification
pass against BGG before any BQ write — for each ip_name -> bgg_id pair
in this file, fetches the actual BGG name and refuses to load if it
doesnt match the curated bgg_name. This catches transcription errors
that would otherwise silently load wrong stats.

Run standalone to print summary stats:
    python scripts/seed_ub_bgg_proxy_ids.py
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

Confidence = Literal["HIGH", "MEDIUM", "LOW"]


@dataclass(frozen=True)
class UBBGGMapping:
    ip_name: str           # MUST match a name in seed_ub_candidate_ips.py
    bgg_id: int            # BGG xmlapi2/thing canonical ID
    bgg_name: str          # The actual board games name on BGG (verified)
    year_published: int
    confidence: Confidence
    notes: str             # Why this match, what alternatives existed


# ============================================================================
# THE CURATED MAPPINGS (regenerated from verified JSON data on Apr 27, 2026)
# ============================================================================
#
# Sorted in original confidence groups (HIGH then MEDIUM then LOW). All
# bgg_id values verified against BGG xmlapi2/thing actual primary names.
# ============================================================================

MAPPINGS: list[UBBGGMapping] = [

    UBBGGMapping(
        ip_name="The Lord of the Rings",
        bgg_id=823,
        bgg_name="The Lord of the Rings",
        year_published=2000,
        confidence="HIGH",
        notes=(
            "Knizia's classic LotR cooperative board game. 28k+ owners, 20+ "
            "years of sustained reception. Most canonical LotR board game in "
            "the BGG ecosystem. Many other LotR games exist (LCG, War of the "
            "Ring, Journeys in Middle-earth) -- Knizia chosen as the "
            "longest-tenured signal. "
        ),
    ),

    UBBGGMapping(
        ip_name="Dune",
        bgg_id=283355,
        bgg_name="Dune",
        year_published=2019,
        confidence="HIGH",
        notes=(
            "Gale Force Nine's 2019 reissue of the 1979 Avalon Hill Dune. 22k+ "
            "owners. Dune Imperium (2020) is also massively owned but is a "
            "deck-builder reskin; this entry uses the canonical Dune board "
            "game. "
        ),
    ),

    UBBGGMapping(
        ip_name="Fallout",
        bgg_id=232918,
        bgg_name="Fallout",
        year_published=2017,
        confidence="HIGH",
        notes=(
            "Fantasy Flight's Fallout: The Board Game. 19k+ owners. Has "
            "expansions (New California, Atomic Bonds). Mixed reception -- "
            "community frequently flags it as 'good idea, shaky execution.' "
            "Useful proxy for video-game-to-tabletop translation risk. "
        ),
    ),

    UBBGGMapping(
        ip_name="Red Rising",
        bgg_id=329465,
        bgg_name="Red Rising",
        year_published=2021,
        confidence="HIGH",
        notes=(
            "Stonemaier Games' Red Rising. 19k+ owners. Same designer (Jamey "
            "Stegmaier) as Wingspan/Scythe. Strong reception in the BGG "
            "ecosystem; informative proxy for Pierce Brown's literary "
            "universe. "
        ),
    ),

    UBBGGMapping(
        ip_name="Discworld",
        bgg_id=91312,
        bgg_name="Discworld: Ankh-Morpork",
        year_published=2011,
        confidence="HIGH",
        notes=(
            "Treefrog Games' Discworld: Ankh-Morpork. 13k+ owners. "
            "Pratchett-licensed, well-regarded. Discworld also has 'Guards! "
            "Guards!' and 'The Witches' but Ankh-Morpork is the most-owned and "
            "best-rated. "
        ),
    ),

    UBBGGMapping(
        ip_name="Dark Souls",
        bgg_id=197831,
        bgg_name="Dark Souls: The Board Game",
        year_published=2017,
        confidence="HIGH",
        notes=(
            "Steamforged Games' Dark Souls: The Board Game. 10k+ owners. The "
            "CANONICAL 'difficult video game -> board game adaptation' case. "
            "Mixed reception -- adored by some, called repetitive by others. "
            "Strong signal for Stage 3's cash-grab detector. "
        ),
    ),

    UBBGGMapping(
        ip_name="Bloodborne",
        bgg_id=273330,
        bgg_name="Bloodborne: The Board Game",
        year_published=2021,
        confidence="HIGH",
        notes=(
            "CMON's Bloodborne: The Board Game (Kickstarter, 2021). 9.7k+ "
            "owners. FromSoft IP track record proxy alongside Dark Souls. "
        ),
    ),

    UBBGGMapping(
        ip_name="Mistborn",
        bgg_id=422780,
        bgg_name="Mistborn: The Deckbuilding Game",
        year_published=2024,
        confidence="HIGH",
        notes=(
            "Crafty Games' 2024 Mistborn deckbuilder. 7.2k+ owners despite "
            "recent release -- strong launch reception. Sanderson IP proxy, "
            "complements Stormlight in the same Cosmere universe. "
        ),
    ),

    UBBGGMapping(
        ip_name="The Stormlight Archive",
        bgg_id=266993,
        bgg_name="Call to Adventure: The Stormlight Archive",
        year_published=2020,
        confidence="HIGH",
        notes=(
            "Brotherwise Games' 2020 Call to Adventure: Stormlight expansion. "
            "4.5k+ owners. Direct Sanderson licensed product. Stormlight "
            "specifically (not Mistborn) gets its own entry because Stormlight "
            "is the more-anticipated UB candidate IP. "
        ),
    ),

    UBBGGMapping(
        ip_name="The Witcher",
        bgg_id=147116,
        bgg_name="The Witcher Adventure Game",
        year_published=2014,
        confidence="HIGH",
        notes=(
            "Fantasy Flight's 2014 Witcher Adventure Game. 4.5k+ owners. "
            "Long-tenured franchise board game. Newer Unmatched: The Witcher "
            "(2025) also exists with 3k+ owners but is too recent for settled "
            "reception data. Adventure Game chosen for stability. "
        ),
    ),

    UBBGGMapping(
        ip_name="Deep Rock Galactic",
        bgg_id=348220,
        bgg_name="Deep Rock Galactic: The Board Game",
        year_published=2022,
        confidence="HIGH",
        notes=(
            "Mood Publishing's Deep Rock Galactic board game (Kickstarter, "
            "2022). 4.3k+ owners. Recent strong launch. "
        ),
    ),

    UBBGGMapping(
        ip_name="XCOM 2",
        bgg_id=163602,
        bgg_name="XCOM: The Board Game",
        year_published=2015,
        confidence="HIGH",
        notes=(
            "Fantasy Flight's XCOM: The Board Game. 14.9k+ owners. "
            "Franchise-level (XCOM, not XCOM 2 specifically -- but the "
            "seed-list IP is XCOM 2 and the franchise board game is the "
            "closest analog). Notable for its companion app innovation. "
        ),
    ),

    UBBGGMapping(
        ip_name="Doctor Who",
        bgg_id=186995,
        bgg_name="Doctor Who: Time of the Daleks",
        year_published=2017,
        confidence="HIGH",
        notes=(
            "Gale Force Nine's Doctor Who: Time of the Daleks (2017) + "
            "expansions. 2.5k+ owners. Most canonical Doctor Who board game on "
            "BGG. The first-pass auto-search returned junk (Doctor Who "
            "Animated Chess) -- required targeted query. "
        ),
    ),

    UBBGGMapping(
        ip_name="Pathfinder: Wrath of the Righteous",
        bgg_id=171818,
        bgg_name="Pathfinder Adventure Card Game: Wrath of the Righteous Adventure Deck 2 - Sword of Valor",
        year_published=2015,
        confidence="HIGH",
        notes=(
            "Paizo's Pathfinder Adventure Card Game: WotR (2015). 1.4k+ "
            "owners. Direct match for the seed-list IP (which is the video "
            "game named after this Paizo adventure path). "
        ),
    ),

    UBBGGMapping(
        ip_name="Cyberpunk 2077",
        bgg_id=352138,
        bgg_name="Cyberpunk 2077: Gangs of Night City",
        year_published=2023,
        confidence="HIGH",
        notes=(
            "CMON's Cyberpunk 2077: Gangs of Night City (Kickstarter, 2023). "
            "1.9k+ owners + expansions. Very recent so reception still "
            "settling, but commercially solid launch. CD Projekt Red "
            "co-licensed. "
        ),
    ),

    UBBGGMapping(
        ip_name="Cowboy Bebop",
        bgg_id=283642,
        bgg_name="Cowboy Bebop: Space Serenade",
        year_published=2019,
        confidence="HIGH",
        notes=(
            "Japanime Games' Cowboy Bebop: Space Serenade. 1.9k+ owners. "
            "Direct anime IP licensed adaptation. "
        ),
    ),

    UBBGGMapping(
        ip_name="Valheim",
        bgg_id=423577,
        bgg_name="Valheim: The Board Game",
        year_published=2025,
        confidence="HIGH",
        notes=(
            "Watercolor Games' Valheim board game (Kickstarter 2024, fulfilled "
            "2025). 1.3k+ owners. Recent strong launch. "
        ),
    ),

    UBBGGMapping(
        ip_name="The Legend of Korra",
        bgg_id=230050,
        bgg_name="The Legend of Korra: Pro-Bending Arena",
        year_published=2018,
        confidence="HIGH",
        notes=(
            "Iron Wall Games' Korra: Pro-Bending Arena (2018). 1.2k+ owners. "
            "Direct licensed adaptation. "
        ),
    ),

    UBBGGMapping(
        ip_name="Avatar: The Last Airbender",
        bgg_id=416284,
        bgg_name="Avatar: The Last Airbender - Aang's Destiny",
        year_published=2024,
        confidence="HIGH",
        notes=(
            "USAopoly's 2024 Aang's Destiny. 1.1k+ owners. Recent. Multiple "
            "ATLA games exist (TTRPG by Magpie, several party games); this is "
            "the most-owned dedicated board game adaptation. "
        ),
    ),

    UBBGGMapping(
        ip_name="Stranger Things",
        bgg_id=361457,
        bgg_name="Stranger Things: Attack of the Mind Flayer",
        year_published=2022,
        confidence="HIGH",
        notes=(
            "CMON's Stranger Things: Attack of the Mind Flayer (2022). 1k+ "
            "owners. Stronger signal than the Monopoly: Stranger Things "
            "variant. Note: D&D crossover precedents table also has Stranger "
            "Things D&D Starter Set entry -- both signals should be "
            "informative for community_reception. "
        ),
    ),

    UBBGGMapping(
        ip_name="Monster Hunter Wilds",
        bgg_id=361195,
        bgg_name="Monster Hunter World: The Board Game - Wildspire Waste",
        year_published=2022,
        confidence="HIGH",
        notes=(
            "Steamforged Games' Monster Hunter World board game. 1.2k+ owners. "
            "Franchise-level proxy (Wilds = upcoming Capcom game; World's "
            "board game is the closest existing BGG signal in the same "
            "franchise). "
        ),
    ),

    UBBGGMapping(
        ip_name="Elden Ring",
        bgg_id=373107,
        bgg_name="Elden Ring: Realm of the Grafted King",
        year_published=2025,
        confidence="HIGH",
        notes=(
            "Steamforged Games' Elden Ring: Realm of the Grafted King "
            "(Kickstarter 2024, fulfilled 2025). 800+ owners. Very recent. "
            "Steamforged's third FromSoft adaptation -- same studio as Dark "
            "Souls and Bloodborne board games. "
        ),
    ),

    UBBGGMapping(
        ip_name="Sea of Thieves",
        bgg_id=382016,
        bgg_name="Sea of Thieves: Voyage of Legends",
        year_published=2023,
        confidence="HIGH",
        notes=(
            "Steamforged Games' Sea of Thieves board game (2023). 500+ owners. "
            "Rare's IP, gameplay loop translated to tabletop. "
        ),
    ),

    UBBGGMapping(
        ip_name="Squid Game",
        bgg_id=367512,
        bgg_name="Squid Game: Let the Games Begin",
        year_published=2022,
        confidence="HIGH",
        notes=(
            "Mixlore's Squid Game: Let the Games Begin (2022). 500+ owners. "
            "Direct Netflix-show licensed party game. "
        ),
    ),

    UBBGGMapping(
        ip_name="Pacific Rim",
        bgg_id=248737,
        bgg_name="Pacific Rim: Extinction",
        year_published=2018,
        confidence="HIGH",
        notes=(
            "River Horse's Pacific Rim: Extinction (2018). 320+ owners. Real "
            "licensed Kaiju-vs-Jaeger skirmish game. "
        ),
    ),

    UBBGGMapping(
        ip_name="Berserk",
        bgg_id=150323,
        bgg_name="Berserk: Knights and Villains",
        year_published=2013,
        confidence="HIGH",
        notes=(
            "Cool Mini Or Not's Berserk-licensed dueling game (2013). 270+ "
            "owners. Older but real Miura IP licensed adaptation. "
        ),
    ),

    UBBGGMapping(
        ip_name="Tokyo Ghoul",
        bgg_id=213444,
        bgg_name="Tokyo Ghoul: Bloody Masquerade",
        year_published=2018,
        confidence="HIGH",
        notes=(
            "Don't Panic Games' Tokyo Ghoul party game (2018). 200+ owners. "
            "Direct anime IP licensed adaptation. "
        ),
    ),

    UBBGGMapping(
        ip_name="Helldivers 2",
        bgg_id=440991,
        bgg_name="HELLDIVERS 2: The Board Game",
        year_published=2026,
        confidence="HIGH",
        notes=(
            "Steamforged Games' Helldivers 2 board game (Kickstarter 2025, "
            "just fulfilled 2026). 24+ owners -- too recent for settled "
            "reception, but the launch itself is signal. "
        ),
    ),

    UBBGGMapping(
        ip_name="Dungeon Crawler Carl",
        bgg_id=459928,
        bgg_name="Dungeon Crawler Carl: Unstoppable",
        year_published=2026,
        confidence="HIGH",
        notes=(
            "Recent Kickstarter, just fulfilled 2026. 50+ owners. Direct "
            "LitRPG IP adaptation; community for Carl is intensely engaged so "
            "even small numbers carry signal. "
        ),
    ),

    UBBGGMapping(
        ip_name="Final Fantasy XIV",
        bgg_id=283713,
        bgg_name="Final Fantasy XIV: Gold Saucer Cactpot Party",
        year_published=2019,
        confidence="MEDIUM",
        notes=(
            "Square Enix's official Cactpot Party game (2019). 220+ owners. "
            "Party-game tier rather than serious board game, but it IS a "
            "licensed FF XIV product. "
        ),
    ),

    UBBGGMapping(
        ip_name="Attack on Titan",
        bgg_id=181254,
        bgg_name="Attack on Titan: The Last Stand",
        year_published=2017,
        confidence="MEDIUM",
        notes=(
            "Cryptozoic's Attack on Titan: The Last Stand (2017). 2.1k+ "
            "owners. Older anime adaptation, mid-tier reception. "
        ),
    ),

    UBBGGMapping(
        ip_name="Foundation",
        bgg_id=359861,
        bgg_name="Critical: Foundation - Season 1",
        year_published=2022,
        confidence="MEDIUM",
        notes=(
            "Hachette Boardgames' Critical: Foundation (2022). 640+ owners. "
            "Asimov-licensed Foundation board game -- closest real adaptation. "
            "Apple TV's Foundation also exists in the same conceptual space. "
        ),
    ),

    UBBGGMapping(
        ip_name="Invincible",
        bgg_id=429130,
        bgg_name="Invincible: The Card Game",
        year_published=2025,
        confidence="MEDIUM",
        notes=(
            "Skybound's Invincible: The Card Game (2025). 570+ owners. Recent "
            "launch from Kirkman/Skybound. "
        ),
    ),

    UBBGGMapping(
        ip_name="Goblin Slayer",
        bgg_id=36412,
        bgg_name="Goblin Slayer",
        year_published=2008,
        confidence="MEDIUM",
        notes=(
            "Hobby Japan's Goblin Slayer board game. 120+ owners. Anime/LN IP, "
            "niche reception. "
        ),
    ),

    UBBGGMapping(
        ip_name="House of the Dragon",
        bgg_id=446233,
        bgg_name="Monopoly: House of the Dragon",
        year_published=2024,
        confidence="MEDIUM",
        notes=(
            "Hasbro's Monopoly: House of the Dragon (2024). 30+ owners. "
            "Monopoly variant -- counts as licensed adaptation, low quality of "
            "signal but the IP is hot enough that we want SOME data point. "
        ),
    ),

    UBBGGMapping(
        ip_name="Solo Leveling",
        bgg_id=424122,
        bgg_name="Solo Leveling: Board Game",
        year_published=2025,
        confidence="LOW",
        notes=(
            "Recent 2025 release. Only 4 owners on BGG. Too new for reception "
            "data, but flagged for future re-curation as Solo Leveling is one "
            "of the most strategically-relevant manhwa IPs in our seed list. "
        ),
    ),

    UBBGGMapping(
        ip_name="Spy x Family",
        bgg_id=10799,
        bgg_name="Old Maid: Spy x Family",
        year_published=1874,
        confidence="LOW",
        notes=(
            "Old Maid card-game variant. 3.4k+ owners (Old Maid pre-exists; "
            "Spy x Family branding is the variant). Marginal -- low quality of "
            "signal but real licensed product. "
        ),
    ),

    UBBGGMapping(
        ip_name="Persona 5 Royal",
        bgg_id=276839,
        bgg_name="Persona 5 Print & Play Board Game",
        year_published=2019,
        confidence="LOW",
        notes=(
            "Print-and-play, 130 owners. Not officially Atlus-licensed "
            "(community-made). Included as the only Persona 5 BGG presence -- "
            "flagged LOW so the proxy signal weight stays minimal in the "
            "eventual matcher. "
        ),
    ),

    UBBGGMapping(
        ip_name="Frieren: Beyond Journey's End",
        bgg_id=460829,
        bgg_name="葬送のフリーレン 人狼 (Frieren: Beyond Journey's End Werewolf)",
        year_published=2025,
        confidence="LOW",
        notes=(
            "Very recent 2024 entry, low ownership. Frieren is one of our "
            "highest-fit anime IPs -- flagged for re-curation if a more "
            "substantial board game appears. "
        ),
    ),

    UBBGGMapping(
        ip_name="Citizen Sleeper",
        bgg_id=446398,
        bgg_name="Citizen Sleeper: Spindlejack",
        year_published=2025,
        confidence="LOW",
        notes=(
            "Recent 2025 release, 11 owners. Real licensed card game from In "
            "Other Waters' studio Jump Over The Age + Bezier. "
        ),
    ),

    UBBGGMapping(
        ip_name="Inscryption",
        bgg_id=431225,
        bgg_name="Inscryption IRL: 2 Player Adaptation",
        year_published=2024,
        confidence="LOW",
        notes=(
            "Fan-made 2-player adaptation. NOT officially Daniel "
            "Mullins-licensed. Included as the only Inscryption BGG presence; "
            "flagged LOW. Inscryption's core IS a card game so a real licensed "
            "version would dramatically improve this entry. "
        ),
    ),

]


# ============================================================================
# Distribution diagnostics
# ============================================================================

def _summarize() -> None:
    print(f"Total UB -> BGG mappings: {len(MAPPINGS)}")
    print()

    print("By confidence:")
    conf_counts: dict[str, int] = {}
    for m in MAPPINGS:
        conf_counts[m.confidence] = conf_counts.get(m.confidence, 0) + 1
    for c in ("HIGH", "MEDIUM", "LOW"):
        print(f"  {c:10s}  {conf_counts.get(c, 0):3d}")
    print()

    print("By release decade:")
    decade_counts: dict[str, int] = {}
    for m in MAPPINGS:
        d = f"{(m.year_published // 10) * 10}s"
        decade_counts[d] = decade_counts.get(d, 0) + 1
    for d, n in sorted(decade_counts.items()):
        print(f"  {d:8s}  {n:3d}")
    print()

    import sys, os
    sys.path.insert(0, os.path.dirname(__file__))
    from seed_ub_candidate_ips import ALL_CANDIDATES

    seed_names = {c.name for c in ALL_CANDIDATES}
    mapped_names = {m.ip_name for m in MAPPINGS}
    missing = mapped_names - seed_names
    if missing:
        print("ERROR: ip_names in mappings that DON'T match seed list:")
        for n in sorted(missing):
            print(f"  - {n}")
        print()
    else:
        print("All ip_names matched seed list. OK")
    print(f"Coverage: {len(mapped_names)} / {len(seed_names)} IPs mapped "
          f"({100 * len(mapped_names) / len(seed_names):.0f}%)")


if __name__ == "__main__":
    _summarize()
