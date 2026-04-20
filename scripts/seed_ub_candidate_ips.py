"""
Arcane Analytics — Universes Beyond Matrix seed list (Step 9.9, Chunk A).

Curated list of ~140 non-D&D/non-MTG IPs, deliberately distributed across
media so the Matrix ships with coverage beyond "video games WotC already
knows are hot." The distribution rule is LOAD-BEARING (guardrail #1 in
Yorri's 9.9 kickoff notes) and enforced as an assertion at import time —
the ratio cannot silently drift when IPs are added or removed.

Distribution contract:
    video_games:  30% <= frac <= 40%     (not >40%: anime/TV/literature can't be crowded out)
    anime_manga:  >= 15%                 (Delicious in Dungeon / Blue Lock / Frieren bucket)
    tv_film:      >= 15%
    literature:   >= 8%
    webtoons_kr:  >= 4%
    other:        >= 4%                  (podcasts, web animation, indie edge cases)

Two salience tiers on every IP:
    tier="winner"   — something WotC already knows is hot. ~30 of these
                      validate the "top 5 makes sense" verification criterion.
    tier="edge"     — on the edge of WotC's awareness. These are where the
                      Matrix earns its seat — ~90-120 of these.

Consumed by enrich_ub_candidates.py (Chunk B) which scores each IP on the
5-dimension rubric and writes to dnd_trends_raw.ub_candidate_enrichment.

Run standalone to print distribution + counts:
    python scripts/seed_ub_candidate_ips.py
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Literal

Tier = Literal["winner", "edge"]
Medium = Literal[
    "video_game", "anime_manga", "tv_film", "literature",
    "webtoon_kr", "other",
]


@dataclass(frozen=True)
class UBCandidateIP:
    name: str
    medium: Medium
    tier: Tier
    # Free-text notes that help the LLM enrichment disambiguate the IP
    # (useful for IPs with name collisions — e.g. "Foundation" the
    # Asimov novels vs. the Apple TV adaptation).
    disambiguation: str = ""
    # Step 9.9 Chunk C — trajectory-signal joins.
    # `steam_app_id` joins to dnd_trends_raw.steam_player_counts for
    # player-count velocity (the IP's "are people actually engaged?"
    # trajectory component). Populate for video-game IPs with Steam
    # presence; leave None for IPs without a Steam app or where
    # multiple apps would cause false signals (e.g. Dune has Spice
    # Wars + Awakening — better handled in 9.9.5 or a future
    # franchise-rollup pass). ~40 of the 53 video_game IPs have
    # Steam presence as of 2026-04-20.
    steam_app_id: int | None = None
    # `fandom_wiki_slug` joins to dnd_trends_raw.fandom_daily_metrics
    # for wiki view-count heat. Matches a slug in the
    # fandom_view_fetcher WIKI_REGISTRY + UB_CANDIDATE_WIKIS dicts.
    # Only populated for IPs we're explicitly harvesting — other IPs
    # land on the 9.9.5 trending-wikis auto-discovery path.
    fandom_wiki_slug: str | None = None


# ──────────────────────────────────────────────────────────────────────────
# VIDEO GAMES (~50 IPs — target 33-36% of total)
# ──────────────────────────────────────────────────────────────────────────
# Not the whole Steam top 100 — picks where a TTRPG licensing angle is
# plausible. Skip pure competitive esports (Counter-Strike, Dota), pure
# sports sims (FIFA), and pure sandbox (Minecraft — too generic for a
# TTRPG adaptation angle). Include: narrative RPGs, action-RPGs,
# tactical/strategy with rich world-building, survival with lore,
# soulslike, CRPG.

VIDEO_GAMES: list[UBCandidateIP] = [
    # Winners — obvious WotC-already-knows picks
    UBCandidateIP("Elden Ring", "video_game", "winner"),
    UBCandidateIP("Cyberpunk 2077", "video_game", "winner"),
    UBCandidateIP("Baldur's Gate 3", "video_game", "winner", "disambiguate from BG1/2"),
    UBCandidateIP("Helldivers 2", "video_game", "winner"),
    UBCandidateIP("Fallout", "video_game", "winner", "franchise (Bethesda games + Amazon TV) — one IP, games are canonical carrier"),
    UBCandidateIP("Final Fantasy XIV", "video_game", "winner"),
    UBCandidateIP("Final Fantasy XVI", "video_game", "winner"),
    UBCandidateIP("Persona 5 Royal", "video_game", "winner"),
    UBCandidateIP("The Witcher 3", "video_game", "winner"),
    UBCandidateIP("Hades", "video_game", "winner", "also Hades II"),
    UBCandidateIP("Monster Hunter Wilds", "video_game", "winner", "franchise"),

    # Soulslike / FromSoft adjacent — heavy TTRPG demographic overlap
    UBCandidateIP("Dark Souls", "video_game", "edge", "franchise incl. DS3"),
    UBCandidateIP("Sekiro: Shadows Die Twice", "video_game", "edge"),
    UBCandidateIP("Bloodborne", "video_game", "edge"),
    UBCandidateIP("Lies of P", "video_game", "edge"),
    UBCandidateIP("Nioh 2", "video_game", "edge"),

    # Action-RPG edge cases
    UBCandidateIP("Path of Exile 2", "video_game", "edge"),
    UBCandidateIP("Genshin Impact", "video_game", "edge"),
    UBCandidateIP("Honkai: Star Rail", "video_game", "edge"),
    UBCandidateIP("Wuthering Waves", "video_game", "edge"),
    UBCandidateIP("Destiny 2", "video_game", "edge"),
    UBCandidateIP("Warframe", "video_game", "edge"),

    # Indie narrative / cult — often have the strongest TTRPG crossover
    UBCandidateIP("Disco Elysium", "video_game", "edge"),
    UBCandidateIP("Pentiment", "video_game", "edge"),
    UBCandidateIP("Citizen Sleeper", "video_game", "edge"),
    UBCandidateIP("Norco", "video_game", "edge"),
    UBCandidateIP("Signalis", "video_game", "edge"),
    UBCandidateIP("Rain World", "video_game", "edge"),
    UBCandidateIP("Caves of Qud", "video_game", "edge"),
    UBCandidateIP("Inscryption", "video_game", "edge"),

    # Strategy / tactics — world-building-forward
    UBCandidateIP("XCOM 2", "video_game", "edge"),
    UBCandidateIP("Total War: Warhammer 3", "video_game", "edge"),
    UBCandidateIP("Warhammer 40,000: Rogue Trader", "video_game", "edge", "already Owlcat TTRPG-adaptation pedigree"),
    UBCandidateIP("Crusader Kings 3", "video_game", "edge"),
    UBCandidateIP("Triangle Strategy", "video_game", "edge"),
    UBCandidateIP("Unicorn Overlord", "video_game", "edge"),

    # Survival / sandbox with lore
    UBCandidateIP("Valheim", "video_game", "edge"),
    UBCandidateIP("Deep Rock Galactic", "video_game", "edge"),
    UBCandidateIP("Sea of Thieves", "video_game", "edge"),
    UBCandidateIP("Rimworld", "video_game", "edge"),
    UBCandidateIP("Dwarf Fortress", "video_game", "edge"),

    # CRPG pillars — adjacent to D&D itself
    UBCandidateIP("Pathfinder: Wrath of the Righteous", "video_game", "edge"),
    UBCandidateIP("Pillars of Eternity II: Deadfire", "video_game", "edge"),
    UBCandidateIP("Tyranny", "video_game", "edge"),
    UBCandidateIP("Divinity: Original Sin 2", "video_game", "edge"),

    # Platformer / metroidvania with world-building
    UBCandidateIP("Hollow Knight", "video_game", "edge"),
    UBCandidateIP("Hollow Knight: Silksong", "video_game", "edge", "Hollow Knight sequel"),

    # FPS / extraction-shooter — emerging TTRPG-adjacent subculture
    UBCandidateIP("Escape from Tarkov", "video_game", "edge"),
    UBCandidateIP("Hunt: Showdown", "video_game", "edge"),

    # Co-op / cult multiplayer
    UBCandidateIP("Lethal Company", "video_game", "edge"),
    UBCandidateIP("Balatro", "video_game", "edge"),

    # Narrative indie
    UBCandidateIP("Roadwarden", "video_game", "edge"),
    UBCandidateIP("Undertale", "video_game", "edge", "franchise incl. Deltarune"),
]


# ──────────────────────────────────────────────────────────────────────────
# TV / FILM (~30 IPs — target ~21%)
# ──────────────────────────────────────────────────────────────────────────

TV_FILM: list[UBCandidateIP] = [
    # Winners
    UBCandidateIP("Dune", "tv_film", "winner", "Villeneuve films + Frank Herbert books (literature row duplicated)"),
    UBCandidateIP("Severance", "tv_film", "winner"),
    UBCandidateIP("Arcane", "tv_film", "winner", "League of Legends animated"),
    UBCandidateIP("The Boys", "tv_film", "winner"),
    UBCandidateIP("Stranger Things", "tv_film", "winner"),
    UBCandidateIP("House of the Dragon", "tv_film", "winner", "GoT prequel"),
    UBCandidateIP("Shogun", "tv_film", "winner", "FX 2024"),
    UBCandidateIP("Foundation", "tv_film", "winner", "Apple TV adaptation"),
    UBCandidateIP("The Witcher", "tv_film", "winner", "Netflix adaptation"),
    UBCandidateIP("The Lord of the Rings: The Rings of Power", "tv_film", "winner"),
    UBCandidateIP("Andor", "tv_film", "winner", "Star Wars"),
    UBCandidateIP("Doctor Who", "tv_film", "winner"),
    UBCandidateIP("Invincible", "tv_film", "winner"),
    # Fallout (Amazon TV) consolidated into the video_game "Fallout" entry above —
    # one IP, games are the canonical licensing carrier. See assertion below.

    # Edge cases
    UBCandidateIP("Scavengers Reign", "tv_film", "edge", "HBO Max cult animated"),
    UBCandidateIP("Dead Boy Detectives", "tv_film", "edge", "Netflix, Sandman-adjacent"),
    UBCandidateIP("3 Body Problem", "tv_film", "edge", "Netflix adaptation"),
    UBCandidateIP("Percy Jackson and the Olympians", "tv_film", "edge", "Disney+"),
    UBCandidateIP("The Sandman", "tv_film", "edge"),
    UBCandidateIP("From", "tv_film", "edge", "MGM+ horror"),
    UBCandidateIP("Silo", "tv_film", "edge", "Apple TV"),
    UBCandidateIP("For All Mankind", "tv_film", "edge", "Apple TV alt-history"),
    UBCandidateIP("Pantheon", "tv_film", "edge", "AMC animated sci-fi"),
    UBCandidateIP("True Detective: Night Country", "tv_film", "edge", "S4 specifically"),
    UBCandidateIP("Pacific Rim", "tv_film", "edge"),
    UBCandidateIP("Avatar: The Last Airbender", "tv_film", "edge", "animated original, not live-action"),
    UBCandidateIP("The Legend of Korra", "tv_film", "edge"),
    UBCandidateIP("Over the Garden Wall", "tv_film", "edge"),
    UBCandidateIP("Hilda", "tv_film", "edge", "Netflix animated"),
    UBCandidateIP("Helluva Boss", "tv_film", "edge"),

    # Star Wars balance (Yorri Apr 20) — Andor alone too narrow.
    UBCandidateIP("The Mandalorian", "tv_film", "winner"),

    # Korean / Japanese live-action drama bucket (Yorri Apr 20) —
    # previously zero coverage.
    UBCandidateIP("Squid Game", "tv_film", "winner", "Korean live-action, winner-tier"),
    UBCandidateIP("Alice in Borderland", "tv_film", "edge", "Japanese live-action, strong combat_translatability angle"),
]


# ──────────────────────────────────────────────────────────────────────────
# ANIME / MANGA (~23 IPs — target ~16%)
# ──────────────────────────────────────────────────────────────────────────

ANIME_MANGA: list[UBCandidateIP] = [
    # Winners — big franchises
    UBCandidateIP("Cowboy Bebop", "anime_manga", "winner"),
    UBCandidateIP("Attack on Titan", "anime_manga", "winner"),
    UBCandidateIP("Jujutsu Kaisen", "anime_manga", "winner"),
    UBCandidateIP("One Piece", "anime_manga", "winner"),
    UBCandidateIP("Demon Slayer", "anime_manga", "winner"),
    UBCandidateIP("Spy x Family", "anime_manga", "winner"),

    # Explicit guardrail picks — the "edge-case surfacing" proof points
    UBCandidateIP("Delicious in Dungeon", "anime_manga", "edge", "Dungeon Meshi — literally D&D-adjacent"),
    UBCandidateIP("Blue Lock", "anime_manga", "edge", "sports-adjacent, unusual TTRPG angle"),
    UBCandidateIP("Frieren: Beyond Journey's End", "anime_manga", "edge", "post-adventure fantasy reflection"),
    UBCandidateIP("Chainsaw Man", "anime_manga", "edge"),
    UBCandidateIP("Vinland Saga", "anime_manga", "edge"),
    UBCandidateIP("Berserk", "anime_manga", "edge"),
    UBCandidateIP("Made in Abyss", "anime_manga", "edge"),
    UBCandidateIP("Mushoku Tensei", "anime_manga", "edge", "isekai with heavy TTRPG mechanics"),
    UBCandidateIP("Re:Zero", "anime_manga", "edge", "dark isekai"),
    UBCandidateIP("Overlord", "anime_manga", "edge", "isekai — literal TTRPG character in an isekai"),
    UBCandidateIP("Konosuba", "anime_manga", "edge", "isekai comedy with D&D-style party"),
    UBCandidateIP("Goblin Slayer", "anime_manga", "edge", "explicit TTRPG adaptation roots"),
    UBCandidateIP("That Time I Got Reincarnated as a Slime", "anime_manga", "edge"),
    UBCandidateIP("Mob Psycho 100", "anime_manga", "edge"),
    UBCandidateIP("Claymore", "anime_manga", "edge"),
    UBCandidateIP("Dorohedoro", "anime_manga", "edge"),
    UBCandidateIP("Tokyo Ghoul", "anime_manga", "edge"),
]


# ──────────────────────────────────────────────────────────────────────────
# LITERATURE (~18 IPs — target ~13%)
# ──────────────────────────────────────────────────────────────────────────

LITERATURE: list[UBCandidateIP] = [
    # Winners — fantasy/sci-fi with mass recognition
    UBCandidateIP("The Lord of the Rings", "literature", "winner", "Tolkien novels — distinct from film/TV"),
    UBCandidateIP("The Stormlight Archive", "literature", "winner", "Sanderson"),
    UBCandidateIP("Mistborn", "literature", "winner", "Sanderson Cosmere"),
    UBCandidateIP("The Wheel of Time", "literature", "winner", "Robert Jordan"),
    UBCandidateIP("Discworld", "literature", "winner", "Terry Pratchett"),

    # Edge cases — critically-loved TTRPG-ripe fantasy/sci-fi
    UBCandidateIP("The Locked Tomb", "literature", "edge", "Tamsyn Muir — Gideon/Harrow/Nona the Ninth"),
    UBCandidateIP("Malazan Book of the Fallen", "literature", "edge", "Erikson — literally a TTRPG campaign origin"),
    UBCandidateIP("Red Rising", "literature", "edge", "Pierce Brown"),
    UBCandidateIP("The Kingkiller Chronicle", "literature", "edge", "Rothfuss"),
    UBCandidateIP("The First Law", "literature", "edge", "Abercrombie"),
    UBCandidateIP("The Broken Earth", "literature", "edge", "N.K. Jemisin"),
    UBCandidateIP("The Poppy War", "literature", "edge", "R.F. Kuang"),
    UBCandidateIP("Children of Time", "literature", "edge", "Adrian Tchaikovsky"),
    UBCandidateIP("Sun Eater", "literature", "edge", "Christopher Ruocchio"),
    UBCandidateIP("The Murderbot Diaries", "literature", "edge", "Martha Wells"),
    UBCandidateIP("Cradle", "literature", "edge", "Will Wight — progression fantasy"),
    UBCandidateIP("Dungeon Crawler Carl", "literature", "edge", "Matt Dinniman — LitRPG, literally TTRPG-mechanics-as-plot"),
    UBCandidateIP("Blacktongue Thief", "literature", "edge", "Christopher Buehlman"),
]


# ──────────────────────────────────────────────────────────────────────────
# WEBTOONS / KOREAN (~8 IPs — target ~6%)
# ──────────────────────────────────────────────────────────────────────────

WEBTOONS_KR: list[UBCandidateIP] = [
    UBCandidateIP("Solo Leveling", "webtoon_kr", "winner", "webtoon origin — anime adaptation followed"),
    UBCandidateIP("Tower of God", "webtoon_kr", "edge"),
    UBCandidateIP("The God of High School", "webtoon_kr", "edge"),
    UBCandidateIP("Omniscient Reader's Viewpoint", "webtoon_kr", "edge", "novel-in-novel isekai"),
    UBCandidateIP("Noblesse", "webtoon_kr", "edge"),
    UBCandidateIP("Hardcore Leveling Warrior", "webtoon_kr", "edge", "literal MMORPG-mechanics webtoon"),
    UBCandidateIP("Unordinary", "webtoon_kr", "edge"),
    UBCandidateIP("Kill Six Billion Demons", "webtoon_kr", "edge", "Abbadon — note: western webcomic but TTRPG-native style"),
]


# ──────────────────────────────────────────────────────────────────────────
# OTHER (~8 IPs — target ~6%)
# ──────────────────────────────────────────────────────────────────────────
# Podcasts, web animation, and crossover IPs that don't fit the main buckets.

OTHER: list[UBCandidateIP] = [
    # Fiction podcasts — strong TTRPG-adjacent community overlap
    UBCandidateIP("Welcome to Night Vale", "other", "edge", "fiction podcast"),
    UBCandidateIP("The Magnus Archives", "other", "edge", "horror fiction podcast — heavy TTRPG overlap"),

    # Web animation / cartoon crossover
    UBCandidateIP("Murder Drones", "other", "edge", "GLITCH/Liam Vickers"),
    UBCandidateIP("Hazbin Hotel", "other", "edge"),

    # Netflix animated musical — explicit guardrail pick
    UBCandidateIP("Kpop Demon Hunters", "other", "winner", "Netflix 2025 — Hasbro pitch references this franchise"),

    # Kaiju — licensing-pipeline frame adjacent
    UBCandidateIP("Godzilla", "other", "winner", "Toho franchise — Monsterverse + Minus One + Legendary"),
    UBCandidateIP("Gamera", "other", "edge", "Kadokawa franchise"),

    # Cthulhu Mythos — non-D&D cosmic horror with established TTRPG (CoC)
    UBCandidateIP("Cthulhu Mythos", "other", "edge", "Lovecraftian — distinct from CoC the game"),
]


# ──────────────────────────────────────────────────────────────────────────
# Signal-stream join tables (Step 9.9 Chunk C)
# ──────────────────────────────────────────────────────────────────────────
# Flat dicts keyed by ip_name, applied via dataclasses.replace so the
# medium-bucket lists above stay scannable without inline plumbing.
# Add/remove entries freely — assertions at import time catch typos
# (dangling keys that don't match any IP).

# Steam app_ids for IPs with Steam presence. Populated 2026-04-20 from
# store.steampowered.com. Skipping: franchise-wide rollups (Dune has
# multiple games, LotR has Shadow of Mordor + LotR Online, etc.), games
# that don't ship through Steam (Genshin/HSR/Wuthering Waves via
# HoYoverse launcher; EFT via its own launcher; Bloodborne/Unicorn
# Overlord console-only). Single-app IPs only for v1 — franchise
# rollups belong in a later pass.
STEAM_APP_IDS: dict[str, int] = {
    # Winners
    "Elden Ring": 1245620,
    "Cyberpunk 2077": 1091500,
    "Baldur's Gate 3": 1086940,
    "Helldivers 2": 553850,
    "Fallout": 377160,               # Fallout 4 as franchise flagship
    "Final Fantasy XVI": 2515020,
    "Persona 5 Royal": 1687950,
    "The Witcher 3": 292030,
    "Hades": 1145360,
    "Monster Hunter Wilds": 2246340,
    # Soulslike / FromSoft adjacent
    "Dark Souls": 374320,            # DS3 as franchise flagship
    "Sekiro: Shadows Die Twice": 814380,
    "Lies of P": 1627720,
    "Nioh 2": 1325200,
    # Action-RPG
    "Path of Exile 2": 2694490,
    "Destiny 2": 1085660,
    "Warframe": 230410,
    # Indie narrative / cult
    "Disco Elysium": 632470,
    "Pentiment": 1205520,
    "Citizen Sleeper": 1608700,
    "Norco": 1221250,
    "Signalis": 1262350,
    "Rain World": 312520,
    "Caves of Qud": 333640,
    "Inscryption": 1092790,
    # Strategy / tactics
    "XCOM 2": 268500,
    "Total War: Warhammer 3": 1142710,
    "Warhammer 40,000: Rogue Trader": 2186680,
    "Crusader Kings 3": 1158310,
    "Triangle Strategy": 2170800,
    # Survival / sandbox
    "Valheim": 892970,
    "Deep Rock Galactic": 548430,
    "Sea of Thieves": 1172620,
    "Rimworld": 294100,
    "Dwarf Fortress": 975370,
    # CRPG
    "Pathfinder: Wrath of the Righteous": 1184370,
    "Pillars of Eternity II: Deadfire": 560130,
    "Tyranny": 362960,
    "Divinity: Original Sin 2": 435150,
    # Platformer / metroidvania
    "Hollow Knight": 367520,
    # FPS / extraction
    "Hunt: Showdown": 594650,
    # Co-op / cult multiplayer
    "Lethal Company": 1966720,
    "Balatro": 2379780,
    # Narrative indie
    "Roadwarden": 1155970,
    "Undertale": 391540,
}

# Fandom wiki slugs for ~20 non-TTRPG IPs we're adding to the Fandom
# harvester (cloud_functions/fandom_view_fetcher). Slugs verified as
# Fandom subdomains 2026-04-20. Note Doctor Who uses "tardis" as its
# wiki slug, not "doctorwho" — Fandom's naming is per-wiki-owner, not
# algorithmic. Any slug that 404s just returns zero articles and is
# logged by the harvester; low operational risk.
#
# Distribution across the seed list's 142 IPs: covers core winners +
# 2-3 explicit edge-case picks per non-video-game medium so the gold
# view always has fandom signal for the commercially-important IPs.
FANDOM_WIKI_SLUGS: dict[str, str] = {
    # Video games (confirming existing Fandom wiki coverage)
    "Cyberpunk 2077": "cyberpunk",
    "Elden Ring": "eldenring",
    "The Witcher 3": "witcher",
    "Warhammer 40,000: Rogue Trader": "warhammer40k",
    # TV / film — big franchises
    "Dune": "dune",
    "Severance": "severance",
    "Arcane": "arcane",
    "Stranger Things": "strangerthings",
    "House of the Dragon": "gameofthrones",    # HotD entries live under GoT wiki
    "The Lord of the Rings: The Rings of Power": "lotr",
    "Andor": "starwars",                       # Star Wars catch-all
    "The Mandalorian": "starwars",
    "Doctor Who": "tardis",                    # wiki slug is "tardis"
    # Anime / manga — guardrail picks + big franchises
    "Cowboy Bebop": "cowboybebop",
    # Delicious in Dungeon intentionally omitted (Apr 20 smoke test):
    # dungeon-meshi.fandom.com returned 0 top-articles via Playwright,
    # likely too new/small for Fandom's algorithmic top-articles surface.
    # The IP stays in the seed list — Reddit/YouTube signal carries the
    # trajectory; fandom_wiki_slug stays NULL so the gold view doesn't
    # JOIN empty signal. Re-add when a working slug is identified
    # (possibly via 9.9.5 auto-discovery).
    "Blue Lock": "bluelock",                    # guardrail pick
    "Frieren: Beyond Journey's End": "frieren",
    "Jujutsu Kaisen": "jujutsu-kaisen",
    "Attack on Titan": "attackontitan",
    "One Piece": "onepiece",
    "Chainsaw Man": "chainsawman",
    # Literature
    "The Lord of the Rings": "lotr",
    "The Stormlight Archive": "stormlightarchive",
    # Other
    "Kpop Demon Hunters": "kpop-demon-hunters",  # guardrail pick
    "Godzilla": "godzilla",
}


# ──────────────────────────────────────────────────────────────────────────
# Assembly + distribution enforcement
# ──────────────────────────────────────────────────────────────────────────

# Enrich the base dataclasses with the signal-stream joins. Frozen
# dataclass + dataclasses.replace keeps immutability while letting
# the top-of-file lists stay readable.
_base_candidates = (
    VIDEO_GAMES + TV_FILM + ANIME_MANGA + LITERATURE + WEBTOONS_KR + OTHER
)
ALL_CANDIDATES: list[UBCandidateIP] = [
    replace(
        c,
        steam_app_id=STEAM_APP_IDS.get(c.name),
        fandom_wiki_slug=FANDOM_WIKI_SLUGS.get(c.name),
    )
    for c in _base_candidates
]


def _verify_join_keys() -> None:
    """Catch typos in STEAM_APP_IDS / FANDOM_WIKI_SLUGS at import time.

    If a key doesn't match any IP name in the seed list, the mapping
    silently no-ops — which is exactly the kind of bug that's invisible
    until the gold view shows missing signal. Fail loud instead.
    """
    names = {c.name for c in _base_candidates}
    stray_steam = set(STEAM_APP_IDS) - names
    stray_fandom = set(FANDOM_WIKI_SLUGS) - names
    assert not stray_steam, (
        f"STEAM_APP_IDS has keys that don't match any IP name: "
        f"{sorted(stray_steam)}. Likely typos."
    )
    assert not stray_fandom, (
        f"FANDOM_WIKI_SLUGS has keys that don't match any IP name: "
        f"{sorted(stray_fandom)}. Likely typos."
    )


_verify_join_keys()


def _distribution_stats() -> dict[str, tuple[int, float]]:
    """Return {medium: (count, fraction)} for quick summary + assertions."""
    total = len(ALL_CANDIDATES)
    buckets: dict[str, int] = {}
    for c in ALL_CANDIDATES:
        buckets[c.medium] = buckets.get(c.medium, 0) + 1
    return {m: (n, n / total) for m, n in buckets.items()}


def _tier_stats() -> dict[str, int]:
    buckets: dict[str, int] = {}
    for c in ALL_CANDIDATES:
        buckets[c.tier] = buckets.get(c.tier, 0) + 1
    return buckets


def _enforce_distribution() -> None:
    """Assertions run at import time. Load-bearing per Yorri's guardrail #1.

    If adding an IP breaks a bucket, these assertions fail immediately —
    the operator MUST rebalance the list rather than silently skew it.
    """
    stats = _distribution_stats()
    total = len(ALL_CANDIDATES)

    # Total size
    assert 120 <= total <= 160, (
        f"Expected 120-160 total IPs, got {total}. "
        f"Rebalance or update this assertion deliberately."
    )

    # Name uniqueness. Learned 2026-04-20: "Fallout" accidentally landed
    # in both video_game and tv_film with different disambiguations,
    # which broke the BQ write into 143 rows for 142 unique IPs — the
    # gold view would have double-counted that IP's signals. A cross-
    # medium IP belongs to ONE row (usually the canonical licensing
    # carrier). Caught at import time rather than at enrichment time.
    names = [c.name for c in ALL_CANDIDATES]
    assert len(names) == len(set(names)), (
        f"Duplicate ip_name in seed list: "
        f"{[n for n in names if names.count(n) > 1]}. "
        f"Consolidate into a single entry (pick the canonical medium)."
    )

    # Video games: 30% ≤ frac ≤ 40%
    vg_frac = stats.get("video_game", (0, 0.0))[1]
    assert 0.30 <= vg_frac <= 0.40, (
        f"video_game fraction {vg_frac:.1%} outside [30%, 40%] "
        f"(target: anime/TV/literature cannot be crowded out)."
    )

    # Anime / manga ≥ 15%
    am_frac = stats.get("anime_manga", (0, 0.0))[1]
    assert am_frac >= 0.15, (
        f"anime_manga fraction {am_frac:.1%} below 15% floor — "
        f"edge-case surfacing (Delicious in Dungeon / Blue Lock / Frieren) "
        f"is why this bucket exists."
    )

    # TV / film ≥ 15%
    tf_frac = stats.get("tv_film", (0, 0.0))[1]
    assert tf_frac >= 0.15, (
        f"tv_film fraction {tf_frac:.1%} below 15% floor."
    )

    # Literature ≥ 8%
    lit_frac = stats.get("literature", (0, 0.0))[1]
    assert lit_frac >= 0.08, (
        f"literature fraction {lit_frac:.1%} below 8% floor."
    )

    # Webtoons ≥ 4%
    wt_frac = stats.get("webtoon_kr", (0, 0.0))[1]
    assert wt_frac >= 0.04, (
        f"webtoon_kr fraction {wt_frac:.1%} below 4% floor."
    )

    # Other ≥ 4%
    other_frac = stats.get("other", (0, 0.0))[1]
    assert other_frac >= 0.04, (
        f"other fraction {other_frac:.1%} below 4% floor."
    )

    # Winner tier target ~ 30 (guardrail: verify "top 5 makes sense").
    # Ceiling bumped 40 → 45 on 2026-04-20 when Mandalorian + Squid Game
    # landed as winner-tier (Yorri explicit additions). Raise again only
    # with explicit approval — edge-case density is what makes the
    # Matrix commercially valuable.
    winners = _tier_stats().get("winner", 0)
    assert 20 <= winners <= 45, (
        f"winner-tier IPs {winners} outside [20, 45]. "
        f"Too few: top-5 verification flaky. Too many: edge cases crowded out."
    )


_enforce_distribution()


# ──────────────────────────────────────────────────────────────────────────
# CLI summary
# ──────────────────────────────────────────────────────────────────────────

def main() -> None:
    print(f"Total IPs: {len(ALL_CANDIDATES)}")
    print()
    print("Distribution by medium:")
    for medium, (count, frac) in sorted(
        _distribution_stats().items(),
        key=lambda kv: -kv[1][0],
    ):
        print(f"  {medium:15s} {count:4d}  ({frac:5.1%})")
    print()
    print("Distribution by tier:")
    for tier, count in sorted(_tier_stats().items()):
        print(f"  {tier:15s} {count:4d}")


if __name__ == "__main__":
    main()
