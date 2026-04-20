"""
Chronicler Track A — archetype detectors + daily routing.

The Chronicler doesn't pick his own story shape. This module does.
Each archetype has a `detect()` function that queries BigQuery, scores
what's interesting today, and returns a filled template (or None). The
top-scored templates get handed to `council.build_chronicler_prompt()`
for generation.

See `project_chronicler_story_archetypes.md` in user memory for the
full 12-archetype taxonomy. Five shipping in Step 9.6 (all use data
we already have in gold_data views):

  1. Spike / Anomaly        — view_trend_spikes
  2. Momentum / Acceleration — composite_trend_score.trend_momentum
  3. Divergence              — composite_hype_vs_play
  4. Saturation              — composite_blue_ocean (oversaturated quadrant)
  5. Leaderboard             — view_leaderboard_clean (+ observation about composition)

Not shipping in 9.6 (queued): Platform Arbitrage, Decay Curve, Sentiment
Inversion, Lifecycle State, Co-movement Cluster, Lead/Lag, Stream Health.
Each of those needs a new BigQuery view or composite.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from google.cloud import bigquery

PROJECT_ID = "dnd-trends-index"
DATASET_ID = "gold_data"

# Minimum salience for a candidate to be eligible for publication. Empty
# days are better than weak stories — see project_chronicler_story_archetypes.md
# "When a shipped archetype produces weak articles" policy.
MIN_SALIENCE = 40.0


@dataclass(frozen=True)
class ArchetypeCandidate:
    """A scored archetype hit with its template data ready for prompt injection."""

    archetype_id: str
    salience: float
    template: str
    raw_data: dict


# ---------------------------------------------------------------------------
# Detectors — one per archetype
# ---------------------------------------------------------------------------


def _detect_spike(bq: bigquery.Client) -> Optional[ArchetypeCandidate]:
    """Archetype 1: Spike / Anomaly.

    A concept's trend score jumped meaningfully in a short window. The salience
    tracks the absolute growth percentage, capped at 200 so a 500% spike
    doesn't monopolize every day's article.
    """
    sql = f"""
        SELECT keyword, current_score, previous_score, growth_pct
        FROM `{PROJECT_ID}.{DATASET_ID}.view_trend_spikes`
        WHERE growth_pct IS NOT NULL
          AND current_score >= 20
        ORDER BY growth_pct DESC
        LIMIT 1
    """
    rows = list(bq.query(sql).result())
    if not rows:
        return None
    row = dict(rows[0])
    growth_pct = float(row.get("growth_pct") or 0)
    if growth_pct < 25:
        return None

    template = (
        f"SPIKE: {row['keyword']} jumped {growth_pct:.1f}% "
        f"({row['previous_score']} -> {row['current_score']}). "
        f"State the jump. Note in one sentence whether an obvious trigger is "
        f"in the input data (creator spike, patch, edition news); if not, say "
        f"so plainly. Do NOT speculate beyond what the data shows."
    )
    return ArchetypeCandidate(
        archetype_id="spike",
        salience=min(growth_pct, 200.0),
        template=template,
        raw_data=row,
    )


def _detect_momentum(bq: bigquery.Client) -> Optional[ArchetypeCandidate]:
    """Archetype 2: Momentum / Acceleration.

    The fastest-moving concept, weighted against its current absolute level.
    A concept going from #1000 to #200 gets more salience than #3 to #2.
    """
    sql = f"""
        SELECT concept_name, category, combined_score, trend_momentum, trend_class
        FROM `{PROJECT_ID}.{DATASET_ID}.composite_trend_score`
        WHERE trend_momentum IS NOT NULL
          AND combined_score > 0
          -- Filter noisy rows: NULL/empty category usually means the row is a
          -- raw keyword from upstream scraping that never got mapped into the
          -- concept library (e.g. "CHRISTMAS BIG SET" fragments). These make
          -- for bad Chronicler articles because the concept isn't a concept.
          AND category IS NOT NULL
          AND TRIM(category) != ''
        ORDER BY trend_momentum DESC
        LIMIT 1
    """
    rows = list(bq.query(sql).result())
    if not rows:
        return None
    row = dict(rows[0])
    momentum = float(row.get("trend_momentum") or 0)
    if momentum < 15:
        return None

    # Salience: raw momentum * small bonus for lower absolute level (breakouts
    # are more interesting than incumbents getting slightly hotter).
    level = float(row.get("combined_score") or 1)
    underdog_bonus = 1.0 + (20.0 / max(level, 5.0))
    salience = momentum * underdog_bonus

    template = (
        f"MOMENTUM: '{row['concept_name']}' ({row['category']}) has the "
        f"highest 7-day acceleration in the index at +{momentum:.1f}%. "
        f"Current absolute score: {level:.1f}. "
        f"State the velocity, state the current level, note the category, "
        f"and note if input data hints at a driver (mod/video/thread). "
        f"Do NOT extrapolate a trajectory unless the data shows one."
    )
    return ArchetypeCandidate(
        archetype_id="momentum",
        salience=salience,
        template=template,
        raw_data=row,
    )


def _detect_divergence(bq: bigquery.Client) -> Optional[ArchetypeCandidate]:
    """Archetype 3: Divergence.

    Hype and play (or digital and physical) decouple. The salience tracks
    `hype_play_gap` — positive = talk without play, negative = play without
    talk. Both directions are interesting.
    """
    sql = f"""
        SELECT concept_name, category, divergence_class, conversion_signal,
               hype_score, play_score, hype_play_gap, talk_to_play_ratio
        FROM `{PROJECT_ID}.{DATASET_ID}.composite_hype_vs_play`
        WHERE hype_play_gap IS NOT NULL
          AND divergence_class IN ('hype_only', 'play_only', 'widening_gap')
          AND category IS NOT NULL AND TRIM(category) != ''
        ORDER BY ABS(hype_play_gap) DESC
        LIMIT 1
    """
    rows = list(bq.query(sql).result())
    if not rows:
        return None
    row = dict(rows[0])
    gap = float(row.get("hype_play_gap") or 0)
    if abs(gap) < 0.3:
        return None

    direction = "hype without play" if gap > 0 else "play without hype"
    template = (
        f"DIVERGENCE: '{row['concept_name']}' ({row['category']}) shows {direction}. "
        f"Hype score {row['hype_score']:.1f}, play score {row['play_score']:.1f}, "
        f"gap {gap:+.2f}. Divergence class: {row['divergence_class']}. "
        f"Name the concept, name the two streams, state the numbers, name the "
        f"direction. Do NOT prescribe — the point of this archetype is that the "
        f"numbers show a decoupling; the reader decides what to do with it."
    )
    return ArchetypeCandidate(
        archetype_id="divergence",
        salience=abs(gap) * 100,
        template=template,
        raw_data=row,
    )


def _detect_saturation(bq: bigquery.Client) -> Optional[ArchetypeCandidate]:
    """Archetype 4: Saturation / Cannibalization.

    A category where supply is high, demand is not keeping up. The inverse
    of blue ocean. `composite_blue_ocean` quadrants: we want oversaturated
    ones specifically.
    """
    sql = f"""
        SELECT concept_name, category, quadrant, demand_score, supply_score,
               demand_supply_gap, supply_gap_type
        FROM `{PROJECT_ID}.{DATASET_ID}.composite_blue_ocean`
        WHERE quadrant IN ('oversaturated', 'red_ocean')
          AND supply_score > demand_score
          AND category IS NOT NULL AND TRIM(category) != ''
        ORDER BY (supply_score - demand_score) DESC
        LIMIT 1
    """
    rows = list(bq.query(sql).result())
    if not rows:
        return None
    row = dict(rows[0])
    supply_over_demand = float(row.get("supply_score", 0) or 0) - float(
        row.get("demand_score", 0) or 0
    )
    if supply_over_demand < 10:
        return None

    template = (
        f"SATURATION: '{row['concept_name']}' ({row['category']}) sits in the "
        f"{row['quadrant']} quadrant. Supply score {row['supply_score']:.1f}, "
        f"demand score {row['demand_score']:.1f}, supply outruns demand by "
        f"{supply_over_demand:.1f}. Supply-gap type: {row['supply_gap_type']}. "
        f"State the category, state the supply/demand numbers, state the gap. "
        f"If the raw data shows a year-over-year project count, include it. "
        f"Do NOT tell creators to stop making this — just show the numbers."
    )
    return ArchetypeCandidate(
        archetype_id="saturation",
        salience=supply_over_demand,
        template=template,
        raw_data=row,
    )


def _detect_leaderboard(bq: bigquery.Client) -> Optional[ArchetypeCandidate]:
    """Archetype 5: Leaderboard (static + velocity observation).

    A top-N ranking PAIRED with an observation about what's unusual in the
    composition. A leaderboard without an observation is a ranking, not a
    story.
    """
    # Pick a single category with the most churn (high variation count) so
    # the story has texture. "Class" and "Spell" are usually rich categories.
    sql_cat = f"""
        SELECT category, COUNT(*) AS n_entries, AVG(score) AS avg_score
        FROM `{PROJECT_ID}.{DATASET_ID}.view_leaderboard_clean`
        WHERE score IS NOT NULL AND category IS NOT NULL
        GROUP BY category
        HAVING n_entries >= 10
        ORDER BY avg_score DESC
        LIMIT 5
    """
    cat_rows = list(bq.query(sql_cat).result())
    if not cat_rows:
        return None

    # Pick the highest-activity category
    category = cat_rows[0]["category"]

    sql_top = f"""
        SELECT display_name, score, variation_count
        FROM `{PROJECT_ID}.{DATASET_ID}.view_leaderboard_clean`
        WHERE category = @category AND score IS NOT NULL
        ORDER BY score DESC
        LIMIT 10
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("category", "STRING", category)]
    )
    top_rows = [dict(r) for r in bq.query(sql_top, job_config=job_config).result()]
    if len(top_rows) < 5:
        return None

    top_names = ", ".join(r["display_name"] for r in top_rows[:5])
    top_1 = top_rows[0]
    gap_to_next = float(top_1["score"] or 0) - float(top_rows[1]["score"] or 0)

    template = (
        f"LEADERBOARD ({category}): Top 5 = {top_names}. "
        f"#1 is '{top_1['display_name']}' at score {top_1['score']:.1f} "
        f"(lead over #2: {gap_to_next:.1f}). "
        f"List the top 5. State the #1-to-#2 gap. Make exactly ONE observation "
        f"about the composition ('3 of 5 are cantrips, unusual for this category' / "
        f"'first time an indie class has entered the top 10' / etc.) grounded in the "
        f"input data or prior context. If no observation is supported by the data, "
        f"skip the observation rather than invent one."
    )
    # Salience: leaderboards have middling-stable salience. Scale by lead size.
    salience = min(50.0 + gap_to_next, 90.0)
    return ArchetypeCandidate(
        archetype_id="leaderboard",
        salience=salience,
        template=template,
        raw_data={"category": category, "top": top_rows},
    )


# ---------------------------------------------------------------------------
# Daily routing
# ---------------------------------------------------------------------------

DETECTORS = [
    _detect_spike,
    _detect_momentum,
    _detect_divergence,
    _detect_saturation,
    _detect_leaderboard,
]


def pick_today(bq: bigquery.Client, k: int = 3) -> list[ArchetypeCandidate]:
    """Run every shipping archetype detector and return the top K candidates.

    K=3 typical: one for Flash, one for Standard, one as a backup in case
    the first hits validation retries. If fewer than K candidates clear
    MIN_SALIENCE, return what we have — caller handles "no stories today"
    as a valid no-op (empty day rather than a weak filler article).
    """
    candidates: list[ArchetypeCandidate] = []
    for detector in DETECTORS:
        try:
            cand = detector(bq)
        except Exception as err:
            # A single detector failing must not block the others. Log and
            # move on — silence beats broken archetype output.
            print(f"[chronicler] detector {detector.__name__} failed: {err}")
            continue
        if cand and cand.salience >= MIN_SALIENCE:
            candidates.append(cand)

    candidates.sort(key=lambda c: c.salience, reverse=True)
    return candidates[:k]
