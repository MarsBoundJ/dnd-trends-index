/*
 * Arcane Analytics — Bouncer API client.
 *
 * Typed fetch helper for the Bouncer REST API. All reads are server-side
 * (Next 16 Server Components) with 1-hour ISR revalidation.
 *
 * NOTE: Next.js 16 does NOT cache fetch() by default. The `next.revalidate`
 * option must be set explicitly on every call that should be cached.
 */

const BOUNCER_URL =
  process.env.NEXT_PUBLIC_BOUNCER_API_URL ??
  'https://us-central1-dnd-trends-index.cloudfunctions.net/bouncer-api'

// ─── Types ────────────────────────────────────────────────────────────────────

export interface BouncerItem {
  name: string
  score: number
  current_score: number
  rank: number
  source: string
  opportunity_index: number
}

export interface BouncerCategory {
  category: string
  heat: number
  items: BouncerItem[]
}

/**
 * Data-layer confidence payload for a single concept, as returned by
 * the Bouncer `/confidence` endpoint (Step 6, §5.1).
 *
 * The `explanation` field is the same JSON object that powers the
 * methodology popover — all factor values are already numeric/parsed.
 * See `gold_views/concept_confidence.sql` for the formula and
 * `FRONTEND_DESIGN_SPEC.md` §5.1/§5.2/§9.16 for the display contract.
 */
export interface ConfidenceEntry {
  concept_name: string
  data_confidence: number
  tier: "copper" | "silver" | "gold" | "platinum" | "mithral"
  explanation: {
    data: number
    ai_grounding: number
    displayed: number
    algo_version: string
    streams_present: number
    families_hit: number
    avg_stream_confidence: number
    agreement: number
    velocity_factor: number
    freshness_factor: number
    sparsity_cap_applied: boolean
    binding_constraint:
      | "single_source"
      | "limited_diversity"
      | "conflicting_signals"
      | "stale_consensus"
      | "thin_stream_data"
      | "strong"
  } | null
}

/**
 * Map keyed by LOWER(concept_name). Keys for unknown concepts are
 * simply absent — callers should fall back to the silver stub (75).
 */
export type ConfidenceMap = Record<string, ConfidenceEntry>

/**
 * A Council article row from `gold_data.daily_articles` filtered to
 * `council_version='v1'`. Step 9b — rendered by `ArticleCard`.
 *
 * Legacy 3-persona articles (persona = "Tavern Keeper" etc.) are excluded
 * by the Bouncer `/articles` endpoint. When the parallel-run window ends
 * and the legacy journalist is retired, this type stays unchanged.
 */
export type CouncilAuthorName =
  | "The Loremaster"
  | "The Bursar"
  | "The Quartermaster"
  | "The Weaver"
  | "The Architect"
  | "The Chronicler"
  // Gamer Gary breaks the "The X" convention on purpose — first-name
  // handle format parallels Sage, signaling the perspective-voice
  // (vs. the Council's expertise-titled voices). Added Step 9.7.
  | "Gamer Gary"
  // The Dean — 8th Council voice, academic-observer register. Lands in
  // Step 9.10 (Industry Fundamentals frame + Track C canonical). Voice
  // bench: Thompson + Galloway + HBR/McKinsey composite (Christensen
  // + Kim). Seeded in advance of 9.10 synthesis via a hand-crafted
  // reference article on 2026-04-21.
  | "The Dean"

/** Track A = Data Dispatches (The Chronicler); B = Council Takes; C =
 *  Fundamentals Reads; D = Corporate Strategy Reads. NULL on legacy rows
 *  that predate Step 9.5's schema migration. */
export type ArticleTrack = "A" | "B" | "C" | "D"

/** Flash = card-only (≤40 words headline+hook, empty body); Standard =
 *  60-280 word body; Report = long-form with tl;dr card + PDF (Step 9.11).
 *  NULL on legacy rows. */
export type ArticleLength = "flash" | "standard" | "report"

export interface Article {
  date: string
  author_name: CouncilAuthorName | string
  author_beat: string
  author_bio: string
  council_version: string
  headline: string
  hook: string
  body_markdown: string
  key_stat: string
  /** Added Step 9.5. NULL on articles written before the schema migration. */
  track?: ArticleTrack | null
  /** Added Step 9.6. NULL on articles written before the schema migration. */
  length?: ArticleLength | null
  /** Added Step 9.5. NULL when no active frame (or article predates the column). */
  frame_id?: string | null
  /**
   * Added Step 9.8. Secondary bylines when a single article is attributed
   * to more than one Council voice (corporate-strategy Track D articles
   * often touch multiple wheelhouses — e.g. Bursar + Weaver on a
   * Universes Beyond signal that's both licensing and digital-platform).
   *
   * Empty array (BigQuery default for ARRAY<STRING> on rows that predate
   * the schema migration or articles the journalist authored single-handed).
   * For Step 9.8 the journalist still writes single-author Track D
   * (Bursar-primary) — the co-byline prose pattern ships separately in
   * Step 9.8b. This field is plumbing-ready ahead of that step.
   */
  co_authors?: (CouncilAuthorName | string)[]
}

// ─── Leaderboards (multi-source) ─────────────────────────────────────────────

/**
 * Supported source keys for the `/leaderboards?source=X` Bouncer endpoint.
 * Each source returns the same top-level shape (`BouncerCategory[]`) but
 * items may carry source-specific extras (e.g. Reddit adds `sentiment`,
 * YouTube adds `consensus_score` + `creator_count`, BGG adds `owned`).
 * For the /overview page cards we only use the common fields (name, score,
 * rank); richer rendering by source is a later polish pass.
 */
export type LeaderboardSource =
  | "google"
  | "reddit"
  | "youtube"
  | "fandom"
  | "wikipedia"
  | "bgg"
  | "rpggeek"
  | "amazon"

/**
 * Fetch category/item leaderboards from the Bouncer /leaderboards endpoint,
 * scoped to a given source. Returns an empty array on error so consumer
 * pages still render with an empty-state rather than hard-failing.
 *
 * Cached server-side for 1 hour via Next 16 ISR, matching fetchBouncerData.
 */
export async function fetchLeaderboards(
  source: LeaderboardSource,
): Promise<BouncerCategory[]> {
  const url = `${BOUNCER_URL}/leaderboards?source=${source}`
  try {
    const res = await fetch(url, { next: { revalidate: 3600 } })
    if (!res.ok) {
      console.error(
        `[bouncer] /leaderboards?source=${source} returned ${res.status} ${res.statusText}`,
      )
      return []
    }
    const body = (await res.json()) as BouncerCategory[] | { error: string }
    if (!Array.isArray(body)) {
      console.error(`[bouncer] /leaderboards?source=${source} error payload:`, body)
      return []
    }
    return body
  } catch (err) {
    console.error(`[bouncer] /leaderboards?source=${source} fetch failed:`, err)
    return []
  }
}

/**
 * Helper: take leaderboard categories and return a flat top-N list of items
 * globally deduplicated by name. Used by the /overview cards where we want
 * one "top 5 across all categories for this source" rather than per-category.
 */
export function flattenTopItems(
  data: BouncerCategory[],
  limit = 5,
): BouncerItem[] {
  const byName = new Map<string, BouncerItem>()
  for (const cat of data) {
    for (const item of cat.items) {
      const existing = byName.get(item.name)
      if (!existing || item.score > existing.score) {
        byName.set(item.name, item)
      }
    }
  }
  return Array.from(byName.values())
    .sort((a, b) => b.score - a.score)
    .slice(0, limit)
}

// ─── Universes Beyond Matrix (Step 9.9) ──────────────────────────────────────

export type UBMedium =
  | "video_game"
  | "anime_manga"
  | "tv_film"
  | "literature"
  | "webtoon_kr"
  | "other"
export type UBTier = "winner" | "edge"

export interface UBRubricDetail {
  composite: number | null
  genre_fit: number | null
  combat_translatability: number | null
  party_dynamics_fit: number | null
  setting_portability: number | null
  fanbase_ttrpg_overlap: number | null
  confidence: number | null
  reasoning: string | null
}

export interface UBFandomDetail {
  wiki_slug: string | null
  hype_sum: number | null
  article_count: number | null
  norm: number | null
  available: boolean
}

export interface UBSteamDetail {
  app_id: number | null
  recent_players: number | null
  prior_players: number | null
  velocity: number | null
  norm: number | null
  available: boolean
}

export interface UBCandidate {
  ip_name: string
  medium: UBMedium
  tier: UBTier
  disambiguation: string | null
  license_fit_score: number | null
  rubric: UBRubricDetail
  fandom: UBFandomDetail
  steam: UBSteamDetail
}

export interface UBMatrixStatus {
  steam_gate_active: boolean
  coverage: {
    rubric: number
    fandom: number
    steam: number
    total: number
  }
  weights: {
    rubric: number
    fandom: number
    steam: number
  }
}

export interface UBMatrixResponse {
  status: UBMatrixStatus
  candidates: UBCandidate[]
}

/** Human-readable label for the medium enum. */
export const UB_MEDIUM_LABEL: Record<UBMedium, string> = {
  video_game: "Video game",
  anime_manga: "Anime / manga",
  tv_film: "TV / film",
  literature: "Literature",
  webtoon_kr: "Webtoon / Korean",
  other: "Other",
}

/**
 * Fetch ranked UB candidates from Bouncer `/universes-beyond-matrix`
 * (Step 9.9). Server-side fetch with 1-hour ISR (matches the rest of
 * the Bouncer client). Returns an empty response shape on API error
 * so the Matrix page still renders with an empty-state.
 */
export async function fetchUBMatrix(limit = 50): Promise<UBMatrixResponse> {
  const url = `${BOUNCER_URL}/universes-beyond-matrix?limit=${limit}`
  try {
    const res = await fetch(url, { next: { revalidate: 3600 } })
    if (!res.ok) {
      console.error(
        `[bouncer] /universes-beyond-matrix returned ${res.status} ${res.statusText}`,
      )
      return {
        status: {
          steam_gate_active: false,
          coverage: { rubric: 0, fandom: 0, steam: 0, total: 0 },
          weights: { rubric: 0, fandom: 0, steam: 0 },
        },
        candidates: [],
      }
    }
    return (await res.json()) as UBMatrixResponse
  } catch (err) {
    console.error("[bouncer] /universes-beyond-matrix fetch failed:", err)
    return {
      status: {
        steam_gate_active: false,
        coverage: { rubric: 0, fandom: 0, steam: 0, total: 0 },
        weights: { rubric: 0, fandom: 0, steam: 0 },
      },
      candidates: [],
    }
  }
}

// ─── Fetch ────────────────────────────────────────────────────────────────────

export async function fetchBouncerData(): Promise<BouncerCategory[]> {
  const res = await fetch(BOUNCER_URL, {
    next: { revalidate: 3600 }, // Re-fetch at most once per hour
  })
  if (!res.ok) {
    throw new Error(`Bouncer API error: ${res.status} ${res.statusText}`)
  }
  return res.json() as Promise<BouncerCategory[]>
}

/**
 * Fetch the latest N Council articles from Bouncer `/articles`. Step 9b.
 *
 * Server-side fetch, cached for 1 hour via Next 16 ISR (matches the rest
 * of the Bouncer client). The Bouncer endpoint filters to
 * `council_version='v1'`, so legacy 3-persona rows never appear here.
 *
 * Returns an empty array on any API error so the articles page still
 * renders with its empty state — avoids hard-failing the whole route
 * when the Cloud Function is cold or the table is empty.
 */
export async function fetchArticles(limit = 20): Promise<Article[]> {
  const url = `${BOUNCER_URL}/articles?limit=${limit}`
  try {
    const res = await fetch(url, { next: { revalidate: 3600 } })
    if (!res.ok) {
      console.error(
        `[bouncer] /articles returned ${res.status} ${res.statusText}`,
      )
      return []
    }
    const body = (await res.json()) as Article[] | { error: string }
    if (!Array.isArray(body)) {
      console.error("[bouncer] /articles error payload:", body)
      return []
    }
    return body
  } catch (err) {
    console.error("[bouncer] /articles fetch failed:", err)
    return []
  }
}

/**
 * Batch-fetch data confidence for a list of concept names from the
 * Bouncer `/confidence` endpoint (Step 6 Option B wiring).
 *
 * - Keys in the response are LOWER(concept_name). Missing names are
 *   silently absent — callers should fall back to the silver stub.
 * - Empty input short-circuits without a network call.
 * - On API error, returns an empty map so the page still renders
 *   (confidence just falls back to stubs everywhere).
 * - Cached server-side for 1 hour to match `fetchBouncerData()`.
 */
export async function fetchConfidence(
  names: string[],
): Promise<ConfidenceMap> {
  if (names.length === 0) return {}
  // Deduplicate case-insensitively before hitting the API.
  const unique = Array.from(new Set(names.map((n) => n.trim()).filter(Boolean)))
  if (unique.length === 0) return {}
  const url = `${BOUNCER_URL}/confidence?names=${encodeURIComponent(
    unique.join(","),
  )}`
  try {
    const res = await fetch(url, { next: { revalidate: 3600 } })
    if (!res.ok) {
      console.error(
        `[bouncer] /confidence returned ${res.status} ${res.statusText}`,
      )
      return {}
    }
    return (await res.json()) as ConfidenceMap
  } catch (err) {
    console.error("[bouncer] /confidence fetch failed:", err)
    return {}
  }
}

/**
 * Card-level confidence aggregation. Given a list of concept names
 * rendered inside one card and the confidence map from
 * `fetchConfidence()`, returns both:
 *   - `score`: the minimum `data_confidence` across looked-up concepts
 *     ("chain is as strong as its weakest link," matching the
 *     `displayed = min(data, ai_grounding)` philosophy from §5.1).
 *   - `binding`: the full ConfidenceEntry for the concept that set
 *     that minimum — fed into the methodology popover so users see
 *     *which* concept is dragging the aggregate down and *why*.
 *   - `hitCount`: how many names resolved. Drives the popover's
 *     "3 of 5 rendered concepts scored" disclosure.
 *
 * Fallback: if no names resolve against the map, returns
 * `{ score: 75, binding: null, hitCount: 0 }` so the card still
 * renders a silver pip with a "no confidence data" popover. This
 * preserves visual behavior for aggregate cards whose items aren't
 * in the concept library yet (e.g. category-name cards).
 */
export interface CardConfidenceResult {
  score: number
  binding: ConfidenceEntry | null
  hitCount: number
  totalCount: number
}

export function cardConfidence(
  names: string[],
  confidenceMap: ConfidenceMap,
  fallback = 75,
): CardConfidenceResult {
  const hits = names
    .map((n) => confidenceMap[n.toLowerCase()])
    .filter((e): e is ConfidenceEntry => e !== undefined)
  if (hits.length === 0) {
    return {
      score: fallback,
      binding: null,
      hitCount: 0,
      totalCount: names.length,
    }
  }
  const binding = hits.reduce((min, cur) =>
    cur.data_confidence < min.data_confidence ? cur : min,
  )
  return {
    score: binding.data_confidence,
    binding,
    hitCount: hits.length,
    totalCount: names.length,
  }
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

/**
 * Deduplicate items within a single category by name, keeping the
 * highest-scoring entry when duplicates exist.
 */
export function deduplicateItems(items: BouncerItem[]): BouncerItem[] {
  const byName = new Map<string, BouncerItem>()
  for (const item of items) {
    const existing = byName.get(item.name)
    if (!existing || item.score > existing.score) {
      byName.set(item.name, item)
    }
  }
  return Array.from(byName.values())
}

/** Find a category by name (case-insensitive). */
export function findCategory(
  data: BouncerCategory[],
  name: string,
): BouncerCategory | undefined {
  return data.find(
    (c) => c.category.toLowerCase() === name.toLowerCase(),
  )
}

/**
 * Return the top N items by opportunity_index across all categories,
 * globally deduplicated by name, excluding items with zero opportunity.
 */
export function topOpportunities(
  data: BouncerCategory[],
  limit = 5,
): BouncerItem[] {
  const byName = new Map<string, BouncerItem>()
  for (const cat of data) {
    for (const item of cat.items) {
      if (item.opportunity_index <= 0) continue
      const existing = byName.get(item.name)
      if (
        !existing ||
        item.opportunity_index > existing.opportunity_index
      ) {
        byName.set(item.name, item)
      }
    }
  }
  return Array.from(byName.values())
    .sort((a, b) => b.opportunity_index - a.opportunity_index)
    .slice(0, limit)
}

/**
 * Return categories sorted by heat descending, limited to `limit` entries.
 * Used by the Category Heat bar chart.
 */
export function topCategoriesByHeat(
  data: BouncerCategory[],
  limit = 10,
): { name: string; heat: number }[] {
  return [...data]
    .sort((a, b) => b.heat - a.heat)
    .slice(0, limit)
    .map((c) => ({ name: c.category, heat: Math.round(c.heat) }))
}
