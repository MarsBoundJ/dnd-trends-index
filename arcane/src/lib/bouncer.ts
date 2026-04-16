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
