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
