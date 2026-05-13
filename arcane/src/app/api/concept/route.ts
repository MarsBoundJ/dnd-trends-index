/*
 * Trusight — Concept Detail API (Step 8).
 *
 * Server-side aggregation route that fetches data for a single concept
 * from multiple Bouncer endpoints and returns a unified payload for
 * the Concept Detail Drawer.
 *
 * This avoids the client making 10+ parallel requests. The route calls:
 *   1. /confidence?names=X — data confidence + methodology breakdown
 *   2. /leaderboards?source=google — Google Trends rank + score
 *   3. /leaderboards?source=fandom — Fandom wiki page views
 *   4. /leaderboards?source=wikipedia — Wikipedia page views
 *   5. /leaderboards?source=reddit — Reddit mentions + sentiment
 *   6. /leaderboards?source=youtube — YouTube creator consensus
 *   7. /leaderboards?source=bgg — BoardGameGeek
 *   8. /search?q=X — concept library metadata (category, active status)
 *
 * All calls run in parallel (Promise.allSettled) so a single slow/failed
 * source doesn't block the whole drawer.
 */

export const dynamic = "force-dynamic"

const BOUNCER_URL =
  process.env.NEXT_PUBLIC_BOUNCER_API_URL ??
  "https://us-central1-dnd-trends-index.cloudfunctions.net/bouncer-api"

// The data sources we query for cross-source ranking. These are the most
// valuable streams for the "evidence view" the spec describes.
const SOURCES = [
  "google",
  "fandom",
  "wikipedia",
  "reddit",
  "youtube",
  "bgg",
  "rpggeek",
  "amazon",
] as const

interface SourceAppearance {
  source: string
  score: number
  rank: number
  category: string
  /** Extra fields vary by source — sentiment, history, etc. */
  extra: Record<string, unknown>
}

interface ConceptDetailPayload {
  name: string
  /** Canonical concept name from the library (may differ in casing). */
  canonical_name: string | null
  category: string | null
  is_active: boolean | null
  /** Data confidence from /confidence endpoint. */
  confidence: {
    score: number
    tier: string
    explanation: Record<string, unknown> | null
  } | null
  /** Where this concept appears across data sources. */
  appearances: SourceAppearance[]
  /** Sources that were queried but had no data for this concept. */
  absent_from: string[]
}

async function fetchJson<T>(path: string): Promise<T | null> {
  try {
    const res = await fetch(`${BOUNCER_URL}${path}`, { cache: "no-store" })
    if (!res.ok) return null
    return (await res.json()) as T
  } catch {
    return null
  }
}

export async function GET(req: Request) {
  const { searchParams } = new URL(req.url)
  const name = searchParams.get("name")?.trim()

  if (!name) {
    return Response.json({ error: "name parameter is required" }, { status: 400 })
  }

  const nameLower = name.toLowerCase()

  // Fire all requests in parallel — don't let one slow source block everything.
  const [confidenceResult, searchResult, ...leaderboardResults] =
    await Promise.allSettled([
      // 1. Confidence data
      fetchJson<Record<string, {
        concept_name: string
        data_confidence: number
        tier: string
        explanation: Record<string, unknown> | null
      }>>(`/confidence?names=${encodeURIComponent(name)}`),

      // 2. Concept library search
      fetchJson<Array<{ name: string; category: string; is_active: boolean }>>(
        `/search?q=${encodeURIComponent(name)}`
      ),

      // 3-10. Leaderboard data per source
      ...SOURCES.map((source) =>
        fetchJson<Array<{ category: string; heat: number; items: Array<Record<string, unknown>> }>>(
          `/leaderboards?source=${source}`
        ).then((data) => ({ source, data }))
      ),
    ])

  // Extract confidence
  let confidence: ConceptDetailPayload["confidence"] = null
  if (confidenceResult.status === "fulfilled" && confidenceResult.value) {
    const entry = confidenceResult.value[nameLower]
    if (entry) {
      confidence = {
        score: entry.data_confidence,
        tier: entry.tier,
        explanation: entry.explanation,
      }
    }
  }

  // Extract concept library metadata
  let canonical_name: string | null = null
  let category: string | null = null
  let is_active: boolean | null = null
  if (searchResult.status === "fulfilled" && searchResult.value) {
    const match = searchResult.value.find(
      (c) => c.name.toLowerCase() === nameLower
    )
    if (match) {
      canonical_name = match.name
      category = match.category
      is_active = match.is_active
    }
  }

  // Extract cross-source appearances
  const appearances: SourceAppearance[] = []
  const absent_from: string[] = []

  for (const result of leaderboardResults) {
    if (result.status !== "fulfilled" || !result.value) continue
    const { source, data } = result.value as {
      source: string
      data: Array<{ category: string; items: Array<Record<string, unknown>> }> | null
    }
    if (!data) {
      absent_from.push(source)
      continue
    }

    // Search through all categories for this concept
    let found = false
    for (const cat of data) {
      for (const item of cat.items) {
        const itemName = String(item.name ?? "").toLowerCase()
        if (itemName === nameLower) {
          appearances.push({
            source,
            score: Number(item.score ?? item.consensus_score ?? 0),
            rank: Number(item.rank ?? 0),
            category: cat.category,
            extra: { ...item },
          })
          found = true
          break
        }
      }
      if (found) break
    }
    if (!found) {
      absent_from.push(source)
    }
  }

  // Sort appearances by score descending
  appearances.sort((a, b) => b.score - a.score)

  const payload: ConceptDetailPayload = {
    name,
    canonical_name,
    category,
    is_active,
    confidence,
    appearances,
    absent_from,
  }

  return Response.json(payload)
}
