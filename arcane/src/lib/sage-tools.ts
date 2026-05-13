/*
 * Trusight — Sage Tool Definitions (Step 7).
 *
 * ~8 tools that let the Sage query live BigQuery data via the Bouncer
 * API instead of hallucinating numbers. Each tool has:
 *   - A Zod v4 inputSchema describing the parameters
 *   - An async execute() function that calls the Bouncer and returns data
 *   - A description that tells the model when to use it
 *
 * The tools are server-side only — they run in the API route handler,
 * not in the browser. The Bouncer API is the single data access layer;
 * tools never query BigQuery directly.
 *
 * See FRONTEND_DESIGN_SPEC.md §6.4 for the design rationale:
 * "when a user asks something like 'how has Astarion trended this
 *  month,' the Sage does not guess — it calls a tool, receives real
 *  data, and summarizes the real result."
 */

import { tool } from "ai"
import { z } from "zod"

// ─── Bouncer URL ──────────────────────────────────────────────────────────────

const BOUNCER_URL =
  process.env.NEXT_PUBLIC_BOUNCER_API_URL ??
  "https://us-central1-dnd-trends-index.cloudfunctions.net/bouncer-api"

/** Typed fetch wrapper that returns JSON or throws with context. */
async function bouncerFetch<T>(path: string): Promise<T> {
  const url = `${BOUNCER_URL}${path}`
  const res = await fetch(url, { cache: "no-store" })
  if (!res.ok) {
    throw new Error(`Bouncer ${path} returned ${res.status}: ${res.statusText}`)
  }
  return res.json() as Promise<T>
}

// ─── Tool definitions ─────────────────────────────────────────────────────────

/**
 * getLeaderboard — the primary data query tool.
 *
 * Returns ranked D&D concepts grouped by category for any data source.
 * This is the tool the Sage should reach for whenever a user asks about
 * rankings, scores, trends, or "what's popular." Each source returns
 * slightly different fields — the Sage should mention which source the
 * data came from.
 */
export const getLeaderboard = tool({
  description:
    "Get ranked D&D concepts from a specific data source. Returns items grouped by " +
    "category (Class, Monster, Subclass, etc.) with scores and rankings. Use this " +
    "when users ask about rankings, popularity, scores, or 'what is trending.' " +
    "Available sources: google (Google Trends, default), fandom (Fandom wiki page views), " +
    "wikipedia (Wikipedia page views), reddit (Reddit mentions + sentiment), " +
    "bgg (BoardGameGeek), rpggeek (RPGGeek), amazon (Amazon sales ranks), " +
    "kickstarter (Kickstarter projects), backerkit (BackerKit projects), " +
    "youtube (YouTube creator consensus).",
  inputSchema: z.object({
    source: z
      .enum([
        "google",
        "fandom",
        "wikipedia",
        "reddit",
        "bgg",
        "rpggeek",
        "amazon",
        "kickstarter",
        "backerkit",
        "youtube",
      ])
      .default("google")
      .describe("The data source to query"),
    category: z
      .string()
      .optional()
      .describe(
        "Filter to a specific category (e.g. 'Class', 'Monster', 'Subclass'). " +
        "If omitted, returns all categories."
      ),
    limit: z
      .number()
      .int()
      .min(1)
      .max(50)
      .default(10)
      .describe("Max items per category to return (default 10, max 50)"),
  }),
  execute: async ({ source, category, limit }) => {
    const data = await bouncerFetch<
      Array<{
        category: string
        heat: number
        items: Array<Record<string, unknown>>
      }>
    >(`/leaderboards?source=${source}`)

    // Filter to requested category if specified
    let filtered = data
    if (category) {
      filtered = data.filter(
        (c) => c.category.toLowerCase() === category.toLowerCase()
      )
      if (filtered.length === 0) {
        return {
          source,
          categories_available: data.map((c) => c.category),
          error: `Category "${category}" not found. Available categories listed above.`,
        }
      }
    }

    // Trim items per category to the limit
    const trimmed = filtered.map((c) => ({
      category: c.category,
      heat: c.heat,
      items: c.items.slice(0, limit),
      total_items: c.items.length,
    }))

    return { source, data: trimmed }
  },
})

/**
 * searchConcepts — fuzzy search across the concept library.
 *
 * Useful when the user asks about a specific concept by name and
 * the Sage needs to find out if it's tracked, what category it's in,
 * and whether it's active.
 */
export const searchConcepts = tool({
  description:
    "Search the D&D concept library by keyword. Returns matching concept names, " +
    "their categories, and whether they're actively tracked. Use when the user " +
    "asks about a specific concept, character, spell, or item by name.",
  inputSchema: z.object({
    query: z.string().describe("The search keyword (fuzzy match, case-insensitive)"),
  }),
  execute: async ({ query }) => {
    const results = await bouncerFetch<
      Array<{ name: string; category: string; is_active: boolean }>
    >(`/search?q=${encodeURIComponent(query)}`)

    return {
      query,
      results: results.slice(0, 20),
      total_matches: results.length,
    }
  },
})

/**
 * getConceptConfidence — data confidence scores for specific concepts.
 *
 * Returns the data_confidence score, tier, and methodology breakdown
 * for named concepts. Use when the user asks "how reliable is this
 * data?" or "how confident are we in Fighter?"
 */
export const getConceptConfidence = tool({
  description:
    "Get data confidence scores and methodology breakdown for specific D&D concepts. " +
    "Returns the confidence score (0-100), tier (copper/silver/gold/platinum/mithral), " +
    "and a detailed explanation of what drives the score (data streams, agreement, " +
    "velocity). Use when the user asks about data reliability or confidence.",
  inputSchema: z.object({
    names: z
      .array(z.string())
      .min(1)
      .max(20)
      .describe("Concept names to look up (e.g. ['Fighter', 'Paladin', 'Ranger'])"),
  }),
  execute: async ({ names }) => {
    const url = `${BOUNCER_URL}/confidence?names=${encodeURIComponent(names.join(","))}`
    const res = await fetch(url, { cache: "no-store" })
    if (!res.ok) {
      return { error: `Confidence API returned ${res.status}`, names }
    }
    const data = await res.json()

    return {
      results: data,
      looked_up: names,
      found: Object.keys(data).length,
    }
  },
})

/**
 * getMarketSummary — executive-level market indicators.
 *
 * Returns four key metrics: edition migration index, DM shortage index,
 * 3rd-party market gap count, and Reddit positive sentiment percentage.
 * Good for "big picture" or "state of D&D" questions.
 */
export const getMarketSummary = tool({
  description:
    "Get an executive summary of the D&D market with four key indicators: " +
    "edition migration index (2024 vs legacy ruleset adoption), DM shortage index " +
    "(player vs DM interest gap), 3rd-party market gap count, and positive " +
    "sentiment percentage from Reddit. Use for big-picture or market overview questions.",
  inputSchema: z.object({}),
  execute: async () => {
    const data = await bouncerFetch<{
      migration_index: number
      dm_shortage_index: number
      gap_3p_count: number
      positive_sentiment: number
    }>("/vista/summary")

    return data
  },
})

/**
 * getTopMovers — the 5 concepts with the most momentum right now.
 *
 * Returns velocity data (7-day movers) with daily interest time series.
 * Good for "what's moving?" or "what just spiked?" questions.
 */
export const getTopMovers = tool({
  description:
    "Get the top 5 fastest-moving D&D concepts by 7-day velocity from Google Trends. " +
    "Returns each concept's name and daily interest time series (array of numbers). " +
    "Use when users ask about momentum, spikes, what's moving, or recent changes.",
  inputSchema: z.object({}),
  execute: async () => {
    const data = await bouncerFetch<
      Array<{ concept: string; trend: number[] }>
    >("/vista/momentum")

    return {
      top_movers: data,
      period: "7-day velocity",
    }
  },
})

/**
 * getDmShortageIndex — player vs DM interest comparison.
 *
 * Shows year-over-year growth for PLAYER and DM interest, revealing
 * whether the DM shortage is growing or shrinking.
 */
export const getDmShortageIndex = tool({
  description:
    "Get the DM shortage data: year-over-year interest comparison between players " +
    "and dungeon masters. Shows current interest, last year's interest, and YoY " +
    "growth percentage for both roles. Use when users ask about DM shortage, " +
    "player-to-DM ratio, or DM recruitment.",
  inputSchema: z.object({}),
  execute: async () => {
    const data = await bouncerFetch<
      Array<{
        persona: string
        current_interest: number
        last_year_interest: number
        yoy_growth: number
      }>
    >("/vista/dm-shortage")

    return { dm_shortage_data: data }
  },
})

/**
 * getEditionMigration — 2024 vs Legacy edition trend data.
 *
 * Returns 30 days of daily data points for both the 2024 ruleset
 * and the legacy ruleset, suitable for trend analysis.
 */
export const getEditionMigration = tool({
  description:
    "Get edition migration trend data: 30 days of daily data comparing 2024 " +
    "ruleset adoption vs legacy ruleset interest. Returns date labels and two " +
    "data series. Use when users ask about edition migration, 2024 vs 5e, " +
    "or ruleset adoption trends.",
  inputSchema: z.object({}),
  execute: async () => {
    const data = await bouncerFetch<{
      labels: string[]
      dataset_2024: number[]
      dataset_legacy: number[]
    }>("/vista/chart_data")

    return {
      period: "last 30 days",
      ...data,
    }
  },
})

/**
 * getSystemHealth — data freshness across all sources.
 *
 * Returns staleness info for each data source table. Use when the
 * user asks "is the data fresh?" or "when was this last updated?"
 */
export const getSystemHealth = tool({
  description:
    "Check data freshness and system health for all data sources. Returns each " +
    "source's status (healthy/stale/critical), last modified timestamp, row count, " +
    "and hours since last update. Use when users ask about data freshness, " +
    "staleness, or system status.",
  inputSchema: z.object({}),
  execute: async () => {
    const data = await bouncerFetch<{
      caldean_cycle: {
        last_run: string
        next_run: string
        mode: string
      }
      sources: Record<
        string,
        {
          table_id: string
          status: string
          last_modified: string
          row_count: number
          hours_since_update: number
        }
      >
    }>("/health")

    return data
  },
})

// ─── Tool registry ────────────────────────────────────────────────────────────

/**
 * All Sage tools, keyed by name. Pass this directly to `streamText({ tools })`.
 *
 * The names are important — they become the `type: "tool-${name}"` part type
 * in the UI message stream, so the client can render tool-specific UIs.
 */
export const sageTools = {
  getLeaderboard,
  searchConcepts,
  getConceptConfidence,
  getMarketSummary,
  getTopMovers,
  getDmShortageIndex,
  getEditionMigration,
  getSystemHealth,
} as const
