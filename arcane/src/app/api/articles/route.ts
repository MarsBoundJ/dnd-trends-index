/*
 * Trusight — Articles API (Step 9b).
 *
 * Thin Next 16 proxy over the Bouncer `/articles` endpoint. Kept as its
 * own route (rather than the Articles page calling Bouncer directly)
 * so that a future Sage tool — or client-side interaction layer — can
 * fetch the same data from a stable same-origin URL.
 *
 * Mirrors the Step 8 `/api/concept` pattern. Cached server-side at the
 * Bouncer layer via `fetchArticles()`'s 1-hour ISR — no client cache.
 */

import { fetchArticles, type Article } from "@/lib/bouncer"

export const dynamic = "force-dynamic"

export async function GET(req: Request) {
  const { searchParams } = new URL(req.url)
  const limitParam = searchParams.get("limit")
  const parsed = limitParam ? Number.parseInt(limitParam, 10) : 20
  const limit = Number.isFinite(parsed) ? Math.max(1, Math.min(parsed, 100)) : 20

  const articles: Article[] = await fetchArticles(limit)
  return Response.json({ articles, count: articles.length })
}
