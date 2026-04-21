/*
 * Arcane Analytics — Articles page (Step 9b).
 *
 * Server Component that renders the latest Council articles as a grid of
 * CardChrome-wrapped ArticleCards. Fetches server-side via fetchArticles
 * with 1-hour ISR — same cadence as the Overview lens.
 *
 * Empty state: during the pre-parallel-run window (Apr 17+) it's possible
 * the feed returns zero rows (e.g. if the scheduled journalist hasn't
 * fired yet today and no backfill exists). We render a Sage-voiced empty
 * card rather than a blank page.
 */

import { fetchArticles } from "@/lib/bouncer"
import { getFrameById } from "@/lib/frames"
import { ArticleCard } from "@/components/article-card"

// Top-of-route ISR — matches /overview. The underlying fetchArticles()
// also carries next.revalidate=3600, but pinning it at the page level
// makes the intent explicit.
export const revalidate = 3600

export const metadata = {
  title: "Articles · Arcane Analytics",
  description:
    "Daily dispatches from the Council of analysts covering the D&D market.",
}

export default async function ArticlesPage() {
  const articles = await fetchArticles(20)

  // Step 9.8 + 9.10 — collect unique frame_ids across framed articles
  // (Track C industry-fundamentals + Track D corporate-strategy) and
  // resolve their labels once. Skip the lookup when there are no
  // framed articles (the common case until a frame is flipped active).
  // Failures fall through as null — the attribution line just doesn't
  // render.
  const framedFrameIds = Array.from(
    new Set(
      articles
        .filter(
          (a) => (a.track === "C" || a.track === "D") && a.frame_id,
        )
        .map((a) => a.frame_id as string),
    ),
  )
  const frameLabelById = new Map<string, string>()
  if (framedFrameIds.length > 0) {
    const frames = await Promise.all(
      framedFrameIds.map(async (id) => {
        try {
          const frame = await getFrameById(id)
          return frame ? ([id, frame.label] as const) : null
        } catch {
          return null
        }
      }),
    )
    for (const entry of frames) {
      if (entry) frameLabelById.set(entry[0], entry[1])
    }
  }

  return (
    <main className="min-h-screen bg-obsidian px-6 py-10 md:py-16">
      <div className="max-w-6xl mx-auto space-y-8">
        <header className="space-y-2">
          <p className="font-mono text-xs uppercase tracking-widest text-ember-bright">
            The Council · Daily Dispatches
          </p>
          <h1 className="font-display text-3xl md:text-4xl font-semibold text-parchment">
            Articles
          </h1>
          <p className="font-sans text-sm text-ash max-w-2xl">
            Five analysts, five beats. Each Council member writes under their
            own byline when an anomaly in their terrain crosses the wire.
            Sage chairs the Council but never publishes — she quotes these
            dispatches on the Overview lens when you ask.
          </p>
        </header>

        {articles.length === 0 ? (
          <EmptyState />
        ) : (
          <section
            className="grid gap-6 md:grid-cols-2"
            aria-label="Council articles"
          >
            {articles.map((a) => (
              // Multiple articles from the same author on the same date are
              // allowed (Gary alone wrote 3 in one day during Step 9.7 smoke
              // tests). date+author_name alone collides; including the
              // headline gives every card a distinct key. Headlines are
              // effectively unique in practice.
              <ArticleCard
                key={`${a.date}:${a.author_name}:${a.headline}`}
                article={a}
                frameLabel={
                  a.frame_id ? frameLabelById.get(a.frame_id) ?? null : null
                }
              />
            ))}
          </section>
        )}
      </div>
    </main>
  )
}

function EmptyState() {
  return (
    <div className="border border-bronze rounded-xl bg-onyx p-8 text-center space-y-3">
      <p className="font-display text-lg font-medium text-parchment">
        The Council hasn&rsquo;t filed yet today.
      </p>
      <p className="font-sans text-sm text-ash max-w-md mx-auto">
        The daily journalist runs every morning at 04:30 Central. If no
        anomaly crossed a Council member&rsquo;s beat worth reporting,
        the feed stays quiet — a signal in itself.
      </p>
    </div>
  )
}
