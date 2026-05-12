/*
 * Trusight — Overview lens loading skeleton.
 *
 * Shown by Next.js while the Server Component fetches from Bouncer.
 * Mirrors the grid layout of the real page so there's no layout shift.
 */

export default function OverviewLoading() {
  return (
    <main className="mx-auto max-w-6xl px-6 py-12 space-y-10">
      {/* Header skeleton */}
      <header className="space-y-3">
        <div className="h-3 w-36 rounded bg-bronze/40 animate-pulse" />
        <div className="h-10 w-72 rounded-lg bg-bronze/30 animate-pulse" />
        <div className="h-4 w-96 rounded bg-bronze/20 animate-pulse" />
      </header>

      {/* Card grid skeleton */}
      <div className="grid grid-cols-1 gap-5 sm:grid-cols-2 lg:grid-cols-3">
        <div className="h-64 rounded-xl border border-bronze bg-onyx animate-pulse" />
        <div className="h-64 rounded-xl border border-bronze bg-onyx animate-pulse sm:col-span-2" />
        <div className="h-64 rounded-xl border border-bronze bg-onyx animate-pulse" />
      </div>
    </main>
  )
}
