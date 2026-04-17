/*
 * Arcane Analytics — root landing stub.
 *
 * Placeholder until the real landing page ("State of the D&D Multiverse", §3.1)
 * is built in a later step. The Overview lens is now live with real data.
 */

export default function RootPage() {
  return (
    <main className="flex min-h-screen flex-col items-center justify-center gap-6 bg-obsidian px-6">
      <div className="text-center space-y-3">
        <p className="font-mono text-xs uppercase tracking-widest text-ember-bright">
          Arcane Analytics · build in progress
        </p>
        <h1 className="font-display text-4xl font-semibold text-parchment">
          Step 3 — Overview Lens
        </h1>
        <p className="font-sans text-base text-ash max-w-sm">
          Real trend data is flowing from Bouncer into CardChrome. The full
          landing page arrives in Step 3+ polish.
        </p>
      </div>

      <p className="font-sans text-sm text-ash/80 max-w-sm text-center">
        Tap <span className="text-ember-bright">Atlas</span> in the header for
        the full site map — Trends, Articles, your Bag of Holding, and what
        the Council is still building.
      </p>

      <nav
        aria-label="Developer harness"
        className="flex flex-col sm:flex-row gap-3"
      >
        <a
          href="/test-card-chrome"
          className="inline-flex items-center justify-center rounded-lg border border-bronze bg-onyx px-4 py-2 font-sans text-xs font-medium text-ash transition-colors hover:bg-iron hover:text-parchment"
        >
          CardChrome harness
        </a>
        <a
          href="/swatch"
          className="inline-flex items-center justify-center rounded-lg border border-bronze bg-onyx px-4 py-2 font-sans text-xs font-medium text-ash transition-colors hover:bg-iron hover:text-parchment"
        >
          Palette &amp; fonts
        </a>
      </nav>

      <p className="font-mono text-xs text-ash/50">
        FRONTEND_DESIGN_SPEC.md · §3.1 landing page is Step 3+
      </p>
    </main>
  )
}
