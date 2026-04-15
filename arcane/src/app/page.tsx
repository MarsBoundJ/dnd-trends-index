/*
 * Arcane Analytics — root landing stub.
 *
 * Placeholder until the real landing page ("State of the D&D Multiverse", §3.1)
 * is built in a later step. Points to the active verification harness.
 */

export default function RootPage() {
  return (
    <main className="flex min-h-screen flex-col items-center justify-center gap-6 bg-obsidian px-6">
      <div className="text-center space-y-3">
        <p className="font-mono text-xs uppercase tracking-widest text-ember-bright">
          Arcane Analytics · build in progress
        </p>
        <h1 className="font-display text-4xl font-semibold text-parchment">
          Step 2 — CardChrome
        </h1>
        <p className="font-sans text-base text-ash max-w-sm">
          The universal card container is ready. The real landing page arrives
          after Step 3 (first lens end-to-end).
        </p>
      </div>

      <nav className="flex flex-col sm:flex-row gap-3">
        <a
          href="/test-card-chrome"
          className="inline-flex items-center justify-center rounded-lg border border-ember bg-onyx px-5 py-2.5 font-sans text-sm font-medium text-parchment transition-colors hover:bg-iron"
        >
          CardChrome harness →
        </a>
        <a
          href="/swatch"
          className="inline-flex items-center justify-center rounded-lg border border-bronze bg-onyx px-5 py-2.5 font-sans text-sm font-medium text-ash transition-colors hover:bg-iron hover:text-parchment"
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
