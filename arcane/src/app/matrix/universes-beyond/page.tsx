/*
 * Trusight — Universes Beyond Matrix (Step 9.9 Chunk E).
 *
 * The flagship saleable feature. Ranks ~142 non-D&D/non-MTG IPs by a
 * composite license_fit_score (rubric-primary + fandom heat + gated
 * Steam velocity). Server Component with 1-hour ISR.
 *
 * Per Yorri's Apr 20 brief, the story here is "rigorous multi-dimensional
 * IP scoring, validated against live market signals where available" —
 * the rubric is featured, not apologized-for. The status bar surfaces
 * coverage + gate state so users see the honest data posture. Each
 * candidate's drawer surfaces every signal's raw value + explicit
 * "coming in 9.9.5" labels for Reddit/YouTube.
 */

import { fetchUBMatrix } from "@/lib/bouncer"
import { UBMatrixCard } from "@/components/ub-matrix-card"

export const revalidate = 3600

export const metadata = {
  title: "Universes Beyond Matrix · Trusight",
  description:
    "Rigorous multi-dimensional scoring of ~140 IPs as Magic: The Gathering Universes Beyond / D&D-campaign-setting licensing candidates.",
}

function StatusBar({
  status,
}: {
  status: Awaited<ReturnType<typeof fetchUBMatrix>>["status"]
}) {
  const rubricPct = Math.round(status.weights.rubric * 100)
  const fandomPct = Math.round(status.weights.fandom * 100)
  const steamPct = Math.round(status.weights.steam * 100)

  return (
    <div className="rounded-xl border border-bronze bg-iron/40 p-4 space-y-3">
      <div className="flex items-baseline justify-between flex-wrap gap-2">
        <h2 className="font-mono uppercase tracking-widest text-[11px] text-ember-bright">
          Composite weighting · active gate
        </h2>
        <span className="font-mono text-[11px] text-ash">
          Rubric {rubricPct}% · Fandom {fandomPct}% · Steam {steamPct}%
        </span>
      </div>
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3 text-xs">
        <CoverageCell label="Rubric" n={status.coverage.rubric} total={status.coverage.total} />
        <CoverageCell label="Fandom" n={status.coverage.fandom} total={status.coverage.total} />
        <CoverageCell label="Steam" n={status.coverage.steam} total={status.coverage.total} />
        <div className="flex flex-col gap-0.5">
          <span className="font-mono uppercase tracking-widest text-[10px] text-ash/70">
            Steam gate
          </span>
          <span
            className={
              status.steam_gate_active
                ? "font-display text-sm font-semibold text-ember-bright"
                : "font-display text-sm font-semibold text-ash"
            }
          >
            {status.steam_gate_active ? "ACTIVE" : "Backfilling"}
          </span>
          {!status.steam_gate_active && (
            <span className="text-[10px] text-ash/70 leading-tight">
              Needs ≥20 IPs with velocity; re-weights automatically.
            </span>
          )}
        </div>
      </div>
    </div>
  )
}

function CoverageCell({
  label,
  n,
  total,
}: {
  label: string
  n: number
  total: number
}) {
  const pct = total > 0 ? Math.round((n / total) * 100) : 0
  return (
    <div className="flex flex-col gap-0.5">
      <span className="font-mono uppercase tracking-widest text-[10px] text-ash/70">
        {label} coverage
      </span>
      <span className="font-display text-sm font-semibold text-parchment tabular-nums">
        {n} / {total}
      </span>
      <span className="text-[10px] text-ash/70">{pct}% of seed list</span>
    </div>
  )
}

export default async function UniversesBeyondMatrixPage() {
  const { status, candidates } = await fetchUBMatrix(50)

  return (
    <main className="min-h-screen bg-obsidian px-6 py-10 md:py-16">
      <div className="max-w-6xl mx-auto space-y-8">
        <header className="space-y-2">
          <p className="font-mono text-xs uppercase tracking-widest text-ember-bright">
            The Matrix · Flagship lens
          </p>
          <h1 className="font-display text-3xl md:text-4xl font-semibold text-parchment">
            Universes Beyond candidates
          </h1>
          <p className="font-sans text-sm text-ash max-w-2xl">
            Non-D&amp;D/non-MTG IPs scored on five calibrated TTRPG-licensing
            dimensions (genre fit, combat translatability, party dynamics,
            setting portability, fanbase TTRPG overlap), validated against
            live market signals where available. The rubric is the
            intellectual core; Fandom heat and Steam velocity tilt the
            ranking where the data says so.
          </p>
        </header>

        <StatusBar status={status} />

        {candidates.length === 0 ? (
          <EmptyState />
        ) : (
          <section
            className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3"
            aria-label="Universes Beyond candidates"
          >
            {candidates.map((c, i) => (
              <UBMatrixCard
                key={`${c.ip_name}:${c.medium}`}
                candidate={c}
                rank={i + 1}
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
        The Matrix is empty.
      </p>
      <p className="font-sans text-sm text-ash max-w-md mx-auto">
        The ranking view hasn&rsquo;t published yet — if you&rsquo;re seeing
        this after deploy, check the Bouncer <code>/universes-beyond-matrix</code>{" "}
        endpoint and the <code>ub_candidate_enrichment</code> table.
      </p>
    </div>
  )
}
