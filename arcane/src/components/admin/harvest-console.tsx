"use client"

/*
 * Trusight — BackerKit Harvest Console (Step 12).
 *
 * Terminal-styled card with a single Run button and a live readout of
 * recent runs. Fire-and-forget — the Run button POSTs to
 * /api/admin/backerkit/run, which kicks off the Cloud Function and
 * flips a Firestore doc's status when it settles. This component
 * polls /api/admin/backerkit/status every 2 seconds while any run is
 * active, then idles.
 *
 * Idle is the cheapest state — no websocket, no SSE, just a stopped
 * interval. Suited to BackerKit's ~minute-scale run time.
 */

import React from "react"
import { Play, Loader2, CheckCircle2, XCircle, User } from "lucide-react"

import { cn } from "@/lib/utils"

interface BackerkitRun {
  runId: string
  type: string
  status: "running" | "completed" | "failed"
  triggeredBy: string
  startedAt: string | null
  finishedAt: string | null
  durationSec: number | null
  summary?: unknown
  error?: string
}

const POLL_INTERVAL_MS = 2000

export function HarvestConsole() {
  const [runs, setRuns] = React.useState<BackerkitRun[]>([])
  const [loading, setLoading] = React.useState(true)
  const [triggering, setTriggering] = React.useState(false)
  const [error, setError] = React.useState<string | null>(null)

  const hasActiveRun = runs.some((r) => r.status === "running")

  const fetchStatus = React.useCallback(async () => {
    try {
      const res = await fetch("/api/admin/backerkit/status?limit=8", {
        cache: "no-store",
      })
      if (!res.ok) {
        throw new Error(`status ${res.status}`)
      }
      const data = (await res.json()) as { runs: BackerkitRun[] }
      setRuns(data.runs)
      setError(null)
    } catch (err) {
      console.error("[harvest-console] status fetch failed:", err)
      setError(err instanceof Error ? err.message : "status fetch failed")
    } finally {
      setLoading(false)
    }
  }, [])

  // Initial fetch on mount.
  React.useEffect(() => {
    fetchStatus()
  }, [fetchStatus])

  // Poll while any run is in-flight; stop cleanly when everything's settled.
  React.useEffect(() => {
    if (!hasActiveRun) return
    const id = setInterval(fetchStatus, POLL_INTERVAL_MS)
    return () => clearInterval(id)
  }, [hasActiveRun, fetchStatus])

  const triggerRun = async () => {
    setTriggering(true)
    setError(null)
    try {
      const res = await fetch("/api/admin/backerkit/run", { method: "POST" })
      if (!res.ok) {
        const body = await res.text().catch(() => "")
        throw new Error(`HTTP ${res.status}${body ? ` — ${body.slice(0, 120)}` : ""}`)
      }
      // Refresh immediately so the new "running" row appears without waiting
      // for the next poll tick.
      await fetchStatus()
    } catch (err) {
      console.error("[harvest-console] run trigger failed:", err)
      setError(err instanceof Error ? err.message : "run trigger failed")
    } finally {
      setTriggering(false)
    }
  }

  return (
    <article
      className={cn(
        "rounded-xl border border-ember/40 bg-onyx",
        "font-mono text-[13px] leading-relaxed",
        "overflow-hidden",
      )}
    >
      {/* Title bar — fake terminal chrome. */}
      <header className="flex items-center justify-between gap-3 border-b border-bronze/40 bg-iron/80 px-4 py-2.5">
        <div className="flex items-center gap-2 text-ember-bright">
          <span className="inline-block h-2 w-2 rounded-full bg-ember-bright" />
          <span className="uppercase tracking-widest text-[11px]">
            backerkit // harvest console
          </span>
        </div>
        <button
          type="button"
          onClick={triggerRun}
          disabled={triggering || hasActiveRun}
          className={cn(
            "inline-flex items-center gap-2 rounded-md px-3 py-1.5",
            "border text-xs font-sans uppercase tracking-widest",
            "transition-colors",
            triggering || hasActiveRun
              ? "border-bronze/40 bg-onyx text-ash/60 cursor-not-allowed"
              : "border-ember bg-iron text-parchment hover:bg-ember hover:text-obsidian",
          )}
        >
          {triggering || hasActiveRun ? (
            <>
              <Loader2 className="h-3.5 w-3.5 animate-spin" aria-hidden />
              {hasActiveRun ? "Running" : "Starting"}
            </>
          ) : (
            <>
              <Play className="h-3.5 w-3.5" aria-hidden />
              Run now
            </>
          )}
        </button>
      </header>

      {/* Body. */}
      <div className="px-4 py-4 space-y-4 min-h-[12rem]">
        <ConsoleLine prefix="$">
          backerkit-harvester ——
          {hasActiveRun ? " running…" : " idle. Press Run now to fire."}
        </ConsoleLine>

        {error && (
          <ConsoleLine prefix="!" variant="error">
            {error}
          </ConsoleLine>
        )}

        {loading && runs.length === 0 && (
          <ConsoleLine prefix="·" variant="dim">
            loading history…
          </ConsoleLine>
        )}

        {!loading && runs.length === 0 && !error && (
          <ConsoleLine prefix="·" variant="dim">
            no runs yet.
          </ConsoleLine>
        )}

        {runs.length > 0 && (
          <div className="space-y-2 pt-2 border-t border-bronze/30">
            <p className="font-sans uppercase tracking-widest text-[10px] text-ash/70">
              Recent runs
            </p>
            <ul className="space-y-2">
              {runs.map((r) => (
                <RunRow key={r.runId} run={r} />
              ))}
            </ul>
          </div>
        )}
      </div>
    </article>
  )
}

// ─── Subcomponents ──────────────────────────────────────────────────────────

function ConsoleLine({
  children,
  prefix,
  variant = "default",
}: {
  children: React.ReactNode
  prefix: string
  variant?: "default" | "error" | "dim"
}) {
  return (
    <p
      className={cn(
        "flex gap-2",
        variant === "error" && "text-red-400",
        variant === "dim" && "text-ash/60",
        variant === "default" && "text-parchment/90",
      )}
    >
      <span className="text-ember-bright shrink-0 select-none">{prefix}</span>
      <span className="whitespace-pre-wrap break-all">{children}</span>
    </p>
  )
}

function RunRow({ run }: { run: BackerkitRun }) {
  const Icon =
    run.status === "completed"
      ? CheckCircle2
      : run.status === "failed"
        ? XCircle
        : Loader2
  const iconClass =
    run.status === "completed"
      ? "text-emerald-400"
      : run.status === "failed"
        ? "text-red-400"
        : "text-ember-bright animate-spin"
  const startedLabel = run.startedAt ? formatRelative(run.startedAt) : "…"
  const message = summaryMessage(run.summary)
  const detail =
    run.status === "failed"
      ? run.error ?? "failed"
      : run.status === "completed"
        ? message
        : null

  return (
    <li className="grid grid-cols-[auto_1fr_auto] items-start gap-3 text-xs">
      <Icon
        className={cn("h-3.5 w-3.5 mt-[3px]", iconClass)}
        aria-hidden
      />
      <div className="flex flex-col gap-0.5 min-w-0">
        <div className="flex items-center gap-2 min-w-0">
          <span className="font-sans text-parchment/90 truncate">
            {run.status}
          </span>
          <span className="text-ash/40">·</span>
          <span className="flex items-center gap-1 text-ash/70 truncate">
            <User className="h-3 w-3 shrink-0" aria-hidden />
            {run.triggeredBy}
          </span>
        </div>
        {detail && (
          <p
            className={cn(
              "font-sans text-[11px] leading-snug truncate",
              run.status === "failed" ? "text-red-400/90" : "text-ember/80",
            )}
            title={detail}
          >
            {detail}
          </p>
        )}
      </div>
      <div className="text-right text-ash/70 shrink-0 mt-[2px]">
        {startedLabel}
        {run.durationSec != null && run.status !== "running" && (
          <span className="ml-2 text-ember/70">{run.durationSec}s</span>
        )}
      </div>
    </li>
  )
}

/**
 * Pull a human-readable message from the run's summary field.
 *
 * The BackerKit harvester returns
 *   { "status": "success", "message": "✅ Inserted 10 BackerKit projects." }
 * — we just surface that message verbatim (minus a leading emoji if any)
 * so operators see exactly how many rows landed. Falls through gracefully
 * for other summary shapes future harvesters might emit (plain string,
 * missing, or a structured object without `message`).
 */
function summaryMessage(summary: unknown): string | null {
  if (!summary) return null
  if (typeof summary === "string") return summary.trim() || null
  if (typeof summary === "object" && summary !== null) {
    const asRecord = summary as Record<string, unknown>
    const msg = asRecord.message
    if (typeof msg === "string" && msg.trim()) {
      // Strip a leading emoji + whitespace for cleaner terminal rendering.
      // e.g. "✅ Inserted 10 BackerKit projects." → "Inserted 10 BackerKit projects."
      return msg.trim().replace(/^\p{Extended_Pictographic}+\s*/u, "")
    }
  }
  return null
}

// Tiny "N seconds ago" / "N minutes ago" / "N hours ago" without a dep.
function formatRelative(iso: string): string {
  const then = new Date(iso).getTime()
  if (!Number.isFinite(then)) return "—"
  const deltaSec = Math.round((Date.now() - then) / 1000)
  if (deltaSec < 60) return `${Math.max(0, deltaSec)}s ago`
  if (deltaSec < 3600) return `${Math.round(deltaSec / 60)}m ago`
  if (deltaSec < 86400) return `${Math.round(deltaSec / 3600)}h ago`
  return `${Math.round(deltaSec / 86400)}d ago`
}
