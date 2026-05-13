"use client"

/*
 * Trusight — Universes Beyond Matrix candidate card + detail sheet
 * (Step 9.9 Chunk E).
 *
 * One click target per IP on the Matrix grid. The card surface shows the
 * composite score, medium, tier, and availability badges for each signal
 * layer.  The drawer (shadcn Sheet) opens with the full data trail —
 * every signal's raw value + a "coming soon" label for streams that
 * aren't yet populated (Reddit + YouTube land in 9.9.5/9.9.6 per the
 * locked roadmap).
 *
 * Be honest about coverage: a rubric-only IP shows the rubric bars and
 * "Fandom heat: coming", NOT a polished zero. Per Yorri's Apr 20 brief.
 */

import { useState } from "react"
import { Sparkles, CheckCircle2, Clock } from "lucide-react"

import {
  Sheet,
  SheetContent,
  SheetHeader,
  SheetTitle,
  SheetTrigger,
} from "@/components/ui/sheet"
import { cn } from "@/lib/utils"
import {
  UB_MEDIUM_LABEL,
  type UBCandidate,
} from "@/lib/bouncer"

const MEDIUM_COLOR: Record<UBCandidate["medium"], string> = {
  video_game: "bg-bronze/15 text-bronze border-bronze/40",
  anime_manga: "bg-ember/15 text-ember-bright border-ember/40",
  tv_film: "bg-parchment/10 text-parchment border-parchment/30",
  literature: "bg-iron text-parchment border-ash/30",
  webtoon_kr: "bg-ember-bright/10 text-ember-bright border-ember-bright/30",
  other: "bg-ash/15 text-parchment border-ash/30",
}

const TIER_COPY = {
  winner: "WINNER",
  edge: "EDGE",
} as const

function formatPct(x: number | null | undefined): string {
  if (x === null || x === undefined || Number.isNaN(x)) return "—"
  return `${Math.round(x * 100)}`
}

function formatScore(x: number | null): string {
  if (x === null || x === undefined) return "—"
  return x.toFixed(3)
}

function RubricBar({ label, value }: { label: string; value: number | null }) {
  const pct = value === null ? 0 : Math.round(value * 100)
  return (
    <div className="space-y-1">
      <div className="flex items-baseline justify-between">
        <span className="font-mono uppercase tracking-widest text-[10px] text-ash">
          {label}
        </span>
        <span className="font-mono text-xs text-parchment tabular-nums">
          {formatPct(value)}
        </span>
      </div>
      <div className="h-1.5 rounded-full bg-iron overflow-hidden">
        <div
          className="h-full bg-ember-bright transition-[width] duration-300"
          style={{ width: `${pct}%` }}
          aria-hidden
        />
      </div>
    </div>
  )
}

function AvailabilityBadge({
  available,
  label,
}: {
  available: boolean
  label: string
}) {
  return (
    <div
      className={cn(
        "flex items-center gap-1.5 px-2 py-0.5 rounded-full",
        "font-mono uppercase tracking-widest text-[10px] border",
        available
          ? "bg-ember/10 text-ember-bright border-ember/40"
          : "bg-iron text-ash border-ash/30",
      )}
    >
      {available ? (
        <CheckCircle2 className="h-3 w-3" aria-hidden />
      ) : (
        <Clock className="h-3 w-3" aria-hidden />
      )}
      <span>{label}</span>
    </div>
  )
}

export function UBMatrixCard({
  candidate,
  rank,
}: {
  candidate: UBCandidate
  rank: number
}) {
  const [open, setOpen] = useState(false)
  const mediumColor = MEDIUM_COLOR[candidate.medium]

  return (
    <Sheet open={open} onOpenChange={setOpen}>
      <SheetTrigger asChild>
        <button
          type="button"
          className={cn(
            "group text-left w-full rounded-xl border border-bronze bg-onyx",
            "p-4 hover:border-ember-bright transition-colors",
            "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 focus-visible:ring-offset-obsidian",
          )}
          aria-label={`Open detail for ${candidate.ip_name}`}
        >
          <div className="flex items-start justify-between gap-3">
            <div className="flex-1 min-w-0">
              <div className="flex items-center gap-2 mb-1">
                <span className="font-mono text-[10px] uppercase tracking-widest text-ash/70 tabular-nums">
                  #{rank}
                </span>
                <span
                  className={cn(
                    "font-mono uppercase tracking-widest text-[9px]",
                    "px-1.5 py-0.5 rounded-full border",
                    mediumColor,
                  )}
                >
                  {UB_MEDIUM_LABEL[candidate.medium]}
                </span>
                {candidate.tier === "winner" && (
                  <span className="font-mono uppercase tracking-widest text-[9px] text-ember-bright">
                    {TIER_COPY.winner}
                  </span>
                )}
              </div>
              <h3 className="font-display text-lg font-semibold text-parchment leading-tight truncate">
                {candidate.ip_name}
              </h3>
            </div>
            <div className="flex flex-col items-end gap-1 shrink-0">
              <span className="font-mono text-[10px] uppercase tracking-widest text-ash/70">
                License-fit
              </span>
              <span className="font-display text-2xl font-semibold text-ember-bright tabular-nums">
                {formatScore(candidate.license_fit_score)}
              </span>
            </div>
          </div>
          <div className="flex items-center gap-1.5 mt-3 flex-wrap">
            <AvailabilityBadge available={true} label="Rubric" />
            <AvailabilityBadge
              available={candidate.fandom.available}
              label={candidate.fandom.available ? "Fandom" : "Fandom: coming"}
            />
            <AvailabilityBadge
              available={candidate.steam.available}
              label={candidate.steam.available ? "Steam velocity" : "Steam: coming"}
            />
          </div>
        </button>
      </SheetTrigger>

      <SheetContent className="bg-onyx border-bronze text-parchment w-full sm:max-w-lg overflow-y-auto">
        <SheetHeader>
          <div className="flex items-center gap-2 mb-1">
            <Sparkles className="h-4 w-4 text-ember-bright" aria-hidden />
            <span className="font-mono uppercase tracking-widest text-[10px] text-ember-bright">
              Universes Beyond candidate · #{rank}
            </span>
          </div>
          <SheetTitle className="font-display text-2xl font-semibold text-parchment">
            {candidate.ip_name}
          </SheetTitle>
          <div className="flex items-center gap-2 text-sm text-ash">
            <span>{UB_MEDIUM_LABEL[candidate.medium]}</span>
            <span aria-hidden>·</span>
            <span className="uppercase tracking-widest text-[10px] font-mono">
              {TIER_COPY[candidate.tier]}
            </span>
            {candidate.disambiguation && (
              <>
                <span aria-hidden>·</span>
                <span className="italic text-xs">{candidate.disambiguation}</span>
              </>
            )}
          </div>
        </SheetHeader>

        <div className="mt-6 space-y-6">
          {/* ── Score + reasoning ─────────────────────────────────── */}
          <section className="space-y-3 border-b border-bronze/40 pb-4">
            <div className="flex items-baseline justify-between">
              <span className="font-mono uppercase tracking-widest text-[10px] text-ash">
                License-fit score
              </span>
              <span className="font-display text-3xl font-semibold text-ember-bright tabular-nums">
                {formatScore(candidate.license_fit_score)}
              </span>
            </div>
            {candidate.rubric.reasoning && (
              <p className="text-sm text-parchment/90 italic leading-snug border-l-2 border-ember/60 pl-3">
                {candidate.rubric.reasoning}
              </p>
            )}
            {candidate.rubric.confidence !== null && (
              <p className="font-mono text-[10px] uppercase tracking-widest text-ash/70">
                Rubric confidence · {formatPct(candidate.rubric.confidence)}%
              </p>
            )}
          </section>

          {/* ── Rubric breakdown ──────────────────────────────────── */}
          <section className="space-y-3">
            <h4 className="font-display text-sm font-semibold text-parchment">
              Rubric dimensions
            </h4>
            <p className="text-xs text-ash">
              Five independent TTRPG-licensing dimensions scored by
              gemini-2.5-flash on all 142 seed IPs.
            </p>
            <div className="space-y-3 pt-1">
              <RubricBar label="Genre fit" value={candidate.rubric.genre_fit} />
              <RubricBar
                label="Combat translatability"
                value={candidate.rubric.combat_translatability}
              />
              <RubricBar
                label="Party dynamics fit"
                value={candidate.rubric.party_dynamics_fit}
              />
              <RubricBar
                label="Setting portability"
                value={candidate.rubric.setting_portability}
              />
              <RubricBar
                label="Fanbase TTRPG overlap"
                value={candidate.rubric.fanbase_ttrpg_overlap}
              />
            </div>
          </section>

          {/* ── Market-signal data trail ──────────────────────────── */}
          <section className="space-y-3 border-t border-bronze/40 pt-4">
            <h4 className="font-display text-sm font-semibold text-parchment">
              Market-signal data trail
            </h4>

            {/* Fandom */}
            <div className="rounded-lg border border-bronze/40 bg-iron/40 p-3 space-y-1.5">
              <div className="flex items-baseline justify-between">
                <span className="font-mono uppercase tracking-widest text-[10px] text-ember-bright">
                  Fandom heat
                </span>
                {candidate.fandom.available ? (
                  <span className="font-mono text-xs text-parchment tabular-nums">
                    hype {candidate.fandom.hype_sum?.toFixed(1) ?? "—"} · {" "}
                    {candidate.fandom.article_count ?? "—"} articles
                  </span>
                ) : (
                  <span className="font-mono text-xs text-ash italic">
                    no wiki mapped yet
                  </span>
                )}
              </div>
              {candidate.fandom.available && candidate.fandom.wiki_slug && (
                <p className="text-xs text-ash">
                  {candidate.fandom.wiki_slug}.fandom.com — SUM(hype_score) ·
                  last 7 days
                </p>
              )}
              {!candidate.fandom.available && (
                <p className="text-xs text-ash">
                  This IP isn&rsquo;t mapped to a Fandom wiki yet. Reddit &
                  YouTube coverage comes online in Step 9.9.5 / 9.9.6.
                </p>
              )}
            </div>

            {/* Steam */}
            <div className="rounded-lg border border-bronze/40 bg-iron/40 p-3 space-y-1.5">
              <div className="flex items-baseline justify-between">
                <span className="font-mono uppercase tracking-widest text-[10px] text-ember-bright">
                  Steam velocity
                </span>
                {candidate.steam.available ? (
                  <span className="font-mono text-xs text-parchment tabular-nums">
                    {candidate.steam.velocity !== null &&
                    candidate.steam.velocity >= 0
                      ? "+"
                      : ""}
                    {candidate.steam.velocity !== null
                      ? (candidate.steam.velocity * 100).toFixed(1) + "%"
                      : "—"}
                  </span>
                ) : candidate.steam.app_id ? (
                  <span className="font-mono text-xs text-ash italic">
                    backfilling (≥1 week of history needed)
                  </span>
                ) : (
                  <span className="font-mono text-xs text-ash italic">
                    no Steam presence
                  </span>
                )}
              </div>
              {candidate.steam.app_id && (
                <p className="text-xs text-ash">
                  app_id {candidate.steam.app_id} · recent{" "}
                  {candidate.steam.recent_players?.toFixed(0) ?? "—"} · prior{" "}
                  {candidate.steam.prior_players?.toFixed(0) ?? "—"}
                </p>
              )}
            </div>

            {/* Reddit — explicit "coming" per Yorri's honest-data-trail directive */}
            <div className="rounded-lg border border-ash/30 bg-iron/20 p-3 space-y-1">
              <div className="flex items-baseline justify-between">
                <span className="font-mono uppercase tracking-widest text-[10px] text-ash">
                  Reddit mentions
                </span>
                <span className="font-mono text-xs text-ash italic">
                  coming in 9.9.5
                </span>
              </div>
              <p className="text-xs text-ash/80">
                Non-D&amp;D Reddit ingestion for UB IPs ships separately —
                our current Reddit stream is D&amp;D-scoped.
              </p>
            </div>

            {/* YouTube */}
            <div className="rounded-lg border border-ash/30 bg-iron/20 p-3 space-y-1">
              <div className="flex items-baseline justify-between">
                <span className="font-mono uppercase tracking-widest text-[10px] text-ash">
                  YouTube watch time
                </span>
                <span className="font-mono text-xs text-ash italic">
                  coming in 9.9.5
                </span>
              </div>
              <p className="text-xs text-ash/80">
                Non-D&amp;D YouTube ingestion for UB IPs ships separately.
              </p>
            </div>
          </section>
        </div>
      </SheetContent>
    </Sheet>
  )
}
