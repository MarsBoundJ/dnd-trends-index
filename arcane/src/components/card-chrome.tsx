"use client"

import React from "react"
import { Bookmark } from "lucide-react"
import { Card, CardContent, CardFooter, CardHeader } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@/components/ui/popover"
import { cn } from "@/lib/utils"
import type { ConfidenceEntry } from "@/lib/bouncer"
import { useSageOptional } from "@/components/sage-panel"
import { useBagStore, useBagHasHydrated } from "@/lib/bag-store"

// ─── Confidence tier system ──────────────────────────────────────────────────
// Maps a 0–100 confidence score to a metal tier per §5.2 and §9.16.
// Thresholds: copper 0–69 / silver 70–79 / gold 80–89 / platinum 90–94 / mithral 95–99.
// "Lead" and "iron" were considered and rejected — see §9.16 in FRONTEND_DESIGN_SPEC.md.

type RarityTier = "copper" | "silver" | "gold" | "platinum" | "mithral"

function confidenceToTier(confidence: number): RarityTier {
  if (confidence >= 95) return "mithral"
  if (confidence >= 90) return "platinum"
  if (confidence >= 80) return "gold"
  if (confidence >= 70) return "silver"
  return "copper"
}

// Lookup maps ensure all variant class strings appear literally in source so
// Tailwind's JIT scanner includes them. Dynamic string interpolation would
// miss them (e.g. `bg-rarity-${tier}` would produce no utility class).

const tierBgClass: Record<RarityTier, string> = {
  copper:   "bg-rarity-copper",
  silver:   "bg-rarity-silver",
  gold:     "bg-rarity-gold",
  platinum: "bg-rarity-platinum",
  mithral:  "bg-rarity-mithral",
}

const tierHoverBorderClass: Record<RarityTier, string> = {
  copper:   "hover:border-rarity-copper",
  silver:   "hover:border-rarity-silver",
  gold:     "hover:border-rarity-gold",
  platinum: "hover:border-rarity-platinum",
  mithral:  "hover:border-rarity-mithral",
}

const tierLabel: Record<RarityTier, string> = {
  copper:   "Copper — Exploratory",
  silver:   "Silver",
  gold:     "Gold",
  platinum: "Platinum",
  mithral:  "Mithral",
}

// Human-readable one-liners for each binding_constraint enum value.
// Mirrors the `why_not_higher` CASE statement in
// gold_views/concept_confidence.sql but lives in TS so we can
// adjust the copy without touching BigQuery.
const bindingCopy: Record<
  NonNullable<ConfidenceEntry["explanation"]>["binding_constraint"],
  string
> = {
  single_source:
    "Only one data stream tracks this concept. Cross-stream corroboration would lift the score.",
  limited_diversity:
    "Signal is present but concentrated in few evidence families. Broader coverage would lift the score.",
  conflicting_signals:
    "Different data families point in different directions. Agreement across families would lift the score.",
  stale_consensus:
    "High absolute volume with little recent movement — evergreen signal, not active momentum.",
  thin_stream_data:
    "Data is present across streams but the underlying data points are thin.",
  strong: "Strong cross-validated evidence across families.",
}

// ─── Props ───────────────────────────────────────────────────────────────────

export interface CardChromeProps {
  /** Card body — any content type: charts, lists, text, stats. */
  children: React.ReactNode
  /** Card title — concept or view name. Displayed in Spectral. */
  title: string
  /** Optional subtitle — category or data-source label. */
  subtitle?: string
  /** Active lens identifier (e.g. "overview", "marketing"). Drives Slot 2 icon in Step 13. */
  lens?: string
  /** Card type identifier (e.g. "chart", "leaderboard", "article"). Drives Slot 1 icon in Step 13. */
  cardType?: string
  /**
   * Confidence score 0–100. Maps to a metal tier (§9.16) which drives:
   * 1. The always-visible pip color (top-right header cluster, both platforms).
   * 2. The hover border-color on desktop (Step 13 upgrades this to an Aceternity halo).
   * Tapping the pip opens the methodology Popover (Step 6).
   */
  confidence: number
  /**
   * Full explanation payload from `gold_data.concept_confidence` for
   * the concept that set this card's score (i.e. the aggregate's
   * binding/weakest-link concept). Drives the methodology Popover.
   * Null = no lookup resolved; the popover shows a fallback message.
   */
  confidenceExplanation?: ConfidenceEntry["explanation"]
  /**
   * Display name of the binding concept (e.g. "Paladin"). Used in the
   * popover header to clarify which concept the breakdown applies to
   * when the card aggregates multiple concepts.
   */
  confidenceBindingName?: string
  /** How many of the card's rendered concepts resolved in the confidence map. */
  confidenceHitCount?: number
  /** Total number of concepts the card attempted to look up. */
  confidenceTotalCount?: number
  /**
   * Stable identifier for this card in the Bag of Holding. If provided,
   * the Stow button auto-wires to the bag store: click = stow, click again
   * = unstow, and the button flips to a "Stowed" state while the card is
   * in the bag. Wired in Step 5.
   *
   * Should be stable across re-renders (e.g. `overview:top-classes`), not
   * a random uuid. This is what `isStowed(id)` checks against.
   */
  cardId?: string
  /**
   * Explicit override for the Stow button. If provided, wins over the
   * auto-wired `cardId` behavior. Mostly useful for the test harness.
   */
  onStow?: () => void
  /** Called when the user taps "Explain" — opens Sage with this card as context. Wired in Step 4. */
  onExplain?: () => void
  /**
   * Plain-text snapshot of this card's data — fed to the Sage as page context
   * when the user taps "Explain". If both `sageContext` and `onExplain` are
   * provided, `onExplain` wins. If a `<SageProvider>` is not mounted above
   * this card (e.g. the harness page), `sageContext` silently no-ops.
   * Wired in Step 4.
   */
  sageContext?: string
  /**
   * Title styling. Defaults to `"compact"` (single-line, truncates with
   * ellipsis) — the right fit for leaderboard/chart cards whose titles
   * are short labels. `"prominent"` scales the title up ~50% and allows
   * it to wrap across lines — used by ArticleCard (Step 9b) where the
   * headline is the main attraction.
   */
  titleVariant?: "compact" | "prominent"
}

// ─── CardChrome ──────────────────────────────────────────────────────────────
/**
 * Universal card container. Every data card, article card, and AI-summary card
 * in Trusight renders inside CardChrome. This is the visual contract
 * described in §4.4 (Strict Chrome, Loose Content): consistent border, padding,
 * corner radius, header bar, two icon slots, confidence pip, and action buttons
 * — regardless of what lives inside.
 *
 * Step 2 scope: visual contract only. No real data, no Sage wiring, no Bag of
 * Holding persistence, no Aceternity halo (Step 13), no Framer Motion (Step 14).
 */
export function CardChrome({
  children,
  title,
  subtitle,
  lens,
  cardType,
  confidence,
  confidenceExplanation,
  confidenceBindingName,
  confidenceHitCount,
  confidenceTotalCount,
  cardId,
  onStow,
  onExplain,
  sageContext,
  titleVariant = "compact",
}: CardChromeProps) {
  const tier = confidenceToTier(confidence)

  // If the caller provided a `sageContext` snapshot and a SageProvider is
  // mounted above us, auto-wire the Explain button to open the Sage panel
  // with this card's context. Explicit `onExplain` always wins — this is
  // the fallback used by the live /overview page (Step 4 §6).
  const sage = useSageOptional()
  const resolvedOnExplain =
    onExplain ??
    (sageContext && sage ? () => sage.openSage(sageContext) : undefined)

  // ── Bag of Holding wiring (Step 5 §3.5) ─────────────────────────────────
  // Subscribe to the `items` array (not `isStowed` directly) so this card
  // re-renders when *its own* stow status changes. We derive the boolean
  // locally from items to stay reactive.
  const bagHydrated = useBagHasHydrated()
  const isStowed = useBagStore((s) =>
    cardId ? s.items.some((i) => i.id === cardId) : false
  )
  const stowCard = useBagStore((s) => s.stowCard)
  const unstow = useBagStore((s) => s.unstow)

  const resolvedOnStow =
    onStow ??
    (cardId
      ? () => {
          if (isStowed) {
            unstow(cardId)
          } else {
            stowCard({
              id: cardId,
              title,
              subtitle,
              lens,
              cardType,
              confidence,
              // Use the same plain-text snapshot we feed the Sage — it's
              // the best record of what the card was showing at stow time.
              snapshot: sageContext ?? "",
            })
          }
        }
      : undefined)

  // Only show "Stowed" state after hydration finishes — otherwise SSR would
  // render "Stow" and the client would flip to "Stowed" on first paint.
  const showStowedState = bagHydrated && isStowed && !!cardId

  return (
    <>
      <Card
        className={cn(
          // Surface & shape — the "strict chrome" from §4.4
          "bg-onyx border border-bronze rounded-xl shadow-none",
          // Flex column so the footer always sticks to the bottom
          "flex flex-col",
          // Hover: border-color transitions to the card's metal tier (§4.3, §9.17).
          // No box-shadow halo here — that's Step 13 (Aceternity).
          "transition-colors duration-200",
          tierHoverBorderClass[tier]
        )}
      >
        {/* ── Header bar ──────────────────────────────────────────────────── */}
        <CardHeader className="flex flex-row items-start gap-3 p-4 pb-3 space-y-0">

          {/* Title + subtitle — gets the full left side, unobstructed (§4.4) */}
          <div className="flex-1 min-w-0">
            <h3
              className={cn(
                "font-display font-semibold text-parchment",
                titleVariant === "prominent"
                  ? "text-xl leading-snug"
                  : "text-sm leading-tight truncate",
              )}
            >
              {title}
            </h3>
            {subtitle && (
              <p className="text-xs text-ash mt-0.5 leading-tight">
                {subtitle}
              </p>
            )}
          </div>

          {/* Metadata cluster — top-right (§4.5, §5.2, §9.17).
              Pip + two icon slots live together, mirroring MtG card layout where
              the tier/cost symbol sits top-right alongside type icons.
              Pip = confidence tier (always visible, both platforms).
              Slot 1 = card type icon. Slot 2 = lens tag icon.
              Real heraldic SVGs arrive in Step 13. */}
          <div className="flex items-center gap-1.5 mt-0.5 shrink-0">

            {/* Confidence pip — tap opens the methodology Popover (§5.2, §9.17) */}
            <Popover>
              <PopoverTrigger asChild>
                <button
                  aria-label={`Confidence: ${confidence}% (${tierLabel[tier]}). Tap for methodology.`}
                  className={cn(
                    "w-2.5 h-2.5 rounded-full",
                    "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-1 focus-visible:ring-offset-onyx",
                    tierBgClass[tier]
                  )}
                />
              </PopoverTrigger>
              <PopoverContent
                side="bottom"
                align="end"
                className="w-80 bg-iron border border-bronze text-parchment p-4 space-y-3"
              >
                {/* Headline: score + tier */}
                <div className="flex items-baseline justify-between gap-3">
                  <div>
                    <div className="font-mono text-xl font-semibold text-parchment leading-none">
                      {confidence}
                      <span className="text-sm text-ash ml-1">/ 100</span>
                    </div>
                    <div className="text-xs text-ash mt-1">
                      {tierLabel[tier]}
                    </div>
                  </div>
                  <div className={cn("w-4 h-4 rounded-full", tierBgClass[tier])} />
                </div>

                {/* Binding concept label + why_not_higher copy */}
                {confidenceExplanation ? (
                  <>
                    <div className="text-[11px] text-ash/80 border-t border-bronze/40 pt-2">
                      {confidenceBindingName && (
                        <p className="font-mono uppercase tracking-widest text-[10px] text-ember mb-1">
                          Weakest link · {confidenceBindingName}
                        </p>
                      )}
                      <p className="text-parchment/80 leading-relaxed">
                        {bindingCopy[confidenceExplanation.binding_constraint]}
                      </p>
                    </div>

                    {/* Factor breakdown */}
                    <dl className="grid grid-cols-2 gap-x-3 gap-y-1.5 text-[11px]">
                      <dt className="text-ash">Streams present</dt>
                      <dd className="font-mono text-parchment text-right">
                        {confidenceExplanation.streams_present}
                      </dd>
                      <dt className="text-ash">Families hit</dt>
                      <dd className="font-mono text-parchment text-right">
                        {confidenceExplanation.families_hit} / 5
                      </dd>
                      <dt className="text-ash">Agreement</dt>
                      <dd className="font-mono text-parchment text-right">
                        {Math.round(confidenceExplanation.agreement * 100)}%
                      </dd>
                      <dt className="text-ash">Velocity</dt>
                      <dd
                        className={cn(
                          "font-mono text-right",
                          confidenceExplanation.velocity_factor < 1
                            ? "text-ember-bright"
                            : "text-parchment"
                        )}
                      >
                        ×{confidenceExplanation.velocity_factor.toFixed(2)}
                      </dd>
                    </dl>

                    {confidenceExplanation.sparsity_cap_applied && (
                      <p className="text-[10px] text-ember-bright font-mono">
                        ⚠ Single-source cap applied (ceiling 79)
                      </p>
                    )}

                    {/* Aggregate disclosure */}
                    {typeof confidenceHitCount === "number" &&
                      typeof confidenceTotalCount === "number" &&
                      confidenceTotalCount > 1 && (
                        <p className="text-[10px] text-ash/70 font-mono border-t border-bronze/40 pt-2">
                          Card score is the minimum across{" "}
                          {confidenceHitCount} of {confidenceTotalCount}{" "}
                          rendered concept
                          {confidenceTotalCount === 1 ? "" : "s"}.
                        </p>
                      )}

                    <p className="text-[10px] text-ash/60 font-mono">
                      algo {confidenceExplanation.algo_version} · data layer
                    </p>
                  </>
                ) : (
                  <div className="text-[11px] text-ash/80 border-t border-bronze/40 pt-2 leading-relaxed">
                    No confidence lookup resolved for this card&rsquo;s items.
                    Showing the silver stub (75) until category-level confidence
                    lands in a future step.
                  </div>
                )}
              </PopoverContent>
            </Popover>

            {/* Icon slot 1 — card type */}
            <span
              aria-label={cardType ? `Card type: ${cardType}` : "Card type (slot 1)"}
              title={cardType ?? "card type"}
              className="flex items-center justify-center w-4 h-4 rounded-sm border border-bronze/40"
            />
            {/* Icon slot 2 — lens tag */}
            <span
              aria-label={lens ? `Lens: ${lens}` : "Lens (slot 2)"}
              title={lens ?? "lens"}
              className="flex items-center justify-center w-4 h-4 rounded-sm border border-bronze/40"
            />
          </div>
        </CardHeader>

        {/* ── Content — fills the flex space, content is fully "loose" (§4.4) */}
        <CardContent className="flex-1 px-4 pb-2 pt-0">
          {children}
        </CardContent>

        {/* ── Footer — Explain + Stow buttons, identical position on every card (§4.4) */}
        <CardFooter className="flex justify-end gap-2 px-4 pb-4 pt-0">
          <Button
            variant="ghost"
            size="sm"
            onClick={resolvedOnExplain}
            disabled={!resolvedOnExplain}
            aria-label="Explain this card"
            className="h-7 px-3 text-xs text-ash hover:text-parchment hover:bg-iron"
          >
            Explain
          </Button>
          {/* NOTE: "Stow" + Bookmark icon may need revisiting — users unfamiliar
              with D&D lingo might not parse the verb immediately. The Bookmark
              icon helps bridge the gap. Revisit in Step 5 when the Bag of
              Holding is wired and we can user-test real reactions. */}
          <Button
            variant="outline"
            size="sm"
            onClick={resolvedOnStow}
            disabled={!resolvedOnStow}
            aria-label={
              showStowedState
                ? "Remove from Bag of Holding"
                : "Stow in Bag of Holding"
            }
            aria-pressed={showStowedState}
            className={cn(
              "h-7 px-3 text-xs gap-1.5 transition-colors",
              showStowedState
                ? "border-ember text-ember-bright hover:border-ember hover:text-parchment hover:bg-iron"
                : "border-bronze text-ash hover:border-ember hover:text-parchment hover:bg-iron"
            )}
          >
            <Bookmark
              className={cn(
                "h-3 w-3 shrink-0",
                showStowedState && "fill-current"
              )}
            />
            {showStowedState ? "Stowed" : "Stow"}
          </Button>
        </CardFooter>
      </Card>
    </>
  )
}
