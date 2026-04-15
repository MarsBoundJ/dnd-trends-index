"use client"

import React from "react"
import { Card, CardContent, CardFooter, CardHeader } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip"
import { cn } from "@/lib/utils"

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
   * 1. The always-visible pip color (top-left header, both platforms).
   * 2. The hover border-color on desktop (Step 13 upgrades this to an Aceternity halo).
   * Full methodology popover on pip tap is wired in Step 6.
   */
  confidence: number
  /** Called when the user taps "Stow" — saves this card to the Bag of Holding. Wired in Step 5. */
  onStow?: () => void
  /** Called when the user taps "Explain" — opens Sage with this card as context. Wired in Step 4. */
  onExplain?: () => void
}

// ─── CardChrome ──────────────────────────────────────────────────────────────
/**
 * Universal card container. Every data card, article card, and AI-summary card
 * in Arcane Analytics renders inside CardChrome. This is the visual contract
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
  onStow,
  onExplain,
}: CardChromeProps) {
  const tier = confidenceToTier(confidence)

  return (
    <TooltipProvider delayDuration={400}>
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

          {/* Confidence pip — top-left, always visible on both platforms (§5.2, §9.17).
              ~10px dot colored by metal tier. Tooltip shows raw % for now;
              full methodology popover (Tap pip → "Data: X% · AI: Y% · Combined: Z%")
              is wired in Step 6. */}
          <div className="flex items-center mt-0.5 shrink-0">
            <Tooltip>
              <TooltipTrigger asChild>
                <button
                  aria-label={`Confidence: ${confidence}% (${tierLabel[tier]}). Tap for methodology.`}
                  className={cn(
                    "w-2.5 h-2.5 rounded-full",
                    "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-1 focus-visible:ring-offset-onyx",
                    tierBgClass[tier]
                  )}
                />
              </TooltipTrigger>
              <TooltipContent
                side="right"
                className="bg-iron border border-bronze text-parchment text-xs py-1 px-2"
              >
                <span className="font-mono font-semibold">{confidence}%</span>
                <span className="text-ash ml-1.5">{tierLabel[tier]}</span>
              </TooltipContent>
            </Tooltip>
          </div>

          {/* Title + subtitle */}
          <div className="flex-1 min-w-0">
            <h3 className="font-display text-sm font-semibold text-parchment leading-tight truncate">
              {title}
            </h3>
            {subtitle && (
              <p className="text-xs text-ash mt-0.5 leading-tight">
                {subtitle}
              </p>
            )}
          </div>

          {/* Icon slots — top-right (§4.5). Exactly two: Slot 1 = card type,
              Slot 2 = lens tag. Empty bordered squares as placeholders now;
              real heraldic guild-mark SVGs arrive in Step 13. */}
          <div className="flex items-center gap-1 mt-0.5 shrink-0">
            <span
              aria-label={cardType ? `Card type: ${cardType}` : "Card type (slot 1)"}
              title={cardType ?? "card type"}
              className="flex items-center justify-center w-4 h-4 rounded-sm border border-bronze/40"
            />
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
            onClick={onExplain}
            aria-label="Explain this card"
            className="h-7 px-3 text-xs text-ash hover:text-parchment hover:bg-iron"
          >
            Explain
          </Button>
          <Button
            variant="outline"
            size="sm"
            onClick={onStow}
            aria-label="Stow in Bag of Holding"
            className="h-7 px-3 text-xs border-bronze text-ash hover:border-ember hover:text-parchment hover:bg-iron"
          >
            Stow
          </Button>
        </CardFooter>
      </Card>
    </TooltipProvider>
  )
}
