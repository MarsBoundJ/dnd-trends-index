"use client"

/*
 * Trusight — Concept Detail Drawer (Step 8).
 *
 * The "evidence view" for a single D&D concept. Slides in from the right
 * on desktop, from the bottom on mobile. Shows cross-source data so a
 * skeptical exec can verify claims about any concept.
 *
 * Contents:
 *   1. Concept header (name, category, active status)
 *   2. Data confidence score + tier + methodology breakdown
 *   3. Cross-source ranking table (where this concept appears)
 *   4. "Explain this concept" button → opens Sage with concept context
 *   5. Stow button → saves concept to Bag of Holding
 *
 * Trigger: any concept name in the app wrapped in <ConceptLink>.
 * Dismissal: click overlay, X button, or Escape key.
 */

import React from "react"
import {
  Sheet,
  SheetContent,
  SheetHeader,
  SheetTitle,
  SheetDescription,
} from "@/components/ui/sheet"
import { Button } from "@/components/ui/button"
import { cn } from "@/lib/utils"
import {
  Sparkles,
  Bookmark,
  ExternalLink,
  TrendingUp,
  TrendingDown,
  Minus,
  Loader2,
} from "lucide-react"
import { useSage } from "@/components/sage-panel"
import { useBagStore, useBagHasHydrated } from "@/lib/bag-store"

// ─── Types ──────────────────────────────────────────────────────────────────

interface SourceAppearance {
  source: string
  score: number
  rank: number
  category: string
  extra: Record<string, unknown>
}

interface ConceptDetail {
  name: string
  canonical_name: string | null
  category: string | null
  is_active: boolean | null
  confidence: {
    score: number
    tier: string
    explanation: Record<string, unknown> | null
  } | null
  appearances: SourceAppearance[]
  absent_from: string[]
}

// ─── Context ────────────────────────────────────────────────────────────────

interface ConceptDrawerContextValue {
  openConcept: (name: string) => void
  closeConcept: () => void
}

const ConceptDrawerContext =
  React.createContext<ConceptDrawerContextValue | null>(null)

export function useConceptDrawer() {
  const ctx = React.useContext(ConceptDrawerContext)
  if (!ctx)
    throw new Error("useConceptDrawer must be used inside <ConceptDrawerProvider>")
  return ctx
}

// ─── Provider ───────────────────────────────────────────────────────────────

export function ConceptDrawerProvider({
  children,
}: {
  children: React.ReactNode
}) {
  const [isOpen, setIsOpen] = React.useState(false)
  const [conceptName, setConceptName] = React.useState<string | null>(null)
  const [detail, setDetail] = React.useState<ConceptDetail | null>(null)
  const [loading, setLoading] = React.useState(false)
  const [error, setError] = React.useState<string | null>(null)

  const openConcept = React.useCallback((name: string) => {
    setConceptName(name)
    setIsOpen(true)
    setDetail(null)
    setLoading(true)
    setError(null)

    fetch(`/api/concept?name=${encodeURIComponent(name)}`)
      .then((res) => {
        if (!res.ok) throw new Error(`${res.status}`)
        return res.json()
      })
      .then((data: ConceptDetail) => {
        setDetail(data)
        setLoading(false)
      })
      .catch((err) => {
        setError(err.message ?? "Failed to load concept data")
        setLoading(false)
      })
  }, [])

  const closeConcept = React.useCallback(() => {
    setIsOpen(false)
  }, [])

  const ctx = React.useMemo(
    () => ({ openConcept, closeConcept }),
    [openConcept, closeConcept]
  )

  // Detect mobile for responsive sheet direction
  const [isMobile, setIsMobile] = React.useState(false)
  React.useEffect(() => {
    const check = () => setIsMobile(window.innerWidth < 768)
    check()
    window.addEventListener("resize", check)
    return () => window.removeEventListener("resize", check)
  }, [])

  return (
    <ConceptDrawerContext.Provider value={ctx}>
      {children}
      <Sheet open={isOpen} onOpenChange={setIsOpen}>
        <SheetContent
          side={isMobile ? "bottom" : "right"}
          className={cn(
            "border-bronze bg-onyx text-parchment overflow-y-auto",
            isMobile
              ? "h-[85vh] rounded-t-2xl"
              : "w-full max-w-lg"
          )}
        >
          {/* Drag handle for mobile — visual affordance for swipe-to-dismiss */}
          {isMobile && (
            <div className="mx-auto mb-4 h-1 w-10 rounded-full bg-bronze/50" />
          )}

          <SheetHeader>
            <SheetTitle className="font-display text-xl text-parchment">
              {conceptName ?? "Concept"}
            </SheetTitle>
            {detail?.category && (
              <SheetDescription className="font-mono text-[10px] uppercase tracking-widest text-ember-bright">
                {detail.category}
                {detail.is_active === false && (
                  <span className="ml-2 text-ash/50">· inactive</span>
                )}
              </SheetDescription>
            )}
          </SheetHeader>

          <div className="mt-6 space-y-6">
            {loading && (
              <div className="flex items-center justify-center py-12">
                <Loader2 className="h-6 w-6 animate-spin text-ember-bright" />
                <span className="ml-2 font-mono text-xs text-ash/70">
                  Loading concept data…
                </span>
              </div>
            )}

            {error && (
              <p className="font-mono text-xs text-rarity-copper">
                Failed to load concept data: {error}
              </p>
            )}

            {detail && !loading && (
              <>
                {/* Confidence section */}
                <ConfidenceSection confidence={detail.confidence} />

                {/* Cross-source appearances */}
                <AppearancesSection
                  appearances={detail.appearances}
                  absentFrom={detail.absent_from}
                />

                {/* Actions */}
                <ActionsSection
                  conceptName={detail.canonical_name ?? detail.name}
                  category={detail.category}
                  appearances={detail.appearances}
                  confidence={detail.confidence}
                />
              </>
            )}
          </div>
        </SheetContent>
      </Sheet>
    </ConceptDrawerContext.Provider>
  )
}

// ─── Confidence section ─────────────────────────────────────────────────────

const TIER_COLORS: Record<string, string> = {
  copper: "text-rarity-copper",
  silver: "text-rarity-silver",
  gold: "text-rarity-gold",
  platinum: "text-rarity-platinum",
  mithral: "text-rarity-mithral",
}

function ConfidenceSection({
  confidence,
}: {
  confidence: ConceptDetail["confidence"]
}) {
  if (!confidence) {
    return (
      <div className="rounded-lg border border-bronze/30 bg-iron px-4 py-3">
        <p className="font-mono text-[10px] uppercase tracking-widest text-ash/50">
          Data Confidence
        </p>
        <p className="mt-1 text-sm text-ash/70">
          No confidence data available for this concept.
        </p>
      </div>
    )
  }

  const colorClass = TIER_COLORS[confidence.tier] ?? "text-ash"
  const explanation = confidence.explanation as Record<string, unknown> | null

  return (
    <div className="rounded-lg border border-bronze/30 bg-iron px-4 py-3">
      <p className="font-mono text-[10px] uppercase tracking-widest text-ash/50">
        Data Confidence
      </p>
      <div className="mt-1 flex items-baseline gap-2">
        <span className={cn("font-display text-2xl font-bold", colorClass)}>
          {confidence.score}%
        </span>
        <span
          className={cn(
            "font-mono text-xs font-semibold uppercase",
            colorClass
          )}
        >
          {confidence.tier}
        </span>
      </div>
      {explanation && (
        <div className="mt-2 grid grid-cols-2 gap-x-4 gap-y-1 font-mono text-[10px] text-ash/60">
          <span>Streams: {String(explanation.streams_present ?? "—")}</span>
          <span>Families: {String(explanation.families_hit ?? "—")}</span>
          <span>
            Agreement: {explanation.agreement != null
              ? `${(Number(explanation.agreement) * 100).toFixed(0)}%`
              : "—"}
          </span>
          <span>
            Velocity: {explanation.velocity_factor != null
              ? `×${Number(explanation.velocity_factor).toFixed(2)}`
              : "—"}
          </span>
          {explanation.binding_constraint != null && (
            <span className="col-span-2 mt-1 text-ash/40">
              Binding: {String(explanation.binding_constraint).replace(/_/g, " ")}
            </span>
          )}
        </div>
      )}
    </div>
  )
}

// ─── Appearances section ────────────────────────────────────────────────────

const SOURCE_LABELS: Record<string, string> = {
  google: "Google Trends",
  fandom: "Fandom Wiki",
  wikipedia: "Wikipedia",
  reddit: "Reddit",
  youtube: "YouTube",
  bgg: "BoardGameGeek",
  rpggeek: "RPGGeek",
  amazon: "Amazon",
  kickstarter: "Kickstarter",
  backerkit: "BackerKit",
}

function AppearancesSection({
  appearances,
  absentFrom,
}: {
  appearances: SourceAppearance[]
  absentFrom: string[]
}) {
  return (
    <div>
      <p className="font-mono text-[10px] uppercase tracking-widest text-ash/50">
        Cross-Source Rankings
      </p>

      {appearances.length === 0 ? (
        <p className="mt-2 text-sm text-ash/70">
          This concept was not found in any active data source.
        </p>
      ) : (
        <div className="mt-2 space-y-1.5">
          {appearances.map((a) => (
            <div
              key={a.source}
              className="flex items-center justify-between rounded border border-bronze/20 bg-onyx px-3 py-2"
            >
              <div className="flex items-center gap-2">
                <ScoreIndicator score={a.score} />
                <div>
                  <span className="text-sm font-medium text-parchment">
                    {SOURCE_LABELS[a.source] ?? a.source}
                  </span>
                  {a.rank > 0 && (
                    <span className="ml-2 font-mono text-[10px] text-ash/50">
                      #{a.rank}
                    </span>
                  )}
                </div>
              </div>
              <span className="font-mono text-sm font-semibold text-parchment">
                {a.score}
              </span>
            </div>
          ))}
        </div>
      )}

      {absentFrom.length > 0 && (
        <p className="mt-2 font-mono text-[10px] text-ash/40">
          Not found in: {absentFrom.map((s) => SOURCE_LABELS[s] ?? s).join(", ")}
        </p>
      )}
    </div>
  )
}

function ScoreIndicator({ score }: { score: number }) {
  if (score >= 80) {
    return <TrendingUp className="h-3.5 w-3.5 text-rarity-gold" />
  }
  if (score >= 50) {
    return <Minus className="h-3.5 w-3.5 text-rarity-silver" />
  }
  return <TrendingDown className="h-3.5 w-3.5 text-rarity-copper" />
}

// ─── Actions section ────────────────────────────────────────────────────────

function ActionsSection({
  conceptName,
  category,
  appearances,
  confidence,
}: {
  conceptName: string
  category: string | null
  appearances: SourceAppearance[]
  confidence: ConceptDetail["confidence"]
}) {
  const { openSage } = useSage()
  const bagHydrated = useBagHasHydrated()
  const { stowSageAnswer, isStowed: checkStowed, unstow } = useBagStore()

  const stowId = `concept:${conceptName.toLowerCase()}`
  const isStowed = bagHydrated && checkStowed(stowId)

  const handleExplain = () => {
    // Build a rich context for the Sage
    const sourceList = appearances
      .map((a) => `${SOURCE_LABELS[a.source] ?? a.source}: score ${a.score}, rank #${a.rank}`)
      .join("\n")

    const ctx = [
      `Concept detail: ${conceptName}`,
      category ? `Category: ${category}` : null,
      confidence
        ? `Data confidence: ${confidence.score}% (${confidence.tier})`
        : null,
      sourceList
        ? `Cross-source appearances:\n${sourceList}`
        : "No cross-source data found.",
    ]
      .filter(Boolean)
      .join("\n")

    openSage(ctx)
  }

  const handleStow = () => {
    if (isStowed) {
      unstow(stowId)
      return
    }
    const summary = appearances
      .map((a) => `${a.source}: ${a.score}`)
      .join(", ")
    stowSageAnswer({
      id: stowId,
      pageContext: `Concept: ${conceptName} (${category ?? "unknown category"})`,
      question: `Concept detail for ${conceptName}`,
      answer: `${conceptName} appears in ${appearances.length} sources. ${summary}. Confidence: ${confidence?.score ?? "unknown"}% ${confidence?.tier ?? ""}.`,
    })
  }

  return (
    <div className="flex gap-2 border-t border-bronze/30 pt-4">
      <Button
        variant="outline"
        size="sm"
        onClick={handleExplain}
        className="flex-1 border-ember/40 text-ember-bright hover:bg-iron"
      >
        <Sparkles className="mr-1.5 h-3.5 w-3.5" />
        Ask the Sage
      </Button>
      <Button
        variant="outline"
        size="sm"
        onClick={handleStow}
        disabled={!bagHydrated}
        className={cn(
          "border-bronze/40 hover:bg-iron",
          isStowed ? "text-ember-bright" : "text-ash"
        )}
      >
        <Bookmark
          className={cn("mr-1.5 h-3.5 w-3.5", isStowed && "fill-current")}
        />
        {isStowed ? "Stowed" : "Stow"}
      </Button>
    </div>
  )
}

// ─── ConceptLink ────────────────────────────────────────────────────────────

/**
 * Wrap any concept name in <ConceptLink> to make it tappable.
 * Opens the Concept Detail Drawer on click.
 *
 * Visual treatment: subtle dotted underline on hover. Doesn't look
 * like a navigation link — more like an interactive data point.
 */
export function ConceptLink({
  name,
  children,
  className,
}: {
  name: string
  children?: React.ReactNode
  className?: string
}) {
  const { openConcept } = useConceptDrawer()

  return (
    <button
      type="button"
      onClick={() => openConcept(name)}
      className={cn(
        "cursor-pointer decoration-bronze/50 decoration-dotted underline-offset-2",
        "hover:underline hover:text-ember-bright",
        "transition-colors focus-visible:outline-none focus-visible:ring-1",
        "focus-visible:ring-ember focus-visible:ring-offset-1 focus-visible:ring-offset-onyx",
        "rounded-sm",
        className
      )}
    >
      {children ?? name}
    </button>
  )
}
