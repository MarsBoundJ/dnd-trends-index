"use client"

/*
 * Arcane Analytics — Atlas navigation (Step 10).
 *
 * The Atlas is the site's full site-map card. It expands from the top-right
 * Compass trigger (in SiteHeader) into a glassmorphic sheet — full-screen
 * on mobile, 500px sidebar on desktop. Each tile is one section of the app,
 * active tiles are linked, planned tiles are disabled with a tooltip.
 *
 * Per FRONTEND_DESIGN_SPEC.md §3.6, this surface doubles as onboarding —
 * we list every destination on the site, built or planned, so a first-time
 * user can see the full shape of the product at a glance.
 *
 * Step 10 deliberately defers first-visit auto-open behavior to a later
 * polish step.
 */

import React from "react"
import Link from "next/link"
import { Sheet, SheetContent } from "@/components/ui/sheet"
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip"
import { cn } from "@/lib/utils"
import { ATLAS_SECTIONS, type AtlasSection } from "@/lib/atlas-sections"

// ─── Context ────────────────────────────────────────────────────────────────

interface AtlasContextValue {
  openAtlas: () => void
  closeAtlas: () => void
  isOpen: boolean
}

const AtlasContext = React.createContext<AtlasContextValue | null>(null)

export function useAtlas() {
  const ctx = React.useContext(AtlasContext)
  if (!ctx) throw new Error("useAtlas must be used inside <AtlasProvider>")
  return ctx
}

// ─── Provider ───────────────────────────────────────────────────────────────

export function AtlasProvider({ children }: { children: React.ReactNode }) {
  const [isOpen, setIsOpen] = React.useState(false)

  // Responsive direction: bottom sheet on mobile, right sidebar on desktop.
  // Same pattern as ConceptDrawerProvider (Step 8).
  const [isMobile, setIsMobile] = React.useState(false)
  React.useEffect(() => {
    const check = () => setIsMobile(window.innerWidth < 768)
    check()
    window.addEventListener("resize", check)
    return () => window.removeEventListener("resize", check)
  }, [])

  const ctx = React.useMemo<AtlasContextValue>(
    () => ({
      openAtlas: () => setIsOpen(true),
      closeAtlas: () => setIsOpen(false),
      isOpen,
    }),
    [isOpen],
  )

  return (
    <AtlasContext.Provider value={ctx}>
      {children}
      <Sheet open={isOpen} onOpenChange={setIsOpen}>
        <SheetContent
          side={isMobile ? "bottom" : "right"}
          className={cn(
            // Glassmorphic per spec §3.6: semi-transparent iron + heavy
            // backdrop blur. Falls back to solid iron if the browser
            // doesn't support backdrop-filter.
            "border-bronze/60 bg-iron/80 backdrop-blur-xl text-parchment",
            "overflow-y-auto",
            isMobile
              ? "h-[92vh] rounded-t-2xl"
              : "w-full sm:max-w-lg",
          )}
        >
          {isMobile && (
            <div className="mx-auto mb-4 h-1 w-10 rounded-full bg-bronze/50" />
          )}

          <AtlasHeader />
          <AtlasGrid onNavigate={() => setIsOpen(false)} />
        </SheetContent>
      </Sheet>
    </AtlasContext.Provider>
  )
}

// ─── Header ─────────────────────────────────────────────────────────────────

function AtlasHeader() {
  return (
    <header className="mb-6 space-y-2">
      <p className="font-mono text-xs uppercase tracking-widest text-ember-bright">
        The Atlas · Site map
      </p>
      <h2 className="font-display text-2xl font-semibold text-parchment">
        Every corner of the Archive.
      </h2>
      <p className="text-sm text-ash leading-relaxed">
        Linked tiles are live. Greyed tiles are planned — the Council and
        I are still building them.
      </p>
    </header>
  )
}

// ─── Grid + Tile ────────────────────────────────────────────────────────────

function AtlasGrid({ onNavigate }: { onNavigate: () => void }) {
  const active = ATLAS_SECTIONS.filter((s) => s.status === "active")
  const planned = ATLAS_SECTIONS.filter((s) => s.status === "planned")

  return (
    <TooltipProvider delayDuration={200}>
      <div className="space-y-6">
        <section aria-label="Available sections">
          <SectionLabel>Available</SectionLabel>
          <ul className="grid grid-cols-1 sm:grid-cols-2 gap-3 mt-3">
            {active.map((s) => (
              <AtlasTile key={s.id} section={s} onNavigate={onNavigate} />
            ))}
          </ul>
        </section>

        <section aria-label="Planned sections">
          <SectionLabel>Planned</SectionLabel>
          <ul className="grid grid-cols-1 sm:grid-cols-2 gap-3 mt-3">
            {planned.map((s) => (
              <AtlasTile key={s.id} section={s} onNavigate={onNavigate} />
            ))}
          </ul>
        </section>
      </div>
    </TooltipProvider>
  )
}

function SectionLabel({ children }: { children: React.ReactNode }) {
  return (
    <p className="font-mono uppercase tracking-widest text-[10px] text-ash/80">
      {children}
    </p>
  )
}

function AtlasTile({
  section,
  onNavigate,
}: {
  section: AtlasSection
  onNavigate: () => void
}) {
  const Icon = section.icon
  const body = (
    <div
      className={cn(
        "h-full w-full rounded-lg border p-3.5 text-left",
        "flex flex-col gap-1.5",
        "transition-colors",
        section.status === "active"
          ? "border-bronze bg-onyx hover:border-ember hover:bg-iron"
          : "border-bronze/40 bg-onyx/50 cursor-not-allowed opacity-70",
      )}
    >
      <div className="flex items-center gap-2">
        <Icon
          className={cn(
            "h-4 w-4 shrink-0",
            section.status === "active" ? "text-ember-bright" : "text-ash/60",
          )}
          aria-hidden
        />
        <span
          className={cn(
            "font-display text-sm font-semibold",
            section.status === "active" ? "text-parchment" : "text-parchment/70",
          )}
        >
          {section.title}
        </span>
      </div>
      <p
        className={cn(
          "text-xs leading-relaxed",
          section.status === "active" ? "text-ash" : "text-ash/70",
        )}
      >
        {section.description}
      </p>
      {section.status === "planned" && section.plannedNote && (
        <p className="font-mono uppercase tracking-widest text-[10px] text-ember/70 mt-1">
          {section.plannedNote}
        </p>
      )}
    </div>
  )

  if (section.status === "active" && section.route) {
    return (
      <li>
        <Link
          href={section.route}
          onClick={onNavigate}
          className="block h-full rounded-lg focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ember focus-visible:ring-offset-2 focus-visible:ring-offset-iron"
        >
          {body}
        </Link>
      </li>
    )
  }

  return (
    <li>
      <Tooltip>
        <TooltipTrigger asChild>
          <div
            role="button"
            tabIndex={0}
            aria-disabled="true"
            className="block h-full rounded-lg focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-bronze focus-visible:ring-offset-2 focus-visible:ring-offset-iron"
          >
            {body}
          </div>
        </TooltipTrigger>
        <TooltipContent
          side="top"
          className="bg-iron border border-bronze text-parchment text-xs max-w-[260px]"
        >
          Not built yet — the Council hasn&rsquo;t filed this beat.
        </TooltipContent>
      </Tooltip>
    </li>
  )
}
