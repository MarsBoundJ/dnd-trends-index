"use client"

/*
 * Arcane Analytics — Site header (Step 10).
 *
 * Persistent top bar mounted in the root layout. Carries:
 *   - Wordmark (Spectral) linking back to /
 *   - Atlas trigger (Compass icon, top-right) opening the site-map sheet
 *
 * Kept deliberately minimal — more slots (auth avatar, Bag badge, Sage
 * quick-launch) land in later steps. Height ~56px, Iron background with
 * a bronze bottom-border rule so the parchment text stays legible against
 * any page content below.
 */

import Link from "next/link"
import { Compass } from "lucide-react"

import { useAtlas } from "@/components/atlas"
import { cn } from "@/lib/utils"

export function SiteHeader() {
  const { openAtlas } = useAtlas()

  return (
    <header
      className={cn(
        "sticky top-0 z-30 w-full",
        "bg-iron/90 backdrop-blur-md",
        "border-b border-bronze/60",
      )}
    >
      <div className="mx-auto flex h-14 max-w-7xl items-center justify-between px-4 md:px-6">
        <Link
          href="/"
          className="group flex items-baseline gap-2 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ember rounded-sm"
        >
          <span className="font-display text-base font-semibold text-parchment group-hover:text-ember-bright transition-colors">
            Arcane Analytics
          </span>
          <span className="hidden sm:inline font-mono text-[10px] uppercase tracking-widest text-ash/60">
            Archive
          </span>
        </Link>

        <button
          type="button"
          onClick={openAtlas}
          aria-label="Open the Atlas site map"
          className={cn(
            "inline-flex items-center gap-2 rounded-md px-3 py-1.5",
            "border border-bronze/60 bg-onyx/60",
            "text-sm text-parchment",
            "transition-colors hover:border-ember hover:bg-iron",
            "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ember focus-visible:ring-offset-2 focus-visible:ring-offset-iron",
          )}
        >
          <Compass className="h-4 w-4 text-ember-bright" aria-hidden />
          <span className="hidden sm:inline">Atlas</span>
        </button>
      </div>
    </header>
  )
}
