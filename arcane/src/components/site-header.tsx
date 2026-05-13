"use client"

/*
 * Trusight — Site header (Steps 10 + 11).
 *
 * Persistent top bar mounted in the root layout. Carries:
 *   - Wordmark (Spectral) linking back to /
 *   - Sign-in / account menu (Step 11)
 *   - Atlas trigger (Compass icon) opening the site-map sheet
 *
 * Remaining slots (Bag badge, Sage quick-launch, Aceternity Spotlight in
 * Step 13) land in later steps. Height ~56px, Iron background with a
 * bronze bottom-border rule so the parchment text stays legible against
 * any page content below.
 */

import Image from "next/image"
import Link from "next/link"
import { Compass } from "lucide-react"

import { useAtlas } from "@/components/atlas"
import { SignInMenu } from "@/components/sign-in-menu"
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
          aria-label="Trusight — home"
          className="group focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ember rounded-sm"
        >
          <Image
            src="/logos/trusight_logo_4k_dark.png"
            alt="Trusight"
            width={180}
            height={29}
            priority
            className="h-7 w-auto group-hover:opacity-90 transition-opacity"
          />
        </Link>

        <div className="flex items-center gap-2">
          <SignInMenu />

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
      </div>
    </header>
  )
}
